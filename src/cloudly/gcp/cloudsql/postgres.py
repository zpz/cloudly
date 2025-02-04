from __future__ import annotations

import concurrent.futures
import http
import time
from multiprocessing.util import Finalize
from typing import Literal

from google.api_core.exceptions import NotFound
from googleapiclient import discovery
from googleapiclient.errors import HttpError

from cloudly.gcp.auth import get_project_id
from cloudly.gcp.compute import (
    basic_resource_labels,
    validate_label_key,
    validate_label_value,
)

from ._postgres import attach_load_balancer


def get_client():
    client = discovery.build('sqladmin', 'v1beta4')
    Finalize(client, client._http.close)
    return client


def _wait_on_operation(name, *, timeout: float = 600):
    t0 = time.perf_counter()
    while True:
        try:
            op = (
                get_client()
                .operations()
                .get(project=get_project_id(), operation=name)
                .execute()
            )
        except http.client.CannotSendHeader:
            # google.auth.exceptions.TransportError:
            # It seems this happens when the op is done (and maybe gone?), but not sure.
            break
        if op['status'] == 'DONE':
            break
        t1 = time.perf_counter()
        if t1 - t0 >= timeout:
            raise concurrent.futures.TimeoutError(f'{t1 - t0} seconds')
        time.sleep(2)


class Instance:
    """
    This is a CloudSQL "instance". It may be a single node or a "master" node with one or more read replicas.
    """

    @classmethod
    def _create(
        cls,
        instance_name: str,
        *,
        region: str,
        database_version: str,
        root_password: str | None = None,
        master_instance_name: str = None,
        machine_type: str | None = None,
        storage_type: Literal['SSD', 'HDD'] | None = None,
        storage_size: str | None = None,
        labels: dict | None = None,
    ):
        """
        Parameters
        ----------
        instance_name
            This needs to be unique in the project.

        region
            Like 'us-west1'.

        machine_type
            Only custom or shared-core machine types are allowed for Postgres. See https://cloud.google.com/sql/docs/postgres/instance-settings

            The default value used by this function is relatively low-end and may not be enough for your application.
        private_network
            The resource link for the VPC network from which the Cloud SQL instance is accessible for private IP. For example, `/projects/myProject/global/networks/default`.
        master_instance_name
            If not `None`, then what is being created is a read replica.
            If `None`, then what is being created is the "master instance".

        See https://cloud.google.com/sql/docs/postgres/create-instance
        """
        # Requirements on name:
        #   length in [1, 62]
        #   pattern `[a-z]([-a-z0-9]*[a-z0-9])?`
        labels = {**basic_resource_labels(), **(labels or {})}
        labels = {
            validate_label_key(k): validate_label_value(v, fix=True)
            for k, v in labels.items()
        }

        # TODO: networking setup; other options
        config = {
            'name': instance_name,
            'master_instance_name': master_instance_name,
            'region': region,
            'databaseVersion': database_version,
            'rootPassword': root_password,
            'settings': {
                'tier': machine_type or 'db-perf-optimized-N-2',
                'userLabels': labels,
                'storageSize': storage_size or '15GB',
                'storageType': storage_type or 'SSD',
                'ipConfiguration': {
                    'ipv4Enabled': True,
                },
            },
        }
        req = get_client().instances().insert(project=get_project_id(), body=config)
        op = req.execute()
        _wait_on_operation(op['name'])
        return instance_name

    @classmethod
    def _delete(cls, instance_name: str) -> None:
        req = (
            get_client()
            .instances()
            .delete(project=get_project_id(), instance=instance_name)
        )
        try:
            op = req.execute()
            _wait_on_operation(op['name'])
        except HttpError as e:
            if e.error_details[0]['reason'] == 'instanceDoesNotExist':
                raise NotFound(f"instance '{instance_name}' does not exist") from e
            raise

    @classmethod
    def _get(cls, instance_name: str) -> dict:
        req = (
            get_client()
            .instances()
            .get(project=get_project_id(), instance=instance_name)
        )
        try:
            return req.execute()
        except HttpError as e:
            if e.error_details[0]['reason'] == 'instanceDoesNotExist':
                raise NotFound(f"instance '{instance_name}' does not exist") from e
            raise

    @classmethod
    def create(
        cls,
        name: str,
        *,
        region: str,
        root_password: str,
        # network_uri: str | None = None,
        # subnet_uri: str | None = None,
        postgres_version: str = '16',
        num_read_replicas: int = 0,
        **kwargs,
    ):
        """
        `name` is the name of the "master instance". This needs to be unique in the project.
        If `num_read_replicas > 0`, names of the replicas are constructed based on this `name`.
        """
        dbversion = f'POSTGRES_{postgres_version}'
        master_instance_name = cls._create(
            name,
            database_version=dbversion,
            region=region,
            root_password=root_password,
            **kwargs,
        )
        # set_default_password(master_instance_name, username, password)

        if num_read_replicas:
            for idx in range(num_read_replicas):
                cls._create(
                    f'{name}-replica-{idx + 1}',
                    master_instance_name=master_instance_name,
                    database_version=dbversion,
                    region=region,
                    **kwargs,
                )
            attach_load_balancer(master_instance_name, zone=f'{region}-a')

        return cls(master_instance_name)

    @classmethod
    def list(cls) -> list[Instance]:
        req = get_client().instances().list(project=get_project_id())
        rep = req.execute()
        return [cls(item['name']) for item in rep['items']]

    def __init__(self, name: str):
        self.name = name
        inst = self.instance
        assert inst['name'] == name
        self.replica_names = inst.get('replicaNames', [])
        self.zone = inst['gceZone']
        self.create_time = inst['createTime']
        self.machine_type = inst['settings']['tier']
        self.instance_type = inst['instanceType']
        self.data_disk_type = inst['settings']['dataDiskType']
        self.data_disk_size_gb = inst['settings']['dataDiskSizeGb']
        self.url = inst.get('self_link')
        self.host_ip = inst.get('ipAddresses', [{}])[0].get('ipAddress')

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}('{self.name}')"

    def __str__(self) -> str:
        return self.__repr__()

    @property
    def instance(self) -> dict:
        return self._get(self.name)

    def state(self) -> str:
        return self.instance['state']

    def delete(self) -> None:
        for name in self.replica_names:
            self._delete(name)
        self._delete(self.name)
