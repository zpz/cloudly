from __future__ import annotations

__all__ = ['Instance', 'connect']


import concurrent.futures
import http
import logging
import time
from multiprocessing.util import Finalize
from typing import Literal

import google.auth
import pg8000
from google.api_core.exceptions import NotFound
from google.cloud.sql.connector import Connector, IPTypes
from googleapiclient import discovery
from googleapiclient.errors import HttpError

from cloudly.gcp.auth import get_project_id
from cloudly.gcp.compute import (
    basic_resource_labels,
    validate_label_key,
    validate_label_value,
)

from ._postgres import attach_load_balancer, delete_load_balancer

logging.getLogger('googleapiclient.discovery_cache').setLevel(logging.WARNING)
# Suppress the message "file_cache is only supported with oauth2client<4.0.0"


def get_client():
    # Beware: if you use the returned object "on-the-fly" in one line of code, then in subsequent lines
    # this object may have been closed; if you call `.execute()` in subsequent lines, that call may still
    # need the connection.
    client = discovery.build('sqladmin', 'v1beta4')
    Finalize(client, client._http.close)
    return client


def _wait_on_operation(name, *, timeout: float = 600):
    t0 = time.perf_counter()
    client = get_client()
    while True:
        try:
            op = (
                client.operations()
                .get(project=get_project_id(), operation=name)
                .execute()
            )
        except (http.client.CannotSendHeader, google.auth.exceptions.TransportError):
            # It seems this happens when the op is done (and maybe gone?), but not sure.
            break
        if op['status'] == 'DONE':
            break
        print(f"waiting on operation '{name}'; current state: {op['status']}")
        t1 = time.perf_counter()
        if t1 - t0 >= timeout:
            raise concurrent.futures.TimeoutError(f'{t1 - t0} seconds')
        time.sleep(10)


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
        root_password
            Password for the user 'postgres'.
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
        op = (
            get_client()
            .instances()
            .insert(project=get_project_id(), body=config)
            .execute()
        )
        _wait_on_operation(op['name'])
        return instance_name

    @classmethod
    def _delete(cls, instance_name: str, *, wait: bool = True) -> str:
        client = get_client()
        req = client.instances().delete(
            project=get_project_id(), instance=instance_name
        )
        try:
            op = req.execute()
            if wait:
                _wait_on_operation(op['name'])
            return op['name']
        except HttpError as e:
            if e.error_details[0]['reason'] == 'instanceDoesNotExist':
                raise NotFound(f"instance '{instance_name}' does not exist") from e
            raise

    @classmethod
    def _get(cls, instance_name: str) -> dict:
        client = get_client()
        req = client.instances().get(project=get_project_id(), instance=instance_name)
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
        load_balancer_machine_type: str | None = None,
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

        if num_read_replicas:
            master = cls(master_instance_name)
            t0 = time.perf_counter()
            while (state := master.state()) != 'RUNNABLE':
                if time.perf_counter() - t0 > 600:
                    raise concurrent.futures.TimeoutError(
                        f"timed out waiting for instance '{master_instance_name}' to be RUNNABLE"
                    )
                print('waiting for master instance; current state:', state)
                time.sleep(5)

            for idx in range(num_read_replicas):
                cls._create(
                    f'{name}-replica-{idx + 1}',
                    master_instance_name=master_instance_name,
                    database_version=dbversion,
                    region=region,
                    **kwargs,
                )
            attach_load_balancer(
                master_instance_name,
                postgres_version=postgres_version,
                zone=f'{region}-a',
                machine_type=load_balancer_machine_type,
            )

        return cls(master_instance_name)

        # TODO: add read replica on-demand to an existing instance?
        # Similarly, can we remove read replica dynamically?

    @classmethod
    def list(cls) -> list[Instance]:
        resp = get_client().instances().list(project=get_project_id()).execute()
        return [cls(item['name']) for item in resp['items']]

    def __init__(self, name: str):
        self.name = name
        inst = self.instance
        assert inst['name'] == name
        self.replica_names = inst.get('replicaNames', [])
        self.region = inst['region']
        self.zone = inst['gceZone']
        self.create_time = inst['createTime']
        self.machine_type = inst['settings']['tier']
        self.instance_type = inst['instanceType']
        self.data_disk_type = inst['settings']['dataDiskType']
        self.data_disk_size_gb = inst['settings']['dataDiskSizeGb']
        self.url = inst.get('self_link')
        self.host_ip = inst.get('ipAddresses', [{}])[0].get('ipAddress')
        self.connection_name = inst['connectionName']

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}('{self.name}')"

    def __str__(self) -> str:
        return self.__repr__()

    @property
    def instance(self) -> dict:
        return self._get(self.name)

    def state(self) -> str:
        # 'PENDING', 'RUNNABLE', etc
        return self.instance['state']

    def delete(self) -> None:
        replicas = self.replica_names
        if replicas:
            delete_load_balancer(self.name, f'{self.region}-a')
            for name in replicas:
                self._delete(name, wait=False)
            t0 = time.perf_counter()
            while replicas := self.instance.get('replicaNames'):
                if time.perf_counter() - t0 > 600:
                    raise concurrent.futures.TimeoutError(
                        'timed out waiting for deletion of replicas'
                    )
                print(f'waiting for deletion of replicas: {replicas}')
                time.sleep(5)
        t0 = time.perf_counter()
        while True:
            try:
                self._delete(self.name)
                break
            except HttpError as e:
                if 'because another operation was already in progress' in str(e):
                    if time.perf_counter() - t0 > 600:
                        raise concurrent.futures.TimeoutError(
                            f"timed out waiting for deletion of master node '{self.name}'"
                        )
                    time.sleep(2)
                else:
                    raise


# Try to make the API closer to that of `psycopg`


class Connection(pg8000.Connection):
    def cursor(self) -> Cursor:
        cu = super().cursor()
        cu.__class__ = Cursor
        return cu

    def execute(self, sql) -> Cursor:
        """
        `pg8000.Connection.execute` is very different from `psycopg.Connection.execute`.
        """
        return self.cursor().execute(sql)


class Cursor(pg8000.Cursor):
    def execute(self, sql, *args):
        """
        `pg800.Cursor.execute` returns None.
        """
        super().execute(sql, args)
        return self


def connect(
    *, connection_name: str, user: str = 'postgres', password: str, db: str = 'postgres'
) -> pg8000.Connection:
    """
    This is a temporary solution.

    First, I'm waiting for Google's 'connector' package to support `psycopg`
    (which they say is in the works, see https://github.com/GoogleCloudPlatform/cloud-sql-python-connector/issues/219).
    Once that's in place, I plan to switch from `pg8000` to `psycopg`.

    Second, I haven't figured out all the GCP authentication stuff. I could not make `psycopg.connect` work with "username" and "password",
    and found this `google.cloud.sql.connector` to work out of the box (with much relief).
    If you have made `psycopg.connect` work, then you don't need to use this function.
    You can use a `psycopg.Connection` with the utilities in `cloudly.sql.postgres`.
    """
    connector = Connector(IPTypes.PUBLIC)
    conn = connector.connect(
        connection_name, 'pg8000', user=user, password=password, db=db
    )
    conn.autocommit = True
    conn.__class__ = Connection
    return conn
