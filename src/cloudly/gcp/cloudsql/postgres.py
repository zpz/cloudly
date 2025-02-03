import concurrent.futures
import time
from multiprocessing.util import Finalize
from typing import Literal
from uuid import uuid4

from googleapiclient import discovery

from cloudly.gcp.auth import get_project_id
from cloudly.gcp.compute import (
    basic_resource_labels,
    validate_label_key,
    validate_label_value,
)

from ._postgres import attach_load_balancer, set_default_password


def get_client():
    client = discovery.build('sqladmin', 'v1beta4')
    Finalize(client, client._http.close)
    return client


def _wait_on_operation(name, *, timeout: float = 600):
    t0 = time.perf_counter()
    while True:
        op = (
            get_client()
            .operations.get(project=get_project_id(), operation=name)
            .execute()
        )
        if op['status'] == 'DONE':
            break
        t1 = time.perf_counter()
        if t1 - t0 >= timeout:
            raise concurrent.futures.TimeoutError(f'{t1 - t0} seconds')
        time.sleep(2)


def create_instance(
    instance_name: str,
    *,
    region: str,
    database_version: str,
    private_network: str,
    master_instance_name: str = None,
    machine_type: str | None = None,
    storage_type: Literal['SSD', 'HDD'] | None = None,
    storage_size: str | None = None,
    labels: dict | None = None,
):
    """
    Parameters
    ----------
    machine_type
        Only custom or shared-core machine types are allowed for Postgres. See https://cloud.google.com/sql/docs/postgres/instance-settings

        The default value used by this function is relatively low-end and may not be enough for your application.
    private_network
        The resource link for the VPC network from which the Cloud SQL instance is accessible for private IP. For example, `/projects/myProject/global/networks/default`.
    master_instance_name
        If not `None`, then what is being created is a read replica.
        If `None`, then what is being created is the "master instance".
    """
    instance_name = f"{instance_name}-{str(uuid4()).replace('-', '')[:8]}"
    # Requirements on name:
    #   length in [1, 62]
    #   pattern `[a-z]([-a-z0-9]*[a-z0-9])?`
    labels = {**basic_resource_labels(), **(labels or {})}
    labels = {
        validate_label_key(k): validate_label_value(v, fix=True)
        for k, v in labels.items()
    }

    config = {
        'name': instance_name,
        'master_instance_name': master_instance_name,
        'region': region,
        'databaseVersion': database_version,
        'settings': {
            'tier': machine_type or 'db-perf-optimized-N-2',
            'userLabels': labels,
            'storageSize': storage_size or '15GB',
            'storageType': storage_type or 'SSD',
            'ipConfiguration': {
                'ipv4Enabled': False,
                'privateNetwork': private_network,
            },
        },
    }
    req = get_client().instances().insert(project=get_project_id(), body=config)
    op = req.execute()
    _wait_on_operation(op['name'])
    return instance_name


def create_cluster(
    name: str,
    *,
    region: str,
    username: str,
    password: str,
    network_uri: str | None = None,
    subnet_uri: str | None = None,
    postgres_version: str = '16',
    num_read_replicas: int = 0,
    **kwargs,
) -> str:
    master_instance = create_instance(
        f'{name}-master',
        database_version=f'POSTGRES_{postgres_version}',
        private_network=network_uri,
        region=region,
        **kwargs,
    )
    set_default_password(master_instance, username, password)

    if num_read_replicas:
        for idx in range(num_read_replicas):
            create_instance(
                f'{name}-replica-{idx + 1}',
                master_instance_name=master_instance,
                **kwargs,
            )
        attach_load_balancer(
            master_instance, network_uri=network_uri, zone=f'{region}-a'
        )

    return master_instance
