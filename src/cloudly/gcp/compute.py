from __future__ import annotations

__all__ = ['Instance', 'InstanceConfig']


import os
import string
from typing import Literal

from google.cloud import compute_v1

from cloudly.util.logging import get_calling_file

from .auth import get_credentials, get_project_id, get_service_account_email


def validate_label_key(val: str) -> str:
    if len(val) < 1 or len(val) > 63:
        raise ValueError(val)
    allowed = string.ascii_lowercase + string.digits + '-_'
    if any(c not in allowed for c in val):
        raise ValueError(val)
    if val[0] not in string.ascii_lowercase:
        raise ValueError(val)
    return val


def validate_label_value(val: str, *, fix: bool = False) -> str:
    val0 = val
    if fix:
        val = val.strip('- ').lower()
        for a in ('<', '>', ' ', '_', '.', ','):
            val = val.replace(a, '-')
        val = val.replace('/', '--')
        val = val.strip(' -')

    if len(val) > 63:
        raise ValueError(f"original: '{val0}'; after fixes: '{val}'")
    allowed = string.ascii_lowercase + string.digits + '-_'
    if any(c not in allowed for c in val):
        raise ValueError(f"original: '{val0}'; after fixes: '{val}'")
    return val


def basic_resource_labels():
    caller = get_calling_file()
    return {
        'created-by-file': os.path.abspath(caller.filename),
        'created-by-line': str(caller.lineno),
        'created-by-function': caller.function,
    }


class InstanceConfig:
    class BootDisk:
        def __init__(
            self, *, disk_size_gb: int | None = None, source_image: str | None = None
        ):
            self._disk_size_gb = disk_size_gb or 100
            self._source_image = (
                source_image
                or 'projects/ubuntu-os-cloud/global/images/family/ubuntu-2404-lts-amd64'
            )

        @property
        def attached_disk(self) -> compute_v1.AttachedDisk:
            return compute_v1.AttachedDisk(
                boot=True,
                auto_delete=True,
                initialize_params=compute_v1.AttachedDiskInitializeParams(
                    source_image=self._source_image,
                ),
                disk_size_gb=self._disk_size_gb,
            )

    class LocalSSD:
        def __init__(self, *, disk_size_gb: int):
            assert disk_size_gb >= 10
            self._disk_size_gb = disk_size_gb
            self._zone = None  # assigned separately after initialization

        @property
        def attached_disk(self) -> compute_v1.AttachedDisk:
            return compute_v1.AttachedDisk(
                type_=compute_v1.AttachedDisk.Type.SCRATCH.name,
                interface='NVME',
                disk_size_gb=self._disk_size_gb,
                initialize_params=compute_v1.AttachedDiskInitializeParams(
                    disk_type=f'zones/{self._zone}/diskTypes/local-ssd',
                ),
                auto_delete=True,
            )

    def __init__(
        self,
        *,
        name: str,
        zone: str,
        machine_type: str,
        labels: dict[str, str] | None = None,
        boot_disk: dict | None = None,
        local_ssd: dict | None = None,
        network_uri: str,
        subnet_uri: str,
        startup_script: str | None = None,
        gpu_type: str | None = None,
        gpu_count: int | None = None,
    ):
        """
        `network_uri` may look like "projects/shared-vpc-admin/global/networks/vpcnet-shared-prod-01".
        `subnet_uri` may look like "https://www.googleapis.com/compute/v1/projects/shared-vpc-admin/regions/<region>/subnetworks/prod-<region>-01".

        `startup_script`: shell script that runs to make preparations before the instance is operational.

        There are some restrictions to the label values.
        See https://cloud.google.com/batch/docs/organize-resources-using-labels
        """
        validate_label_key(name)

        labels = {**basic_resource_labels(), **(labels or {})}
        labels = {
            validate_label_key(k): validate_label_value(v, fix=True)
            for k, v in labels.items()
        }

        disks = []
        disks.append(self.BootDisk(**(boot_disk or {})).attached_disk)
        if local_ssd:
            ssd = self.LocalSSD(**local_ssd)
            ssd._zone = zone
            disks.append(ssd.attached_disk)

        network = compute_v1.NetworkInterface(
            network=network_uri, subnetwork=subnet_uri
        )
        metadata = None
        if startup_script:
            metadata = compute_v1.Metadata(
                items=[compute_v1.Items(key='startup-script', value=startup_script)]
            )
        service_accounts = [
            compute_v1.ServiceAccount(
                email=get_service_account_email(),
                scopes=['https://www.googleapis.com/auth/cloud-platform'],
            ),
        ]
        guest_accelerators = None
        scheduling = None
        if gpu_type and gpu_count:
            guest_accelerators = [
                compute_v1.AcceleratorConfig(
                    accelerator_count=gpu_count,
                    accelerator_type=f'projects/{get_project_id()}/zones/{zone}/acceleratorTypes/{gpu_type}',
                )
            ]
            scheduling = compute_v1.Scheduling(on_host_maintenance='TERMINATE')
            # See https://cloud.google.com/compute/docs/instances/setting-vm-host-options

        self._instance = compute_v1.Instance(
            name=name,
            machine_type=f'zones/{zone}/machineTypes/{machine_type}',
            labels=labels,
            disks=disks,
            network_interfaces=[network],
            metadata=metadata,
            service_accounts=service_accounts,
            guest_accelerators=guest_accelerators,
            scheduling=scheduling,
        )

    @property
    def instance(self) -> compute_v1.Instance:
        return self._instance


class Instance:
    @classmethod
    def _client(cls) -> compute_v1.InstancesClient:
        return compute_v1.InstancesClient(credentials=get_credentials())

    @classmethod
    def create(cls, *, name: str, zone: str, **kwargs) -> Instance:
        config = InstanceConfig(name=name, zone=zone, **kwargs).instance
        req = compute_v1.InsertInstanceRequest(
            project=get_project_id(), zone=zone, instance_resource=config
        )
        op = cls._client().insert(req)
        op.result()
        # This could raise `google.api_core.exceptions.Forbidden` with message "... QUOTA_EXCEEDED ..."
        return cls(name, zone)

    @classmethod
    def list(cls, zone: str) -> list[Instance]:
        req = compute_v1.ListInstancesRequest(project=get_project_id(), zone=zone)
        resp = cls._client().list(req)
        zz = []
        for r in resp:
            o = cls(r.name, zone)
            o._instance = r
            zz.append(o)
        return zz

    def __init__(self, name: str, zone: str):
        self._name = name
        self._zone = zone
        self._instance = None

    @property
    def name(self) -> str:
        return self._name

    def _refresh(self):
        req = compute_v1.GetInstanceRequest(
            instance=self._name, project=get_project_id(), zone=self._zone
        )
        self._instance = self._client().get(req)
        # This could raise `google.api_core.exceptions.NotFound`

    def delete(self) -> None:
        req = compute_v1.DeleteInstanceRequest(
            instance=self._name, project=get_project_id(), zone=self._zone
        )
        op = self._client().delete(req)
        op.result()

    def state(
        self,
    ) -> Literal[
        'PROVISIONING',
        'STAGING',
        'RUNNING',
        'STOPPING',
        'SUSPENDING',
        'SUSPENDED',
        'REPAIRING',
        'TERMINATED',
    ]:
        self._refresh()
        return self._instance.status
