from __future__ import annotations

__all__ = ['Instance', 'InstanceConfig']


import os
import string
import warnings
from typing import Literal

from google.cloud import compute_v1

from cloudly.util.logging import get_calling_file

from .auth import get_credentials, get_project_id, get_service_account_email


def validate_label_key(val: str) -> str:
    # TODO: use `re`.
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
        val = '__' + val[-61:].lstrip('-_')
        warnings.warn(
            f"long value was truncated; original value '{val0}' was changed to '{val}"
        )
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
            self, *, size_gb: int = 30, source_image: str = 'projects/ubuntu-os-cloud/global/images/family/ubuntu-2404-lts-amd64',
        ):
            if size_gb:
                assert size_gb >= 30, f'{size_gb} >= 30'
            self.size_gb = size_gb  # GCP default is 30
            self.source_image = source_image

        @property
        def disk(self) -> compute_v1.AttachedDisk:
            return compute_v1.AttachedDisk(
                boot=True,
                auto_delete=True,
                initialize_params=compute_v1.AttachedDiskInitializeParams(
                    source_image=self.source_image,
                ),
                disk_size_gb=self.size_gb,
            )

    class LocalSSD:
        def __init__(self, *, size_gb: int):
            assert size_gb >= 10
            self.size_gb = size_gb
            self.zone = None  # assigned separately after initialization

        @property
        def disk(self) -> compute_v1.AttachedDisk:
            return compute_v1.AttachedDisk(
                type_=compute_v1.AttachedDisk.Type.SCRATCH.name,
                interface='NVME',
                disk_size_gb=self.size_gb,
                initialize_params=compute_v1.AttachedDiskInitializeParams(
                    disk_type=f'zones/{self.zone}/diskTypes/local-ssd',
                ),
                auto_delete=True,
            )

    class GPU:
        def __init__(self, *, gpu_type: str, gpu_count: int):
            """
            `gpu_type`: values like 'nvidia-tesla-t4', 'nvidia-tesla-v100', etc.
            """
            assert gpu_type
            assert gpu_count
            self.gpu_type = gpu_type
            self.gpu_count = gpu_count
            self.zone = None  # to be assigned separately

        @property
        def accelerator(self) -> compute_v1.AcceleratorConfig:
            return compute_v1.AcceleratorConfig(
                accelerator_count=self.gpu_count,
                accelerator_type=f'projects/{get_project_id()}/zones/{self.zone}/acceleratorTypes/{self.gpu_type}',
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
        gpu: dict | None = None,
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
        disks.append(self.BootDisk(**(boot_disk or {})).disk)
        if local_ssd:
            ssd = self.LocalSSD(**local_ssd)
            ssd.zone = zone
            disks.append(ssd.disk)

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
        if gpu:
            gpu = self.GPU(**gpu)
            gpu.zone = zone
            guest_accelerators = [gpu.accelerator]
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
        self.name = name
        self.zone = zone

    @property
    def instance(self) -> compute_v1.Instance:
        return self._instance


def _call_client(method: str, *args, **kwargs):
    with compute_v1.InstancesClient(credentials=get_credentials()) as client:
        return getattr(client, method)(*args, **kwargs)


class Instance:
    @classmethod
    def create(cls, config: InstanceConfig) -> Instance:
        req = compute_v1.InsertInstanceRequest(
            project=get_project_id(),
            zone=config.zone,
            instance_resource=config.instance,
        )
        op = _call_client('insert', req)
        op.result()
        # This could raise `google.api_core.exceptions.Forbidden` with message "... QUOTA_EXCEEDED ..."
        return cls(config.name, config.zone)

    @classmethod
    def list(cls, zone: str) -> list[Instance]:
        req = compute_v1.ListInstancesRequest(project=get_project_id(), zone=zone)
        resp = _call_client('list', req)
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

    def __repr__(self):
        return f"{self.__class__.__name__}('{self.name}')"

    def __str__(self):
        return self.__repr__()

    @property
    def name(self) -> str:
        return self._name

    def _refresh(self):
        req = compute_v1.GetInstanceRequest(
            instance=self._name, project=get_project_id(), zone=self._zone
        )
        self._instance = _call_client('get', req)
        # This could raise `google.api_core.exceptions.NotFound`

    def delete(self) -> None:
        req = compute_v1.DeleteInstanceRequest(
            instance=self._name, project=get_project_id(), zone=self._zone
        )
        op = _call_client('delete', req)
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
