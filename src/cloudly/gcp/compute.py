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


# Using GPUs
# https://cloud.google.com/nvidia?hl=en
# https://cloud.google.com/compute/docs/gpus/install-drivers-gpu
# https://cloud.google.com/deep-learning-vm/docs/images
# https://cloud.google.com/deep-learning-vm/docs/images#listing-versions
# https://cloud.google.com/deep-learning-vm/docs/create-vm-instance-gcloud
# https://www.googlecloudcommunity.com/gc/Infrastructure-Compute-Storage/Frequent-NVIDIA-drivers-reinstall-needed-on-boot/m-p/797413
#
# There are mainly two things to make GPU work:
# (1) specify a machine with GPU
# (2) install nvidia drivers
#
# The easy route seems to be:
# (1) use a machine-type that comes with GPUs
# (2) use a boot-image that has nvidia drivers installed
#
# Worked-out scenarios:
#
#    machine_type: 'g2-standard-16'
#    boot_disk: {'size_gb': 100, 'source_image': 'projects/deeplearning-platform-release/global/images/family/pytorch-latest-gpu'}
#
# This machine comes with a Nvidia L4, python 3.10 and pytorch. The OS is Debian 11 (bullseye).
#
# In place of 'pytorch-latest-gpu', these images also worked in tests: 'common-cu123', 'common-cu124'.
# The 'common-...' images does not have pytorch installed.
#
# In place of 'g2-standard-16', machine-type 'a2-highgpu-1g' also worked, coming with a Nvidia A100.
#
# You can also specify flexible machine_type and GPUs but use a Deeplearning boot image to ease the installation
# of Nvidia drivers. The following gets a machine with two T4 GPUs:
#
#   machine_type: 'n1-standard-8
#   gpu: {'gpu_count': 2, 'gpu_type': 'nvidia-tesla-t4'}
#   boot_disk: {'size_gb': 100, 'source_image': 'projects/deeplearning-platform-release/global/images/family/common-cu124'}


# Taken from
#   https://github.com/GoogleCloudPlatform/compute-gpu-installation
# not working yet
cuda_installer = """
#!/bin/bash
if test -f /opt/google/cuda-installer
then
  exit
fi

sudo mkdir -p /opt/google/cuda-installer/
cd /opt/google/cuda-installer/ || exit

sudo curl -fSsL -O https://github.com/GoogleCloudPlatform/compute-gpu-installation/releases/download/cuda-installer-v1.2.0/cuda_installer.pyz
sudo python3 cuda_installer.pyz install_cuda
"""


class InstanceConfig:
    class BootDisk:
        def __init__(
            self,
            *,
            size_gb: int = 50,
            source_image: str = 'projects/ubuntu-os-cloud/global/images/family/ubuntu-2404-lts-amd64',
        ):
            # See a list of OS images: https://cloud.google.com/compute/docs/images/os-details
            # Another good image might be 'projects/debian-cloud/global/images/family/debian-11'.
            if size_gb:
                assert size_gb >= 30, f'{size_gb} >= 30'
            self.size_gb = size_gb  # GCP default is 30, but a GPU machine would require at least 40
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

        @property
        def startup_script(self) -> str:
            if self.source_image.startswith('projects/deeplearning-platform-release'):
                if '-gpu' in self.source_image or '-cu' in self.source_image:
                    return 'sudo /opt/deeplearning/install-driver.sh'
            return ''

    class LocalSSD:
        def __init__(self, *, size_gb: int, mount_path: str = '/mnt', mode: str = 'rw'):
            """
            `size_gb` should be a multiple of 375. If not,
            the next greater multiple of 375 will be used.


            """
            a, b = divmod(size_gb, 375)
            if 0 < b < 300:
                # Fail rather than round up a great deal, for visibility.
                raise ValueError(
                    f'`size_gb` for LocalSSD should be a multiple of 375; got {size_gb}'
                )
            elif b:
                # Round up with a warning.
                warnings.warn(
                    f'`size_gb` for LocalSSD is rounded up from {size_gb} to {375 * (a + 1)}'
                )
                size_gb = 375 * (a + 1)
            else:  # b == 0
                if a == 0:
                    raise ValueError(
                        f'`size_gb` for LocalSSD should be a multiple of 375; got {size_gb}'
                    )
            self.size_gb = size_gb
            self.zone = None  # assigned separately after initialization
            self.mount_path = mount_path
            self.mode = mode

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

        @property
        def mount_script(self) -> str:
            # See https://cloud.google.com/compute/docs/disks/add-local-ssd#formatandmount
            # We support a single local SSD disk. It's name is always 'google-local-nvme-ssd-0'.
            mode = self.mode
            if mode == 'ro':
                mode = 'r'
            return '\n'.join(
                'sudo mkfs.ext4 -F /dev/disk/by-id/google-local-nvme-ssd-0',
                f'sudo mkdir -p {self.mount_path}',
                f'sudo mount /dev/disk/by-id/google-local-nvme-ssd-0 {self.mount_path}',
                f'sudo chmod a+{mode} {self.mount_path}' '',
            )

    class GPU:
        def __init__(self, *, gpu_type: str, gpu_count: int):
            """
            Use `gcloud compute accelerator-types list` to see valid values of `gpu_type`.
            Some examples: 'nvidia-tesla-t4', 'nvidia-l4', 'nvidia-tesla-a100', 'nvidia-tesla-v100'
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

        startup_scripts = []
        disks = []

        boot_disk = self.BootDisk(**(boot_disk or {}))
        startup_scripts.append(boot_disk.startup_script)
        disks.append(boot_disk.disk)

        if local_ssd:
            ssd = self.LocalSSD(**local_ssd)
            ssd.zone = zone
            disks.append(ssd.disk)
            startup_scripts.append(ssd.mount_script)

        guest_accelerators = None
        scheduling = None
        if machine_type.split('-')[0] in ('a3', 'a2', 'g2'):
            # machine types that come with GPUs
            if gpu is not None:
                raise ValueError(
                    f'machine_type {machine_type} comes with GPUs; you should not specify `gpu` again'
                )
            scheduling = compute_v1.Scheduling(on_host_maintenance='TERMINATE')
        elif gpu:
            gpu = self.GPU(**gpu)
            gpu.zone = zone
            guest_accelerators = [gpu.accelerator]
            scheduling = compute_v1.Scheduling(on_host_maintenance='TERMINATE')
            # See https://cloud.google.com/compute/docs/instances/setting-vm-host-options

        network = compute_v1.NetworkInterface(
            network=network_uri, subnetwork=subnet_uri
        )

        metadata = None
        if not startup_script:
            if startup_scripts:
                startup_script = '\n'.join(['#!/bin/bash'] + startup_scripts)
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
        # printing the output to see info.
        return self._instance

    @property
    def definition(self) -> dict:
        return type(self._instance).to_dict(self._instance)


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
        return [cls(r.name, zone) for r in resp]

    def __init__(self, name: str, zone: str):
        self.name = name
        self.zone = zone
        self.instance = None
        self.refresh()

    def __repr__(self):
        return f"{self.__class__.__name__}('{self.name}', '{self.zone}')"

    def __str__(self):
        return self.__repr__()

    def refresh(self):
        req = compute_v1.GetInstanceRequest(
            instance=self.name, project=get_project_id(), zone=self.zone
        )
        self.instance = _call_client('get', req)
        # This could raise `google.api_core.exceptions.NotFound`
        return self

    @property
    def machine_type(self) -> str:
        return self.instance.machine_type.split('/')[-1]

    @property
    def gpu(self) -> dict:
        z = self.instance.guest_accelerators
        if len(z):
            return type(z[0]).to_dict(z[0])
        return {}

    @property
    def disks(self) -> list[dict]:
        return [
            {
                'boot': disk.boot,
                'name': disk.device_name,
                'size_gb': disk.disk_size_gb,
                'index': disk.index,
                'mode': disk.mode,
            }
            for disk in self.instance.disks
        ]  # first is boot disk

    @property
    def creation_timestamp(self) -> str:
        return self.instance.creation_timestamp

    @property
    def last_start_timestamp(self) -> str:
        return self.instance.last_start_timestamp

    @property
    def last_stop_timestamp(self) -> str:
        # Can be ''.
        return self.instance.last_stop_timestamp

    @property
    def ip(self) -> str:
        # IP address.
        # If you try to `ssh` into this IP address right after seeing success of `Instance.create(...)`,
        # you may get "connection refused". Just wait a few more seconds for the machine to be ready.
        return self.instance.network_interfaces[0].network_i_p

    def delete(self) -> None:
        req = compute_v1.DeleteInstanceRequest(
            instance=self.name, project=get_project_id(), zone=self.zone
        )
        op = _call_client('delete', req)
        op.result()
        self.instance = None

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
        return self.instance.status
