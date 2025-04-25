from __future__ import annotations

__all__ = ['Instance', 'InstanceConfig']


import logging
import os
import string
import warnings
from typing import Literal

from google.cloud import compute_v1

from cloudly.util.logging import get_calling_file

from .auth import get_credentials, get_project_id

logger = logging.getLogger(__name__)


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
    """
    See https://cloud.google.com/compute/docs/labeling-resources
    """
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


def validate_local_ssd_size_gb(size_gb: int) -> int:
    # Use the returned value.
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
    return size_gb


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
#   machine_type: 'n1-standard-8'
#   gpu: {'gpu_count': 2, 'gpu_type': 'nvidia-tesla-t4'}
#   boot_disk: {'size_gb': 100, 'source_image': 'projects/deeplearning-platform-release/global/images/family/common-cu124'}
#
# Finally, you can also use the common boot image (which has no consideration for GPU) and install cuda drivers on startup
# (which is taken care of by this module). (The 'deeplearning' images above also need to run installation, only simpler,
# because the script is already on the disk.) The following worked in tests:
#
#  machine_type: 'n1-standard-8'
#  gpu: {'gpu_count': 2, 'gpu_type': 'nvidia-tesla-t4'}
#  boot_disk: {'size_gb': 100}

# Taken from
#   https://github.com/GoogleCloudPlatform/compute-gpu-installation
#
# This script will reboot the machine 2 or 3 times, so it may take
# a few minutes for the machine to be ready. Being able to `ssh` into
# the machine does not guarantee this script has all finished.
# Instead, type `nvidia-smi`. If it works, this script has finished.
cuda_installer = """
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
            source_image: str = 'projects/debian-cloud/global/images/family/debian-11',
        ):
            # See a list of OS images: https://cloud.google.com/compute/docs/images/os-details
            # Another good image might be 'projects/ubuntu-os-cloud/global/images/family/ubuntu-2404-lts-amd64',

            if size_gb:
                assert size_gb >= 30, f'{size_gb} >= 30'
            self.size_gb = size_gb  # GCP default is 30, but a GPU machine would require at least 40
            if source_image.count('/') == 1:
                proj, fam = source_image.split('/')
                source_image = f'projects/{proj}/global/images/family/{fam}'
            self.source_image = source_image

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
            self.size_gb = validate_local_ssd_size_gb(size_gb)
            self.mount_path = mount_path
            self.mode = mode

        def disk(self, zone: str) -> compute_v1.AttachedDisk:
            return compute_v1.AttachedDisk(
                type_=compute_v1.AttachedDisk.Type.SCRATCH.name,
                interface='NVME',
                disk_size_gb=self.size_gb,
                initialize_params=compute_v1.AttachedDiskInitializeParams(
                    disk_type=f'zones/{zone}/diskTypes/local-ssd',
                ),
                auto_delete=True,
            )

        @property
        def startup_script(self) -> str:
            # See https://cloud.google.com/compute/docs/disks/add-local-ssd#formatandmount
            # We support a single local SSD disk. It's name is always 'google-local-nvme-ssd-0'.
            mode = self.mode
            if mode == 'ro':
                mode = 'r'
            return '\n'.join(
                (
                    'sudo mkfs.ext4 -F /dev/disk/by-id/google-local-nvme-ssd-0',
                    f'sudo mkdir -p {self.mount_path}',
                    f'sudo mount /dev/disk/by-id/google-local-nvme-ssd-0 {self.mount_path}',
                    f'sudo chmod a+{mode} {self.mount_path}',
                )
            )

    class GPU:
        def __init__(self, *, gpu_type: str, gpu_count: int = 1):
            """
            Use `gcloud compute accelerator-types list` to see valid values of `gpu_type`.
            Some examples: 'nvidia-tesla-t4', 'nvidia-l4', 'nvidia-tesla-a100', 'nvidia-tesla-v100'
            """
            assert gpu_type
            assert gpu_count
            self.gpu_type = gpu_type
            self.gpu_count = gpu_count

        def accelerator(self, zone: str) -> compute_v1.AcceleratorConfig:
            return compute_v1.AcceleratorConfig(
                accelerator_count=self.gpu_count,
                accelerator_type=f'projects/{get_project_id()}/zones/{zone}/acceleratorTypes/{self.gpu_type}',
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
        network_uri: str = None,
        subnet_uri: str = None,
        startup_script: str | None = None,
        gpu: dict | None = None,
    ):
        """
        `name` is a "display name", but also plays the role of an ID because it must be unique for the project
        in the specified region.

        `name` must be 1-63 characters long and match the regular expression ``[a-z]([-a-z0-9]*[a-z0-9])?``
        which means the first character must be a lowercase letter, and all following characters must be a dash,
        lowercase letter, or digit, except the last character, which cannot be a dash.

        `zone` is like 'us-west1-a'.

        `machine_type`: cheap, low-end machines suitable for lightweights tests:

            't2a-standard-1' (1 CPU 4 GiB, $0.0385 / hour)
            't2d-standard-1' (1 CPU 4 GiB, $0.0422 / hour)
            'c4a-standard-1' (1 CPU 4 GiB, $0.0449 / hour)
            'n1-standard-1' (1 CPU 3.75 GiB, $0.0475 / hour)
            'e2-standard-2' (2 CPUs 8 GiB, $0.067 / hour)
            'n2d-standard-2' (2 CPUs 8 GiB, $0.084 / hour)
            'n4-standard-2' (2 CPUs 8 GiB, $0.0948 / hour)
            'n2-standard-2' (2 CPUs 8 GiB, $0.097 / hour)
            'e2-standard-4' (4 CPUs 16 GiB, $0.134 / hour)
            'e2-standard-8' (8 CPUs 32 GiB, $0.27 / hour)

        See https://cloud.google.com/compute/all-pricing?hl=en

        `network_uri` may look like "projects/shared-vpc-admin/global/networks/vpcnet-shared-prod-01".
        `subnet_uri` may look like "https://www.googleapis.com/compute/v1/projects/shared-vpc-admin/regions/<region>/subnetworks/prod-<region>-01".
        If `None`, the project's default network and subnet (for the specified region) will be used.
        See https://cloud.google.com/compute/docs/networking/network-overview

        `startup_script`: shell script that installs software and makes any other preps before the instance becomes operational.
        If provided, this must handle everything, as the script will not be augmented in this function.
        Common concerns include mounting local disks and installing cuda drivers (if you attach GPUs).

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

        boot_disk = self.BootDisk(**(boot_disk or {}))
        disks.append(boot_disk.disk())

        if local_ssd:
            local_ssd = self.LocalSSD(**local_ssd)
            disks.append(local_ssd.disk(zone))

        guest_accelerators = None
        scheduling = None
        if machine_type.split('-')[0] in ('a3', 'a2', 'g2'):
            # machine types that come with GPUs
            if gpu is not None:
                raise ValueError(
                    f'machine_type {machine_type} comes with GPUs; you should not specify `gpu` again'
                )
            scheduling = compute_v1.Scheduling(on_host_maintenance='TERMINATE')
            gpu = True
        elif gpu:
            gpu = self.GPU(**gpu)
            guest_accelerators = [gpu.accelerator(zone)]
            scheduling = compute_v1.Scheduling(on_host_maintenance='TERMINATE')
            # See https://cloud.google.com/compute/docs/instances/setting-vm-host-options
            gpu = True
        else:
            gpu = False

        network = compute_v1.NetworkInterface(
            network=network_uri, subnetwork=subnet_uri
        )

        metadata = None
        if not startup_script:
            scripts = []
            if local_ssd:
                scripts.append(local_ssd.startup_script)
            if boot_disk.startup_script:
                scripts.append(boot_disk.startup_script)
            else:
                if gpu:
                    scripts.append(cuda_installer)
                    # If boot_disk has script, it is installing cuda driver
                    # (as that is the only scenario where BootDisk has script),
                    # hence `cuda_installer` is not used.

                    # NOTE: `cuda_installer` must be the last component of the startup script,
                    # because `cuda_installer` will reboot the machine and continue afterwards.
            if scripts:
                startup_script = '\n'.join(['#!/bin/bash'] + scripts)
        if startup_script:
            metadata = compute_v1.Metadata(
                items=[compute_v1.Items(key='startup-script', value=startup_script)]
            )

        service_accounts = [
            compute_v1.ServiceAccount(
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
        self.startup_script = startup_script

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
    def create(cls, config: InstanceConfig | dict) -> Instance:
        if not isinstance(config, InstanceConfig):
            config = InstanceConfig(**config)
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
        """
        `name` is either the "Name" or "Instance Id" shown on GCP dashboard.
        """
        self.name = name
        self.zone = zone
        self.instance = None
        self._refresh()

    def __repr__(self):
        return f"{self.__class__.__name__}('{self.name}', '{self.zone}')"

    def __str__(self):
        return self.__repr__()

    def _refresh(self):
        req = compute_v1.GetInstanceRequest(
            instance=self.name, project=get_project_id(), zone=self.zone
        )
        self.instance = _call_client('get', req)
        # This could raise `google.api_core.exceptions.NotFound`
        return self

    @property
    def id(self) -> int:
        return self.instance.id

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
        return self._refresh().instance.last_start_timestamp

    @property
    def last_stop_timestamp(self) -> str:
        # Can be ''.
        return self._refresh().instance.last_stop_timestamp

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
        logger.info('deleting %s', self)
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
        return self._refresh().instance.status
