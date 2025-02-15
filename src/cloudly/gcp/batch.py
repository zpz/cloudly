"""
This module wraps a subset of the GCP Batch functionalities that are useful and adequate for typical applications.

User code runs within a Docker container. Running a standalone script is not supported by this wrapper module.
"""

from __future__ import annotations

__all__ = ['Job', 'JobConfig']

from typing import Literal

from google.cloud import batch_v1
from google.protobuf.duration_pb2 import Duration

from .auth import get_credentials, get_project_id
from .compute import (
    basic_resource_labels,
    validate_label_key,
    validate_label_value,
    validate_local_ssd_size_gb,
)

# Using GPUs
#
# See https://www.googlecloudcommunity.com/gc/Infrastructure-Compute-Storage/GCP-Batch-use-NVDIA-GPU-to-train-models-what-installation-are/m-p/784063
#
# One scenario that seemed to work:
#
#    allocation_policy
#        machine_type: 'n1-standard-16'
#    gpu: {'gpu_type': 'nvidia-tesla-4', 'gpu_count': 1}
#    task_group:
#        task_spec:
#            container:
#                image_uri: <ubuntu 20.04 image>
#                commands: 'nvidia-smi'
#                options: '--rm --init'
#
# Test reported success, so at least the command `nvidia-smi` was present in the container.
#
# Another scenario that seemed to work:
#
#    allocation_policy
#        machine_type: 'g2-standard-16'
#    task_group:
#        task_spec:
#            container:
#                image_uri: <ubuntu 20.04 image>
#                commands: 'nvidia-smi'
#                options: '--rm --init'
#
# The g2 machine comes with GPUs.
# Note the absence of `gpu: {...}` setting.
# Also note that `--runtime=nvidia` was not accepted, whereas `--gpus=all` was not necessary.
#
# Some accommodations for GPU have been made by this module if you do not specify them explicitly.
# The accommodations mainly concern `install_gpu_drivers` and `boot_disk`.
# See `JobConfig.allocation_policy` for details.


class TaskConfig:
    class Container:
        def __init__(
            self,
            *,
            image_uri: str,
            commands: str,
            options: str | None = None,
            local_ssd_disk: JobConfig.LocalSSD | None = None,
            local_ssd_mount_path: str = None,
            **kwargs,
        ):
            """
            Parameters
            ----------
            image_uri
                The full tag of the image.
            commands
                The commands to be run within the Docker container, such as 'python -m mypackage.mymodule --arg1 x --arg2 y'.
                This is a single string that is run as a shell script. Inside the container, the command that is executed is

                    /bin/bash -c "<commands>"

                This is the command you would type verbatim in the console inside the container.
            options
                The option string to be applied to `docker run`, such as '-e NAME=abc --network host'. As this example shows,
                environment variables that you want to be passed into the container are also handled by `options`.

                Usually `options` should contain '--rm --init --log-driver=gcplogs', among others.
            local_ssd_disk
                This is provided by `TaskConfig.__init__`; user should not provide this directly.
            """

            if local_ssd_disk is not None:
                volumes = [
                    f'{local_ssd_disk.mount_path}:{local_ssd_mount_path or local_ssd_disk.mount_path}:{local_ssd_disk.mode}'
                ]
            else:
                volumes = None

            self._container = batch_v1.Runnable.Container(
                image_uri=image_uri,
                commands=['-c', commands],
                options=options,
                entrypoint='/bin/bash',
                volumes=volumes,
                **kwargs,
            )

        @property
        def container(self) -> batch_v1.Runnable.Container:
            return self._container

    def __init__(
        self,
        *,
        container: dict,
        max_run_duration_seconds: int | None = None,
        max_retry_count: int = 0,
        ignore_exit_status: bool = False,
        always_run: bool = False,
        local_ssd_disk: JobConfig.LocalSSD | None = None,
        **kwargs,
    ):
        """
        Parameters
        ----------
        local_ssd_disk
            This is provided by `JobConfig.__init__`. User should not provide this directly.
        """
        container = self.Container(**container, local_ssd_disk=local_ssd_disk)
        runnable = batch_v1.Runnable(
            container=container.container,
            ignore_exit_status=ignore_exit_status,
            always_run=always_run,
        )

        if max_run_duration_seconds:
            max_run_duration = Duration(seconds=max_run_duration_seconds)
        else:
            max_run_duration = None

        self._task_spec = batch_v1.TaskSpec(
            runnables=[runnable],
            max_run_duration=max_run_duration,
            max_retry_count=max_retry_count,
            volumes=None if local_ssd_disk is None else [local_ssd_disk.volume],
            **kwargs,
        )

    @property
    def task_spec(self) -> batch_v1.TaskSpec:
        return self._task_spec


class JobConfig:
    class BootDisk:
        def __init__(
            self,
            *,
            size_gb: int,
            disk_type: Literal[
                'pd-balanced', 'pd-extreme', 'pd-ssd', 'pd-standard'
            ] = 'pd-balanced',
            image: str | None = None,
        ):
            """
            `image`: 'batch-debian' seems to be a good value for GPUs; otherwise `batch-cos` may
                also work well. Leave it at `None` until needed.
                See https://cloud.google.com/batch/docs/vm-os-environment-overview
            """
            assert size_gb >= 30
            self.size_gb = size_gb
            self.type_ = disk_type
            self.image = image

        @property
        def disk(self) -> batch_v1.AllocationPolicy.Disk:
            return batch_v1.AllocationPolicy.Disk(
                image=self.image,
                type_=self.type_,
                size_gb=self.size_gb,
            )

    class LocalSSD:
        """
        Local SSDs are attached to each worker node for use during the lifetime of the tasks.
        They are not "persistent" storage that lives beyond the batch job.
        """

        def __init__(
            self,
            *,
            size_gb: int,
            device_name: str = 'local-ssd',
            mount_path: str = '/mnt',
            mode: Literal['ro', 'rw'] = 'rw',
        ):
            """
            `size_gb` should be a multiple of 375. If not,
            the next greater multiple of 375 will be used.
            """
            self.size_gb = validate_local_ssd_size_gb(size_gb)
            self.device_name = device_name
            self.mount_path = mount_path
            self.mode = mode

        @property
        def disk(self) -> batch_v1.AllocationPolicy.AttachedDisk:
            return batch_v1.AllocationPolicy.AttachedDisk(
                new_disk=batch_v1.AllocationPolicy.Disk(
                    type_='local-ssd', size_gb=self.size_gb
                ),
                device_name=self.device_name,
            )

        @property
        def volume(self) -> batch_v1.Volume:
            opts = ['async', self.mode]
            if self.mode == 'ro':
                opts.append('noload')
            return batch_v1.Volume(
                device_name=self.device_name,
                mount_path=self.mount_path,
                mount_options=opts,
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

        @property
        def accelerator(self) -> batch_v1.AllocationPolicy.Accelerator:
            return batch_v1.AllocationPolicy.Accelerator(
                type_=self.gpu_type, count=self.gpu_count
            )

    @classmethod
    def task_group(
        cls,
        *,
        task_spec: dict,
        task_count: int = 1,
        task_count_per_node: int = 1,
        parallelism: int | None = None,
        permissive_ssh: bool = True,
        **kwargs,
    ) -> batch_v1.TaskGroup:
        """
        Parameters
        ----------
        task_count
            Number of tasks to be created.
        task_count_per_node
            Number of tasks that can be running on a work node at any time.
        parallelism
            Number of tasks that can be running across all nodes at any time.
        """
        task_spec = TaskConfig(**task_spec).task_spec
        return batch_v1.TaskGroup(
            task_spec=task_spec,
            task_count=task_count,
            parallelism=parallelism,
            task_count_per_node=task_count_per_node,
            permissive_ssh=permissive_ssh,
            **kwargs,
        )

    @classmethod
    def allocation_policy(
        cls,
        *,
        region: str,
        labels: dict,
        network_uri: str | None = None,
        subnet_uri: str | None = None,
        machine_type: str,
        no_external_ip_address: bool = False,
        provisioning_model: Literal['standard', 'spot', 'preemptible'] = 'standard',
        boot_disk: dict | None = None,
        gpu: GPU | None = None,
        local_ssd_disk: LocalSSD | None = None,
        install_gpu_drivers: bool | None = None,
        **kwargs,
    ) -> batch_v1.AllocationPolicy:
        """
        Parameters
        ----------
        region
            Like 'us-central1', 'us-west1', etc.
        network_uri, subnet_uri
            If missing and `no_external_ip_address` is `False`, the default for the project in the specified region will be used.

            If `no_external_ip_address` is `True`, then both must be provided.

            See https://cloud.google.com/compute/docs/networking/network-overview
            and `google.cloud.batch_v1.types.job.AllocationPolicy.NetworkInterface`.
        labels, gpu, local_ssd_disk
            These are provided by `JobConfig.__init__`. User should not provide them directly.
        """
        network = batch_v1.AllocationPolicy.NetworkInterface(
            network=network_uri,
            subnetwork=subnet_uri,
            no_external_ip_address=no_external_ip_address,
        )
        provisioning_model = getattr(
            batch_v1.AllocationPolicy.ProvisioningModel, provisioning_model.upper()
        )

        if gpu or machine_type.split('-')[0] in ('a2', 'a3', 'g2'):
            if boot_disk:
                if boot_disk.get('image', None) is None:
                    boot_disk['image'] = 'batch-debian'
                if boot_disk.get('size_gb', None) is None:
                    boot_disk['size_gb'] = 50
            else:
                boot_disk = {'size_gb': 50, 'image': 'batch-debian'}
            if install_gpu_drivers is None:
                install_gpu_drivers = True

        if boot_disk:
            boot_disk = cls.BootDisk(**boot_disk)

        instance_policy = batch_v1.AllocationPolicy.InstancePolicy(
            machine_type=machine_type,
            accelerators=[gpu.accelerator] if gpu else None,
            provisioning_model=provisioning_model,
            boot_disk=None if boot_disk is None else boot_disk.disk,
            disks=None if local_ssd_disk is None else [local_ssd_disk.disk],
        )
        instance_policy_template = batch_v1.AllocationPolicy.InstancePolicyOrTemplate(
            policy=instance_policy,
            install_gpu_drivers=install_gpu_drivers,
        )

        return batch_v1.AllocationPolicy(
            location=batch_v1.AllocationPolicy.LocationPolicy(
                allowed_locations=[f'regions/{region}'],
            ),
            instances=[instance_policy_template],
            labels=labels,
            network=batch_v1.AllocationPolicy.NetworkPolicy(
                network_interfaces=[network],
            ),
            service_account=batch_v1.ServiceAccount(
                # email=get_service_account_email(),
            ),
            **kwargs,
        )

    @classmethod
    def labels(cls, **kwargs) -> dict[str, str]:
        # There are some restrictions to the label values.
        # See https://cloud.google.com/batch/docs/organize-resources-using-labels
        zz = {**basic_resource_labels(), **kwargs}
        zz = {
            validate_label_key(k): validate_label_value(v, fix=True)
            for k, v in zz.items()
        }
        return zz

    def __init__(
        self,
        *,
        task_group: dict,
        allocation_policy: dict,
        labels: dict[str, str] | None = None,
        logs_policy: batch_v1.LogsPolicy | None = None,
        gpu: dict | None = None,
        local_ssd: dict | None = None,
        **kwargs,
    ):
        if gpu:
            gpu = self.GPU(**gpu)
            # assert 'gpu' not in task_group['task_spec']['container']
            # task_group['task_spec']['container']['gpu'] = gpu

        if local_ssd:
            local_ssd_disk = self.LocalSSD(**local_ssd)
            assert 'local_ssd_disk' not in task_group['task_spec']
            task_group['task_spec']['local_ssd_disk'] = local_ssd_disk
        else:
            local_ssd_disk = None

        labels = self.labels(**(labels or {}))

        task_group = self.task_group(**task_group)
        allocation_policy = self.allocation_policy(
            gpu=gpu,
            local_ssd_disk=local_ssd_disk,
            labels=labels,
            **allocation_policy,
        )
        if logs_policy is None:
            logs_policy = batch_v1.LogsPolicy(
                destination=batch_v1.LogsPolicy.Destination.CLOUD_LOGGING
            )

        self._job = batch_v1.Job(
            task_groups=[task_group],
            allocation_policy=allocation_policy,
            logs_policy=logs_policy,
            labels=labels,
            **kwargs,
        )

    @property
    def job(self) -> batch_v1.Job:
        return self._job

    @property
    def definition(self) -> dict:
        # `self._job.__str__` actually is formatted nicely,
        # but we choose to return the built-in dict type.
        # If you want a nice printout, just print `self.job`.
        return type(self._job).to_dict(self._job)

    @property
    def region(self) -> str:
        return self._job.allocation_policy.location.allowed_locations[0].split('/')[1]


def _call_client(method: str, *args, **kwargs):
    with batch_v1.BatchServiceClient(credentials=get_credentials()) as client:
        return getattr(client, method)(*args, **kwargs)


class Job:
    @classmethod
    def create(cls, name: str, config: JobConfig | dict) -> Job:
        """
        There are some restrictions on the form of `name`; see GCP doc for details or `cloudly.gcp.compute`.
        In addition, the batch name must be unique in the project and the region.
        """
        if not isinstance(config, JobConfig):
            config = JobConfig(**config)
        validate_label_key(name)
        req = batch_v1.CreateJobRequest(
            parent=f'projects/{get_project_id()}/locations/{config.region}',
            job_id=name,
            job=config.job,
        )
        job = _call_client('create_job', req)
        return cls(job)

    @classmethod
    def list(cls, *, region: str) -> list[Job]:
        req = batch_v1.ListJobsRequest(
            parent=f'projects/{get_project_id()}/locations/{region}'
        )
        jobs = _call_client('list_jobs', req)
        return [cls(j) for j in jobs]

    def __init__(self, name_or_obj: str | batch_v1.Job, /):
        """
        `name` is like 'projects/<project-id>/locations/<location>/jobs/<name>'.
        This can also be considered the job "ID" or "URI".
        This is available from the object returned by :meth:`create`.

        `job` is not necessary because it can be created if needed.
        It is accepted in case you already have it. See :meth:`create`, :meth:`list`.
        """
        if isinstance(name_or_obj, str):
            self.name = name_or_obj
            self.job = None
            self._refresh()
        else:
            self.name = name_or_obj.name
            self.job = name_or_obj

    def __repr__(self):
        return f"{self.__class__.__name__}('{self.name}')"

    def __str__(self):
        return self.__repr__()

    def _refresh(self):
        req = batch_v1.GetJobRequest(name=self.name)
        self.job = _call_client('get_job', req)
        return self

    @property
    def create_time(self):
        return self.job.create_time

    @property
    def update_time(self):
        return self._refresh().job.update_time

    @property
    def definition(self) -> dict:
        return type(self.job).to_dict(self.job)

    def status(self) -> batch_v1.JobStatus:
        """
        The returned `JobStatus` object has some useful attributes you can access;
        see :meth:`state` for an example.
        """
        return self._refresh().job.status

    def state(
        self,
    ) -> Literal[
        'STATE_UNSPECIFIED',
        'QUEUED',
        'SCHEDULED',
        'RUNNING',
        'SUCCEEDED',
        'FAILED',
        'DELETION_IN_PROGRESS',
    ]:
        return self.status().state.name

    def delete(self) -> None:
        req = batch_v1.DeleteJobRequest(name=self.name)
        _call_client('delete_job', req)
        self.job = None
