"""
This module wraps a subset of the GCP Batch functionalities that are useful and adequate for typical applications.

User code runs within a Docker container. Running a standalone script is not supported by this wrapper module.
"""

from __future__ import annotations

__all__ = ['Job', 'JobConfig']

import warnings
from typing import Literal

from google.cloud import batch_v1
from google.protobuf.duration_pb2 import Duration

from .auth import get_credentials, get_project_id, get_service_account_email
from .compute import basic_resource_labels, validate_label_key, validate_label_value


class TaskConfig:
    class Container:
        def __init__(
            self,
            *,
            image_uri: str,
            commands: str,
            options: str | None = None,
            local_ssd_disk: JobConfig.LocalSSD | None = None,
            disk_mount_path: str = None,
            gpu: JobConfig.GPU | None = None,
            **kwargs,
        ):
            """
            Parameters
            ----------
            image_uri
                The full tag of the image.
            commands
                The commands to be run within the Docker container, such as 'python -m mypackage.mymodule --arg1 x --arg2 y'.

                This is the command you would run if you are within the container.
            options
                The option string to be applied to `docker run`, such as '-e NAME=abc --network host'. As this example shows,
                environment variables that you want to be passed into the container are also handled by `options`.
            """
            if not commands.startswith('-c '):
                commands = '-c ' + commands

            if options:
                options = ' ' + options.strip() + ' '  # to help search in it
            else:
                options = ''
            if ' --rm ' not in options:
                options += ' --rm '
            if ' --init ' not in options:
                options += '--init '

            if gpu:
                if ' --runtime=nvidia ' not in options:
                    options = options + '--runtime=nvidia '

            if ' --log-driver ' not in options and ' --log-driver=' not in options:
                options += '--log-driver=gcplogs '
            # TODO: is this necessary? is this enough by itself?

            if local_ssd_disk is not None:
                volumes = [
                    f'{local_ssd_disk.mount_path}:{disk_mount_path or local_ssd_disk.mount_path}:{local_ssd_disk.access_mode}'
                ]
            else:
                volumes = None

            options = options.strip()
            if not options:
                options = None
            self._container = batch_v1.Runnable.Container(
                image_uri=image_uri,
                commands=[commands],
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
        max_retry_count: int | None = None,
        ignore_exit_status: bool | None = None,
        always_run: bool | None = None,
        local_ssd_disk: JobConfig.LocalSSD | None = None,
        **kwargs,
    ):
        if ignore_exit_status is None:
            ignore_exit_status = False
        if always_run is None:
            always_run = False
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
            max_retry_count=max_retry_count or 0,
            volumes=None if local_ssd_disk is None else [local_ssd_disk.volume],
            **kwargs,
        )

    @property
    def task_spec(self) -> batch_v1.TaskSpec:
        return self._task_spec


class JobConfig:
    class BootDisk:
        # See
        #   https://cloud.google.com/batch/docs/vm-os-environment-overview
        def __init__(
            self,
            *,
            size_gb: int,
            disk_type: Literal['pd-balanced', 'pd-extreme', 'pd-ssd', 'pd-standard']
            | None = None,
            image: str | None = None,
        ):
            assert size_gb >= 30
            self.size_gb = size_gb
            self.type_ = disk_type or 'pd-balanced'
            self.image = image or 'batch-cos'

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
            device_name: str | None = None,
            mount_path: str | None = None,
            access_mode: Literal['ro', 'rw'] | None = None,
        ):
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
            self.device_name = device_name or 'local-ssd'
            self.mount_path = mount_path or '/mnt'
            self.access_mode = access_mode or 'rw'

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
            opts = ['async', self.access_mode]
            if self.access_mode == 'ro':
                opts.append('noload')
            return batch_v1.Volume(
                device_name=self.device_name,
                mount_path=self.mount_path,
                mount_options=opts,
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
        task_count: int | None = None,
        task_count_per_node: int | None = None,
        parallelism: int | None = None,
        permissive_ssh: bool | None = None,
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
        if permissive_ssh is None:
            permissive_ssh = True
        task_spec = TaskConfig(**task_spec).task_spec
        return batch_v1.TaskGroup(
            task_spec=task_spec,
            task_count=task_count or 1,
            parallelism=parallelism,
            task_count_per_node=task_count_per_node or 1,
            permissive_ssh=permissive_ssh,
            **kwargs,
        )

    @classmethod
    def allocation_policy(
        cls,
        *,
        region: str,
        labels: dict,
        network_uri: str,
        subnet_uri: str,
        machine_type: str,
        no_external_ip_address: bool | None = None,
        provisioning_model: Literal['standard', 'spot', 'preemptible'] | None = None,
        boot_disk: BootDisk | None = None,
        gpu: GPU | None = None,
        local_ssd_disk: LocalSSD | None = None,
        **kwargs,
    ) -> batch_v1.AllocationPolicy:
        """
        Parameters
        ----------
        region
            Like 'us-central1', 'us-west1', etc.
        network_uri
            Could be like this: 'projects/shared-vpc-admin/global/networks/vpcnet-shared-prod-01'.
        subnet_uri
            Could be like this: 'https://www.googleapis.com/compute/v1/projects/shared-vpc-admin/regions/<region>/subnetworks/prod-<region>-01'
        """
        if no_external_ip_address is None:
            no_external_ip_address = True
        if provisioning_model is None:
            provisioning_model = 'standard'

        network = batch_v1.AllocationPolicy.NetworkInterface(
            network=network_uri,
            subnetwork=subnet_uri,
            no_external_ip_address=no_external_ip_address,
        )
        provisioning_model = getattr(
            batch_v1.AllocationPolicy.ProvisioningModel, provisioning_model.upper()
        )

        instance_policy = batch_v1.AllocationPolicy.InstancePolicy(
            machine_type=machine_type,
            accelerators=[gpu.accelerator] if gpu else None,
            provisioning_model=provisioning_model,
            boot_disk=None if boot_disk is None else boot_disk.disk,
            disks=None if local_ssd_disk is None else [local_ssd_disk.disk],
        )
        instance_policy_template = batch_v1.AllocationPolicy.InstancePolicyOrTemplate(
            policy=instance_policy,
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
                email=get_service_account_email(),
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
        boot_disk: dict | None = None,
        local_ssd: dict | None = None,
        gpu: dict | None = None,
        **kwargs,
    ):
        if gpu:
            gpu = self.GPU(**gpu)
            assert 'gpu' not in task_group['task_spec']['container']
            task_group['task_spec']['container']['gpu'] = gpu

        if boot_disk:
            boot_disk = self.BootDisk(**boot_disk)

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
            boot_disk=boot_disk,
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
    def region(self) -> str:
        return self._job.allocation_policy.location.allowed_locations[0].split('/')[1]


class Job:
    @classmethod
    def _client(self):
        return batch_v1.BatchServiceClient(credentials=get_credentials())
        # TODO: this client object has a context manager; check it out.

    # def define(cls, **kwargs) -> JobConfig:
    #     return JobConfig(**kwargs)

    @classmethod
    def create(cls, *, name: str, config: JobConfig) -> Job:
        """
        There are some restrictions on the form of `name`; see GCP doc for details.
        In addition, the batch name must be unique. For this reason, user may want to construct the name
        with some randomness.
        """
        validate_label_key(name)
        req = batch_v1.CreateJobRequest(
            parent=f'projects/{get_project_id()}/locations/{config.region}',
            job_id=name,
            job=config.job,
        )
        job = cls._client().create_job(req)
        return cls(job)

    @classmethod
    def list(cls, *, region: str) -> list[Job]:
        req = batch_v1.ListJobsRequest(
            parent=f'projects/{get_project_id()}/locations/{region}'
        )
        jobs = cls._client().list_jobs(request=req)
        return [cls(j) for j in jobs]

    def __init__(self, name: str | batch_v1.Job, /):
        """
        `name` is like 'projects/<project-id>/locations/<location>/jobs/<name>'.
        This can also be considered the job "ID" or "URI".
        This is available from the object returned by :meth:`create`.

        `job` is not needed because it can be created if needed.
        It is accepted in case you already have it. See :meth:`create`, :meth:`list`.
        """
        if isinstance(name, str):
            self._name = name
            self._job = None
        else:
            self._name = name.name
            self._job = name

    @property
    def name(self) -> str:
        return self._name

    def _refresh(self):
        req = batch_v1.GetJobRequest(name=self.name)
        self._job = self._client().get_job(request=req)

    def status(self) -> batch_v1.JobStatus:
        """
        The returned `JobStatus` object has some useful attributes you can access;
        see :meth:`state` for an example.
        """
        self._refresh()
        return self._job.status

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
        self._client().delete_job(req)
