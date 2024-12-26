from __future__ import annotations

from typing import Literal

from google.cloud import batch_v1

from .auth import get_credentials, get_project_id, get_service_account_email


class JobConfig:
    @classmethod
    def task_groups(cls):
        pass

    @classmethod
    def allocation_policy(
        cls,
        *,
        region: str,
        labels: dict,
        network_uri: str,
        subnet_uri: str,
        no_external_ip_address: bool = True,
        provisioning_model: Literal['standard', 'spot', 'preemptible'] = 'standard',
    ):
        """
        `network_uri` could be like this: 'projects/shared-vpc-admin/global/networks/vpcnet...'.
        `subnet_uri` could be like this: 'https://www.googleapis.com/compute/v1/projects/shared-vpc-admin/regions/<region>/subnetworks/prod-<region>-01'
        """
        network = batch_v1.AllocationPolicy.NetworkInterface(
            network=network_uri,
            subnetwork=subnet_uri,
            no_external_ip_address=no_external_ip_address,
        )
        provision = getattr(
            batch_v1.AllocationPolicy.ProvisioningModel, provisioning_model.upper()
        )
        instance_policy = batch_v1.AllocationPolicy.InstancePolicy(
            provisioning_model=provision,
        )

        return batch_v1.AllocationPolicy(
            location=batch_v1.AllocationPolicy.LocationPolicy(
                allowed_locations=[f'regions/{region}'],
            ),
            instances=[instance_policy],
            labels=labels,
            network=batch_v1.AllocationPolicy.NetworkPolicy(
                network_interfaces=[network],
            ),
            service_account=batch_v1.ServiceAccount(
                email=get_service_account_email(),
            ),
        )

    @classmethod
    def logs_policy(cls):
        pass

    @classmethod
    def labels(cls):
        pass

    def __init__(self, **kwargs):
        self._job = batch_v1.Job(
            task_groups=self.task_groups(),
            allocation_policy=self.allocation_policy(),
            logs_policy=self.logs_policy(),
            labels=self.labels(),
        )

    @property
    def job(self) -> batch_v1.Job:
        return self._job


class Job:
    @classmethod
    def client(self):
        return batch_v1.BatchServiceClient(credentials=get_credentials())

    @classmethod
    def create(cls, *, region: str, name: str, **kwargs) -> Job:
        """
        There are some restrictions to the form of `name`; see GCP doc for details.

        `region` is like 'us-central1'.
        """
        req = batch_v1.CreateJobRequest(
            parent=f'projects/{get_project_id()}/locations/{region}',
            job_id=name,
            job=JobConfig(**kwargs).job,
        )
        job = cls.client().create_job(req)
        obj = cls(job.name)
        obj._job = job
        return obj

    @classmethod
    def list(cls, *, region: str) -> list[Job]:
        req = batch_v1.ListJobsRequest(
            parent=f'projects/{get_project_id()}/locations/{region}'
        )
        jobs = cls.client().list_jobs(request=req)
        return [cls(j.name) for j in jobs]

    def __init__(self, name: str):
        """
        `name` is like 'projects/<project-id>/locations/<location>/jobs/<name>'.
        This can also be considered the job "ID" or "URI".
        """
        self._name = name
        self._job: batch_v1.Job = None

    @property
    def name(self):
        return self._name

    def _refresh(self):
        req = batch_v1.GetJobRequest(name=self.name)
        self._job = self.client().get_job(request=req)

    def status(self) -> batch_v1.JobStatus:
        """
        The returned `JobStatus` object has some useful attributes you can check out.
        """
        self._refresh()
        return self._job.status

    def state(self) -> str:
        """
        Return string values like 'DONE', 'ERROR', 'CANCELLED', 'CANCEL_STARTED', 'SUCCEEDED', 'FAILED', 'DELETION_IN_PROGRESS'.
        """
        return self.status().state.name

    def delete(self):
        req = batch_v1.DeleteJobRequest(name=self.name)
        self.client().delete_job(req)
