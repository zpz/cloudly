__all__ = ['Job']

import json
from typing import Literal

from google.cloud import scheduler_v1

from .auth import get_credentials, get_project_id, get_service_account_email
from .compute import validate_label_key
from .workflows import Workflow


def _call_client(method: str, *args, **kwargs):
    with scheduler_v1.CloudSchedulerClient(credentials=get_credentials()) as client:
        return getattr(client, method)(*args, **kwargs)


class Job:
    @classmethod
    def create(
        cls,
        name: str,
        *,
        cron_schedule: str,
        workflow: Workflow,
        workflow_args: dict | None = None,
        timezone: str,
    ):
        """
        Parameters
        ----------
        name
            You often want to add some randomness to the name to guarantee its uniqueness.
        cron_schedule
            See https://cloud.google.com/scheduler/docs/configuring/cron-job-schedules
        """
        validate_label_key(name)
        parent = f'projects/{get_project_id()}/locations/{workflow.region}'
        if workflow_args:
            body = json.dumps(workflow_args).encode('utf-8')
        else:
            body = None
        job = scheduler_v1.Job(
            name=f'{parent}/jobs/{name}',
            schedule=cron_schedule,
            time_zone=timezone,
            http_target=scheduler_v1.HttpTarget(
                uri=f'https://workflowexecutions.googleapis.com/v1/{workflow.name}/executions',
                http_method=scheduler_v1.HttpMethod(scheduler_v1.HttpMethod.POST),
                oauth_token=scheduler_v1.OAuthToken(
                    service_account_email=get_service_account_email()
                ),
                body=body,
            ),
        )
        req = scheduler_v1.CreateJobRequest(parent=parent, job=job)
        resp = _call_client('create_job', req)
        return cls(resp)

    def __init__(self, name: str | scheduler_v1.Job, /):
        if isinstance(name, str):
            self._name = name
            self._job = None
        else:
            self._name = name.name
            self._job = name

    def __repr__(self):
        return f"{self.__class__.__name__}('{self.name}')"

    def __str__(self):
        return self.__repr__()

    @property
    def name(self) -> str:
        return self._name

    def _refresh(self):
        req = scheduler_v1.GetJobRequest(name=self._name)
        self._job = _call_client('get_job', req)

    def delete(self):
        req = scheduler_v1.DeleteJobRequest(name=self._name)
        _call_client('delete_job', req)

    def state(
        self,
    ) -> Literal['ACTIVE', 'ENABLED', 'PAUSED', 'DISABLED', 'STATE_UNSPECIFIED']:
        self._refresh()
        return self._job.state.name
