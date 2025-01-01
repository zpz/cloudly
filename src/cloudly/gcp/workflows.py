from __future__ import annotations

__all__ = ['Workflow', 'Execution', 'Step', 'BatchStep']

import atexit
import datetime
import json
from collections.abc import Sequence
from typing import Literal

from google.cloud import workflows_v1
from google.cloud.workflows import executions_v1

from .auth import get_credentials, get_project_id
from .batch import JobConfig as BatchJobConfig

_workflow_client_ = None


def _cleanup():
    global _workflow_client_
    if _workflow_client_ is not None:
        _workflow_client_.__exit__(None, None, None)
        _workflow_client_ = None


atexit.register(_cleanup)


def _call_workflow_client(meth: str, *args, **kwargs):
    global _workflow_client_
    if _workflow_client_ is None:
        _workflow_client_ = workflows_v1.WorkflowsClient(
            credentials=get_credentials()
        ).__enter__()
    return getattr(_workflow_client_, meth)(*args, **kwargs)
    # Can not use context manager in each call, like with "execution client";
    # would be `ValueError: Cannot invoke RPC on closed channel!`.
    # Don't know why; probably due to interaction with "execution client".


def _call_execution_client(meth: str, *args, **kwargs):
    with executions_v1.ExecutionsClient(credentials=get_credentials()) as client:
        return getattr(client, meth)(*args, **kwargs)


class Step:
    def __init__(self, name: str, content: dict):
        """
        `content` is a dict of the step's action, e.g.,

            {
              "call": "http.get",
              "args": {
                "url": "https://host.com/api1",
              },
              "result": "api_response1",
            }

        In "nested steps", `content` can be like this::

            {
              "steps": [
                {
                  "step_1": {
                    "call": "http.get",
                    "args": {
                      "url": "https://host.com/api1",
                    },
                    "result": "api_response1",
                  },
                },
                {
                  "step_2": {
                    "assign": [
                      {"varA": "Monday"},
                      {"varB": "Tuesday"},
                    ]
                  }
                },
              ]
            }

        where each element of the list can be the output of `Step.render` of some step.
        """
        self.name = name
        self._content = content

    @property
    def definition(self) -> dict:
        return {self.name: self._content}


class ParallelStep(Step):
    pass


class BatchStep(Step):
    """
    See
      https://atamel.dev/posts/2023/05-30_workflows_batch_connector/
      https://cloud.google.com/workflows/docs/reference/googleapis/batch/Overview
    """

    def __init__(
        self,
        name: str,
        config: BatchJobConfig,
        *,
        keep_batch_job: bool = False,
    ):
        job_config = json.loads(type(config.job).to_json(config.job))
        job_id = name.replace('_', '-')
        parent = f'projects/{get_project_id()}/locations/{config.region}'
        result_name = name.replace('-', '_') + '_result'
        create_job = {
            'call': 'googleapis.batch.v1.projects.locations.jobs.create',
            'args': {
                'parent': parent,
                'jobId': job_id,
                'body': job_config,
            },
            'result': result_name,
        }
        if keep_batch_job:
            delete_job = {
                'call': 'googleapis.batch.v1.projects.locations.jobs.delete',
                'args': {
                    'name': f'{parent}/jobs/{job_id}',
                },
            }
            content = {
                'steps': [
                    {'create_job': create_job},
                    {'delete_job': delete_job},
                ]
            }
        else:
            content = create_job

        # `job_id`` requirement: ^[a-z]([a-z0-9-]{0,61}[a-z0-9])?$  Note in particular: doesn't allow underscore.
        # `result` name must be a valid variable (or identifier) name, e.g. it can't contain dash.
        # Experiments suggested that `job_id` and `result` name do not have fixed relation with the step `name`;
        # I made changes to both and it still worked.
        super().__init__(name, content)
        self.job_url = f'https://batch.googleapis.com/v1/{parent}/jobs/{job_id}'
        self.job_id = job_id
        self.result_name = result_name


class Execution:
    def __init__(self, name: str | executions_v1.Execution):
        if isinstance(name, str):
            self._name = name
            self._execution = None
        else:
            self._name = name.name
            self._execution = name

    def __repr__(self):
        return f"{self.__class__.__name__}('{self.name}')"

    def __str__(self):
        return self.__repr__()

    @property
    def name(self):
        return self._name

    def _refresh(self):
        req = executions_v1.GetExecutionRequest(name=self.name)
        self._execution = _call_execution_client('get_execution', req)

    @property
    def start_time(self) -> datetime.datetime:
        if self._execution is None:
            self._refresh()
        return self._execution.start_time

    @property
    def end_time(self) -> datetime.datetime:
        # If not finished (`state()` returns "ACTIVE"), this returns `None`.
        self._refresh()
        return self._execution.end_time

    @property
    def argument(self):
        if self._execution is None:
            self._refresh()
        return self._execution.argument

    def result(self):
        self._refresh()
        return str(self._execution.result)

    def status(self):
        # If still running, this returns which step is currently running.
        self._refresh()
        return self._execution.status

    def state(
        self,
    ) -> Literal[
        'STATE_UNSPECIFIED',
        'ACTIVE',
        'SUCCEEDED',
        'FAILED',
        'CANCELLED',
        'UNAVAILABLE',
        'QUEUED',
    ]:
        self._refresh()
        return self._execution.state.name

    def cancel(self):
        req = executions_v1.CancelExecutionRequest(name=self.name)
        _call_execution_client('cancel_execution', req)


class WorkflowConfig:
    def __init__(self, steps: Sequence[Step]):
        """
        If your workflow requires command-line arguments, you should access individual arguments
        using `dot`, for example, "args.name", "args.age".
        Correspondingly in :meth:`execute`, you need to pass a dict to `args`,
        e.g. `{'name': 'Tom', 'age': 38}`.
        """
        self._content = {
            'params': ['args'],
            'steps': [s.definition for s in steps],
        }

    @property
    def definition(self) -> dict:
        return {'main': self._content}


class Workflow:
    @classmethod
    def create(
        cls,
        *,
        name: str,
        config: WorkflowConfig,
        region: str,
    ) -> Workflow:
        """
        `name` needs to be unique, hence it's recommended to construct it with some randomness.

        If you create a workflow for a, say, batch job, then you probably should get `region`
        from the batch job definition. I don't know whether it's allowed for a workflow to
        contain jobs spanning regions.
        """
        workflow = workflows_v1.Workflow(source_contents=json.dumps(config.definition))
        req = workflows_v1.CreateWorkflowRequest(
            parent=f'projects/{get_project_id()}/locations/{region}',
            workflow=workflow,
            workflow_id=name,
        )
        op = _call_workflow_client('create_workflow', req)
        resp = op.result()
        return cls(resp)

    @classmethod
    def list(cls, region: str) -> list[Workflow]:
        req = workflows_v1.ListWorkflowsRequest(
            parent=f'projects/{get_project_id()}/locations/{region}'
        )
        resp = _call_workflow_client('list_workflows', req)
        return [cls(r) for r in resp]

    def __init__(self, name: str | workflows_v1.Workflow, /):
        """
        `name` is like "projects/<project_id>/locations/<region>/workflows/<name>".
        """
        if isinstance(name, str):
            self._name = name
            self._workflow = None
        else:
            self._name = name.name
            self._workflow = name

    def __repr__(self):
        return f"{self.__class__.__name__}('{self.name}')"

    def __str__(self):
        return self.__repr__()

    @property
    def name(self) -> str:
        return self._name

    @property
    def region(self) -> str:
        return self.name.split('locations/')[1].split('/')[0]

    @property
    def definition(self) -> dict:
        if self._workflow is None:
            self._refresh()
        return json.loads(self._workflow.source_contents)

    @property
    def create_time(self) -> datetime.datetime:
        if self._workflow is None:
            self._refresh()
        return self._workflow.create_time

    @property
    def update_time(self) -> datetime.datetime:
        self._refresh()
        return self._workflow.update_time

    @property
    def revision_id(self):
        self._refresh()
        return self._workflow.revision_id

    def _refresh(self):
        req = workflows_v1.GetWorkflowRequest(name=self._name)
        self._workflow = _call_workflow_client('get_workflow', req)

    def state(self) -> Literal['STATE_UNSPECIFIED', 'ACTIVE', 'UNAVAILABLE']:
        self._refresh()
        return self._workflow.state.name

    def update(self, config: WorkflowConfig):
        self._refresh()
        self._workflow.source_contents = json.dumps(config.definition)
        req = workflows_v1.UpdateWorkflowRequest(workflow=self._workflow)
        op = _call_workflow_client('update_workflow', req)
        self._workflow = op.result()

    def execute(self, args: dict | None = None) -> Execution:
        if args:
            exe = executions_v1.Execution(argument=json.dumps(args))
        else:
            exe = executions_v1.Execution()
        req = executions_v1.CreateExecutionRequest(parent=self._name, execution=exe)
        resp = _call_execution_client('create_execution', req)
        return Execution(resp)

    def delete(self) -> None:
        req = workflows_v1.DeleteWorkflowRequest(name=self._name)
        _call_workflow_client('delete_workflow', req)

    def list_executions(self) -> list[Execution]:
        req = executions_v1.ListExecutionsRequest(parent=self.name)
        resp = _call_execution_client('list_executions', req)
        return [Execution(r) for r in resp]
