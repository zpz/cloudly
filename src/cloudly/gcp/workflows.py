from __future__ import annotations

__all__ = ['Workflow', 'WorkflowConfig', 'Execution', 'Step', 'BatchStep']

import atexit
import datetime
import json
import string
from collections.abc import Sequence
from typing import Literal

from google.cloud import workflows_v1
from google.cloud.workflows import executions_v1

from .auth import get_credentials, get_project_id
from .batch import JobConfig as BatchJobConfig
from .compute import validate_label_key

_workflow_client_ = None


def _cleanup():
    global _workflow_client_
    if _workflow_client_ is not None:
        _workflow_client_.__exit__(None, None, None)
        _workflow_client_ = None


atexit.register(_cleanup)


def validate_identifier_name(val: str) -> str:
    # TODO: use `re`.
    if val[0] not in string.ascii_lowercase:
        raise ValueError(val)
    for v in val[1:]:
        if v not in string.ascii_lowercase and v not in string.digits and v != '_':
            raise ValueError(val)
    return val


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

        With "nested steps", `content` will be like this::

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

        where each element of the list can be the output of `Step.definition` of some step.
        """
        validate_label_key(name)
        self.name = name
        self._content = content

    @property
    def definition(self) -> dict:
        return {self.name: self._content}


class BatchStep(Step):
    """
    Running a Batch job using Workflows.

    See
      https://atamel.dev/posts/2023/05-30_workflows_batch_connector/
      https://cloud.google.com/workflows/docs/reference/googleapis/batch/Overview
      https://cloud.google.com/workflows/docs/sleeping
    """

    def __init__(
        self,
        name: str,
        config: BatchJobConfig,
        *,
        delete_batch_job: bool = True,
    ):
        validate_label_key(name)
        job_config = json.loads(type(config.job).to_json(config.job))
        job_id = name.replace('_', '-')
        parent = f'projects/{get_project_id()}/locations/{config.region}'
        result_name = name.replace('-', '_') + '_result'
        validate_identifier_name(result_name)
        steps = []
        steps.append(
            {
                'log_create': {
                    'call': 'sys.log',
                    'args': {'data': f'creating and running the batch job {job_id}'},
                }
            }
        )
        steps.append(
            {
                'create_job': {
                    'call': 'googleapis.batch.v1.projects.locations.jobs.create',
                    'args': {
                        'parent': parent,
                        'jobId': job_id,
                        'body': job_config,
                    },
                    'result': result_name,
                    # This "result" seems to be the entire batch-job config, and not the "result" of the job's run.
                }
            }
        )  # This uses Workflow's batch "connector" to create and run the batch job, waiting for its completion.
        steps.append(
            {
                'log_create_result': {
                    'call': 'sys.log',
                    'args': {
                        'data': f'${{"result of batch job {job_id}: \n" + str({result_name})}}'
                    },
                }
            }
        )
        if delete_batch_job:
            # TODO: how to delete only on batch success?
            steps.append(
                {
                    'log_delete': {
                        'call': 'sys.log',
                        'args': {'data': f'deleting the batch job {job_id}'},
                    }
                }
            )
            steps.append(
                {
                    'delete_job': {
                        'call': 'googleapis.batch.v1.projects.locations.jobs.delete',
                        'args': {
                            'name': f'{parent}/jobs/{job_id}',
                        },
                    }
                }
            )

        content = {'steps': steps}

        # `job_id`` requirement: ^[a-z]([a-z0-9-]{0,61}[a-z0-9])?$  Note in particular: doesn't allow underscore.
        # `result` name must be a valid variable (or identifier) name, e.g. it can't contain dash.
        # Experiments suggested that `job_id` and `result` name do not have fixed relation with the step `name`;
        # I made changes to both and it still worked.
        super().__init__(name, content)
        self.job_url = f'https://batch.googleapis.com/v1/{parent}/jobs/{job_id}'
        self.job_id = job_id  # If you keep the batch job, then this might be the ID to use for tracking.
        self.result_name = result_name
        self.region = config.region


class WorkflowConfig:
    def __init__(self, steps: Sequence[Step]):
        """
        If your workflow requires command-line arguments, you should access individual arguments
        using `dot` notation, for example, "args.name", "args.age".
        Correspondingly in :meth:`Workflow.execute`, you need to pass a dict to `args`,
        e.g. `{'name': 'Tom', 'age': 38}`.

        The 'params' is provided whether your job needs it. If not needed,
        don't provide args in :meth:`Workflow.execute` and use use it in the workflow "steps".
        """
        self._content = {
            'params': ['args'],
            'steps': [s.definition for s in steps],
        }
        self._workflow = workflows_v1.Workflow(
            source_contents=json.dumps(self.definition)
        )

    @property
    def definition(self) -> dict:
        return {'main': self._content}

    @property
    def workflow(self) -> workflows_v1.Workflow:
        return self._workflow


class Workflow:
    @classmethod
    def create(
        cls,
        name: str,
        config: WorkflowConfig,
        *,
        region: str,
    ) -> Workflow:
        """
        `name` needs to be unique, hence it's recommended to construct it with some randomness.

        If you create a workflow for a, say, batch job, then you probably should get `region`
        from the batch job definition. I don't know whether it's allowed for a workflow to
        contain jobs spanning regions.
        """
        validate_label_key(name)
        req = workflows_v1.CreateWorkflowRequest(
            parent=f'projects/{get_project_id()}/locations/{region}',
            workflow=config.workflow,
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

    def __init__(self, name_or_obj: str | workflows_v1.Workflow, /):
        """
        `name` is like "projects/<project_id>/locations/<region>/workflows/<name>".
        """
        if isinstance(name_or_obj, str):
            self.name = name_or_obj
            self.workflow = None
            self._refresh()
        else:
            self.name = name_or_obj.name
            self.workflow = name_or_obj

    def __repr__(self):
        return f"{self.__class__.__name__}('{self.name}')"

    def __str__(self):
        return self.__repr__()

    def _refresh(self):
        req = workflows_v1.GetWorkflowRequest(name=self.name)
        self.workflow = _call_workflow_client('get_workflow', req)
        return self

    @property
    def region(self) -> str:
        return self.name.split('locations/')[1].split('/')[0]

    @property
    def definition(self) -> dict:
        return json.loads(self.workflow.source_contents)

    @property
    def create_time(self) -> datetime.datetime:
        return self.workflow.create_time

    @property
    def update_time(self) -> datetime.datetime:
        return self._refresh().workflow.update_time

    @property
    def revision_id(self) -> str:
        return self._refresh().workflow.revision_id

    def state(self) -> Literal['STATE_UNSPECIFIED', 'ACTIVE', 'UNAVAILABLE']:
        return self._refresh().workflow.state.name

    def update(self, config: WorkflowConfig):
        self.workflow.source_contents = json.dumps(config.definition)
        req = workflows_v1.UpdateWorkflowRequest(workflow=self.workflow)
        op = _call_workflow_client('update_workflow', req)
        self.workflow = op.result()
        return self

    def execute(self, args: dict | None = None) -> Execution:
        if args:
            exe = executions_v1.Execution(argument=json.dumps(args))
        else:
            exe = executions_v1.Execution()
        req = executions_v1.CreateExecutionRequest(parent=self.name, execution=exe)
        resp = _call_execution_client('create_execution', req)
        self._refresh()
        return Execution(resp)

    def delete(self) -> None:
        req = workflows_v1.DeleteWorkflowRequest(name=self.name)
        _call_workflow_client('delete_workflow', req)
        self.workflow = None

    def list_executions(self) -> list[Execution]:
        req = executions_v1.ListExecutionsRequest(parent=self.name)
        resp = _call_execution_client('list_executions', req)
        return [Execution(r) for r in resp]


class Execution:
    def __init__(self, name_or_obj: str | executions_v1.Execution):
        if isinstance(name_or_obj, str):
            self.name = name_or_obj
            self.execution = None
            self._refresh()
        else:
            self.name = name_or_obj.name
            self.execution = name_or_obj

    def __repr__(self):
        return f"{self.__class__.__name__}('{self.name}')"

    def __str__(self):
        return self.__repr__()

    def _refresh(self):
        req = executions_v1.GetExecutionRequest(name=self.name)
        self.execution = _call_execution_client('get_execution', req)
        return self

    @property
    def start_time(self) -> datetime.datetime:
        return self.execution.start_time

    @property
    def end_time(self) -> datetime.datetime:
        # If not finished (`state()` returns "ACTIVE"), this returns `None`.
        return self._refresh().execution.end_time

    @property
    def argument(self):
        return self.execution.argument

    def result(self):
        return str(self._refresh().execution.result)

    def status(self):
        # If still running, this returns which step is currently running.
        return self._refresh().execution.status

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
        return self._refresh().execution.state.name

    def cancel(self):
        req = executions_v1.CancelExecutionRequest(name=self.name)
        _call_execution_client('cancel_execution', req)
        self._refresh()
