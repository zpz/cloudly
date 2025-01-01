from __future__ import annotations

__all__ = ['Workflow', 'Execution', 'Step', 'BatchStep']

import atexit
import datetime
import json
import uuid
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
    def content(self) -> dict:
        return {self.name: self._content}


class WaitStep(Step):
    """
    Wait for a GCP service such as a Batch job to finish.

    TODO: does not work; fix this.
    """

    def __init__(
        self,
        name: str,
        *,
        job_url: str,
        success_state: str = 'SUCCEEDED',
        failure_state: str = 'ERROR',
        poll_interval_seconds: int = 10,
    ):
        uid = str(uuid.uuid4()).replace('-', '')[:8]
        content = {
            'steps': [
                {
                    f'poll_{uid}': {
                        'call': 'http.get',
                        'args': {
                            'url': job_url,
                            'auth': {'type': 'OAuth2'},
                        },
                        'result': f'poll_{uid}_result',
                    }
                },
                {
                    f'check_{uid}': {
                        'switch': [
                            {
                                'condition': f'${{poll_{uid}_result.body.status.state == "{success_state}"}}',
                                'next': f'log_{uid}',
                            },
                            {
                                'condition': f'${{poll_{uid}_result.body.status.state == "{failure_state}"}}',
                                'raise': f'{name} error: ${{poll_{uid}_result.body.status.state}}',
                            },
                        ],
                        'next': f'sleep_{uid}',
                    }
                },
                {
                    f'sleep_{uid}': {
                        'call': 'sys.sleep',
                        'args': {'seconds': poll_interval_seconds},
                        'next': f'poll_{uid}',
                    }
                },
                {
                    f'log_{uid}': {
                        'call': 'sys.log',
                        'args': {
                            'data': f'{name} state: ${{poll_{uid}_result.body.status.state}}',
                        },
                    }
                },
            ]
        }
        # TODO: the `log` step may not be very useful.
        # TODO: how happens after the `raise`?
        super().__init__(name, content)


class ParallelStep(Step):
    pass


class BatchStep(Step):
    def __init__(
        self,
        name: str,
        config: BatchJobConfig,
        *,
        result_name: str = None,
    ):
        """
        If you want Workflows to wait for this batch job to finish, add a :meth:`WaitStep` after this,
        using `self.job_url` as the argument `job_url` to `WaitStep`.

        If you want to assign the result to a variable, pass in `result_name`. Usually, you can use
        `name.replace('-', '_') + '_result'`.
        """
        api_url = f'https://batch.googleapis.com/v1/projects/{get_project_id()}/locations/{config.region}/jobs'
        job_config = json.loads(type(config.job).to_json(config.job))
        job_id = name.replace('_', '-')

        content = {
            'call': 'http.post',
            'args': {
                'url': api_url,
                'query': {'job_id': job_id},
                'headers': {'Content-Type': 'application/json'},
                'auth': {'type': 'OAuth2'},
                'body': job_config,
            },
        }
        if result_name:
            assert '-' not in result_name, f"'-' not in '{result_name}'"
            content['result'] = result_name
        # `job_id`` requirement: ^[a-z]([a-z0-9-]{0,61}[a-z0-9])?$  Note in particular: doesn't allow underscore.
        # `result` name must be a valid variable (or identifier) name, e.g. it can't contain dash.
        # Experiments suggested that `job_id` and `result` name do not have fixed relation with the step `name`;
        # I made changes to both and it still worked.
        super().__init__(name, content)
        self.job_url = f'{api_url}/{job_id}'
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


class Workflow:
    @classmethod
    def create(
        cls,
        name: str,
        steps: Sequence[Step],
        *,
        region: str,
        args_name: str = None,
    ) -> Workflow:
        """
        `name` needs to be unique, hence it's recommended to construct it with some randomness.

        If you create a workflow for a, say, batch job, then you probably should get `region`
        from the batch job definition. I don't know whether it's allowed for a workflow to
        contain jobs spanning regions.

        If your workflow requires command-line arguments, you should specify a single name for them,
        and access individual arguments using `dot`, for example, "args.name", "args.age", etc, where
        `args_name` is "args". Correspondingly in :meth:`execute`, you need to pass a dict to `args`,
        e.g. `{'name': 'Tom', 'age': 38}`.
        """
        content = {'main': {}}
        if args_name:
            content['main']['params'] = [args_name]
        content['main']['steps'] = [s.content for s in steps]
        content = json.dumps(content)

        workflow = workflows_v1.Workflow(source_contents=content)
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
    def content(self) -> dict:
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

    def _refresh(self):
        req = workflows_v1.GetWorkflowRequest(name=self._name)
        self._workflow = _call_workflow_client('get_workflow', req)

    def state(self) -> Literal['STATE_UNSPECIFIED', 'ACTIVE', 'UNAVAILABLE']:
        self._refresh()
        return self._workflow.state.name

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
