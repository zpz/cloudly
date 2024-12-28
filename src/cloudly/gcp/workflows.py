from __future__ import annotations

import json
import uuid
from collections.abc import Sequence

from google.cloud import workflows_v1
from google.cloud.workflows import executions_v1

from .auth import get_credentials, get_project_id
from .batch import JobConfig as BatchJobConfig


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

    def render(self) -> dict[str, dict]:
        return {self.name: self._content}


class WaitStep(Step):
    """
    Wait for a GCP service such as a Batch job to finish.
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
                            'data': f'${{"{name} state: poll_{uid}_result.body.status.state"}}',
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
    def __init__(self, name: str, config: BatchJobConfig):
        # If you want Workflows to wait for this batch job to finish, add a :meth:`WaitStep` after this,
        # using `self.job_url` as the argument `job_url` to `WaitStep`.
        api_url = f'https://batch.googleapis.com/v1/projects/{get_project_id()}/locations/{config.region}/jobs'
        job_config = json.loads(type(config.job).to_json(config.job))
        content = {
            'call': 'http.post',
            'args': {
                'url': api_url,
                'query': {'job_id': f'${{"{name}"}}'},
                'headers': {'Content-Type': 'application/json'},
                'auth': {'type': 'OAuth2'},
                'body': job_config,
            },
            'result': f'{name}_result',
        }
        super().__init__(name, content)
        self.job_url = f'{api_url}/{name}'


class Execution:
    def __init__(self, name: str | executions_v1.Execution):
        if isinstance(name, str):
            self._name = name
            self._execution = None
        else:
            self._name = name.name
            self._execution = name

    @property
    def name(self):
        return self._name

    def _refresh(self):
        req = executions_v1.GetExecutionRequest(name=self.name)
        self._execution = Workflow._execution_client().get_execution(req)

    def result(self):
        self._refresh()
        return str(self._execution.result)

    @property
    def state(self) -> str:
        """
        Returned value is like "ACTIVE", "FAILED", CANCELLED", etc.
        """
        self._refresh()
        return self._execution.state.name


class Workflow:
    @classmethod
    def _workflow_client(cls):
        return workflows_v1.WorkflowsClient(credentials=get_credentials())

    @classmethod
    def _execution_client(cls):
        return workflows_v1.ExecutionsClient(credentials=get_credentials())

    @classmethod
    def create(
        cls,
        name: str,
        steps: Sequence[Step],
        *,
        region: str,
        param_names: Sequence[str] | None = None,
    ) -> Workflow:
        """
        `name` needs to be unique, hence it's recommended to construct it with some randomness.
        """
        content = {}
        if param_names:
            content['params'] = list(param_names)
        content['steps'] = [s.render() for s in steps]
        content = json.dumps(content)

        workflow = workflows_v1.Workflow(source_contents=content)
        req = workflows_v1.CreateWorkflowRequest(
            parent=f'projects/{get_project_id()}/locations/{region}',
            workflow=workflow,
            workflow_id=name,
        )
        op = cls._workflow_client().create_workflow(req)
        resp = op.result()
        return cls(resp)

    def __init__(self, name: str | workflows_v1.Workflow):
        """
        `name` is like "projects/<project_id>/locations/<region>/workflows/<name>".
        """
        if isinstance(name, str):
            self._name = name
            self._workflow = None
        else:
            self._name = name.name
            self._workflow = name

    @property
    def name(self) -> str:
        return self.name

    def _refresh(self):
        req = workflows_v1.GetWorkflowRequest(name=self._name)
        self._workflow = self._workflow_client().get_workflow(req)

    def execute(self, args: dict | None = None) -> Execution:
        if args:
            exe = executions_v1.Execution(argument=json.dumps(args))
        else:
            exe = executions_v1.Execution()
        req = executions_v1.CreateExecutionRequest(parent=self._name, execution=exe)
        resp = self._execution_client().create_execution(req)
        return Execution(resp)

    def delete(self) -> None:
        req = workflows_v1.DeleteWorkflowRequest(name=self._name)
        self._workflow_client().delete_workflow(req)

    def list_executions(self) -> list[Execution]:
        req = executions_v1.ListExecutionsRequest(parent=self.name)
        resp = self._execution_client().list_executions(req)
        return [Execution(r) for r in resp]
