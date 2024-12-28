from __future__ import annotations

import uuid
from collections.abc import Iterable, Sequence
import json

from google.cloud import workflows_v1
from .auth import get_credentials, get_project_id
from google.cloud.workflows import executions_v1


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
        return {
            self.name: self._content
        }


class WaitStep(Step):
    """
    Wait for a GCP service such as a Batch job to finish.
    """
    def __init__(self, name: str, *, job_url: str, success_state: str = 'SUCCEEDED', failure_state: str = 'ERROR', poll_interval_seconds: int = 10):
        uid = str(uuid.uuid4()).replace('-', '')[:8]
        content = {
            'steps': [
                {
                    f'poll_{uid}':
                    {
                        'call': 'http.get',
                        'args': {
                            'url': job_url,
                            'auth': {'type': 'OAuth2'},
                        },
                        'result': f'poll_{uid}_result',
                    }
                },
                {
                    f'check_{uid}':
                    {
                        'switch': [
                            {
                                'condition': f'${{poll_{uid}_result.body.status.state == "{success_state}"}}',
                                'next': f'log_{uid}',
                            },
                            {
                                'condition': f'${{poll_{uid}_result.body.status.state == "{failure_state}"}}',
                                'raise': f'{name} error: ${{poll_{uid}_result.body.status.state}}'
                            }
                        ],
                        'next': f'sleep_{uid}',
                    }
                },
                {
                    f'sleep_{uid}':
                    {
                        'call': 'sys.sleep',
                        'args': {'seconds': poll_interval_seconds},
                        'next': f'poll_{uid}',
                    }
                },
                {
                    f'log_{uid}':
                    {
                        'call': 'sys.log',
                        'args': {
                            'data': f'${{"{name} state: poll_{uid}_result.body.status.state"}}',
                        }
                    }
                }
            ]
        }
        super().__init__(name, content)


class ParallelStep(Step):
    pass


class BatchStep(Step):
    pass


class Execution:
    @classmethod
    def _execution_client(cls):
        return workflows_v1.ExecutionsClient(credentials=get_credentials())

    def __init__(self, execution: executions_v1.Execution):
        self._execution = execution

    @property
    def name(self):
        return self._execution.name
    
    def _refresh(self):
        req = executions_v1.GetExecutionRequest(name=self.name)
        self._execution = self._execution_client().get_execution(req)

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
    def create(cls, name: str, steps: Sequence[Step], *, region: str, param_names: Sequence[str] | None = None) -> Workflow:
        """
        `name` needs to be unique, hence it's recommended to construct it with some randomness.
        """
        content = {}
        if param_names:
            content['params'] = list(param_names)
        content['steps'] = [s.render() for s in steps]
        content = json.dumps(content)

        workflow = workflows_v1.Workflow(source_contents=content)
        req = workflows_v1.CreateWorkflowRequest(parent=f"projects/{get_project_id()}/locations/{region}", workflow=workflow, workflow_id=name)
        op = cls._workflow_client().create_workflow(req)
        resp = op.result()
        obj = cls(resp.name, resp)
        return obj
    
    def __init__(self, name: str, workflow: workflows_v1.Workflow | None = None):
        """
        `name` is like "projects/<project_id>/locations/<region>/workflows/<name>".
        """
        self._name = name
        self._workflow = workflow

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



