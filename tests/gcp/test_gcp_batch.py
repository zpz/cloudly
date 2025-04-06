from time import sleep
from uuid import uuid4

from cloudly.gcp.batch import Job, JobConfig

REGION = 'us-west1'


def test_batch():
    config = JobConfig(
        task_group={
            'task_spec': {
                'container': {
                    'image_uri': 'docker.io/library/debian:stable-slim',
                    'commands': 'echo yes',
                }
            },
        },
        allocation_policy={
            'machine_type': 'n1-standard-1',
            'region': REGION,
        },
    )
    job = Job.create(f'test-batch-job-{str(uuid4()).split("-")[0]}', config)
    print('created batch job:', job)
    try:
        print('name:', job.name)
        print('status:', job.status())
        print('state:', job.state())
        print('definition:', job.definition)
        jobs = Job.list(region=REGION)
        assert job.name in [j.name for j in jobs]
        n = 0
        while True:
            n += 1
            s = job.state()
            if s in ('SUCCEEDED', 'FAILED'):
                print('state:', s)
                if s == 'FAILED':
                    raise Exception('job failed')
                break
            if n > 100:
                print('state:', s)
                raise Exception('has not finished after a long time')
            sleep(2)
    finally:
        job.delete()
        for _ in range(10):
            try:
                assert job.name not in [v.name for v in Job.list(region=REGION)]
                break
            except AssertionError:
                if _ < 9:
                    sleep(10)
                else:
                    raise
