from uuid import uuid4

from cloudly.gcp.cloudsql.postgres import Instance

REGION = 'us-west1'


def test_instance():
    inst = Instance.create(
        name=f"test-pg-{str(uuid4()).split('-')[0]}",
        region=REGION,
        root_password='rootuser',  # noqa: S106
        num_read_replicas=1,
    )
    print(inst)
    try:
        print('name:', inst.name)
        assert inst.name in [v.name for v in Instance.list()]
        print('replicas:', inst.replica_names)
        print('instance_type:', inst.instance_type)
        print('url:', inst.url)
        print('host_ip:', inst.host_ip)
    finally:
        # inst.delete()
        pass
