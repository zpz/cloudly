from cloudly.gcp.compute import Instance, InstanceConfig

ZONE = 'us-west1-a'


def test_compute():
    config = InstanceConfig(
        name='test',
        zone=ZONE,
        machine_type='n1-standard-1',
    )
    inst = Instance.create(config)
    try:
        assert inst.name in [v.name for v in Instance.list(ZONE)]
        assert inst.machine_type == 'n1-standard-1'
        assert not inst.gpu
        assert len(inst.disks) == 1
        print('IP:', inst.ip)
        print('ID', inst.id)
        assert inst.state() == 'RUNNING', inst.state()
    finally:
        inst.delete()
        assert inst.name not in [v.name for v in Instance.list(ZONE)]
