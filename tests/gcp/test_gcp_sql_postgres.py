import pytest

from cloudly.experimental.sql.postgres import database, list_databases
from cloudly.gcp.sql.postgres import Instance, connect

REGION = 'us-west1'


@pytest.fixture(scope='session')
def instance():
    inst = Instance.create(
        name='test-gcp-pg',
        region=REGION,
        root_password='rootuser',  # noqa: S106
        num_read_replicas=1,
    )
    print(inst)
    try:
        yield inst
    finally:
        inst.delete()


def test_instance(instance):
    inst = instance
    print('name:', inst.name)
    assert inst.name in [v.name for v in Instance.list()]
    assert len(inst.replica_names) > 0
    print('replicas:', inst.replica_names)
    assert all([r.startswith(inst.name) for r in inst.replica_names])
    assert inst.instance_type == 'CLOUD_SQL_INSTANCE'
    assert inst.url is None
    print('host_ip:', inst.host_ip)
    assert inst.state() == 'RUNNABLE'


def test_basic(instance):
    with connect(connection_name=instance.connection_name, password='rootuser') as conn:  # noqa: S106
        dbs = list_databases(conn)
        print('databases:', dbs)
        if 'test' not in dbs:
            conn.execute('CREATE DATABASE test')

    with connect(
        connection_name=instance.connection_name,
        password='rootuser',  # noqa: S106
        db='test',
    ) as conn:
        db = database(conn.cursor())
        print('tables:', db.list_tables())
        tb = db.table('test_basic')
        tb.drop_if_exists()
        conn.execute('CREATE TABLE test_basic (name VARCHAR(50), age INTEGER)')
        assert tb.exists()
        cols = tb.column_names
        print('cols:', cols)
        keys = tb.primary_keys
        print('keys:', keys)
        assert tb.count_rows() == 0
