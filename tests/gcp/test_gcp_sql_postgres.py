import pytest

from cloudly.gcp.sql.postgres import Instance, connect
from cloudly.sql.postgres import database, enable_pgvector, list_databases

REGION = 'us-west1'


@pytest.fixture(scope='session')
def instance():
    # inst = PostgresInstance.create(
    #     name=f"test-pg",
    #     region=REGION,
    #     root_password='rootuser',  # noqa: S106
    #     num_read_replicas=1,
    # )
    inst = Instance('test-pg')
    print(inst)
    try:
        yield inst
    finally:
        # inst.delete()
        pass


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
        tb.insert_rows(
            (
                ('Tom', 28),
                ('Jack', 46),
                ('Peter', 21),
                ('Joe', 13),
            )
        )
        assert tb.count_rows() == 4
        tb.insert_rows((('Tom', 28), ('Jason', 6), ('Joe', 13), ('Jessica', 20)))
        assert tb.count_rows() == 8


def test_pgvector(instance):
    with connect(connection_name=instance.connection_name, password='rootuser') as conn:  # noqa: S106
        if 'test' not in list_databases(conn):
            conn.execute('CREATE DATABASE test')

    with connect(
        connection_name=instance.connection_name,
        password='rootuser',  # noqa: S106
        db='test',
    ) as conn:
        enable_pgvector(conn)
        db = database(conn)
        db.table('test_vector').drop_if_exists()
        dim = 4
        conn.execute(
            f'CREATE TABLE test_vector ( name VARCHAR, size INTEGER, embedding vector({dim}))'
        )
        table = db.table('test_vector')
        assert table.exists()
        index_name = table.create_vector_index('embedding')
        print(f"created vector index '{index_name}' on table '{table.table_name}'")
        assert index_name in table.list_vector_indexes()
