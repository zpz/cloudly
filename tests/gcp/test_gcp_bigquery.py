import pickle

import google.api_core.exceptions
import pytest

from cloudly.gcp import bigquery
from cloudly.gcp.auth import get_project_id
from cloudly.gcp.storage import GcsBlobUpath


def test_list_datasets():
    assert 'tmp' in bigquery.list_datasets()


def test_create():
    def check_table(table):
        assert table.exists()
        z = table.insert_rows(
            (
                {'name': 'Tom', 'age': 28},
                {'name': 'John', 'age': 58},
                {'name': 'Ali', 'age': 80},
            )
        )
        print(z)
        assert table.count_rows() == 3

    name = bigquery._make_temp_name()
    sql = f"""\
        CREATE TABLE `{get_project_id()}.tmp.{name}` (
            name STRING,
            age INTEGER
        )"""
    bigquery.get_client().query(sql)
    table = bigquery.Dataset('tmp').table(name)
    try:
        check_table(table)
        with pytest.raises(google.api_core.exceptions.Conflict):
            table.create([('name', 'STRING'), ('age', 'INTEGER')])
    finally:
        table.drop()

    table = bigquery.Dataset('tmp').temp_table()
    table.create(
        [
            ('name', 'STRING'),
            ('age', 'INTEGER'),
        ]
    )
    try:
        check_table(table)
    finally:
        table.drop()

    table = bigquery.Dataset('tmp').temp_table()
    table.create(
        [
            ('name', 'STRING', 'REQUIRED'),
            bigquery.SchemaField('age', 'INTEGER'),
        ]
    )
    try:
        check_table(table)
    finally:
        table.drop()


def test_temp_table():
    print()
    data = [
        {'name': 'Tom', 'age': 38},
        {'name': 'Peter', 'age': 56},
        {'name': 'Jane', 'age': 23},
        {'name': 'Luke', 'age': 99},
    ]

    tab = bigquery.Dataset('tmp').temp_table()
    print(tab.qualified_table_id)
    z = tab.load_from_json(data)
    print(z)
    y = list(tab.read_rows())
    assert len(y) == len(data)

    print(list(tab.read_rows(as_dict=False)))

    print(list(tab.stream_read_rows(as_dict=True)))

    assert tab.list_partitions() is None

    zz = tab.insert_rows([{'name': 'Paul', 'age': 60}, {'name': 'Jessica', 'age': 9}])
    print(zz)

    assert tab.count_rows() == len(data) + 2

    assert tab.table_id in bigquery.Dataset('tmp').list_tables()

    path = GcsBlobUpath('/test/bq', bucket_name='zpz-tmp')
    path.rmrf()
    tab.extract_to_uri(str(path / 'part-*.parquet'))

    tab2 = bigquery.Dataset('tmp').temp_table()
    tab2.load_from_uri(str(path / '*.parquet'))
    yy = list(tab2.read_rows())
    print(yy)
    assert len(yy) == len(data) + 2

    tab3 = bigquery.Dataset('tmp').table(tab.table_id)

    assert yy == list(tab3.read_rows())
    assert tab2.exists()

    tab4 = pickle.loads(pickle.dumps(tab2))
    assert len(data) + 2 == tab4.count_rows()

    sql = f'SELECT name FROM `{tab.qualified_table_id}` WHERE age > 30'
    older = bigquery.Dataset('tmp').temp_table()
    older.load_from_query(sql)
    names = sorted(row['name'] for row in older.read_rows(as_dict=True))
    assert names == sorted(['Tom', 'Peter', 'Luke', 'Paul'])

    tab.drop()
    path.rmrf()
    assert not tab3.exists()
