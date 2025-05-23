import pickle

import google.api_core.exceptions
import pytest

from cloudly.gcp import bigquery
from cloudly.gcp.auth import get_project_id
from cloudly.gcp.storage import GcsBlobUpath


def test_list_datasets():
    assert 'tmp' in bigquery.list_datasets()


def _test_labels(tab):
    tab.update_labels(
        {
            'friendly_name': 'tom',
            'age': '38',
        }
    )
    labs = tab.labels
    print('labels', labs)
    assert labs['friendly_name'] == 'tom'
    assert labs['age'] == '38'
    tab.update_labels({'city': 'ny'})
    assert tab.labels['city'] == 'ny'
    tab.update_labels({'age': None})
    assert 'age' not in tab.labels


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
        assert not z
        assert table.count_rows() == 3

    name = bigquery.Dataset.make_temp_table_name()
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


def test_table():
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
    print(tab.count_rows())

    assert tab.count_rows() == len(data) + 2
    assert tab.table_id in bigquery.Dataset('tmp').list_tables()

    _test_labels(tab)

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


def test_view():
    data = [
        {'name': 'Tom', 'age': 38},
        {'name': 'Peter', 'age': 61},
        {'name': 'Jessica', 'age': 22},
        {'name': 'Joe', 'age': 8},
        {'name': 'John', 'age': 15},
    ]
    table = bigquery.Dataset('tmp').temp_table().load_from_json(data)
    print(table.qualified_table_id)
    try:
        for materialized in (False, True):
            view = (
                bigquery.Dataset('tmp')
                .view('old')
                .drop_if_exists()
                .create(
                    f"""\
            SELECT name
            FROM `{table.qualified_table_id}`
            WHERE age > 20
            """,
                    materialized=materialized,
                )
            )

            try:
                print(view.qualified_view_id)
                assert view.view.table_type == (
                    'MATERIALIZED_VIEW' if materialized else 'VIEW'
                )
                assert view.count_rows() == 3
                assert view.view_id in bigquery.Dataset('tmp').list_views()
                assert sorted(row[0] for row in view.read_rows()) == [
                    'Jessica',
                    'Peter',
                    'Tom',
                ]
                # _test_labels(view)
            finally:
                view.drop()
    finally:
        table.drop()


def test_external():
    data = [
        {'name': 'Tom', 'age': 38},
        {'name': 'Peter', 'age': 61},
        {'name': 'Jessica', 'age': 22},
        {'name': 'Joe', 'age': 8},
        {'name': 'John', 'age': 15},
    ]
    table = bigquery.Dataset('tmp').temp_table().load_from_json(data)
    try:
        path = GcsBlobUpath('/test/bq', bucket_name='zpz-tmp')
        path.rmrf()
        table.extract_to_uri(str(path / 'part-*.parquet'))

        etable = (
            bigquery.Dataset('tmp')
            .external_table('abc')
            .drop_if_exists()
            .create(str(path / 'part-*.parquet'), source_format='PARQUET')
        )
        try:
            assert etable.count_rows() == 5
            rows = sorted(etable.read_rows(as_dict=True), key=lambda x: x['age'])
            assert rows == [
                {'name': 'Joe', 'age': 8},
                {'name': 'John', 'age': 15},
                {'name': 'Jessica', 'age': 22},
                {'name': 'Tom', 'age': 38},
                {'name': 'Peter', 'age': 61},
            ]
            _test_labels(etable)

        finally:
            etable.drop()
    finally:
        table.drop()


def test_scalar_function():
    data = [
        {'name': 'Tom', 'age': 38},
        {'name': 'Peter', 'age': 61},
        {'name': 'Jessica', 'age': 22},
        {'name': 'Joe', 'age': 8},
        {'name': 'John', 'age': 15},
    ]
    table = bigquery.dataset('tmp').temp_table().load_from_json(data)
    try:
        assert table.exists()
        sql = f"""
            CREATE TEMP FUNCTION double_age(year INT64)
            RETURNS INT64
            AS (
                year + year
            );
            SELECT name, age, double_age(age) as older
            FROM `{table.qualified_table_id}`;
        """
        zz = list(bigquery.read(sql, as_dict=True))
        for row in zz:
            assert row['older'] == 2 * row['age']

        udf = (
            bigquery.dataset('tmp')
            .scalar_function('get_old')
            .drop_if_exists()
            .create(
                language='SQL',
                body='year * 2',
                arguments=[
                    bigquery.ScalarFunction.routine_input_argument('year', 'INT64')
                ],
                return_type='int64',
            )
        )
        assert udf.exists()
        assert udf.routine_id in bigquery.dataset('tmp').list_scalar_functions()

        sql = f"""
            SELECT name, age, `{udf.qualified_routine_id}`(age) as older
            FROM `{table.qualified_table_id}`
        """
        zz = list(bigquery.read(sql, as_dict=True))
        for z in zz:
            assert z['older'] == z['age'] * 2

        udf = (
            bigquery.dataset('tmp')
            .scalar_function('merge')
            .drop_if_exists()
            .create(
                language='JAVASCRIPT',
                arguments=[
                    bigquery.ScalarFunction.routine_input_argument('name', 'STRING'),
                    bigquery.ScalarFunction.routine_input_argument('age', 'int64'),
                ],
                body="return name + '-' + age;",
                return_type='string',
            )
        )
        assert udf.exists()

        sql = f"""
            SELECT name, age, `{udf.qualified_routine_id}`(name, age) as merged
            FROM `{table.qualified_table_id}`
        """
        for row in bigquery.read(sql, as_dict=True):
            assert row['merged'] == f'{row["name"]}-{row["age"]}'

    finally:
        table.drop()
