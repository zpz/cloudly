"""
This module provides some convenience functions operating on a database or a table.
In addition to these utilities, you will use functionalities of "connection", "cursor", and "connection pool"
directly from `psycopg` and `psycopg_pool`.

The functionality does not try to be comprehensive. For example, there is no "database management" functions
for creating/deleting a database, because I don't feel much need for programmatically manage databases.
For the few occasions of setting-up/tearing-down a database, just go to a dashboard. Or you can use `psycopg`
for that.
"""

from __future__ import annotations

__all__ = [
    'connect',
    'database',
    'list_databases',
    'Connection',
    'ConnectionPool',
    'Cursor',
    'Database',
    'Table',
    'enable_pgvector',
]


from collections.abc import Sequence
from typing import Literal

import pg8000
import psycopg
import psycopg_pool

DuplicateDatabase = psycopg.errors.DuplicateDatabase
DuplicateTable = psycopg.errors.DuplicateTable
UndefinedTable = psycopg.errors.UndefinedTable

connect = psycopg.connect
Connection = psycopg.Connection
Cursor = psycopg.Cursor
ConnectionPool = psycopg_pool.ConnectionPool


# About pgvector on CloudSQL, see
#   https://cloud.google.com/blog/products/databases/using-pgvector-llms-and-langchain-with-google-cloud-databases


def enable_pgvector(conn: Connection | Cursor) -> None:
    """
    This "installs" the pgvector extension on the instance.

    See https://cloud.google.com/blog/products/databases/using-pgvector-llms-and-langchain-with-google-cloud-databases
    """
    conn.execute('CREATE EXTENSION IF NOT EXISTS vector')


def database(conn: Connection | Cursor) -> Database:
    return Database(conn)


def list_databases(conn: Connection | Cursor) -> list[str]:
    cursor = conn.execute('SELECT datname FROM pg_database')
    return sorted(row[0] for row in cursor.fetchall())


class Database:
    def __init__(self, conn: Connection | Cursor):
        """
        `conn` is a Connection that has been opened to the database of interest,
        or a Cursor out of such a connection.
        Various methods of this class call `conn.execute`.
        If `conn` is a Connection, `conn.execute` opens and closes a new Cursor.
        If `conn` is a Cursor, `conn.execute` re-uses the single cursor.
        """
        self._conn = conn

    def list_tables(self) -> list[str]:
        cursor = self._conn.execute("""\
            SELECT table_name 
            FROM information_schema.tables
            WHERE table_schema='public' AND table_type='BASE TABLE'""")
        return sorted(row[0] for row in cursor.fetchall())

    def table(self, table_name: str) -> Table:
        return Table(table_name, self._conn)


class Table:
    def __init__(self, table_name: str, conn: Connection | Cursor):
        self.table_name = table_name
        self._conn = conn

    def __repr__(self):
        return f"{self.__class__.__name__}('{self.table_name}')"

    def __str__(self):
        return self.__repr__()

    def exists(self) -> bool:
        cursor = self._conn.execute(
            f"SELECT EXISTS (SELECT FROM pg_tables WHERE tablename = '{self.table_name}')"
        )
        return cursor.fetchone()[0]

    def drop(self, *, not_found_ok: bool = False) -> None:
        sql = f'DROP TABLE {self.table_name}'
        try:
            self._conn.execute(sql)
        except UndefinedTable:
            if not_found_ok:
                return self
            raise
        except pg8000.exceptions.DatabaseError as e:
            if 'does not exist' in str(e) and not_found_ok:
                return self
            raise
        except Exception:
            print('SQL:')
            print(sql)
            raise
        return self

    def drop_if_exists(self) -> None:
        self.drop(not_found_ok=True)

    def count_rows(self) -> int:
        cursor = self._conn.execute(f"SELECT COUNT(*) FROM '{self.table_name}'")
        return cursor.fetchone()[0]

    @property
    def column_names(self) -> list[str]:
        cursor = self._conn.execute(
            f"SELECT column_name FROM information_schema.columns WHERE table_name = '{self.table_name}'"
        )
        return [row[0] for row in cursor.fetchall()]

    @property
    def primary_keys(self) -> list[str]:
        sql = f"""\
            SELECT a.attname
            FROM pg_index a
            JOIN pg_attribute b
                ON b.attrelid = a.indrelid
                    AND b.attnum = ANY(a.indkey)
            WHERE a.indrelid = '{self.table_name}'::regclass
                AND a.indisprimary
        """
        cursor = self._conn.execute(sql)
        return [row[0] for row in cursor.fetchall()]

    def insert_rows(self, data: Sequence[tuple], *, overwirte: bool = False) -> None:
        keys = self.primary_keys
        nrows = len(data)
        ncols = len(data[0])
        template = ', '.join([str(('%s',) * ncols)] * nrows)
        if keys and overwirte:
            sql = f"""\
                INSERT INTO {self.table_name}
                VALUES
                {template}
                ON CONFLICT ({', '.join(keys)}) DO UPDATE SET {', '.join(f'{col} = EXCLUDED.{col}' for col in self.column_names)}
            """
        else:
            sql = f'INSERT INTO {self.table_name} VALUES {template}'
        self._conn.execute(sql, sum(data, ()))

    def list_vector_indexes(self) -> list[str]:
        sql = f"SELECT indexname FROM pg_indexes WHERE tablename = '{self.table_name}'"
        cursor = self._conn.execute(sql)
        return sorted(row[0] for row in cursor.fetchall())

    def create_vector_index(
        self,
        vector_col: str,
        index_name: str | None = None,
        *,
        index_type: Literal['hnsw', 'ivfflat'] = 'hnsw',
        distance_metric: Literal['l2', 'cosine'] = 'l2',
        partial_index_condition: str | None = None,
        **index_opts,
    ) -> str:
        """
        Parameters
        ----------
        index_name
            Required if `partial_index_condition` is not `None`.
        partial_index_condition
            If the index is to be built only only rows meeting certain conditions, this is the condition after 'WHERE', for example,

                'age > 30'

            See https://www.postgresql.org/docs/current/indexes-partial.html
        index_opts
            Options pertaining to the selected type of index. See
            https://github.com/pgvector/pgvector?tab=readme-ov-file#hnsw
            https://github.com/pgvector/pgvector?tab=readme-ov-file#ivfflat

        When this index-building is ongoing, the table is locked against writing.
        If this is not desirable, there is a "CONCURRENTLY' option but that would take longer to complete.
        """
        if not index_name:
            if partial_index_condition:
                raise ValueError(
                    '`index_name` is required when `partial_index_condition` is not `None`'
                )
            index_name = f'{self.table_name}_{vector_col}_vector'

        sql = f"""\
            CREATE INDEX {index_name} ON {self.table_name} USING {index_type} ({vector_col} vector_{distance_metric}_ops)
            """
        if index_type == 'ivfflat':
            sql = f"{sql} WITH (lists = {index_opts.get('lists', 100)})"
        else:
            assert index_type == 'hnsw'
            sql = f"{sql} WITH (ef_construction={index_opts.get('ef_construction', 64)}, m={index_opts.get('m', 16)})"
        if partial_index_condition:
            sql = f'{sql} WHERE {partial_index_condition}'
        self._conn.execute(sql)
        return index_name

    def drop_vector_index(self, index_name: str, *, not_found_ok: bool = False) -> None:
        try:
            self._conn.execute(f'DROP INDEX {index_name}')
        except Exception:
            if not_found_ok:  # TODO
                raise
            raise

    def rebuild_vector_index(self, index_name: str):
        """
        Rebuild the index from scratch. The operation is not very different from dropping and then re-creating the index.

        The table is locked against writing during this operation.
        """
        self._conn.execute(f'REINDEX INDEX {index_name}')

    def vector_distance_sql(
        self,
        query: list[float],
        *,
        vector_index_name: str,
        distance_name: str = 'distance',
    ):
        indexdef = self._conn.execute(
            f'SELECT indexdef FROM pg_indexes WHERE tablename = {self.table_name} AND indexname = {vector_index_name}'
        ).fetchone()[0]
        # This is the SQL statement used in `create_vector_index`.
        # TODO: parse out info from the def string.
        print(indexdef)
