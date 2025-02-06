"""
This module provides some convenience functions operating on a database or a table.
In addition to these utilities, you will use functionalities of "connection", "cursor", and "connection pool"
directly from `psycopg` and `psycopg_pool`.

This module is general Postgres client code. It is NOT tied to GCP.

The functionality does not try to be comprehensive. For example, there is no "database management" functions
for creating/deleting a database, because I don't feel much need for programmatically manage databases.
For the few occasions of setting-up/tearing-down a database, just go to a dashboard. Or you can use `psycopg`
for that.
"""

from __future__ import annotations

from collections.abc import Sequence

import psycopg
import psycopg_pool
from typing_extensions import Self

DuplicateDatabase = psycopg.errors.DuplicateDatabase
DuplicateTable = psycopg.errors.DuplicateTable
UndefinedTable = psycopg.errors.UndefinedTable

connect = psycopg.connect
Connection = psycopg.Connection
Cursor = psycopg.Cursor
ConnectionPool = psycopg_pool.ConnectionPool


def enable_pgvector(conn: Connection | Cursor) -> None:
    """
    This "installs" the pgvector extension on the instance.

    See https://cloud.google.com/blog/products/databases/using-pgvector-llms-and-langchain-with-google-cloud-databases
    """
    conn.execute('CREATE EXTENSION IF NOT EXISTS vector')


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

    def drop(self, *, not_found_ok: bool = False) -> Self:
        try:
            self._conn.execute(f"DROP TABLE '{self.table_name}'")
        except UndefinedTable:
            if not_found_ok:
                return self
            raise
        return self

    def drop_if_exists(self):
        return self.drop(not_found_ok=True)

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
        if keys and overwirte:
            raise NotImplementedError
            # TODO
            # need to use the "ON CONFLICT ... DO UPDATE SET ..." statement.
        else:
            template = ', '.join([str(('%s',) * ncols)] * nrows)
            sql = f'INSERT INTO {self.table_name} VALUES {template}'
            self._conn.execute(sql, sum(data, ()))
