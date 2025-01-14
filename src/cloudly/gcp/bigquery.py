from __future__ import annotations

__all__ = [
    'get_client',
    'get_storage_client',
    'list_datasets',
    'read_streams',
    'Dataset',
    'Table',
    'SchemaField',
]


import logging
import queue
import threading
from collections.abc import Iterable, Iterator, Sequence
from multiprocessing.util import Finalize
from typing import Any, Literal

import google.api_core.exceptions
from google.cloud import bigquery, bigquery_storage
from google.cloud.bigquery import RangePartitioning, SchemaField, TimePartitioning
from typing_extensions import Self

from cloudly.biglist.parquet import ParquetBatchData
from cloudly.gcp.auth import get_credentials, get_project_id
from cloudly.util.datetime import utcnow

logger = logging.getLogger(__name__)


def get_client() -> bigquery.Client:
    # To execute a SQL statement that's not covered in this module,
    # use the `query` method of the returned client.
    client = bigquery.Client(credentials=get_credentials(), project=get_project_id())

    # This is what `client.close()` or `client.__exit__(...)` does.
    def close_client(http):
        http._auth_request.session.close()
        http.close()

    Finalize(client, close_client, args=(client._http,))
    return client


def get_storage_client() -> bigquery_storage.BigQueryReadClient:
    client = bigquery_storage.BigQueryReadClient(credentials=get_credentials())

    # `client.transport.close()` is what `client.__exit__(...)` does.
    Finalize(client, client.transport.close)

    return client


def list_datasets(project_id: str = None) -> list[str]:
    datasets = get_client().list_datasets(project=project_id or get_project_id())
    ids = sorted(d.dataset_id for d in datasets)
    return ids


def read_streams(stream_names: Iterable[str]) -> Iterator[ParquetBatchData]:
    # The "Storage API" is used to pull data at high speed and throughput from a large table by
    # concurrently pulling from "streams".
    # The stream names are obtained by `Table.create_streams`.
    # The stream names returned by `Table.create_streams` may be passed to multiple
    # workers (threads, processes, machines) for concurrent and/or distributed consumption.
    # This function shows how one worker can consume an iterable of stream names.
    # User may need to adapt this code with additional setup.
    client = get_storage_client()
    for stream_name in stream_names:
        logger.debug("pulling data from stream '%s'", stream_name)
        reader = client.read_rows(stream_name)
        npages = 0
        for page in reader.rows().pages:
            yield ParquetBatchData(page.to_arrow())
            npages += 1
        logger.debug("pulled data from stream '%s' with %d pages", stream_name, npages)


class Dataset:
    """
    We do not provide functions for dataset management (create, delete, etc)
    because it's unlikely that a project need dynamic datasets.
    For dataset management, just go to GCP dashboard.
    """

    def __init__(self, dataset_id: str, project_id: str = None):
        self.dataset_id = dataset_id
        self.project_id = project_id or get_project_id()

    @property
    def dataset(self) -> bigquery.Dataset:
        return get_client().get_dataset(
            bigquery.DatasetReference.from_string(self.dataset_id, self.project_id)
        )

    def list_tables(self) -> list[str]:
        tables = get_client().list_tables(self.dataset)
        return sorted(t.table_id for t in tables if t.table_type == 'TABLE')

    def list_external_tables(self) -> list[str]:
        tables = get_client().list_tables(self.dataset)
        return sorted(t.table_id for t in tables if t.table_type == 'EXTERNAL')

    def table(self, table_id: str) -> Table:
        return Table(
            table_id=table_id, dataset_id=self.dataset_id, project_id=self.project_id
        )

    def temp_table(self, *, prefix=None, postfix=None) -> Table:
        """
        It's recommended to create a dataset named 'tmp' in your account and configure
        an expiration time for its tables. Then use this dataset for temp tables.

        Each BQ query saves its results in a (temporary by default) table.
        You can put the results in a table under your control (instead of letting
        BQ decide), and then use the reading methods of the table to get the results,
        like this::

            sql = "..."
            table = Dataset('tmp').temp_table().load_from_query(sql)
            for row in table.read_rows():
                ...
        """
        t = self.table(_make_temp_name(prefix=prefix, postfix=postfix))
        t.drop_if_exists()
        return t

    def external_table(self, table_id: str) -> ExternalTable:
        return ExternalTable(
            table_id=table_id, dataset_id=self.dataset_id, project_id=self.project_id
        )


class _Table:
    """
    This class contains functionalities that are common to "native tables" and "external tables".
    """

    def __init__(self, table_id: str, dataset_id: str, project_id: str = None):
        self.table_id = table_id
        self.dataset_id = dataset_id
        self.project_id = project_id or get_project_id()

    @property
    def qualified_table_id(self) -> str:
        return f'{self.project_id}.{self.dataset_id}.{self.table_id}'

    def __repr__(self):
        return f"{self.__class__.__name__}('{self.qualified_table_id}')"

    def __str__(self):
        return self.__repr__()

    def __reduce__(self):
        return type(self), (self.table_id, self.dataset_id, self.project_id)

    @property
    def table(self) -> bigquery.Table:
        return get_client().get_table(self.qualified_table_id)

    def exists(self) -> bool:
        try:
            _ = self.table
            return True
        except google.api_core.exceptions.NotFound:
            return False

    def drop(self, *, not_found_ok: bool = False) -> Self:
        get_client().delete_table(self.qualified_table_id, not_found_ok=not_found_ok)
        # May raise `google.api_core.exceptions.NotFound`.
        return self

    def drop_if_exists(self) -> Self:
        self.drop(not_found_ok=True)
        return self

    def count_rows(self) -> int:
        sql = f'SELECT COUNT(*) FROM `{self.qualified_table_id}`'
        job = get_client().query(sql)
        return list(job.result())[0][0]

    def read_rows(
        self,
        *,
        as_dict: bool = False,
        selected_fields: Sequence[str] | None = None,
        max_results: int | None = None,
        start_index: int | None = None,
        page_size: int | None = None,
        **kwargs,
    ) -> Iterator:
        """
        This is used to read a small-ish table. For large tables, see :meth:`Table.create_streams`
        and related functions.

        Parameters
        ----------
        selected_fields
            Fields to return. If specified, and if `as_dict` is `False` (so that each row is returned as a tuple),
            the order of these fields is derived from the table schema. Their order in this input list is ignored.
        max_results
            Max number of rows to retrieve.
        start_index
            Used together, `start_index` and `max_results` specify the exact chunk of rows to retrieve.
        """
        if selected_fields:
            schema = self.table.schema
            # Ensure the selected columns are listed in the order as they appear
            # in the table schema.
            selected_fields = [s for s in schema if s.name in selected_fields]
        rows = get_client().list_rows(
            self.qualified_table_id,
            selected_fields=selected_fields,
            max_results=max_results,
            start_index=start_index,
            page_size=page_size,
            **kwargs,
        )
        if as_dict:
            for row in rows:
                yield dict(zip(row.keys(), row.values()))
        else:
            for row in rows:
                yield row.values()

    def extract_to_uri(
        self,
        dest_uris: str | Sequence[str],
        *,
        destination_format: str = 'PARQUET',
        compression: str = 'snappy',
        wait: bool = True,
        **kwargs,
    ):
        """
        Usually, you should specify a location in Google Cloud Storage that is empty (hence it acts like a "folder").
        You can specify individual blob name(s) like "gs://mybucket/myproject/myfolder/mydata.parquet" or specify a pattern
        like "gs://mybucket/myproject/myfolder/part-*.parquet".
        """
        if isinstance(dest_uris, str):
            dest_uris = [dest_uris]

        job_config = bigquery.ExtractJobConfig(
            destination_format=destination_format, compression=compression, **kwargs
        )
        job = get_client().extract_table(
            self.qualified_table_id,
            dest_uris,
            job_config=job_config,
        )
        return job.result() if wait else job


def _load_job_config(
    *,
    clustering_fields: Sequence[str] | None = None,
    range_partitioning: RangePartitioning | None = None,
    time_partitioning: TimePartitioning | None = None,
    write_disposition: Literal[
        'WRITE_APPEND', 'WRITE_TRUNCATE', 'WRITE_EMPTY'
    ] = 'WRITE_EMPTY',
    schema: Sequence[SchemaField] | None = None,
    **kwargs,
):
    """
    `write_disposition`: the default value "WRITE_EMPTY" means the table
    must be empty or non-existent. See `bigquery.enums.WriteDisposition` for details.
    """
    if 'source_format' in kwargs:
        kwargs['source_format'] = {
            'CSV': bigquery.SourceFormat.CSV,
            'PARQUET': bigquery.SourceFormat.PARQUET,
            'AVRO': bigquery.SourceFormat.AVRO,
            'ORC': bigquery.SourceFormat.ORC,
        }[kwargs['source_format']]

    return bigquery.LoadJobConfig(
        clustering_fields=clustering_fields,
        range_partitioning=range_partitioning,
        time_partitioning=time_partitioning,
        write_disposition=write_disposition,
        schema=schema,
        autodetect=(schema is None),
        **kwargs,
    )


def _query_job_config(
    *,
    destination: str,
    use_query_cache: bool = True,
    allow_large_results: bool = True,
    write_disposition: Literal[
        'WRITE_APPEND', 'WRITE_TRUNCATE', 'WRITE_EMPTY'
    ] = 'WRITE_EMPTY',
    **kwargs,
):
    return bigquery.QueryJobConfig(
        allow_large_results=allow_large_results,
        use_query_cache=use_query_cache,
        destination=destination,
        write_disposition=write_disposition,
        **kwargs,
    )


def _make_temp_name(prefix: str = None, postfix: str = None):
    name = utcnow().strftime('%Y%m%d-%H%M%S')
    if prefix:
        name = f"{prefix.strip(' -_')}-{name}"
    if postfix:
        name = f"{name}-{postfix.strip(' -_')}"
    return name


class Table(_Table):
    """
    A `Table` object is an in-memory representation. A corresponding table may or may not exist in BQ.

    The `load_*` methods by default will populate a new (or empty) table, typically auto-detecting the schema.
    There are options to make them append to an existing table.

    Some method require the table to not yet exist in BQ. For example, :meth:`create`.

    Some methods require the table to exist, for example, `insert_rows` and the data-reading methods.
    """

    def create(
        self,
        columns: Sequence[SchemaField | tuple[str, str] | tuple[str, str, str]],
        *,
        clustering_fields: Sequence[str] | None = None,
    ) -> Self:
        """
        Create the "physical" table in BQ.

        At this moment, `self` contains name of the table, but the table does not actually
        exist in BQ. If the table already exists, an exception will be raised.

        This method only supports simple table definition. For more flexibility,
        use `get_client().query(...)` with the raw SQL statement for table creation.

        If a table is deleted and then created again, it may not be accessible right away
        (getting `NotFound` error), because of "eventual consistency".

        Parameters
        ----------
        columns
            Column definitions. For any column that is defined by a tuple of strings,
            the most reliable form is two strings specifying "name" and "type".
            A third string would specify "mode" with acceptable values including
            "NULLABLE" (default), "REQUIRED", and "REPEATED". Any column spec with
            more than three strings would be hard to maintain.
        clustering
            This is a list of columns to cluster the table by.
            Clustering is analogous to indexing.
        """
        schema = [
            col if isinstance(col, SchemaField) else SchemaField(*col)
            for col in columns
        ]
        table = bigquery.Table(self.qualified_table_id, schema=schema)
        if clustering_fields:
            table.clustering_fields = clustering_fields
        get_client().create_table(table, exists_ok=False)
        # If the table exists, `google.api_core.exceptions.Conflict` will be raised.
        return self

    def load_from_query(self, sql: str, *, wait: bool = True, **kwargs):
        """
        Load the result of the query `sql` into the current table.
        """
        job_config = _query_job_config(destination=self.qualified_table_id, **kwargs)
        job = get_client().query(sql, job_config=job_config)
        return job.result() if wait else job

    def load_from_uri(
        self,
        uris: str | Sequence[str],
        *,
        wait: bool = True,
        source_format: Literal['CSV', 'PARQUET', 'AVRO', 'ORC'] = 'PARQUET',
        **kwargs,
    ):
        """
        Load the content of the data files into the current table.

        `uris` are data files in Google Cloud Storage, formatted like "gs://<bucket_name>/<object_name_or_glob>".
        It can contain patterns like "gs://mybucket/myproject/myfolder/*.parquet".
        """
        job_config = _load_job_config(source_format=source_format, **kwargs)
        job = get_client().load_table_from_uri(
            uris, destination=self.qualified_table_id, job_config=job_config
        )
        return job.result() if wait else job

    def load_from_json(
        self,
        data: Iterable[dict],
        *,
        wait: bool = True,
        **kwargs,
    ):
        """
        `data` is an iterable of dicts for rows.

        Actually, the `data` doesn't have much to do with "json".
        """
        job_config = _load_job_config(**kwargs)
        job = get_client().load_table_from_json(
            data, destination=self.qualified_table_id, job_config=job_config
        )
        return job.result() if wait else job

    def insert_rows(
        self, data: Iterable[tuple] | Iterable[dict], **kwargs
    ) -> list[dict[str, Any]]:
        """
        The table must physically exist in BQ.

        Return info about rows that had insertion error.
        If there was no error, an empty list is returned.
        """
        return get_client().insert_rows(self.table, data, **kwargs)

    def list_partitions(self) -> list[str]:
        # sql = f"""\
        #     SELECT partition_id
        #     FROM `{self.project_id}.{self.dataset_id}.INFORMATION_SCHEMA.PARTITIONS`
        #     WHERE table_name = '{self.table_id}'"""
        # zz = get_client().query(sql).result()
        # return [z[0] for z in zz]
        try:
            return sorted(get_client().list_partitions(self.qualified_table_id))
        except google.api_core.exceptions.BadRequest as e:
            if (
                'Cannot read partition information from a table that is not partitioned'
                in str(e)
            ):
                return None
            raise

    def create_streams(
        self,
        *,
        max_stream_count: int = 0,
        selected_fields: Sequence[str] | None = None,
        row_restriction: str | None = None,
    ) -> list[str]:
        """
        This is the first step in reading a large table using the "Storage API".
        The returned stream names can be passed to multiple workers
        (including distributed) for concurrent reading.

        The streams expire in 6 hours after creation.

        Parameters
        ----------
        selected_fields
            The names of the fields (i.e. columns) to be retrieved.
            If specified, these fields in the resultant stream schema
            do not respect their order in `selected_fields`. Rather,
            they follow their order in the table's schema.
        row_restriction
            This is a SQL expression that can appear in a WHERE clause, for example::

                "numerical_field BETWEEN 1.0 AND 2.0"
                "nullable_field is not NULL"

            Aggregates are not supported.
        max_stream_count
            Default 0 lets the BQ server decide. It appears to be 100 in experience.

        Returns
        -------
        list
            List of stream names to be used by reader functions.

        See :func:`read_streams`, :meth:`stream_read_rows`.
        """
        table = f'projects/{self.project_id}/datasets/{self.dataset_id}/tables/{self.table_id}'
        session = bigquery_storage.types.ReadSession()
        session.table = table
        session.data_format = bigquery_storage.types.DataFormat.ARROW
        if selected_fields:
            session.read_options.selected_fields = selected_fields
        if row_restriction:
            session.read_options.row_restriction = row_restriction

        client = get_storage_client()
        session = client.create_read_session(
            parent=f'projects/{self.project_id}',
            read_session=session,
            max_stream_count=max_stream_count,
        )
        return [s.name for s in session.streams]

    def stream_read_rows(
        self, *, as_dict: bool = False, num_workers: int = 2, **kwargs
    ) -> Iterator:
        """
        This method uses background threads to pull the table data using the "Storage API".
        The pulled row data are transferred to the current thread to be yielded, and consumed
        by the caller of this method. This is meant for high-throughput data pull of a large table.

        Note that the data are consumed in the current thread (by the caller of this method).
        If it is more suitable to consume the data concurrently in multiple threads or processes
        (or even distributed machines), this method is not the right solution. Instead, see
        :func:`read_streams`.
        """
        stream_names = self.create_streams(**kwargs)
        q = queue.SimpleQueue()
        for n in stream_names:
            q.put(n)
        q.put(None)

        def read(q_names, q_out, client):
            while True:
                stream_name = q_names.get()
                if stream_name is None:
                    q_names.put(None)
                    q_out.put(None)
                    break

                reader = client.read_rows(stream_name)
                for page in reader.rows().pages:
                    q_out.put(ParquetBatchData(page.to_arrow()))

        qq = queue.Queue(maxsize=1000)
        client = get_storage_client()

        workers = [
            threading.Thread(target=read, args=(q, qq, client))
            for _ in range(num_workers)
        ]
        for w in workers:
            w.start()

        k = 0
        while True:
            batch = qq.get()
            if batch is None:
                k += 1
                if k == num_workers:
                    break
                continue

            if as_dict:
                if batch.num_columns == 1:
                    colname = batch.column_names[0]
                    for x in batch:
                        yield {colname: x}
                else:
                    yield from batch
            else:
                if batch.num_columns == 1:
                    for x in batch:
                        yield (x,)
                else:
                    for row in batch:
                        yield tuple(row.values())


class ExternalTable(_Table):
    def create(
        self,
        source_uris: str | Sequence[str],
        *,
        source_format: Literal['AVRO', 'CSV', 'ORC', 'PARQUET'] = 'PARQUET',
        schema: Sequence[SchemaField] | None = None,
        options=None,
    ):
        """
        Parameters
        ----------
        source_uris
            Blobs in Google Cloud Storage, optionally containing '*' patterns, like "gs://mybucket/myproject/myfolder/*.parquet".
        options
            Additional options to go with `source_format`.

        For more flexible table definitions, use `get_client().query(...)` with raw SQL statements.
        """
        if isinstance(source_uris, str):
            source_uris = [source_uris]
        config = bigquery.ExternalTable(source_format)
        config.source_uris = source_uris
        config.autodetect = schema is None
        if schema is not None:
            config.schema = schema
        if options:
            if source_format == 'AVRO':
                config.avro_options = options
            elif source_format == 'CSV':
                config.csv_options = options
            elif source_format == 'PARQUET':
                config.parquet_options = options
            elif source_format == 'ORC':
                raise ValueError(
                    "the 'ORC' source format does not take additional options"
                )
            else:
                raise ValueError(
                    f"unknown value for `source_format`: '{source_format}'"
                )
        table = bigquery.Table(self.qualified_table_id, schema=schema)
        table.external_data_configuration = config
        get_client().create_table(table, exists_ok=False)
        # If the table exists, `google.api_core.exceptions.Conflict` will be raised.
        return self
