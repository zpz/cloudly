from __future__ import annotations

__all__ = [
    'get_client',
    'list_datasets',
    'dataset',
    'table',
    'query',
    'read',
    'read_to_table',
    'read_streams',
    'QueryJob',
    'SchemaField',
    'Dataset',
    'Table',
    'ExternalTable',
    'View',
    'ScalarFunction',
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
from cloudly.util.datetime import utcnow

from .auth import get_credentials, get_project_id
from .compute import validate_label_key, validate_label_value

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


def dataset(dataset_id: str, project_id: str | None = None) -> Dataset:
    """
    Usually you get to a "table" via a "dataset", for example,

    ::

        from cloudly.gcp import bigquery

        table = bigquery.dataset('tmp').table('abc').drop_if_exists().load_from_json(...)
    """
    return Dataset(dataset_id, project_id)


def table(table_id: str, dataset_id: str, project_id: str | None = None) -> Table:
    return Table(table_id=table_id, dataset_id=dataset_id, project_id=project_id)


class QueryJob:
    def __init__(self, job: bigquery.QueryJob | str):
        if not isinstance(job, str):
            self._job = job
            job = job.job_id
        self.job_id = job

    @property
    def job(self) -> bigquery.QueryJob:
        try:
            return get_client().get_job(self.job_id)
            # It may take some time for this to be available.
            # If we instantiated `QueryJob` using a just-created job (instead of a job-id),
            # we'll fall back to using that job object.
        except google.api_core.exceptions.NotFound as e:
            try:
                return self._job
            except AttributeError:
                raise e

    @property
    def table(self) -> Table:
        dest = self.job.destination
        return Table(
            dest.table_id,
            dest.dataset_id,
            dest.project,
        )

    @property
    def state(self) -> str:
        # 'RUNNING', 'DONE', etc
        return self.job.state

    def result(self, *, timeout: float | None = None) -> bigquery.RowIterator:
        # This will wait for the job to return result.
        # After it returns a `bigquery.RowIterator` but before the user has iterated over it,
        # simple tests showed `job.state` is 'DONE' and `job.done()` is `True`;
        # but I don't know whether that is always the case.
        job = self.job
        try:
            z = job.result(timeout=timeout)
            # Timeout will raise `concurrent.futures.TimeoutError`.
        except (
            google.api_core.exceptions.BadRequest,
            google.api_core.exceptions.NotFound,
        ):
            print()
            print(job.query)
            print()
            raise
        # A possible customization is to alert on high cost here.
        return z

    def done(self) -> bool:
        return self.job.done()


def query(
    sql: str, *, wait: bool = True, job_config: None | dict = None, **kwargs
) -> bigquery.RowIterator | QueryJob:
    """
    Some job config options you may want to know: `use_query_cache` (default `True`), `write_disposition`
    (`Literal['WRITE_APPEND', 'WRITE_TRUNCATE', 'WRITE_EMPTY']`; default 'WRITE_EMPTY').

    Using this function with all default parameters is suitable for queries
    that do not return very useful info, such as create/update/delete operations.
    """
    if isinstance(job_config, dict):
        job_config = bigquery.QueryJobConfig(**job_config)
    job = QueryJob(get_client().query(sql, job_config=job_config, **kwargs))
    if wait:
        return job.result()
    return job


def read_to_table(sql: str, *, wait: bool = True, **kwargs) -> Table:
    """
    This makes the temporary result table explicitly accessible.
    The temporary result table is available for up to 24 hours; see

        https://cloud.google.com/bigquery/docs/cached-results

    If you need more control, create a table and call `load_from_query`
    to populate it, like this::

        table = Dataset('tmp').temp_table().load_from_query(sql)
    """
    job = query(sql, wait=False, **kwargs)
    if wait:
        job.result()
    return job.table


def read(sql: str, *, as_dict: bool = False, **kwargs) -> Iterator:
    """
    This function executes a `SELECT ...` (or maybe other) statement and iterates over the result rows.

    In general, BQ creates a temporary table to host the result of `query`.
    The temporary table is in a temporary dataset (named with a leading underscore)
    in the user's account.

    If you need more control, you can get the table object by using :func:`read_to_table`.
    """
    rows = query(sql, wait=True, **kwargs)
    if as_dict:
        for row in rows:
            yield dict(zip(row.keys(), row.values()))
    else:
        for row in rows:
            yield row.values()


def read_streams(
    stream_names: Iterable[str],
    *,
    num_workers: int = 2,
    as_dict: bool = False,
    queue_cls=queue.Queue,
) -> Iterator[tuple] | Iterator[dict]:
    """
    The "Storage API" is used to pull data at high speed and throughput from a large table by
    concurrently pulling from "streams".
    The stream names are obtained by `Table.create_streams`.
    The stream names returned by `Table.create_streams` may be passed to multiple
    workers (threads, processes, machines) for concurrent and/or distributed consumption.
    This function shows how one worker can consume an iterable of stream names.
    User may need to adapt this code with additional setup.

    The parameter `queue_cls` is provided to allow customization that facilitates early-stopping
    of the worker threads. One particular possibility in mind is `ResponsiveQueue` in the package `mpservice`.
    """

    def enqueue(names, q):
        for n in names:
            q.put(n)
        q.put(None)

    def read_stream(qin, qout, client):
        while True:
            name = qin.get()
            if name is None:
                qin.put(None)
                break
            logger.debug("pulling data from stream '%s'", name)
            reader = client.read_rows(name)
            npages = 0
            for page in reader.rows().pages:
                batch = ParquetBatchData(page.to_arrow())
                qout.put(batch)
                npages += 1
            logger.debug("pulled data from stream '%s' with %d pages", name, npages)
        qout.put(None)

    qin = queue_cls(maxsize=1)
    qout = queue_cls(maxsize=100)

    workers = []
    workers.append(
        threading.Thread(
            target=enqueue,
            args=(stream_names, qin),
            name='thread-read-BQ-streams-0',
        )
    )

    client = get_storage_client()
    for idx in range(num_workers):
        workers.append(
            threading.Thread(
                target=read_stream,
                args=(qin, qout, client),
                name=f'thread-read-BQ-streams-{idx + 1}',
            )
        )

    for w in workers:
        w.start()

    nnone = 0
    while True:
        batch = qout.get()
        if batch is None:
            nnone += 1
            if nnone == num_workers:
                break
            else:
                continue
        batch.row_as_dict = as_dict
        yield from batch

    for w in workers:
        w.join()


class Dataset:
    """
    We do not provide functions for dataset management (create, delete, etc)
    because it's unlikely that a project needs dynamic datasets.
    For dataset management, just go to GCP dashboard.
    """

    @staticmethod
    def make_temp_table_name(prefix: str = None, postfix: str = None) -> str:
        name = utcnow().strftime('%Y%m%d_%H%M%S')
        if prefix:
            name = f'{prefix.strip(" -_").replace(" ", "_").replace("-", "_")}_{name}'
        if postfix:
            name = f'{name}_{postfix.strip(" -_").replace(" ", "_").replace("-", "_")}'
        return name

    def __init__(self, dataset_id: str, project_id: str = None):
        self.dataset_id = dataset_id
        self.project_id = project_id or get_project_id()

    @property
    def qualified_dataset_id(self) -> str:
        return f'{self.project_id}.{self.dataset_id}'

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}('{self.qualified_dataset_id}')"

    def __str__(self) -> str:
        return self.__repr__()

    def __reduce__(self):
        return type(self), (self.dataset_id, self.project_id)

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

    def list_views(self) -> list[str]:
        tables = get_client().list_tables(self.dataset)
        return sorted(
            t.table_id for t in tables if t.table_type in ('VIEW', 'MATERIALIZED_VIEW')
        )

    def list_scalar_functions(self) -> list[str]:
        routines = get_client().list_routines(self.dataset_id)
        return sorted(r.routine_id for r in routines if r.type_ == 'SCALAR_FUNCTION')

    def table(self, table_id: str) -> Table:
        return Table(
            table_id=table_id, dataset_id=self.dataset_id, project_id=self.project_id
        )

    def temp_table(self, *, prefix=None, postfix=None) -> Table:
        """
        It's recommended to create a dataset named 'tmp' in your account and configure
        an expiration time for its tables. Then use this dataset for temp tables.

        Each BQ query saves its results in a temporary table.
        You can put the results in a table under your control (instead of letting
        BQ decide), and then use the reading methods of the table to get the results,
        like this::

            sql = "..."
            table = Dataset('tmp').temp_table().load_from_query(sql)
            for row in table.read_rows():
                ...
        """
        t = self.table(self.make_temp_table_name(prefix=prefix, postfix=postfix))
        t.drop_if_exists()
        return t

    def external_table(self, table_id: str) -> ExternalTable:
        return ExternalTable(
            table_id=table_id, dataset_id=self.dataset_id, project_id=self.project_id
        )

    def view(self, view_id: str) -> View:
        return View(view_id, self.dataset_id, self.project_id)

    def scalar_function(self, routine_id: str) -> ScalarFunction:
        return ScalarFunction(routine_id, self.dataset_id, self.project_id)


class _Table:
    """
    This class contains functionalities that are common to all types of BQ tables, namely,
    "regular" tables, external tables, and views.
    """

    def __init__(self, table_id: str, dataset_id: str, project_id: str = None):
        self.table_id = table_id
        self.dataset_id = dataset_id
        self.project_id = project_id or get_project_id()

    @property
    def qualified_table_id(self) -> str:
        """
        When using this table ID in SQL statements, you often want to enclose that
        between backticks (i.e. f"`{table.qualified_table_id}`").
        """
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
        # This is accurate but may be expensive.
        sql = f'SELECT COUNT(*) FROM `{self.qualified_table_id}`'
        return list(read(sql))[0][0]

    @property
    def labels(self) -> dict[str, str]:
        return self.table.labels

    def update_labels(self, kv: dict[str, str]) -> None:
        """
        Add label entries.

        If a value is `None`, the named label is removed if it exists.

        Labels that are not listed in `kv` are left intact.

        TODO: test on this method failed on a View with `PreconditionFaield`.
        """
        tab = self.table
        labels = tab.labels
        for k, v in kv.items():
            validate_label_key(k)
            if v is not None:
                validate_label_value(v)
            labels[k] = v
        tab.labels = labels
        get_client().update_table(tab, ['labels'])


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
    return bigquery.LoadJobConfig(
        clustering_fields=clustering_fields,
        range_partitioning=range_partitioning,
        time_partitioning=time_partitioning,
        write_disposition=write_disposition,
        schema=schema,
        autodetect=(schema is None),
        **kwargs,
    )


class Table(_Table):
    """
    A `Table` object is an in-memory representation. A corresponding table may or may not exist in BQ.

    The `load_*` methods by default will populate a new (or empty) table, typically auto-detecting the schema.
    There are options to make them append to an existing table.

    Some methods require the table to not yet exist in BQ. For example, :meth:`create`.

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
        use `cloudly.gcp.bigquery.query(...)` with the raw SQL statement for table creation.

        If a table is deleted and then created again, it may not be accessible right away
        (getting `NotFound` error), due to "eventual consistency".

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

    def load_from_query(
        self, sql: str, *, job_config: None | dict = None, wait: bool = True, **kwargs
    ):
        """
        Load the result of the query `sql` into the current table.
        """
        z = query(
            sql,
            job_config={'destination': self.qualified_table_id, **(job_config or {})},
            wait=wait,
            **kwargs,
        )
        if wait:
            return self
        return z

    def load_from_uri(
        self,
        uris: str | Sequence[str],
        source_format: Literal['CSV', 'PARQUET', 'AVRO', 'ORC'] = 'PARQUET',
        *,
        job_config: dict | None = None,
        wait: bool = True,
        **kwargs,
    ):
        """
        Load the content of the data files into the current table.

        `uris` are data files in Google Cloud Storage, formatted like "gs://<bucket_name>/<object_name_or_glob>".
        It can contain patterns like "gs://mybucket/myproject/myfolder/*.parquet".

        The source data storage is not required to be in the same region as the current table.
        """
        source_format = source_format.upper()
        source_format = {
            'CSV': bigquery.SourceFormat.CSV,
            'PARQUET': bigquery.SourceFormat.PARQUET,
            'AVRO': bigquery.SourceFormat.AVRO,
            'ORC': bigquery.SourceFormat.ORC,
        }[source_format]

        job_config = _load_job_config(source_format=source_format, **(job_config or {}))
        job = get_client().load_table_from_uri(
            uris,
            destination=self.qualified_table_id,
            job_config=job_config,
            **kwargs,
        )
        if wait:
            job.result()
            return self
        return job

    def load_from_json(
        self,
        data: Iterable[dict],
        *,
        wait: bool = True,
        job_config: dict | None = None,
        **kwargs,
    ):
        """
        `data` is an iterable of dicts for rows.

        Actually, the `data` doesn't have much to do with "json".
        """
        job_config = _load_job_config(**(job_config or {}))
        job = get_client().load_table_from_json(
            data,
            destination=self.qualified_table_id,
            job_config=job_config,
            **kwargs,
        )
        if wait:
            job.result()
            return self
        return job

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
        # zz = read(sql)
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
        self,
        *,
        as_dict: bool = False,
        num_workers: int = 2,
        max_stream_count=0,
        selected_fields=None,
        row_restriction=None,
    ) -> Iterator:
        """
        This method uses background threads to pull the table data using the "Storage API".
        The pulled row data are transferred to the current thread to be yielded, and consumed
        by the caller of this method. This is meant for high-throughput data pull of a large table.

        Note that this is the sole consumer of the whole table: data pull happens within this
        single function (although it uses multiple threads); pulled data are consumed by the caller
        of this function (although the caller could put the pulled data in a queue and let it
        be consumed by other threads or processes).

        For more flexibility, you can call :meth:`create_streams` to get the stream names, then
        in a second step use the stream names in a way of your choosing, for example, the stream
        names may be distributed to multiple nodes and used in parallel. See :func:`read_streams`.
        """
        stream_names = self.create_streams(
            max_stream_count=max_stream_count,
            selected_fields=selected_fields,
            row_restriction=row_restriction,
        )
        yield from read_streams(stream_names, num_workers=num_workers, as_dict=as_dict)

    def extract_to_uri(
        self,
        dest_uris: str | Sequence[str],
        destination_format: str = 'PARQUET',
        *,
        job_config: dict | None = None,
        wait: bool = True,
        **kwargs,
    ):
        """
        Usually, you should specify a location in Google Cloud Storage that is empty (hence it acts like a "folder").
        You can specify individual blob name(s) like "gs://mybucket/myproject/myfolder/mydata.parquet" or specify a pattern
        like "gs://mybucket/myproject/myfolder/part-*.parquet".

        The destination storage is not required to be in the same region as the source table.
        """
        if isinstance(dest_uris, str):
            dest_uris = [dest_uris]

        job_config = bigquery.ExtractJobConfig(
            destination_format=destination_format.upper(),
            **(job_config or {}),
        )
        job = get_client().extract_table(
            self.qualified_table_id,
            dest_uris,
            job_config=job_config,
            **kwargs,
        )
        return job.result() if wait else job


class ExternalTable(_Table):
    def create(
        self,
        source_uris: str | Sequence[str],
        source_format: Literal['AVRO', 'CSV', 'ORC', 'PARQUET'] = 'PARQUET',
        *,
        columns: Sequence[SchemaField | tuple[str, str] | tuple[str, str, str]] = None,
        options=None,
    ) -> Self:
        """
        Parameters
        ----------
        source_uris
            Blobs in Google Cloud Storage, optionally containing '*' patterns, like "gs://mybucket/myproject/myfolder/*.parquet".
        options
            Additional options to go with `source_format`.

        The source data and the created external table need to reside in the same "region" (like "us-west1").

        For more flexible table definitions, use `query(...)` with raw SQL statements.
        """
        source_format = source_format.upper()
        if isinstance(source_uris, str):
            source_uris = [source_uris]
        config = bigquery.ExternalConfig(source_format)
        config.source_uris = source_uris
        if columns:
            schema = [
                col if isinstance(col, SchemaField) else SchemaField(*col)
                for col in columns
            ]
            config.schema = schema
            config.autodetect = False
        else:
            schema = None
            config.autodetect = True

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

    def read_rows(
        self,
        *,
        as_dict: bool = False,
    ) -> Iterator:
        # This method is likely not suitable if the result set is huge.
        sql = f'SELECT * FROM `{self.qualified_table_id}`'
        yield from read(sql, as_dict=as_dict)


class View(_Table):
    """
    Views are read-only.

    You can't use Storage API to read a view. The workaround would be to execute a query on the view,
    save the result in a (temporary) table, then read the table.
    """

    def __init__(self, view_id: str, dataset_id: str, projec_id: str = None):
        super().__init__(view_id, dataset_id, projec_id)

    def create(self, sql: str, *, materialized: bool = False) -> Self:
        """
        `sql` is a "SELECT ..." statement that defines the view.

        If you create a materialized view, BQ will start populating it right away,
        which may continue for some time even after this function returns.
        """
        view = bigquery.Table(self.qualified_view_id)
        if materialized:
            view.mview_query = sql
        else:
            view.view_query = sql
        get_client().create_table(view, exists_ok=False)
        # If the view exists, `google.api_core.exceptions.Conflict` will be raised.
        return self

    @property
    def qualified_view_id(self) -> str:
        return self.qualified_table_id

    @property
    def view_id(self) -> str:
        return self.table_id

    @property
    def view(self) -> bigquery.Table:
        return self.table
        # `self.view.table_type` is either 'VIEW' or 'MATERIALIZED_VIEW'.

    def read_rows(
        self,
        *,
        as_dict: bool = False,
    ) -> Iterator:
        # This method is likely not suitable if the result set is huge.
        sql = f'SELECT * FROM `{self.qualified_view_id}`'
        yield from read(sql, as_dict=as_dict)


class _Routine:
    """
    Common functionalities for all types of BQ routines, namely "scalar functions"
    and "table functions".
    """

    @classmethod
    def routine_data_type(
        cls, data_type: str, element_type: str | None = None
    ) -> bigquery.StandardSqlDataType:
        """
        `data_type`, like "STRING", "FLOAT16". See `bigquery.enums.StandardSqlTypeNames`.
        `element_type`: data_type of elements when `data_type` is `ARRAY`.
        """
        type_kind = getattr(bigquery.StandardSqlTypeNames, data_type.upper())
        if element_type:
            assert data_type.upper() == 'ARRAY'
            array_element_type = cls.routine_data_type(element_type)
            return bigquery.StandardSqlDataType(
                type_kind, array_element_type=array_element_type
            )
        else:
            assert data_type.upper() != 'ARRAY'
            return bigquery.StandardSqlDataType(type_kind)

    @classmethod
    def routine_input_argument(
        cls, name: str, data_type: str, *, kind: str = 'FIXED_TYPE', **kwargs
    ):
        return bigquery.RoutineArgument(
            name=name,
            data_type=cls.routine_data_type(data_type, **kwargs),
            kind=kind,
        )

    def __init__(self, routine_id: str, dataset_id: str, project_id: str | None = None):
        self.routine_id = routine_id
        self.dataset_id = dataset_id
        self.project_id = project_id or get_project_id()

    @property
    def qualified_routine_id(self) -> str:
        return f'{self.project_id}.{self.dataset_id}.{self.routine_id}'

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}('{self.qualified_routine_id}')"

    def __str__(self) -> str:
        return self.__repr__()

    def __reduce__(self):
        return type(self), (self.routine_id, self.dataset_id, self.project_id)

    @property
    def routine(self) -> bigquery.Routine:
        return get_client().get_routine(self.qualified_routine_id)

    def exists(self) -> bool:
        try:
            _ = self.routine
            return True
        except google.api_core.exceptions.NotFound:
            return False

    def drop(self, *, not_found_ok: bool = False) -> Self:
        get_client().delete_routine(
            self.qualified_routine_id, not_found_ok=not_found_ok
        )
        # May raise `google.api_core.exceptions.NotFound`.
        return self

    def drop_if_exists(self) -> Self:
        self.drop(not_found_ok=True)
        return self


class ScalarFunction(_Routine):
    """
    This is commonly referred to as "user-defined function" or UDF.

    A main motivation for this class is the use case where the UDF code
    is assembled in Python code for consistency with other pars of the code,
    for example, the JS function code may plug in certain constants.
    """

    def create(
        self,
        *,
        arguments: Sequence[bigquery.RoutineArgument],
        imported_libraries: Sequence[str] | None = None,
        body: str,
        return_type: str,
        language: Literal['SQL', 'JAVASCRIPT', 'JS'],
        description: str | None = None,
    ) -> Self:
        """
        Parameters
        ----------
        arguments
            Each argument may be created by :meth:`_Routine.routine_input_argument`.

            If the routine takes no arg, pass in an empty list.
        body
            The Javascript function code as a string.
        return_type
            If this is not provided, the return type will be inferred at query time.
        imported_libraries
            A list of str paths to external libraries stored in Google Cloud Storage, e.g.,
            `['gs://bq_js_udfs/lib1.js', 'gs://bq_js_udfs/lib2.js']`.
        """
        language = language.upper()
        if language == 'JS':
            language = 'JAVASCRIPT'
        routine = bigquery.routine.Routine(
            self.qualified_routine_id,
            type_=bigquery.routine.RoutineType.SCALAR_FUNCTION,
            language=language.upper(),
            arguments=arguments,
            imported_libraries=imported_libraries,
            return_type=self.routine_data_type(return_type),
            body=body,
            description=description,
        )
        get_client().create_routine(routine, exists_ok=False)
        # If the routine exists, will raise `Conflict`.
        return self
