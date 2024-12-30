from collections.abc import Sequence
from typing import Literal
import time

from google.cloud import bigquery, bigquery_storage
import google.api_core.exceptions
from google.cloud.bigquery import RangePartitioning, TimePartitioning, SchemaField

from cloudly.gcp.auth import get_credentials, get_project_id


def get_client() -> bigquery.Client:
    return bigquery.Client(credentials=get_credentials(), project=get_project_id())


def get_storage_client() -> bigquery_storage.BigQueryReadClient:
    # Use the returned object in a context manager. Do not share the object across threads.
    # TODO: is this non-sharing rule obsolete?
    return bigquery_storage.BigQueryReadClient(credentials=get_credentials())


def list_dataset_ids() -> list[str]:
    datasets = get_client().list_datasets()
    ids = sorted(d.dataset_id for d in datasets)
    return ids


def wait_on_job(job_id: str, *, sleep_seconds: float | None = None):
    client = get_client()
    while True:
        job = client.get_job(job_id)
        if job.state != 'RUNNING':
            break
        time.sleep(sleep_seconds or 1.0)
    job.result()  # will raise error if any
    return job


class Dataset:
    def __init__(self, dataset_id: str):
        self.dataset_id = dataset_id

    def list_table_ids(self) -> list[str]:
        tables = get_client().list_tables(self.dataset_id)
        return sorted(
            t.table_id for t in tables if t.table_type=='TABLE'
        )
    
    def list_external_table_ids(self) -> list[str]:
        tables = get_client().list_tables(self.dataset_id)
        return sorted(
            t.table_id for t in tables if t.table_type=='EXTERNAL'
        )
    

class _Table:
    """
    This class contains functionalities that are common to "native tables" and "external tables".
    """
    def __init__(self, table_id: str, dataset_id: str):
        self.table_id = table_id
        self.dataset_id = dataset_id
        self.project_id = get_project_id()
        self._table: bigquery.Table = None

    @property
    def qualified_table_id(self) -> str:
        return f"{self.project_id}.{self.dataset_id}.{self.table_id}"

    def drop(self):
        get_client().delete_table(self.qualified_table_id, not_found_ok=False)

    def drop_if_exists(self):
        try:
            self.drop()
        except google.api_core.exceptions.NotFound:
            pass

    def count_rows(self) -> int:
        sql = f"SELECT COUNT(*) FROM `{self.qualified_table_id}`"
        job = get_client().query(sql)
        return list(job.result())[0][0]


def _load_job_config(*,
        clustering_fields: Sequence[str] | None = None,
        range_partitioning: RangePartitioning | None = None,
        time_partitioning: TimePartitioning | None = None,
        write_disposition: Literal['WRITE_APPEND', 'WRITE_TRUNCATE', 'WRITE_EMPTY'] = 'WRITE_EMPTY',
        **kwargs,
):
    if 'source_format' in kwargs:
        assert kwargs['source_format'] in ('CSV', 'PARQUET', 'AVRO', 'ORC')
        kwargs['source_format'] = getattr(bigquery.SourceFormat, kwargs['source_format'])

    return bigquery.LoadJobConfig(
        clustering_fields=clustering_fields,
        range_partitioning=range_partitioning,
        time_partitioning=time_partitioning,
        write_disposition=write_disposition,
        **kwargs,
    )


def _query_job_config(*, 
        destination: str,
        use_query_cache: bool = True,
        allow_large_results: bool = True,
        write_disposition: Literal['WRITE_APPEND', 'WRITE_TRUNCATE', 'WRITE_EMPTY'] = 'WRITE_EMPTY',
        **kwargs,
):
    return bigquery.QueryJobConfig(
        allow_large_results=allow_large_results,
        use_query_cache=use_query_cache,
        destination=destination,
        write_disposition=write_disposition,
        **kwargs)


class Table(_Table):
    def create(self, schema: Sequence[SchemaField], *, clustering_fields: Sequence[str] | None = None,
               range_partitioning: RangePartitioning | None = None,
               time_partitioning: TimePartitioning | None = None):
        table = bigquery.Table(self.qualified_table_id, schema=schema)
        table.clustering_fields = clustering_fields
        table.range_partitioning = range_partitioning
        table.time_partitioning = time_partitioning
        self._table = get_client().create_table(table, exists_ok=False)
        return self
    
    def import_from_query(self, sql: str, *, wait_sleep_seconds=None):
        job_config = _query_job_config(destination=self.qualified_table_id)
        job = get_client().query(sql, job_config=job_config)
        wait_on_job(job.job_id, sleep_seconds=wait_sleep_seconds)
        return self

    def import_from_uri(self, uris: str | Sequence[str], *, source_format: Literal['CSV', 'PARQUET', 'AVRO', 'ORC'], wait_sleep_seconds=None):
        """
        `uris` are data files in Google Cloud Storage, formatted like "gs://<bucket_name>/<object_name_or_glob>".
        """
        job_config = _load_job_config(autodetect=True, source_format=source_format)
        job = get_client().load_table_from_uri(uris, destination=self.qualified_table_id, job_config=job_config)
        wait_on_job(job.job_id, sleep_seconds=wait_sleep_seconds)
        return self


class ExternalTable(_Table):
    pass


