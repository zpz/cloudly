from __future__ import annotations

__all__ = [
    'ParquetFileReader',
    'ParquetBatchData',
    'read_parquet_file',
    'make_parquet_schema',
    'write_arrays_to_parquet',
    'write_pylist_to_parquet',
]


import itertools
import logging
from collections.abc import Iterable, Iterator, Sequence
from multiprocessing.util import Finalize

import pyarrow
from pyarrow.fs import FileSystem, GcsFileSystem
from pyarrow.parquet import FileMetaData, ParquetFile

from cloudly.upathlib import LocalUpath, PathType, Upath, resolve_path
from cloudly.util.serializer import make_parquet_schema

try:
    from cloudly.gcp.auth import get_credentials
except ImportError:
    pass
from cloudly.util.seq import Seq, locate_idx_in_chunked_seq

from ._util import FileReader

# If data is in Google Cloud Storage, `pyarrow.fs.GcsFileSystem` accepts "access_token"
# and "credential_token_expiration". These can be obtained via
# a "google.oauth2.service_account.Credentials" object, e.g.
#
#   cred = google.oauth2.service_account.Credentials.from_service_info(
#       info_json, scopes=['https://www.googleapis.com/auth/cloud-platform'])
# or
#   cred = google.auth.default(scopes=['https://www.googleapis.com/auth/cloud-platform'])
#
#   auth_req = google.auth.transport.requests.Request()
#   cred.refresh(auth_req)
#   # now `cred` has `token` and `expiry`; expiration appears to be in a few hours
#
#   gcs = pyarrow.fs.GcsFileSystem(access_token=cred.token, credential_token_expiration=cred.expiry)
#   pfile = pyarrow.parquet.ParquetFile(gcs.open_input_file('bucket-name/path/to/file.parquet'))


logger = logging.getLogger(__name__)


class ParquetFileReader(FileReader):
    @classmethod
    def get_gcsfs(cls, *, valid_for_seconds=600) -> GcsFileSystem:
        """
        Obtain a `pyarrow.fs.GcsFileSystem`_ object with credentials given so that
        the GCP default process of inferring credentials (which involves
        env vars and file reading etc) will not be triggered.

        This is provided under the (un-verified) assumption that the
        default credential inference process is a high overhead.
        """
        cred, renewed = get_credentials(
            valid_for_seconds=valid_for_seconds,
            return_state=True,
        )
        if renewed or getattr(cls, '_GCSFS', None) is None:
            fs = GcsFileSystem(
                access_token=cred.token,
                credential_token_expiration=cred.expiry,
            )  # This call takes non-trivial time.
            cls._GCSFS = fs
        return cls._GCSFS

    @classmethod
    def load_file(cls, path: Upath) -> ParquetFile:
        """
        This reads *meta* info and constructs a ``pyarrow.parquet.ParquetFile`` object.
        This does not load the entire file.
        See :meth:`load` for eager loading.

        Parameters
        ----------
        path
            Path of the file.
        """
        fs, pp = FileSystem.from_uri(str(path))

        if isinstance(fs, GcsFileSystem):
            try:
                ff = cls.get_gcsfs()
                file = ParquetFile(pp, filesystem=ff)
            except PermissionError as e:
                # This was observed in GCP.
                # I still don't understand why.
                # Assuming this is due to some tricky timing issue in the credential refresh,
                # give it another try.
                logger.error(f'{e}; retry once')
                try:
                    ff = cls.get_gcsfs()
                    file = ParquetFile(pp, filesystem=ff)
                except PermissionError as e:
                    logger.error(f'{e} on retry; fall back on pyarrow')
                    file = ParquetFile(pp, filesystem=fs)
        else:
            file = ParquetFile(pp, filesystem=fs)

        Finalize(file, file.reader.close)
        # NOTE: can not use
        #
        #   Finalize(file, file.close, kwargs={'force': True})
        #
        # because the instance method `file.close` can't be used as the callback---the
        # object `file` is no long available at that time.
        #
        # See https://github.com/apache/arrow/issues/35318
        return file

    def __init__(self, path: PathType):
        """
        Parameters
        ----------
        path
            Path of a Parquet file.
        """
        self.path: Upath = resolve_path(path)
        self._reset()

    def _reset(self):
        self._file: ParquetFile | None = None
        self._data: ParquetBatchData | None = None

        self._row_groups_num_rows = None
        self._row_groups_num_rows_cumsum = None
        self._row_groups: None | list[ParquetBatchData] = None

        self._column_names = None
        self._columns = {}
        self._getitem_last_row_group = None

        self._scalar_as_py = None
        self.scalar_as_py = True

    def __getstate__(self):
        return (self.path,)

    def __setstate__(self, data):
        self.path = data[0]
        self._reset()

    @property
    def scalar_as_py(self) -> bool:
        """
        ``scalar_as_py`` controls whether the values returned by :meth:`__getitem__`
        (or indirectly by :meth:`__iter__`) are converted from a `pyarrow.Scalar`_ type
        such as `pyarrow.lib.StringScalar`_ to a Python builtin type such as ``str``.

        This property can be toggled anytime to take effect until it is toggled again.

        :getter: Returns this property's value.
        :setter: Sets this property's value.
        """
        if self._scalar_as_py is None:
            self._scalar_as_py = True
        return self._scalar_as_py

    @scalar_as_py.setter
    def scalar_as_py(self, value: bool):
        self._scalar_as_py = bool(value)
        if self._data is not None:
            self._data.scalar_as_py = self._scalar_as_py
        if self._row_groups:
            for r in self._row_groups:
                if r is not None:
                    r.scalar_as_py = self._scalar_as_py

    def __len__(self) -> int:
        return self.num_rows

    def load(self) -> None:
        """Eagerly read the whole file into memory as a table."""
        if self._data is None:
            self._data = ParquetBatchData(
                self.file.read(columns=self._column_names, use_threads=True),
            )
            self._data.scalar_as_py = self.scalar_as_py
            if self.num_row_groups == 1:
                assert self._row_groups is None
                self._row_groups = [self._data]

    @property
    def file(self) -> ParquetFile:
        """Return a `pyarrow.parquet.ParquetFile`_ object.

        Upon initiation of a :class:`ParquetFileReader` object,
        the file is not read at all. When this property is requested,
        the file is accessed to construct a `pyarrow.parquet.ParquetFile`_ object.

        """
        if self._file is None:
            self._file = self.load_file(self.path)
        return self._file

    @property
    def metadata(self) -> FileMetaData:
        return self.file.metadata

    @property
    def num_rows(self) -> int:
        return self.metadata.num_rows

    @property
    def num_row_groups(self) -> int:
        return self.metadata.num_row_groups

    @property
    def num_columns(self) -> int:
        if self._column_names:
            return len(self._column_names)
        return self.metadata.num_columns

    @property
    def column_names(self) -> list[str]:
        if self._column_names:
            return self._column_names
        return self.metadata.schema.names

    def data(self) -> ParquetBatchData:
        """Return the entire data in the file."""
        self.load()
        return self._data

    def _locate_row_group_for_item(self, idx: int):
        # Assuming user is checking neighboring items,
        # then the requested item may be in the same row-group
        # as the item requested last time.
        if self._row_groups_num_rows is None:
            meta = self.metadata
            self._row_groups_num_rows = [
                meta.row_group(i).num_rows for i in range(self.num_row_groups)
            ]
            self._row_groups_num_rows_cumsum = list(
                itertools.accumulate(self._row_groups_num_rows)
            )

        igrp, idx_in_grp, group_info = locate_idx_in_chunked_seq(
            idx, self._row_groups_num_rows_cumsum, self._getitem_last_row_group
        )
        self._getitem_last_row_group = group_info
        return igrp, idx_in_grp

    def __getitem__(self, idx: int):
        """
        Get one row (or "record").

        If the object has a single column, then return its value in the specified row.
        If the object has multiple columns, return a dict with column names as keys.
        The values are converted to Python builtin types if :data:`scalar_as_py`
        is ``True``.

        Parameters
        ----------
        idx
            Row index in this file.
            Negative value counts from the end as expected.
        """
        if idx < 0:
            idx = self.num_rows + idx
        if idx < 0 or idx >= self.num_rows:
            raise IndexError(idx)

        if self._data is not None:
            return self._data[idx]

        igrp, idx_in_row_group = self._locate_row_group_for_item(idx)
        row_group = self.row_group(igrp)
        return row_group[idx_in_row_group]

    def __iter__(self):
        """
        Iterate over rows.
        The type of yielded individual elements is the same as the return of :meth:`__getitem__`.
        """
        if self._data is None:
            for batch in self.iter_batches():
                yield from batch
        else:
            yield from self._data

    def iter_batches(self, batch_size=10_000) -> Iterator[ParquetBatchData]:
        if self._data is None:
            for batch in self.file.iter_batches(
                batch_size=batch_size,
                columns=self._column_names,
                use_threads=True,
            ):
                z = ParquetBatchData(batch)
                z.scalar_as_py = self.scalar_as_py
                yield z
        else:
            for batch in self._data.data().to_batches(batch_size):
                z = ParquetBatchData(batch)
                z.scalar_as_py = self.scalar_as_py
                yield z

    def row_group(self, idx: int) -> ParquetBatchData:
        """
        Parameters
        ----------
        idx
            Index of the row group of interest.
        """
        assert 0 <= idx < self.num_row_groups
        if self._row_groups is None:
            self._row_groups = [None] * self.num_row_groups
        if self._row_groups[idx] is None:
            z = ParquetBatchData(
                self.file.read_row_group(idx, columns=self._column_names),
            )
            z.scalar_as_py = self.scalar_as_py
            self._row_groups[idx] = z
            if self.num_row_groups == 1:
                assert self._data is None
                self._data = self._row_groups[0]
        return self._row_groups[idx]

    def columns(self, cols: Sequence[str]) -> ParquetFileReader:
        """
        Return a new :class:`ParquetFileReader` object that will only load
        the specified columns.

        The columns of interest have to be within currently available columns.
        In other words, a series of calls to this method would incrementally
        narrow down the selection of columns. (Note this returns a new
        :class:`ParquetFileReader`, hence one can call :meth:`columns` again on the
        returned object.)

        This method "slices" the data by columns, in contrast to other
        data access methods that select rows.

        Parameters
        ----------
        cols
            Names of the columns to select.

        Examples
        --------
        >>> obj = ParquetFileReader('file_path')  # doctest: +SKIP
        >>> obj1 = obj.columns(['a', 'b', 'c'])  # doctest: +SKIP
        >>> print(obj1[2])  # doctest: +SKIP
        >>> obj2 = obj1.columns(['b', 'c'])  # doctest: +SKIP
        >>> print(obj2[3])  # doctest: +SKIP
        >>> obj3 = obj.columns(['d'])  # doctest: +SKIP
        >>> for v in obj:  # doctest: +SKIP
        >>>     print(v)  # doctest: +SKIP
        """
        assert len(set(cols)) == len(cols)  # no repeat values

        if self._column_names:
            if all(col in self._column_names for col in cols):
                if len(cols) == len(self._column_names):
                    return self
            else:
                cc = [col for col in cols if col not in self._column_names]
                raise ValueError(
                    f'cannot select the columns {cc} because they are not in existing set of columns'
                )

        obj = self.__class__(self.path)
        obj.scalar_as_py = self.scalar_as_py
        obj._file = self._file
        obj._row_groups_num_rows = self._row_groups_num_rows
        obj._row_groups_num_rows_cumsum = self._row_groups_num_rows_cumsum
        if self._row_groups:
            obj._row_groups = [
                None if v is None else v.columns(cols) for v in self._row_groups
            ]
        if self._data is not None:
            obj._data = self._data.columns(cols)
        # TODO: also carry over `self._columns`?
        obj._column_names = cols
        return obj

    def column(self, idx_or_name: int | str) -> pyarrow.ChunkedArray:
        """Select a single column.

        Note: while :meth:`columns` returns a new :class:`ParquetFileReader`,
        :meth:`column` returns a `pyarrow.ChunkedArray`_.
        """
        z = self._columns.get(idx_or_name)
        if z is not None:
            return z
        if self._data is not None:
            return self._data.column(idx_or_name)
        if isinstance(idx_or_name, int):
            idx = idx_or_name
            name = self.column_names[idx]
        else:
            name = idx_or_name
            idx = self.column_names.index(name)
        z = self.file.read(columns=[name]).column(name)
        self._columns[idx] = z
        self._columns[name] = z
        return z


class ParquetBatchData(Seq):
    """
    ``ParquetBatchData`` wraps a `pyarrow.Table`_ or `pyarrow.RecordBatch`_.
    The data is already in memory; this class does not involve file reading.

    :meth:`ParquetFileReader.data` and :meth:`ParquetFileReader.iter_batches` both
    return or yield ParquetBatchData.
    In addition, the method :meth:`columns` of this class returns a new object
    of this class.

    Objects of this class can be pickled.
    """

    def __init__(
        self,
        data: pyarrow.Table | pyarrow.RecordBatch,
    ):
        # `self.scalar_as_py` may be toggled anytime
        # and have its effect right away.
        self._data = data
        self.scalar_as_py = True
        """Indicate whether scalar values should be converted to Python types from `pyarrow`_ types."""
        self.row_as_dict = True
        """`__getitem__` and `__iter__` produce tuples if `row_as_dict` is `False`; otherwise dicts."""
        self.num_rows = data.num_rows
        self.num_columns = data.num_columns
        self.column_names = data.schema.names

    def __repr__(self):
        return '<{} with {} rows, {} columns>'.format(
            self.__class__.__name__,
            self.num_rows,
            self.num_columns,
        )

    def __str__(self):
        return self.__repr__()

    def data(self) -> pyarrow.Table | pyarrow.RecordBatch:
        """Return the underlying `pyarrow`_ data."""
        return self._data

    def __len__(self) -> int:
        return self.num_rows

    def __getitem__(self, idx: int):
        """
        Get one row (or "record").

        Return a tuple or a dict depending on the value of `self.row_as_dict`.
        The values are converted to Python builtin types if :data:`scalar_as_py`
        is ``True``.

        Parameters
        ----------
        idx
            Row index in this batch.
            Negative value counts from the end as expected.
        """
        if idx < 0:
            idx = self.num_rows + idx
        if idx < 0 or idx >= self.num_rows:
            raise IndexError(idx)

        if self.row_as_dict:
            z = {col: self._data.column(col)[idx] for col in self.column_names}
            if self.scalar_as_py:
                return {k: v.as_py() for k, v in z.items()}
            return z
        else:
            z = tuple(self._data.column(col)[idx] for col in self.column_names)
            if self.scalar_as_py:
                return tuple(v.as_py() for v in z)
            return z

    def __iter__(self):
        """
        Iterate over rows.
        The type of yielded individual elements is the same as :meth:`__getitem__`.
        """
        names = self.column_names
        if self.scalar_as_py:
            if self.row_as_dict:
                for row in zip(*self._data.columns):
                    yield dict(zip(names, (v.as_py() for v in row)))
            else:
                for row in zip(*self._data.columns):
                    yield tuple(v.as_py() for v in row)
        else:
            if self.row_as_dict:
                for row in zip(*self._data.columns):
                    yield dict(zip(names, row))
            else:
                for row in zip(*self._data.columns):
                    yield tuple(row)

    def columns(self, cols: Sequence[str]) -> ParquetBatchData:
        """
        Return a new :class:`ParquetBatchData` object that only contains
        the specified columns.

        The columns of interest have to be within currently available columns.
        In other words, a series of calls to this method would incrementally
        narrow down the selection of columns.
        (Note this returns a new :class:`ParquetBatchData`,
        hence one can call :meth:`columns` again on the returned object.)

        This method "slices" the data by columns, in contrast to other
        data access methods that select rows.

        Parameters
        ----------
        cols
            Names of the columns to select.

        Examples
        --------
        >>> obj = ParquetBatchData(parquet_table)  # doctest: +SKIP
        >>> obj1 = obj.columns(['a', 'b', 'c'])  # doctest: +SKIP
        >>> print(obj1[2])  # doctest: +SKIP
        >>> obj2 = obj1.columns(['b', 'c'])  # doctest: +SKIP
        >>> print(obj2[3])  # doctest: +SKIP
        >>> obj3 = obj.columns(['d'])  # doctest: +SKIP
        >>> for v in obj:  # doctest: +SKIP
        >>>     print(v)  # doctest: +SKIP
        """
        assert len(set(cols)) == len(cols)  # no repeat values

        if all(col in self.column_names for col in cols):
            if len(cols) == len(self.column_names):
                return self
        else:
            cc = [col for col in cols if col not in self.column_names]
            raise ValueError(
                f'cannot select the columns {cc} because they are not in existing set of columns'
            )

        z = self.__class__(self._data.select(cols))
        z.scalar_as_py = self.scalar_as_py
        return z

    def column(self, idx_or_name: int | str) -> pyarrow.Array | pyarrow.ChunkedArray:
        """
        Select a single column specified by name or index.

        If ``self._data`` is `pyarrow.Table`_, return `pyarrow.ChunkedArray`_.
        If ``self._data`` is `pyarrow.RecordBatch`_, return `pyarrow.Array`_.
        """
        return self._data.column(idx_or_name)


def read_parquet_file(path: PathType) -> ParquetFileReader:
    """
    Parameters
    ----------
    path
        Path of the file.
    """
    return ParquetFileReader(path)


def write_parquet_table(
    table: pyarrow.Table,
    path: PathType,
    **kwargs,
) -> None:
    """
    If the file already exists, it will be overwritten.

    Parameters
    ----------
    path
        Path of the file to create and write to.
    table
        pyarrow Table object.
    **kwargs
        Passed on to `pyarrow.parquet.write_table() <https://arrow.apache.org/docs/python/generated/pyarrow.parquet.write_table.html>`_.
    """
    path = resolve_path(path)
    if isinstance(path, LocalUpath):
        path.parent.path.mkdir(exist_ok=True, parents=True)
    ff, pp = FileSystem.from_uri(str(path))
    if isinstance(ff, GcsFileSystem):
        ff = ParquetFileReader.get_gcsfs()
    pyarrow.parquet.write_table(table, ff.open_output_stream(pp), **kwargs)


def write_arrays_to_parquet(
    data: Sequence[pyarrow.Array | pyarrow.ChunkedArray | Iterable],
    path: PathType,
    *,
    names: Sequence[str],
    **kwargs,
) -> None:
    """
    Parameters
    ----------
    path
        Path of the file to create and write to.
    data
        A list of data arrays.
    names
        List of names for the arrays in ``data``.
    **kwargs
        Passed on to `pyarrow.parquet.write_table() <https://arrow.apache.org/docs/python/generated/pyarrow.parquet.write_table.html>`_.
    """
    assert len(names) == len(data)
    arrays = [
        a if isinstance(a, (pyarrow.Array, pyarrow.ChunkedArray)) else pyarrow.array(a)
        for a in data
    ]
    table = pyarrow.Table.from_arrays(arrays, names=names)
    return write_parquet_table(table, path, **kwargs)


def write_pylist_to_parquet(
    data: Sequence,
    path: PathType,
    *,
    schema=None,
    schema_spec=None,
    metadata=None,
    **kwargs,
):
    if schema is not None:
        assert schema_spec is None
    elif schema_spec is not None:
        assert schema is None
        schema = make_parquet_schema(schema_spec)
    table = pyarrow.Table.from_pylist(data, schema=schema, metadata=metadata)
    return write_parquet_table(table, path, **kwargs)
