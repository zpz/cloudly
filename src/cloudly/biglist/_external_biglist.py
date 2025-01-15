from __future__ import annotations

import functools
import itertools
import logging
from collections.abc import Sequence

from cloudly.upathlib import PathType, Upath, resolve_path

from ._base import BiglistBase, get_global_thread_pool
from ._util import (
    BiglistFileSeq,
    FileSeq,
)
from .parquet import (
    ParquetFileReader,
)

logger = logging.getLogger(__name__)


class ExternalBiglist(BiglistBase):
    """
    A `ExternalBiglist` provides read functionalities for existing files
    that are not created by `biglist`.

    As long as you use an `ExternalBiglist` object to read, it is assumed that
    the dataset (all the data files) have not changed since the `ExternalBiglist`
    object was created by :meth:`new`.
    """

    @classmethod
    def get_file_meta(cls, p: Upath, storage_format: str) -> dict:
        # If a custom storage format supports getting `num_rows` w/o loading up
        # the data file, define a subclass to customize this method.
        if storage_format == 'parquet':
            ff = ParquetFileReader.load_file(p)
            meta = ff.metadata
            return {
                'path': str(p),  # str of full path
                'num_rows': meta.num_rows,
                # "row_groups_num_rows": [
                #     meta.row_group(k).num_rows for k in range(meta.num_row_groups)
                # ],
            }
        return {'path': str(p)}

    @classmethod
    def new(
        cls,
        data_path: PathType | Sequence[PathType],
        path: PathType | None = None,
        *,
        storage_format: str | None = None,
        deserialize_kwargs: dict | None = None,
        datafile_ext: str | None = None,
        **kwargs,
    ) -> ExternalBiglist:
        """
        This classmethod gathers info of the specified data files and
        saves the info to facilitate reading the data files.
        The data files remain "external" to the :class:`ExternalBiglist` object;
        the "data" persisted and managed by the `ExternalBiglist` object
        are the meta info about the data files.

        If the number of data files is small, it's feasible to create a temporary
        object of this class (by leaving ``path`` at the default value ``None``)
        "on-the-fly" for one-time use.

        Parameters
        ----------
        data_path
            File(s) or folder(s) containing data files.

            If this is a single path, then it's either a file or a directory.
            If this is a list, each element is either a file or a directory;
            there can be a mix of files and directories.
            Directories are traversed recursively for data files.
            The paths can be local, or in the cloud (specified by `cloudly.upathlib.BlobUpath`),
            or a mix of both.

            Once the info of all data files are gathered,
            their order is fixed as far as this :class:`ExternalBiglist` is concerned.
            The data sequence represented by this ExternalBiglist follows this
            order of the data files. The order is determined as follows:

                The order of the entries in ``data_path`` is preserved; if any entry is a
                directory, the files therein (recursively) are sorted by the string
                value of each file's full path.
        path
            Passed on to :meth:`BiglistBase.new` of :class:`BiglistBase`.
            This is the location where the `ExternalBiglist` object stores info to facilitate
            it's reading functions.
        datafile_ext
            Only files with this extension will be included. The default `None` would
            include all files.

            This is simply the trailing part of the file name. No '.' (dot) is appended.
            So if you mean ".parquet", then pass in ".parquet" rather than "parquet".
        deserialize_kwargs
            Additional keyword arguments to the deserialization function.

            This is rarely needed.
        **kwargs
            additional arguments are passed on to :meth:`__init__`.
        """
        if isinstance(data_path, PathType):
            data_path = [resolve_path(data_path)]
        else:
            data_path = [resolve_path(p) for p in data_path]

        if storage_format.replace('_', '-') not in cls.registered_storage_formats:
            raise ValueError(f"invalid value of `storage_format`: '{storage_format}'")

        pool = get_global_thread_pool()
        tasks = []
        for p in data_path:
            if p.is_file():
                if not datafile_ext or p.name.endswith(datafile_ext):
                    tasks.append(pool.submit(cls.get_file_meta, p, storage_format))
            else:
                tt = []
                for pp in p.riterdir():
                    if not datafile_ext or pp.name.endswith(datafile_ext):
                        tt.append(
                            (
                                str(pp),
                                pool.submit(cls.get_file_meta, pp, storage_format),
                            )
                        )
                tt.sort()
                for p, t in tt:
                    tasks.append(t)

        assert tasks
        datafiles = []
        for k, t in enumerate(tasks):
            datafiles.append(t.result())
            if (k + 1) % 1000 == 0:
                logger.info('processed %d files', k + 1)

        if 'num_rows' in datafiles[0]:
            # In this case, the ExternalBiglist object supports
            # `__getitem__` by index because it knows how many elements
            # each data file contains.
            datafiles_cumlength = list(
                itertools.accumulate(v['num_rows'] for v in datafiles)
            )
            data_files_info = [
                (a['path'], a['num_rows'], b)
                for a, b in zip(datafiles, datafiles_cumlength)
            ]
        else:
            # In this case, the ExternalBiglist object does not support
            # `__getitem__` by index.
            data_files_info = [(a['path'],) for a in datafiles]

        init_info = {
            'datapath': [str(p) for p in data_path],
            'data_files_info': data_files_info,
            'storage_format': storage_format,
            'storage_version': 1,
            # `storage_version` is a flag for certain breaking changes in the implementation,
            # such that certain parts of the code (mainly concerning I/O) need to
            # branch into different treatments according to the version.
            # This has little relation to `storage_format`.
            # version 1 designator introduced in version 0.7.4.
        }
        if deserialize_kwargs:
            init_info['deserialize_kwargs'] = deserialize_kwargs

        obj = super().new(path, init_info=init_info, **kwargs)  # type: ignore

        return obj

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.support_indexing = len(self.info['data_files_info'][0]) > 1
        self._deserialize_kwargs = self.info.get('deserialize_kwargs', {})

    def __repr__(self):
        if self.support_indexing:
            return f"<{self.__class__.__name__} at '{self.path}' with {len(self)} records in {len(self.files)} data file(s) stored at {self.info['datapath']}>"
        return f"<{self.__class__.__name__} at '{self.path}' in {len(self.files)} data file(s) stored at {self.info['datapath']}>"

    def __getitem__(self, idx):
        if self.support_indexing:
            return super().__getitem__(idx)
        raise RuntimeError(
            f"the storage format '{self.storage_format}' does not support indexing"
        )

    @property
    def storage_format(self) -> str:
        return self.info['storage_format'].replace('_', '-')

    @property
    def storage_version(self) -> int:
        return self.info.get('storage_version', 0)

    @property
    def files(self):
        # This method should be cheap to call.
        if self.storage_format == 'parquet':
            return ParquetFileSeq(
                self.path,
                self.info['data_files_info'],
            )

        serde = self.registered_storage_formats[self.storage_format]
        fun = serde.load
        if self._deserialize_kwargs:
            fun = functools.partial(fun, **self._deserialize_kwargs)
        return BiglistFileSeq(
            self.path,
            [
                (str(self.data_path / row[0]), *row[1:])
                for row in self.info['data_files_info']
            ],
            fun,
        )


class ParquetFileSeq(FileSeq[ParquetFileReader]):
    def __init__(
        self,
        root_dir: Upath,
        data_files_info: list[tuple[str, int, int]],
    ):
        """
        Parameters
        ----------
        root_dir
            Root directory for storage of meta info.
        data_files_info
            A list of data files that constitute the file sequence.
            Each tuple in the list is comprised of a file path (relative to
            ``root_dir``), number of data items in the file, and cumulative
            number of data items in the files up to the one at hand.
            Therefore, the order of the files in the list is significant.
        """
        self._root_dir = root_dir
        self._data_files_info = data_files_info

    @property
    def path(self):
        return self._root_dir

    @property
    def data_files_info(self):
        return self._data_files_info

    def __getitem__(self, idx: int):
        return ParquetFileReader(
            self._data_files_info[idx][0],
        )
