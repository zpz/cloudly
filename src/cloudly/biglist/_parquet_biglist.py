from __future__ import annotations

import itertools
import logging
from collections.abc import Sequence

from cloudly.upathlib import PathType, Upath, resolve_path

from ._base import BiglistBase, get_global_thread_pool
from ._util import (
    FileSeq,
)
from .parquet import (
    ParquetFileReader,
)

logger = logging.getLogger(__name__)


class ParquetBiglist(BiglistBase):
    """
    ``ParquetBiglist`` defines a kind of "external biglist", that is,
    it points to pre-existing Parquet files and provides facilities to read them.

    As long as you use a ParquetBiglist object to read, it is assumed that
    the dataset (all the data files) have not changed since the object was created
    by :meth:`new`.
    """

    @classmethod
    def new(
        cls,
        data_path: PathType | Sequence[PathType],
        path: PathType | None = None,
        *,
        suffix: str = '.parquet',
        **kwargs,
    ) -> ParquetBiglist:
        """
        This classmethod gathers info of the specified data files and
        saves the info to facilitate reading the data files.
        The data files remain "external" to the :class:`ParquetBiglist` object;
        the "data" persisted and managed by the ParquetBiglist object
        are the meta info about the Parquet data files.

        If the number of data files is small, it's feasible to create a temporary
        object of this class (by leaving ``path`` at the default value ``None``)
        "on-the-fly" for one-time use.

        Parameters
        ----------
        path
            Passed on to :meth:`BiglistBase.new` of :class:`BiglistBase`.
        data_path
            Parquet file(s) or folder(s) containing Parquet files.

            If this is a single path, then it's either a Parquet file or a directory.
            If this is a list, each element is either a Parquet file or a directory;
            there can be a mix of files and directories.
            Directories are traversed recursively for Parquet files.
            The paths can be local, or in the cloud, or a mix of both.

            Once the info of all Parquet files are gathered,
            their order is fixed as far as this :class:`ParquetBiglist` is concerned.
            The data sequence represented by this ParquetBiglist follows this
            order of the files. The order is determined as follows:

                The order of the entries in ``data_path`` is preserved; if any entry is a
                directory, the files therein (recursively) are sorted by the string
                value of each file's full path.

        suffix
            Only files with this suffix will be included.
            To include all files, use ``suffix='*'``.

        **kwargs
            additional arguments are passed on to :meth:`__init__`.
        """
        if isinstance(data_path, PathType):
            data_path = [resolve_path(data_path)]
        else:
            data_path = [resolve_path(p) for p in data_path]

        def get_file_meta(p: Upath):
            ff = ParquetFileReader.load_file(p)
            meta = ff.metadata
            return {
                'path': str(p),  # str of full path
                'num_rows': meta.num_rows,
                # "row_groups_num_rows": [
                #     meta.row_group(k).num_rows for k in range(meta.num_row_groups)
                # ],
            }

        pool = get_global_thread_pool()
        tasks = []
        for p in data_path:
            if p.is_file():
                if suffix == '*' or p.name.endswith(suffix):
                    tasks.append(pool.submit(get_file_meta, p))
            else:
                tt = []
                for pp in p.riterdir():
                    if suffix == '*' or pp.name.endswith(suffix):
                        tt.append((str(pp), pool.submit(get_file_meta, pp)))
                tt.sort()
                for p, t in tt:
                    tasks.append(t)

        assert tasks
        datafiles = []
        for k, t in enumerate(tasks):
            datafiles.append(t.result())
            if (k + 1) % 1000 == 0:
                logger.info('processed %d files', k + 1)

        datafiles_cumlength = list(
            itertools.accumulate(v['num_rows'] for v in datafiles)
        )

        obj = super().new(path, **kwargs)  # type: ignore
        obj.info['datapath'] = [str(p) for p in data_path]

        # Removed in 0.7.4
        # obj.info["datafiles"] = datafiles
        # obj.info["datafiles_cumlength"] = datafiles_cumlength

        # Added in 0.7.4
        data_files_info = [
            (a['path'], a['num_rows'], b)
            for a, b in zip(datafiles, datafiles_cumlength)
        ]
        obj.info['data_files_info'] = data_files_info

        obj.info['storage_format'] = 'parquet'
        obj.info['storage_version'] = 1
        # `storage_version` is a flag for certain breaking changes in the implementation,
        # such that certain parts of the code (mainly concerning I/O) need to
        # branch into different treatments according to the version.
        # This has little relation to `storage_format`.
        # version 1 designator introduced in version 0.7.4.
        # prior to 0.7.4 it is absent, and considered 0.

        obj._info_file.write_json(obj.info, overwrite=True)

        return obj

    def __init__(self, *args, **kwargs):
        """Please see doc of the base class."""
        super().__init__(*args, **kwargs)

        # For back compat. Added in 0.7.4.
        if self.info and 'data_files_info' not in self.info:
            # This is not called by ``new``, instead is opening an existing dataset
            assert self.storage_version == 0
            data_files_info = [
                (a['path'], a['num_rows'], b)
                for a, b in zip(
                    self.info['datafiles'], self.info['datafiles_cumlength']
                )
            ]
            self.info['data_files_info'] = data_files_info
            with self._info_file.lock() as ff:
                ff.write_json(self.info, overwrite=True)

    def __repr__(self):
        return f"<{self.__class__.__name__} at '{self.path}' with {len(self)} records in {len(self.files)} data file(s) stored at {self.info['datapath']}>"

    @property
    def storage_version(self) -> int:
        return self.info.get('storage_version', 0)

    @property
    def files(self):
        # This method should be cheap to call.
        return ParquetFileSeq(
            self.path,
            self.info['data_files_info'],
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
