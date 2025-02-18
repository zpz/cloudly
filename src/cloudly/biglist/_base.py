from __future__ import annotations

import bisect
import concurrent.futures
import logging
import os
import queue
import string
import tempfile
import threading
import uuid
import weakref
from abc import abstractmethod
from collections.abc import Iterator

from cloudly.upathlib import LocalUpath, PathType, Upath, resolve_path
from cloudly.util.seq import Element, Seq
from cloudly.util.serializer import (
    AvroSerializer,
    CsvSerializer,
    NewlineDelimitedOrjsonSeriealizer,
    ParquetSerializer,
    PickleSerializer,
    Serializer,
    ZstdPickleSerializer,
)

from ._util import FileReader, FileSeq

logger = logging.getLogger(__name__)


_global_thread_pool_ = weakref.WeakValueDictionary()
_global_thread_pool_lock_ = threading.Lock()


def get_global_thread_pool():
    # Refer to ``get_shared_thread_pool`` in package ``mpservice.concurrent.futures``.

    with _global_thread_pool_lock_:
        executor = _global_thread_pool_.get('_biglist_')
        if executor is None or executor._shutdown:
            executor = concurrent.futures.ThreadPoolExecutor()
            _global_thread_pool_['_biglist_'] = executor
    return executor


if hasattr(os, 'register_at_fork'):  # this is not available on Windows

    def _clear_global_state():
        executor = _global_thread_pool_.get('_biglist_')
        if executor is not None:
            executor.shutdown(wait=False)
            _global_thread_pool_.pop('_biglist_', None)

        global _global_thread_pool_lock_
        try:
            _global_thread_pool_lock_.release()
        except RuntimeError:  # 'release unlocked lock'
            pass
        _global_thread_pool_lock_ = threading.Lock()

    os.register_at_fork(after_in_child=_clear_global_state)


class BiglistBase(Seq[Element]):
    """
    This base class contains code mainly concerning *reading*.
    The subclass :class:`~biglist.Biglist` adds functionalities for writing.
    Another subclass :class:`~biglist.ExternalBiglist` is read-only.
    Here, "reading" and "read-only" is talking about the *data files*.
    This class always needs to write meta info *about* the data files.
    In addition, the subclass :class:`~biglist.Biglist` also creates and manages
    the data files, whereas :class:`~biglist.ExternalBiglist` provides methods
    to read existing data files, treating them as read-only.

    This class is generic with a parameter indicating the type of the data items,
    but this is useful only for the subclass :class:`~biglist.Biglist`.
    For the subclass :class:`~biglist.ExternalBiglist`, this parameter is essentially ``Any``
    because the data items (or rows) in Parquet files are composite and flexible.
    """

    registered_storage_formats = {
        'avro': AvroSerializer,
        'pickle': PickleSerializer,
        'pickle-zstd': ZstdPickleSerializer,
        'parquet': ParquetSerializer,
        'csv': CsvSerializer,
        'newline-delimited-json': NewlineDelimitedOrjsonSeriealizer,
    }

    @classmethod
    def register_storage_format(
        cls,
        name: str,
        serializer: type[Serializer],
    ) -> None:
        """
        Register a new serializer to handle data file dumping and loading.

        This class has a few serializers registered out of the box.
        They should be adequate for most applications.

        Parameters
        ----------
        name
            Name of the format to be associated with the new serializer.

            After registering the new serializer with name "xyz", one can use
            ``storage_format='xyz'`` in calls to :meth:`new`.
            When reading the object back from persistence,
            make sure this registry is also in place so that the correct
            deserializer can be found.

        serializer
            A subclass of `cloudly.util.serializer.Serializer <https://github.com/zpz/cloudly/blob/main/src/cloudly/util/serializer.py>`_.

            Although this class needs to provide the ``Serializer`` API, it is possible to write data files in text mode.
            The registered 'csv' and 'newline-delimited-json' formats do that.
        """
        good = string.ascii_letters + string.digits + '-_'
        assert all(n in good for n in name)
        if name.replace('_', '-') in cls.registered_storage_formats:
            raise ValueError(f"serializer '{name}' is already registered")
        name = name.replace('_', '-')
        cls.registered_storage_formats[name] = serializer

    @classmethod
    def get_temp_path(cls) -> Upath:
        """
        If user does not specify ``path`` when calling :meth:`new` (in a subclass),
        this method is used to determine a temporary directory.

        This implementation returns a temporary location in the local file system.

        Subclasses may want to customize this if they prefer other ways
        to find temporary locations. For example, they may want
        to use a temporary location in a cloud storage.
        """
        path = LocalUpath(os.path.abspath(tempfile.gettempdir()), str(uuid.uuid4()))  # type: ignore
        return path  # type: ignore

    @classmethod
    def new(
        cls,
        path: PathType | None = None,
        *,
        init_info: dict = None,
        **kwargs,
    ) -> BiglistBase:
        """
        Create a new object of this class (of a subclass, to be precise) and then add data to it.

        Parameters
        ----------
        path
            A directory in which this :class:`BiglistBase` will save whatever it needs to save.

            The directory must be non-existent.
            It is not necessary to pre-create the parent directory of this path.

            This path can be either on local disk or in a cloud storage.

            If not specified, :meth:`BiglistBase.get_temp_path`
            is called to determine a temporary path.

            The subclass :class:`~biglist.Biglist` saves both data and meta-info in this path.
            The subclass :class:`~biglist.ExternalBiglist` saves meta-info only.

        init_info
            Initial info that should be written into the *info* file before ``__init__`` is called.
            This is in addition to whatever this method internally decides to write.

            The info file `info.json` is written before :meth:`__init__` is called.
            In :meth:`__init__`, this file is read into ``self.info``.

            This parameter can be used to write some high-level info that ``__init__``
            needs.

            If the info is not needed in ``__init__``, then user can always add it
            to ``self.info`` after the object has been instantiated, hence saving general info
            in ``info.json`` is not the intended use of this parameter.

            User rarely needs to use this parameter. It is mainly used by the internals
            of the method ``new`` of subclasses.
        **kwargs
            additional arguments are passed on to :meth:`__init__`.

        Notes
        -----
        A :class:`BiglistBase` object construction is in either of the two modes
        below:

        a) create a new :class:`BiglistBase` to store new data.

        b) create a :class:`BiglistBase` object pointing to storage of
           existing data, which was created by a previous call to :meth:`new`.

        In case (a), one has called :meth:`new`. In case (b), one has called
        ``BiglistBase(..)`` (i.e. :meth:`__init__`).

        Some settings are applicable only in mode (a), b/c in
        mode (b) they can't be changed and, if needed, should only
        use the value already set in mode (a).
        Such settings can be parameters to :meth:`new`
        but should not be parameters to :meth:`__init__`.
        Examples include ``storage_format`` and ``batch_size`` for the subclass :class:`~biglist.Biglist`.
        These settings typically should be taken care of in :meth:`new`,
        before and/or after the object has been created by a call to :meth:`__init__`
        within :meth:`new`.

        :meth:`__init__` should be defined in such a way that it works for
        both a barebone object that is created in this :meth:`new`, as well as a
        fleshed out object that already has data in persistence.

        Some settings may be applicable to an existing :class:`BiglistBase` object, e.g.,
        they control styles of display and not intrinsic attributes persisted along
        with the BiglistBase.
        Such settings should be parameters to :meth:`__init__` but not to :meth:`new`.
        If provided in a call to :meth:`new`, these parameters will be passed on to :meth:`__init__`.

        Subclass authors should keep these considerations in mind.
        """

        if not path:
            path = cls.get_temp_path()
        path = resolve_path(path)
        if path.is_dir():
            raise Exception(f'directory "{path}" already exists')
        if path.is_file():
            raise FileExistsError(path)
        (path / 'info.json').write_json(init_info or {}, overwrite=False)
        obj = cls(path, **kwargs)
        return obj

    def __init__(
        self,
        path: PathType,
    ):
        """
        Parameters
        ----------
        path
            Directory that contains files written by an instance
            of this class.
        """
        self.path: Upath = resolve_path(path)
        """Root directory of the storage space for this object."""

        self._read_buffer: Seq[Element] | None = None
        self._read_buffer_file_idx = None
        self._read_buffer_item_range: tuple[int, int] | None = None
        # `self._read_buffer` contains the content of the file
        # indicated by `self._read_buffer_file_idx`.

        self._thread_pool_ = None

        self.info: dict
        """Various meta info."""

        self._info_file = self.path / 'info.json'
        self.info = self._info_file.read_json()
        self._n_read_threads = 3

    def __repr__(self):
        return f"<{self.__class__.__name__} at '{self.path}' with {self.num_data_items} elements in {self.num_data_files} data file(s)>"

    def __str__(self):
        return self.__repr__()

    def __len__(self) -> int:
        """
        Number of data items in this biglist.

        This is an alias to :meth:`num_data_items`.
        """
        return self.num_data_items

    def __getstate__(self):
        return (self.path,)

    def __setstate__(self, data):
        self.__init__(data[0])

    def _get_thread_pool(self):
        if self._thread_pool_ is None:
            self._thread_pool_ = get_global_thread_pool()
        return self._thread_pool_

    def destroy(self, *, concurrent=True) -> None:
        self.path.rmrf(concurrent=concurrent)

    def __getitem__(self, idx: int) -> Element:
        """
        Access a data item by its index; negative index works as expected.
        """
        # This is not optimized for speed.
        # For better speed, use ``__iter__``.

        if not isinstance(idx, int):
            raise TypeError(
                f'{self.__class__.__name__} indices must be integers, not {type(idx).__name__}'
            )

        if idx >= 0 and self._read_buffer_file_idx is not None:
            n1, n2 = self._read_buffer_item_range  # type: ignore
            if n1 <= idx < n2:
                return self._read_buffer[idx - n1]  # type: ignore

        files = self.files
        if files:
            data_files_cumlength = [v[-1] for v in files.data_files_info]
            length = data_files_cumlength[-1]
            nfiles = len(files)
        else:
            data_files_cumlength = []
            length = 0
            nfiles = 0
        idx = range(length)[idx]

        if idx >= length:
            raise IndexError(idx)

        ifile0 = 0
        ifile1 = nfiles
        if self._read_buffer_file_idx is not None:
            n1, n2 = self._read_buffer_item_range  # type: ignore
            if idx < n1:
                ifile1 = self._read_buffer_file_idx  # pylint: disable=access-member-before-definition
            elif idx < n2:
                return self._read_buffer[idx - n1]  # type: ignore
            else:
                ifile0 = self._read_buffer_file_idx + 1  # pylint: disable=access-member-before-definition

        # Now find the data file that contains the target item.
        ifile = bisect.bisect_right(data_files_cumlength, idx, lo=ifile0, hi=ifile1)
        # `ifile`: index of data file that contains the target element.
        # `n`: total length before `ifile`.
        if ifile == 0:
            n = 0
        else:
            n = data_files_cumlength[ifile - 1]
        self._read_buffer_item_range = (n, data_files_cumlength[ifile])

        data = files[ifile]
        self._read_buffer_file_idx = ifile
        self._read_buffer = data
        return data[idx - n]

    def __iter__(self) -> Iterator[Element]:
        """
        Iterate over all the elements.

        When there are multiple data files, as the data in one file is being yielded,
        the next file(s) may be pre-loaded in background threads.
        For this reason, although the following is equivalent in the final result::

            for file in self.files:
                for item in file:
                    ... use item ...

        it could be less efficient than iterating over `self` directly, as in

        ::

            for item in self:
                ... use item ...
        """
        files = self.files
        if not files:
            return

        if len(files) == 1:
            z = files[0]
            z.load()
            yield from z
        else:
            ndatafiles = len(files)

            max_workers = min(self._n_read_threads, ndatafiles)
            tasks = queue.Queue(max_workers)
            executor = self._get_thread_pool()

            def _read_file(idx):
                z = files[idx]
                z.load()
                return z

            for i in range(max_workers):
                t = executor.submit(_read_file, i)
                tasks.put(t)
            nfiles_queued = max_workers

            for _ in range(ndatafiles):
                t = tasks.get()
                file_reader = t.result()

                # Before starting to yield data, take care of the
                # downloading queue to keep it busy.
                if nfiles_queued < ndatafiles:
                    # `nfiles_queued` is the index of the next file to download.
                    t = executor.submit(_read_file, nfiles_queued)
                    tasks.put(t)
                    nfiles_queued += 1

                yield from file_reader

    @property
    def num_data_files(self) -> int:
        return len(self.files)

    @property
    def num_data_items(self) -> int:
        # This assumes the current object is the only one
        # that may be appending to the biglist.
        # In other words, if the current object is one of
        # of a number of workers that are concurrently using
        # this biglist, then all the other workers are reading only.
        files = self.files
        if files:
            return files.num_data_items
        return 0

    @property
    @abstractmethod
    def files(self) -> FileSeq[FileReader[Element]]:
        raise NotImplementedError
