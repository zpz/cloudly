"""
:class:`Upath` is an abstract base class that defines the APIs and some of the implementation.
Subclasses tailor to particular storage systems.
Currently there are two production-ready subclasses; they implement ``Upath``
for local file systems and Google Cloud Storage, respectively.

The APIs follow the style of the standard library
`pathlib <https://docs.python.org/3/library/pathlib.html>`_ where appropriate.
"""

from __future__ import annotations

# Enable using `Upath` in type annotations in the code
# that defines this class.
# https://stackoverflow.com/a/49872353
# Will no longer be needed in Python 3.10.
import abc
import contextlib
import datetime
import functools
import os
import os.path
import pathlib
import queue
import sys
import threading
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from io import BufferedReader, UnsupportedOperation
from typing import (
    Any,
    Callable,
    Union,
)

from tqdm.auto import tqdm
from typing_extensions import Self

from ._util import MAX_THREADS, get_shared_thread_pool
from .serializer import (
    JsonSerializer,
    PickleSerializer,
    ZstdPickleSerializer,
)

# End user may want to do this:
#  logging.getLogger('urllib3.connectionpool').setLevel(logging.ERROR)
# to suppress the "urllib3 connection lost" warning.


class LockAcquireError(TimeoutError):
    pass


class LockReleaseError(RuntimeError):
    pass


@dataclass
class FileInfo:
    ctime: float  #: Creation time as a POSIX timestamp.
    mtime: float  #: Last modification time as a POSIX timestamp.
    time_created: datetime.datetime  #: Creation time as an ``datetime`` object.
    time_modified: (
        datetime.datetime
    )  #: Last modification time as an ``datetime`` object.
    size: int  #: In bytes.
    details: Any  #: Platform-dependent.


@functools.total_ordering
class Upath(abc.ABC):
    def __init__(
        self,
        *pathsegments: str,
    ):
        """
        Create a ``Upath`` instance. Because ``Upath`` is an abstract class,
        this is always called on a subclass to instantiate a path on the specific
        storage system.

        Subclasses for cloud blob stores may need to add additional parameters
        representing, e.g., container/bucket name, etc.

        Parameters
        ----------
        *pathsegments
            Analogous to the input to `pathlib.Path <https://docs.python.org/3/library/pathlib.html#pathlib.Path>`_.
            The first segment may or may not start with ``'/'``.
            The path constructed with ``*pathsegments`` is always "absolute" under a known "root".

            For a local POSIX file system, the root is the usual ``'/'``.

            For a local Windows file system, the root is resolved to a particular "drive".

            For Azure blob store, the root is that in a "container".

            For AWS and GCP blob stores, the root is that in a "bucket".

            If missing, the path constructed is the "root". However,
            the subclass :class:`LocalUpath` plugs in the current working directory
            for a missing ``*pathsegments``.

            .. note:: If one segment starts with ``'/'``, it will reset to the "root"
                and discard all the segments that have come before it. For example,
                ``Upath('work', 'projects', '/', 'projects')``
                is the same as ``Upath('/', 'projects)``.

            .. note:: The first element of ``*pathsegments`` may start with some platform-specific
                strings. For example, ``'/'`` on Linux, ``'c://'`` on Windows, ``'gs://'`` on
                Google Cloud Storage. Please see subclasses for specifics.
        """

        self._path = os.path.normpath(os.path.join('/', *pathsegments))  # pylint: disable=no-value-for-parameter
        # For LocalUpath on Windows, this is like 'C:\\Users\\username\\path'.
        # For LocalUpath on Linux, and BlobUpath, this is always absolute starting with '/'.
        # It does not have a trailing `/` unless the path is just `/` itself.
        self._thread_pools = {}

    def __getstate__(self):
        # Do not pickle `self._thread_pools`.
        return (self._path,)

    def __setstate__(self, data):
        self._path = data[0]
        self._thread_pools = {}

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}('{self._path}')"
        # Subclass may want to customize this method to add more info,
        # e.g. "bucket" name.

    def __str__(self) -> str:
        return self._path

    def __eq__(self, other) -> bool:
        if other.__class__ is not self.__class__:
            return NotImplemented
        return self.as_uri() == other.as_uri()

    def __lt__(self, other) -> bool:
        if other.__class__ is not self.__class__:
            return NotImplemented
        return self.as_uri() < other.as_uri()

    def __hash__(self) -> int:
        return hash(self.as_uri())

    def __truediv__(self, key: str) -> Self:
        """
        This method is invoked by ``self / key``.
        This calls the method :meth:`joinpath`.
        """
        return self.joinpath(key)

    def _run_in_executor(
        self,
        tasks: Iterable[tuple[Callable, tuple, dict, str]],
        quiet: bool,
    ):
        """
        This method is used to run multiple I/O jobs concurrently, e.g.
        uploading/downloading all files in a folder recursively.

        Parameters
        ----------
        tasks
            Each element is a tuple of (func, args, kwargs, description).
        """
        if not isinstance(tasks, list):
            tasks = list(tasks)
        n_tasks = len(tasks)
        if not n_tasks:
            return

        pbar = None
        executor = get_shared_thread_pool('upathlib', MAX_THREADS)

        if not quiet:
            pbar = tqdm(
                total=n_tasks,
                bar_format='{percentage:5.1f}%, {n:.0f}/{total_fmt}, {elapsed} | {desc}',
            )

        def enqueue(tasks, executor, q, to_stop):
            for func, args, kwargs, desc in tasks:
                t = executor.submit(func, *args, **kwargs)
                # This has limited capacity, to control the speed of
                # task submission to the executor.
                q.put((t, desc))
                if to_stop.is_set():
                    break
            q.put(None)

        try:
            q = queue.Queue(executor._max_workers)
            to_stop = threading.Event()
            task = executor.submit(enqueue, tasks, executor, q, to_stop)

            try:
                while True:
                    z = q.get()
                    if z is None:
                        break
                    t, desc = z
                    if pbar:
                        pbar.set_description_str(desc)
                        pbar.update(0.5)
                    try:
                        yield t.result()
                    except Exception:
                        to_stop.set()
                        while True:
                            z = q.get()
                            if z is None:
                                break
                            t, desc = z
                            t.cancel()
                            # This may not succeed, but there isn't a good way to
                            # guarantee cancellation here.
                        raise
                    if pbar:
                        pbar.update(0.5)
            finally:
                _ = task.result()
        finally:
            if pbar:
                pbar.close()

    @property
    def path(self) -> pathlib.PurePath:
        """
        The `pathlib.PurePath <https://docs.python.org/3/library/pathlib.html#pathlib.PurePath>`_
        version of the internal path string.

        In the subclass :class:`LocalUpath`, this property is overridden to return a
        `pathlib.Path <https://docs.python.org/3/library/pathlib.html#pathlib.Path>`_,
        which is a subclass of
        `pathlib.PurePath <https://docs.python.org/3/library/pathlib.html#pathlib.PurePath>`_.

        In subclasses for cloud blob stores, this implementation stays in effect.
        """
        return pathlib.PurePath(self._path)

    @abc.abstractmethod
    def as_uri(self) -> str:
        """
        Represent the path as a file URI.
        See subclasses for platform-dependent specifics.
        """
        raise NotImplementedError

    @property
    def name(self) -> str:
        """
        A string representing the final path component, excluding the drive and root, if any.

        This is the ``name`` component of ``self.path``.
        If ``self.path`` is ``'/'`` (the root), then an empty string is returned.
        (The name of the root path is empty.)

        Examples
        --------
        >>> from cloudly.upathlib import LocalUpath
        >>> p = LocalUpath('/tmp/test/upathlib/data/sales.txt.gz')
        >>> p.path
        PosixPath('/tmp/test/upathlib/data/sales.txt.gz')
        >>> p.name
        'sales.txt.gz'
        >>> p.parent.parent.parent.parent
        LocalUpath('/tmp')
        >>> p.parent.parent.parent.parent.name
        'tmp'
        >>> p.parent.parent.parent.parent.parent
        LocalUpath('/')
        >>> p.parent.parent.parent.parent.parent.name
        ''
        >>> # the parent of root is still root:
        >>> p.parent.parent.parent.parent.parent.parent
        LocalUpath('/')
        """
        return self.path.name

    @property
    def stem(self) -> str:
        """
        The final path component, without its suffix.

        This is the "stem" part of ``self.name``.

        Examples
        --------
        >>> from cloudly.upathlib import LocalUpath
        >>> p = LocalUpath('/tmp/test/upathlib/data/sales.txt')
        >>> p
        LocalUpath('/tmp/test/upathlib/data/sales.txt')
        >>> p.path
        PosixPath('/tmp/test/upathlib/data/sales.txt')
        >>> p.name
        'sales.txt'
        >>> p.stem
        'sales'
        >>> p = LocalUpath('/tmp/test/upathlib/data/sales.txt.gz')
        >>> p.stem
        'sales.txt'
        """
        return self.path.stem

    @property
    def suffix(self) -> str:
        """
        The file extension of the final component, if any
        """
        return self.path.suffix

    @property
    def suffixes(self) -> list[str]:
        """
        A list of the path's file extensions.

        Examples
        --------
        >>> p = LocalUpath('/tmp/test/upathlib/data/sales.txt')
        >>> p.suffix
        '.txt'
        >>> p.suffixes
        ['.txt']
        >>> p = LocalUpath('/tmp/test/upathlib/data/sales.txt.gz')
        >>> p.suffix
        '.gz'
        >>> p.suffixes
        ['.txt', '.gz']
        """
        return self.path.suffixes

    def exists(self) -> bool:
        """Return ``True`` if the path is an existing file or dir;
        ``False`` otherwise.

        Examples
        --------
        In a blob store with blobs

        ::

            /a/b/cd
            /a/b/cd/e.txt

        ``'/a/b/cd'`` exists, and is both a file and a dir;
        ``'/a/b/cd/e.txt'`` exists, and is a file;
        ``'/a/b'`` exists, and is a dir;
        ``'/a/b/c'`` does not exist.
        """
        return self.is_file() or self.is_dir()

    @abc.abstractmethod
    def is_dir(self) -> bool:
        """Return ``True`` if the path is an existing directory; ``False`` otherwise.

        If there exists a file named like

        ::

            /a/b/c/d.txt

        we say ``'/a'``, ``'/a/b'``, ``'/a/b/c'`` are existing directories.

        In a cloud blob store, there's no such thing as an
        "empty directory", because there is no concept of "directory".
        A blob store just consists of files (aka blobs) with names,
        which could contain the letter '/', with no special meaning
        attached to it.
        We interpret the name ``'/a/b'`` as a directory
        to emulate the familiar concept in a local file system when
        there exist files named ``'/a/b/*'``.

        In a local file system, there can be empty directories.
        However, it is recommended to not have empty directories.

        There is no method for "creating an empty dir" (like the Linux command ``mkdir``).
        Simply create a file under the dir, and the dir will come into being.
        This is analogous to we create files all the time---we don't "create" an empty file
        in advance; we simply write to the would-be path of the file to be created.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def is_file(self) -> bool:
        """Return ``True`` if the path is an existing file; ``False`` otherwise.

        In a cloud blob store, a path can be both a file and a dir. For
        example, if these blobs exist::

            /a/b/c/d.txt
            /a/b/c

        we say ``/a/b/c`` is a "file", and also a "dir".
        User is recommended to avoid such namings.

        This situation does not happen in a local file system.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def file_info(self) -> FileInfo | None:
        """
        If :meth:`is_file` is ``False``, return ``None``; otherwise, return file info.
        """
        raise NotImplementedError

    @property
    def parent(self) -> Self:
        """
        The parent of the path.

        If the path is the root, then the parent is still the root.
        """
        return self._with_path(str(self.path.parent))

    @property
    @abc.abstractmethod
    def root(self) -> Self:
        """
        Return a new path representing the root.
        """
        raise NotImplementedError

    def _with_path(self, *paths) -> Self:
        """
        Return a new object of the same class at the specified ``*paths``.
        The new path is unrelated to the current path; in other words,
        the new path is not "relative" to the current path.

        The main use case is with a cloud blob store.
        For example, return a new path in the same store with the same
        account and bucket info.
        """
        # TODO: the implementation is a little hacky.
        r = self.root
        r._path = os.path.normpath(os.path.join('/', *paths))
        return r

    def joinpath(self, *other: str) -> Self:
        """Join this path with more segments, return the new path object.

        Calling this method is equivalent to combining the path with each
        of the ``other`` arguments in turn.

        If ``self`` was created by ``Upath(*segs)``, then this method essentially
        returns ``Upath(*segs, *other)``.

        If ``*other`` is a single string, there is a shortcut by the operator
        ``/``, implemented by :meth:`__truediv__`.
        """
        return self._with_path(os.path.join(self._path, *other))

    def with_name(self, name: str) -> Self:
        """
        Return a new path the the "name" part substituted by the new value.
        If the original path doesn't have a name (i.e. the original path is the root),
        ``ValueError`` is raised.

        Examples
        --------
        >>> p = LocalUpath('/tmp/test/upathlib/data/sales.txt.gz')
        >>> p.with_name('sales.data')
        LocalUpath('/tmp/test/upathlib/data/sales.data')
        """
        return self._with_path(str(self.path.with_name(name)))

    def with_stem(self, stem: str) -> Self:
        return self._with_path(str(self.path.with_stem(stem)))

    def with_suffix(self, suffix: str) -> Self:
        """
        Return a new path with the suffix replaced by the specified value.
        If the original path doesn't have a suffix, the new suffix is appended instead.
        If ``suffix`` is an empty string, the original suffix is removed.

        ``suffix`` should include a dot, like ``'.txt'``.

        Examples
        --------
        >>> p = LocalUpath('/tmp/test/upathlib/data/sales.txt.gz')
        >>>
        >>> # replace the last suffix:
        >>> p.with_suffix('.data')
        LocalUpath('/tmp/test/upathlib/data/sales.txt.data')
        >>>
        >>> # remove the last suffix:
        >>> p.with_suffix('')
        LocalUpath('/tmp/test/upathlib/data/sales.txt')
        >>>
        >>> p.with_suffix('').with_suffix('.bin')
        LocalUpath('/tmp/test/upathlib/data/sales.bin')
        >>>
        >>> pp = p.with_suffix('').with_suffix('')
        >>> pp
        LocalUpath('/tmp/test/upathlib/data/sales')
        >>>
        >>> # no suffix to remove:
        >>> pp.with_suffix('')
        LocalUpath('/tmp/test/upathlib/data/sales')
        >>>
        >>> # add a suffix:
        >>> pp.with_suffix('.pickle')
        LocalUpath('/tmp/test/upathlib/data/sales.pickle')
        """
        return self._with_path(str(self.path.with_suffix(suffix)))

    @abc.abstractmethod
    def write_bytes(
        self, data: bytes | BufferedReader, *, overwrite: bool = False
    ) -> None:
        """Write bytes ``data`` to the current file.

        Parent "directories" are created as needed, if applicable.

        If ``overwrite`` is ``False`` and the current file exists, ``FileExistsError`` is raised.

        ``data`` is either "byte-like" (such as bytes, bytearray, memoryview) or "file-like"
        open in "binary" mode. In the second case, the file should be positioned at the beginning
        (such as by calling ``.seek(0)``.)
        """
        raise NotImplementedError

    @abc.abstractmethod
    def read_bytes(self) -> bytes:
        """
        Return the binary contents of the pointed-to file as a bytes object.

        If ``self`` is not a file or does not exist,
        ``FileNotFoundError`` is raised.
        """
        raise NotImplementedError

    def write_text(
        self,
        data: str,
        *,
        overwrite: bool = False,
        encoding: str | None = None,
        errors: str | None = None,
    ) -> None:
        """Write text ``data`` to the current file.

        Parent "directories" are created as needed, if applicable.

        If ``overwrite`` is ``False`` and the current file exists, ``FileExistsError`` is raised.

        ``encoding`` and ``errors`` are passed to `encode() <https://docs.python.org/3/library/stdtypes.html#str.encode>`_.
        Usually you should leave them at the default values.
        """
        z = data.encode(encoding=encoding or 'utf-8', errors=errors or 'strict')
        self.write_bytes(z, overwrite=overwrite)

    def read_text(
        self, *, encoding: str | None = None, errors: str | None = None
    ) -> str:
        """
        Return the decoded contents of the pointed-to file as a string.

        If ``self`` is not a file or does not exist,
        ``FileNotFoundError`` is raised.

        ``encoding`` and ``errors`` are passed to `decode() <https://docs.python.org/3/library/stdtypes.html#bytes.decode>`_.
        Usually you should leave them at the default values.
        """
        # Refer to https://docs.python.org/3/library/functions.html#open
        # and https://docs.python.org/3/library/codecs.html#module-codecs
        return self.read_bytes().decode(
            encoding=encoding or 'utf-8', errors=errors or 'strict'
        )

    def write_json(self, data: Any, *, overwrite=False, **kwargs) -> None:
        return JsonSerializer.dump(data, self, overwrite=overwrite, **kwargs)

    def read_json(self, **kwargs) -> Any:
        return JsonSerializer.load(self, **kwargs)

    def write_pickle(self, data: Any, *, overwrite=False, **kwargs) -> None:
        return PickleSerializer.dump(data, self, overwrite=overwrite, **kwargs)

    def read_pickle(self, **kwargs) -> Any:
        return PickleSerializer.load(self, **kwargs)

    def write_pickle_zstd(self, data: Any, *, overwrite=False, **kwargs) -> None:
        return ZstdPickleSerializer.dump(data, self, overwrite=overwrite, **kwargs)

    def read_pickle_zstd(self, **kwargs) -> Any:
        return ZstdPickleSerializer.load(self, **kwargs)

    def _dir_to_dir(
        self,
        *,
        source,
        target,
        method: str,
        method_on_source: bool,
        overwrite: bool,
        quiet: bool,
        concurrent: bool = True,
    ) -> int:
        def foo():
            source_path = source.path
            ovwt = overwrite
            for p in source.riterdir():
                extra = str(p.path.relative_to(source_path))
                if method_on_source:
                    yield (
                        getattr(p, method),
                        (target / extra,),
                        {'overwrite': ovwt},
                        extra,
                    )
                else:
                    yield (
                        getattr(target / extra, method),
                        (p,),
                        {'overwrite': ovwt},
                        extra,
                    )

        n = 0
        if concurrent:
            for _ in self._run_in_executor(foo(), quiet):
                n += 1
        else:
            for f, args, kwargs, _ in foo():
                f(*args, **kwargs)
                n += 1
        return n

    def copy_dir(
        self,
        source: str | Upath,
        *,
        overwrite: bool = False,
        quiet: bool = False,
        concurrent: bool = True,
    ) -> int:
        """Copy the content of the source directory recursively to ``self``.

        If ``source`` is an string, then it is in the same store as the current path,
        and it is either absolute, or relative to ``self.parent``.

        If ``source`` is not a string, then it must be an instance of a :meth:`Upath` subclass,
        and it may be in any store system.

        Immediate children of ``source`` will be copied as immediate children of ``self``.

        There is no such error as "target directory exists" as the copy-operation
        only concerns individual files.
        If the target directory (i.e. ``self``) contains files that do not have counterparts
        in the source directory, they will stay untouched.

        ``overwrite`` is file-wise. If ``False``, any existing target file will raise ``FileExistsError`` and
        halt the operation. If ``True``, any existing target file will be overwritten by the source file.

        ``quiet`` controls whether to print out progress info.

        Returns
        -------
        int
            The number of files copied.
        """
        target = self
        if isinstance(source, str):
            source = target.parent / source
        if source == target:
            return 0

        if not quiet:
            print(f'Copying from {source!r} into {target!r}', file=sys.stderr)
        return self._dir_to_dir(
            source=source,
            target=target,
            method='copy_file',
            method_on_source=False,
            overwrite=overwrite,
            quiet=quiet,
            concurrent=concurrent,
        )

    def _copy_file(self, source: Upath, target: Upath, *, overwrite: bool = False):
        target.write_bytes(source.read_bytes(), overwrite=overwrite)

    def copy_file(self, source: str | Upath, *, overwrite: bool = False) -> None:
        """Copy the ``source`` file to the current file (i.e. ``self``).

        If ``source`` is str, then it is in the same store as the current path,
        and it is either absolute, or relative to ``self.parent``.
        For example, if ``self`` is ``'/a/b/c/d.txt'``, then
        ``source='e.txt'`` means ``'/a/b/c/e.txt'``.

        If ``source`` is not a string, then it must be an instance of a :meth:`Upath` subclass,
        and it may be in any storage system.

        If ``source`` is not an existing file, ``FileNotFoundError`` is raised.

        If ``self`` is an existing file and ``overwrite`` is ``False``,
        ``FileExistsError`` is raised. If ``overwrite`` is ``True``,
        then the ``self`` will be overwritten.

        If ``type(source)`` is ``LocalUpath`` and ``self`` is an existing directory,
        then ``IsADirectoryError`` is raised.
        In a cloud blob store, there is no concrete "directory".
        For example, suppose ``self`` is 'gs://mybucket/backup/record' on Google Cloud Storage,
        and ``source`` is '/experiment/data', then ``source`` is 'gs://mybucket/experiment/data'.
        If there exists blob 'gs://mybucket/backup/record/y', then we say 'gs://mybucket/backup/record'
        is a "directory". However, this is merely a "virtual" concept, or an emulation
        of the "directory" concept on local disk. As long as the ``self`` path is not an
        existing blob, the copy will proceed with no problem.
        Nevertheless, such naming is confusing and better avoided.

        Return the number of files copied (either 0 or 1).
        """
        target = self
        if isinstance(source, str):
            source = target.parent / source
        if source == target:
            return
        self._copy_file(source, target, overwrite=overwrite)

    def remove_dir(self, *, quiet: bool = True, concurrent: bool = True) -> int:
        """Remove the current directory (i.e. ``self``) and all its contents recursively.

        Essentially, this removes each file that is yielded by :meth:`riterdir`.
        Subclasses should take care to remove "empty directories", if applicable,
        that are left behind.

        ``quiet`` controls whether to print progress info.

        Returns
        -------
        int
            The number of files removed.
        """

        def foo():
            for p in self.riterdir():
                yield p.remove_file, [], {}, str(p.path.relative_to(self.path))

        n = 0
        if concurrent:
            for _ in self._run_in_executor(foo(), quiet):
                n += 1
        else:
            for f, *_ in foo():
                f()
                n += 1
        return n

    @abc.abstractmethod
    def remove_file(self) -> None:
        """Remove the current file (i.e. ``self``).

        If ``self`` is not an existing file, ``FileNotFoundError`` is raised.
        If the file exists but can't be removed, the platform-dependent
        exception is propagated.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def iterdir(self) -> Iterator[Self]:
        """Yield the immediate (i.e. non-recursive) children
        of the current dir (i.e. ``self``).

        The yielded elements are instances of the same class.
        Each yielded element is either a file or a dir.
        There is no guarantee on the order of the returned elements.

        If ``self`` is not a dir (e.g. maybe it's a file),
        or does not exist at all, nothing is yielded (resulting in an
        empty iterable); no exception is raised.

        .. seealso:: :meth:`riterdir`.
        """
        raise NotImplementedError

    def ls(self) -> list[Self]:
        """Return the elements yielded by :meth:`iterdir` in a sorted list.

        Sorting is by a full path string maintained internally.

        The returned list may be empty.
        """
        return sorted(self.iterdir())

    @abc.abstractmethod
    def riterdir(self) -> Iterator[Self]:
        """Yield files under the current dir (i.e. ``self``) *recursively*.
        The method name means "recursive iterdir".

        The yielded elements are instances of the same class.
        They represent existing files.

        Compared to :meth:`iterdir`, this is recursive, and yields
        *files* only. Empty subdirectories will have no representation
        in the return.

        Similar to :meth:`iterdir`, if ``self`` is not a dir or does not exist,
        then nothing is yielded, and no exception is raised either.

        There is no guarantee on the order of the returned elements.
        """
        raise NotImplementedError

    def rmrf(self, *, quiet: bool = True, concurrent: bool = False) -> int:
        """Remove the current file or dir (i.e. ``self``) recursively.

        Analogous to the Linux command ``rm -rf``, hence the name of this method.

        Return the number of files removed.

        For example, if these blobs are present::

            /a/b/c/d/e.txt
            /a/b/c/kk.data
            /a/b/c

        then ``Upath('/a/b/c').rmrf()`` would remove all of them.

        ``concurrent`` is ``False`` by default because this method is often used in
        ``__del__`` of user classes, and thread pool is problematic in ``__del__``.
        """
        if self._path == '/':
            raise UnsupportedOperation('`rmrf` not allowed on root directory')
        try:
            self.remove_file()
        except (FileNotFoundError, IsADirectoryError):
            n = 0
        else:
            n = 1
        try:
            m = self.remove_dir(quiet=quiet, concurrent=concurrent)
        except FileNotFoundError:
            m = 0
        return n + m

    @contextlib.contextmanager
    @abc.abstractmethod
    def lock(self, *, timeout: int = None) -> Self:
        """Lock the current file (i.e. ``self``), in order to have exclusive access to the code block
        that has possession of the lock.

        ``timeout``: if the lock can't be acquired within ``timeout`` seconds,
        ``LockAcquireError`` is raised. If ``None``, wait for a default
        reasonably long time. To wait "forever", just pass in a large number.

        ``timeout=0`` is a valid input, meaning making exactly one attempt to acquire a lock.

        Once a lock is acquired, it will not expire until this contextmanager exits.
        In other words, this is timeout for the "lock acquisition", not for the
        lock itself. Actual waiting time could be slightly longer or shorter.

        This is a "mandatory lock", as opposed to an "advisory lock".
        The intended use case is for this lock to be used
        for implementing a (cooperative) "code lock".

        As this abstract method is to be used as a context manager,
        a subclass should use ``yield`` in its implementation.
        The ``yield`` statement should yield `self`.

        One way to achieve cooperative locking on a file via this
        lock is like this::

            f = Upath('abc.txt')
            with f.lock():
                ...
                # Now read from or write to `f` with exclusive access.

        Depending on the capabilities of the specific storage system,
        the lock may be on the current file itself or on another, helper file
        as an implementation detail. In the latter case, this method should delete
        the helper file upon release of the lock, if possible.

        Some storage engines may not provide the capability to implement
        this lock.
        """
        raise NotImplementedError


PathType = Union[str, pathlib.Path, Upath]
