"""
``VersionedUploadable`` helps store and use a "dataset" in an exclusive directory in consistent and convenient ways. Specifically,

1. The dataset is identified by a version string that is generated and sortable (datetime-based),
   so that the "newest" is always the "latest" version, and code can infer the latest version.
   The full path of the storage location is managed for the user, who only needs the version.

2. The storage can be either local (on disk) or remote (in a cloud blob store). There are methods to download/upload between local and remote storages.

   However, "local" and "remote" are just labels for the two storage locations. They can be both on local disk, or both
   in the same cloud storage (in different "locations"), or in two different cloud storages, or one on local disk and the other
   in a cloud blob store. This is controlled by the classmethods :meth:`VersionedUploadable.local_cls_upath` and :meth:`VersionedUploadable.remote_cls_upath`,
   which should be implemented by subclasses.

3. Within the dataset, one can conveniently specify sub-directories and files relative to the "root",
   and read/write. This navigation is the same regardless of whether the storage is local or remote.
"""

from __future__ import annotations

__all__ = [
    'VersionExistsError',
    'VersionNotFoundError',
    'VersionedUploadable',
]


import functools
import logging
import string
from abc import ABC, abstractmethod
from io import UnsupportedOperation
from typing import Any

from cloudly.util.datetime import utcnow

from ._blob import BlobUpath
from ._local import LocalUpath
from ._upath import Upath

logger = logging.getLogger(__name__)


class VersionExistsError(Exception):
    pass


class VersionNotFoundError(Exception):
    pass


ALNUM = string.ascii_letters + string.digits


def is_version(version: str) -> bool:
    # "[A-Za-z0-9][A-Za-z0-9._-]*"
    if not version:
        return False
    return (version[0] in ALNUM) and all(v in ALNUM or v in '._-' for v in version)


def make_version(tag: str = None) -> str:
    """
    Make a version string based on current UTC time in this format

    ::

        '20210816-082342-tag'

    where `'-tag'` is omitted if ``tag`` is falsy.

    Such version strings are sortable by time as there is practically no chance of collision
    between two versions.
    """
    ver = utcnow().strftime('%Y%m%d-%H%M%S')
    if tag:
        tag = tag.strip(' _-')
        if tag:
            assert is_version(tag)
            ver = ver + '-' + tag
    return ver


class VersionedUploadable(ABC):
    """
    A subclass will customize :meth:`remote_cls_upath` and :meth:`local_cls_upath`.
    """

    @classmethod
    def resolve_version(
        cls, version: str, remote: bool | None = None
    ) -> tuple[str, bool]:
        """
        Given ``version`` as one of the special values---'latest-local', 'latest-remote', and 'latest'---or
        an actual version string, and ``remote`` as ``None`` or explicit ``True``/``False``, figure out
        the actual version and its remote-ness.

        This is called by :meth:`__init__`.

        Parameters
        ----------
        version
            Either one of the special values 'latest', 'latest-local', and 'latest-remote', or an actual
            version string like '20210322-120529'.

            If ``version`` is an actual version string, then ``version`` and ``remote`` are returned as is,
            even if ``remote`` is ``None``. It is checked that ``version`` is a valid version string, but
            existence of the version is not checked.

        remote
            If ``True``, look in remote (cloud) storage only.
            If ``False``, look in local storage only.
            If ``None``, look in both remote and local.

            If ``version`` is 'latest-local', then ``remote`` must be ``False`` or ``None``.

            If ``version`` is 'latest-remote', then ``remote`` must be ``True`` or ``None``.

            If ``version`` is 'latest', then find the latest between local
            and remote storages if ``remote`` is ``None``, otherwise ``version`` becomes
            'latest-remote' or 'latest-local' according to the value of ``remote``.

        Returns
        -------
        tuple
            A tuple of two elements: the actual version string, and remote-ness.

            Raises ``ValueError`` if the parameters are incompatible.

            Raises ``VersionNotFoundError`` if no version is found that satisfies the request.
        """
        if version == 'latest-local':
            if remote is True:
                raise ValueError(
                    "version 'latest-local' is not valid with `remote=True`"
                )
            vers = cls.local_versions()
            if not vers:
                raise VersionNotFoundError(
                    f'could not find a local version of {cls.__name__}'
                )
            version = vers[-1]
            return version, False

        if version == 'latest-remote':
            if remote is False:
                raise ValueError(
                    "version 'latest-remote' is not valid with `remote=False`"
                )
            vers = cls.remote_versions()
            if not vers:
                raise VersionNotFoundError(
                    f'could not find a remote version of {cls.__name__}'
                )
            version = vers[-1]
            return version, True

        if version == 'latest':
            if remote is True:
                return cls.resolve_version('latest-remote', True)
            if remote is False:
                return cls.resolve_version('latest-local', False)
            assert remote is None
            v_remote = cls.remote_versions()
            v_local = cls.local_versions()
            if v_remote:
                v_r = v_remote[-1]
                if v_local:
                    v_l = v_local[-1]
                    if v_r > v_l:
                        return v_r, True
                    return v_l, False
                return v_r, True
            if v_local:
                v_l = v_local[-1]
                return v_l, False
            raise VersionNotFoundError(f'could not find a version of {cls.__name__}')

        assert is_version(version), f'is_version({version})'
        # Exact version string (not 'latest...') are not checked for existence.
        return version, remote

    @classmethod
    def parse_version(cls, version: str) -> dict[str, str]:
        version_str_len = 8 + 1 + 6  # '20210816-082342'
        return {
            'datetime': version[:version_str_len],
            'tag': version[(version_str_len + 1) :],
        }

    @classmethod
    @abstractmethod
    def local_cls_upath(cls) -> LocalUpath:
        """
        A subclass implements this method to determine the full path on the local disk
        for the entity represented by the particular subclass,
        i.e. a particular type of "dataset".

        The file-system structure directly under this path is determined by this class.
        Currently it contains a subdirectory called 'versions', in which goes
        on subdirectory per version, named after the version string.

        In the directory of one particular version, the content is determined by the user.
        User can create whatever subdirectories and files they want.
        This class uses one meta file in the root of the version's directory and the file is named
        "info.json".

        .. seealso:: :meth:`local_version_upath`.
        """
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def remote_cls_upath(cls) -> BlobUpath:
        """
        Analogous to :meth:`local_cls_upath` but on the remote side.

        .. seealso:: :meth:`remote_version_upath`.
        """
        raise NotImplementedError

    @classmethod
    def local_version_upath(cls, version: str) -> LocalUpath:
        """
        Root directory of the specified version in the local storage.
        """
        assert is_version(version)
        return cls.local_cls_upath() / 'versions' / version

    @classmethod
    def remote_version_upath(cls, version: str) -> BlobUpath:
        """
        Root directory of the specified version in the remote storage.
        """
        assert is_version(version)
        return cls.remote_cls_upath() / 'versions' / version

    @classmethod
    def local_versions(cls) -> list[str]:
        """
        Get a (potentially empty) list of the versions that exist on the local disk.

        The elements in the list are sorted from small (old) to large (new).

        Because ``remote_versions`` and ``local_versions`` get "directories"
        v/o checking their content, they might get invalid (corrupt or empty)
        versions. User should delete such bad versions as they are discovered.
        """
        ll = (cls.local_cls_upath() / 'versions').iterdir()
        return sorted(p.name for p in ll)

    @classmethod
    def remote_versions(cls) -> list[str]:
        """
        Analogous to :meth:`local_versions` but on the remote side.
        """
        ll = (cls.remote_cls_upath() / 'versions').iterdir()
        return sorted(p.name for p in ll)

    @classmethod
    def has_local_version(cls, version: str) -> bool:
        """
        A version is considered existent if and only if the file "info.json"
        exists in its root directory.
        """
        return (cls.local_version_upath(version) / 'info.json').is_file()

    @classmethod
    def has_remote_version(cls, version: str) -> bool:
        """
        Analogous to :meth:`has_local_version` but on the remote side.
        """
        return (cls.remote_version_upath(version) / 'info.json').is_file()

    @classmethod
    def remove_local_version(cls, version: str, **kwargs) -> None:
        """
        Delete the entire directory of the specified version on the local disk.

        By default, there is neither warning before the deletion nor progress printouts.

        Parameters
        ----------
        version
            The exact version string.
            If the version does not exist, it's a no-op.
        **kwargs
            Passed on to :meth:`~upathlib.Upath.remove_dir`.
        """
        cls.local_version_upath(version).remove_dir(**kwargs)

    @classmethod
    def remove_remote_version(cls, version: str, **kwargs) -> None:
        """
        Analogous to :meth:`remove_local_version` but on the remote side.
        """
        cls.remote_version_upath(version).remove_dir(**kwargs)

    @classmethod
    def new(
        cls, *, tag: str = None, remote: bool = False, **kwargs
    ) -> VersionedUploadable:
        """
        If a subclass needs additional setup on a newly created object, they may
        choose to override this classmethod ``new``.

        The optional ``tag`` appends (human readable) info to the auto created version string,
        which is based on current date and time.

        The returned object has attribute ``info``, which is an empty dict.
        Nothing has been written to storage.
        """
        remote = bool(remote)
        # Ensure this is not `None`.

        version = make_version(tag)

        if remote:
            upath = cls.remote_version_upath(version)
            if upath.is_file() or upath.is_dir():
                raise VersionExistsError(
                    f"remote version '{version}' of {cls.__name__} already exists"
                )
        else:
            upath = cls.local_version_upath(version)
            if upath.is_file():
                raise VersionExistsError(
                    f"local version '{version}' of {cls.__name__} already exists"
                )
            if upath.is_dir():
                if list(upath.iterdir()):  # not empty
                    raise VersionExistsError(
                        f"local version '{version}' of {cls.__name__} already exists"
                    )
                upath.rmrf()

        obj = cls(version, remote=remote, require_exists=False, **kwargs)  # type: ignore
        # `kwargs`, if not empty, contains parameters accepted by `__init__` of a subclass
        return obj

    def __init__(
        self, version: str, *, remote: bool | None = None, require_exists: bool = True
    ):
        """
        This loads up an **existing** version for reading and writing.
        The create a **new** version, use the classmethod :meth:`new`.

        Parameters
        ----------
        version
            Either the actual version string, or one of 'latest', 'latest-local', and 'latest-remote'.

        remote
            Look for the version in local or remote storage?

            If an explicit bool, it must be compatible with ``version``. For example,
            ``version='latest-remote'`` and ``remote=False`` are not compatible.

            If ``None``, and ``version='latest'``, then the latest version between local and remote
            is found and used. If local and remote have the same latest version, then the local one
            is used.

            If ``None``, and ``version`` is an exact version, then find it either locally or remotely
            wherever it exists. If the version exists in both storages, then the local one is used.

        require_exists
            Default is ``True``. If ``version`` is an exact version string but the version does not exist,
            ``VersionNotFoundError`` is raised. Usually you should leave this at the default.
            This is mainly for the call of ``__init__`` in :meth:`new`, where it needs to use
            ``require_exists=False``.
        """
        a, b = self.resolve_version(version, remote)
        #: version of the object
        self.version: str = a
        #: remote-ness of the object
        self.remote: bool = b

        if self.remote is None:
            # This is encountered only when `__init__` is called by user directly
            # (that is, not called by `new`), user specifies an exact `version` (that is,
            # not "latest", "latest-local", "latest-remote"), and `remote` is `None`).
            if self.has_local_version(self.version):
                # Prefer local version to remote version if the former exists.
                self.remote = False
            elif self.has_remote_version(self.version):
                self.remote = True
            else:
                raise VersionNotFoundError(
                    f"could not find version '{self.version}' of {self.__class__.__name__}"
                )

        try:
            self.info = self.path('info.json').read_json()
        except FileNotFoundError:
            if require_exists:
                raise VersionNotFoundError(
                    f"{'remote' if self.remote else 'local'} version '{self.version}' of {self.__class__.__name__} does not exist"
                )
            self.info = {}

        # A subclass usually should call `super().__init__` to settle `self.version` and `self.remote`,
        # then init object attributes to default values or load them if certain files exist.
        #
        # A subclass may also want to do different things according to the value of `self.remote`.

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}(version='{self.version}', remote={self.remote})"
        )

    def __str__(self) -> str:
        return self.__repr__()

    @functools.cached_property
    def upath(self) -> Upath:
        """
        Return the root directory of ``self``.

        This is consistent with the remote-ness of ``self``.
        """
        if self.remote:
            return self.remote_version_upath(self.version)
        return self.local_version_upath(self.version)

    def path(self, *args: str) -> Upath:
        """
        Return a path relative to the root directory of ``self``.

        Examples:

            self.path()                               # the root path
            self.path('info.json')                    # file in root directory
            self.path('abc', 'de', 'data.parquet')    # file 'abc/de/data.parquet' under root directory
            self.path('abc/de/data.parquet')          # same as above

        Typically, you proceed to read or write with the returned path (object), e.g.,

        ::

            self.path('info.json').write_json(self.info, overwrite=True)
            info = self.path('info.json').read_json()

        This is consistent with the remote-ness of ``self``.
        In other words, if ``self`` is local, then the returned path is local (under the local root directory);
        otherwise, the returned path is remote.

        ``self.path('abc.txt')`` is equivalent to ``(self.upath / 'abc.txt')``.

        .. note:: Don't start ``args`` with ``'/'``.
        """
        return self.upath.joinpath(*args)

    def save(self) -> None:
        """
        A subclass should re-implement this method to save its own stuff
        like data, summary, and whatever, and in the end call
        ``super().save()``.
        """
        self.path('info.json').write_json(self.info, overwrite=True)

    def download(self, path: str = None, *, overwrite: bool = False, **kwargs) -> int:
        """
        Download the entire dataset or specified parts of it.

        If the current object already points to a local version, then
        ``UnsupportedOperation`` is raised.

        If you know certain files have changed, you can bring remote/local into sync
        by downloading/uploading those particular files.

        Parameters
        ----------
        path
            Specific subdirectory or file to download.
            If ``None``, the entire version is downloaded.

            If ``None``, and the local version already exists, and ``overwrite`` is ``False``,
            then download will not happen. However, if the local version is incomplete or corrupt compared
            to the remote counterpart (the same version), the code wouldn't know.

            If not ``None``, then the specified sub-directory or file will be downloaded
            (into the expected locations for the version).
            This is meant for "repair work" if you know certain parts of the local version are corrupt or missing.
            If the version does not exist locally, there is hardly a scenario for downloading only parts of it
            (and that may cause issues later, as you are creating an incomplete local version).

        overwrite
            If ``True``, overwrite any file that exists locally.

        **kwargs
            If ``path`` is ``None``, this is passed on to ``upathlib.Upath.download_dir``.
            If ``path`` is not ``None``, this is ignored.

        Returns
        -------
        int
            The number of files downloaded.

        Warnings
        --------
        You should not use ``overwrite=True`` lightly just to ensure it proceeds.
        The default ``overwrite=False`` prevents re-downloading when the local version
        already exists. Try to benefit from such savings as far as you can.
        """
        if not self.remote:
            raise UnsupportedOperation(
                'can not download an object that is in local mode'
            )

        source = self.upath
        target = self.local_version_upath(self.version)

        if path:
            source = source / path
            target = target / path
            if source.is_file():
                target.copy_file(source, overwrite=overwrite)
                return 1
            else:
                return target.copy_dir(source, overwrite=overwrite, **kwargs)

        if not overwrite:
            if self.has_local_version(self.version):
                logger.info('local version of %r exists; upload is skipped', self)
                return 0
        return target.copy_dir(source, overwrite=True, **kwargs)

    def upload(self, path: str = None, *, overwrite: bool = False, **kwargs) -> int:
        """
        Analogous to ``download``.

        Return the number of files uploaded.
        """
        if self.remote:
            raise UnsupportedOperation(
                'can not upload an object that is in remote mode'
            )

        source = self.upath
        target = self.remote_version_upath(self.version)

        if path:
            source = source / path
            target = target / path
            if source.is_file():
                target.copy_file(source, overwrite=overwrite)
                return 1
            else:
                return target.copy_dir(source, overwrite=overwrite, **kwargs)
        if not overwrite:
            if self.has_remote_version(self.version):
                logger.info('remote version of %r exists; upload is skipped', self)
                return 0
        return target.copy_dir(source, overwrite=True, **kwargs)

    def ensure_local(
        self, *, init_kwargs: dict[str, Any] = None, **kwargs
    ) -> VersionedUploadable:
        """
        Return a local object of this version that exists.

        If ``self`` is local, then ``self`` is returned.

        Otherwise, if the local version does not exist, it will be downloaded.
        If the local version exists, downloading will not happen.
        (This code has to assume the local version is sound. though.)
        To force downloading regardless, pass in ``overwrite=True``, but don't do that lightly!

        Parameters
        ----------
        init_kwargs
            For special needs of a subclass that defines additional arguments for its ``__init__``.

        **kwargs
            Passed on to ``download``.

        .. note:: Calling ``ensure_local`` does not make the current object local;
            you need to receive and use the returned object, which *is* local.
        """
        if not self.remote:
            return self
        self.download(**kwargs)
        return self.__class__(
            self.version,
            remote=False,
            **(init_kwargs or {}),
        )

    def ensure_remote(
        self, *, init_kwargs: dict[str, Any] = None, **kwargs
    ) -> VersionedUploadable:
        if self.remote:
            return self
        self.upload(**kwargs)
        return self.__class__(
            self.version,
            remote=True,
            **(init_kwargs or {}),
        )
