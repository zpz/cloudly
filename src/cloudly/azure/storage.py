# WARNING
# This module is likely buggy as I stopped using Azure Blob Store and testing against it
# since sometime in 2021 I guess.

from __future__ import annotations

__all__ = ['AzureBlobUpath']


# Enable using `Upath` in type annotations in the code
# that defines this class.
# https://stackoverflow.com/a/49872353
# Will no longer be needed in Python 3.10.
import os
import random
import time
from collections.abc import Iterator
from contextlib import contextmanager
from io import BufferedReader, UnsupportedOperation

# from azure.storage.blob.aio import (
# ContainerClient as aContainerClient,
# BlobClient as aBlobClient,
# BlobLeaseClient as aBlobLeaseClient,
# )
from azure.core.exceptions import (
    HttpResponseError,
    ResourceExistsError,
    ResourceNotFoundError,
)
from azure.storage.blob import BlobClient, BlobLeaseClient, ContainerClient
from typing_extensions import Self

from cloudly.upathlib import (
    BlobUpath,
    FileInfo,
    LocalPathType,
    LocalUpath,
    LockAcquireError,
    LockReleaseError,
    Upath,
)
from cloudly.upathlib._blob import _resolve_local_path
from cloudly.util.datetime import utcnow

# End user may want to do this:
# logging.getLogger("azure.storage").setLevel(logging.WARNING)
# logging.getLogger("azure.core.pipeline.policies").setLevel(logging.WARNING)


class AzureBlobUpath(BlobUpath):
    _ACCOUNT_NAME = None
    _ACCOUNT_KEY = None
    _SAS_TOKEN = None

    @classmethod
    def get_account_info(cls):
        # Subclass needs to customize this method or
        # hard-code relevant class attributes directly.
        # TODO: does Azure have a way to infer this info if the code
        # is running on an Azure machine?
        return {
            'account_url': f'https://{cls._ACCOUNT_NAME}.blob.core.windows.net',
            'credential': cls._SAS_TOKEN or cls._ACCOUNT_KEY,
        }

    def __init__(
        self,
        *paths: str,
        container_name: str = None,
    ):
        if container_name is None:
            assert len(paths) == 1, paths
            path = paths[0]
            account_url = self.get_account_info()['account_url']
            assert path.startswith(account_url), path
            path = path[len(account_url) :]
            k = path.find('/')
            if k < 0:
                container_name = path
                paths = ('/',)
            else:
                container_name = path[:k]
                paths = (path[k:],)

        super().__init__(*paths)

        self._container_name = container_name

        self._container_client: ContainerClient = None
        self._blob_client: BlobClient = None
        # self._a_container_client: Optional[aContainerClient] = None
        # self._a_blob_client: Optional[aBlobClient] = None
        self._lease: BlobLeaseClient = None
        self._lock_count: int = 0

    def __getstate__(self):
        return self._container_name, super().__getstate__()

    def __setstate__(self, data):
        self._container_name, z1 = data
        self._container_client = None
        self._blob_client = None
        self._lease = None
        self._lock_count = 0
        super().__setstate__(z1)

    def __repr__(self) -> str:
        return "{}('{}', container_name='{}')".format(
            self.__class__.__name__, self._path, self._container_name
        )

    def __str__(self) -> str:
        return f"{self._container_name}://{self._path.lstrip('/')}"

    def as_uri(self) -> str:
        # TODO: what's the conventional lead word for Azure?
        return f"azure://{self._container_name}/{self._path.lstrip('/')}"

    @property
    def container_name(self):
        return self._container_name

    def is_file(self) -> bool:
        with self._provide_blob_client():
            return self._blob_client.exists()

    def file_info(self):
        try:
            with self._provide_blob_client():
                info = self._blob_client.get_blob_properties()
                return FileInfo(
                    ctime=info.creation_time.timestamp(),
                    mtime=info.last_modified.timestamp(),
                    time_created=info.creation_time,
                    time_modified=info.last_modified,
                    size=info.size,
                    details=info,
                )
                # If an existing file is written to again using
                # `write_...(..., overwrite=True)`,
                # then its `ctime` will not change; only `mtime`
                # will be updated.
        except ResourceNotFoundError:
            return None

    @property
    def root(self) -> AzureBlobUpath:
        """
        Return a new path representing the root of the same container.
        """
        return self.__class__(
            container_name=self._container_name,
        )

    def _copy_file_from(self, source, *, overwrite=False):
        # TODO: use `overwrite`
        with self._provide_blob_client():
            with source._provide_blob_client():
                copy = self._blob_client.start_copy_from_url(
                    source._blob_client.url,
                    requires_sync=True,
                )
                assert copy['copy_status'] == 'success', copy['copy_status']

    def _copy_file(self, source: Upath, target: Upath, *, overwrite=False):
        if isinstance(source, AzureBlobUpath):
            if isinstance(target, AzureBlobUpath):
                target._copy_file_from(source, overwrite=overwrite)
                return
            if isinstance(target, LocalUpath):
                source._download_file(target, overwrite=overwrite)
                return
        else:
            assert isinstance(target, AzureBlobUpath)
            if isinstance(source, LocalUpath):
                target._upload_file(source, overwrite=overwrite)
                return
        super()._copy_file(source, target, overwrite=overwrite)

    def _download_file(self, target: LocalPathType, *, overwrite=False) -> None:
        target = _resolve_local_path(target)
        if target.is_file():
            if not overwrite:
                raise FileExistsError(f"File exists: '{target}'")
            target.remove_file()
        elif target.is_dir():
            raise IsADirectoryError(f"Is a directory: '{target}'")

        with self._provide_blob_client():
            # TODO: check behavior of `download_blob` about
            # overwrite.
            os.makedirs(str(target.parent), exist_ok=True)
            with open(str(target), 'wb') as f:
                data = self._blob_client.download_blob()
                data.readinto(f)

    def _upload_file(self, source: LocalPathType, *, overwrite: bool = False):
        source = _resolve_local_path(source)
        if self.is_file():
            if not overwrite:
                # TODO: check the behavior of `upload_blob` related to
                # behavior about overwrite.
                raise FileExistsError(f"File exists: '{self}'")
            self.remove_file()
        with self._provide_blob_client():
            with open(str(source), 'rb') as data:
                self._blob_client.upload_blob(data)

    def iterdir(self) -> Iterator[Self]:
        with self._provide_container_client():
            prefix = self.blob_name + '/'
            k = len(prefix)
            for p in self._container_client.walk_blobs(name_starts_with=prefix):
                yield self / p.name[k:]

    def _acquire_lease(self, timeout: int = None):
        if timeout is None:
            timeout = 300
        t0 = time.perf_counter()
        while True:
            try:
                self.write_text(utcnow().isoformat(), overwrite=True)
                try:
                    self._lease = self._blob_client.acquire_lease(
                        lease_duration=-1, timeout=10
                    )
                    return
                except ResourceNotFoundError:
                    continue  # go to the outer looper to write the file again
                except HttpResponseError as e:
                    if e.status_code == 409 and e.error_code == 'LeaseAlreadyPresent':  # pylint: disable=no-member
                        # Having a lease held by others. Continue to wait.
                        # This may happen when another client placed the lease
                        # on this blob right after we've created it, that is,
                        # another worker's won out in `acquire_lease`.
                        pass
                    else:
                        raise
            except HttpResponseError as e:
                if e.status_code == 412 and e.error_code == 'LeaseIdMissing':  # pylint: disable=no-member
                    # Blob exists and has a lease on it. Wait and try again.
                    pass
                else:
                    raise

            t1 = time.perf_counter()
            if t1 - t0 >= timeout:
                raise LockAcquireError(self, t1 - t0)
            time.sleep(random.uniform(0.05, 1.0))

    @contextmanager
    def lock(self, *, timeout=None):
        """
        References:
        https://docs.microsoft.com/en-us/azure/storage/blobs/concurrency-manage?tabs=dotnet
        """
        with self._provide_blob_client():
            if self._lease is None:
                self._acquire_lease(timeout)
                self._lock_count = 1
            else:
                self._lock_count += 1
            try:
                yield self
            finally:
                self._lock_count -= 1
                if self._lock_count <= 0:
                    try:
                        # try:
                        #     self.remove_file()
                        # except FileNotFoundError:
                        #     pass
                        self._lease.release()
                        self._lease = None
                        self._lock_count = 0
                    except Exception as e:
                        raise LockReleaseError(
                            f'failed to release lock file {self}'
                        ) from e

    # @asynccontextmanager
    # async def a_lock(self, *, timeout=None):
    #     loop = asyncio.get_running_loop()
    #     with self._provide_blob_client():
    #         # TODO: this context manager is sync

    #         if self._lease is None:
    #             ff = functools.partial(self._acquire_lease, timeout=timeout)
    #             await loop.run_in_executor(None, ff)
    #             self._lock_count = 1
    #         else:
    #             self._lock_count += 1
    #         try:
    #             yield
    #         finally:
    #             self._lock_count -= 1
    #             if self._lock_count <= 0:
    #                 await loop.run_in_executor(None, self._lease.release)
    #                 self._lease = None
    #                 self._lock_count = 0

    @contextmanager
    def _provide_blob_client(self):
        if self._blob_client is None:
            bc = BlobClient(
                container_name=self._container_name,
                blob_name=self.blob_name,
                **self.get_account_info(),
            )
            self._blob_client = bc
            try:
                with bc:
                    yield
            finally:
                self._blob_client = None
        else:
            yield

    # TODO: how to optimize this part so that
    # new objects can reuse the ContainerClient?

    @contextmanager
    def _provide_container_client(self):
        if self._container_client is None:
            cc = ContainerClient(
                container_name=self._container_name,
                **self.get_account_info(),
            )
            self._container_client = cc
            try:
                with cc:
                    yield
            finally:
                self._container_client = None
        else:
            yield

    def read_bytes(self) -> bytes:
        with self._provide_blob_client():
            try:
                return self._blob_client.download_blob().readall()
            except ResourceNotFoundError as e:
                raise FileNotFoundError(f"No such file: '{self}'") from e

    def remove_file(self):
        with self._provide_blob_client():
            try:
                self._blob_client.delete_blob(
                    delete_snapshots='include', lease=self._lease
                )
            except ResourceNotFoundError:
                raise FileNotFoundError(f"No such file: '{self}'")

    def riterdir(self) -> Iterator[Self]:
        with self._provide_container_client():
            prefix = self.blob_name + '/'
            k = len(prefix)
            for p in self._container_client.list_blobs(name_starts_with=prefix):
                yield self / p.name[k:]

    def write_bytes(self, data: bytes | BufferedReader, *, overwrite=False) -> None:
        if self._path == '/':
            raise UnsupportedOperation(f"Can not write to root as a blob: '{self}'")

        with self._provide_blob_client():
            try:
                self._blob_client.upload_blob(
                    data, overwrite=overwrite, lease=self._lease
                )
            except ResourceExistsError as e:
                raise FileExistsError(f"File eixsts: '{self}'") from e
