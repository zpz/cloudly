from __future__ import annotations

__all__ = ['GcsBlobUpath']


# Enable using `Upath` in type annotations in the code
# that defines this class.
# https://stackoverflow.com/a/49872353
# Will no longer be needed in Python 3.10.
import contextlib
import logging
import os
import time
from collections.abc import Iterator
from io import BufferedReader, BytesIO, UnsupportedOperation

from google import resumable_media
from google.api_core.exceptions import (
    NotFound,
    PreconditionFailed,
    RetryError,
)
from google.api_core.retry import Retry, if_exception_type
from google.cloud import storage
from google.cloud.storage.retry import (
    DEFAULT_RETRY,
)
from typing_extensions import Self

from cloudly.gcp.auth import get_credentials, get_project_id
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
from cloudly.upathlib._util import MAX_THREADS, get_shared_thread_pool

# To see retry info, add the following in user code.
# There is one message per retry.
# logging.getLogger('google.api_core.retry').setLevel(logging.DEBUG)


# Many blob methods have a parameter `timeout`.
# The default value, 60 seconds, is defined as ``google.cloud.storage.constants._DEFAULT_TIMEOUT``.
# From my reading, this is the `timeout` parameter to `google.cloud.storage.client._connection.api_request`,
# that is, the `timeout` parameter to `google.cloud._http.JSONConnection.api_request`.
# In other words, this is the http request "connection timeout" to server.
# It is not a "retry" timeout.
# `google.cloud` is the repo python-cloud-core.
# `DEFAULT_RETRY` is used in `google.cloud.storage` extensively.
# If you want to increase the timeout (default to 120) across the board,
# you may hack certain attributes of this object.
# See `google.api_core.retry.exponential_sleep_generator`.


logger = logging.getLogger(__name__)

# When downloading large files, there may be INFO logs from `google.resumable_media._helpers` about "No MD5 checksum was returned...".
# You may want to suppress that log by setting its level to WARNING.

# 67108864 = 256 * 1024 * 256 = 64 MB
MEGABYTES32 = 33554432
MEGABYTES64 = 67108864
LARGE_FILE_SIZE = MEGABYTES64
NOTSET = object()

# The availability of `Retry.with_timeout` is in a messy state
# between versions of `google-api-core`.
# `upathlib` requires a recent version which should be fine.
assert hasattr(DEFAULT_RETRY, 'with_timeout')


class GcsBlobUpath(BlobUpath):
    """
    GcsBlobUpath implements the :class:`~upathlib.Upath` API for
    Google Cloud Storage using the package
    `google-cloud-storage <https://github.com/googleapis/python-storage/tree/main>`_.
    """

    _CLIENT: storage.Client = None
    # The `storage.Client` object is not pickle-able.
    # But if it is copied into another "forked" process, it will function properly.
    # Hence this is safe with multiprocessing, be it forked or spawned.
    # In a "spawned" process, this will start as None.

    _LOCK_EXPIRE_IN_SECONDS: int = 3600
    # Things performed while holding a `lock` should finish within
    # this many seconds. If a worker tries but fails to acquire a lock on a file,
    # and finds the lock file has existed this long, it assumes the file
    # is "dead" because somehow the previous locking failed to delete the file properly,
    # and it will delete this lock file, and retry lock acquisition.
    #
    # Usually you don't need to customize this.

    @classmethod
    def _client(cls) -> storage.Client:
        """
        Return a client to the GCS service.

        This does not make HTTP calls if things in cache are still valid.
        """
        cred, renewed = get_credentials(return_state=True)
        if cls._CLIENT is None or renewed:
            cls._CLIENT = storage.Client(
                project=get_project_id(),
                credentials=cred,
            )
        return cls._CLIENT

    def __init__(
        self,
        *paths: str,
        bucket_name: str = None,
    ):
        """
        If ``bucket_name`` is ``None``, then ``*paths`` should be a single string
        starting with 'gs://<bucket-name>/'.

        If ``bucket_name`` is specified, then ``*paths`` specify path components
        under the root of the bucket.

        Examples
        --------
        These several calls are equivalent::

            >>> GcsBlobUpath('experiments', 'data', 'first.data', bucket_name='backup')
            GcsBlobUpath('gs://backup/experiments/data/first.data')
            >>> GcsBlobUpath('/experiments/data/first.data', bucket_name='backup')
            GcsBlobUpath('gs://backup/experiments/data/first.data')
            >>> GcsBlobUpath('gs://backup/experiments/data/first.data')
            GcsBlobUpath('gs://backup/experiments/data/first.data')
            >>> GcsBlobUpath('gs://backup', 'experiments', 'data/first.data')
            GcsBlobUpath('gs://backup/experiments/data/first.data')
        """
        if bucket_name is None:
            # The first arg must be like
            #   'gs://bucket-name'
            # or
            #   'gs://bucket-name/path...'

            p0 = paths[0]
            assert p0.startswith('gs://'), p0
            p0 = p0[5:]
            k = p0.find('/')
            if k < 0:
                bucket_name = p0
                paths = paths[1:]
            else:
                bucket_name = p0[:k]
                p0 = p0[k:]
                paths = (p0, *paths[1:])

        super().__init__(*paths)
        assert bucket_name, bucket_name
        self.bucket_name = bucket_name
        self._lock_count: int = 0
        self._generation = None
        self._quiet_multidownload = True
        self._blob_ = None

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}('{self.as_uri()}')"

    def __str__(self) -> str:
        return self.as_uri()

    def __getstate__(self):
        # `Bucket` objects can't be pickled.
        # I believe `Blob` objects can't be either.
        # The `service_account.Credentials` class object can be pickled.
        return (
            self.bucket_name,
            self._quiet_multidownload,
        ), super().__getstate__()

    def __setstate__(self, data):
        (self.bucket_name, self._quiet_multidownload), z1 = data
        self._blob_ = None
        self._lock_count = 0
        self._generation = None
        super().__setstate__(z1)

    def _bucket(self) -> storage.Bucket:
        """
        Return a Bucket object, via :meth:`_client`.
        """
        cl = self._client()
        # This will make HTTP calls only if needed.
        return cl.bucket(self.bucket_name, user_project=get_project_id())
        # This does not make HTTP calls.

    def _blob(self, *, reload=False) -> storage.Blob:
        self._blob_ = self._bucket().blob(self.blob_name)
        # This constructs a Blob object irrespective of whether the blob
        # exists in GCS. This does not call the storage
        # server to get up-to-date info of the blob.
        #
        # This object is suitable for making read/write calls that
        # talk to the storage. It's not suitable for accessing "properties" that
        # simply returns whatever is in the Blob object (which may be entirely
        # disconnected from the actual blob in the cloud), for example, `metadata`.

        if reload:
            self._blob_.reload(client=self._client())
            # Equivalent to `self._bucket().get_blob(self.blob_name)`

        return self._blob_
        # This object, after making calls to the cloud storage, may contain some useful info.
        # This cache is for internal use.

    def as_uri(self) -> str:
        """
        Represent the path as a file URI, like 'gs://bucket-name/path/to/blob'.
        """
        return f"gs://{self.bucket_name}/{self._path.lstrip('/')}"

    def is_file(self) -> bool:
        """
        The result of this call is not cached, in case the object is modified anytime
        by other clients.
        """
        return self._blob().exists(self._client())

    def is_dir(self) -> bool:
        """
        If there is a dummy blob with name ``f"{self.name}/"``,
        this will return ``True``.
        This is the case after creating a "folder" on the GCP dashboard.
        In programmatic use, it's recommended to avoid such situations so that
        ``is_dir()`` returns ``True`` if and only if there are blobs
        "under" the current path.
        """
        prefix = self.blob_name + '/'
        blobs = self._client().list_blobs(
            self._bucket(),
            prefix=prefix,
            max_results=1,
            page_size=1,
            fields='items(name),nextPageToken',
        )
        return len(list(blobs)) > 0

    def file_info(self, *, timeout=None) -> FileInfo | None:
        """
        Return file info if the current path is a file;
        otherwise return ``None``.
        """
        try:
            b = self._blob(reload=True)
        except NotFound:
            return None
        return FileInfo(
            ctime=b.time_created.timestamp(),
            mtime=b.updated.timestamp(),
            time_created=b.time_created,
            time_modified=b.updated,
            size=b.size,  # bytes
            details=b._properties,
        )
        # If an existing file is written to again using `write_...`,
        # then its `ctime` and `mtime` are both updated.
        # My experiments showed that `ctime` and `mtime` are equal.

    @property
    def root(self) -> GcsBlobUpath:
        """
        Return a new path representing the root of the same bucket.
        """
        obj = self.__class__(
            bucket_name=self.bucket_name,
        )
        return obj

    def _write_from_buffer(
        self,
        file_obj,
        *,
        overwrite=False,
        content_type=None,
        size=None,
        retry=DEFAULT_RETRY,
    ):
        if self._path == '/':
            raise UnsupportedOperation(f"can not write to root as a blob: '{self}'")

        # `Blob.upload_from_file` gets the data by `file.obj.read()` and uses the data
        # going forward, including during retry, hence we don't need to worry about
        # rewinding `file_obj` for retry.

        if_generation_match = None if overwrite else 0

        def func():
            file_obj.seek(0)  # needed in case of retry
            self._blob().upload_from_file(
                file_obj,
                content_type=content_type,
                size=size,
                client=self._client(),
                if_generation_match=if_generation_match,
                retry=None,
            )

        # `upload_from_file` ultimately uses `google.resumable_media.requests` to do the
        # uploading. `resumable_media` has its own retry facilities. When a retry-eligible exception
        # happens but time is up, the original exception is raised.
        # This is in contrast to `google.cloud.api_core.retry.Retry`, which will
        # raise `RetryError` with the original exception as its `.cause` attribute.

        # If `file_obj` is large, this will sequentially upload chunks.

        try:
            try:
                retry(func)()
                # Blob data rate limit is 1 update per second.
            except RetryError as e:
                raise e.cause
        except PreconditionFailed as e:
            raise FileExistsError(f"File exists: '{self}'") from e

        # TODO: set "create_time", 'update_time" to be the same
        # as the source local file?
        # Blob objects has methods `_set_properties`, `_patch_property`,
        # `patch`.

    def write_bytes(
        self,
        data: bytes | BufferedReader,
        *,
        overwrite=False,
        **kwargs,
    ):
        """
        Write bytes ``data`` to the current blob.

        In the usual case, ``data`` is bytes.
        The case where ``data`` is a
        `io.BufferedReader <https://docs.python.org/3/library/io.html#io.BufferedReader>`_ object, such as an open file,
        is not well tested.
        """
        try:
            memoryview(data)
        except TypeError:  # file-like data
            pass
        else:  # bytes-like data
            data = BytesIO(data)
            data.seek(0)

        self._write_from_buffer(
            data,
            overwrite=overwrite,
            **kwargs,
        )
        # prev versions used `content_type='text/plain'` and worked; I don't remember why that choice was made

    def _multipart_download(self, blob_size, file_obj):
        client = self._client()
        blob = self._blob()

        def _download(start, end):
            # Both `start` and `end` are inclusive.
            # The very first `start` should be 0.
            buffer = BytesIO()
            target_size = end - start + 1
            current_size = 0
            while True:
                try:
                    blob.download_to_file(
                        buffer,
                        client=client,
                        start=start + current_size,
                        end=end,
                        raw_download=True,
                    )
                    # "checksum mismatch" errors were encountered when downloading Parquet files.
                    # `raw_download=True` seems to prevent that error.
                except NotFound as e:
                    raise FileNotFoundError(f"No such file: '{blob}'") from e
                current_size = buffer.tell()
                if current_size >= target_size:
                    break
                # When a large number (say 10) of workers independently download
                # the same large blob, practice showed the number of bytes downloaded
                # may not match the number that was requested.
                # I did not see this mentioned in GCP doc.
            buffer.seek(0)
            if current_size > target_size:
                buffer.truncate(target_size)
            return buffer
            # TODO:
            # Some speedup might be possible if we do not write into `buffer`, but rather
            # return the response (which is written into `buffer` here) and later write it into
            # `file_obj` directly in correct order. But that hack would be somewhat involved.
            # See `google.cloud.storage.blob.Blob.download_to_file`,
            # `google.cloud.storage.blob.Blob._do_download`, and its use of
            # `google.resumable_media.requests.RawDownload` (passing `stream=None` to it).

        executor = get_shared_thread_pool('upathlib-gcs', MAX_THREADS - 2)
        k = 0
        tasks = []
        while True:
            kk = min(k + LARGE_FILE_SIZE, blob_size)
            t = executor.submit(_download, k, kk - 1)
            tasks.append(t)
            k = kk
            if k >= blob_size:
                break

        try:
            it = 0
            for t in tasks:
                buf = t.result()
                file_obj.write(buf.getbuffer())
                buf.close()
                it += 1
        except Exception:
            for tt in tasks[it:]:
                if not tt.done():
                    tt.cancel()
                    # This may not succeed, but there isn't a good way to
                    # guarantee cancellation here.
            raise

    def _read_into_buffer(self, file_obj, *, concurrent=True):
        file_info = self.file_info()
        if not file_info:
            raise FileNotFoundError(f"No such file: '{self}'")
        file_size = file_info.size  # bytes
        if file_size <= LARGE_FILE_SIZE or not concurrent:
            try:
                self._blob().download_to_file(
                    file_obj, client=self._client(), raw_download=True
                )
                # "checksum mismatch" errors were encountered when downloading Parquet files.
                # `raw_download=True` seems to prevent that error.
                return
            except NotFound as e:
                raise FileNotFoundError(f"No such file: '{self}'") from e
        else:
            self._multipart_download(file_size, file_obj)

    def read_bytes(self, **kwargs) -> bytes:
        """
        Return the content of the current blob as bytes.
        """
        buffer = BytesIO()
        self._read_into_buffer(buffer, **kwargs)
        return buffer.getvalue()

    # Google imposes rate limiting on create/update/delete requests.
    # According to Google doc, https://cloud.google.com/storage/quotas,
    #   There is a limit on writes to the same object name. This limit is once per second.

    def _copy_file(self, source: Upath, target: Upath, *, overwrite=False) -> None:
        if isinstance(source, GcsBlobUpath):
            if isinstance(target, GcsBlobUpath):
                # https://cloud.google.com/storage/docs/copying-renaming-moving-objects
                try:
                    source._bucket().copy_blob(
                        source._blob(),
                        target._bucket(),
                        target.blob_name,
                        client=source._client(),
                        if_generation_match=None if overwrite else 0,
                    )
                except NotFound as e:
                    raise FileNotFoundError(f"No such file: '{source}'") from e
                except PreconditionFailed as e:
                    raise FileExistsError(f"File exists: '{target}'") from e
                return
            if isinstance(target, LocalUpath):
                source._download_file(target, overwrite=overwrite)
                return
        else:
            assert isinstance(target, GcsBlobUpath)
            if isinstance(source, LocalUpath):
                target._upload_file(source, overwrite=overwrite)
                return
        super()._copy_file(source, target, overwrite=overwrite)

    def _download_file(
        self, target: LocalPathType, *, overwrite=False, concurrent=True
    ) -> None:
        """
        Download the content of the current blob to ``target``.
        """
        target = _resolve_local_path(target)
        if target.is_file():
            if not overwrite:
                raise FileExistsError(f"File exists: '{target}'")
            target.remove_file()
        elif target.is_dir():
            raise IsADirectoryError(f"Is a directory: '{target}'")

        os.makedirs(str(target.parent), exist_ok=True)
        try:
            with open(target, 'wb') as file_obj:
                # If `target` is an existing directory,
                # will raise `IsADirectoryError`.
                self._read_into_buffer(file_obj, concurrent=concurrent)
            updated = self._blob_.updated
            if updated is not None:
                mtime = updated.timestamp()
                os.utime(target, (mtime, mtime))
        except resumable_media.DataCorruption:
            target.remove_file()
            raise

    def _upload_file(self, source: LocalPathType, *, overwrite=False) -> None:
        """
        Upload the content of ``source`` to the current blob.
        """
        source = _resolve_local_path(source)
        filename = str(source)
        content_type = self._blob()._get_content_type(None, filename=filename)

        if self.is_file():
            if not overwrite:
                raise FileExistsError(f"File exists: '{self}'")
            self.remove_file()

        with open(filename, 'rb') as file_obj:
            total_bytes = os.fstat(file_obj.fileno()).st_size
            self._write_from_buffer(
                file_obj,
                size=total_bytes,
                content_type=content_type,
                overwrite=overwrite,
            )
            # TODO:
            # If the file is large, current implementation uploads chunks sequentially.
            # To upload chunks concurrently, check out `Blob.compose`.

    def iterdir(self) -> Iterator[Self]:
        """
        Yield immediate children under the current dir.
        """
        # From Google doc:
        #
        # Lists all the blobs in the bucket that begin with the prefix.
        #
        # This can be used to list all blobs in a "folder", e.g. "public/".
        #
        # The delimiter argument can be used to restrict the results to only the
        # "files" in the given "folder". Without the delimiter, the entire tree under
        # the prefix is returned. For example, given these blobs:
        #
        #     a/1.txt
        #     a/b/2.txt
        #
        # If you specify prefix ='a/', without a delimiter, you'll get back:
        #
        #     a/1.txt
        #     a/b/2.txt
        #
        # However, if you specify prefix='a/' and delimiter='/', you'll get back
        # only the file directly under 'a/':
        #
        #     a/1.txt
        #
        # As part of the response, you'll also get back a blobs.prefixes entity
        # that lists the "subfolders" under `a/`:
        #
        #     a/b/
        #
        # Search "List the objects in a bucket using a prefix filter | Cloud Storage"
        #
        # You can "create folder" on the Google Cloud Storage dashboard. What it does
        # seems to create a dummy blob named with a '/' at the end and sized 0.
        # This case is handled in this function.

        prefix = self.blob_name + '/'
        k = len(prefix)
        blobs = self._client().list_blobs(self._bucket(), prefix=prefix, delimiter='/')
        for p in blobs:
            if p.name == prefix:
                # This happens if users has used the dashboard to "create a folder".
                # This seems to be a valid blob except its size is 0.
                # If user deliberately created a blob with this name and with content,
                # it's ignored. Do not use this name for a blob!
                continue
            obj = self / p.name[k:]  # files
            yield obj
        for p in blobs.prefixes:
            yield self / p[k:].rstrip('/')  # "subdirectories"
            # If this is an "empty subfolder", it is counted but it can be
            # misleading. User should avoid creating such empty folders.

    def remove_dir(self, **kwargs) -> int:
        """
        Remove the current dir and all the content under it recursively.
        Return the number of blobs removed.
        """
        z = super().remove_dir(**kwargs)
        prefix = self.blob_name + '/'
        for p in self._client().list_blobs(self._bucket(), prefix=prefix):
            assert p.name.endswith('/'), p.name
            p.delete()
        return z

    def remove_file(self) -> None:
        """
        Remove the current blob.
        """
        try:
            self._blob().delete(client=self._client())
        except NotFound as e:
            raise FileNotFoundError(f"No such file: '{self}'") from e

    def riterdir(self) -> Iterator[Self]:
        """
        Yield all blobs recursively under the current dir.
        """
        prefix = self.blob_name + '/'
        k = len(prefix)
        for p in self._client().list_blobs(self._bucket(), prefix=prefix):
            if p.name.endswith('/'):
                # This can be an "empty folder"---better not create them!
                # Worse, this is an actual blob name---do not do this!
                continue
            obj = self / p.name[k:]
            yield obj

    def _acquire_lease(
        self,
        *,
        timeout,
    ):
        # Tweaking on the retry timing considers the particular use case with `upathlib.Multiplexer`.
        # Similarly in `_release_lease`.

        if self._path == '/':
            raise UnsupportedOperation('can not write to root as a blob', self)

        t0 = time.perf_counter()
        lockfile = self.with_suffix(self.suffix + '.lock')
        try:
            try:
                Retry(
                    predicate=if_exception_type(FileExistsError),
                    initial=1.0,
                    maximum=10.0,
                    multiplier=1.2,
                    timeout=timeout,
                )(lockfile.write_bytes)(
                    b'0',
                    overwrite=False,
                )
                self._generation = lockfile._blob_.generation
            except RetryError as e:
                # `RetryError` originates from only one place, in `google.cloud.api_core.retry.retry_target`.
                # The message is "Deadline of ...s exceeded while calling target function, last exception: ...".
                # We are not losing much info by raising `e.cause` directly. That may ease downstream exception handling.
                raise e.cause
        # except FileExistsError as e:
        #     finfo = lockfile.file_info()
        #     now = datetime.now(timezone.utc)
        #     file_age = (now - finfo.time_created).total_seconds()
        #     if file_age > self._LOCK_EXPIRE_IN_SECONDS:
        #         # If the file is old,
        #         # assume it is a dead file, that is, the last lock operation
        #         # somehow failed and did not delete the file.
        #         logger.warning(
        #             "the locker file '%s' was created %d seconds ago; assuming it is dead and deleting it",
        #             self,
        #             int(file_age),
        #         )
        #         try:
        #             lockfile.remove_file()
        #         except FileExistsError:
        #             pass
        #         # If this fails, the exception will propagate, which is not LockAcquireError.
        #         # After deleting the file, try it again:
        #         self._acquire_lease(timeout=timeout)
        #     else:
        #         raise LockAcquireError(
        #             f"Failed to lock '{self}' trying for {time.perf_counter() - t0:.2f} seconds; gave up on {e!r}"
        #         ) from e
        except Exception as e:
            raise LockAcquireError(
                f"Failed to lock '{self}' trying for {time.perf_counter() - t0:.2f} seconds; gave up on {e!r}"
            ) from e

    def _release_lease(self):
        # TODO:
        # once got "RemoteDisconnected" error after 0.01 seconds.
        t0 = time.perf_counter()
        lockfile = self.with_suffix(self.suffix + '.lock')
        try:
            try:
                try:
                    lockfile._blob().delete(
                        client=self._client(),
                        if_generation_match=self._generation,
                    )
                    # The current worker (who has created this lock file) should be the only one
                    # that does this deletion.
                    self._generation = None
                except RetryError as e:
                    raise e.cause
            except NotFound:
                raise FileNotFoundError(f"No such file: '{self}'")
        except Exception as e:
            t1 = time.perf_counter()
            raise LockReleaseError(
                f"Failed to unlock '{self}' trying for {t1 - t0:.2f} seconds; gave up on {e!r}"
            ) from e

    @contextlib.contextmanager
    def lock(
        self,
        *,
        timeout=None,
    ):
        """
        This implementation does not prevent the file from being deleted
        by other workers that does not use this method.
        It relies on the assumption that this blob
        is used *cooperatively* solely between workers in this locking logic.

        ``timeout`` is the wait time for acquiring or releasing the lease.
        If ``None``, the default value 600 seconds is used.
        If ``0``, exactly one attempt is made to acquire a lock.

        Note: `timeout = None` does not mean infinite wait.
        It means a default wait time. If user wants longer wait,
        just pass in a large number.
        """
        # References:
        # https://www.joyfulbikeshedding.com/blog/2021-05-19-robust-distributed-locking-algorithm-based-on-google-cloud-storage.html
        # https://cloud.google.com/storage/docs/generations-preconditions
        # https://cloud.google.com/storage/docs/gsutil/addlhelp/ObjectVersioningandConcurrencyControl
        if timeout is None:
            timeout = 600.0

        if self._lock_count == 0:
            self._acquire_lease(timeout=timeout)
        self._lock_count += 1
        try:
            yield self
        finally:
            self._lock_count -= 1
            if self._lock_count == 0:
                self._release_lease()

    def open(self, mode='r', **kwargs):
        """
        Use this on a blob (not a "directory") as a context manager.
        See Google documentation.
        """
        return self._blob().open(mode, **kwargs)

    def read_meta(self) -> dict[str, str]:
        # "custom metadata", see
        # https://cloud.google.com/storage/docs/metadata
        # https://cloud.google.com/storage/docs/viewing-editing-metadata#storage-view-object-metadata-python
        #
        # Metadata read/write can only be performed on a blob that already exists, that is,
        # something has to be written as the blob data so that the blob exists.
        return self._blob(reload=True).metadata

    def write_meta(self, data: dict[str, str]):
        # The values should be small. There's a size limit: 8 KiB for all
        # custom keys and values combined.
        # `data` only needs to include elements you want to update.
        # To remove a custom key, set its value to `None`.
        # TODO: check whether this actually removes the key in the blob's metadata.
        blob = self._blob()
        blob.metadata = data
        blob.patch(
            client=self._client(),
            retry=DEFAULT_RETRY,
        )
        # `blob.metadata` (`self._blob_.metadata`) now contains the updated metadata,
        # not just the input `data`, but the current updated metadata of the blob.
        #
        # Metadata rate limit is 1 update per second.
        # Default retry in Google code is None if there is no precondition on metageneration.
        # TODO: think more about this retry.
