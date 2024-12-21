from __future__ import annotations

import contextlib
from collections.abc import Iterator
from datetime import datetime
from uuid import uuid4

from typing_extensions import Self

import cloudly.upathlib._tests as alltests
from cloudly.upathlib import BlobUpath, FileInfo


class ResourceNotFoundError(Exception):
    pass


class ResourceExistsError(Exception):
    pass


class FakeBlobStore:
    """A in-memory blob store for illustration purposes"""

    def __init__(self):
        self._data = {
            'bucket_a': {},
            'bucket_b': {},
        }
        self._meta = {
            'bucket_a': {},
            'bucket_b': {},
        }

    def write_bytes(self, bucket: str, name: str, data: bytes, overwrite: bool = False):
        assert isinstance(data, bytes)
        if name in self._data[bucket] and not overwrite:
            raise ResourceExistsError
        ctime = datetime.now()
        fi = FileInfo(
            ctime=ctime.timestamp(),
            mtime=ctime.timestamp(),
            time_created=ctime,
            time_modified=ctime,
            size=len(data),
            details={},
        )
        self._data[bucket][name] = data
        self._meta[bucket][name] = fi

    def read_bytes(self, bucket: str, name: str):
        z = self._data[bucket]
        try:
            return z[name]
        except KeyError:
            raise ResourceNotFoundError(name)

    def list_blobs(self, bucket: str, prefix: str):
        bb = [k for k in self._data[bucket] if k.startswith(prefix)]
        yield from bb

    def delete_blob(self, bucket: str, name: str):
        z = self._data[bucket]
        try:
            del z[name]
            del self._meta[bucket][name]
        except KeyError:
            raise ResourceNotFoundError(name)

    def copy_blob(self, bucket: str, name: str, target: str, *, overwrite=False):
        self.write_bytes(
            bucket=bucket,
            name=target,
            data=self.read_bytes(bucket, name),
            overwrite=overwrite,
        )

    def exists(self, bucket: str, name: str):
        z = self._data[bucket]
        return name in z

    def file_info(self, bucket: str, name: str):
        try:
            return self._meta[bucket][name]
        except KeyError:
            return


_store = FakeBlobStore()


class FakeBlobUpath(BlobUpath):
    """This Upath implementation for the FakeBlobstore
    can be used for testing basic functionalities.

    This also showcases the essential methods that
    a concrete subclass of BlobUpath needs to implement."""

    def __init__(self, *parts: str, bucket: str):
        super().__init__(*parts)
        self._bucket = bucket

    def as_uri(self) -> str:
        return f'fake://{self._path}'

    def file_info(self):
        return _store.file_info(self._bucket, self._path)

    def is_file(self) -> bool:
        return _store.exists(self._bucket, self._path)

    def _copy_file(self, source, target, *, overwrite=False):
        if isinstance(source, FakeBlobUpath) and isinstance(target, FakeBlobUpath):
            _store.copy_blob(
                source._bucket, source._path, target._path, overwrite=overwrite
            )
        else:
            super()._copy_file(source, target, overwrite=overwrite)

    @contextlib.contextmanager
    def lock(self, *, timeout=None):
        # place holder
        yield self

    def read_bytes(self) -> bytes:
        try:
            return _store.read_bytes(self._bucket, self._path)
        except ResourceNotFoundError as e:
            raise FileNotFoundError(self) from e

    def riterdir(self) -> Iterator[Self]:
        p = self._path
        if not p.endswith('/'):
            p += '/'
        for pp in _store.list_blobs(self._bucket, p):
            yield self / pp[len(p) :]

    def remove_file(self):
        try:
            _store.delete_blob(self._bucket, self._path)
        except ResourceNotFoundError:
            raise FileNotFoundError(self)

    @property
    def root(self) -> Self:
        return self.__class__('/', bucket=self._bucket)

    def write_bytes(self, data, *, overwrite=False):
        try:
            _store.write_bytes(self._bucket, self._path, data, overwrite=overwrite)
        except ResourceExistsError as e:
            raise FileExistsError(self) from e


def test_all():
    p = FakeBlobUpath('/tmp/test', bucket='bucket_a') / str(uuid4())
    try:
        p.rmrf()
        alltests.test_all(p)
    finally:
        p.rmrf()
