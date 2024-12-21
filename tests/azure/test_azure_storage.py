import io
from datetime import datetime
from types import SimpleNamespace
from uuid import uuid4

import pytest

import cloudly.upathlib._tests as alltests
from cloudly.azure.storage import (
    AzureBlobUpath,
    ResourceExistsError,
    ResourceNotFoundError,
)

CONTAINERS = {}


class ContainerClient:
    def __init__(self, *, container_name, **kwargs):
        self.name = container_name
        CONTAINERS.setdefault(container_name, {})

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        pass

    def list_blobs(self, name_starts_with=None):
        for n in list(CONTAINERS[self.name]):
            # Use `list` to fix the result set.
            # Otherwise there will be "dict size changed during iteration"
            # error.
            if name_starts_with:
                if n.startswith(name_starts_with):
                    yield BlobClient(self.name, n)
            else:
                yield BlobClient(self.name, n)

    def walk_blobs(self, name_starts_with=None):
        z = list(self.list_blobs(name_starts_with))
        k = 0 if not name_starts_with else len(name_starts_with)
        zz = []
        for v in z:
            vv = v.name[k:].split('/')[0]
            zz.append(v.name[:k] + vv)
        return (BlobClient(self.name, v) for v in set(zz))


class BlobClient:
    def __init__(self, container_name, blob_name, **kwargs):
        self._container_name = container_name
        self.name = blob_name
        self.url = container_name + ' ' + blob_name
        if container_name not in CONTAINERS:
            CONTAINERS[container_name] = {}

    def __repr__(self):
        return f'{self.__class__.__name__}({self.name})'

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        pass

    def exists(self):
        return self.name in CONTAINERS[self._container_name]

    def delete_blob(self, **kwargs):
        try:
            del CONTAINERS[self._container_name][self.name]
        except KeyError:
            raise ResourceNotFoundError(self.name)

    def upload_blob(self, data, overwrite=False, **kwargs):
        if not overwrite and self.exists():
            raise ResourceExistsError(self.name)
        if isinstance(data, io.BufferedReader):
            data = data.read()
        CONTAINERS[self._container_name][self.name] = {
            'data': data,
            'time_created': datetime.now(),
            'time_modified': datetime.now(),
            'size': len(data),
        }

    def download_blob(self):
        z = CONTAINERS[self._container_name][self.name]['data']

        class Foo:
            def __init__(self, data):
                self._data = data

            def readall(self):
                return self._data

            def readinto(self, f):
                f.write(self._data)

        return Foo(z)

    def get_blob_properties(self):
        try:
            me = CONTAINERS[self._container_name][self.name]
            info = SimpleNamespace()
            info.creation_time = me['time_created']
            info.last_modified = me['time_modified']
            info.size = me['size']
            return info
        except KeyError:
            raise ResourceNotFoundError(self.name)

    def start_copy_from_url(self, url, **kwargs):
        source = BlobClient(*url.split())
        self.upload_blob(source.download_blob().readall())
        return {'copy_status': 'success'}


class BlobLeaseClient:
    pass


@pytest.fixture()
def azure(mocker):
    mocker.patch('cloudly.azure.storage.ContainerClient', ContainerClient)
    mocker.patch('cloudly.azure.storage.BlobClient', BlobClient)
    mocker.patch('cloudly.azure.storage.BlobLeaseClient', BlobLeaseClient)
    mocker.patch('cloudly.azure.storage.AzureBlobUpath._ACCOUNT_NAME', 'abc')
    mocker.patch('cloudly.azure.storage.AzureBlobUpath._ACCOUNT_KEY', 'xyz')
    mocker.patch('cloudly.azure.storage.AzureBlobUpath._SAS_TOKEN', '1010')

    c = AzureBlobUpath(
        '/tmp/test',
        container_name='test',
    ) / str(uuid4())
    try:
        c.rmrf()
        yield c
    finally:
        c.rmrf()


def test_all(azure):
    alltests.test_all(azure)


# def test_lock(azure):
#     alltests.test_lock(azure)
