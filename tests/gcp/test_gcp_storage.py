from datetime import datetime
from io import BytesIO
from uuid import uuid4

import pytest
from google.api_core.exceptions import NotFound

import cloudly.upathlib._tests as alltests
from cloudly.gcp.storage import GcsBlobUpath


class Blob:
    def __init__(self, name, bucket):
        self.name = name
        self._bucket = bucket
        self._updated = None
        self._generation = None
        self._time_created = None
        self._size = None

    def delete(self, client=None, if_generation_match=None):
        try:
            del self._bucket._blobs[self.name]
        except KeyError:
            raise NotFound(self.name)

    def exists(self, client=None):
        return self.name in self._bucket._blobs

    def reload(self, client=None, timeout=None, retry=None):
        if not self.exists(client):
            raise NotFound(self.name)
        meta = self._bucket._blobs[self.name]
        self._updated = meta['time_updated']
        self._generation = meta['generation']
        self._time_created = meta['time_created']
        self._size = meta['size']

    def _get_content_type(self, content_type, filename):
        return 'ok'

    def upload_from_file(self, data, *, if_generation_match=None, **ignore):
        if if_generation_match == 0 and self.name in self._bucket._blobs:
            raise FileExistsError(self.name)
        data = data.read()
        gen = self._bucket._blobs.get(self.name, {'generation': 0})['generation']
        self._bucket._blobs[self.name] = {
            'data': data,
            'time_created': datetime.now(),
            'time_updated': datetime.now(),
            'size': len(data),
            'generation': gen + 1,
        }

    def download_to_file(self, file_obj, client=None, raw_download=None):
        try:
            z = self._bucket._blobs[self.name]
            file_obj.write(z['data'])
        except KeyError:
            raise NotFound(self.name)

    @property
    def time_created(self):
        return self._time_created

    @property
    def updated(self):
        return self._updated

    @property
    def size(self):
        return self._size

    @property
    def _properties(self):
        return {}

    @property
    def generation(self):
        return self._generation


bucket_blobs = {}


class Bucket:
    def __init__(self, name):
        self._name = name
        bucket_blobs.setdefault(name, {})
        self._blobs = bucket_blobs[name]

    def blob(self, name):
        return Blob(name, self)

    def copy_blob(
        self,
        blob,
        target_bucket,
        target_blob_name,
        client=None,
        if_generation_match=None,
    ):
        buffer = BytesIO()
        blob.download_to_file(buffer)
        buffer.seek(0)
        target_bucket.blob(target_blob_name).upload_from_file(
            buffer, if_generation_match=if_generation_match
        )


class Page:
    def __init__(self, blob_name):
        self.name = blob_name

    @property
    def prefixes(self):
        yield self.name


class BlobLists:
    def __init__(self, bucket, prefix, delimiter=None, max_results=None):
        self._bucket = bucket
        self._prefix = prefix
        self._delimiter = delimiter
        self._max_results = max_results

    def __iter__(self):
        zz = [x for x in self._bucket._blobs if x.startswith(self._prefix)]
        if not self._delimiter:
            yield from (self._bucket.blob(z) for z in zz)
        else:
            k = len(self._prefix)
            yield from (
                self._bucket.blob(z) for z in zz if self._delimiter not in z[k:]
            )

    @property
    def prefixes(self):
        if self._delimiter:
            k = len(self._prefix)
            zz = []
            for x in self._bucket._blobs:
                if x.startswith(self._prefix):
                    n = x[k:]
                    if self._delimiter in n:
                        kk = n.find(self._delimiter)
                        zz.append(x[: (k + kk + 1)])
            zz = set(zz)
            return zz
        return []


class Client:
    def __init__(self, *args, **kwargs):
        pass

    def bucket(self, name, user_project):
        return Bucket(name)

    def list_blobs(
        self,
        bucket,
        prefix,
        delimiter=None,
        max_results=None,
        page_size=None,
        fields=None,
    ):
        return BlobLists(bucket, prefix, delimiter, max_results=max_results)


@pytest.fixture()
def gcp(mocker):
    print()
    mocker.patch('cloudly.gcp.storage.storage.Client', Client)
    mocker.patch('cloudly.gcp.storage.get_project_id', lambda: 'abc')
    mocker.patch(
        'cloudly.gcp.storage.get_credentials', lambda return_state: (None, False)
    )
    mocker.patch('cloudly.gcp.storage.GcsBlobUpath._CLIENT', Client())
    c = GcsBlobUpath(
        '/tmp/test',
        bucket_name='test',
    ) / str(uuid4())
    print('c._bucket', c._bucket())
    c.rmrf()
    try:
        yield c
    finally:
        c.rmrf()


def test_all(gcp):
    alltests.test_all(gcp)


# def test_lock(gcp):
#     upathlib._tests.test_lock(gcp)
