import os
import os.path
import uuid
from shutil import rmtree
from tempfile import mkdtemp

import pytest

from cloudly.gcp.storage import GcsBlobUpath


def test_upload():
    blob = GcsBlobUpath('/test/upathlib/gcp', str(uuid.uuid4()), bucket_name='zpz-tmp')
    blob.rmrf()
    try:
        file = '/tmp/gcp/testput'
        if os.path.isfile(file):
            os.remove(file)
        os.makedirs('/tmp/gcp', exist_ok=True)
        open(file, 'w').write('abc')

        blob.joinpath(os.path.basename(file)).upload_file(file)
        blob.joinpath('file2.txt').upload_file(file)

        assert sorted(blob.iterdir()) == [
            blob / 'file2.txt',
            blob / 'testput',
        ]

        blob /= 'ab/cd/ef'
        blob.joinpath('../xy/file2.txt').upload_file(file)
        assert blob.joinpath('../xy/file2.txt').exists()

        blob /= '../xy'
        assert blob.joinpath('../xy/file2.txt').exists()

        blob.joinpath('newone/').upload_file(file)
        assert blob.joinpath('newone').exists()

        os.remove(file)
        with pytest.raises(FileNotFoundError):
            blob.joinpath('tt').upload_file(file)
    finally:
        blob.rmrf()


def test_upload_download_dir():
    blob = GcsBlobUpath('/test/upathlib/gcp', str(uuid.uuid4()), bucket_name='zpz-tmp')
    blob.rmrf()
    try:
        local_root = mkdtemp()

        os.makedirs(os.path.join(local_root, 'a', 'b', 'c'))
        os.makedirs(os.path.join(local_root, 'd'))
        with open(os.path.join(local_root, 'a', 'a.txt'), 'w') as f:
            f.write('aa')
        with open(os.path.join(local_root, 'a', 'b', 'ab.txt'), 'w') as f:
            f.write('aa')
        with open(os.path.join(local_root, 'a', 'b', 'c', 'abc.txt'), 'w') as f:
            f.write('aa')
        with open(os.path.join(local_root, 'd', 'd.txt'), 'w') as f:
            f.write('aa')

        blob /= os.path.basename(local_root)

        blob.upload_dir(local_root)
        rmtree(local_root)
        assert sorted(blob.iterdir()) == sorted([blob / 'a', blob / 'd'])

        local_root = mkdtemp()

        blob.joinpath('a').download_dir(os.path.join(local_root, 'a'), overwrite=True)
        assert os.path.isdir(os.path.join(local_root, 'a'))
        assert os.path.isdir(os.path.join(local_root, 'a/b'))
        assert os.path.isfile(os.path.join(local_root, 'a/b', 'ab.txt'))
        assert os.path.isdir(os.path.join(local_root, 'a', 'b', 'c'))
        assert os.path.isfile(os.path.join(local_root, 'a', 'b', 'c', 'abc.txt'))

        blob.joinpath('d').download_dir(os.path.join(local_root, 'd'), overwrite=True)
        assert os.path.isfile(os.path.join(local_root, 'd', 'd.txt'))
        assert os.path.isfile(os.path.join(local_root, 'a/a.txt'))
        assert os.path.isdir(os.path.join(local_root, 'a/b'))
        assert os.path.isfile(os.path.join(local_root, 'a', 'b', 'ab.txt'))
        assert os.path.isdir(os.path.join(local_root, 'a', 'b', 'c'))
        assert os.path.isfile(os.path.join(local_root, 'a', 'b', 'c', 'abc.txt'))
    finally:
        blob.rmrf()
