# User runs these tests with a `Upath` object of their subclass,
# in a path that is safe for testing.

import concurrent.futures
import os
import pathlib
import random
import time
from uuid import uuid4

import pytest

from cloudly.upathlib import LocalUpath, LockAcquireError, Upath

IS_WIN = os.name != 'posix'


def test_basic(p: Upath):
    pp = p / '/abc/def/'
    if isinstance(pp, LocalUpath):
        assert pp.path == pathlib.Path('/abc/def').absolute()
    else:
        assert pp.path == pathlib.PurePath('/abc/def')
    print(repr(pp))

    pp = pp / 'x/y/z'
    if isinstance(pp, LocalUpath):
        assert pp.path == pathlib.Path('/abc/def/x/y/z').absolute()
    else:
        assert pp.path == pathlib.PurePath('/abc/def/x/y/z')

    print(repr(pp))

    pp /= 'xy/z'
    if isinstance(pp, LocalUpath):
        assert str(pp.path) == str(pathlib.Path('/abc/def/x/y/z/xy/z').absolute())
    else:
        assert str(pp.path) == '/abc/def/x/y/z/xy/z'

    assert pp._path == str(pp.path)
    pp /= '..'
    if isinstance(pp, LocalUpath):
        assert pp._path == str(pathlib.Path('/abc/def/x/y/z/xy').absolute())
    else:
        assert pp._path == '/abc/def/x/y/z/xy'

    if isinstance(pp, LocalUpath):
        pp.joinpath('..')._path == str(pathlib.Path('/abc/def/x/y/z').absolute())
        pp.joinpath('..', '..', '..', '..', '..')._path == str(
            pathlib.Path('/').absolute()
        )
    else:
        pp.joinpath('..')._path == '/abc/def/x/y/z'
        pp.joinpath('..', '..', '..', '..', '..')._path == '/'


def test_joinpath(path: Upath):
    try:
        pp = path.joinpath('/abc/def/', 'x/y') / 'ab.txt'
        if isinstance(pp, LocalUpath):
            assert str(pp.path) == str(pathlib.Path('/abc/def/x/y/ab.txt').absolute())
        else:
            assert str(pp.path) == '/abc/def/x/y/ab.txt'

        pp = pp.joinpath('../a/b.txt')
        assert pp == path / '/abc/def' / 'x/y/a/b.txt'
        assert pp.name == 'b.txt'
        assert pp.suffix == '.txt'

        p = pp

        pp = pp / '../../../../'
        if isinstance(pp, LocalUpath):
            assert str(pp.path) == str(pathlib.Path('/abc/def').absolute())
        else:
            assert str(pp.path) == '/abc/def'

        pp = p.joinpath('a', '.', 'b/c.data')
        if isinstance(pp, LocalUpath):
            assert str(pp.path) == str(
                pathlib.Path('/abc/def/x/y/a/b.txt/a/b/c.data').absolute()
            )
        else:
            assert str(pp.path) == '/abc/def/x/y/a/b.txt/a/b/c.data'

    except Exception:
        print('')
        print('repr:  ', repr(pp))
        print('str:   ', str(pp))
        print('path:  ', pp.path)
        print('_path: ', pp._path)
        raise


def test_compare(p: Upath):
    assert p.joinpath('abc/def') / 'x/y/z' == p / 'abc/def/x/y' / 'z'
    assert p / 'abc/def' < p.joinpath('abc/def', 'x')
    assert p.joinpath('abc/def/x', 'y/z') > p.joinpath('abc/def', 'x/y')


def test_read_write_rm_navigate(p: Upath):
    init_path = p._path

    p.rmrf()

    p1 = p / 'abc.txt'
    assert not p1.exists()
    p1.write_text('abc')

    assert p1.is_file()
    assert not p1.is_dir()

    assert p1.exists()
    assert p1.read_text() == 'abc'

    with pytest.raises(FileExistsError):
        p1.write_text('abcd')

    p1.write_json({'data': 'abcd'}, overwrite=True)  # type: ignore
    assert p1.read_json() == {'data': 'abcd'}  # type: ignore

    p /= 'a'
    if isinstance(p, LocalUpath):
        assert p._path == str(pathlib.Path(f'{init_path}/a').absolute())
    else:
        assert p._path == f'{init_path}/a'

    assert not p.is_file()
    assert not p.is_dir()
    assert not p.exists()

    time.sleep(0.3)
    # wait to ensure the next file as diff timestamp
    # from the first file.

    p2 = p.joinpath('x.data')
    p2.write_bytes(b'x')
    p /= '..'
    assert p._path == init_path
    assert p2 == p.joinpath('a', 'x.data')
    assert p2.read_bytes() == b'x'

    p3 = p / 'a'

    assert p.ls() == [p3, p1]

    assert sorted(p.riterdir()) == [p2, p1]
    assert p3.file_info() is None
    fi1 = p1.file_info()
    fi2 = p2.file_info()
    print('')
    print('p1:', fi1)
    print('p2:', fi2)
    print('')
    assert fi1.mtime < fi2.mtime  # type: ignore
    print('file 1 size:', fi1.size)  # type: ignore
    print('file 2 size:', fi2.size)  # type: ignore
    assert fi1.size > fi2.size  # type: ignore

    assert p3.is_dir()
    assert p3.remove_dir() == 1
    assert not p3.exists()
    assert not p2.exists()

    assert p3.remove_dir() == 0
    assert p.ls() == [p1]
    assert p1.remove_dir() == 0
    p1.remove_file()
    with pytest.raises(FileNotFoundError):
        p1.remove_file()
    assert p.rmrf() == 0


def test_copy(p: Upath):
    source = p
    source.rmrf()

    target = LocalUpath('/tmp/upath-test-target') / str(uuid4())
    target.rmrf()
    try:
        source_file = source / 'testfile'
        source_file.write_text('abc', overwrite=True)

        target.copy_file(source_file)
        assert target.read_text() == 'abc'

        with pytest.raises(FileNotFoundError if IS_WIN else NotADirectoryError):
            # cant' write to `target/'samplefile'`
            # because `target` is a file.
            target.joinpath('samplefile').copy_dir(source)

        target.rmrf()
        p2 = target.joinpath('samplefile')
        p2.copy_dir(source)
        p3 = p2 / source_file.name
        assert target.ls() == [p2]
        assert p2.ls() == [p3]
        assert p3.read_text() == 'abc'

        p1 = source / 'a' / 'b' / 'c'
        assert p1.copy_dir(p2) == 1
        p4 = p1 / source_file.name
        assert p4.read_text() == 'abc'

        assert (source / 'a' / 'b').copy_dir(p2) == 1
        assert (source / 'a' / 'b' / source_file.name).read_text() == 'abc'
    finally:
        target.rmrf()


def _access_in_mp(root: Upath, path: str, timeout):
    p = root / path
    t0 = time.perf_counter()
    try:
        with p.lock(timeout=timeout):
            return time.perf_counter() - t0
    except LockAcquireError:
        return t0 - time.perf_counter()


def test_lock1(p: Upath, timeout=None, wait=8):
    p.rmrf()
    pp = p / 'testlock'
    with pp.lock(timeout=timeout):
        with concurrent.futures.ProcessPoolExecutor(1) as pool:
            t = pool.submit(_access_in_mp, p / '/', pp._path, wait)
            z = t.result()
            print('mp returned after', z, 'seconds')
            # The work is not able to acquire the lock:
            assert z <= -(wait / 2)
    if not isinstance(p, LocalUpath):
        assert not pp.exists()


def _inc_in_mp(counter, idx):
    t0 = time.perf_counter()
    n = 0
    while time.perf_counter() - t0 < 5:
        with counter.lock():
            x = counter.read_text()
            print('x:', x, 'worker', idx, flush=True)
            time.sleep(random.random() * 0.1)
            counter.write_text(str(int(x) + 1), overwrite=True)
            n += 1
            print('        worker', idx, n, flush=True)
        time.sleep(random.random() * 0.1)
    return idx, n


def test_lock2(p: Upath):
    p.rmrf()
    counter = p / 'counter'
    counter.write_text('0')
    time.sleep(0.2)
    with concurrent.futures.ProcessPoolExecutor(30) as pool:
        tt = [pool.submit(_inc_in_mp, counter, i) for i in range(30)]
        results = [t.result() for t in tt]
        print('results:')
        for v in sorted(results):
            print(v)
        total1 = sum(v[1] for v in results)
        total2 = int(counter.read_text())
        print('')
        print(total1, total2)
        assert total1 == total2
    if not isinstance(p, LocalUpath):
        assert not counter.with_suffix('.lock').exists()


def test_lock(p: Upath):
    test_lock1(p)
    test_lock2(p)


def test_all(p: Upath):
    test_basic(p)
    test_joinpath(p)
    test_compare(p)

    test_read_write_rm_navigate(p)
    test_copy(p)

    # test_lock(p)
