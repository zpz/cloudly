import os
import pathlib
from uuid import uuid4

import pytest

import cloudly.upathlib._tests as alltests
from cloudly.upathlib import LocalUpath, serializer


@pytest.fixture
def test_path():
    if os.name == "posix":
        p = LocalUpath("/tmp/upathlib_local_test") / str(uuid4())
    else:
        p = LocalUpath(str(pathlib.Path.home() / "tmp/upathlib_local_test")) / str(
            uuid4()
        )
    try:
        p.rmrf()
        yield p
    finally:
        p.rmrf()


def test_localupath_init():
    p = LocalUpath()
    assert p._path == str(pathlib.Path.cwd())
    p = LocalUpath("a", "b", "c", "d")
    assert str(p.path) == str(pathlib.Path(pathlib.Path.cwd(), "a", "b", "c", "d"))


def test_all(test_path):
    alltests.test_all(test_path)


def test_lock():
    test_path = LocalUpath("/tmp/upathlib_local_test") / "test-lock"
    # Use a fix path to see whether there are issues, because
    # the implementation leaves the lock file around.
    alltests.test_lock(test_path)


def test_rename(test_path):
    p = test_path
    p.rmrf()

    (p / "a/a.txt").write_text("a")
    (p / "b/b.txt").write_text("b")
    (p / "c/d/e.txt").write_text("e")
    (p / "c/d.txt").write_text("d")

    assert (p / "a/a.txt").read_text() == "a"

    p.joinpath("a/a.txt").rename_file("b/a.txt")
    assert not (p / "a/a.txt").exists()
    assert (p / "a/b/a.txt").read_text() == "a"

    pp = (p / "c").rename_dir("a/c")

    assert (pp / "d/e.txt").read_text() == "e"
    assert (pp / "d.txt").read_text() == "d"
    assert not (p / "c").exists()


def test_pathlike(test_path):
    p = test_path
    p.write_text("abc")
    with open(p) as file:
        assert file.read() == "abc"


def test_pickle(test_path):
    p = test_path
    p.rmrf()
    pp = p / "data.dat"

    data = [1, 2, {"a": 3, "b": [1, "c"]}]

    pp.write_pickle(data, overwrite=True)
    assert pp.read_pickle() == data
    pp.write_pickle_zstd(data, overwrite=True)
    assert pp.read_pickle_zstd() == data

    pp.write_json(data, overwrite=True)
    assert pp.read_json() == data

    serializer.Lz4PickleSerializer.dump(data, pp, overwrite=True)
    assert serializer.Lz4PickleSerializer.load(pp) == data
