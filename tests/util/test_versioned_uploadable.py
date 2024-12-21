import os.path

from cloudly.upathlib import LocalUpath
from cloudly.util.versioned_uploadable import VersionedUploadable

joinpath = os.path.join


class MyVersionedUploadable(VersionedUploadable):
    # Because we don't have a cloud storage for testing,
    # we designate one directory on the local disk to play the role
    # of "remote storage".
    @classmethod
    def local_cls_upath(cls) -> LocalUpath:
        return LocalUpath(
            "/tmp/upathlib-test/local/data/versioned-uploadables",
            cls.__name__,
        )

    @classmethod
    def remote_cls_upath(cls) -> LocalUpath:
        return LocalUpath(
            "/tmp/upathlib-test/remote/data/versioned-uploadables",
            cls.__name__,
        )


def test_vu():
    box = MyVersionedUploadable.new()
    box.path("abc.txt").write_text("abc")
    box.path("sub/abc.pkl").write_pickle({"x": "abc"})
    box.path("sub/deep/abc.json").write_json({"y": "xyz"})
    assert not MyVersionedUploadable.has_local_version(box.version)

    box.save()
    assert MyVersionedUploadable.has_local_version(box.version)

    assert box.path("abc.txt").read_text() == "abc"
    assert box.path("sub/abc.pkl").read_pickle() == {"x": "abc"}
    assert box.path("sub/deep/abc.json").read_json() == {"y": "xyz"}

    n = box.upload()
    assert n == 4  # including 'info.json'

    rbox = MyVersionedUploadable(box.version, remote=True)

    assert rbox.path("abc.txt").read_text() == "abc"
    assert rbox.path("sub/abc.pkl").read_pickle() == {"x": "abc"}
    assert rbox.path("sub/deep/abc.json").read_json() == {"y": "xyz"}

    rbox.path("sub/def.txt").write_text("def")
    assert rbox.path("sub/def.txt").read_text() == "def"

    n = rbox.download("sub/def.txt")
    assert n == 1
    assert box.path("sub/def.txt").read_text() == "def"

    n = rbox.download()
    rbox.path("abc.txt").write_text("123", overwrite=True)
    n = rbox.download()
    assert n == 0

    n = rbox.download(overwrite=True)
    assert n == 5

    box.path("new/good/data1.txt").write_text("yes")
    box.path("new/bad/data2.txt").write_text("no")
    n = box.upload("new")
    assert n == 2

    assert rbox.path("new/good/data1.txt").read_text() == "yes"

    (box.upath / "sub/def.txt").remove_file()
    assert not box.path("sub/def.txt").is_file()
    (rbox.upath / "sub/def.txt").remove_file()
    assert not rbox.path("sub/def.txt").is_file()

    MyVersionedUploadable.remove_local_version(box.version)
    assert not MyVersionedUploadable.has_local_version(box.version)

    n = rbox.download()
    assert n == 6
    assert MyVersionedUploadable.has_local_version(rbox.version)

    MyVersionedUploadable.remove_remote_version(rbox.version)
    assert not MyVersionedUploadable.has_remote_version(rbox.version)

    box.remove_local_version(box.version)
    assert not box.has_local_version(box.version)
