from pathlib import Path

from cloudly.upathlib import LocalUpath, resolve_path
from cloudly.gcp.storage import GcsBlobUpath


def test_resolve_path():
    p = resolve_path("/abc/de")
    assert isinstance(p, LocalUpath)
    p = resolve_path(Path("./ab"))
    assert isinstance(p, LocalUpath)
    p = resolve_path(LocalUpath("./a"))
    assert isinstance(p, LocalUpath)

    p = resolve_path("gs://mybucket/abc")
    assert isinstance(p, GcsBlobUpath)
