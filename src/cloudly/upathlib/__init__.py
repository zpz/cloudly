"""
*upathlib*
defines a unified API for cloud blob store (aka "object store") as well as local file systems.

Attention is focused on identifying the *most essential* functionalities
while working with a blob store for data processing.
Functionalities in a traditional local file system that are secondary in these tasks---such
as symbolic links, fine-grained permissions, and various access modes---are ignored.

End user should look to the class :class:`~cloudly.upathlib.Upath` for documentation of the API.
Local file system is implemented by :class:`LocalUpath`, which subclasses Upath.
Client for Google Cloud Storage (i.e. blob store on GCP) is implemented by another Upath subclass,
namely :class:`~cloudly.upathlib.GcsBlobUpath`.

One use case is the module `cloudly.biglist`,
where the class `Biglist` takes a `Upath` object to indicate its location of storage.
It does not care whether the storage is local or in a cloud blob store---it
simply uses the common API to operate the storage.
"""

__all__ = [
    'Upath',
    'LocalUpath',
    'BlobUpath',
    'PathType',
    'LocalPathType',
    'resolve_path',
    'FileInfo',
    'LockAcquireError',
    'LockReleaseError',
    'serializer',
    'versioned_uploadable',
]


from pathlib import Path

from . import serializer, versioned_uploadable
from ._blob import BlobUpath
from ._local import LocalPathType, LocalUpath
from ._upath import FileInfo, LockAcquireError, LockReleaseError, PathType, Upath


def resolve_path(path: PathType) -> Upath:
    if isinstance(path, str):
        if path.startswith('gs://'):
            # If you encounter a "gs://..." path but
            # you haven't installed GCS dependencies,
            # you'll get an exception!
            from cloudly.gcp.storage import GcsBlobUpath

            return GcsBlobUpath(path)
        if path.startswith('s3://'):
            raise NotImplementedError('AWS S3 storage is not implemented')
        if path.startswith('https://'):
            if 'blob.core.windows.net' in path:
                from cloudly.azure.storage import AzureBlobUpath

                return AzureBlobUpath(path)
            raise ValueError(f"unrecognized value: '{path}'")
        path = Path(path)
    if isinstance(path, Path):
        return LocalUpath(str(path.resolve().absolute()))
    if not isinstance(path, Upath):
        raise TypeError(f'`{type(path)}` is provided while `{PathType}` is expected')
    return path
