import gc
import json
import pickle
import threading
import zlib
from contextlib import contextmanager
from typing import Protocol, TypeVar

import zstandard

# zstandard has good compression ratio and also quite fast.
# It is very "balanced".
# lz4 has lower compression ratio than zstandard but is much faster.
#
# See:
#   https://gregoryszorc.com/blog/2017/03/07/better-compression-with-zstandard/
#   https://stackoverflow.com/questions/67537111/how-do-i-decide-between-lz4-and-snappy-compression
#   https://gist.github.com/oldcai/7230548

T = TypeVar('T')


MEGABYTE = 1048576  # 1024 * 1024
ZLIB_LEVEL = 3  # official default is 6
ZSTD_LEVEL = 3  # official default is 3
LZ4_LEVEL = (
    0  # official default is 0; high-compression value is 3, much slower at compressing
)
PICKLE_PROTOCOL = pickle.HIGHEST_PROTOCOL


@contextmanager
def _gc(data):
    turnedoff = False
    if len(data) >= MEGABYTE * 10 and gc.isenabled():
        gc.disable()
        turnedoff = True
    try:
        yield
    finally:
        if turnedoff:
            gc.enable()


class Serializer(Protocol):
    @classmethod
    def serialize(cls, x: T, **kwargs) -> bytes: ...

    @classmethod
    def deserialize(cls, y: bytes, **kwargs) -> T: ...

    @classmethod
    def dump(cls, x: T, file, *, overwrite: bool = False, **kwargs) -> None:
        # `file` is a `Upath` object.
        y = cls.serialize(x, **kwargs)
        file.write_bytes(y, overwrite=overwrite)

    @classmethod
    def load(cls, file, **kwargs) -> T:
        # `file` is a `Upath` object.
        y = file.read_bytes()
        return cls.deserialize(y, **kwargs)


class JsonSerializer(Serializer):
    @classmethod
    def serialize(cls, x, *, encoding=None, errors=None, **kwargs) -> bytes:
        return json.dumps(x, **kwargs).encode(
            encoding=encoding or 'utf-8', errors=errors or 'strict'
        )

    @classmethod
    def deserialize(cls, y, *, encoding=None, errors=None, **kwargs):
        with _gc(y):
            return json.loads(
                y.decode(encoding=encoding or 'utf-8', errors=errors or 'strict'),
                **kwargs,
            )


class PickleSerializer(Serializer):
    @classmethod
    def serialize(cls, x, *, protocol=None, **kwargs) -> bytes:
        return pickle.dumps(x, protocol=protocol or PICKLE_PROTOCOL, **kwargs)

    @classmethod
    def deserialize(cls, y, **kwargs):
        with _gc(y):
            return pickle.loads(y, **kwargs)


class ZPickleSerializer(PickleSerializer):
    # In general, this is not the best choice of compression.
    # Use `zstandard` or `lz4 instead.
    @classmethod
    def serialize(cls, x, *, level=ZLIB_LEVEL, **kwargs) -> bytes:
        y = super().serialize(x, **kwargs)
        return zlib.compress(y, level=level)

    @classmethod
    def deserialize(cls, y, **kwargs):
        y = zlib.decompress(y)
        return super().deserialize(y, **kwargs)


class ZstdCompressor(threading.local):
    # See doc string in ``cpython / Lib / _threading_local.py``.

    # See doc on `ZstdCompressor` and `ZstdDecompressor` in
    # (github python-zstandard) `zstandard / backend_cffi.py`.

    # The `ZstdCompressor` and `ZstdDecompressor` objects can't be pickled.
    # If there are issues related to forking, check out ``os.register_at_fork``.

    def __init__(self):
        self._compressor: dict[tuple[int, int], zstandard.ZstdCompressor] = {}
        self._decompressor: zstandard.ZstdDecompressor = None

    def compress(self, x, *, level=ZSTD_LEVEL, threads=0):
        """
        Parameters
        ----------
        threads
            Number of threads to use to compress data concurrently. When set,
            compression operations are performed on multiple threads. The default
            value (0) disables multi-threaded compression. A value of ``-1`` means
            to set the number of threads to the number of detected logical CPUs.
        """
        c = self._compressor.get((level, threads))
        if c is None:
            c = zstandard.ZstdCompressor(level=level, threads=threads)
            self._compressor[(level, threads)] = c
        return c.compress(x)

    def decompress(self, y):
        if self._decompressor is None:
            self._decompressor = zstandard.ZstdDecompressor()
        return self._decompressor.decompress(y)


class ZstdPickleSerializer(PickleSerializer):
    _compressor = ZstdCompressor()

    @classmethod
    def serialize(cls, x, *, level=ZSTD_LEVEL, threads=0, **kwargs) -> bytes:
        y = super().serialize(x, **kwargs)
        return cls._compressor.compress(y, level=level, threads=threads)

    @classmethod
    def deserialize(cls, y, **kwargs):
        y = cls._compressor.decompress(y)
        return super().deserialize(y, **kwargs)


try:
    # To use this, just install the Python package `lz4`.
    import lz4.frame
except ImportError:
    pass
else:

    class Lz4PickleSerializer(PickleSerializer):
        @classmethod
        def serialize(cls, x, *, level=LZ4_LEVEL, **kwargs) -> bytes:
            y = super().serialize(x, **kwargs)
            return lz4.frame.compress(y, compression_level=level)

        @classmethod
        def deserialize(cls, y, **kwargs):
            y = lz4.frame.decompress(y)
            return super().deserialize(y, **kwargs)


try:
    # To use this, just install the Python package `orjson`.
    import orjson
except ImportError:
    pass
else:

    class OrjsonSerializer(Serializer):
        @classmethod
        def serialize(cls, x, **kwargs) -> bytes:
            return orjson.dumps(x, **kwargs)

        @classmethod
        def deserialize(cls, y: bytes, **kwargs):
            return orjson.loads(y, **kwargs)

    class ZOrjsonSerializer(OrjsonSerializer):
        # In general, this is not the best choice of compression.
        # Use `zstandard` or `lz4 instead.
        @classmethod
        def serialize(cls, x, *, level=ZLIB_LEVEL, **kwargs) -> bytes:
            y = super().serialize(x, **kwargs)
            return zlib.compress(y, level=level)

        @classmethod
        def deserialize(cls, y, **kwargs):
            y = zlib.decompress(y)
            return super().deserialize(y, **kwargs)

    class ZstdOrjsonSerializer(OrjsonSerializer):
        _compressor = ZstdCompressor()

        @classmethod
        def serialize(cls, x, *, level=ZSTD_LEVEL, threads=0, **kwargs) -> bytes:
            y = super().serialize(x, **kwargs)
            return cls._compressor.compress(y, level=level, threads=threads)

        @classmethod
        def deserialize(cls, y, **kwargs):
            y = cls._compressor.decompress(y)
            return super().deserialize(y, **kwargs)

    try:
        # To use this, just install the Python package `lz4`.
        import lz4.frame
    except ImportError:
        pass
    else:

        class Lz4OrjsonSerializer(OrjsonSerializer):
            @classmethod
            def serialize(cls, x, *, level=LZ4_LEVEL, **kwargs) -> bytes:
                y = super().serialize(x, **kwargs)
                return lz4.frame.compress(y, compression_level=level)

            @classmethod
            def deserialize(cls, y, **kwargs):
                y = lz4.frame.decompress(y)
                return super().deserialize(y, **kwargs)
