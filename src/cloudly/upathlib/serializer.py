import gc
import io
import json
import pickle
import threading
import zlib
from collections.abc import Iterable, Sequence
from contextlib import contextmanager
from typing import Protocol, TypeVar

import pyarrow
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


def make_parquet_type(type_spec: str | Sequence):
    """
    ``type_spec`` is a spec of arguments to one of pyarrow's data type
    `factory functions <https://arrow.apache.org/docs/python/api/datatypes.html#factory-functions>`_.

    For simple types, this may be just the type name (or function name), e.g. ``'bool_'``, ``'string'``, ``'float64'``.

    For type functions expecting arguments, this is a list or tuple with the type name followed by other arguments,
    for example,

    ::

        ('time32', 's')
        ('decimal128', 5, -3)

    For compound types (types constructed by other types), this is a "recursive" structure, such as

    ::

        ('list_', 'int64')
        ('list_', ('time32', 's'), 5)

    where the second element is the spec for the member type, or

    ::

        ('map_', 'string', ('list_', 'int64'), True)

    where the second and third elements are specs for the key type and value type, respectively,
    and the fourth element is the optional argument ``keys_sorted`` to
    `pyarrow.map_() <https://arrow.apache.org/docs/python/generated/pyarrow.map_.html#pyarrow.map_>`_.
    Below is an example of a struct type::

        ('struct', [('name', 'string', False), ('age', 'uint8', True), ('income', ('struct', (('currency', 'string'), ('amount', 'uint64'))), False)])

    Here, the second element is the list of fields in the struct.
    Each field is expressed by a spec that is taken by :meth:`make_parquet_field`.
    """
    if isinstance(type_spec, str):
        type_name = type_spec
        args = ()
    else:
        type_name = type_spec[0]
        args = type_spec[1:]

    if type_name in ('string', 'float64', 'bool_', 'int8', 'int64', 'uint8', 'uint64'):
        assert not args
        return getattr(pyarrow, type_name)()

    if type_name == 'list_':
        if len(args) > 2:
            raise ValueError(f"'pyarrow.list_' expects 1 or 2 args, got `{args}`")
        return pyarrow.list_(make_parquet_type(args[0]), *args[1:])

    if type_name in ('map_', 'dictionary'):
        if len(args) > 3:
            raise ValueError(f"'pyarrow.{type_name}' expects 2 or 3 args, got `{args}`")
        return getattr(pyarrow, type_name)(
            make_parquet_type(args[0]),
            make_parquet_type(args[1]),
            *args[2:],
        )

    if type_name == 'struct':
        assert len(args) == 1
        return pyarrow.struct((make_parquet_field(v) for v in args[0]))

    if type_name == 'large_list':
        assert len(args) == 1
        return pyarrow.large_list(make_parquet_type(args[0]))

    if type_name in (
        'int16',
        'int32',
        'uint16',
        'uint32',
        'float32',
        'date32',
        'date64',
        'month_day_nano_interval',
        'utf8',
        'large_binary',
        'large_string',
        'large_utf8',
        'null',
    ):
        assert not args
        return getattr(pyarrow, type_name)()

    if type_name in ('time32', 'time64', 'duration'):
        assert len(args) == 1
    elif type_name in ('timestamp', 'decimal128'):
        assert len(args) in (1, 2)
    elif type_name in ('binary',):
        assert len(args) <= 1
    else:
        raise ValueError(f"unknown pyarrow type '{type_name}'")
    return getattr(pyarrow, type_name)(*args)


def make_parquet_field(field_spec: Sequence):
    """
    ``filed_spec`` is a list or tuple with 2, 3, or 4 elements.
    The first element is the name of the field.
    The second element is the spec of the type, to be passed to function :func:`make_parquet_type`.
    Additional elements are the optional ``nullable`` and ``metadata`` to the function
    `pyarrow.field() <https://arrow.apache.org/docs/python/generated/pyarrow.field.html#pyarrow.field>`_.
    """
    field_name = field_spec[0]
    type_spec = field_spec[1]
    assert len(field_spec) <= 4  # two optional elements are `nullable` and `metadata`.
    return pyarrow.field(field_name, make_parquet_type(type_spec), *field_spec[3:])


def make_parquet_schema(fields_spec: Iterable[Sequence]):
    """
    This function constructs a pyarrow schema that is expressed by simple Python types
    that can be json-serialized.

    ``fields_spec`` is a list or tuple, each of its elements accepted by :func:`make_parquet_field`.

    This function is motivated by the need of :class:`~biglist._biglist.ParquetSerializer`.
    When :class:`biglist.Biglist` uses a "storage-format" that takes options (such as 'parquet'),
    these options can be passed into :func:`biglist.Biglist.new` (via ``serialize_kwargs`` and ``deserialize_kwargs``) and saved in "info.json".
    However, this requires the options to be json-serializable.
    Therefore, the argument ``schema`` to :meth:`ParquetSerializer.serialize() <biglist._biglist.ParquetSerializer.serialize>`
    can not be used by this mechanism.
    As an alternative, user can use the argument ``schema_spec``;
    this argument can be saved in "info.json", and it is handled by this function.
    """
    return pyarrow.schema((make_parquet_field(v) for v in fields_spec))


class ParquetSerializer(Serializer):
    @classmethod
    def serialize(
        cls,
        x: list[dict],
        schema: pyarrow.Schema = None,
        schema_spec: Sequence = None,
        metadata=None,
        **kwargs,
    ):
        """
        `x` is a list of data items. Each item is a dict. In the output Parquet file,
        each item is a "row".

        The content of the item dict should follow a regular pattern.
        Not every structure is supported. The data `x` must be acceptable to
        `pyarrow.Table.from_pylist <https://arrow.apache.org/docs/python/generated/pyarrow.Table.html#pyarrow.Table.from_pylist>`_. If unsure, use a list with a couple data elements
        and experiment with ``pyarrow.Table.from_pylist`` directly.

        When using ``storage_format='parquet'`` for :class:`Biglist`, each data element is a dict
        with a consistent structure that is acceptable to ``pyarrow.Table.from_pylist``.
        When reading the Biglist, the original Python data elements are returned.
        (A record read out may not be exactly equal to the original that was written, in that
        elements that were missing in a record when written may have been filled in with ``None``
        when read back out.)
        In other words, the reading is *not* like that of :class:`~biglist.ParquetBiglist`.
        You can always create a separate ParquetBiglist for the data files of the Biglist
        in order to use Parquet-style data reading. The data files are valid Parquet files.

        If neither ``schema`` nor ``schema_spec`` is specified, then the data schema is auto-inferred
        based on the first element of ``x``. If this does not work, you can specify either ``schema`` or ``schema_spec``.
        The advantage of ``schema_spec`` is that it is json-serializable Python types, hence can be passed into
        :meth:`Biglist.new() <biglist.Biglist.new>` via ``serialize_kwargs`` and saved in "info.json" of the biglist.

        If ``schema_spec`` is not flexible or powerful enough for your use case, then you may have to use ``schema``.
        """
        if schema is not None:
            assert schema_spec is None
        elif schema_spec is not None:
            assert schema is None
            schema = make_parquet_schema(schema_spec)
        table = pyarrow.Table.from_pylist(x, schema=schema, metadata=metadata)
        sink = io.BytesIO()
        writer = pyarrow.parquet.ParquetWriter(sink, table.schema, **kwargs)
        writer.write_table(table)
        writer.close()
        sink.seek(0)
        # return sink.getvalue()  # bytes
        # return sink.getbuffer()  # memoryview
        return sink
        # this is a file-like object; `cloudly.upathlib.LocalUpath.write_bytes` and `cloudly.upathlib.GcsBlobUpath.write_bytes` accept this.
        # We do not return the bytes because `cloudly.upathlib.GcsBlobUpath.write_bytes` can take file-like objects directly.

    @classmethod
    def deserialize(cls, y: bytes, **kwargs):
        try:
            memoryview(y)
        except TypeError:
            pass  # `y` is a file-like object
        else:
            # `y` is a bytes-like object
            y = io.BytesIO(y)
        table = pyarrow.parquet.ParquetFile(y, **kwargs).read()
        return table.to_pylist()


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
