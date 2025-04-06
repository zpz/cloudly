import csv
import gc
import io
import json
import pickle
import threading
import zlib
from collections.abc import Iterable, Sequence
from contextlib import contextmanager
from typing import Any, Protocol, TypeVar

import fastavro
import orjson
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

    # @classmethod
    # def dump(cls, x: T, file, *, overwrite: bool = False, **kwargs) -> None:
    #     # `file` is a `Upath` object.
    #     y = cls.serialize(x, **kwargs)
    #     file.write_bytes(y, overwrite=overwrite)

    # @classmethod
    # def load(cls, file, **kwargs) -> T:
    #     # `file` is a `Upath` object.
    #     y = file.read_bytes()
    #     return cls.deserialize(y, **kwargs)


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


class OrjsonSerializer(Serializer):
    @classmethod
    def serialize(cls, x, **kwargs) -> bytes:
        return orjson.dumps(x, **kwargs)

    @classmethod
    def deserialize(cls, y: bytes, **kwargs):
        return orjson.loads(y, **kwargs)


# class ZOrjsonSerializer(OrjsonSerializer):
#     # In general, this is not the best choice of compression.
#     # Use `zstandard` or `lz4 instead.
#     @classmethod
#     def serialize(cls, x, *, level=ZLIB_LEVEL, **kwargs) -> bytes:
#         y = super().serialize(x, **kwargs)
#         return zlib.compress(y, level=level)

#     @classmethod
#     def deserialize(cls, y, **kwargs):
#         y = zlib.decompress(y)
#         return super().deserialize(y, **kwargs)


# class ZstdOrjsonSerializer(OrjsonSerializer):
#     _compressor = ZstdCompressor()

#     @classmethod
#     def serialize(cls, x, *, level=ZSTD_LEVEL, threads=0, **kwargs) -> bytes:
#         y = super().serialize(x, **kwargs)
#         return cls._compressor.compress(y, level=level, threads=threads)

#     @classmethod
#     def deserialize(cls, y, **kwargs):
#         y = cls._compressor.decompress(y)
#         return super().deserialize(y, **kwargs)


class NewlineDelimitedOrjsonSeriealizer(Serializer):
    # This format does not require the rows to have compatible format,
    # as each row is independently JSON serialized and deserialized,
    # although in practice the file is typically a data files with rows of
    # a certain common format.
    @classmethod
    def serialize(cls, x: Iterable[T], **kwargs):
        return b'\r\n'.join(orjson.dumps(row, **kwargs) for row in x)

    @classmethod
    def deserialize(cls, y, **kwargs) -> list[T]:
        # Note that JSON serialize/deserialize will change tuples to lists,
        # because JSON does not have a tuple type.
        return [orjson.loads(row, **kwargs) for row in y.split(b'\r\n')]


class CsvSerializer(Serializer):
    # CSV requires the rows to have some consistent format.
    # There are options to handle missing fields, extra fields, etc.
    @classmethod
    def serialize(
        cls,
        x: Iterable[Sequence] | Iterable[dict[str, Any]],
        **kwargs,
    ) -> bytes:
        # If `x` is an iterable of tuples or lists, then the first element
        # is the field names. This is to be consistent with :meth:`deserialize`.
        try:
            row = next(x)
        except TypeError:
            x = iter(x)
            row = next(x)

        sink = io.StringIO()
        if isinstance(row, dict):
            # Writing dicts in this branch could be much slower than
            # writing tuples in the next branch.
            writer = csv.DictWriter(sink, fieldnames=list(row.keys()), **kwargs)
            writer.writeheader()
            writer.writerow(row)
            writer.writerows(x)
        else:
            fieldnames = row
            writer = csv.writer(sink, **kwargs)
            writer.writerow(fieldnames)
            writer.writerows(x)

        sink.seek(0)
        return sink.getvalue().encode('utf-8')

    @classmethod
    def deserialize(
        cls, y: bytes, *, as_dict: bool = False, **kwargs
    ) -> list[tuple] | list[dict[str, Any]]:
        y = io.StringIO(y.decode('utf-8'))
        reader = csv.reader(y, **kwargs)
        fieldnames = tuple(next(reader))
        col0 = 0 if fieldnames[0] else 1  # skip the first column if it's empty
        if as_dict:
            return [
                {k: v for k, v in zip(fieldnames[col0:], row[col0:])} for row in reader
            ]  # list[dict]
        return [
            fieldnames[col0:],
            *(tuple(row[col0:]) for row in reader),
        ]  # list[tuple]
        # The first row is `fieldnames`.


def _make_avro_schema(x, name: str) -> dict:
    if isinstance(x, int):
        return {'name': name, 'type': 'int'}
    if isinstance(x, float):
        return {'name': name, 'type': 'double'}
    if isinstance(x, str):
        return {'name': name, 'type': 'string'}
    if isinstance(x, dict):
        fields = []
        for key, val in x.items():
            z = _make_avro_schema(val, key)
            if len(z) < 3:
                fields.append(z)
            else:
                fields.append({'name': key, 'type': z})
        return {'name': name, 'type': 'record', 'fields': fields}
    if isinstance(x, list):
        assert len(x) > 0, (
            'empty list is not supported, because its type can not be inferred'
        )
        z0 = _make_avro_schema(x[0], name + '_item')
        if len(x) > 1:
            for v in x[1:]:
                z1 = _make_avro_schema(v, name + '_item')
                assert z1 == z0, (
                    f'schema for x[0] ({x[0]}): {z0}; schema for x[?] ({v}): {z1}'
                )
        if len(z0) < 3:
            items = z0['type']
        else:
            items = z0
        return {'name': name, 'type': 'array', 'items': items}

    raise ValueError('unrecognized value of type "' + type(x).__name__ + '"')


def make_avro_schema(value: dict, name: str, namespace: str) -> dict:
    """
    `value` is a `dict` whose members are either 'simple types' or 'compound types'.

    'simple types' include:
        int, float, str

    'compound types' include:
        dict: whose elements are simple or compound types
        list: whose elements are all the same simple or compound type
    """
    sch = {'namespace': namespace, **_make_avro_schema(value, name)}
    return sch


class AvroSerializer(Serializer):
    @classmethod
    def serialize(cls, x: Iterable[dict], *, schema: dict) -> io.BytesIO:
        # If this function is used repeatedly, you want to call `fastavro.parsed_schema`
        # and pass in its output as `schema`.
        # You may infer the schema use :func:`make_avro_schema` given an example data element.
        sink = io.BytesIO()
        fastavro.writer(sink, schema, x)
        sink.seek(0)
        return sink

    @classmethod
    def deserialize(cls, y) -> list[dict]:
        try:
            memoryview(y)
        except TypeError:
            pass  # `y` is a file-like object
        else:
            # `y` is a bytes-like object
            y = io.BytesIO(y)
        return list(fastavro.reader(y))


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

    # class Lz4OrjsonSerializer(OrjsonSerializer):
    #     @classmethod
    #     def serialize(cls, x, *, level=LZ4_LEVEL, **kwargs) -> bytes:
    #         y = super().serialize(x, **kwargs)
    #         return lz4.frame.compress(y, compression_level=level)

    #     @classmethod
    #     def deserialize(cls, y, **kwargs):
    #         y = lz4.frame.decompress(y)
    #         return super().deserialize(y, **kwargs)


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
        In other words, the reading is *not* like that of :class:`~biglist.ExternalBiglist`.
        You can always create a separate ExternalBiglist for the data files of the Biglist
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
