import os
from concurrent.futures import ThreadPoolExecutor

from cloudly.upathlib import LocalUpath
from cloudly.util.serializer import (
    AvroSerializer,
    CsvSerializer,
    JsonSerializer,
    Lz4PickleSerializer,
    NewlineDelimitedOrjsonSeriealizer,
    OrjsonSerializer,
    PickleSerializer,
    ZPickleSerializer,
    ZstdCompressor,
    ZstdPickleSerializer,
    make_avro_schema,
)

data = [12, 23.8, {'a': [9, 'xyz'], 'b': {'first': 3, 'second': 2.3}}, None]


def test_all():
    for serde in (
        JsonSerializer,
        PickleSerializer,
        ZPickleSerializer,
        ZstdPickleSerializer,
        Lz4PickleSerializer,
        OrjsonSerializer,
    ):
        print(serde)
        y = serde.serialize(data)
        z = serde.deserialize(y)
        assert z == data


def test_zstdcompressor():
    me = ZstdCompressor()
    assert len(me._compressor) == 0
    assert me._decompressor is None
    me._compressor[(1, 2)] = 3
    me._decompressor = 8

    def _check():
        assert len(me._compressor) == 0
        assert me._decompressor is None
        me._compressor[(3, 4)] = 5
        me._decompressor = 'a'
        return True

    with ThreadPoolExecutor() as pool:
        t = pool.submit(_check)
        assert t.result()

    assert me._compressor == {(1, 2): 3}
    assert me._decompressor == 8


def _check(data):
    y = ZstdPickleSerializer.serialize(data)
    z = ZstdPickleSerializer.deserialize(y)
    assert z == data
    return True


def test_zstd():
    assert _check(data)

    with ThreadPoolExecutor() as pool:
        t = pool.submit(_check, data=data)
        assert t.result()


def test_csv():
    names = ['name', 'age', 'city']
    data = [
        ('Tom', 28, 'New York'),
        ('Jane', 23, 'LA'),
        ('Peter', 66, 'SF'),
    ]

    # data rows are tuples

    print()
    x = CsvSerializer.serialize(data, fieldnames=names)
    print('CSV serialized:')
    print(x)
    y = CsvSerializer.deserialize(x)
    print('CSV deserialized:')
    print(y)

    file = LocalUpath(
        os.environ.get('TMPDIR', '/tmp'), 'cloudly/test/test-serializer-csv-tuple.csv'
    )
    file.rmrf()
    CsvSerializer.dump(data, file, fieldnames=names)
    # TODO: here, manually check the file and verify it looks right.
    y = CsvSerializer.load(file)
    print('CSV loaded:')
    print(y)

    # data rows are dicts
    print()
    x = CsvSerializer.serialize([dict(zip(names, row)) for row in data])
    print('CSV serialized:')
    print(x)
    y = CsvSerializer.deserialize(x)
    print('CSV deserialized:')
    print(y)

    file = LocalUpath(
        os.environ.get('TMPDIR', '/tmp'), 'cloudly/test/test-serializer-csv-dict.csv'
    )
    file.rmrf()
    CsvSerializer.dump([dict(zip(names, row)) for row in data], file)
    # TODO: here, manually check the file and verify it looks right.
    y = CsvSerializer.load(file)
    print('CSV loaded:')
    print(y)


def test_newline_delimited_json():
    names = ['name', 'age', 'city']
    data = [
        ('Tom', 28, 'New York'),
        ('Jane', 23, 'LA'),
        ('Peter', 66, 'SF'),
    ]

    # data rows are tuples

    print()
    x = NewlineDelimitedOrjsonSeriealizer.serialize(data)
    print('NDJSON serialized:')
    print(x)
    y = NewlineDelimitedOrjsonSeriealizer.deserialize(x)
    print('NDJSON deserialized:')
    print(y)

    file = LocalUpath(
        os.environ.get('TMPDIR', '/tmp'),
        'cloudly/test/test-serializer-ndjson-tuple.json',
    )
    file.rmrf()
    NewlineDelimitedOrjsonSeriealizer.dump(data, file)
    # TODO: here, manually check the file and verify it looks right.
    y = NewlineDelimitedOrjsonSeriealizer.load(file)
    print('NDJSON loaded:')
    print(y)

    # data rows are dicts
    print()
    x = NewlineDelimitedOrjsonSeriealizer.serialize(
        [dict(zip(names, row)) for row in data]
    )
    print('NDJSON serialized:')
    print(x)
    y = NewlineDelimitedOrjsonSeriealizer.deserialize(x)
    print('NDJSON deserialized:')
    print(y)

    file = LocalUpath(
        os.environ.get('TMPDIR', '/tmp'),
        'cloudly/test/test-serializer-ndjson-dict.json',
    )
    file.rmrf()
    NewlineDelimitedOrjsonSeriealizer.dump(
        [dict(zip(names, row)) for row in data], file
    )
    # TODO: here, manually check the file and verify it looks right.
    y = NewlineDelimitedOrjsonSeriealizer.load(file)
    print('NDJSON loaded:')
    print(y)


def test_avro():
    schema = {
        'doc': 'A weather reading.',
        'name': 'Weather',
        'namespace': 'test',
        'type': 'record',
        'fields': [
            {'name': 'station', 'type': 'string'},
            {'name': 'time', 'type': 'long'},
            {'name': 'temp', 'type': 'int'},
        ],
    }
    records = [
        {'station': '011990-99999', 'temp': 0, 'time': 1433269388},
        {'station': '011980-99299', 'temp': 22, 'time': 1433270389},
        {'station': '011920-99899', 'temp': -11, 'time': 1433273379},
        {'station': '012650-99499', 'temp': 111, 'time': 1433275478},
    ]
    print()
    y = AvroSerializer.serialize(records, schema=schema)
    print('AVRO serialized:')
    print(y.getvalue())
    z = AvroSerializer.deserialize(y)
    assert z == records

    sch = make_avro_schema(records[0], schema['name'], schema['namespace'])
    print('autodetected schema:')
    print(sch)

    file = LocalUpath(
        os.environ.get('TMPDIR', '/tmp'), 'cloudly/test/test-serializer-avro.avro'
    )
    file.rmrf()
    AvroSerializer.dump(records, file, schema=sch)
    y = AvroSerializer.load(file)
    print('avro loaded:')
    print(y)
    assert y == records
