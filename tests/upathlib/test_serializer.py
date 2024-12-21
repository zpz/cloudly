from concurrent.futures import ThreadPoolExecutor

from cloudly.upathlib.serializer import (
    JsonSerializer,
    Lz4OrjsonSerializer,
    Lz4PickleSerializer,
    OrjsonSerializer,
    PickleSerializer,
    ZPickleSerializer,
    ZstdCompressor,
    ZstdOrjsonSerializer,
    ZstdPickleSerializer,
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
        ZstdOrjsonSerializer,
        Lz4OrjsonSerializer,
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
    y = ZstdOrjsonSerializer.serialize(data)
    z = ZstdOrjsonSerializer.deserialize(y)
    assert z == data
    return True


def test_zstd():
    assert _check(data)

    with ThreadPoolExecutor() as pool:
        t = pool.submit(_check, data=data)
        assert t.result()
