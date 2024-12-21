from time import perf_counter

import numpy
from faker import Faker

from cloudly.upathlib.serializer import (
    CompressedOrjsonSerializer,
    CompressedPickleSerializer,
    JsonByteSerializer,
    JsonSerializer,
    OrjsonSerializer,
    PickleSerializer,
)


def make_native(seed=1234):
    fake = Faker()
    Faker.seed(seed)
    return {
        'data': {
            'float': [x + 0.3 for x in range(10000)],
            'int': list(range(10000)),
        },
        'attributes': {fake.name(): fake.sentence() for _ in range(5000)},
        'details': [fake.text() for _ in range(2000)],
    }


def make_numpy(seed=5678):
    numpy.random.seed(seed)
    return {
        'int': numpy.random.randint(1, 100000, 100000),
        'float': numpy.random.rand(1000, 1000),
    }


native_data = make_native()
numpy_data = make_numpy()


def bench(ser, data, nrepeat):
    t0 = perf_counter()
    for _ in range(nrepeat):
        z = ser.serialize(data)
    t1 = perf_counter()
    for _ in range(nrepeat):
        ser.deserialize(z)
    t2 = perf_counter()
    print(
        f'{ser.__name__:26} {t1 - t0:6.2f} seconds, {len(z):>8} bytes, {t2 - t1:6.2f} seconds'
    )


def main():
    print('==== native data ====')
    bench(JsonSerializer, native_data, 1000)
    bench(JsonByteSerializer, native_data, 1000)
    bench(PickleSerializer, native_data, 1000)
    bench(OrjsonSerializer, native_data, 1000)
    bench(CompressedPickleSerializer, native_data, 1000)
    bench(CompressedOrjsonSerializer, native_data, 1000)
    print('')
    print('==== numpy data ====')
    bench(PickleSerializer, numpy_data, 100)
    bench(OrjsonSerializer, numpy_data, 100)
    bench(CompressedPickleSerializer, numpy_data, 100)
    bench(CompressedOrjsonSerializer, numpy_data, 100)


main()

"""
Benchmark results:
    2022/4/15
    CPU 3.10GHz x 2, memory 15.5GiB

==== native data ====
JsonSerializer              10.92 seconds,   719512 bytes,   6.41 seconds
JsonByteSerializer          10.84 seconds,   719512 bytes,   6.44 seconds
PickleSerializer             2.67 seconds,   688480 bytes,   2.54 seconds
OrjsonSerializer             1.80 seconds,   687844 bytes,   4.03 seconds
CompressedPickleSerializer  21.86 seconds,   278810 bytes,   5.47 seconds
CompressedOrjsonSerializer  21.02 seconds,   259538 bytes,   6.70 seconds

==== numpy data ====
PickleSerializer             0.55 seconds,  8800228 bytes,   0.10 seconds
OrjsonSerializer             5.80 seconds, 19860988 bytes,  12.97 seconds
CompressedPickleSerializer  40.32 seconds,  7880118 bytes,   5.40 seconds
CompressedOrjsonSerializer  92.53 seconds,  9282023 bytes,  21.42 seconds
"""
