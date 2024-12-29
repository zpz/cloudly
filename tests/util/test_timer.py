import time

from cloudly.util.timer import timer


def myfunc(n):
    time.sleep(n)
    return n


def test_timer():
    print()
    print('time a named block')
    with timer('my first block'):
        print('my first block is busy')
        time.sleep(1.5)

    print('time an unnamed block')
    with timer():
        print('my second block is busy')
        time.sleep(0.3)

    print('time a func')
    y = timer()(myfunc)(2)
    assert y == 2
