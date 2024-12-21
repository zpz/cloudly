from datetime import datetime

from boltons import timeutils

from cloudly.util.datetime import (
    centralnow,
    easternnow,
    isonow,
    mountainnow,
    pacificnow,
    utcnow,
)


def assert_same_time(t1, t2):
    assert t1.year == t2.year
    assert t1.month == t2.month
    assert t1.day == t2.day
    assert t1.hour == t2.hour
    assert t1.minute == t2.minute
    assert abs(t1.second - t2.second) < 2


def test_datetime():
    print()
    print(utcnow())
    print(pacificnow())
    assert_same_time(utcnow(), datetime.now(timeutils.UTC))
    assert_same_time(easternnow(), datetime.now(timeutils.Eastern))
    assert_same_time(centralnow(), datetime.now(timeutils.Central))
    assert_same_time(mountainnow(), datetime.now(timeutils.Mountain))
    assert_same_time(pacificnow(), datetime.now(timeutils.Pacific))


def test_isonow():
    t1 = isonow()
    t2 = utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f')
    print()
    print(t1)
    print(t2)
    assert t1.startswith(t2[:-4])
