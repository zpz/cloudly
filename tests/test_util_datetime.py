from cloudly.util.datetime import (
    centralnow,
    easternnow,
    isoformatnow,
    mountainnow,
    pacificnow,
    tznow,
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
    assert_same_time(utcnow(), tznow("utc"))
    assert_same_time(easternnow(), tznow("America/New_York"))
    assert_same_time(centralnow(), tznow("America/Chicago"))
    assert_same_time(mountainnow(), tznow("America/Denver"))
    assert_same_time(pacificnow(), tznow("America/Los_Angeles"))


def test_isoformatnow():
    t1 = isoformatnow()
    t2 = utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f")
    print()
    print(t1)
    print(t2)
    assert t1.startswith(t2[:-4])
