__all__ = ['utcnow', 'easternnow', 'centralnow', 'pacificnow', 'tznow', 'isonow']


from datetime import datetime
from zoneinfo import ZoneInfo


def tznow(tzname) -> datetime:
    """
    Get the current ``datetime`` in the specified timezone.

    Args:
        tzname: the ISO standard timezone name,
        such as ``Africa/Cairo``, ``America/Los_Angeles``.
    """
    if tzname == 'utc':
        tzname = 'UTC'
    return datetime.now(ZoneInfo(tzname))


def utcnow() -> datetime:
    return tznow('UTC')


def easternnow() -> datetime:
    return tznow('America/New_York')


def centralnow() -> datetime:
    return tznow('America/Chicago')


def mountainnow() -> datetime:
    return tznow('America/Denver')


def pacificnow() -> datetime:
    return tznow('America/Los_Angeles')


def isonow() -> str:
    """
    This function creates a timestamp string with fixed format like

        '2020-08-22T08:09:13.401346+00:00'

    The returned string has fixed length. The microseconds section
    always takes 6 digits, even if they are all zeros.

    Strings created by this function can be compared to
    determine time order. There is no need to parse the string
    into `datetime` objects.

    The returned string is often written as a timestamp file, like

        open(file_name, 'w').write(make_timestamp())

    Another way to make a fixed-length datetime str is to use
    `datetime.strftime('%Y%m%d%H%M%S.%f')`.
    """
    return utcnow().isoformat(timespec='microseconds')
