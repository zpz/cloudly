from collections.abc import Callable
from time import perf_counter


def friendly_duration(seconds: float):
    msg = []
    if seconds >= 3600:
        hours, seconds = divmod(seconds, 3600)
        hours = int(hours)
        if hours > 1:
            msg.append(f'{hours} hours')
        else:
            msg.append(f'{hours} hour')
        seconds = int(seconds)
    if seconds >= 60:
        minutes, seconds = divmod(seconds, 60)
        minutes = int(minutes)
        if minutes > 1:
            msg.append(f'{minutes} minutes')
        else:
            msg.append(f'{minutes} minute')
        seconds = int(seconds)
    if isinstance(seconds, int):
        if seconds > 1:
            msg.append(f'{seconds} seconds')
        else:
            msg.append(f'{seconds} second')
    else:
        msg.append(f'{round(seconds, 4)} seconds')
    return ' '.join(msg)


class timer:
    """
    The class ``timer`` (intentionally un-capitalized) can be used as
    a function decorator or a block context manager. As a function decorator::

        @timer()
        def myfunc(...)
            ...

    As a block timer:

        with timer(name='block 1'):
            ...
            ...

    In the first usage, you usually leave `name` to the default behavior, which
    plugs in the name of the function that's being decorated. In the second usage,
    you may choose to specify a `name` for the enclosed block; otherwise, there is no
    printout of "... started" and "... finished" enclosing the execution of the block.

    The optional `print_func` is the builtin function `print` by default.
    It may be useful to pass in a logging function, such as `print_func=logger.info`.
    However, the line-number info of the log message will be with regard to the current module
    rather than the context where `timer` is being used.
    """

    def __init__(self, name: str = None, *, print_func: Callable = None):
        self._name = name
        self._print_func = print_func or print

    def __call__(self, f: Callable):
        def decorated(*args, **kwargs):
            if not self._name:
                self._name = f"function '{f.__name__}'"
            with self:
                z = f(*args, **kwargs)
            return z

        return decorated

    def __enter__(self):
        self._t0 = perf_counter()
        if self._name:
            self._print_func(f'"{self._name}" started ...')

    def __exit__(self, *args, **kwargs):
        dur = friendly_duration(perf_counter() - self._t0)
        if self._name:
            self._print_func(f'... "{self._name}" finished')
            self._print_func(f'"{self._name}" took {dur} to finish')
        else:
            self._print_func(f'... took {dur}')
