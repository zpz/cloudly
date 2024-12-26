"""
Configure logging.

A call to the function ``config_logger`` in a launching script
is all that is needed to set up the logging format.
Usually the 'level' argument is the only argument one needs to customize::

    config_logger(level='info')

Do not call this in library modules. Library modules should have::

    logger = logging.getLogger(__name__)

and then just use ``logger`` to write logs without concern about formatting,
destination of the log message, etc.
"""

__all__ = ['config_logger', 'set_level', 'add_console_handler']


import inspect
import logging
import logging.handlers
import os
import sys
import time
import warnings
from datetime import datetime
from logging import Formatter
from typing import Union

# When exceptions are raised during logging, then,
# the default implementation of handleError() in Handler
# checks to see if a module-level variable,
#   raiseExceptions,
# is set.
# If set, a traceback is printed to sys.stderr.
# If not set, the exception is swallowed.
#
# If no logging configuration is provided, then for Python 2.x,
#   If logging.raiseExceptions is False (production mode),
#       the event is silently dropped.
#   If logging.raiseExceptions is True (development mode),
#       a message 'No handlers could be found for logger X.Y.Z'
#       is printed once.


# Turn off annoyance in ptpython when setting DEBUG logging
logging.getLogger('parso').setLevel(logging.ERROR)

logging.captureWarnings(True)
warnings.filterwarnings('default', category=ResourceWarning)
warnings.filterwarnings('default', category=DeprecationWarning)
warnings.filterwarnings('ignore', category=DeprecationWarning, module='ptpython')
warnings.filterwarnings('ignore', category=DeprecationWarning, module='jedi')


rootlogger = logging.getLogger()
logger = logging.getLogger(__name__)


class DynamicFormatter(Formatter):
    def __init__(
        self,
        *args,
        with_datetime: bool = True,
        with_timezone: bool = True,
        with_level: bool = True,
        **kwargs,
    ):
        # TODO: could add parameters to customize formatting, as new ideas emerges.
        super().__init__(*args, **kwargs)
        self._with_datetime = with_datetime
        self._with_timezone = with_timezone
        self._with_level = with_level
        self._tz = datetime.now().astimezone().tzname()

    def format(self, record):
        r = record
        if self._with_datetime:
            asctime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(r.created))
            msecs = int((r.created % 1) * 10000)
            fmt = f'{asctime}.{msecs} '
            if self._with_timezone:
                fmt += f'{self._tz} '
        else:
            fmt = ''
        if self._with_level:
            fmt += f'{r.levelname: <12} '
        fmt += f'%(message)s     [( {r.name}, {r.lineno}, {r.funcName}'

        p = r.processName
        t = r.threadName
        if t.endswith(f' ({r.funcName})'):
            t = t[: -(len(r.funcName) + 3)]
        tk = getattr(r, 'taskName', None)
        if p == 'MainProcess':
            if t == 'MainThread':
                if tk is None:
                    fmt += ' )]'
                else:
                    fmt += f' | {tk} )]'
            else:
                if tk is None:
                    fmt += f' | {t} )]'
                else:
                    fmt += f' | {t}, {tk} )]'
        else:
            if t == 'MainThread':
                if tk is None:
                    fmt += f' | {p} <{r.process}> )]'
                else:
                    fmt += f' | {p} <{r.process}>, {tk} )]'
            else:
                if tk is None:
                    fmt += f' | {p} <{r.process}>, {t} )]'
                else:
                    fmt += f' | {p} <{r.process}>, {t}, {tk} )]'

        formatter = Formatter(fmt)
        return formatter.format(record)
        # We cannot completely construct the message and return it w/o making use
        # of a Formatter's `format` method. Somehow that wouldn't have the correct behavior
        # for `logger.exception`---it would not print the traceback.


def set_level(level: Union[str, int] = logging.INFO) -> int:
    """
    In one application, call `set_level` on the top level.
    Usually you should do this only once, and do not set level on any handler.

    Return the original level that was in effect before this function was called.
    By default, the original level is `logging.INFO` or `logging.WARNING`.

    Parameters
    ----------
    level
        Either the integers `logging.DEBUG`, `logging.INFO`, etc., or strings
        "debug", "info", etc.
    """
    if isinstance(level, str):
        level = getattr(logging, level.upper())
    level0 = rootlogger.level
    rootlogger.setLevel(level)
    return level0


def add_console_handler(**kwargs) -> logging.Handler:
    """
    Log to ``sys.stderr``.
    """
    h = logging.StreamHandler()
    h.setFormatter(DynamicFormatter(**kwargs))
    rootlogger.addHandler(h)
    return h


def config_logger(level=logging.INFO, **kwargs):
    # For use in one-off scripts.
    set_level(level)
    add_console_handler(**kwargs)


def add_disk_handler(
    *,
    foldername: str = None,
    maxBytes=1_000_000,
    backupCount=20,
    delay=True,
    **kwargs,
):
    if foldername:
        foldername = foldername.rstrip('/')
    else:
        launcher = get_calling_file()
        foldername == f"{os.environ.get('LOGDIR', '/tmp/log')}/{launcher.lstrip('/').replace('/', '-')}"
    print(f"Log files are located in '{foldername}'")
    os.makedirs(foldername, exist_ok=True)
    h = logging.handlers.RotatingFileHandler(
        filename=foldername + '/current',
        maxBytes=maxBytes,
        backupCount=backupCount,
        delay=delay,
    )
    h.setFormatter(DynamicFormatter(**kwargs))
    rootlogger.addHandler(h)
    return foldername


def log_uncaught_exception(handlers=None, logger=logger):
    # locally bind `handlers` and `logger` in case the global references are gone
    # when the exception handler is invoked.
    if handlers is None:
        handlers = rootlogger.handlers

    def handle_exception(exc_type, exc_val, exc_tb):
        if not issubclass(exc_tb, KeyboardInterrupt):
            if sys.version_info.minor < 11:
                logging.currentframe = lambda: sys._getframe(1)
            fn, lno, func, sinfo = logger.findCaller(stack_info=False, stacklevel=1)
            record = logger.makeRecord(
                logger.name,
                logging.CRITICAL,
                fn,
                lno,
                msg=exc_val,
                args=(),
                exc_info=(exc_type, exc_val, exc_tb),
                func=func,
                sinfo=sinfo,
            )

            for h in handlers:
                h.handle(record)

        sys.__excepthook__(exc_type, exc_val, exc_tb)

    sys.excepthook = handle_exception


def get_calling_file() -> inspect.FrameInfo:
    """
    This function finds the "launch script" of the currently running program.
    The "launch script" is either a `.py` file or the Python interpreter.

    The returned object has attribute `filename`, which is the full path of the launch script.
    The value `'<stdin>'` suggests the Python interpreter, i.e. it's in an interactive Python session.

        $ ptpython
        >>> from cloudly.util.logging import get_calling_file
        >>> def func_a():
        ...     return get_calling_file()
        >>> def func_b():
        ...     z = func_a()
        ...     print(z)
        ...     print()
        ...     print(z.filename)
        >>> func_b()
        FrameInfo(frame=<frame at 0x7f54d8009e30, file '<stdin>', line 2, code func_a>, filename='<stdin>', lineno=2, function='func_a', code_context=None, index=None)

        <stdin>
        >>>
    """
    st = inspect.stack()
    caller = None
    for s in st:
        if os.path.basename(s.filename) == 'runpy.py':
            # `runpy.py` is usually what finds and launches the module specified by the
            # `-m` command line switch. So, the last step was the user script. Stop here.
            break
        if '_pytest/python.py' in s.filename:
            # Don't follow into py.test; stop at the test file
            break
        if s.filename == '<stdin>':
            # This is the Python interpreter.
            caller = s
            break
        caller = s
    return caller
