import logging
from multiprocessing import Process
from threading import Thread

from cloudly.util.logging import config_logger, rootlogger

logger = logging.getLogger(__name__)


def process_thread_worker():
    logger.info('info in process/thread')


def process_worker():
    logger.info('info in process')

    workers = [Thread(target=process_thread_worker) for _ in (1, 2)]
    for w in workers:
        w.start()
    for w in workers:
        w.join()


def thread_worker():
    logger.info('info in thread')


# This test does not assert any result.
# It will pass as long as the code does not crash.
# To see the printout, run this script with Python.
def test_logging():
    config_logger()
    logger.debug('debug info')
    logger.info('some info')
    logger.warning('warning! #%d', 38)
    logger.error('something is wrong!')
    logger.critical(
        'something terrible has happened! omg omg omg OMG OMG OMG next line OMG next line OMG yes go to next line\nOMG OMG'
    )

    p = Process(target=process_worker)
    p.start()
    p.join()

    t = Thread(target=thread_worker, name='MyThread')
    t.start()
    t.join()

    try:
        raise ValueError('an intentional ValueError exception')
    except Exception as e:
        logger.exception(e)

    print()

    rootlogger.handlers = []
    config_logger(with_datetime=False)
    logger.info('some info w/o datetime')

    rootlogger.handlers = []
    config_logger(with_timezone=False)
    logger.info('some info w/o timezone')

    rootlogger.handlers = []
    config_logger(with_level=False)
    logger.info('some info w/o level')

    rootlogger.handlers = []
    config_logger(with_datetime=False, with_timezone=False)
    logger.info('some info w/o datetime and timezone')

    rootlogger.handlers = []
    config_logger(with_datetime=False, with_level=False)
    logger.info('some info w/o datetime and level')

    rootlogger.handlers = []
    config_logger(with_timezone=False, with_level=False)
    logger.info('some info w/o timezone and level')

    rootlogger.handlers = []
    config_logger(with_datetime=False, with_timezone=False, with_level=False)
    logger.info('some info w/o datetime, timezone and level')


if __name__ == '__main__':
    test_logging()
