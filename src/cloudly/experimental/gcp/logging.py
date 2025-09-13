import logging
import queue
import time

import google.cloud.logging_v2

logging.getLogger('urllib3.connectionpool').setLevel(logging.WARNING)
rootlogger = logging.getLogger()


# Hack Google code to replace `time.time` by `time.perf_counter`.
# `perf_counter` has much higher resolution w/o more overhead.
# See https://www.webucator.com/article/python-clocks-explained/
def _get_many(queue_: queue.Queue, *, max_items: int = None, max_latency: float = 0):
    """
    Get multiple items from a queue.
    Get at least one (blocking) and at most ``max_items`` items (non-blocking) from a given queue.
    Do not mark the items as done.

    Parameters
    ----------
    queue_
        The Queue to get items from.
    max_items
        The max number of items to get.
        If ``None``, then all available items in the queue are returned.
    max_latency
        The max number of seconds to wait for more than one item from a queue.
        This number includes the time required to retrieve the first item.

    Returns
    -------
        list
            Items retrieved from the queue.
    """
    # DEBUG: to verify this hacked function is being used.
    # VERIFIED.
    # print('_get_many', max_items, max_latency)

    start = time.perf_counter()
    # Always return at least one item.
    items = [queue_.get()]
    while max_items is None or len(items) < max_items:
        try:
            elapsed = time.perf_counter() - start
            timeout = max(0, max_latency - elapsed)
            items.append(queue_.get(timeout=timeout))
        except queue.Empty:
            break
    return items


google.cloud.logging_v2.handlers.transports.background_thread._get_many = _get_many
