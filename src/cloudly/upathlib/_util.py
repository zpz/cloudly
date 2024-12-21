import os
import threading
import warnings
import weakref
from concurrent.futures import ThreadPoolExecutor

MAX_THREADS = min(32, (os.cpu_count() or 1) + 4)
"""
This default is suitable for I/O bound operations.
This value is what is used by `concurrent.futures.ThreadPoolExecutor <https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.ThreadPoolExecutor>`_.
For others, you may want to specify a smaller value.
"""


# Copied from ``mpservice.concurrent.futures``.

_global_thread_pools_: dict[str, ThreadPoolExecutor] = weakref.WeakValueDictionary()
_global_thread_pools_lock: threading.Lock = threading.Lock()


def get_shared_thread_pool(
    name: str = 'default', max_workers: int | None = None
) -> ThreadPoolExecutor:
    with _global_thread_pools_lock:
        executor = _global_thread_pools_.get(name)
        # If the named pool exists, it is returned; the input `max_workers` is ignored.
        if executor is None or executor._shutdown:
            # `executor._shutdown` is True if user inadvertently called `shutdown` on the executor.
            if name == 'default':
                if max_workers is not None:
                    warnings.warn(
                        f"size of the 'default' thread pool is determined internally; the input {max_workers} is ignored"
                    )
                    max_workers = None
            else:
                if max_workers is not None:
                    assert 1 <= max_workers <= 64, max_workers
            executor = ThreadPoolExecutor(max_workers)
            _global_thread_pools_[name] = executor
    return executor


if hasattr(os, 'register_at_fork'):  # not available on Windows

    def _clear_global_state():
        for box in (_global_thread_pools_,):
            for name in list(box.keys()):
                pool = box.get(name)
                if pool is not None:
                    # TODO: if `pool` has locks, are there problems?
                    pool.shutdown(wait=False)
                box.pop(name, None)

        global _global_thread_pools_lock
        try:
            _global_thread_pools_lock.release()
        except RuntimeError:  # 'release unlocked lock'
            pass
        _global_thread_pools_lock = threading.Lock()

    os.register_at_fork(after_in_child=_clear_global_state)
