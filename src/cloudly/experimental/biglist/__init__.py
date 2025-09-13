import threading
from collections.abc import Iterator
from queue import Queue

from mpservice.queue import IterableQueue, StopRequested

from cloudly.biglist import BiglistFileReader
from cloudly.util.multiplexer import Multiplexer


def iter_biglist_files(
    mux_id: str,
    *,
    num_workers: int = 2,
    mux_cls=Multiplexer,
    to_stop=None,
) -> Iterator[BiglistFileReader]:
    """
    This helper function uses ``Multiplexer`` to read data in a Biglist.

    ``muxid`` is the ID of a ``Multiplexer[FileReader]``.

    Example::

        # 'controller' code
        data = Biglist(...)
        mux = Multiplexer.new(data.files)
        mux_id = mux.create_read_session()

        # 'worker' code
        for batch in iter_biglist_files(mux_id):
            for item in batch:
                ...  # use data element ``item``

    A Multiplexer distributes a stream of "file" objects of a biglist for reading the data.
    When the biglist resides in the cloud, then because fetching blobs is a network call,
    it is possible that the data consumer is faster than the blob fetcher.
    This function uses multiple worker threads to fetch blobs concurrently. It also handles
    early breakout and some exceptions.
    """
    if to_stop is None:
        to_stop = threading.Event()
    q = IterableQueue(Queue(maxsize=4), to_stop=to_stop, num_suppliers=num_workers)

    def worker(mux_id, q, to_stop):
        try:
            for file in mux_cls(mux_id):
                file.load()
                q.put(file)
        except StopRequested:
            pass
        except:
            to_stop.set()
            raise
        finally:
            q.put_end()

    workers = [
        threading.Thread(
            target=worker,
            args=(mux_id, q, to_stop),
        )
        for _ in range(num_workers)
    ]
    for w in workers:
        w.start()
    try:
        yield from q
    except StopRequested:
        pass
    except:
        to_stop.set()
        raise
    finally:
        for w in workers:
            w.join()
