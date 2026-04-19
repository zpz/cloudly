import asyncio
import logging
import queue
import threading
from collections.abc import Iterable, Iterator
from multiprocessing.util import Finalize

import aiohttp
from mpservice.streamer import fifo_stream
from mpservice.threading import Thread

# `aiohttp` is  not listed as a dependency of `cloudly`.
# Just install it if you use the current module.

logger = logging.getLogger(__name__)


class NotFoundError(Exception):
    pass


# NOTE: beware of the cache when speed benchmarking.
# TODO: the cache is based on time; can we base on frequency of use?
class Multidownloader:
    def __init__(
        self,
        *,
        cache_size: int = 10_000,
        num_downloaders: int = 100,
        timeout: int | float = 5,
    ):
        """
        cache_size
            The cache acts as both an input rate limiter and a result cache.
            If ``url0`` has been submitted and still downloading, another submission
            of ``url0`` will wait for the ongoing download.
            If ``url0`` has been downloaded and returned to user but is still in the cache,
            then another submission of ``url0`` will grab the asset in the cache.

            When the cache is full, new submission will wait.
            The cache remains half-full once it reaches there, i.e. after an asset has been
            returned to the user, it will be removed from the cache only if the cache
            is more than half full.
            Cache depletion happens in :meth:`redeem`.
            If :meth:`redeem` is slower than :meth:`submit`, the cache content will grow
            towards full.

        num_downloaders
            The number of concurrent, async downloads that can happen.
            This affects throughput.
            Experiments showed that `64` is considerably slower than `100`, while above `100`
            the throughput does not change much.
        """
        assert num_downloaders > 0
        assert cache_size >= num_downloaders
        self._num_downloaders = num_downloaders
        self._cache_size = cache_size
        self._cache_watermark = cache_size // 2
        self._cache = {}
        self._cache_lock = threading.Lock()
        self._cache_not_full = threading.Condition(self._cache_lock)
        self._timeout = timeout

        to_shutdown = threading.Event()

        q = queue.Queue()
        self._thread = Thread(
            target=self._async_worker_thread,
            args=(q, to_shutdown),
            daemon=True,
        )
        self._thread.start()
        self._worker = q.get()

        Finalize(self, to_shutdown.set)

    def __getstate__(self):
        return (
            self._cache_size,
            self._num_downloaders,
            self._timeout,
        )

    def __setstate__(self, data):
        self.__init__(cache_size=data[0], num_downloaders=data[1], timeout=data[2])

    def _async_worker_thread(self, q, to_shutdown):
        async def main(loop, to_shutdown):
            sem = asyncio.Semaphore(self._num_downloaders)
            transport = aiohttp.TCPConnector(
                limit=self._num_downloaders,
                ttl_dns_cache=600,
                keepalive_timeout=600,
            )
            async with aiohttp.ClientSession(
                connector=transport,
                timeout=aiohttp.ClientTimeout(total=self._timeout),
            ) as session:
                q.put((loop, session, sem))
                while True:
                    if to_shutdown.is_set():
                        break
                    await asyncio.sleep(1)

        loop = asyncio.new_event_loop()
        loop.run_until_complete(main(loop, to_shutdown))

    async def a_download(self, url: str, *, session, sem, **kwargs):
        # A typical application would customize this method in a subclass.
        # See :class:`ImageDownloader` for an example.
        async with sem:
            async with session.get(url, **kwargs) as response:
                return await response.read()

    def submit(self, url: str | dict, **download_kwargs) -> str:
        """
        Submit an asset URL for downloading.
        The returned "key" is to be used later with :meth:`redeem` for retrieving the result.

        This will block and wait if the cache is full.
        """
        # The cache imposes some control on the amount of pending requests,
        # hence we do not use "sem" to control the concurrency here.
        # We may have a cache-ful of requests submitted,
        # and then the actual downloading is further controlled by "sem"
        # in the download function.
        key = str(url)
        with self._cache_not_full:
            entry = self._cache.pop(key, None)
            if entry is not None:
                # The url is in the cache.
                # The downloading for this url may or may not have finished.
                entry['n_submit'] += 1
                self._cache[key] = entry
                # Put at end of cache, noting that dict preserves insertion order.
                return key

            while len(self._cache) >= self._cache_size:
                # Wait for calls to ``self.redeem`` to make some space.
                self._cache_not_full.wait(0.5)
                # The wait will terminate once `redeem` has called
                # `notify`, hence the wait time 0.5 (sec) will not
                # cause unnecessary long wait.

            # Now after waking up, situation may have changed.
            # Maybe another thread has just submitted this url.
            entry = self._cache.get(key)
            if entry is not None:
                entry['n_submit'] += 1
                # This time, do not pop and add at end,
                # b/c it's a recent addition, it must be at or near the end already.
                return key

            loop, sess, sem = self._worker
            if not isinstance(url, str):
                # `url` is a dict containing `url` and other keyword args.
                download_kwargs = {**download_kwargs, **url}
                # For the same kwarg specified by both `download_kwargs` and `url`,
                # the value in `url` takes precedence, b/c it is a more specific
                # spec, that is, it is specified along with one particular URL,
                # whereas the `download_kwargs` to this function are supposed to be unchanging
                # in repeated calls to this function for multiple requests.
                url = download_kwargs.pop('url')
            fut = asyncio.run_coroutine_threadsafe(
                self.a_download(url, session=sess, sem=sem, **download_kwargs),
                loop,
            )
            # Request the (async) download to execute in the worker thread,
            # which manages an async loop. Once the execution finishes,
            # the result will populate the Future object `fut`.

            self._cache[key] = {'fut': fut, 'n_submit': 1, 'n_redeem': 0}

        return key

    def redeem(self, key: str, *, return_exception: bool = False) -> bytes:
        """
        Call ``redeem`` after the ``url`` has been submitted.
        For one submission, call ``redeem`` exactly once.
        ``key`` is the value returned by :meth:`submit`.

        This will block and wait if the result is not yet available.
        """
        cache = self._cache
        with self._cache_lock:
            entry = cache[key]
            # This needs to hold the lock, for else in very tricky situations
            # this could raise KeyError b/c the entry happens to be taken off in ``self.submit``
            # by ``pop``.

        # I think the following, w/o lock, may not be absolutely safe
        # when multiple threads call ``redeem``.
        err = None
        try:
            result = entry['fut'].result()  # wait until finished
        except Exception as e:
            err = e

        with self._cache_not_full:
            entry['n_redeem'] += 1
            if len(cache) > self._cache_watermark:
                # Try to remove the oldest entry in the cache.
                # This may or may not be `entry`.
                k, v = next(iter(cache.items()))
                if v['n_redeem'] >= v['n_submit']:
                    # All requests have been redeemed.
                    # This entry is just a cache for possible future use.
                    # It's safe to delete it.
                    del cache[k]
                    self._cache_not_full.notify()
        if err is not None:
            if return_exception:
                return err
            else:
                raise err
        return result

    def cancel(self, key: str) -> bool:
        # Refer to ``redeem``.
        cache = self._cache
        fut = None
        with self._cache_lock:
            entry = cache[key]
            entry['n_redeem'] += 1
            if len(cache) > self._cache_watermark:
                if entry['n_redeem'] >= entry['n_submit']:
                    fut = entry('fut')
                    del cache[key]
                    self._cache_not_full.notify()
                else:
                    k, v = next(iter(cache.items()))
                    if v['n_redeem'] >= v['n_submit']:
                        del cache[k]
                        self._cache_not_full.notify()
        if fut is None:
            return False
        return fut.cancel()

    def get(
        self, url: str | dict, *, return_exception=False, **download_kwargs
    ) -> bytes:
        key = self.submit(url, **download_kwargs)
        return self.redeem(key, return_exception=return_exception)

    def stream(
        self,
        urls: Iterable[str | dict],
        *,
        return_x: bool = False,
        return_exceptions: bool = False,
        preprocessor: callable = None,
        **download_kwargs,
    ) -> Iterator:
        """
        urls
            Usually a stream of asset URLs, but see ``preprocessor``.
        preprocessor
            A function that takes the input (one element of ``urls``)
            and returns the url. This is used in streaming where the input
            ``urls`` contains not only URLs.
            Example:

                preprocessor=lambda x: x[-1]

            In this example, the input is a tuple (or list), where the actual URL
            is the last element, and other things are simply ignored
            (while they will be kept if ``return_x=True``).

            This is a parameter to ``fifo_stream``.
        """
        # This function uses a worker thread to take URLs off the incoming stream
        # and submit them to a thread-pool for downloading; actual downloading happens in the
        # thread pool. In the current thread, it may block while waiting on the "Future"
        # objects of the downloading operation.
        # This method is thread safe, meaning the object can be used in
        # multiple threads with this method called independently.

        submit = self.submit
        redeem = self.redeem
        cancel_ = self.cancel

        class Fut:
            def __init__(self, key):
                self._key = key

            def result(self):
                return redeem(self._key, return_exception=False)

            def cancel(self):
                return cancel_(self._key)

        def feed(url, **kwargs):
            return Fut(submit(url, **kwargs))

        return fifo_stream(
            urls,
            feed,
            return_x=return_x,
            return_exceptions=return_exceptions,
            capacity=self._cache_size,
            preprocessor=preprocessor,
            name=f'{self.__class__.__name__}-stream-submitter',
            **download_kwargs,
        )


class ImageDownloader(Multidownloader):
    async def a_download(
        self, url, *, session, sem, height: int = None, width: int = None
    ) -> bytes:
        async with sem:
            if height and width:
                image_params = f'?odnHeight={height}&odnWidth={width}&odnBg=FFFFFF'
                url = url + image_params
            elif height is None and width is None:
                pass
            else:
                raise ValueError(
                    f"'height' is {height} while 'width' is {width}. "
                    f'Both of the them need to be integer if image needs to be resized, '
                    f'otherwise BOTH of them need to be None'
                )

            try:
                async with session.get(url, raise_for_status=True) as response:
                    image_bytes = await response.read()
            except aiohttp.ClientResponseError as e:
                if e.status == 404:
                    raise NotFoundError(url) from e
                    # We special-case this error because in a stream of image requests,
                    # some image URLs might be erronous, causing this error,
                    # but we don't want them to halt the other downloads, yet
                    # we still want to know image URLs that caused this error.
                raise

            # There may be other types of errors
            return image_bytes
