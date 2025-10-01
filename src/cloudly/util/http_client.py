from __future__ import annotations

import io
import logging
import pickle
from contextlib import contextmanager
from time import perf_counter

import httpcore
import httpx
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_random_exponential,
)

from cloudly.util.serializer import (
    OrjsonSerializer,
    PickleSerializer,
)

logging.getLogger('httpx').setLevel(logging.WARNING)

logger = logging.getLogger(__name__)


class RequestTimeoutError(RuntimeError):
    # Connection timed out from the client side;
    # no meaningful response was received from the server.
    pass


# You may want to customize the `httpx.Timeout` and `httpx.Limits` values
# used. Both are accepted as parameters for `httpx.Client` and `httpx.AsyncClient`.
#
# The httpx default timeout is 5.0; in some use cases I used this value:
#
#   httpx.Timeout(60.0, connect=60.0, read=60.0 * 5, write=60.0)
#
# The default `httpx.Limits` uses `keepalive_expiry=5.0`. In some use cases
# I used the following:
#
#   httpx.Limits(
#       max_connections=100,
#       max_keepalive_connections=20,
#       keepalive_expiry=60.0,  # httpx default is 5.0
#   )

# When making repeated HTTP calls, you should create one ``httpx.Client``
# object and share it throughout, as opposed to creating a new one
# for each call. The former is more efficient.
# The `httpx.Client` class supports context management.


# Fix httpx.HTTPStatusError pickle error, as of at least httpx version 0.23.3
# https://github.com/encode/httpx/issues/1990
try:
    err = httpx.HTTPStatusError('error', request=None, response=None)
    _ = pickle.loads(pickle.dumps(err))
except TypeError:

    def _make_httpstatuserror(msg, request, response):
        return httpx.HTTPStatusError(msg, request=request, response=response)

    def _reduce_(self):
        return _make_httpstatuserror, (self.args[0], self._request, self.response)

    httpx.HTTPStatusError.__reduce__ = _reduce_


def get_response_data(response):
    """
    In httpx client code, use this function to parse data out of
    the response object returned from a HTTP request.
    """
    response_content_type = response.headers.get('content-type', '')
    if response_content_type.startswith('text/plain'):
        data = response.text
    elif response_content_type == 'application/json':
        data = response.json()
    elif response_content_type == 'application/orjson-stream':
        data = OrjsonSerializer.deserialize(response.content)
    elif response_content_type == 'application/pickle-stream':
        data = PickleSerializer.deserialize(response.content)
    # handle other types as needed
    elif response_content_type.startswith('image/'):
        data = response.content
    elif response_content_type.startswith('text/html; charset=utf-8'):
        data = response.text
    else:
        data = response
    return data


async def a_get_response_data(response):
    """
    In httpx client code, use this function to parse data out of
    the response object returned from a HTTP request.
    """
    response_content_type = response.headers.get('content-type', '')
    if response_content_type.startswith('text/plain'):
        data = response.text
    elif response_content_type == 'application/json':
        data = response.json()
    elif response_content_type == 'application/orjson-stream':
        data = OrjsonSerializer.deserialize(await response.aread())
    elif response_content_type == 'application/pickle-stream':
        data = PickleSerializer.deserialize(await response.aread())
    # handle other types as needed
    elif response_content_type.startswith('image/'):
        data = response.content
    elif response_content_type.startswith('text/html; charset=utf-8'):
        data = response.text
    else:
        data = response
    return data


def rest_request(
    url,
    method,
    *,
    session: httpx.Client,
    payload=None,
    payload_type: str = None,
    _stream: bool = False,  # experimental
    **kwargs,
):
    """
    Note: this is a sync function. For repeated use, this may be used in threads
    in a streaming pipeline or in an async context.
    """
    args = {}
    if method in ('get', 'GET'):
        if payload_type is None:
            payload_type = 'json'
        else:
            assert payload_type == 'json'
        if payload:
            args = {'params': payload}
    elif method in ('post', 'POST'):
        if payload:
            if payload_type is None:
                payload_type = 'json'
            if isinstance(payload, bytes):
                args = {'content': io.BytesIO(payload)}
            elif payload_type == 'text':
                args = {'content': io.BytesIO(payload.encode())}
                payload_type = 'text/plain'
            else:
                if payload_type == 'json':
                    args = {'json': payload}
                elif payload_type == 'orjson_stream':
                    args = {'content': io.BytesIO(OrjsonSerializer.serialize(payload))}
                elif payload_type == 'pickle-stream':
                    args = {'content': io.BytesIO(PickleSerializer.serialize(payload))}
                else:
                    raise ValueError(f"payload_type '{payload_type}' is not supported")
                payload_type = 'application/' + payload_type
    elif method in ('put', 'PUT'):
        if payload:
            args = {'content': payload}
    elif method in ('delete', 'DELETE'):
        assert not payload
    else:
        raise ValueError('unknown method', method)

    kwargs = {**args, **kwargs}
    if payload_type:
        if 'headers' in kwargs:
            kwargs['headers'].setdefault('content-type', payload_type)
        else:
            kwargs['headers'] = {'content-type': payload_type}

    try:
        if _stream:
            response = session.stream(method, url, **kwargs)
            return response
        else:
            response = getattr(session, method.lower())(url, **kwargs)
    except httpx.ConnectTimeout as e:
        raise RequestTimeoutError() from e

    try:
        response.raise_for_status()
        # This may raise `httpx.HTTPStatusError` (among others).
        # As of httpx 0.23.3, there's abug that makes this exception object
        # un-pickleable. The error message is
        #
        # >>> err = httpx.HTTPStatusError(...)
        # >>> y = pickle.dumps(err)
        # >>> z = pickle.loads(y)
        # >>> pickle.loads(pickle.dumps(y))
        # Traceback (most recent call last):
        # File "<stdin>", line 1, in <module>
        # TypeError: __init__() missing 2 required keyword-only arguments: 'request' and 'response'
        #
        # __init__() missing 2 required keyword-only arguments: 'request' and 'response'
        # >>>
        #
        # This httpx bug is fixed by the `__reduce__` hack earlier in this module.
    except httpx.HTTPStatusError as e:
        if response.headers.get('content-type') is None and response.extensions[
            'reason_phrase'
        ].endsth(b'connect: operation timed out'):
            raise RequestTimeoutError(
                response.status_code,  # 403
                response.extension['reason_phrase'],
            ) from e
            # TODO: look into how long it took `rest_request` to raise
            # this exception. It seems shorter than the `timeout` arg to `session`,
            # which is 60 seconds by default.
        else:
            raise
    else:
        return get_response_data(response)


@contextmanager
def stream_request(*args, **kwargs):
    with rest_request(*args, _stream=True, **kwargs) as response:
        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            if response.headers.get('content-type') is None and response.extensions[
                'reason_phrase'
            ].endsth(b'connect: operation timed out'):
                raise RequestTimeoutError(
                    response.status_code,  # 403
                    response.extension['reason_phrase'],
                ) from e
                # TODO: look into how long it took `rest_request` to raise
                # this exception. It seems shorter than the `timeout` arg to `session`,
                # which is 60 seconds by default.
            else:
                raise
        else:
            yield response


@retry(
    reraise=True,
    stop=stop_after_attempt(10),
    wait=wait_random_exponential(multiplier=1, max=60),
    retry=retry_if_exception_type(
        (
            httpx.TimeoutException,
            httpcore.TimeoutException,
            httpx.RemoteProtocolError,
            httpcore.RemoteProtocolError,
            httpx.ReadError,
            httpcore.ReadError,
        )
    ),
)
async def _a_request(func, url, **kwargs):
    time0 = perf_counter()
    try:
        response = await func(url, **kwargs)
        return response
    except (httpx.TimeoutException, httpcore.TimeoutException) as e:
        time1 = perf_counter()
        timeout_duration = time1 - time0
        logger.error(
            'HTTP request timed out after %d seconds with %s: %s',
            timeout_duration,
            e.__class__.__name__,
            str(e),
        )
        raise
    except (httpx.RemoteProtocolError, httpcore.RemoteProtocolError) as e:
        time1 = perf_counter()
        timeout_duration = time1 - time0
        logger.error(
            'HTTP request timed out after %d seconds with %s: %s',
            timeout_duration,
            e.__class__.__name__,
            str(e),
        )
        raise
    except ConnectionError as e:
        time1 = perf_counter()
        timeout_duration = time1 - time0
        logger.error(
            'HTTP request timed out after %d seconds with %s: %s',
            timeout_duration,
            e.__class__.__name__,
            str(e),
        )
        raise
    except Exception as e:
        logger.error('%s: %s', e.__class__.__name__, e)
        raise


async def a_rest_request(
    url,
    method,
    *,
    session: httpx.AsyncClient,
    payload=None,
    payload_type: str = None,
    **kwargs,
):
    """
    Make an sync call to a REST API.

    `payload` is a Python native type, usually `dict.

    The client `session` is managed by the caller.
    """
    args = {}
    if method in ('get', 'GET'):
        func = session.get
        if payload_type is None:
            payload_type = 'json'
        else:
            assert payload_type == 'json'
        if payload:
            args = {'params': payload}
    elif method in ('post', 'POST'):
        func = session.post
        if payload:
            # TODO: should 'data' be 'content' instead?
            if payload_type is None:
                payload_type = 'json'
            if isinstance(payload, bytes):
                args = {'content': io.BytesIO(payload)}
            elif payload_type == 'text':
                args = {'content': payload.encode()}
                payload_type = 'text/plain'
            else:
                if payload_type == 'json':
                    args = {'json': payload}
                elif payload_type == 'orjson_stream':
                    args = {'content': OrjsonSerializer.serialize(payload)}
                elif payload_type == 'pickle-stream':
                    args = {'content': PickleSerializer.serialize(payload)}
                else:
                    raise ValueError(f"payload_type '{payload_type}' is not supported")
                payload_type = 'application/' + payload_type
    elif method in ('put', 'PUT'):
        func = session.put
        if payload:
            args = {'content': payload}
    elif method in ('delete', 'DELETE'):
        func = session.delete
        assert not payload
    else:
        raise ValueError('unknown method', method)

    kwargs = {**args, **kwargs}
    if payload_type:
        if 'headers' in kwargs:
            kwargs['headers'].setdefault('content-type', payload_type)
        else:
            kwargs['headers'] = {'content-type': payload_type}

    try:
        response = await _a_request(func, url, **kwargs)
    except httpx.ConnectTimeout as e:
        raise RequestTimeoutError() from e

    try:
        response.raise_for_status()
    except httpx.HTTPStatusError as e:
        if response.headers.get('content-type') is None and response.extensions[
            'reason_phrase'
        ].endsth(b'connect: operation timed out'):
            raise RequestTimeoutError(
                response.status_code,  # 403
                response.extension['reason_phrase'],
            ) from e
            # TODO: look into how long it took `rest_request` to raise
            # this exception. It seems shorter than the `timeout` arg to `session`,
            # which is 60 seconds by default.
        else:
            raise
    else:
        return await a_get_response_data(response)
