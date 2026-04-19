from __future__ import annotations

import io
import logging
import pickle

import httpx

logging.getLogger('httpx').setLevel(logging.WARNING)

logger = logging.getLogger(__name__)


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


# Fix httpx.HTTPStatusError pickle error, as of at least httpx version 0.28.1
# https://github.com/encode/httpx/issues/1990
#
# As of httpx 0.28.1, there's a bug that makes instances of httpx.HTTPStatusError
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
# The following hack fixes this problem.
try:
    err = httpx.HTTPStatusError('error', request=None, response=None)
    _ = pickle.loads(pickle.dumps(err))
except TypeError:

    def _make_httpstatuserror(msg, request, response):
        return httpx.HTTPStatusError(msg, request=request, response=response)

    def _reduce_(self):
        return _make_httpstatuserror, (self.args[0], self._request, self.response)

    httpx.HTTPStatusError.__reduce__ = _reduce_


def _get_payload_args(method, payload, payload_type, **kwargs):
    method = method.lower()
    args = {}
    if method == 'get':
        if payload:
            args = {'params': payload}
            if payload_type:
                assert payload_type == 'json'
            else:
                payload_type = 'json'
    elif method == 'post':
        if payload:
            if isinstance(payload, bytes):
                args = {'content': payload}
                assert payload_type and payload_type not in ('json', 'text')
                payload_type = 'application/' + payload_type
                # The corresponding server code will handle the received bytes
                # according to the payload type. User can design custom data types
                # here as long as the client and server sides have agreement on
                # how to handle it. The caller of this function is responsible for
                # converting the data to bytes; the server is responsible for
                # converting the received bytes to custom data type according
                # to insider knowledge about the data type, as signaled by the value
                # of `payload_type`, which can be a custom value.
            elif isinstance(payload, str):
                args = {'content': payload.encode()}
                if payload_type:
                    assert payload_type == 'text'
                payload_type = 'text/plain'
            else:
                if payload_type:
                    assert payload_type == 'json'
                args = {'json': payload}
                payload_type = 'application/json'
    elif method == 'put':
        if payload:
            args = {'content': payload}
            if not payload_type:
                payload_type = 'application/json'
    elif method == 'delete':
        assert not payload
    else:
        raise ValueError('unknown method', method)

    kwargs = {**args, **kwargs}
    if payload_type:
        if 'headers' in kwargs:
            kwargs['headers'].setdefault('content-type', payload_type)
        else:
            kwargs['headers'] = {'content-type': payload_type}

    return method, kwargs


def request(
    url,
    method,
    *,
    session: httpx.Client,
    payload=None,
    payload_type: str = None,
    **kwargs,
):
    """
    Note: this is a sync function. For repeated use, this may be used in threads
    in a streaming pipeline or in an async context.
    """
    method, kwargs = _get_payload_args(method, payload, payload_type, **kwargs)
    if 'content' in kwargs and isinstance(kwargs['content'], bytes):
        kwargs['content'] = io.BytesIO(kwargs['content'])

    response = getattr(session, method)(url, **kwargs)
    # This could raise `httpx.ConnectTimeout`.
    response.raise_for_status()
    # This may raise `httpx.HTTPStatusError`.
    # User can look into the specific issues by checking the properties
    # `request` and `response` of this exception instance.

    response_content_type = response.headers.get('content-type', '')
    if response_content_type.startswith('text/'):
        return response.text
    if response_content_type == 'application/json':
        return response.json()
    if response_content_type.startswith('image/'):
        return response.content
    if response_content_type.startswith('application/'):
        return response.content
    return response


async def a_request(
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

    `payload` is a Python native type, usually `dict`.

    The client `session` is managed by the caller.
    """
    method, kwargs = _get_payload_args(method, payload, payload_type, **kwargs)
    # Note: in contrast to the sync version, the `content` bytes are not put in
    # a `io.BytesIO`.

    response = await getattr(session, method)(url, **kwargs)
    # I've observed these exceptions:
    #   httpx.TimeoutException, httpcore.TimeoutException
    #   httpx.RemoteProtocolError, httpcore.RemoteProtocolError
    #   httpx.ReadError, httpcore.ReadError
    #   ConnectionError

    response.raise_for_status()

    response_content_type = response.headers.get('content-type', '')
    if response_content_type.startswith('text/'):
        return response.text
    if response_content_type == 'application/json':
        return response.json()
    if response_content_type.startswith('image/'):
        return await response.aread()
    if response_content_type.startswith('application/'):
        return await response.aread()
    return response
