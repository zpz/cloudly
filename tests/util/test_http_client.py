import json

import pytest
from httpx import AsyncClient, Client

from cloudly.util.http_client import a_request, request


def test_get():
    with Client() as client:
        y = request('https://httpbin.org/get', 'get', session=client)
        assert isinstance(y, dict)
        assert 'origin' in y
        assert y['url'] == 'https://httpbin.org/get'

        y = request(
            'https://httpbin.org/get',
            'get',
            session=client,
            payload={'name': 'cloudly', 'path': 'cloudly.util.http_client'},
        )
        assert y['args'] == {'name': 'cloudly', 'path': 'cloudly.util.http_client'}


@pytest.mark.asyncio
async def test_a_get():
    async with AsyncClient() as client:
        y = await a_request('https://httpbin.org/get', 'get', session=client)
        assert isinstance(y, dict)
        assert 'origin' in y
        assert y['url'] == 'https://httpbin.org/get'

        y = await a_request(
            'https://httpbin.org/get',
            'get',
            session=client,
            payload={'name': 'cloudly', 'path': 'cloudly.util.http_client'},
        )
        assert y['args'] == {'name': 'cloudly', 'path': 'cloudly.util.http_client'}


def test_post():
    with Client() as client:
        y = request(
            'https://httpbin.org/post',
            'post',
            session=client,
            payload={'q1': 'yes', 'q2': 'no'},
        )
        assert isinstance(y, dict)
        assert 'origin' in y
        assert y['url'] == 'https://httpbin.org/post'
        assert json.loads(y['data']) == {'q1': 'yes', 'q2': 'no'}


@pytest.mark.asyncio
async def test_a_post():
    async with AsyncClient() as client:
        y = await a_request(
            'https://httpbin.org/post',
            'post',
            session=client,
            payload={'q1': 'yes', 'q2': 'no'},
        )
        assert isinstance(y, dict)
        assert 'origin' in y
        assert y['url'] == 'https://httpbin.org/post'
        assert json.loads(y['data']) == {'q1': 'yes', 'q2': 'no'}
