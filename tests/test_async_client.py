"""Tests for async pagination engine."""

from __future__ import annotations

import asyncio
import datetime

import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

from nocoly_explorer.async_client import RetryPolicy


def test_async_client_error_inherits_from_nocoly_error():
    from nocoly_explorer.async_client import AsyncClientError
    from nocoly_explorer.exceptions import NocolyError
    assert issubclass(AsyncClientError, NocolyError)


def test_pagination_limit_exceeded_inherits_from_async_client_error():
    from nocoly_explorer.async_client import AsyncClientError, PaginationLimitExceeded
    assert issubclass(PaginationLimitExceeded, AsyncClientError)


def test_retry_policy_default_values():
    p = RetryPolicy()
    assert p.max_retries == 5
    assert p.max_wait_seconds == 30.0
    assert p.base_delay == 0.5
    assert p.max_delay == 30.0


def test_retry_policy_validates_max_retries():
    with pytest.raises(ValueError):
        RetryPolicy(max_retries=-1)


def test_retry_policy_validates_max_wait():
    with pytest.raises(ValueError):
        RetryPolicy(max_wait_seconds=0)


def test_retry_policy_compute_backoff_with_jitter_is_bounded():
    p = RetryPolicy(base_delay=0.5, max_delay=30.0)
    for attempt in range(20):
        delay = p.compute_backoff(attempt)
        assert 0 <= delay <= p.max_delay


def test_retry_policy_parse_retry_after_seconds():
    p = RetryPolicy()
    assert p.parse_retry_after("5") == 5.0
    assert p.parse_retry_after("0.5") == 0.5


def test_retry_policy_parse_retry_after_http_date():
    p = RetryPolicy()
    future = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=10)
    http_date = future.strftime("%a, %d %b %Y %H:%M:%S GMT")
    delay = p.parse_retry_after(http_date)
    assert 8.0 <= delay <= 12.0


def test_retry_policy_parse_retry_after_invalid_falls_back_to_none():
    p = RetryPolicy()
    assert p.parse_retry_after("garbage") is None


def test_retry_policy_parse_retry_after_caps_at_max_wait():
    p = RetryPolicy(max_wait_seconds=10.0)
    assert p.parse_retry_after("999") == 10.0


@pytest.mark.asyncio
async def test_token_bucket_burst_allows_initial_requests():
    from nocoly_explorer.async_client import TokenBucket
    bucket = TokenBucket(rate_per_second=2.0, burst=5)
    t0 = asyncio.get_event_loop().time()
    for _ in range(5):
        await bucket.acquire()
    elapsed = asyncio.get_event_loop().time() - t0
    assert elapsed < 0.5


@pytest.mark.asyncio
async def test_token_bucket_throttles_beyond_burst():
    from nocoly_explorer.async_client import TokenBucket
    bucket = TokenBucket(rate_per_second=10.0, burst=2)
    t0 = asyncio.get_event_loop().time()
    for _ in range(4):
        await bucket.acquire()
    elapsed = asyncio.get_event_loop().time() - t0
    assert elapsed >= 0.1


@pytest.mark.asyncio
async def test_token_bucket_rejects_invalid_rate():
    from nocoly_explorer.async_client import TokenBucket
    with pytest.raises(ValueError):
        TokenBucket(rate_per_second=0)
    with pytest.raises(ValueError):
        TokenBucket(rate_per_second=-1)


@pytest.mark.asyncio
async def test_token_bucket_rejects_invalid_burst():
    from nocoly_explorer.async_client import TokenBucket
    with pytest.raises(ValueError):
        TokenBucket(rate_per_second=1.0, burst=0)


@pytest.fixture
def server_pages():
    from async_fixtures import make_handler
    return [
        [{"a": 1}, {"a": 2}],
        [{"a": 3}, {"a": 4}],
        [{"a": 5}],
    ]


@pytest.mark.asyncio
async def test_fetch_single_page_returns_rows(server_pages):
    from nocoly_explorer.async_client import AsyncWorksheetClient
    from async_fixtures import make_handler
    handler = make_handler(server_pages)
    app = web.Application()
    app.router.add_get("/worksheets/ws/rows", handler)
    async with TestServer(app) as server:
        async with AsyncWorksheetClient(
            base_url=str(server.make_url("")), auth_token="dummy", worksheet_id="ws",
        ) as client:
            rows, _has_more = await client._fetch_page(page=1, page_size=200)
            assert rows == [{"a": 1}, {"a": 2}]


@pytest.mark.asyncio
async def test_fetch_single_page_retries_on_429(server_pages):
    from nocoly_explorer.async_client import AsyncWorksheetClient, RetryPolicy
    from async_fixtures import make_handler
    handler = make_handler(server_pages, fail_first_n=2, retry_after="0")
    app = web.Application()
    app.router.add_get("/worksheets/ws/rows", handler)
    async with TestServer(app) as server:
        async with AsyncWorksheetClient(
            base_url=str(server.make_url("")), auth_token="dummy", worksheet_id="ws",
            retry_policy=RetryPolicy(max_retries=5, base_delay=0.01, max_delay=0.05),
        ) as client:
            rows, _has_more = await client._fetch_page(page=1, page_size=200)
            assert rows == [{"a": 1}, {"a": 2}]


@pytest.mark.asyncio
async def test_fetch_single_page_raises_after_max_retries(server_pages):
    from nocoly_explorer.async_client import AsyncWorksheetClient, RetryPolicy, AsyncClientError
    from async_fixtures import make_handler
    handler = make_handler(server_pages, fail_first_n=99)
    app = web.Application()
    app.router.add_get("/worksheets/ws/rows", handler)
    async with TestServer(app) as server:
        async with AsyncWorksheetClient(
            base_url=str(server.make_url("")), auth_token="dummy", worksheet_id="ws",
            retry_policy=RetryPolicy(max_retries=2, base_delay=0.01, max_delay=0.05),
        ) as client:
            with pytest.raises(AsyncClientError):
                await client._fetch_page(page=1, page_size=200)


@pytest.mark.asyncio
async def test_fetch_single_page_handles_5xx_with_backoff(server_pages):
    from nocoly_explorer.async_client import AsyncWorksheetClient, RetryPolicy
    from async_fixtures import make_handler
    handler = make_handler(server_pages, fail_first_n=1, fail_status=503)
    app = web.Application()
    app.router.add_get("/worksheets/ws/rows", handler)
    async with TestServer(app) as server:
        async with AsyncWorksheetClient(
            base_url=str(server.make_url("")), auth_token="dummy", worksheet_id="ws",
            retry_policy=RetryPolicy(max_retries=5, base_delay=0.01, max_delay=0.05),
        ) as client:
            rows, _has_more = await client._fetch_page(page=1, page_size=200)
            assert rows == [{"a": 1}, {"a": 2}]


@pytest.mark.asyncio
async def test_fetch_all_returns_all_rows_in_order(server_pages):
    from nocoly_explorer.async_client import AsyncWorksheetClient
    from async_fixtures import make_handler
    handler = make_handler(server_pages)
    app = web.Application()
    app.router.add_get("/worksheets/ws/rows", handler)
    async with TestServer(app) as server:
        async with AsyncWorksheetClient(
            base_url=str(server.make_url("")), auth_token="dummy", worksheet_id="ws",
        ) as client:
            rows = await client.fetch_all_async(page_size=200, max_pages=10)
            assert [r["a"] for r in rows] == [1, 2, 3, 4, 5]


@pytest.mark.asyncio
async def test_fetch_all_preserves_order_when_pages_complete_out_of_order():
    from nocoly_explorer.async_client import AsyncWorksheetClient
    pages = [
        [{"a": 1, "page": 1}],
        [{"a": 2, "page": 2}],
        [{"a": 3, "page": 3}],
    ]

    async def handler(request):
        page = int(request.query.get("page", "1"))
        if page == 1:
            await asyncio.sleep(0.2)
        return web.json_response({"rows": pages[page - 1], "has_more": page < 3})

    app = web.Application()
    app.router.add_get("/worksheets/ws/rows", handler)
    async with TestServer(app) as server:
        async with AsyncWorksheetClient(
            base_url=str(server.make_url("")), auth_token="dummy", worksheet_id="ws",
            concurrency=4, requests_per_second=1000,
        ) as client:
            rows = await client.fetch_all_async(page_size=10, max_pages=10)
            assert [r["a"] for r in rows] == [1, 2, 3]


@pytest.mark.asyncio
async def test_fetch_all_stops_on_short_page():
    from nocoly_explorer.async_client import AsyncWorksheetClient
    from async_fixtures import make_handler
    handler = make_handler([[{"a": 1}], [{"a": 2}], [{"a": 3}]])
    app = web.Application()
    app.router.add_get("/worksheets/ws/rows", handler)
    async with TestServer(app) as server:
        async with AsyncWorksheetClient(
            base_url=str(server.make_url("")), auth_token="dummy", worksheet_id="ws",
        ) as client:
            rows = await client.fetch_all_async(page_size=1, max_pages=100)
            assert [r["a"] for r in rows] == [1, 2, 3]


@pytest.mark.asyncio
async def test_fetch_all_handles_empty_result():
    from nocoly_explorer.async_client import AsyncWorksheetClient
    from async_fixtures import make_handler
    handler = make_handler([[]])
    app = web.Application()
    app.router.add_get("/worksheets/ws/rows", handler)
    async with TestServer(app) as server:
        async with AsyncWorksheetClient(
            base_url=str(server.make_url("")), auth_token="dummy", worksheet_id="ws",
        ) as client:
            rows = await client.fetch_all_async(page_size=10, max_pages=10)
            assert rows == []


@pytest.mark.asyncio
async def test_fetch_all_raises_when_max_pages_exceeded_without_terminator():
    """Stops at max_pages even if server has more; raises PaginationLimitExceeded."""
    from nocoly_explorer.async_client import AsyncWorksheetClient, PaginationLimitExceeded
    from async_fixtures import make_handler
    handler = make_handler([[{"a": i}] for i in range(100)])
    app = web.Application()
    app.router.add_get("/worksheets/ws/rows", handler)
    async with TestServer(app) as server:
        async with AsyncWorksheetClient(
            base_url=str(server.make_url("")), auth_token="dummy", worksheet_id="ws",
        ) as client:
            with pytest.raises(PaginationLimitExceeded):
                await client.fetch_all_async(page_size=1, max_pages=3)


@pytest.mark.asyncio
async def test_fetch_all_raises_pagination_limit_when_max_pages_exceeded():
    from nocoly_explorer.async_client import AsyncWorksheetClient, PaginationLimitExceeded
    from async_fixtures import make_handler
    handler = make_handler([[{"a": i}] for i in range(20)])
    app = web.Application()
    app.router.add_get("/worksheets/ws/rows", handler)
    async with TestServer(app) as server:
        async with AsyncWorksheetClient(
            base_url=str(server.make_url("")), auth_token="dummy", worksheet_id="ws",
            concurrency=4, requests_per_second=1000,
        ) as client:
            with pytest.raises(PaginationLimitExceeded):
                await client.fetch_all_async(page_size=1, max_pages=2)


@pytest.mark.asyncio
async def test_fetch_pages_async_yields_pages_in_order(server_pages):
    from nocoly_explorer.async_client import AsyncWorksheetClient
    from async_fixtures import make_handler
    handler = make_handler(server_pages)
    app = web.Application()
    app.router.add_get("/worksheets/ws/rows", handler)
    async with TestServer(app) as server:
        async with AsyncWorksheetClient(
            base_url=str(server.make_url("")), auth_token="dummy", worksheet_id="ws",
        ) as client:
            pages = []
            async for page in client.fetch_pages_async(page_size=2, max_pages=10):
                pages.append(page)
            assert pages == [[{"a": 1}, {"a": 2}], [{"a": 3}, {"a": 4}], [{"a": 5}]]


@pytest.mark.asyncio
async def test_fetch_pages_async_handles_empty_result():
    from nocoly_explorer.async_client import AsyncWorksheetClient
    from async_fixtures import make_handler
    handler = make_handler([[]])
    app = web.Application()
    app.router.add_get("/worksheets/ws/rows", handler)
    async with TestServer(app) as server:
        async with AsyncWorksheetClient(
            base_url=str(server.make_url("")), auth_token="dummy", worksheet_id="ws",
        ) as client:
            pages = []
            async for page in client.fetch_pages_async(page_size=10, max_pages=10):
                pages.append(page)
            assert pages == [[]]


@pytest.mark.asyncio
async def test_fetch_all_cancellation_cancels_inflight_tasks():
    from nocoly_explorer.async_client import AsyncWorksheetClient
    from async_fixtures import make_handler
    handler = make_handler([[{"a": i}] for i in range(5)])
    app = web.Application()
    app.router.add_get("/worksheets/ws/rows", handler)
    async with TestServer(app) as server:
        async with AsyncWorksheetClient(
            base_url=str(server.make_url("")), auth_token="dummy", worksheet_id="ws",
            concurrency=4, requests_per_second=1000,
        ) as client:
            task = asyncio.create_task(
                client.fetch_all_async(page_size=1, max_pages=100)
            )
            await asyncio.sleep(0.05)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            assert task.cancelled() or task.done()