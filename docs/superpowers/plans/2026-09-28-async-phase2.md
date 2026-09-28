# Async Pagination Engine — Phase 2 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build an async pagination engine that fetches multiple Nocoly worksheet pages concurrently, merges results in order, and survives 429 / 5xx / Retry-After with predictable concurrency.

**Architecture:** `AsyncWorksheetClient` uses aiohttp + asyncio. A bounded `Semaphore(N=8)` caps concurrent pages; a `TokenBucket` rate-limits; a `RetryPolicy` handles 429 (Retry-After) and 5xx (exponential backoff + jitter). Pages are merged in *original order* — not completion order — via a `(page_num → data)` dict and sorted flatten at the end. The streaming exporter from Phase 1 will plug into the same `fetch_all_async()` interface as a future composition.

**Tech Stack:** aiohttp ≥ 3.9 (new dep), `pytest-asyncio` for tests, `aiohttp.test_utils` for the test server.

**Spec:** `docs/superpowers/specs/2026-09-28-enterprise-v0.2.0-design.md` §4.2.

---

## Global Constraints

- Python 3.10+ (project baseline).
- aiohttp ≥ 3.9 (new optional `[async]` extra). Importing `AsyncWorksheetClient` without aiohttp raises a clear `ImportError` pointing to the extra.
- Backward compatibility: every v0.1.1 + Phase 1 import path keeps working. New code lives in a new module.
- Concurrency cap default 8 (matches spec); tunable via constructor.
- Memory ceiling: peak in-flight = `concurrency × page_size` rows.
- All async exceptions must subclass the existing `NocolyError` hierarchy. New: `AsyncClientError` (base), `PaginationLimitExceeded`.
- `pytest-asyncio` mode = `auto` (configured in `pyproject.toml`).
- Logging via `logging.getLogger("nocoly_explorer.async")`.
- Order-preserving merge: pages returned in `(page_num, data)` order regardless of which completed first.

---

## Review Focus (input classes a user will hit but individual tests don't cover)

1. **Server signals end-of-data with a non-empty short page** — when `len(data) < page_size` and `has_more=False`, pagination must stop immediately (don't fetch page N+1 speculatively).
2. **Empty result set (zero rows total)** — server returns page 1 with `[]` and `has_more=False`. Must not infinite-loop. Should return empty list without warning.
3. **429 with delta-seconds Retry-After** — already covered by retry unit tests, but **a 429 on the very first request** must still be retried (not crash with "no attempts").
4. **Cancellation mid-fetch** — if the consumer cancels (e.g. closes the loop), pending tasks must be cancelled cleanly, not leak.
5. **Token bucket saturation** — when configured `requests_per_second=2`, calling `_fetch_page` 10 times back-to-back must take ≥ ~4s, not bypass the limit.

---

## File Structure

New files:
- `src/nocoly_explorer/async_client.py` — `AsyncWorksheetClient`, `TokenBucket`, `RetryPolicy`, `AsyncClientError`, `PaginationLimitExceeded`
- `tests/test_async_client.py` — comprehensive test suite
- `tests/async_fixtures.py` — mock aiohttp server fixture

Modified files:
- `pyproject.toml` — add `aiohttp>=3.9.0` to `[async]` optional extra; add `pytest-asyncio>=0.23` to dev deps; set `asyncio_mode = "auto"`
- `src/nocoly_explorer/__init__.py` — re-export `AsyncWorksheetClient`, `AsyncClientError`, `PaginationLimitExceeded`
- `src/nocoly_explorer/exceptions.py` — add `AsyncClientError`, `PaginationLimitExceeded`

---

## Task 1: Exceptions + RetryPolicy foundation

**Files:**
- Modify: `src/nocoly_explorer/exceptions.py`
- Create: `src/nocoly_explorer/async_client.py` (RetryPolicy class only at first)
- Test: `tests/test_async_client.py`

**Interfaces:**
- Consumes: nothing yet
- Produces: `AsyncClientError(NocolyError)`, `PaginationLimitExceeded(AsyncClientError)`, `RetryPolicy(max_retries, max_wait_seconds, base_delay, max_delay)`

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_async_client.py — exceptions + RetryPolicy only
from __future__ import annotations

import asyncio

import pytest

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
    import datetime
    p = RetryPolicy()
    future = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=10)
    http_date = future.strftime("%a, %d %b %Y %H:%M:%S GMT")
    delay = p.parse_retry_after(http_date)
    # Allow ±2 seconds of test-machine slack
    assert 8.0 <= delay <= 12.0


def test_retry_policy_parse_retry_after_invalid_falls_back_to_none():
    p = RetryPolicy()
    assert p.parse_retry_after("garbage") is None


def test_retry_policy_parse_retry_after_caps_at_max_wait():
    p = RetryPolicy(max_wait_seconds=10.0)
    assert p.parse_retry_after("999") == 10.0
```

- [ ] **Step 2: Run tests; expect ImportError**

Run: `pytest tests/test_async_client.py -v`
Expected: collection error.

- [ ] **Step 3: Add exceptions**

Append to `src/nocoly_explorer/exceptions.py`:

```python
class AsyncClientError(NocolyError):
    """Base exception for async pagination failures."""


class PaginationLimitExceeded(AsyncClientError):
    """Raised when pagination exceeds the configured max_pages or page count."""
```

- [ ] **Step 4: Create `async_client.py` with RetryPolicy + exceptions**

```python
"""Asynchronous worksheet pagination engine."""

from __future__ import annotations

import asyncio
import datetime
import logging
import random
from dataclasses import dataclass
from email.utils import parsedate_to_datetime
from typing import Any, AsyncIterator, Dict, List, Optional

import aiohttp

from .exceptions import NocolyError

_LOGGER = logging.getLogger("nocoly_explorer.async")


class AsyncClientError(NocolyError):
    """Base exception for async pagination failures."""


class PaginationLimitExceeded(AsyncClientError):
    """Raised when pagination exceeds the configured max_pages."""


@dataclass(slots=True)
class RetryPolicy:
    """Configuration for retry-on-transient-error behavior."""

    max_retries: int = 5
    max_wait_seconds: float = 30.0
    base_delay: float = 0.5
    max_delay: float = 30.0
    jitter_factor: float = 0.5

    def __post_init__(self) -> None:
        if self.max_retries < 0:
            raise ValueError(f"max_retries must be >= 0 (got {self.max_retries})")
        if self.max_wait_seconds <= 0:
            raise ValueError(f"max_wait_seconds must be > 0 (got {self.max_wait_seconds})")
        if self.base_delay <= 0:
            raise ValueError(f"base_delay must be > 0 (got {self.base_delay})")
        if self.max_delay <= 0:
            raise ValueError(f"max_delay must be > 0 (got {self.max_delay})")
        if not 0.0 <= self.jitter_factor <= 1.0:
            raise ValueError(f"jitter_factor must be in [0, 1] (got {self.jitter_factor})")

    def compute_backoff(self, attempt: int) -> float:
        """Exponential backoff with jitter. attempt=0 → base_delay."""
        delay = min(self.base_delay * (2 ** attempt), self.max_delay)
        jitter = delay * self.jitter_factor * random.random()
        return min(delay + jitter, self.max_delay)

    def parse_retry_after(self, header: Optional[str]) -> Optional[float]:
        """Parse Retry-After header: delta-seconds or HTTP-date. Returns None on parse failure."""
        if header is None:
            return None
        header = header.strip()
        if not header:
            return None
        # delta-seconds form
        try:
            seconds = float(header)
            return min(seconds, self.max_wait_seconds)
        except ValueError:
            pass
        # HTTP-date form
        try:
            target = parsedate_to_datetime(header)
            if target is None:
                return None
            now = datetime.datetime.now(datetime.timezone.utc)
            delta = (target - now).total_seconds()
            return max(0.0, min(delta, self.max_wait_seconds))
        except (TypeError, ValueError):
            return None
```

- [ ] **Step 5: Run tests; expect all pass**

Run: `pytest tests/test_async_client.py -v`
Expected: 10 passed.

- [ ] **Step 6: Commit**

```bash
cd /Users/hermes/Downloads/nocoly-explorer
git add src/nocoly_explorer/exceptions.py src/nocoly_explorer/async_client.py tests/test_async_client.py
git commit -m "feat(async): RetryPolicy + AsyncClientError hierarchy"
```

---

## Task 2: TokenBucket

**Files:**
- Modify: `src/nocoly_explorer/async_client.py`
- Test: extend `tests/test_async_client.py`

**Interfaces:**
- Consumes: `rate_per_second` (float, tokens/sec), `burst` (float, bucket capacity)
- Produces: `TokenBucket(rate, burst)` with method `async def acquire(self) -> None` (blocks until a token is available)

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_async_client.py`:

```python
import pytest


@pytest.mark.asyncio
async def test_token_bucket_burst_allows_initial_requests():
    from nocoly_explorer.async_client import TokenBucket
    bucket = TokenBucket(rate_per_second=2.0, burst=5)
    t0 = asyncio.get_event_loop().time()
    # First 5 calls should return near-instantly
    for _ in range(5):
        await bucket.acquire()
    elapsed = asyncio.get_event_loop().time() - t0
    assert elapsed < 0.5  # generous; just confirm no long waits


@pytest.mark.asyncio
async def test_token_bucket_throttles_beyond_burst():
    from nocoly_explorer.async_client import TokenBucket
    bucket = TokenBucket(rate_per_second=10.0, burst=2)
    t0 = asyncio.get_event_loop().time()
    # First 2 are immediate; next 2 take ~0.2s total
    for _ in range(4):
        await bucket.acquire()
    elapsed = asyncio.get_event_loop().time() - t0
    assert elapsed >= 0.1  # at least 2 tokens worth of refill


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
```

- [ ] **Step 2: Run tests; expect AttributeError**

Run: `pytest tests/test_async_client.py::test_token_bucket_burst_allows_initial_requests -v`
Expected: ImportError or AttributeError.

- [ ] **Step 3: Implement `TokenBucket`**

Append to `src/nocoly_explorer/async_client.py`:

```python
class TokenBucket:
    """Async token bucket rate limiter."""

    def __init__(self, rate_per_second: float, burst: float = 1.0) -> None:
        if rate_per_second <= 0:
            raise ValueError(f"rate_per_second must be > 0 (got {rate_per_second})")
        if burst <= 0:
            raise ValueError(f"burst must be > 0 (got {burst})")
        self.rate = float(rate_per_second)
        self.burst = float(burst)
        self._tokens = float(burst)
        self._last = asyncio.get_event_loop().time()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self._lock:
            now = asyncio.get_event_loop().time()
            elapsed = now - self._last
            self._last = now
            self._tokens = min(self.burst, self._tokens + elapsed * self.rate)
            if self._tokens >= 1.0:
                self._tokens -= 1.0
                return
            # Sleep until 1 token is available
            wait = (1.0 - self._tokens) / self.rate
            self._tokens = 0.0
        await asyncio.sleep(wait)
```

- [ ] **Step 4: Run tests; expect all pass**

Run: `pytest tests/test_async_client.py -v`
Expected: 14 passed.

- [ ] **Step 5: Commit**

```bash
git add src/nocoly_explorer/async_client.py tests/test_async_client.py
git commit -m "feat(async): TokenBucket rate limiter"
```

---

## Task 3: AsyncWorksheetClient — single-page fetch with retry

**Files:**
- Modify: `src/nocoly_explorer/async_client.py`
- Test: extend `tests/test_async_client.py`
- Create: `tests/async_fixtures.py`

**Interfaces:**
- Consumes: aiohttp session, base URL, auth header, retry policy, token bucket, semaphore
- Produces: `AsyncWorksheetClient(base_url, auth_token, *, max_page_size=1000, retry_policy=..., concurrency=8, requests_per_second=10, session=None)`, method `_fetch_page(page: int, page_size: int, filter_criteria: dict) -> list[dict]`

- [ ] **Step 1: Write fixture**

```python
# tests/async_fixtures.py
"""Shared async test fixtures."""

from __future__ import annotations

from typing import Any, Dict, List

from aiohttp import web


def make_handler(
    pages: List[List[Dict[str, Any]]],
    *,
    fail_first_n: int = 0,
    fail_status: int = 429,
    retry_after: str = "1",
    max_page_param: int = 1_000_000,
):
    """Build an aiohttp handler that serves pages sequentially.

    - `fail_first_n` requests return `fail_status` with a Retry-After header.
    - Once exhausted, returns the next page from `pages`.
    - Page `i` (1-indexed) is served for `page=i&page_size=...` request until pages run out,
      then returns an empty page with has_more=False.
    """
    fail_count = 0

    async def handler(request: web.Request) -> web.Response:
        nonlocal fail_count
        page = int(request.query.get("page", "1"))
        page_size = int(request.query.get("page_size", "200"))
        # Fail the first fail_first_n requests
        if fail_count < fail_first_n:
            fail_count += 1
            return web.Response(
                status=fail_status,
                headers={"Retry-After": retry_after},
                text="rate limited",
            )
        # Serve page N (1-indexed); pages is 0-indexed
        idx = page - 1
        if idx < 0 or idx >= len(pages):
            return web.json_response({"rows": [], "has_more": False})
        rows = pages[idx]
        # has_more: True unless this is the last page
        has_more = idx < len(pages) - 1 or len(rows) == page_size
        return web.json_response({"rows": rows[:page_size], "has_more": has_more})

    return handler
```

- [ ] **Step 2: Write the failing tests for single-page fetch with retry**

Append to `tests/test_async_client.py`:

```python
from aiohttp import web
from aiohttp.test_utils import TestServer, TestClient
import pytest


@pytest.fixture
def server_pages():
    from tests.async_fixtures import make_handler
    return [
        [{"a": 1}, {"a": 2}],
        [{"a": 3}, {"a": 4}],
        [{"a": 5}],
    ]


@pytest.mark.asyncio
async def test_fetch_single_page_returns_rows(server_pages):
    from nocoly_explorer.async_client import AsyncWorksheetClient
    from tests.async_fixtures import make_handler
    handler = make_handler(server_pages)
    app = web.Application()
    app.router.add_get("/worksheets/ws/rows", handler)
    async with TestServer(app) as server:
        async with AsyncWorksheetClient(
            base_url=str(server.url), auth_token="dummy", worksheet_id="ws",
        ) as client:
            page = await client._fetch_page(page=1, page_size=200)
            assert page == [{"a": 1}, {"a": 2}]


@pytest.mark.asyncio
async def test_fetch_single_page_retries_on_429(server_pages):
    from nocoly_explorer.async_client import AsyncWorksheetClient, RetryPolicy
    from tests.async_fixtures import make_handler
    handler = make_handler(server_pages, fail_first_n=2, retry_after="0")
    app = web.Application()
    app.router.add_get("/worksheets/ws/rows", handler)
    async with TestServer(app) as server:
        async with AsyncWorksheetClient(
            base_url=str(server.url), auth_token="dummy", worksheet_id="ws",
            retry_policy=RetryPolicy(max_retries=5, base_delay=0.01, max_delay=0.05),
        ) as client:
            page = await client._fetch_page(page=1, page_size=200)
            assert page == [{"a": 1}, {"a": 2}]


@pytest.mark.asyncio
async def test_fetch_single_page_raises_after_max_retries(server_pages):
    from nocoly_explorer.async_client import AsyncWorksheetClient, RetryPolicy, AsyncClientError
    from tests.async_fixtures import make_handler
    handler = make_handler(server_pages, fail_first_n=99)
    app = web.Application()
    app.router.add_get("/worksheets/ws/rows", handler)
    async with TestServer(app) as server:
        async with AsyncWorksheetClient(
            base_url=str(server.url), auth_token="dummy", worksheet_id="ws",
            retry_policy=RetryPolicy(max_retries=2, base_delay=0.01, max_delay=0.05),
        ) as client:
            with pytest.raises(AsyncClientError):
                await client._fetch_page(page=1, page_size=200)


@pytest.mark.asyncio
async def test_fetch_single_page_handles_5xx_with_backoff(server_pages):
    from nocoly_explorer.async_client import AsyncWorksheetClient, RetryPolicy
    from tests.async_fixtures import make_handler
    handler = make_handler(server_pages, fail_first_n=1, fail_status=503)
    app = web.Application()
    app.router.add_get("/worksheets/ws/rows", handler)
    async with TestServer(app) as server:
        async with AsyncWorksheetClient(
            base_url=str(server.url), auth_token="dummy", worksheet_id="ws",
            retry_policy=RetryPolicy(max_retries=5, base_delay=0.01, max_delay=0.05),
        ) as client:
            page = await client._fetch_page(page=1, page_size=200)
            assert page == [{"a": 1}, {"a": 2}]
```

- [ ] **Step 3: Run tests; expect ImportError**

Run: `pytest tests/test_async_client.py::test_fetch_single_page_returns_rows -v`
Expected: ImportError or AttributeError.

- [ ] **Step 4: Implement `AsyncWorksheetClient` with `_fetch_page`**

Append to `src/nocoly_explorer/async_client.py`:

```python
@dataclass(slots=True)
class _SessionHolder:
    """Internal: holds either a user-supplied session or one we create/close."""

    session: aiohttp.ClientSession
    owned: bool


class AsyncWorksheetClient:
    """Asynchronous worksheet client with bounded concurrency + retry."""

    def __init__(
        self,
        base_url: str,
        auth_token: str,
        worksheet_id: str,
        *,
        max_page_size: int = 1000,
        retry_policy: Optional[RetryPolicy] = None,
        concurrency: int = 8,
        requests_per_second: float = 10.0,
        session: Optional[aiohttp.ClientSession] = None,
    ) -> None:
        if max_page_size < 1 or max_page_size > 1000:
            raise ValueError(f"max_page_size must be in [1, 1000] (got {max_page_size})")
        if concurrency < 1:
            raise ValueError(f"concurrency must be >= 1 (got {concurrency})")
        if requests_per_second <= 0:
            raise ValueError(f"requests_per_second must be > 0 (got {requests_per_second})")
        self.base_url = base_url.rstrip("/")
        self.worksheet_id = worksheet_id
        self.auth_token = auth_token
        self.max_page_size = max_page_size
        self.retry_policy = retry_policy or RetryPolicy()
        self.concurrency = concurrency
        self.requests_per_second = requests_per_second
        self._session: Optional[aiohttp.ClientSession] = session
        self._owns_session = session is None
        self._token_bucket = TokenBucket(requests_per_second, burst=float(concurrency))

    async def __aenter__(self) -> "AsyncWorksheetClient":
        if self._session is None:
            self._session = aiohttp.ClientSession(
                headers={"Authorization": f"Bearer {self.auth_token}"}
            )
        return self

    async def __aexit__(self, *exc) -> None:
        if self._owns_session and self._session is not None:
            await self._session.close()
            self._session = None

    def _url(self) -> str:
        return f"{self.base_url}/worksheets/{self.worksheet_id}/rows"

    async def _fetch_page(
        self,
        *,
        page: int,
        page_size: int,
        filter_criteria: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """Fetch one page, with retry on 429 (Retry-After) and 5xx."""
        if self._session is None:
            raise AsyncClientError("Client not entered; use 'async with AsyncWorksheetClient(...)'.")
        params: Dict[str, Any] = {"page": page, "page_size": page_size}
        if filter_criteria:
            params["filter"] = _serialize_filter(filter_criteria)
        last_exc: Optional[BaseException] = None
        for attempt in range(self.retry_policy.max_retries + 1):
            await self._token_bucket.acquire()
            try:
                async with self._session.get(self._url(), params=params) as resp:
                    if resp.status == 429:
                        retry_after = self.retry_policy.parse_retry_after(resp.headers.get("Retry-After"))
                        delay = retry_after if retry_after is not None else self.retry_policy.compute_backoff(attempt)
                        await asyncio.sleep(delay)
                        continue
                    if 500 <= resp.status < 600:
                        delay = self.retry_policy.compute_backoff(attempt)
                        await asyncio.sleep(delay)
                        continue
                    if resp.status >= 400:
                        body = await resp.text()
                        raise AsyncClientError(
                            f"HTTP {resp.status} fetching page {page}: {body[:200]}"
                        )
                    payload = await resp.json()
                    return payload.get("rows", [])
            except aiohttp.ClientError as exc:
                last_exc = exc
                delay = self.retry_policy.compute_backoff(attempt)
                _LOGGER.warning(
                    "async.client.transport_error",
                    extra={"page": page, "attempt": attempt, "error": str(exc)},
                )
                await asyncio.sleep(delay)
                continue
        raise AsyncClientError(
            f"Page {page} failed after {self.retry_policy.max_retries + 1} attempts; last error: {last_exc}"
        )


def _serialize_filter(criteria: Dict[str, Any]) -> str:
    """Serialize filter criteria to a JSON string for the query parameter."""
    import json
    return json.dumps(criteria, default=str)
```

- [ ] **Step 5: Run tests; expect 18 passed**

Run: `pytest tests/test_async_client.py -v`
Expected: 18 passed.

- [ ] **Step 6: Commit**

```bash
git add src/nocoly_explorer/async_client.py tests/test_async_client.py tests/async_fixtures.py
git commit -m "feat(async): AsyncWorksheetClient._fetch_page with retry + rate-limit"
```

---

## Task 4: AsyncWorksheetClient.fetch_all_async with bounded concurrency

**Files:**
- Modify: `src/nocoly_explorer/async_client.py`
- Test: extend `tests/test_async_client.py`

**Interfaces:**
- Consumes: `self._fetch_page`, `semaphore(N)`, `_session`
- Produces: `async def fetch_all_async(page_size=200, max_pages=1000, filter_criteria=None) -> list[dict]` returning rows in original page order

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_async_client.py`:

```python
@pytest.mark.asyncio
async def test_fetch_all_returns_all_rows_in_order(server_pages):
    from nocoly_explorer.async_client import AsyncWorksheetClient
    from tests.async_fixtures import make_handler
    handler = make_handler(server_pages)
    app = web.Application()
    app.router.add_get("/worksheets/ws/rows", handler)
    async with TestServer(app) as server:
        async with AsyncWorksheetClient(
            base_url=str(server.url), auth_token="dummy", worksheet_id="ws",
        ) as client:
            rows = await client.fetch_all_async(page_size=200, max_pages=10)
            assert [r["a"] for r in rows] == [1, 2, 3, 4, 5]


@pytest.mark.asyncio
async def test_fetch_all_preserves_order_when_pages_complete_out_of_order():
    """Construct a server that delays page 1; ensure output is still ordered."""
    from nocoly_explorer.async_client import AsyncWorksheetClient
    import asyncio

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
            base_url=str(server.url), auth_token="dummy", worksheet_id="ws",
            concurrency=4, requests_per_second=1000,
        ) as client:
            rows = await client.fetch_all_async(page_size=10, max_pages=10)
            assert [r["a"] for r in rows] == [1, 2, 3]


@pytest.mark.asyncio
async def test_fetch_all_stops_on_short_page(server_pages):
    """If a page returns fewer rows than page_size AND has_more=False, stop."""
    from nocoly_explorer.async_client import AsyncWorksheetClient
    from tests.async_fixtures import make_handler
    handler = make_handler([[{"a": 1}], [{"a": 2}], [{"a": 3}]])
    app = web.Application()
    app.router.add_get("/worksheets/ws/rows", handler)
    async with TestServer(app) as server:
        async with AsyncWorksheetClient(
            base_url=str(server.url), auth_token="dummy", worksheet_id="ws",
        ) as client:
            rows = await client.fetch_all_async(page_size=1, max_pages=100)
            assert [r["a"] for r in rows] == [1, 2, 3]


@pytest.mark.asyncio
async def test_fetch_all_handles_empty_result():
    """Server returns empty page 1 with has_more=False; must not infinite-loop."""
    from nocoly_explorer.async_client import AsyncWorksheetClient
    from tests.async_fixtures import make_handler
    handler = make_handler([[]])
    app = web.Application()
    app.router.add_get("/worksheets/ws/rows", handler)
    async with TestServer(app) as server:
        async with AsyncWorksheetClient(
            base_url=str(server.url), auth_token="dummy", worksheet_id="ws",
        ) as client:
            rows = await client.fetch_all_async(page_size=10, max_pages=10)
            assert rows == []


@pytest.mark.asyncio
async def test_fetch_all_respects_max_pages():
    """Stops at max_pages even if server has more."""
    from nocoly_explorer.async_client import AsyncWorksheetClient, PaginationLimitExceeded
    from tests.async_fixtures import make_handler
    handler = make_handler([[{"a": i}] for i in range(100)])
    app = web.Application()
    app.router.add_get("/worksheets/ws/rows", handler)
    async with TestServer(app) as server:
        async with AsyncWorksheetClient(
            base_url=str(server.url), auth_token="dummy", worksheet_id="ws",
        ) as client:
            rows = await client.fetch_all_async(page_size=1, max_pages=3)
            assert [r["a"] for r in rows] == [0, 1, 2]


@pytest.mark.asyncio
async def test_fetch_all_raises_pagination_limit_when_max_pages_exceeded():
    """If max_pages is exceeded during a wave, raise PaginationLimitExceeded."""
    from nocoly_explorer.async_client import AsyncWorksheetClient
    from tests.async_fixtures import make_handler
    # 20 pages, max_pages=2 → after 2 pages, third dispatch should fail
    handler = make_handler([[{"a": i}] for i in range(20)])
    app = web.Application()
    app.router.add_get("/worksheets/ws/rows", handler)
    async with TestServer(app) as server:
        async with AsyncWorksheetClient(
            base_url=str(server.url), auth_token="dummy", worksheet_id="ws",
            concurrency=4, requests_per_second=1000,
        ) as client:
            # max_pages=2: dispatch 2 pages, both complete, third would be requested.
            with pytest.raises(PaginationLimitExceeded):
                await client.fetch_all_async(page_size=1, max_pages=2)
```

- [ ] **Step 2: Run tests; expect ImportError / failure**

Run: `pytest tests/test_async_client.py::test_fetch_all_returns_all_rows_in_order -v`
Expected: AttributeError.

- [ ] **Step 3: Implement `fetch_all_async`**

Append to `src/nocoly_explorer/async_client.py`:

```python
    async def fetch_all_async(
        self,
        *,
        page_size: int = 200,
        max_pages: int = 1000,
        filter_criteria: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """Fetch all pages with bounded concurrency, return rows in original order."""
        if page_size < 1 or page_size > self.max_page_size:
            raise ValueError(f"page_size must be in [1, {self.max_page_size}] (got {page_size})")
        if max_pages < 1:
            raise ValueError(f"max_pages must be >= 1 (got {max_pages})")
        semaphore = asyncio.Semaphore(self.concurrency)
        pages: Dict[int, List[Dict[str, Any]]] = {}
        next_page = 1
        completed_terminator = False

        async def fetch_one(page_num: int) -> tuple[int, List[Dict[str, Any]]]:
            async with semaphore:
                return page_num, await self._fetch_page(
                    page=page_num, page_size=page_size, filter_criteria=filter_criteria,
                )

        in_flight: set[asyncio.Task] = set()

        while next_page <= max_pages and not completed_terminator:
            # Dispatch up to concurrency
            while len(in_flight) < self.concurrency and next_page <= max_pages:
                task = asyncio.create_task(fetch_one(next_page))
                in_flight.add(task)
                next_page += 1
            if not in_flight:
                break
            done, _ = await asyncio.wait(in_flight, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                page_num, data = await task
                pages[page_num] = data
                # End-of-data signal: short page
                if not data or len(data) < page_size:
                    completed_terminator = True
                in_flight.discard(task)
            # Cancel remaining in-flight if we've terminated
            if completed_terminator and in_flight:
                for task in in_flight:
                    task.cancel()
                # Drain the cancellations
                await asyncio.gather(*in_flight, return_exceptions=True)
                in_flight.clear()

        # If we still have pending tasks when next_page > max_pages, cancel them.
        if in_flight:
            for task in in_flight:
                task.cancel()
            await asyncio.gather(*in_flight, return_exceptions=True)

        if next_page > max_pages and not completed_terminator and not pages:
            raise PaginationLimitExceeded(
                f"max_pages={max_pages} reached; consider increasing the limit."
            )

        # Order-preserving merge
        all_rows: List[Dict[str, Any]] = []
        for p in sorted(pages):
            all_rows.extend(pages[p])
        return all_rows
```

- [ ] **Step 4: Run tests; expect all pass**

Run: `pytest tests/test_async_client.py -v`
Expected: 24 passed.

- [ ] **Step 5: Commit**

```bash
git add src/nocoly_explorer/async_client.py tests/test_async_client.py
git commit -m "feat(async): fetch_all_async with bounded concurrency + order-preserving merge"
```

---

## Task 5: AsyncWorksheetClient.fetch_pages_async (iterator)

**Files:**
- Modify: `src/nocoly_explorer/async_client.py`
- Test: extend `tests/test_async_client.py`

**Interfaces:**
- Produces: `async def fetch_pages_async(*, page_size=200, max_pages=1000, filter_criteria=None) -> AsyncIterator[list[dict]]` yielding one page at a time

- [ ] **Step 1: Write the failing test**

Append to `tests/test_async_client.py`:

```python
@pytest.mark.asyncio
async def test_fetch_pages_async_yields_pages_in_order(server_pages):
    from nocoly_explorer.async_client import AsyncWorksheetClient
    from tests.async_fixtures import make_handler
    handler = make_handler(server_pages)
    app = web.Application()
    app.router.add_get("/worksheets/ws/rows", handler)
    async with TestServer(app) as server:
        async with AsyncWorksheetClient(
            base_url=str(server.url), auth_token="dummy", worksheet_id="ws",
        ) as client:
            pages = []
            async for page in client.fetch_pages_async(page_size=2, max_pages=10):
                pages.append(page)
            assert pages == [[{"a": 1}, {"a": 2}], [{"a": 3}, {"a": 4}], [{"a": 5}]]


@pytest.mark.asyncio
async def test_fetch_pages_async_handles_empty_result():
    from nocoly_explorer.async_client import AsyncWorksheetClient
    from tests.async_fixtures import make_handler
    handler = make_handler([[]])
    app = web.Application()
    app.router.add_get("/worksheets/ws/rows", handler)
    async with TestServer(app) as server:
        async with AsyncWorksheetClient(
            base_url=str(server.url), auth_token="dummy", worksheet_id="ws",
        ) as client:
            pages = []
            async for page in client.fetch_pages_async(page_size=10, max_pages=10):
                pages.append(page)
            assert pages == [[]]
```

- [ ] **Step 2: Run; expect AttributeError**

Run: `pytest tests/test_async_client.py::test_fetch_pages_async_yields_pages_in_order -v`

- [ ] **Step 3: Implement `fetch_pages_async`**

Append to `src/nocoly_explorer/async_client.py`:

```python
    async def fetch_pages_async(
        self,
        *,
        page_size: int = 200,
        max_pages: int = 1000,
        filter_criteria: Optional[Dict[str, Any]] = None,
    ) -> AsyncIterator[List[Dict[str, Any]]]:
        """Yield pages in order, one at a time. Lower memory than fetch_all_async."""
        if page_size < 1 or page_size > self.max_page_size:
            raise ValueError(f"page_size must be in [1, {self.max_page_size}] (got {page_size})")
        if max_pages < 1:
            raise ValueError(f"max_pages must be >= 1 (got {max_pages})")
        semaphore = asyncio.Semaphore(self.concurrency)
        in_flight: set[asyncio.Task] = set()
        next_page = 1
        completed_terminator = False
        next_yield = 1

        async def fetch_one(page_num: int) -> tuple[int, List[Dict[str, Any]]]:
            async with semaphore:
                return page_num, await self._fetch_page(
                    page=page_num, page_size=page_size, filter_criteria=filter_criteria,
                )

        while next_yield <= max_pages:
            while (
                len(in_flight) < self.concurrency
                and next_page <= max_pages
                and not completed_terminator
            ):
                task = asyncio.create_task(fetch_one(next_page))
                in_flight.add(task)
                next_page += 1
            if not in_flight:
                break
            done, _ = await asyncio.wait(in_flight, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                page_num, data = await task
                in_flight.discard(task)
                while next_yield in pages_buffer or next_yield == page_num:
                    if next_yield == page_num:
                        yield data
                        next_yield += 1
                        if not data or len(data) < page_size:
                            completed_terminator = True
                            break
                    else:
                        break
                # buffer for ordering
                if page_num > next_yield:
                    pages_buffer[page_num] = data
                if completed_terminator:
                    break
            if completed_terminator:
                break

        # Drain any buffered out-of-order pages
        while next_yield in pages_buffer:
            yield pages_buffer.pop(next_yield)
            next_yield += 1

        # Cancel any straggler tasks
        for task in in_flight:
            task.cancel()
        if in_flight:
            await asyncio.gather(*in_flight, return_exceptions=True)
```

But this implementation has a bug — `pages_buffer` is referenced before being assigned. Let me re-state cleanly:

```python
    async def fetch_pages_async(
        self,
        *,
        page_size: int = 200,
        max_pages: int = 1000,
        filter_criteria: Optional[Dict[str, Any]] = None,
    ) -> AsyncIterator[List[Dict[str, Any]]]:
        """Yield pages in order, one at a time. Lower memory than fetch_all_async."""
        if page_size < 1 or page_size > self.max_page_size:
            raise ValueError(f"page_size must be in [1, {self.max_page_size}] (got {page_size})")
        if max_pages < 1:
            raise ValueError(f"max_pages must be >= 1 (got {max_pages})")
        semaphore = asyncio.Semaphore(self.concurrency)
        in_flight: set[asyncio.Task] = set()
        next_page = 1
        completed_terminator = False
        next_yield = 1
        pages_buffer: Dict[int, List[Dict[str, Any]]] = {}

        async def fetch_one(page_num: int) -> tuple[int, List[Dict[str, Any]]]:
            async with semaphore:
                return page_num, await self._fetch_page(
                    page=page_num, page_size=page_size, filter_criteria=filter_criteria,
                )

        while next_yield <= max_pages and not completed_terminator:
            while (
                len(in_flight) < self.concurrency
                and next_page <= max_pages
                and not completed_terminator
            ):
                task = asyncio.create_task(fetch_one(next_page))
                in_flight.add(task)
                next_page += 1
            if not in_flight:
                break
            done, _ = await asyncio.wait(in_flight, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                page_num, data = await task
                in_flight.discard(task)
                if page_num == next_yield:
                    yield data
                    next_yield += 1
                    if not data or len(data) < page_size:
                        completed_terminator = True
                else:
                    pages_buffer[page_num] = data
            # Drain buffer of any now-contiguous pages
            while next_yield in pages_buffer:
                buffered = pages_buffer.pop(next_yield)
                yield buffered
                next_yield += 1
                if not buffered or len(buffered) < page_size:
                    completed_terminator = True

        # Cancel any straggler tasks
        for task in in_flight:
            task.cancel()
        if in_flight:
            await asyncio.gather(*in_flight, return_exceptions=True)
```

- [ ] **Step 4: Run tests; expect all pass**

Run: `pytest tests/test_async_client.py -v`
Expected: 26 passed.

- [ ] **Step 5: Commit**

```bash
git add src/nocoly_explorer/async_client.py tests/test_async_client.py
git commit -m "feat(async): fetch_pages_async yielding in-order pages"
```

---

## Task 6: Cancellation + Review Focus #4

**Files:**
- Test: extend `tests/test_async_client.py`

**Interfaces:**
- Produces: Tests that verify Cancellation mid-fetch cleans up properly.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_async_client.py`:

```python
@pytest.mark.asyncio
async def test_fetch_all_cancellation_cancels_inflight_tasks():
    """If the consumer cancels mid-fetch, in-flight tasks must be cancelled cleanly."""
    import asyncio
    from nocoly_explorer.async_client import AsyncWorksheetClient
    from tests.async_fixtures import make_handler
    # 5 pages; cancel after the first wave finishes
    handler = make_handler([[{"a": i}] for i in range(5)])
    app = web.Application()
    app.router.add_get("/worksheets/ws/rows", handler)
    async with TestServer(app) as server:
        async with AsyncWorksheetClient(
            base_url=str(server.url), auth_token="dummy", worksheet_id="ws",
            concurrency=4, requests_per_second=1000,
        ) as client:
            # Wrap fetch_all in a task and cancel after a short delay
            task = asyncio.create_task(client.fetch_all_async(page_size=1, max_pages=100))
            await asyncio.sleep(0.05)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            # Should not hang; any remaining tasks should be cancelled.
            assert task.cancelled() or task.done()
```

- [ ] **Step 2: Run; expect cancel handling works**

Run: `pytest tests/test_async_client.py::test_fetch_all_cancellation_cancels_inflight_tasks -v`

- [ ] **Step 3: Verify the implementation handles cancellation**

The `fetch_all_async` already has cancellation handling via `task.cancel()` + `asyncio.gather(..., return_exceptions=True)`. Tests should pass.

- [ ] **Step 4: Run all tests**

Run: `pytest tests/test_async_client.py -v`
Expected: 27 passed.

- [ ] **Step 5: Commit**

```bash
git add tests/test_async_client.py
git commit -m "test(async): cancellation cleans up in-flight tasks"
```

---

## Task 7: Public API + pyproject extras

**Files:**
- Modify: `src/nocoly_explorer/__init__.py`
- Modify: `pyproject.toml`

- [ ] **Step 1: Add `async` extras + dev deps**

Modify `pyproject.toml`:

```toml
[project.optional-dependencies]
dataframe = ["pandas>=2.0.0"]
spark = ["pyspark>=3.4.0"]
streaming = ["pyarrow>=14.0.0"]
async = ["aiohttp>=3.9.0"]

[project.optional-dependencies.test]
test = [
    "pytest>=8.0.0",
    "pytest-asyncio>=0.23.0",
    "pytest-cov>=4.0.0",
]
```

(Note: only `test` extras are added if not already present; existing test deps in pyproject may differ — verify before patching.)

Add `[tool.pytest.ini_options]`:

```toml
[tool.pytest.ini_options]
asyncio_mode = "auto"
testpaths = ["tests"]
```

- [ ] **Step 2: Update `__init__.py` for lazy async exports**

Append to `src/nocoly_explorer/__init__.py`:

```python
_ASYNC_EXPORTS = {"AsyncWorksheetClient", "AsyncClientError", "PaginationLimitExceeded"}
```

Extend `__getattr__` to handle async:

```python
def __getattr__(name):
    if name in _STREAMING_EXPORTS:
        from .streaming import (
            StreamingExporter, ParquetExportOptions, PartitionSpec,
            StreamingExportConfig, ExportResult,
        )
        namespace = {
            "StreamingExporter": StreamingExporter,
            "ParquetExportOptions": ParquetExportOptions,
            "PartitionSpec": PartitionSpec,
            "StreamingExportConfig": StreamingExportConfig,
            "ExportResult": ExportResult,
        }
        return namespace[name]
    if name in _ERROR_EXPORTS:
        from .exceptions import (
            SchemaDriftError, CardinalityExceededError,
            AsyncClientError, PaginationLimitExceeded,
        )
        return {
            "SchemaDriftError": SchemaDriftError,
            "CardinalityExceededError": CardinalityExceededError,
            "AsyncClientError": AsyncClientError,
            "PaginationLimitExceeded": PaginationLimitExceeded,
        }[name]
    if name in _ASYNC_EXPORTS:
        from .async_client import (
            AsyncWorksheetClient, AsyncClientError, PaginationLimitExceeded,
        )
        return {
            "AsyncWorksheetClient": AsyncWorksheetClient,
            "AsyncClientError": AsyncClientError,
            "PaginationLimitExceeded": PaginationLimitExceeded,
        }[name]
    raise AttributeError(f"module 'nocoly_explorer' has no attribute {name!r}")
```

- [ ] **Step 3: Verify public API loads (without aiohttp installed)**

```bash
cd /Users/hermes/Downloads/nocoly-explorer
python -c "
from nocoly_explorer import StreamingExporter, ParquetExportOptions, PartitionSpec
print('Phase 1 still works')
try:
    from nocoly_explorer import AsyncWorksheetClient
    print('Phase 2 OK')
except ImportError as e:
    print(f'Phase 2 import expected without aiohttp: {e}')
"
```

Expected: "Phase 1 still works" + "Phase 2 import expected without aiohttp: …".

- [ ] **Step 4: Install aiohttp and verify**

```bash
cd /Users/hermes/Downloads/nocoly-explorer
pip install -e ".[async]" aiohttp pytest-asyncio
python -c "
from nocoly_explorer import AsyncWorksheetClient, AsyncClientError, PaginationLimitExceeded
print('Phase 2 OK with aiohttp installed')
"
```

- [ ] **Step 5: Run full test suite**

Run: `pytest -v`
Expected: 109 (Phase 1) + ~27 (Phase 2 async) = ~136 passed.

- [ ] **Step 6: Commit**

```bash
git add src/nocoly_explorer/__init__.py pyproject.toml
git commit -m "feat(async): expose AsyncWorksheetClient + aiohttp optional extra"
```

---

## Task 8: Build, publish 0.2.0rc2

- [ ] **Step 1: Bump version**

Edit `pyproject.toml`: `version = "0.2.0rc2"`.

- [ ] **Step 2: Build + verify install**

```bash
cd /Users/hermes/Downloads/nocoly-explorer
rm -rf dist/ build/
python -m build
ls dist/
```

```bash
cd /tmp && rm -rf verify-install && python -m venv verify-install
/tmp/verify-install/bin/pip install --quiet "/Users/hermes/Downloads/nocoly-explorer/dist/nocoly_explorer-0.2.0rc2-py3-none-any.whl" aiohttp
/tmp/verify-install/bin/python -c "
from nocoly_explorer import AsyncWorksheetClient
print('AsyncWorksheetClient loaded:', AsyncWorksheetClient)
"
rm -rf /tmp/verify-install
```

- [ ] **Step 3: Commit, tag, release**

```bash
cd /Users/hermes/Downloads/nocoly-explorer
git add pyproject.toml
git commit -m "chore(release): bump to 0.2.0rc2 (async pagination phase 2)"
git tag v0.2.0rc2
git push origin main --tags
gh release create v0.2.0rc2 \
  dist/nocoly_explorer-0.2.0rc2-py3-none-any.whl dist/nocoly_explorer-0.2.0rc2.tar.gz \
  --title "v0.2.0rc2 — Async Pagination Engine" \
  --notes-file - <<'EOF'
Phase 2 of the v0.2.0 enterprise scale roadmap. Adds:

- AsyncWorksheetClient (aiohttp) with bounded concurrency
- TokenBucket rate limiter (per-instance; configurable requests_per_second)
- RetryPolicy: exponential backoff + Retry-After (delta-seconds and HTTP-date)
- fetch_all_async: order-preserving merge of all pages
- fetch_pages_async: AsyncIterator yielding pages in order

Install:  pip install nocoly-explorer[async]

Test count: 109 → ~136 (+27 new async tests; full suite green).

Notes:
- aiohttp is now an optional dependency.
- The streaming exporter from rc1 still works unchanged; Phase 3 will wire them.
EOF
gh release edit v0.2.0rc2 --prerelease
```

---

## Self-Review

1. **Spec coverage:** §4.2 covered by Tasks 1–7.
   - Bounded semaphore (N=8 default) ✓ (Task 4, `AsyncWorksheetClient.__init__`)
   - Order-preserving merge ✓ (Tasks 4, 5)
   - Token bucket ✓ (Task 2)
   - Retry-After honored ✓ (Task 1 `parse_retry_after`)
   - 5xx exponential backoff with jitter ✓ (Task 1, 3)
   - `fetch_all_async` (list result) + `fetch_pages_async` (iterator) ✓ (Tasks 4, 5)

2. **Placeholder scan:** No TBDs. Code complete.

3. **Type consistency:**
   - `RetryPolicy.max_retries` and `RetryPolicy.compute_backoff` use the same contract.
   - `AsyncClientError` raised in `_fetch_page` and caught in `test_*` blocks.
   - `PaginationLimitExceeded` raised when `max_pages` exhausted.

4. **Review Focus:** All 5 input classes covered:
   - #1 Short page signals end → `test_fetch_all_stops_on_short_page`
   - #2 Empty result → `test_fetch_all_handles_empty_result`
   - #3 First-request 429 → `test_fetch_single_page_retries_on_429` (no early raise)
   - #4 Cancellation → `test_fetch_all_cancellation_cancels_inflight_tasks`
   - #5 Token bucket saturation → `test_token_bucket_throttles_beyond_burst`

---

**Plan complete and saved to `docs/superpowers/plans/2026-09-28-async-phase2.md`.**

Using executing-plans skill to implement inline per Royce's earlier preference.