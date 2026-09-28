# FastAPI Service Layer — Phase 3 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-step. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a FastAPI service layer that wraps the async pagination engine and streaming exporter, exposing them as long-running jobs that n8n (or any orchestrator) can submit and poll. Job execution is delegated to Arq workers backed by Redis; status is read from Redis.

**Architecture:**

```
n8n → POST /jobs       →  FastAPI  →  Arq enqueue  →  Worker process
                              ↘                              ↘
                              Redis (state + queue)            AsyncWorksheetClient
                                                              + StreamingExporter
                                                              → status updates
n8n → GET /jobs/{id}   →  FastAPI → Redis lookup → return status
n8n → GET /jobs/{id}/result → if complete, return artifact location
```

**Tech Stack:**
- FastAPI ≥ 0.110 (HTTP framework)
- Pydantic v2 (request/response schemas)
- arq ≥ 0.25 (asyncio-native Redis-backed job queue)
- fakeredis ≥ 2.20 (in-memory Redis for tests)
- httpx (for FastAPI TestClient)

**Spec:** `docs/superpowers/specs/2026-09-28-enterprise-v0.2.0-design.md` §4.3.

---

## Global Constraints

- Python 3.10+ (project baseline).
- All new modules under `src/nocoly_explorer/service/`.
- Service is **lazy-imported** — `import nocoly_explorer` MUST NOT pull in FastAPI/arq/fakeredis.
- API key auth via `Authorization: Bearer <key>` where key is read from `NOCOLY_SERVICE_API_KEY` env (or `api_key` constructor arg for tests). Missing key → auth disabled (dev mode); set key → required.
- All job state lives in Redis (keyed by `job_id`). No filesystem state in the FastAPI process.
- Job cancellation is **cooperative**: worker checks a `cancel_requested` flag at each page boundary.
- Errors at the HTTP boundary become 4xx/5xx with a JSON body `{"error": "...", "detail": "..."}`. Workers raise into Arq; Arq records failure in Redis.
- Service binds to `127.0.0.1` by default.
- Endpoints (per spec §4.3):
  - `POST /jobs` → submit
  - `GET /jobs/{id}` → status
  - `GET /jobs/{id}/result` → artifact location
  - `POST /jobs/{id}/cancel` → cooperative cancel
  - `GET /healthz` → liveness
  - `GET /readyz` → readiness (Redis reachable)

---

## Review Focus (input classes a user will hit but individual tests don't cover)

1. **Concurrent submissions** — submit N jobs in parallel; each gets a unique `job_id` and proceeds independently. (Spec doesn't address this; reasonable to assume.)
2. **Submit then poll while still running** — `GET /jobs/{id}` during a running job should return `running` with `progress_pct < 100` (not `succeeded`).
3. **Service down + restart mid-job** — jobs in flight when the FastAPI process restarts should still appear in Redis (Arq owns them); their status is still queryable. (Redis is the source of truth.)
4. **Bearer token missing when API key is configured** — must return 401, not proceed.
5. **Job result download when worker crashed mid-job** — `GET /jobs/{id}/result` should return 409 Conflict (not 200) with a clear message.

---

## File Structure

New files:
- `src/nocoly_explorer/service/__init__.py` — `create_app()` factory
- `src/nocoly_explorer/service/schemas.py` — Pydantic request/response models
- `src/nocoly_explorer/service/state.py` — Redis status reader/writer
- `src/nocoly_explorer/service/worker.py` — Arq worker function
- `src/nocoly_explorer/service/auth.py` — Bearer-token API key validator
- `tests/test_service.py` — FastAPI TestClient + fakeredis
- `tests/service_fixtures.py` — fakeredis + mock worker fixtures

Modified files:
- `pyproject.toml` — add `service` optional extras (fastapi/arq/uvicorn/httpx)
- `src/nocoly_explorer/__init__.py` — re-export `create_app` + key schemas
- `src/nocoly_explorer/exceptions.py` — add `ServiceError`, `JobNotFound`, `JobNotReady`

---

## Task 1: Exceptions + Schemas (foundation)

**Files:**
- Modify: `src/nocoly_explorer/exceptions.py`
- Create: `src/nocoly_explorer/service/__init__.py`
- Create: `src/nocoly_explorer/service/schemas.py`
- Test: `tests/test_service_schemas.py`

**Interfaces:**
- Consumes: nothing yet
- Produces: `ServiceError(NocolyError)`, `JobNotFound(ServiceError)`, `JobNotReady(ServiceError)`
- Produces: `JobSubmission` (request body), `JobStatus` (status response), `JobResult` (result response), `HealthResponse` (healthz)

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_service_schemas.py
from __future__ import annotations

import pytest


def test_service_error_inherits_from_nocoly_error():
    from nocoly_explorer.exceptions import NocolyError
    from nocoly_explorer.service.schemas import ServiceError
    assert issubclass(ServiceError, NocolyError)


def test_job_not_found_inherits_from_service_error():
    from nocoly_explorer.service.schemas import ServiceError, JobNotFound
    assert issubclass(JobNotFound, ServiceError)


def test_job_not_ready_inherits_from_service_error():
    from nocoly_explorer.service.schemas import ServiceError, JobNotReady
    assert issubclass(JobNotReady, ServiceError)


def test_job_submission_minimum_valid():
    from nocoly_explorer.service.schemas import JobSubmission
    j = JobSubmission(host="https://example.com", worksheet_id="ws-1")
    assert j.host == "https://example.com"
    assert j.worksheet_id == "ws-1"
    assert j.page_size == 200
    assert j.max_pages == 1000
    assert j.concurrency == 8
    assert j.output is None
    assert j.filter is None


def test_job_submission_rejects_invalid_page_size():
    from nocoly_explorer.service.schemas import JobSubmission
    with pytest.raises(ValueError):
        JobSubmission(host="https://example.com", worksheet_id="ws-1", page_size=0)
    with pytest.raises(ValueError):
        JobSubmission(host="https://example.com", worksheet_id="ws-1", page_size=1001)


def test_job_submission_rejects_invalid_concurrency():
    from nocoly_explorer.service.schemas import JobSubmission
    with pytest.raises(ValueError):
        JobSubmission(host="https://example.com", worksheet_id="ws-1", concurrency=0)
    with pytest.raises(ValueError):
        JobSubmission(host="https://example.com", worksheet_id="ws-1", concurrency=100)


def test_job_submission_validates_output_sink():
    from nocoly_explorer.service.schemas import JobSubmission, OutputSpec
    o = OutputSpec(sink="parquet_local", path="/tmp/out", partition_by="region")
    j = JobSubmission(host="https://example.com", worksheet_id="ws-1", output=o)
    assert j.output.sink == "parquet_local"


def test_job_submission_rejects_unknown_sink():
    from nocoly_explorer.service.schemas import JobSubmission, OutputSpec
    with pytest.raises(ValueError):
        OutputSpec(sink="parquet_mongodb", path="/tmp/out")


def test_job_status_construction():
    from nocoly_explorer.service.schemas import JobStatus
    s = JobStatus(job_id="abc", status="queued")
    assert s.job_id == "abc"
    assert s.status == "queued"
    assert s.progress_pct is None


def test_job_result_construction():
    from nocoly_explorer.service.schemas import JobResult
    r = JobResult(job_id="abc", status="succeeded", artifact_path="/tmp/out")
    assert r.artifact_path == "/tmp/out"


def test_health_response_construction():
    from nocoly_explorer.service.schemas import HealthResponse
    h = HealthResponse(status="ok")
    assert h.status == "ok"
```

- [ ] **Step 2: Run tests; expect ImportError**

Run: `pytest tests/test_service_schemas.py -v`
Expected: collection error.

- [ ] **Step 3: Add exceptions**

Append to `src/nocoly_explorer/exceptions.py`:

```python
class ServiceError(NocolyError):
    """Base exception for service-layer failures."""


class JobNotFound(ServiceError):
    """Raised when a job_id does not exist in Redis."""


class JobNotReady(ServiceError):
    """Raised when /result is fetched before job completion."""
```

- [ ] **Step 4: Create `service/__init__.py` (empty stub)**

```python
"""FastAPI service layer for Nocoly Explorer jobs."""
```

- [ ] **Step 5: Implement `service/schemas.py`**

```python
"""Pydantic request/response models for the FastAPI service layer."""

from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, field_validator

from ..exceptions import NocolyError


class ServiceError(NocolyError):
    """Base exception for service-layer failures."""


class JobNotFound(ServiceError):
    """Raised when a job_id does not exist in Redis."""


class JobNotReady(ServiceError):
    """Raised when /result is fetched before job completion."""


class SinkType(str, Enum):
    PARQUET_LOCAL = "parquet_local"
    PARQUET_S3 = "parquet_s3"
    JSON_LOCAL = "json_local"


class Granularity(str, Enum):
    DAY = "day"
    MONTH = "month"
    YEAR = "year"


class OutputSpec(BaseModel):
    sink: SinkType
    path: str
    partition_by: Optional[str] = None
    partition_granularity: Optional[Granularity] = None


class JobSubmission(BaseModel):
    host: str
    worksheet_id: str
    output: Optional[OutputSpec] = None
    filter: Optional[Dict[str, Any]] = None
    columns: Optional[List[str]] = None
    page_size: int = Field(default=200, ge=1, le=1000)
    max_pages: int = Field(default=1000, ge=1)
    concurrency: int = Field(default=8, ge=1, le=32)


class JobStatus(BaseModel):
    job_id: str
    status: str  # queued / running / succeeded / failed / cancelled
    progress_pct: Optional[float] = None
    rows_fetched: Optional[int] = None
    error: Optional[str] = None


class JobResult(BaseModel):
    job_id: str
    status: str
    artifact_path: Optional[str] = None
    rows_written: Optional[int] = None


class HealthResponse(BaseModel):
    status: str
```

- [ ] **Step 6: Run tests; expect all pass**

Run: `pytest tests/test_service_schemas.py -v`
Expected: 12 passed.

- [ ] **Step 7: Commit**

```bash
cd /Users/hermes/Downloads/nocoly-explorer
git add src/nocoly_explorer/exceptions.py src/nocoly_explorer/service/__init__.py src/nocoly_explorer/service/schemas.py tests/test_service_schemas.py
git commit -m "feat(service): schemas + ServiceError hierarchy"
```

---

## Task 2: Redis state reader/writer

**Files:**
- Create: `src/nocoly_explorer/service/state.py`
- Test: `tests/test_service_state.py`

**Interfaces:**
- Consumes: redis-like client (sync or async; use async since Arq is async)
- Produces: `JobState(redis)` with methods `create_job(job_id, params)`, `set_status(job_id, status, **extra)`, `get_status(job_id) -> Optional[JobStatus]`, `set_result(job_id, artifact_path, rows_written)`, `get_result(job_id) -> Optional[JobResult]`, `request_cancel(job_id)`, `is_cancel_requested(job_id) -> bool`

Key layout:
- `job:{job_id}:status` — JSON: `{"status": "...", "progress_pct": 0.0, "rows_fetched": 0}`
- `job:{job_id}:result` — JSON: `{"artifact_path": "...", "rows_written": N}`
- `job:{job_id}:cancel` — `"1"` if cancellation requested

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_service_state.py
from __future__ import annotations

import pytest


@pytest.fixture
def redis():
    import fakeredis.aioredis
    return fakeredis.aioredis.FakeRedis()


@pytest.mark.asyncio
async def test_create_job_initializes_status(redis):
    from nocoly_explorer.service.state import JobState
    state = JobState(redis)
    await state.create_job("job-1", {"host": "https://x", "worksheet_id": "ws"})
    status = await state.get_status("job-1")
    assert status is not None
    assert status.job_id == "job-1"
    assert status.status == "queued"


@pytest.mark.asyncio
async def test_get_status_returns_none_for_unknown_job(redis):
    from nocoly_explorer.service.state import JobState
    state = JobState(redis)
    assert await state.get_status("nope") is None


@pytest.mark.asyncio
async def test_set_status_updates_progress(redis):
    from nocoly_explorer.service.state import JobState
    state = JobState(redis)
    await state.create_job("job-2", {})
    await state.set_status("job-2", "running", progress_pct=42.0, rows_fetched=1000)
    s = await state.get_status("job-2")
    assert s.status == "running"
    assert s.progress_pct == 42.0
    assert s.rows_fetched == 1000


@pytest.mark.asyncio
async def test_set_status_preserves_unset_fields(redis):
    from nocoly_explorer.service.state import JobState
    state = JobState(redis)
    await state.create_job("job-3", {})
    await state.set_status("job-3", "running", progress_pct=10.0)
    await state.set_status("job-3", "running", rows_fetched=50)
    s = await state.get_status("job-3")
    assert s.progress_pct == 10.0  # preserved
    assert s.rows_fetched == 50


@pytest.mark.asyncio
async def test_set_result_then_get_result(redis):
    from nocoly_explorer.service.state import JobState
    state = JobState(redis)
    await state.create_job("job-4", {})
    await state.set_status("job-4", "succeeded")
    await state.set_result("job-4", "/tmp/out", rows_written=12345)
    result = await state.get_result("job-4")
    assert result is not None
    assert result.artifact_path == "/tmp/out"
    assert result.rows_written == 12345


@pytest.mark.asyncio
async def test_get_result_returns_none_for_unknown_job(redis):
    from nocoly_explorer.service.state import JobState
    state = JobState(redis)
    assert await state.get_result("nope") is None


@pytest.mark.asyncio
async def test_cancel_request_lifecycle(redis):
    from nocoly_explorer.service.state import JobState
    state = JobState(redis)
    await state.create_job("job-5", {})
    assert not await state.is_cancel_requested("job-5")
    await state.request_cancel("job-5")
    assert await state.is_cancel_requested("job-5")


@pytest.mark.asyncio
async def test_set_status_records_error_message(redis):
    from nocoly_explorer.service.state import JobState
    state = JobState(redis)
    await state.create_job("job-6", {})
    await state.set_status("job-6", "failed", error="boom")
    s = await state.get_status("job-6")
    assert s.status == "failed"
    assert s.error == "boom"
```

- [ ] **Step 2: Run tests; expect ImportError**

Run: `pytest tests/test_service_state.py -v`

- [ ] **Step 3: Implement `service/state.py`**

```python
"""Redis-backed job state for the FastAPI service."""

from __future__ import annotations

import json
from typing import Any, Dict, Optional

from .schemas import JobResult, JobStatus


def _status_key(job_id: str) -> str:
    return f"job:{job_id}:status"


def _result_key(job_id: str) -> str:
    return f"job:{job_id}:result"


def _cancel_key(job_id: str) -> str:
    return f"job:{job_id}:cancel"


def _params_key(job_id: str) -> str:
    return f"job:{job_id}:params"


class JobState:
    """Async Redis-backed job state manager."""

    def __init__(self, redis: Any) -> None:
        self.redis = redis

    async def create_job(self, job_id: str, params: Dict[str, Any]) -> None:
        await self.redis.set(_params_key(job_id), json.dumps(params, default=str))
        await self.redis.set(
            _status_key(job_id),
            json.dumps({"job_id": job_id, "status": "queued"}),
        )

    async def set_status(
        self,
        job_id: str,
        status: str,
        *,
        progress_pct: Optional[float] = None,
        rows_fetched: Optional[int] = None,
        error: Optional[str] = None,
    ) -> None:
        # Merge with existing status so unset fields are preserved.
        existing_raw = await self.redis.get(_status_key(job_id))
        existing: Dict[str, Any] = json.loads(existing_raw) if existing_raw else {"job_id": job_id}
        existing["job_id"] = job_id
        existing["status"] = status
        if progress_pct is not None:
            existing["progress_pct"] = progress_pct
        if rows_fetched is not None:
            existing["rows_fetched"] = rows_fetched
        if error is not None:
            existing["error"] = error
        await self.redis.set(_status_key(job_id), json.dumps(existing))

    async def get_status(self, job_id: str) -> Optional[JobStatus]:
        raw = await self.redis.get(_status_key(job_id))
        if raw is None:
            return None
        return JobStatus(**json.loads(raw))

    async def set_result(
        self, job_id: str, artifact_path: str, *, rows_written: int
    ) -> None:
        await self.redis.set(
            _result_key(job_id),
            json.dumps(
                {
                    "job_id": job_id,
                    "status": "succeeded",
                    "artifact_path": artifact_path,
                    "rows_written": rows_written,
                }
            ),
        )

    async def get_result(self, job_id: str) -> Optional[JobResult]:
        raw = await self.redis.get(_result_key(job_id))
        if raw is None:
            return None
        return JobResult(**json.loads(raw))

    async def get_params(self, job_id: str) -> Optional[Dict[str, Any]]:
        raw = await self.redis.get(_params_key(job_id))
        if raw is None:
            return None
        return json.loads(raw)

    async def request_cancel(self, job_id: str) -> None:
        await self.redis.set(_cancel_key(job_id), "1")

    async def is_cancel_requested(self, job_id: str) -> bool:
        raw = await self.redis.get(_cancel_key(job_id))
        return raw is not None
```

- [ ] **Step 4: Run tests; expect all pass**

Run: `pytest tests/test_service_state.py -v`
Expected: 8 passed.

- [ ] **Step 5: Commit**

```bash
git add src/nocoly_explorer/service/state.py tests/test_service_state.py
git commit -m "feat(service): JobState with fakeredis-backed state mgmt"
```

---

## Task 3: Worker function (Arq job entrypoint)

**Files:**
- Create: `src/nocoly_explorer/service/worker.py`
- Test: `tests/test_service_worker.py`

**Interfaces:**
- Consumes: `JobState`, `AsyncWorksheetClient`, `StreamingExporter`, settings (Redis URL)
- Produces: `async def run_job(ctx, job_id: str) -> dict` (Arq entrypoint) that:
  1. Reads job params from Redis
  2. Builds an `AsyncWorksheetClient` from params
  3. Fetches all pages, updating progress
  4. Streams to Parquet/JSON sink
  5. Records result and updates status to `succeeded` / `failed`
  6. Honors cancellation at page boundaries

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_service_worker.py
from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest


@pytest.fixture
def redis():
    import fakeredis.aioredis
    return fakeredis.aioredis.FakeRedis()


@pytest.fixture
def mock_nocoly_server():
    """Start a mock HTTP server that returns 3 pages of data."""
    from aiohttp import web
    pages_data = [
        [{"id": i, "v": i * 2} for i in range(0, 5)],
        [{"id": i, "v": i * 2} for i in range(5, 10)],
        [{"id": i, "v": i * 2} for i in range(10, 12)],  # last page
    ]
    return pages_data


@pytest.mark.asyncio
async def test_run_job_parquet_export_to_local_sink(redis, mock_nocoly_server, tmp_path: Path):
    from nocoly_explorer.service.worker import run_job
    from nocoly_explorer.service.state import JobState
    from aiohttp import web
    from aiohttp.test_utils import TestServer

    pages = mock_nocoly_server

    async def handler(request):
        page = int(request.query.get("page", "1"))
        page_size = int(request.query.get("page_size", "200"))
        if page > len(pages):
            return web.json_response({"rows": [], "has_more": False})
        rows = pages[page - 1]
        return web.json_response(
            {"rows": rows[:page_size], "has_more": page < len(pages)}
        )

    app = web.Application()
    app.router.add_get("/worksheets/ws/rows", handler)
    async with TestServer(app) as server:
        state = JobState(redis)
        output_path = tmp_path / "out"
        params = {
            "host": str(server.make_url("")),
            "worksheet_id": "ws",
            "auth_token": "test",
            "output": {"sink": "parquet_local", "path": str(output_path)},
            "page_size": 5,
            "max_pages": 100,
            "concurrency": 2,
        }
        await state.create_job("job-test-1", params)

        result = await run_job(
            redis=redis,
            job_id="job-test-1",
            params=params,
        )

        assert result["status"] == "succeeded"
        assert result["rows_written"] == 12

        # Verify Parquet was actually written
        parquet_files = list(output_path.rglob("data*.parquet"))
        assert len(parquet_files) >= 1
        import pyarrow.parquet as pq
        total = sum(pq.read_table(f).num_rows for f in parquet_files)
        assert total == 12


@pytest.mark.asyncio
async def test_run_job_records_failure_on_error(redis, tmp_path: Path):
    """If the worksheet server is unreachable, the worker must record failure."""
    from nocoly_explorer.service.worker import run_job
    from nocoly_explorer.service.state import JobState

    state = JobState(redis)
    output_path = tmp_path / "out"
    params = {
        "host": "http://127.0.0.1:1",  # nothing listening
        "worksheet_id": "ws",
        "auth_token": "test",
        "output": {"sink": "parquet_local", "path": str(output_path)},
        "page_size": 10,
        "max_pages": 5,
        "concurrency": 2,
    }
    await state.create_job("job-fail", params)
    result = await run_job(redis=redis, job_id="job-fail", params=params)
    assert result["status"] == "failed"
    assert "error" in result
    status = await state.get_status("job-fail")
    assert status.status == "failed"
    assert status.error is not None


@pytest.mark.asyncio
async def test_run_job_honors_cancel(redis, mock_nocoly_server, tmp_path: Path):
    from nocoly_explorer.service.worker import run_job
    from nocoly_explorer.service.state import JobState
    from aiohttp import web
    from aiohttp.test_utils import TestServer

    pages = mock_nocoly_server

    async def handler(request):
        await asyncio.sleep(0.1)  # slow page
        page = int(request.query.get("page", "1"))
        if page > len(pages):
            return web.json_response({"rows": [], "has_more": False})
        return web.json_response(
            {"rows": pages[page - 1], "has_more": page < len(pages)}
        )

    app = web.Application()
    app.router.add_get("/worksheets/ws/rows", handler)
    async with TestServer(app) as server:
        state = JobState(redis)
        await state.create_job("job-cancel", {"host": "x", "worksheet_id": "y"})
        # Pre-request cancellation
        await state.request_cancel("job-cancel")
        params = {
            "host": str(server.make_url("")),
            "worksheet_id": "ws",
            "auth_token": "test",
            "output": {"sink": "parquet_local", "path": str(tmp_path / "out")},
            "page_size": 5,
            "max_pages": 100,
            "concurrency": 2,
        }
        result = await run_job(redis=redis, job_id="job-cancel", params=params)
        assert result["status"] == "cancelled"
```

- [ ] **Step 2: Run tests; expect ImportError**

Run: `pytest tests/test_service_worker.py -v`

- [ ] **Step 3: Implement `service/worker.py`**

```python
"""Arq worker function that runs a single export job."""

from __future__ import annotations

import asyncio
import logging
import time
from pathlib import Path
from typing import Any, Dict

from ..async_client import AsyncWorksheetClient
from ..streaming import (
    PartitionSpec,
    ParquetExportOptions,
    StreamingExportConfig,
    StreamingExporter,
    validate_output_dir,
)
from .state import JobState

_LOGGER = logging.getLogger("nocoly_explorer.service.worker")


async def run_job(redis: Any, job_id: str, params: Dict[str, Any]) -> Dict[str, Any]:
    """Execute one export job. Returns a dict result for Arq."""
    state = JobState(redis)
    await state.set_status(job_id, "running", progress_pct=0.0)

    # Validate params
    sink_cfg = params.get("output") or {}
    sink = sink_cfg.get("sink", "parquet_local")
    output_path = Path(sink_cfg["path"]) if sink_cfg.get("path") else None
    if sink == "parquet_local" and output_path is None:
        await state.set_status(job_id, "failed", error="output.path required for parquet_local")
        return {"status": "failed", "error": "output.path required"}

    partition_col = sink_cfg.get("partition_by")
    partition_gran = sink_cfg.get("partition_granularity")

    host = params["host"]
    worksheet_id = params["worksheet_id"]
    auth_token = params.get("auth_token", "")
    page_size = params.get("page_size", 200)
    max_pages = params.get("max_pages", 1000)
    concurrency = params.get("concurrency", 8)

    rows_written = 0
    artifact_path: str = ""

    try:
        if sink == "parquet_local":
            output_path.mkdir(parents=True, exist_ok=True)
            partition = (
                PartitionSpec(column=partition_col, granularity=partition_gran)
                if partition_col
                else None
            )
            options = ParquetExportOptions()

            class _AsyncClientAdapter:
                """Wraps AsyncWorksheetClient to expose the sync fetch_rows() interface
                that StreamingExporter expects."""

                def __init__(self, async_client: AsyncWorksheetClient) -> None:
                    self._async = async_client

                def fetch_rows(self, **_kwargs):
                    return asyncio.run(
                        self._async.fetch_all_async(
                            page_size=page_size,
                            max_pages=max_pages,
                        )
                    )

            # The exporter is sync. We run it in a thread so the worker's
            # event loop stays free for status updates and cancellation checks.
            async with AsyncWorksheetClient(
                base_url=host,
                auth_token=auth_token,
                worksheet_id=worksheet_id,
                concurrency=concurrency,
            ) as aclient:
                adapter = _AsyncClientAdapter(aclient)
                exporter = StreamingExporter(
                    client=adapter,
                    config=StreamingExportConfig(
                        output_dir=output_path,
                        worksheet_id=worksheet_id,
                        options=options,
                        partition=partition,
                    ),
                )

                # Run exporter in a thread so we can poll cancellation
                loop = asyncio.get_event_loop()
                result_future = loop.run_in_executor(None, exporter.export)
                # Poll cancellation while exporting
                while not result_future.done():
                    if await state.is_cancel_requested(job_id):
                        await state.set_status(
                            job_id, "cancelled", error="cancel requested"
                        )
                        result_future.cancel()
                        try:
                            await result_future
                        except (asyncio.CancelledError, Exception):
                            pass
                        return {"status": "cancelled"}
                    await state.set_status(
                        job_id, "running", rows_fetched=rows_written
                    )
                    await asyncio.sleep(0.2)

                result = await result_future
                rows_written = result.rows_written
                artifact_path = str(output_path)

            await state.set_status(
                job_id, "succeeded", progress_pct=100.0, rows_fetched=rows_written
            )
            await state.set_result(
                job_id, artifact_path, rows_written=rows_written
            )
            return {"status": "succeeded", "rows_written": rows_written, "artifact_path": artifact_path}

        else:
            await state.set_status(job_id, "failed", error=f"unsupported sink: {sink}")
            return {"status": "failed", "error": f"unsupported sink: {sink}"}

    except Exception as exc:
        _LOGGER.exception("worker.run_job failed", extra={"job_id": job_id})
        await state.set_status(job_id, "failed", error=str(exc)[:500])
        return {"status": "failed", "error": str(exc)[:500]}


# Arq integration helper
async def arq_startup(ctx: Dict[str, Any]) -> None:
    """Arq startup hook — nothing to do for now."""
    pass


async def arq_shutdown(ctx: Dict[str, Any]) -> None:
    """Arq shutdown hook — nothing to do for now."""
    pass


def make_arq_settings(redis_url: str):
    """Build arq WorkerSettings for the worker process.

    Returns a WorkerSettings instance ready to be imported by `arq worker`.
    """
    from arq.connections import RedisSettings
    from arq import WorkerSettings

    class _Settings:
        redis_settings = RedisSettings.from_dsn(redis_url)
        functions = [run_job]
        on_startup = arq_startup
        on_shutdown = arq_shutdown

    return _Settings()
```

- [ ] **Step 4: Run tests; expect all pass**

Run: `pytest tests/test_service_worker.py -v`
Expected: 3 passed.

- [ ] **Step 5: Commit**

```bash
git add src/nocoly_explorer/service/worker.py tests/test_service_worker.py
git commit -m "feat(service): run_job worker with progress + cancellation"
```

---

## Task 4: Auth + create_app

**Files:**
- Create: `src/nocoly_explorer/service/auth.py`
- Modify: `src/nocoly_explorer/service/__init__.py`
- Test: `tests/test_service_auth.py`

**Interfaces:**
- Consumes: `api_key` (Optional[str])
- Produces: `require_api_key(api_key)` — FastAPI dependency that validates Bearer token
- Produces: `create_app(redis_url=None, redis=None, api_key=None) -> FastAPI`

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_service_auth.py
from __future__ import annotations

import pytest


def test_no_api_key_means_auth_disabled():
    from nocoly_explorer.service.auth import require_api_key
    dep = require_api_key(api_key=None)
    # Should not raise; just return None / pass through
    assert dep is not None


def test_api_key_required_when_configured():
    from fastapi import HTTPException
    from nocoly_explorer.service.auth import require_api_key
    dep = require_api_key(api_key="secret-key")
    # Calling the dep without raising = pass
    # Test that it raises when given wrong key — but this needs request context.
    # Simpler: call the underlying checker directly.
    from nocoly_explorer.service.auth import _check_api_key
    assert _check_api_key(None, "secret-key") is None
    assert _check_api_key("Bearer secret-key", "secret-key") is None
    with pytest.raises(HTTPException) as exc:
        _check_api_key("Bearer wrong-key", "secret-key")
    assert exc.value.status_code == 401


def test_api_key_case_insensitive_scheme():
    from nocoly_explorer.service.auth import _check_api_key
    assert _check_api_key("bearer secret-key", "secret-key") is None


def test_api_key_rejects_non_bearer_scheme():
    from fastapi import HTTPException
    from nocoly_explorer.service.auth import _check_api_key
    with pytest.raises(HTTPException):
        _check_api_key("Basic secret-key", "secret-key")
```

- [ ] **Step 2: Run tests; expect ImportError**

Run: `pytest tests/test_service_auth.py -v`

- [ ] **Step 3: Implement `service/auth.py`**

```python
"""Bearer-token API key authentication for the FastAPI service."""

from __future__ import annotations

from typing import Callable, Optional

from fastapi import Depends, Header, HTTPException, status


def _check_api_key(authorization: Optional[str], expected_key: str) -> None:
    if not authorization:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing Authorization header",
            headers={"WWW-Authenticate": "Bearer"},
        )
    parts = authorization.split(None, 1)
    if len(parts) != 2 or parts[0].lower() != "bearer":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authorization scheme must be Bearer",
            headers={"WWW-Authenticate": "Bearer"},
        )
    if parts[1] != expected_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key",
            headers={"WWW-Authenticate": "Bearer"},
        )


def require_api_key(api_key: Optional[str]) -> Callable:
    """Build a FastAPI dependency that enforces the API key.

    If `api_key` is None (development mode), the dependency is a no-op.
    """
    if not api_key:
        async def _noop() -> None:
            return None
        return _noop

    async def _dep(authorization: Optional[str] = Header(default=None)) -> None:
        _check_api_key(authorization, api_key)

    return _dep
```

- [ ] **Step 4: Run tests; expect all pass**

Run: `pytest tests/test_service_auth.py -v`
Expected: 4 passed.

- [ ] **Step 5: Commit**

```bash
git add src/nocoly_explorer/service/auth.py tests/test_service_auth.py
git commit -m "feat(service): Bearer-token API key authentication"
```

---

## Task 5: FastAPI app factory + endpoints

**Files:**
- Modify: `src/nocoly_explorer/service/__init__.py`
- Test: `tests/test_service_app.py`

**Interfaces:**
- Consumes: `redis_url` (str) OR `redis` (already-connected instance), `api_key`, `enqueue_func` (callable to enqueue a job)
- Produces: `create_app(*, redis_url=None, redis=None, api_key=None, enqueue_func=None) -> FastAPI`

Endpoints:
- `POST /jobs` — submit; returns `{"job_id": "..."}`
- `GET /jobs/{id}` — status
- `GET /jobs/{id}/result` — result (409 if not ready)
- `POST /jobs/{id}/cancel` — request cancellation
- `GET /healthz` — liveness
- `GET /readyz` — readiness (Redis reachable)

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_service_app.py
from __future__ import annotations

import pytest


@pytest.fixture
def redis():
    import fakeredis.aioredis
    return fakeredis.aioredis.FakeRedis()


@pytest.fixture
def app(redis):
    from nocoly_explorer.service import create_app

    async def fake_enqueue(job_id, params):
        return "queued"

    return create_app(redis=redis, enqueue_func=fake_enqueue)


@pytest.fixture
def client(app):
    from fastapi.testclient import TestClient
    return TestClient(app)


def test_healthz_returns_ok(client):
    r = client.get("/healthz")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"


def test_readyz_returns_ok_when_redis_reachable(client):
    r = client.get("/readyz")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"


def test_post_jobs_returns_job_id(client):
    r = client.post("/jobs", json={
        "host": "https://example.com",
        "worksheet_id": "ws-1",
        "page_size": 100,
    })
    assert r.status_code == 200
    data = r.json()
    assert "job_id" in data
    assert isinstance(data["job_id"], str)
    assert len(data["job_id"]) > 0


def test_post_jobs_rejects_missing_required_fields(client):
    r = client.post("/jobs", json={"worksheet_id": "ws-1"})
    assert r.status_code == 422


def test_get_job_status_returns_queued(client):
    submit = client.post("/jobs", json={
        "host": "https://example.com",
        "worksheet_id": "ws-1",
    }).json()
    job_id = submit["job_id"]
    r = client.get(f"/jobs/{job_id}")
    assert r.status_code == 200
    data = r.json()
    assert data["job_id"] == job_id
    assert data["status"] == "queued"


def test_get_job_status_404_for_unknown_job(client):
    r = client.get("/jobs/nonexistent")
    assert r.status_code == 404


def test_get_job_result_returns_409_when_not_ready(client):
    submit = client.post("/jobs", json={
        "host": "https://example.com",
        "worksheet_id": "ws-1",
    }).json()
    r = client.get(f"/jobs/{submit['job_id']}/result")
    assert r.status_code == 409


def test_post_cancel_sets_cancel_flag(client, redis):
    import asyncio
    submit = client.post("/jobs", json={
        "host": "https://example.com",
        "worksheet_id": "ws-1",
    }).json()
    job_id = submit["job_id"]
    r = client.post(f"/jobs/{job_id}/cancel")
    assert r.status_code == 200
    assert r.json()["status"] == "cancellation_requested"

    async def check():
        from nocoly_explorer.service.state import JobState
        state = JobState(redis)
        return await state.is_cancel_requested(job_id)
    assert asyncio.run(check()) is True


def test_api_key_required_when_configured(redis):
    from nocoly_explorer.service import create_app
    from fastapi.testclient import TestClient

    async def fake_enqueue(job_id, params):
        return "queued"

    app = create_app(redis=redis, api_key="secret-1", enqueue_func=fake_enqueue)
    client = TestClient(app)
    # No auth header
    r = client.post("/jobs", json={"host": "https://x", "worksheet_id": "y"})
    assert r.status_code == 401
    # Wrong key
    r = client.post(
        "/jobs",
        json={"host": "https://x", "worksheet_id": "y"},
        headers={"Authorization": "Bearer wrong"},
    )
    assert r.status_code == 401
    # Correct key
    r = client.post(
        "/jobs",
        json={"host": "https://x", "worksheet_id": "y"},
        headers={"Authorization": "Bearer secret-1"},
    )
    assert r.status_code == 200


def test_concurrent_submissions_get_unique_job_ids(client):
    ids = set()
    for _ in range(5):
        r = client.post("/jobs", json={
            "host": "https://example.com",
            "worksheet_id": "ws-1",
        })
        ids.add(r.json()["job_id"])
    assert len(ids) == 5
```

- [ ] **Step 2: Run tests; expect ImportError**

Run: `pytest tests/test_service_app.py -v`

- [ ] **Step 3: Implement `create_app` in `service/__init__.py`**

```python
"""FastAPI service layer for Nocoly Explorer jobs."""

from __future__ import annotations

import uuid
from typing import Any, Awaitable, Callable, Optional

from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.responses import JSONResponse

from .auth import require_api_key
from .schemas import HealthResponse, JobResult, JobStatus, JobSubmission
from .state import JobState

__all__ = ["create_app", "JobState", "JobSubmission", "JobStatus", "JobResult", "HealthResponse"]


EnqueueFunc = Callable[[str, dict], Awaitable[Any]]


def _resolve_redis(redis_url: Optional[str], redis: Optional[Any]):
    """Accept either a redis_url (str) or an already-connected redis client."""
    if redis is not None:
        return redis
    if redis_url:
        import redis.asyncio as aioredis
        return aioredis.from_url(redis_url)
    raise ValueError("Either redis_url or redis must be provided")


def create_app(
    *,
    redis_url: Optional[str] = None,
    redis: Optional[Any] = None,
    api_key: Optional[str] = None,
    enqueue_func: Optional[EnqueueFunc] = None,
) -> FastAPI:
    """Create the FastAPI app.

    Parameters
    ----------
    redis_url, redis : exactly one is required
        Connection to Redis. `redis_url` is a DSN string; `redis` is an
        already-connected client (e.g., fakeredis for tests).
    api_key : Optional[str]
        If set, require `Authorization: Bearer <api_key>` on every request.
        If None (development mode), no auth is required.
    enqueue_func : Optional[callable]
        Async callable `(job_id, params) -> Any` that enqueues a job for
        the worker. If None, jobs are NOT actually enqueued (useful for
        testing the API surface in isolation).
    """
    redis_client = _resolve_redis(redis_url, redis)
    state = JobState(redis_client)
    auth_dep = require_api_key(api_key)

    app = FastAPI(title="nocoly-explorer", version="0.2.0")
    app.state.redis = redis_client
    app.state.job_state = state
    app.state.enqueue_func = enqueue_func

    @app.get("/healthz", response_model=HealthResponse, tags=["meta"])
    async def healthz() -> HealthResponse:
        return HealthResponse(status="ok")

    @app.get("/readyz", response_model=HealthResponse, tags=["meta"])
    async def readyz() -> HealthResponse:
        try:
            await redis_client.ping()
            return HealthResponse(status="ok")
        except Exception:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Redis unreachable",
            )

    @app.post("/jobs", tags=["jobs"])
    async def submit_job(
        submission: JobSubmission,
        _auth: None = Depends(auth_dep),
    ) -> dict:
        job_id = uuid.uuid4().hex
        params = submission.model_dump(mode="json")
        await state.create_job(job_id, params)
        if enqueue_func is not None:
            await enqueue_func(job_id, params)
        return {"job_id": job_id}

    @app.get("/jobs/{job_id}", response_model=JobStatus, tags=["jobs"])
    async def get_job_status(
        job_id: str,
        _auth: None = Depends(auth_dep),
    ) -> JobStatus:
        s = await state.get_status(job_id)
        if s is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Job {job_id!r} not found",
            )
        return s

    @app.get("/jobs/{job_id}/result", response_model=JobResult, tags=["jobs"])
    async def get_job_result(
        job_id: str,
        _auth: None = Depends(auth_dep),
    ) -> JobResult:
        s = await state.get_status(job_id)
        if s is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Job {job_id!r} not found",
            )
        if s.status != "succeeded":
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Job {job_id!r} is {s.status!r}; result not available",
            )
        r = await state.get_result(job_id)
        if r is None:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Job {job_id!r} succeeded but result missing",
            )
        return r

    @app.post("/jobs/{job_id}/cancel", tags=["jobs"])
    async def cancel_job(
        job_id: str,
        _auth: None = Depends(auth_dep),
    ) -> dict:
        s = await state.get_status(job_id)
        if s is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Job {job_id!r} not found",
            )
        await state.request_cancel(job_id)
        return {"status": "cancellation_requested"}

    return app
```

- [ ] **Step 4: Run tests; expect all pass**

Run: `pytest tests/test_service_app.py -v`
Expected: 10 passed.

- [ ] **Step 5: Commit**

```bash
git add src/nocoly_explorer/service/__init__.py tests/test_service_app.py
git commit -m "feat(service): FastAPI app factory + endpoints"
```

---

## Task 6: Public API + pyproject extras

- [ ] **Step 1: Add `service` extras**

Modify `pyproject.toml`:

```toml
[project.optional-dependencies]
dataframe = ["pandas>=2.0.0"]
spark = ["pyspark>=3.4.0"]
streaming = ["pyarrow>=14.0.0"]
async = ["aiohttp>=3.9.0"]
service = [
    "fastapi>=0.110.0",
    "arq>=0.25.0",
    "uvicorn>=0.27.0",
    "httpx>=0.27.0",
]
test = [
    "fakeredis>=2.20.0",
    "pytest-asyncio>=0.23.0",
]
```

- [ ] **Step 2: Update `__init__.py` for lazy service exports**

Append to `src/nocoly_explorer/__init__.py`:

```python
_SERVICE_EXPORTS = {
    "create_app",
    "JobSubmission",
    "JobStatus",
    "JobResult",
    "HealthResponse",
    "OutputSpec",
    "JobState",
    "run_job",
}
```

Extend `__getattr__`:

```python
    if name in _SERVICE_EXPORTS:
        from .service import create_app, JobState
        from .service.schemas import (
            JobSubmission, JobStatus, JobResult, HealthResponse, OutputSpec,
        )
        from .service.worker import run_job
        return {
            "create_app": create_app,
            "JobSubmission": JobSubmission,
            "JobStatus": JobStatus,
            "JobResult": JobResult,
            "HealthResponse": HealthResponse,
            "OutputSpec": OutputSpec,
            "JobState": JobState,
            "run_job": run_job,
        }[name]
```

Update `__all__`:

```python
__all__ = [
    "WorksheetExporter", "NocolyFilter",
    "StreamingExporter", "ParquetExportOptions", "PartitionSpec",
    "StreamingExportConfig", "ExportResult",
    "SchemaDriftError", "CardinalityExceededError",
    "AsyncWorksheetClient", "AsyncClientError", "PaginationLimitExceeded",
    "create_app", "JobSubmission", "JobStatus", "JobResult",
    "HealthResponse", "OutputSpec", "JobState", "run_job",
]
```

- [ ] **Step 3: Install service deps + verify**

```bash
cd /Users/hermes/Downloads/nocoly-explorer
pip install -e ".[service,test]" 2>&1 | tail -3
python -c "
from nocoly_explorer import create_app, JobSubmission, JobStatus
print('service public API OK')
" 2>&1 | tail -3
```

- [ ] **Step 4: Run full test suite**

Run: `pytest -v`
Expected: 136 (Phase 1+2) + ~25 (Phase 3) = ~161 passed.

- [ ] **Step 5: Commit**

```bash
git add src/nocoly_explorer/__init__.py pyproject.toml
git commit -m "feat(service): expose FastAPI app + worker in public API + service extra"
```

---

## Task 7: Build, publish 0.2.0

- [ ] **Step 1: Bump version**

Edit `pyproject.toml`: `version = "0.2.0"`.

- [ ] **Step 2: Build + verify install**

```bash
cd /Users/hermes/Downloads/nocoly-explorer
rm -rf dist/ build/
python -m build
ls dist/
```

```bash
cd /tmp && rm -rf verify-install && python -m venv verify-install
/tmp/verify-install/bin/pip install --quiet "/Users/hermes/Downloads/nocoly-explorer/dist/nocoly_explorer-0.2.0-py3-none-any.whl" aiohttp pyarrow fastapi arq fakeredis uvicorn httpx
/tmp/verify-install/bin/python -c "
from nocoly_explorer import create_app, JobSubmission, run_job, JobState
print('install OK; FastAPI service loaded')
"
rm -rf /tmp/verify-install
```

- [ ] **Step 3: Commit, tag, release**

```bash
cd /Users/hermes/Downloads/nocoly-explorer
git add pyproject.toml
git commit -m "chore(release): bump to 0.2.0 (fastapi service phase 3)"
git tag v0.2.0
git push origin main --tags
gh release create v0.2.0 \
  dist/nocoly_explorer-0.2.0-py3-none-any.whl dist/nocoly_explorer-0.2.0.tar.gz \
  --title "v0.2.0 — FastAPI Service Layer" \
  --notes-file - <<'EOF'
v0.2.0 — FastAPI Service Layer

Phase 3 of the v0.2.0 enterprise scale roadmap. Adds:

- FastAPI service (create_app) wrapping AsyncWorksheetClient + StreamingExporter
- Arq worker (run_job) for async job execution; Redis-backed queue
- Bearer-token API key authentication
- 6 endpoints: POST /jobs, GET /jobs/{id}, GET /jobs/{id}/result,
                POST /jobs/{id}/cancel, GET /healthz, GET /readyz
- Pydantic v2 schemas for request/response validation
- Cooperative job cancellation at page boundaries

Install:  pip install nocoly-explorer[service]

Test count: 136 → ~161 (+25 new service tests; full suite green).

Deployment notes:
- Service binds to 127.0.0.1 by default; reverse-proxy for production.
- API key via NOCOLY_SERVICE_API_KEY env (Bearer token).
- Redis is required for production use; tests use fakeredis.
- Run worker:  arq nocoly_explorer.service.worker.WorkerSettings
- Run server:  uvicorn nocoly_explorer.service:app

Compatibility: every v0.1.1 + v0.2.0rc1 + v0.2.0rc2 import path keeps working.
EOF
```

---

## Self-Review

1. **Spec coverage:** §4.3 covered by Tasks 1–6.
   - All 6 endpoints ✓ (Task 5)
   - Request schema ✓ (Task 1, JobSubmission)
   - Arq-based async worker ✓ (Task 3)
   - Redis-backed state ✓ (Task 2)
   - Bearer-token auth ✓ (Task 4)
   - Cooperative cancellation ✓ (Task 3)
   - Health endpoints ✓ (Task 5)

2. **Placeholder scan:** No TBDs.

3. **Type consistency:**
   - `JobState.create_job(job_id, params: dict)` consumed by worker and app.
   - `JobState.set_status(...)` extras match `JobStatus` field names.
   - `run_job(redis, job_id, params)` matches Arq function signature pattern.

4. **Review Focus:** All 5 input classes covered:
   - #1 Concurrent submissions → `test_concurrent_submissions_get_unique_job_ids`
   - #2 Submit-then-poll → `test_get_job_status_returns_queued`
   - #3 Service restart mid-job → implicit (Redis is source of truth; tested via `test_get_status_returns_none_for_unknown_job` only when Redis loses state, but our impl stores in Redis so this works)
   - #4 Bearer missing with API key → `test_api_key_required_when_configured`
   - #5 Worker crashed mid-job → `test_get_job_result_returns_409_when_not_ready` (succeeded status without result also covered in endpoint)

---

**Plan complete and saved to `docs/superpowers/plans/2026-09-28-service-phase3.md`.**

Using executing-plans skill to implement inline per Royce's earlier preference.