# Nocoly Explorer v0.2.0 — Enterprise Scale Design Spec

**Date:** 2026-09-28
**Author:** Hermes (architectural design for rollroyces/nocoly-explorer)
**Status:** Draft — pending user review

---

## 1. Purpose & Scope

This spec upgrades `nocoly-explorer` from a single-machine downloader (v0.1.1) into a production-grade data pipeline component. Four new subsystems are introduced, each addressing a real scaling problem the v0.1.1 client cannot solve:

| # | Subsystem | Solves | Status in v0.1.1 |
|---|-----------|--------|------------------|
| 1 | **Streaming Parquet Exporter** | 25GB+ sheets exhaust memory | Held in RAM, then written |
| 2 | **Async Pagination Engine** | Slow single-threaded pagination | Sequential, page-by-page |
| 3 | **FastAPI Service Layer** | No way to invoke from n8n pipelines | Python-only API |
| 4 | **Incremental Sync Engine** | Re-downloads everything every run | No state, no delta |

**Out of scope for v0.2.0:** OAuth flows, multi-tenant auth, full Delta Lake writes (we write Parquet that Databricks reads; a future v0.3 may add `deltalake` support), distributed execution across multiple worker machines (single-machine jobs are the primary use case).

---

## 2. Design Decisions (the calls I made)

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Scope approach | One coherent spec, sequential subsystem implementation | Coherent interfaces; each subsystem reviewed/tested before the next |
| Code maturity | Production with notebook ergonomics | Real pipelines + notebooks must both work |
| Job queue | **Arq** (asyncio-native, Redis-backed) | Matches `aiohttp` choice for Task 3; lightweight; same async event loop as the rest of the codebase |
| Sync state | **SQLite** behind `SyncStateStore` protocol | Transactional, crash-safe, single-machine. Protocol makes S3/Blob swap trivial later |
| Streaming Parquet engine | **PyArrow** with row-group flushing | Lowest-overhead Parquet writer; row-group size = Databricks sweet spot (128MB) |
| Partitioning strategy | **Hive-style** by user-chosen date/category column | Native to Spark/Databricks; downstream `spark.read.parquet(path)` auto-discovers |
| Schema enforcement | **PyArrow schema** (declared or inferred from first page) | Catches API drift before write; explicit at the seam |
| Concurrency model | **Bounded semaphore** (default 8 concurrent pages) | Predictable load on the API server; tunable |
| Rate limiting | **Per-host token bucket** with Retry-After honored | Survives transient bursts; doesn't hammer on 429 |
| FastAPI dispatch | FastAPI dispatches to Arq worker; HTTP endpoint is just submit + status | Single source of truth for job state in Redis |

---

## 3. Public API Surface (v0.2.0)

The new public API is **additive** — every v0.1.1 import path still works. New entry points:

```python
# Existing (unchanged)
from nocoly_explorer import WorksheetExporter, NocolyFilter

# New — streaming Parquet (Task 1)
from nocoly_explorer import StreamingExporter, ParquetExportOptions, PartitionSpec

# New — async pagination (Task 2 → call it Task 2 of the spec, but Task 3 of the user's request)
from nocoly_explorer import AsyncWorksheetClient

# New — incremental sync (Task 4)
from nocoly_explorer import IncrementalSyncEngine, SyncStateStore, LocalSQLiteStore

# New — FastAPI app factory (Task 2 of the spec, but Task 3 of the user's request: see note below)
from nocoly_explorer.service import create_app
```

> **Note on naming:** the user's task numbering and my internal numbering differ. I'll refer to subsystems by their user-facing name throughout: "Streaming Parquet", "FastAPI service", "Async Pagination", "Incremental Sync".

**Optional dependencies** added in `pyproject.toml`:

```toml
[project.optional-dependencies]
dataframe = ["pandas>=2.0.0"]
spark = ["pyspark>=3.4.0"]
streaming = ["pyarrow>=14.0.0"]
async = ["aiohttp>=3.9.0"]
service = [
    "fastapi>=0.110.0",
    "uvicorn[standard]>=0.27.0",
    "arq>=0.25.0",
    "redis>=5.0.0",
]
dev = ["pytest>=8.0", "pytest-asyncio>=0.23", "pytest-aiohttp>=1.0"]
```

---

## 4. Subsystem Design

### 4.1 Streaming Parquet Exporter (Task 1)

**Goal:** Stream paginated API responses to a Parquet sink without ever holding more than one row-group's worth of rows in memory. Support dynamic Hive-style partitioning by a user-chosen column.

**Architecture:**

```
StreamingExporter
 ├── WorksheetClient       (existing v0.1.1 — fetches one page at a time)
 ├── ParquetSchemaManager  (declares or infers PyArrow schema)
 ├── RowGroupBuffer        (accumulates rows until row-group target size)
 ├── PartitionRouter       (computes partition key for each row, buffers per partition)
 └── ParquetWriter         (PyArrow ParquetWriter, one per partition file)
```

**Why this shape:**
- **Streaming, not batching** — pages are flushed as soon as a row-group is full. With page_size=1000 rows × ~50 columns, and a 128MB row-group target, we accumulate ~10–100 pages per row-group depending on row size. Memory ceiling = 128MB regardless of total dataset size.
- **PartitionRouter decoupled from writer** — partition routing is a pure function (row → partition path). Easy to test, easy to swap (e.g., time-bucketed partitioning).
- **One writer per partition** — Parquet writer is not thread-safe and can't be reopened cheaply. Spawning a writer per discovered partition is the only correct way to do Hive-style partitioning incrementally.

**Memory model (the architectural decision the user asked about):**

```
Maximum in-flight memory = row_group_buffer_size
                         + row_group_buffer_size (PyArrow Table construction)
                         + 1 page (current fetch_rows result)
                         + small per-partition metadata
                       ≈ 2 × row_group_buffer_size + page_size × row_size
                       ≈ 256MB + ~10MB for default page_size=200
```

Default `row_group_buffer_bytes = 128 * 1024 * 1024` (128MB). Tunable per export.

**Partitioning logic:**
- User specifies `PartitionSpec(column="created_date", granularity="day")` or `PartitionSpec(column="region", granularity=None)`.
- For `granularity="day"`, partition column must be date-typed; partition key = `created_date.strftime("%Y-%m-%d")`. Path = `output_dir/created_date=YYYY-MM-DD/part-0.parquet`.
- For `granularity=None`, partition key = `str(row[column])`. Path = `output_dir/region=HK/part-0.parquet`.
- Cardinality guard: if more than N unique partition keys are seen, the exporter raises `OutputValidationError` (default N=10000). Free-text columns with millions of values would create a directory-per-value explosion — the guard forces users to bucket first.

**PySpark schema enforcement:**
- `ParquetSchemaManager` either takes an explicit `pyarrow.Schema` or infers one from the first page.
- Every subsequent page is validated against the schema. New columns raise `SchemaDriftError` (default: ignore extra columns; allow explicit `on_schema_drift="error"` to fail loudly).
- Downstream `spark.read.parquet(path)` uses the same schema file, so the contract is honored on the read side too.

**Failure modes & handling:**
| Mode | Behavior |
|------|----------|
| Network error mid-page | Existing `WorksheetClient` retry/backoff. After exhaustion, the in-flight row-group is abandoned (partial Parquet files are not promoted). User can re-run the export. |
| Schema drift mid-export | `on_schema_drift="error"` raises `SchemaDriftError`. `"ignore"` continues with the original schema, dropping unknown columns. |
| Partition cardinality exceeded | `OutputValidationError` at the time of discovery. Clean abort. |
| Output directory not writable | `OSError` raised at first `mkdir()`. |

**Files added:**
- `src/nocoly_explorer/streaming.py` — `StreamingExporter`, `ParquetExportOptions`, `PartitionSpec`
- `tests/test_streaming.py` — round-trip tests, partition routing, schema drift, cardinality guard
- `tests/fixtures/` — small JSON fixtures for testing

### 4.2 Async Pagination Engine (Task 3 of the user's request)

**Goal:** Fetch multiple pages concurrently; merge results into a unified result (DataFrame, list of dicts, or write to a sink). Honor 429 backoff. Bound concurrency.

**Architecture:**

```
AsyncWorksheetClient
 ├── aiohttp.ClientSession (single, connection-pooled)
 ├── TokenBucket          (per-host rate limiter)
 ├── RetryPolicy          (exponential backoff, Retry-After honored)
 └── _fetch_page()        (returns parsed JSON or raises)
```

**Concurrency model (the architectural decision the user asked about):**

- **`asyncio.Semaphore(N)`** wraps each `_fetch_page()` call. Default N=8.
- Pages are dispatched in waves. Wave 1: pages 1..N. As each page completes, page N+1 is dispatched. This bounds memory at N × page_size rows in flight.
- For 100k rows / page_size=200 = 500 pages. With N=8 concurrency, that's ~63 waves. At 200ms per page: ~13 seconds. Compare to v0.1.1 sequential: 100 seconds.
- **Order-preserving merge** — pages are merged in original page order, not completion order. Done by tracking `(page_number, data)` tuples and sorting by page number before flattening.

```python
async def fetch_all_async(self, ...) -> list[dict]:
    pages: dict[int, list[dict]] = {}
    next_page = 1
    in_flight: set[asyncio.Task] = set()
    semaphore = asyncio.Semaphore(self.concurrency)

    async def fetch_one(page: int) -> tuple[int, list[dict]]:
        async with semaphore:
            return page, await self._fetch_page_with_retry(page)

    while next_page <= max_pages or in_flight:
        # Dispatch new pages up to concurrency limit
        while len(in_flight) < self.concurrency and next_page <= max_pages:
            task = asyncio.create_task(fetch_one(next_page))
            in_flight.add(task)
            task.add_done_callback(in_flight.discard)
            next_page += 1
        if in_flight:
            done, _ = await asyncio.wait(in_flight, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                page_num, data = await task
                pages[page_num] = data
                if not data or len(data) < self.page_size:
                    # Server signaled end; cancel pending
                    next_page = max_pages + 1
                    break

    # Merge in order
    all_rows = []
    for p in sorted(pages):
        all_rows.extend(pages[p])
    return all_rows
```

**Rate-limit handling:**
- `_parse_retry_after()` from v0.1.1 is reused (delta-seconds + HTTP-date).
- 429 → sleep for Retry-After (capped at `max_wait_seconds`), then retry. Up to `max_retries`.
- 5xx → exponential backoff with jitter.
- A token bucket limits non-429 concurrency: the bucket fills at `requests_per_second` (default 10) and the semaphore blocks requests when empty.

**Failure modes & handling:**
| Mode | Behavior |
|------|----------|
| Single page fails after retries | That page is raised; in-flight tasks are cancelled (`asyncio.gather(..., return_exceptions=False)`); partial result discarded |
| Connection closed mid-response | Treated as transport error; retry with backoff |
| Schema drift mid-stream | Same as Task 1 — explicit error or ignore policy |
| Server sends duplicate page numbers | Last-write-wins (rare; logs at DEBUG) |

**Files added:**
- `src/nocoly_explorer/async_client.py` — `AsyncWorksheetClient`
- `tests/test_async_client.py` — uses `pytest-asyncio` + `aiohttp.test_utils`

### 4.3 FastAPI Service Layer (Task 2 of the user's request)

**Goal:** HTTP wrapper around the core download/transform pipeline so n8n (or any orchestrator) can submit and monitor jobs.

**Architecture:**

```
n8n → POST /jobs  →  FastAPI  →  Arq enqueue  →  Worker process
                              ↘                              ↘
                               Redis (state, queue)            AsyncWorksheetClient
                                                              + StreamingExporter
                                                              → status updates

n8n → GET /jobs/{id} → FastAPI → Redis lookup → return status
n8n → GET /jobs/{id}/result → if complete, return artifact location
```

**Why Arq over Celery/RQ/BackgroundTasks:**
- **Asyncio-native** — workers run in the same async event loop as `AsyncWorksheetClient`. No thread pool bridging.
- **Lightweight** — single Redis dependency. No result backend config, no broker routing.
- **Job durability** — unlike FastAPI `BackgroundTasks`, jobs survive worker restart. Redis holds the queue.
- **Status polling** — workers write status updates to Redis (`job.status`, `job.progress_pct`, `job.error`). FastAPI reads them.

**Endpoints:**

| Method | Path | Purpose |
|--------|------|---------|
| `POST` | `/jobs` | Submit a new extraction job. Returns `{"job_id": "..."}`. Body matches the `JobSubmission` Pydantic schema. |
| `GET` | `/jobs/{id}` | Job status: `queued` / `running` / `succeeded` / `failed` / `cancelled`. Includes `progress_pct` and `rows_fetched` if available. |
| `GET` | `/jobs/{id}/result` | If `succeeded`, returns the artifact location (S3 path, local path, or pre-signed URL depending on sink config). |
| `POST` | `/jobs/{id}/cancel` | Cooperative cancellation; worker checks at next page boundary. |
| `GET` | `/healthz` | Liveness probe. |
| `GET` | `/readyz` | Readiness — Redis reachable, schema valid. |

**Request schema (the JSON schema for n8n's HTTP Request node):**

```json
{
  "title": "NocolyJobSubmission",
  "type": "object",
  "required": ["host", "worksheet_id"],
  "properties": {
    "host":            {"type": "string", "description": "e.g. https://bpm-uat.chinachemgroup.com"},
    "worksheet_id":    {"type": "string"},
    "output": {
      "type": "object",
      "properties": {
        "sink": {"enum": ["parquet_local", "parquet_s3", "json_local"]},
        "path": {"type": "string"},
        "partition_by": {"type": "string"},
        "partition_granularity": {"enum": ["day", "month", "year", null]}
      }
    },
    "filter":           {"type": "object", "description": "Filter DSL or flat dict (auto-converted)"},
    "columns":          {"type": "array", "items": {"type": "string"}},
    "page_size":        {"type": "integer", "default": 200, "maximum": 1000},
    "max_pages":        {"type": "integer", "default": 1000, "minimum": 1},
    "concurrency":      {"type": "integer", "default": 8, "minimum": 1, "maximum": 32},
    "incremental":      {"type": "object"},
    "credentials":      {"type": "object", "description": "Either static (app_key/app_sign) or databricks (scope/secret_names)"},
    "env_prefix":       {"type": "string"}
  }
}
```

**n8n HTTP Request node configuration (drop-in):**

- **Method:** POST
- **URL:** `{{$env.NOCOLY_SERVICE_URL}}/jobs`
- **Headers:** `Content-Type: application/json`
- **Body (JSON):** match the schema above; values can use n8n expressions like `{{$json.worksheet_id}}`
- **Response handling:** n8n captures `{"job_id": "..."}`; use a subsequent HTTP Request node polling `{{service_url}}/jobs/{job_id}` until `status == "succeeded"`.

**Security:**
- Service binds to `127.0.0.1` by default; deployment behind a reverse proxy (nginx/Caddy) for production.
- API key via `NOCOLY_SERVICE_API_KEY` env var; Bearer token on `Authorization` header. FastAPI dependency that validates.
- Credentials for the actual Nocoly API are never sent over HTTP to this service — the worker reads them from its own env/config.

**Files added:**
- `src/nocoly_explorer/service/__init__.py` — `create_app()` factory
- `src/nocoly_explorer/service/schemas.py` — Pydantic request/response models
- `src/nocoly_explorer/service/worker.py` — Arq worker functions
- `src/nocoly_explorer/service/state.py` — Redis status reader/writer
- `tests/test_service.py` — uses FastAPI TestClient + fakeredis

### 4.4 Incremental Sync Engine (Task 4)

**Goal:** Track the highest `_updatedAt` seen per worksheet; on subsequent runs, fetch only rows with `_updatedAt > last_seen`; merge with existing local artifacts.

**Architecture:**

```
IncrementalSyncEngine
 ├── SyncStateStore          (Protocol)
 │   ├── LocalSQLiteStore     (default implementation)
 │   └── [future: S3, Blob]
 ├── WorksheetClient         (existing v0.1.1)
 └── SinkWriter              (writes to local Parquet/CSV/JSON, appends or rewrites)
```

**State shape** (SQLite schema):

```sql
CREATE TABLE sync_state (
  worksheet_id      TEXT PRIMARY KEY,
  host              TEXT NOT NULL,
  last_updated_at   TEXT NOT NULL,    -- ISO-8601 timestamp; highest seen
  last_sync_at      TEXT NOT NULL,    -- ISO-8601; when last run completed
  rows_synced       INTEGER NOT NULL,
  schema_hash       TEXT              -- detect upstream schema changes
);

CREATE TABLE sync_runs (
  run_id            TEXT PRIMARY KEY,
  worksheet_id      TEXT NOT NULL,
  started_at        TEXT NOT NULL,
  finished_at       TEXT,
  rows_fetched      INTEGER,
  status            TEXT NOT NULL,    -- running / succeeded / failed
  error_message     TEXT
);
```

**Algorithm:**

1. Read `last_updated_at` for `(host, worksheet_id)` from `SyncStateStore`. If no state exists, this is a full sync.
2. Build the filter: `_updatedAt > last_updated_at`. If full sync, no filter.
3. Fetch pages (sync or async — composable with Task 2's async client).
4. As rows arrive, filter for max `_updatedAt`. Append to the local artifact (Parquet: `pyarrow.parquet.ParquetWriter` append mode; CSV/JSON: read + rewrite — CSV append for partial compatibility).
5. On success, atomically update `sync_state` (transaction).
6. On failure, the run is recorded as failed; state is **not** updated. Re-running resumes from the last successful state.

**Race conditions:**
- Two simultaneous syncs for the same worksheet: detected by `started_at > last_sync_at` in `sync_runs`. The second sync raises `ConcurrentSyncError`.
- API returns rows out of order (e.g., a row updated at T+5 arrives before a row updated at T+10 due to pagination). The max seen so far is tracked in-memory; on completion, the highest `_updatedAt` observed is recorded as `last_updated_at`.

**Files added:**
- `src/nocoly_explorer/sync.py` — `IncrementalSyncEngine`, `SyncStateStore`, `LocalSQLiteStore`
- `tests/test_sync.py` — full-state; delta-state; concurrent sync; schema-drift detection

---

## 5. Cross-Cutting Concerns

### 5.1 Dependencies

`pyproject.toml` extras remain backward-compatible. New extras (`streaming`, `async`, `service`) gate heavy dependencies. The base install never requires `pyarrow`, `aiohttp`, `fastapi`, or `arq`.

### 5.2 Configuration

`PackageConfig` gains new optional fields for the new subsystems:

```python
@dataclass(slots=True)
class PackageConfig:
    # existing fields...
    
    # New: streaming Parquet defaults
    streaming_row_group_bytes: int = 128 * 1024 * 1024
    streaming_max_partition_cardinality: int = 10000
    streaming_on_schema_drift: str = "ignore"  # or "error"
    
    # New: async pagination defaults
    async_concurrency: int = 8
    async_requests_per_second: float = 10.0
    
    # New: service layer
    service_redis_url: str = "redis://localhost:6379/0"
    service_api_key: Optional[str] = None
```

These can be set in the config file (`nocoly.toml`) or programmatically.

### 5.3 Observability

All subsystems use the existing `logging` pattern. New loggers:

- `nocoly_explorer.streaming`
- `nocoly_explorer.async_client`
- `nocoly_explorer.service`
- `nocoly_explorer.sync`

Each emits structured events (`page.fetched`, `row_group.flushed`, `partition.created`, `job.submitted`, `job.status_changed`, `sync.state_updated`) suitable for shipping to Datadog/Loki.

### 5.4 Error Hierarchy

Existing `NocolyError` hierarchy is extended (additive):

```python
class NocolyError(Exception): ...
class MissingCredentialsError(NocolyError): ...
class OutputValidationError(NocolyError): ...
class EnvironmentDetectionError(NocolyError): ...

# New in v0.2.0
class SchemaDriftError(NocolyError): ...
class CardinalityExceededError(OutputValidationError): ...  # subclass for clarity
class ConcurrentSyncError(NocolyError): ...
class JobNotFoundError(NocolyError): ...
class JobCancelledError(NocolyError): ...
```

### 5.5 Testing Strategy

- **Unit tests:** every new module has tests using `pytest`. Async tests use `pytest-asyncio` (auto mode).
- **HTTP mocking:** `aioresponses` for `aiohttp`; existing monkeypatching pattern for sync `requests`.
- **Parquet round-trip:** write a streaming export to a tmp dir, read it back with `pyarrow.parquet.read_table()`, assert schema and row count match.
- **Service tests:** FastAPI `TestClient` with `fakeredis` for the queue; Arq worker tested with `arq.worker.create_pool()` against fakeredis.
- **Integration test (gated):** a CI matrix job that runs `pytest -m integration` against a real Nocoly sandbox, skipped if env vars aren't set.

Target: **120+ tests total** by v0.2.0 (current: 56).

### 5.6 Backward Compatibility

Every v0.1.1 import path and behavior is preserved. New modules are purely additive. `WorksheetExporter` and the sync `WorksheetClient` are unchanged; new behavior is exposed via new classes (`StreamingExporter`, `AsyncWorksheetClient`, `IncrementalSyncEngine`).

---

## 6. Implementation Sequence

Because Tasks 2 and 3 (in my numbering) and Task 4 build on the interfaces of Task 1, I will implement in this order:

| Phase | Subsystem | Spec sections | Estimated scope |
|-------|-----------|---------------|-----------------|
| **1** | Streaming Parquet Exporter | §4.1 | ~500 lines + ~300 lines tests |
| **2** | Async Pagination Engine | §4.2 | ~400 lines + ~250 lines tests |
| **3** | FastAPI Service Layer | §4.3 | ~600 lines (app + worker) + ~400 lines tests |
| **4** | Incremental Sync Engine | §4.4 | ~300 lines + ~250 lines tests |

After each phase: full test suite green, version bumped (0.2.0rc1, 0.2.0rc2, ...), tag, release notes.

---

## 7. Open Questions for User Review

1. **PyArrow vs Polars for Parquet writing** — PyArrow is the lowest-overhead choice but its API is more verbose. Polars is faster for some workloads but heavier dependency. I'm going with **PyArrow** unless you say otherwise.

2. **FastAPI process model** — single uvicorn process dispatching to a separate Arq worker process is the canonical setup, but it requires a Redis. Alternative: in-process worker (same process handles HTTP and jobs via a thread). Trade-off: simpler ops vs job loss on restart. I'm going with **separate worker + Redis** because 25GB jobs shouldn't live in the same process as the API.

3. **Sync state TTL** — should `last_updated_at` records expire if a worksheet hasn't been synced in N days? I'm going with **no expiry** by default, but exposing a `purge_stale(max_age_days=90)` method on `SyncStateStore`.

4. **Partitioning column cardinality guard** — what's a sane default? I'm picking **10,000 unique values** based on the assumption that anything with more is either a free-text column (should be bucketed) or a unique-ID column (should not be partitioned by). Override via `ParquetExportOptions(max_partition_cardinality=N)`.

---

## 8. Spec Self-Review

- **Placeholders:** None. All sections have concrete code-shaped decisions.
- **Internal consistency:** Task 2 (async) and Task 1 (streaming) compose: the async client emits pages; the streaming exporter consumes them. The interface boundary is `AsyncWorksheetClient.fetch_all_async() -> AsyncIterator[PageChunk]`, returning chunks as soon as pages complete. The streaming exporter writes them.
- **Scope:** Single v0.2.0 release. Decomposed into 4 phases that can ship independently as release candidates.
- **Ambiguity:** "Optimized for Databricks" is now concrete: Hive-style partitioning, 128MB row groups, PyArrow schema → Spark read.

---

**Next step after your approval:** invoke `superpowers:writing-plans` to produce an implementation plan for Phase 1 (Streaming Parquet Exporter), then proceed phase by phase.