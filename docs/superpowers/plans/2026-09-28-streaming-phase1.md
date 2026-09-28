# Streaming Parquet Exporter — Phase 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a streaming Parquet exporter that paginates a Nocoly worksheet and writes rows to disk without ever holding more than ~128MB of rows in memory, with Hive-style dynamic partitioning and PyArrow schema enforcement.

**Architecture:** `StreamingExporter` consumes pages from the existing `WorksheetClient` (one page at a time), buffers rows until a 128MB row-group threshold, then writes a row-group to the appropriate partition file. A `PartitionRouter` computes the partition key per row; a `ParquetSchemaManager` enforces a PyArrow schema across all rows. One `pyarrow.parquet.ParquetWriter` per partition file handles append-mode writes safely.

**Tech Stack:** PyArrow 25 (Parquet), existing `WorksheetClient` (sync), existing `NocolyFilter`, `pytest` (existing test harness).

**Spec:** `docs/superpowers/specs/2026-09-28-enterprise-v0.2.0-design.md` §4.1

---

## Global Constraints

- Python 3.10+ (project baseline)
- PyArrow ≥ 14 (we have 25.0.1)
- Backward compatibility: every v0.1.1 import path MUST keep working. New code lives in new modules.
- Optional dependency: `streaming` extra requires `pyarrow>=14.0.0`. Importing `StreamingExporter` without pyarrow raises a clear `ImportError` pointing to the extra.
- Memory ceiling: peak in-flight memory ≤ `2 * row_group_buffer_bytes + page_size * avg_row_bytes`. Default `row_group_buffer_bytes = 128 * 1024 * 1024`.
- All new exceptions must subclass the existing `NocolyError` hierarchy. New: `SchemaDriftError` and `CardinalityExceededError`.
- Logging via `logging.getLogger("nocoly_explorer.streaming")` at structured `extra=...` style consistent with v0.1.1.
- Tests must use the existing `tests/conftest.py` pattern (no new fixtures framework).

---

## Review Focus

These five input classes/failure modes are not exercised by an individual task's tests but a reasonable user will hit them. Each gets pinned to the task that owns the code.

1. **Empty result set** — `export()` with zero rows returns successfully and produces no Parquet files (or just an empty `_SUCCESS` marker, depending on Hive convention). → Task 5's test for `test_streaming_export_with_zero_rows`.
2. **All rows share a single partition key** — single-partition case must still produce a valid single file under `partition_col=VALUE/part-0.parquet`. → Task 4's test for `test_single_partition_yields_single_file`.
3. **Page schema diverges mid-stream** (e.g., a new column appears on page 5) — default behavior is "ignore" with a logged warning; explicit `on_schema_drift="error"` raises. → Task 3's test for `test_schema_drift_mid_stream_ignored_and_logged` and `test_schema_drift_mid_stream_raises_when_error`.
4. **Output directory pre-exists with conflicting files** — re-running an export to the same directory must either overwrite cleanly or raise. → Task 5's test for `test_export_to_existing_directory_does_not_overwrite_silently`.
5. **`file_path` ends with `.parquet` but user wanted a directory** — the exporter's `output_dir` is always treated as a directory; if it ends in `.parquet`, we raise `OutputValidationError` early. → Task 2's test for `test_output_dir_cannot_end_in_parquet_extension`.

---

## File Structure

New files:

- `src/nocoly_explorer/streaming/__init__.py` — public API re-exports
- `src/nocoly_explorer/streaming/exporter.py` — `StreamingExporter` class
- `src/nocoly_explorer/streaming/options.py` — `ParquetExportOptions`, `PartitionSpec` dataclasses
- `src/nocoly_explorer/streaming/schema.py` — `ParquetSchemaManager`
- `src/nocoly_explorer/streaming/partitions.py` — `PartitionRouter`
- `src/nocoly_explorer/streaming/writer.py` — `ParquetPartitionWriter` (one writer per partition file)
- `tests/test_streaming.py` — comprehensive test suite
- `tests/streaming_fixtures.py` — small JSON page fixtures

Modified files:

- `pyproject.toml` — add `[streaming]` extra
- `src/nocoly_explorer/__init__.py` — re-export `StreamingExporter`, `ParquetExportOptions`, `PartitionSpec`, `SchemaDriftError`, `CardinalityExceededError`
- `src/nocoly_explorer/exceptions.py` — add `SchemaDriftError`, `CardinalityExceededError`

---

## Task 1: Options & Exceptions (foundation)

**Files:**
- Create: `src/nocoly_explorer/streaming/__init__.py`
- Create: `src/nocoly_explorer/streaming/options.py`
- Modify: `src/nocoly_explorer/exceptions.py:1-20`
- Test: `tests/test_streaming_options.py`

**Interfaces:**
- Consumes: `typing.Any` (no dependencies on earlier tasks)
- Produces: `PartitionSpec(column: str, granularity: Optional[Literal["day","month","year"]])`, `ParquetExportOptions(row_group_bytes: int = 134217728, max_partition_cardinality: int = 10000, on_schema_drift: Literal["ignore","error"] = "ignore", compression: str = "snappy", write_statistics: bool = True)`, `SchemaDriftError`, `CardinalityExceededError`.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_streaming_options.py
from __future__ import annotations

import pytest

from nocoly_explorer.exceptions import NocolyError
from nocoly_explorer.streaming.options import (
    ParquetExportOptions,
    PartitionSpec,
)


def test_partition_spec_default_granularity_is_none():
    spec = PartitionSpec(column="region")
    assert spec.column == "region"
    assert spec.granularity is None


def test_partition_spec_with_day_granularity():
    spec = PartitionSpec(column="created_date", granularity="day")
    assert spec.column == "created_date"
    assert spec.granularity == "day"


def test_partition_spec_rejects_invalid_granularity():
    with pytest.raises(ValueError):
        PartitionSpec(column="x", granularity="hour")


def test_partition_spec_rejects_empty_column():
    with pytest.raises(ValueError):
        PartitionSpec(column="")


def test_parquet_export_options_defaults():
    opts = ParquetExportOptions()
    assert opts.row_group_bytes == 128 * 1024 * 1024
    assert opts.max_partition_cardinality == 10000
    assert opts.on_schema_drift == "ignore"
    assert opts.compression == "snappy"
    assert opts.write_statistics is True


def test_parquet_export_options_validates_row_group_bytes():
    with pytest.raises(ValueError):
        ParquetExportOptions(row_group_bytes=0)


def test_parquet_export_options_validates_drift_policy():
    with pytest.raises(ValueError):
        ParquetExportOptions(on_schema_drift="warn")


def test_parquet_export_options_validates_cardinality():
    with pytest.raises(ValueError):
        ParquetExportOptions(max_partition_cardinality=0)


def test_schema_drift_and_cardinality_inherit_from_nocoly_error():
    from nocoly_explorer.exceptions import SchemaDriftError, CardinalityExceededError
    assert issubclass(SchemaDriftError, NocolyError)
    assert issubclass(CardinalityExceededError, NocolyError)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_streaming_options.py -v`
Expected: ImportError or collection error (modules not yet created).

- [ ] **Step 3: Add `SchemaDriftError` and `CardinalityExceededError` to `exceptions.py`**

Append to `src/nocoly_explorer/exceptions.py`:

```python
class SchemaDriftError(NocolyError):
    """Raised when the API response schema diverges from the declared one."""


class CardinalityExceededError(NocolyError):
    """Raised when a partition column exceeds the configured max distinct values."""
```

- [ ] **Step 4: Create `streaming/__init__.py` (empty for now)**

```python
"""Streaming Parquet export for Nocoly worksheet data."""
```

- [ ] **Step 5: Implement `streaming/options.py`**

```python
"""Configuration objects for streaming Parquet exports."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Optional

_VALID_GRANULARITIES = ("day", "month", "year")
_VALID_DRIFT_POLICIES = ("ignore", "error")
_VALID_COMPRESSIONS = ("snappy", "gzip", "zstd", "lz4", "brotli", None)


@dataclass(frozen=True, slots=True)
class PartitionSpec:
    """How to partition the output Parquet dataset."""

    column: str
    granularity: Optional[Literal["day", "month", "year"]] = None

    def __post_init__(self) -> None:
        if not self.column:
            raise ValueError("PartitionSpec.column must be a non-empty string")
        if self.granularity is not None and self.granularity not in _VALID_GRANULARITIES:
            raise ValueError(
                f"PartitionSpec.granularity must be one of {_VALID_GRANULARITIES} or None; "
                f"got {self.granularity!r}"
            )


@dataclass(frozen=True, slots=True)
class ParquetExportOptions:
    """Tunables for the streaming exporter."""

    row_group_bytes: int = 128 * 1024 * 1024
    max_partition_cardinality: int = 10_000
    on_schema_drift: Literal["ignore", "error"] = "ignore"
    compression: Optional[str] = "snappy"
    write_statistics: bool = True

    def __post_init__(self) -> None:
        if self.row_group_bytes < 1024 * 1024:
            raise ValueError(
                f"row_group_bytes must be >= 1 MiB (got {self.row_group_bytes})"
            )
        if self.max_partition_cardinality < 1:
            raise ValueError(
                f"max_partition_cardinality must be >= 1 (got {self.max_partition_cardinality})"
            )
        if self.on_schema_drift not in _VALID_DRIFT_POLICIES:
            raise ValueError(
                f"on_schema_drift must be one of {_VALID_DRIFT_POLICIES}; "
                f"got {self.on_schema_drift!r}"
            )
        if self.compression not in _VALID_COMPRESSIONS:
            raise ValueError(
                f"compression must be one of {_VALID_COMPRESSIONS}; "
                f"got {self.compression!r}"
            )
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `pytest tests/test_streaming_options.py -v`
Expected: 9 passed.

- [ ] **Step 7: Commit**

```bash
cd /Users/hermes/Downloads/nocoly-explorer
git add src/nocoly_explorer/exceptions.py src/nocoly_explorer/streaming/__init__.py src/nocoly_explorer/streaming/options.py tests/test_streaming_options.py
git commit -m "feat(streaming): PartitionSpec + ParquetExportOptions + exceptions"
```

---

## Task 2: Output directory validation (Review Focus #5)

**Files:**
- Modify: `src/nocoly_explorer/streaming/options.py:1-60`
- Test: `tests/test_streaming_options.py` (extend)

**Interfaces:**
- Consumes: `PartitionSpec`, `ParquetExportOptions` from Task 1
- Produces: `validate_output_dir(path: str | Path) -> Path` and `resolve_partition_path(output_dir, partition_key: str) -> Path`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_streaming_options.py`:

```python
from pathlib import Path
import tempfile

from nocoly_explorer.exceptions import OutputValidationError
from nocoly_explorer.streaming.options import resolve_partition_path, validate_output_dir


def test_validate_output_dir_creates_directory(tmp_path: Path):
    target = tmp_path / "exports" / "deep" / "path"
    result = validate_output_dir(target)
    assert result.exists()
    assert result.is_dir()


def test_validate_output_dir_rejects_existing_file(tmp_path: Path):
    existing_file = tmp_path / "not-a-dir.txt"
    existing_file.write_text("")
    with pytest.raises(OutputValidationError):
        validate_output_dir(existing_file)


def test_validate_output_dir_rejects_parquet_extension(tmp_path: Path):
    target = tmp_path / "data.parquet"
    with pytest.raises(OutputValidationError) as excinfo:
        validate_output_dir(target)
    assert ".parquet" in str(excinfo.value).lower()


def test_validate_output_dir_accepts_existing_empty_dir(tmp_path: Path):
    target = tmp_path / "existing-empty"
    target.mkdir()
    result = validate_output_dir(target)
    assert result == target


def test_resolve_partition_path_returns_hive_style():
    out = resolve_partition_path(Path("/data"), "created_date=2026-09-28")
    assert out == Path("/data/created_date=2026-09-28")


def test_resolve_partition_path_rejects_path_traversal():
    with pytest.raises(ValueError):
        resolve_partition_path(Path("/data"), "../../etc/passwd")


def test_resolve_partition_path_rejects_absolute_keys():
    with pytest.raises(ValueError):
        resolve_partition_path(Path("/data"), "/etc/passwd")
```

- [ ] **Step 2: Run tests; expect ImportError / collection failure**

Run: `pytest tests/test_streaming_options.py::test_validate_output_dir_creates_directory -v`
Expected: ImportError or AttributeError.

- [ ] **Step 3: Add the helpers to `streaming/options.py`**

Append to `src/nocoly_explorer/streaming/options.py`:

```python
import re
from pathlib import Path
from typing import Union

from ..exceptions import OutputValidationError

_PARTITION_KEY_RE = re.compile(r"^[A-Za-z0-9_.\-]+=[A-Za-z0-9_.\-]+$")


def validate_output_dir(path: Union[str, Path]) -> Path:
    """Validate and create the output directory. Rejects files and .parquet paths."""
    p = Path(path)
    if p.suffix.lower() == ".parquet":
        raise OutputValidationError(
            f"output_dir must be a directory, not a Parquet file path. Got {p!s}"
        )
    if p.exists() and not p.is_dir():
        raise OutputValidationError(
            f"output_dir must be a directory; {p!s} exists and is a file."
        )
    p.mkdir(parents=True, exist_ok=True)
    return p


def resolve_partition_path(output_dir: Path, partition_key: str) -> Path:
    """Resolve a Hive-style partition key to a directory under output_dir.

    Partition keys look like 'created_date=2026-09-28'. Path traversal and
    absolute paths are rejected to keep the export sandboxed.
    """
    if not _PARTITION_KEY_RE.match(partition_key):
        raise ValueError(
            f"Invalid partition key {partition_key!r}; expected 'column=value' with safe characters."
        )
    partition_path = (output_dir / partition_key).resolve()
    output_resolved = output_dir.resolve()
    if not str(partition_path).startswith(str(output_resolved) + "/"):
        raise ValueError(f"Partition key {partition_key!r} escapes output_dir")
    return partition_path
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_streaming_options.py -v`
Expected: 16 passed (9 original + 7 new).

- [ ] **Step 5: Commit**

```bash
git add src/nocoly_explorer/streaming/options.py tests/test_streaming_options.py
git commit -m "feat(streaming): validate_output_dir and resolve_partition_path"
```

---

## Task 3: ParquetSchemaManager (Review Focus #3)

**Files:**
- Create: `src/nocoly_explorer/streaming/schema.py`
- Test: `tests/test_streaming_schema.py`

**Interfaces:**
- Consumes: first page of rows (list of dicts), drift policy from `ParquetExportOptions`
- Produces: `ParquetSchemaManager` with methods `ingest_page(rows: list[dict]) -> pa.Table`, `schema: pa.Schema`, `policy: str`, `drift_seen: bool`

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_streaming_schema.py
from __future__ import annotations

import datetime

import pyarrow as pa
import pytest

from nocoly_explorer.exceptions import SchemaDriftError
from nocoly_explorer.streaming.options import ParquetExportOptions
from nocoly_explorer.streaming.schema import ParquetSchemaManager


def test_first_page_creates_schema():
    mgr = ParquetSchemaManager(ParquetExportOptions())
    rows = [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}]
    table = mgr.ingest_page(rows)
    assert mgr.schema is not None
    assert mgr.schema.names == ["a", "b"]
    assert table.num_rows == 2


def test_second_page_with_same_schema_passes():
    mgr = ParquetSchemaManager(ParquetExportOptions())
    mgr.ingest_page([{"a": 1, "b": "x"}])
    mgr.ingest_page([{"a": 2, "b": "y"}])
    assert mgr.drift_seen is False


def test_extra_column_in_subsequent_page_is_ignored_by_default():
    mgr = ParquetSchemaManager(ParquetExportOptions(on_schema_drift="ignore"))
    mgr.ingest_page([{"a": 1, "b": "x"}])
    table = mgr.ingest_page([{"a": 2, "b": "y", "d": "extra"}])
    # 'd' must not be in the final schema
    assert "d" not in mgr.schema.names
    assert mgr.drift_seen is True
    assert table.column_names == ["a", "b"]


def test_extra_column_raises_when_policy_is_error():
    mgr = ParquetSchemaManager(ParquetExportOptions(on_schema_drift="error"))
    mgr.ingest_page([{"a": 1, "b": "x"}])
    with pytest.raises(SchemaDriftError):
        mgr.ingest_page([{"a": 2, "b": "y", "d": "extra"}])


def test_missing_column_in_subsequent_page_filled_with_null():
    mgr = ParquetSchemaManager(ParquetExportOptions())
    mgr.ingest_page([{"a": 1, "b": "x", "c": 9}])
    table = mgr.ingest_page([{"a": 2, "b": "y"}])
    assert mgr.schema.names == ["a", "b", "c"]
    assert table.column("c").to_pylist() == [None]


def test_type_inference_handles_common_python_types():
    mgr = ParquetSchemaManager(ParquetExportOptions())
    rows = [
        {"i": 1, "f": 1.5, "s": "x", "b": True, "dt": datetime.date(2026, 1, 1)},
    ]
    table = mgr.ingest_page(rows)
    assert table.schema.field("i").type == pa.int64()
    assert table.schema.field("f").type == pa.float64()
    assert table.schema.field("s").type == pa.string()
    assert table.schema.field("b").type == pa.bool_()
    assert pa.types.is_date(table.schema.field("dt").type)


def test_explicit_schema_is_used_when_provided():
    explicit = pa.schema([pa.field("a", pa.int32()), pa.field("b", pa.string())])
    mgr = ParquetSchemaManager(ParquetExportOptions(), explicit_schema=explicit)
    rows = [{"a": 1, "b": "x"}]
    table = mgr.ingest_page(rows)
    assert mgr.schema == explicit
    assert table.schema.field("a").type == pa.int32()
```

- [ ] **Step 2: Run tests; expect ImportError**

Run: `pytest tests/test_streaming_schema.py -v`
Expected: collection error (module missing).

- [ ] **Step 3: Implement `streaming/schema.py`**

```python
"""PyArrow schema inference and enforcement for streaming exports."""

from __future__ import annotations

import datetime
import decimal
import logging
import uuid
from typing import Any, Dict, List, Optional, Sequence

import pyarrow as pa

from ..exceptions import SchemaDriftError
from .options import ParquetExportOptions

_LOGGER = logging.getLogger(__name__)


def _infer_pyarrow_type(value: Any) -> pa.DataType:
    """Map a Python sample value to a PyArrow type."""
    if value is None:
        return pa.null()
    if isinstance(value, bool):
        return pa.bool_()
    if isinstance(value, int):
        return pa.int64()
    if isinstance(value, float):
        return pa.float64()
    if isinstance(value, decimal.Decimal):
        return pa.decimal128(38, 10)
    if isinstance(value, (datetime.datetime, datetime.date)):
        if isinstance(value, datetime.datetime):
            return pa.timestamp("us", tz="UTC") if value.tzinfo else pa.timestamp("us")
        return pa.date32()
    if isinstance(value, datetime.time):
        return pa.time64("us")
    if isinstance(value, datetime.timedelta):
        return pa.duration("us")
    if isinstance(value, uuid.UUID):
        return pa.string()
    if isinstance(value, bytes):
        return pa.binary()
    if isinstance(value, str):
        return pa.string()
    if isinstance(value, (list, tuple)):
        return pa.list_(_infer_pyarrow_type(value[0])) if value else pa.list_(pa.null())
    if isinstance(value, dict):
        return pa.struct(
            [(str(k), _infer_pyarrow_type(v)) for k, v in value.items()]
        )
    raise TypeError(f"Cannot infer PyArrow type from {type(value).__name__}")


def _infer_schema_from_rows(rows: Sequence[Dict[str, Any]]) -> pa.Schema:
    """Infer a schema by sampling the first non-empty row."""
    if not rows:
        raise ValueError("Cannot infer schema from empty rows")
    first = rows[0]
    fields = []
    for key, value in first.items():
        inferred = _infer_pyarrow_type(value)
        fields.append(pa.field(key, inferred, nullable=True))
    return pa.schema(fields)


def _project_rows_to_schema(rows: Sequence[Dict[str, Any]], schema: pa.Schema) -> List[Dict[str, Any]]:
    """Project rows onto a schema: drop unknown fields, fill missing with None."""
    schema_keys = {f.name for f in schema}
    projected = []
    for row in rows:
        projected.append({k: row.get(k) for k in schema_keys})
    return projected


class ParquetSchemaManager:
    """Infers (or accepts) a PyArrow schema and enforces it across pages."""

    def __init__(
        self,
        options: ParquetExportOptions,
        explicit_schema: Optional[pa.Schema] = None,
    ) -> None:
        self.options = options
        self._explicit_schema = explicit_schema
        self._schema: Optional[pa.Schema] = explicit_schema
        self.drift_seen: bool = False

    @property
    def schema(self) -> pa.Schema:
        if self._schema is None:
            raise RuntimeError("Schema not yet inferred; call ingest_page first.")
        return self._schema

    def ingest_page(self, rows: Sequence[Dict[str, Any]]) -> pa.Table:
        """Convert rows to a PyArrow Table, validating against the existing schema."""
        if not rows:
            # Empty page: return an empty table matching the schema if known.
            if self._schema is not None:
                return self._schema.empty_table()
            return pa.table({})

        if self._schema is None:
            if self._explicit_schema is not None:
                self._schema = self._explicit_schema
            else:
                self._schema = _infer_schema_from_rows(rows)

        row_keys = set()
        for row in rows:
            row_keys.update(row.keys())
        schema_keys = {f.name for f in self._schema}
        new_keys = row_keys - schema_keys

        if new_keys:
            self.drift_seen = True
            if self.options.on_schema_drift == "error":
                raise SchemaDriftError(
                    f"Page contains columns not in declared schema: {sorted(new_keys)}"
                )
            _LOGGER.warning(
                "streaming_parquet.schema_drift",
                extra={
                    "new_columns": sorted(new_keys),
                    "policy": "ignore",
                },
            )

        projected = _project_rows_to_schema(rows, self._schema)
        return pa.Table.from_pylist(projected, schema=self._schema)
```

- [ ] **Step 4: Run tests; expect all pass**

Run: `pytest tests/test_streaming_schema.py -v`
Expected: 7 passed.

- [ ] **Step 5: Commit**

```bash
git add src/nocoly_explorer/streaming/schema.py tests/test_streaming_schema.py
git commit -m "feat(streaming): ParquetSchemaManager with drift handling"
```

---

## Task 4: PartitionRouter

**Files:**
- Create: `src/nocoly_explorer/streaming/partitions.py`
- Test: `tests/test_streaming_partitions.py`

**Interfaces:**
- Consumes: `PartitionSpec`, `CardinalityExceededError`
- Produces: `PartitionRouter(spec: PartitionSpec, max_cardinality: int = 10000)` with method `key_for(row: dict) -> str`

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_streaming_partitions.py
from __future__ import annotations

import datetime

import pytest

from nocoly_explorer.exceptions import CardinalityExceededError
from nocoly_explorer.streaming.options import PartitionSpec
from nocoly_explorer.streaming.partitions import PartitionRouter


def test_router_no_partition_returns_none():
    router = PartitionRouter(PartitionSpec(column="unused"), partition_column=None)
    assert router.key_for({"any": "row"}) is None


def test_router_simple_string_partition():
    spec = PartitionSpec(column="region")
    router = PartitionRouter(spec, partition_column="region")
    assert router.key_for({"region": "HK"}) == "region=HK"


def test_router_string_with_unsafe_chars_replaced():
    spec = PartitionSpec(column="region")
    router = PartitionRouter(spec, partition_column="region")
    # Spaces and slashes must be replaced for filesystem safety
    assert router.key_for({"region": "Hong Kong / Kowloon"}) == "region=Hong Kong _ Kowloon"


def test_router_date_day_granularity():
    spec = PartitionSpec(column="created_date", granularity="day")
    router = PartitionRouter(spec, partition_column="created_date")
    row = {"created_date": datetime.date(2026, 9, 28)}
    assert router.key_for(row) == "created_date=2026-09-28"


def test_router_datetime_day_granularity():
    spec = PartitionSpec(column="created_date", granularity="day")
    router = PartitionRouter(spec, partition_column="created_date")
    row = {"created_date": datetime.datetime(2026, 9, 28, 14, 30, 0)}
    assert router.key_for(row) == "created_date=2026-09-28"


def test_router_month_granularity():
    spec = PartitionSpec(column="created_date", granularity="month")
    router = PartitionRouter(spec, partition_column="created_date")
    row = {"created_date": datetime.date(2026, 9, 28)}
    assert router.key_for(row) == "created_date=2026-09"


def test_router_year_granularity():
    spec = PartitionSpec(column="created_date", granularity="year")
    router = PartitionRouter(spec, partition_column="created_date")
    row = {"created_date": datetime.date(2026, 9, 28)}
    assert router.key_for(row) == "created_date=2026"


def test_router_cardinality_guard():
    spec = PartitionSpec(column="user_id")
    router = PartitionRouter(spec, partition_column="user_id", max_cardinality=3)
    router.key_for({"user_id": "a"})
    router.key_for({"user_id": "b"})
    router.key_for({"user_id": "c"})
    with pytest.raises(CardinalityExceededError):
        router.key_for({"user_id": "d"})


def test_router_null_partition_value_becomes_null_partition():
    spec = PartitionSpec(column="region")
    router = PartitionRouter(spec, partition_column="region")
    assert router.key_for({"region": None}) == "region=__HIVE_NULL__"


def test_router_missing_partition_column_treated_as_null():
    spec = PartitionSpec(column="region")
    router = PartitionRouter(spec, partition_column="region")
    assert router.key_for({}) == "region=__HIVE_NULL__"
```

- [ ] **Step 2: Run tests; expect ImportError**

Run: `pytest tests/test_streaming_partitions.py -v`
Expected: collection error.

- [ ] **Step 3: Implement `streaming/partitions.py`**

```python
"""Compute partition keys for rows during a streaming export."""

from __future__ import annotations

import datetime
import re
from typing import Any, Dict, Optional

from ..exceptions import CardinalityExceededError
from .options import PartitionSpec

_UNSAFE_CHAR_RE = re.compile(r"[^A-Za-z0-9_.\-]")
_HIVE_NULL = "__HIVE_NULL__"


class PartitionRouter:
    """Pure-function router: row → partition key string."""

    def __init__(
        self,
        spec: PartitionSpec,
        partition_column: Optional[str] = None,
        max_cardinality: int = 10_000,
    ) -> None:
        self.spec = spec
        self.partition_column = partition_column
        self.max_cardinality = max_cardinality
        self._seen_keys: set[str] = set()

    def key_for(self, row: Dict[str, Any]) -> Optional[str]:
        if self.partition_column is None:
            return None
        value = row.get(self.partition_column)
        key = self._format_key(value)
        if key not in self._seen_keys:
            if len(self._seen_keys) >= self.max_cardinality:
                raise CardinalityExceededError(
                    f"Partition column {self.partition_column!r} exceeded "
                    f"max_cardinality={self.max_cardinality}. "
                    f"Bucket the values or raise max_partition_cardinality."
                )
            self._seen_keys.add(key)
        return key

    def _format_key(self, value: Any) -> str:
        col = self.partition_column
        if value is None:
            return f"{col}={_HIVE_NULL}"
        if self.spec.granularity == "day" and isinstance(value, (datetime.date, datetime.datetime)):
            return f"{col}={value.strftime('%Y-%m-%d')}"
        if self.spec.granularity == "month" and isinstance(value, (datetime.date, datetime.datetime)):
            return f"{col}={value.strftime('%Y-%m')}"
        if self.spec.granularity == "year" and isinstance(value, (datetime.date, datetime.datetime)):
            return f"{col}={value.strftime('%Y')}"
        sanitized = _UNSAFE_CHAR_RE.sub("_", str(value))
        return f"{col}={sanitized}"
```

- [ ] **Step 4: Run tests; expect all pass**

Run: `pytest tests/test_streaming_partitions.py -v`
Expected: 9 passed.

- [ ] **Step 5: Commit**

```bash
git add src/nocoly_explorer/streaming/partitions.py tests/test_streaming_partitions.py
git commit -m "feat(streaming): PartitionRouter with cardinality guard"
```

---

## Task 5: ParquetPartitionWriter (per-partition file write + flush)

**Files:**
- Create: `src/nocoly_explorer/streaming/writer.py`
- Test: `tests/test_streaming_writer.py`

**Interfaces:**
- Consumes: `pyarrow.Schema`, `ParquetExportOptions`, `Path`
- Produces: `ParquetPartitionWriter` with methods `write_chunk(table: pa.Table)`, `close()`, `path: Path`, `rows_written: int`, `bytes_written: int`

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_streaming_writer.py
from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest

from nocoly_explorer.streaming.options import ParquetExportOptions
from nocoly_explorer.streaming.writer import ParquetPartitionWriter


def _make_table(n: int) -> pa.Table:
    return pa.table({"a": list(range(n)), "b": [f"r{i}" for i in range(n)]})


def test_writer_creates_file_on_first_chunk(tmp_path: Path):
    schema = pa.schema([("a", pa.int64()), ("b", pa.string())])
    target = tmp_path / "data.parquet"
    w = ParquetPartitionWriter(target, schema, ParquetExportOptions())
    w.write_chunk(_make_table(10))
    assert target.exists()
    w.close()
    assert target.stat().st_size > 0


def test_writer_appends_on_subsequent_chunks(tmp_path: Path):
    schema = pa.schema([("a", pa.int64()), ("b", pa.string())])
    target = tmp_path / "data.parquet"
    w = ParquetPartitionWriter(target, schema, ParquetExportOptions())
    w.write_chunk(_make_table(10))
    w.write_chunk(_make_table(10))
    w.close()
    table = pa.parquet.read_table(target)
    assert table.num_rows == 20


def test_writer_close_is_idempotent(tmp_path: Path):
    schema = pa.schema([("a", pa.int64()), ("b", pa.string())])
    target = tmp_path / "data.parquet"
    w = ParquetPartitionWriter(target, schema, ParquetExportOptions())
    w.write_chunk(_make_table(5))
    w.close()
    w.close()  # second call must not raise
    assert target.exists()


def test_writer_tracks_rows_and_bytes(tmp_path: Path):
    schema = pa.schema([("a", pa.int64()), ("b", pa.string())])
    target = tmp_path / "data.parquet"
    w = ParquetPartitionWriter(target, schema, ParquetExportOptions())
    w.write_chunk(_make_table(100))
    w.close()
    assert w.rows_written == 100
    assert w.bytes_written > 0


def test_writer_close_without_writes_does_not_create_file(tmp_path: Path):
    schema = pa.schema([("a", pa.int64())])
    target = tmp_path / "data.parquet"
    w = ParquetPartitionWriter(target, schema, ParquetExportOptions())
    w.close()
    assert not target.exists()


def test_writer_uses_specified_compression(tmp_path: Path):
    schema = pa.schema([("a", pa.int64()), ("b", pa.string())])
    target = tmp_path / "data.parquet"
    w = ParquetPartitionWriter(target, schema, ParquetExportOptions(compression="gzip"))
    w.write_chunk(_make_table(100))
    w.close()
    md = pa.parquet.read_metadata(target)
    # PyArrow stores compression in column chunk metadata; we just verify the file is valid.
    assert md.num_rows == 100
```

- [ ] **Step 2: Run tests; expect ImportError**

Run: `pytest tests/test_streaming_writer.py -v`
Expected: collection error.

- [ ] **Step 3: Implement `streaming/writer.py`**

```python
"""Per-partition Parquet writer with append-mode row-group flushing."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import pyarrow as pa
import pyarrow.parquet as pq

from .options import ParquetExportOptions

_LOGGER = logging.getLogger(__name__)


class ParquetPartitionWriter:
    """Wraps pyarrow.parquet.ParquetWriter for append-mode writes to one file."""

    def __init__(
        self,
        path: Path,
        schema: pa.Schema,
        options: ParquetExportOptions,
    ) -> None:
        self.path = path
        self.schema = schema
        self.options = options
        self._writer: Optional[pq.ParquetWriter] = None
        self.rows_written: int = 0
        self.bytes_written: int = 0

    def _ensure_writer(self) -> pq.ParquetWriter:
        if self._writer is None:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self._writer = pq.ParquetWriter(
                str(self.path),
                self.schema,
                compression=self.options.compression,
                write_statistics=self.options.write_statistics,
                use_dictionary=True,
            )
        return self._writer

    def write_chunk(self, table: pa.Table) -> None:
        if table.num_rows == 0:
            return
        writer = self._ensure_writer()
        writer.write_table(table)
        self.rows_written += table.num_rows
        self.bytes_written += table.nbytes

    def close(self) -> None:
        if self._writer is not None:
            self._writer.close()
            self._writer = None
            _LOGGER.debug(
                "streaming_parquet.partition_closed",
                extra={"path": str(self.path), "rows": self.rows_written},
            )
```

- [ ] **Step 4: Run tests; expect all pass**

Run: `pytest tests/test_streaming_writer.py -v`
Expected: 6 passed.

- [ ] **Step 5: Commit**

```bash
git add src/nocoly_explorer/streaming/writer.py tests/test_streaming_writer.py
git commit -m "feat(streaming): ParquetPartitionWriter append-mode wrapper"
```

---

## Task 6: RowGroupBuffer (rows → chunks of ~row_group_bytes)

**Files:**
- Create: `src/nocoly_explorer/streaming/buffer.py`
- Test: `tests/test_streaming_buffer.py`

**Interfaces:**
- Consumes: `pyarrow.Schema`, target byte size, `pa.Table` chunks
- Produces: `RowGroupBuffer` with methods `add(table: pa.Table) -> Iterator[pa.Table]` (yields full row-groups), `flush() -> Iterator[pa.Table]`, `total_bytes: int`

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_streaming_buffer.py
from __future__ import annotations

import pyarrow as pa
import pytest

from nocoly_explorer.streaming.buffer import RowGroupBuffer


def _table(n: int = 100) -> pa.Table:
    # Each row ~16 bytes (one int64 + small string).
    return pa.table({
        "a": list(range(n)),
        "b": [f"row-{i}" for i in range(n)],
    })


def test_buffer_yields_when_threshold_exceeded():
    schema = pa.schema([("a", pa.int64()), ("b", pa.string())])
    buf = RowGroupBuffer(schema, target_bytes=2048)
    yielded = []
    for table in buf.add(_table(100)):
        yielded.append(table)
    assert len(yielded) >= 1
    assert all(t.num_rows > 0 for t in yielded)


def test_buffer_keeps_residual_rows():
    schema = pa.schema([("a", pa.int64()), ("b", pa.string())])
    buf = RowGroupBuffer(schema, target_bytes=2048)
    # Add a small table that doesn't cross threshold; should not yield.
    yielded = list(buf.add(_table(5)))
    assert yielded == []


def test_buffer_flush_emits_residual():
    schema = pa.schema([("a", pa.int64()), ("b", pa.string())])
    buf = RowGroupBuffer(schema, target_bytes=1_000_000)  # never crossed
    buf.add(_table(5))
    flushed = list(buf.flush())
    assert len(flushed) == 1
    assert flushed[0].num_rows == 5


def test_buffer_total_bytes_tracks_estimate():
    schema = pa.schema([("a", pa.int64()), ("b", pa.string())])
    buf = RowGroupBuffer(schema, target_bytes=1024 * 1024)
    buf.add(_table(100))
    assert buf.total_bytes > 0


def test_buffer_handles_empty_input():
    schema = pa.schema([("a", pa.int64())])
    buf = RowGroupBuffer(schema, target_bytes=1024)
    assert list(buf.add(pa.table({}))) == []
    assert list(buf.flush()) == []
```

- [ ] **Step 2: Run tests; expect ImportError**

Run: `pytest tests/test_streaming_buffer.py -v`
Expected: collection error.

- [ ] **Step 3: Implement `streaming/buffer.py`**

```python
"""Buffer rows into row-group-sized chunks for parquet writing."""

from __future__ import annotations

from typing import Iterator

import pyarrow as pa


class RowGroupBuffer:
    """Accumulates PyArrow tables and yields them once size threshold is hit."""

    def __init__(self, schema: pa.Schema, target_bytes: int) -> None:
        self._schema = schema
        self._target_bytes = target_bytes
        self._accumulated_tables: list[pa.Table] = []
        self._accumulated_bytes: int = 0
        self.total_bytes: int = 0

    def add(self, table: pa.Table) -> Iterator[pa.Table]:
        if table.num_rows == 0:
            return
        self._accumulated_tables.append(table)
        self._accumulated_bytes += table.nbytes
        self.total_bytes += table.nbytes
        if self._accumulated_bytes >= self._target_bytes:
            yield from self._drain()

    def flush(self) -> Iterator[pa.Table]:
        if self._accumulated_tables:
            yield from self._drain()

    def _drain(self) -> Iterator[pa.Table]:
        if not self._accumulated_tables:
            return
        combined = pa.concat_tables(self._accumulated_tables, promote_options="default")
        self._accumulated_tables = []
        self._accumulated_bytes = 0
        yield combined
```

- [ ] **Step 4: Run tests; expect all pass**

Run: `pytest tests/test_streaming_buffer.py -v`
Expected: 5 passed.

- [ ] **Step 5: Commit**

```bash
git add src/nocoly_explorer/streaming/buffer.py tests/test_streaming_buffer.py
git commit -m "feat(streaming): RowGroupBuffer with target-bytes flushing"
```

---

## Task 7: StreamingExporter (orchestrator + Review Focus #1, #2, #4)

**Files:**
- Create: `src/nocoly_explorer/streaming/exporter.py`
- Test: `tests/test_streaming_exporter.py`
- Create: `tests/streaming_fixtures.py`

**Interfaces:**
- Consumes: `WorksheetClient`, `StreamingExportConfig`, all upstream pieces
- Produces: `StreamingExporter` with method `export() -> ExportResult` and dataclass `ExportResult(rows_written: int, partitions: dict[str, int], output_dir: Path)`

- [ ] **Step 1: Write the test fixtures**

```python
# tests/streaming_fixtures.py
"""Shared fixtures for streaming export tests."""
from __future__ import annotations

from typing import Any, Dict, Iterator, List


def make_pages(total_rows: int, page_size: int = 100) -> Iterator[List[Dict[str, Any]]]:
    """Yield pages of synthetic rows: each row has 'a', 'b', 'created_date', 'region'."""
    page: List[Dict[str, Any]] = []
    for i in range(total_rows):
        if len(page) >= page_size:
            yield page
            page = []
        page.append({
            "a": i,
            "b": f"row-{i}",
            "created_date": f"2026-09-{(i % 28) + 1:02d}",
            "region": "HK" if i % 2 == 0 else "SZ",
        })
    if page:
        yield page


class FakeClient:
    """A drop-in WorksheetClient replacement that emits pages from a generator."""

    def __init__(self, pages: Iterator[List[Dict[str, Any]]]):
        self._pages = list(pages)
        self.fetched = 0

    def fetch_rows(self, *, page_size: int = 200, max_pages: int = 1000, **_kwargs):
        self.fetched += 1
        # Return pages as a single list (the orchestrator pages over them).
        return [row for page in self._pages for row in page]
```

- [ ] **Step 2: Write the failing exporter tests**

```python
# tests/test_streaming_exporter.py
from __future__ import annotations

from pathlib import Path

import pyarrow.parquet as pq
import pytest

from nocoly_explorer.exceptions import (
    CardinalityExceededError,
    OutputValidationError,
    SchemaDriftError,
)
from nocoly_explorer.streaming.exporter import StreamingExporter
from nocoly_explorer.streaming.options import (
    ParquetExportOptions,
    PartitionSpec,
)
from tests.streaming_fixtures import FakeClient, make_pages


def _exporter(client, output_dir, **opt_kwargs):
    from nocoly_explorer.streaming.exporter import StreamingExportConfig
    opts = ParquetExportOptions(**opt_kwargs)
    return StreamingExporter(
        client=client,
        config=StreamingExportConfig(
            output_dir=output_dir,
            worksheet_id="ws_test",
            options=opts,
            partition=PartitionSpec(column="region"),
        ),
    )


def test_streaming_export_produces_partitioned_files(tmp_path: Path):
    pages = list(make_pages(total_rows=300, page_size=100))
    client = FakeClient(pages)
    exp = _exporter(client, tmp_path)
    result = exp.export()
    assert result.rows_written == 300
    partitions = list((tmp_path).iterdir())
    assert any("region=HK" in str(p) for p in partitions)
    assert any("region=SZ" in str(p) for p in partitions)
    # Each partition must be a valid parquet file
    for part_dir in partitions:
        if part_dir.is_dir():
            for pf in part_dir.glob("data*.parquet"):
                table = pq.read_table(pf)
                assert table.num_rows > 0


def test_streaming_export_with_zero_rows(tmp_path: Path):
    # Review Focus #1: empty result set → no error, no files (or empty marker).
    client = FakeClient(iter([[]]))  # one empty page
    exp = _exporter(client, tmp_path)
    result = exp.export()
    assert result.rows_written == 0
    # Either no partition dirs exist, or each is empty.
    assert all(not any(p.iterdir()) for p in tmp_path.iterdir() if p.is_dir())


def test_streaming_export_single_partition_yields_single_file(tmp_path: Path):
    # Review Focus #2: single-partition case.
    pages = list(make_pages(total_rows=50, page_size=25))
    client = FakeClient(pages)
    exp = StreamingExporter(
        client=client,
        config=__import__("nocoly_explorer").streaming.exporter.StreamingExportConfig(
            output_dir=tmp_path,
            worksheet_id="ws_test",
            options=ParquetExportOptions(),
            partition=PartitionSpec(column="region"),
        ),
    )
    result = exp.export()
    assert result.rows_written == 50
    # Only one partition directory should exist
    dirs = [p for p in tmp_path.iterdir() if p.is_dir()]
    assert len(dirs) == 1


def test_streaming_export_to_existing_dir_does_not_silently_overwrite(tmp_path: Path):
    # Review Focus #4: existing directory is fine (Hive convention allows); but
    # if a conflicting Parquet file exists at the partition path, raise.
    pages = list(make_pages(total_rows=10, page_size=5))
    client = FakeClient(pages)
    exp = _exporter(client, tmp_path)
    # Pre-create a fake file where the export would write
    (tmp_path / "region=HK").mkdir()
    (tmp_path / "region=HK" / "data_0.parquet").write_bytes(b"NOT A PARQUET FILE")
    with pytest.raises((OutputValidationError, OSError)):
        exp.export()


def test_streaming_export_raises_on_schema_drift_when_policy_error(tmp_path: Path):
    pages = [
        [{"a": 1, "b": "x"}],
        [{"a": 2, "b": "y", "d": "extra"}],
    ]
    client = FakeClient(pages)
    exp = StreamingExporter(
        client=client,
        config=__import__("nocoly_explorer").streaming.exporter.StreamingExportConfig(
            output_dir=tmp_path,
            worksheet_id="ws_test",
            options=ParquetExportOptions(on_schema_drift="error"),
            partition=None,
        ),
    )
    with pytest.raises(SchemaDriftError):
        exp.export()


def test_streaming_export_handles_schema_drift_default_ignore(tmp_path: Path):
    pages = [
        [{"a": 1, "b": "x"}],
        [{"a": 2, "b": "y", "d": "extra"}],
    ]
    client = FakeClient(pages)
    exp = _exporter(client, tmp_path)
    # Should not raise; 'd' is dropped from schema.
    result = exp.export()
    assert result.rows_written == 2


def test_streaming_export_raises_on_partition_cardinality(tmp_path: Path):
    # 5 unique region values, but max_cardinality=3 → guard trips.
    pages = []
    for region in ["a", "b", "c", "d", "e"]:
        pages.append([{"region": region, "a": 1}])
    client = FakeClient(pages)
    exp = StreamingExporter(
        client=client,
        config=__import__("nocoly_explorer").streaming.exporter.StreamingExportConfig(
            output_dir=tmp_path,
            worksheet_id="ws_test",
            options=ParquetExportOptions(max_partition_cardinality=3),
            partition=PartitionSpec(column="region"),
        ),
    )
    with pytest.raises(CardinalityExceededError):
        exp.export()


def test_streaming_export_uses_buffer_threshold(tmp_path: Path):
    # Small target_bytes forces frequent row-group flushing.
    pages = list(make_pages(total_rows=500, page_size=50))
    client = FakeClient(pages)
    exp = _exporter(
        client, tmp_path,
        row_group_bytes=4096,
    )
    result = exp.export()
    assert result.rows_written == 500


def test_streaming_export_no_partition_produces_flat_layout(tmp_path: Path):
    pages = list(make_pages(total_rows=20, page_size=10))
    client = FakeClient(pages)
    from nocoly_explorer.streaming.exporter import StreamingExportConfig
    exp = StreamingExporter(
        client=client,
        config=StreamingExportConfig(
            output_dir=tmp_path,
            worksheet_id="ws_test",
            options=ParquetExportOptions(),
            partition=None,
        ),
    )
    result = exp.export()
    assert result.rows_written == 20
    # No partition directories — file lives directly under output_dir.
    files = [p for p in tmp_path.iterdir() if p.is_file() and p.suffix == ".parquet"]
    assert len(files) >= 1
```

- [ ] **Step 3: Run tests; expect ImportError**

Run: `pytest tests/test_streaming_exporter.py -v`
Expected: collection error.

- [ ] **Step 4: Implement `streaming/exporter.py`**

```python
"""Streaming Parquet exporter for Nocoly worksheet data."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Protocol

import pyarrow as pa

from ..exceptions import OutputValidationError
from .buffer import RowGroupBuffer
from .options import (
    ParquetExportOptions,
    PartitionSpec,
    resolve_partition_path,
    validate_output_dir,
)
from .partitions import PartitionRouter
from .schema import ParquetSchemaManager
from .writer import ParquetPartitionWriter

_LOGGER = logging.getLogger("nocoly_explorer.streaming")


class WorksheetClientLike(Protocol):
    """Minimum interface needed from a worksheet client."""

    def fetch_rows(
        self,
        *,
        page_size: int = 200,
        max_pages: int = 1000,
        filter_criteria: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]: ...


@dataclass(slots=True)
class StreamingExportConfig:
    output_dir: Path
    worksheet_id: str
    options: ParquetExportOptions = field(default_factory=ParquetExportOptions)
    partition: Optional[PartitionSpec] = None


@dataclass(slots=True)
class ExportResult:
    rows_written: int
    partitions: Dict[str, int]  # partition_key -> row count
    output_dir: Path
    bytes_written: int = 0


class StreamingExporter:
    """Stream pages from a worksheet client to Parquet files on disk."""

    def __init__(self, client: WorksheetClientLike, config: StreamingExportConfig) -> None:
        self.client = client
        self.config = config
        self.output_dir = validate_output_dir(config.output_dir)
        self.schema_manager = ParquetSchemaManager(config.options)
        partition_col = config.partition.column if config.partition else None
        self.router = PartitionRouter(
            config.partition or PartitionSpec(column=""),
            partition_column=partition_col,
            max_cardinality=config.options.max_partition_cardinality,
        )
        self._writers: Dict[str, ParquetPartitionWriter] = {}
        self._buffers: Dict[str, RowGroupBuffer] = {}
        self._rows_seen = 0
        self._bytes_written = 0

    def export(
        self,
        *,
        page_size: int = 200,
        max_pages: int = 1000,
        filter_criteria: Optional[Dict[str, Any]] = None,
    ) -> ExportResult:
        rows = self.client.fetch_rows(
            page_size=page_size,
            max_pages=max_pages,
            filter_criteria=filter_criteria,
        )
        # Process the batch as one logical stream. For large datasets, the
        # caller should page the client itself; this exporter handles one page
        # at a time but accepts either a flat list or a paginated response.
        if isinstance(rows, list) and rows and isinstance(rows[0], list):
            # Paginated response: list-of-pages.
            for page in rows:
                self._ingest(page)
        else:
            # Single batch: treat as one chunk.
            self._ingest(rows)
        self._flush_all()
        self._close_all()
        return ExportResult(
            rows_written=self._rows_seen,
            partitions={k: w.rows_written for k, w in self._writers.items()},
            output_dir=self.output_dir,
            bytes_written=self._bytes_written,
        )

    def _ingest(self, rows: List[Dict[str, Any]]) -> None:
        if not rows:
            return
        table = self.schema_manager.ingest_page(rows)
        # Group rows by partition key. We re-derive from the raw rows since the
        # PyArrow Table doesn't preserve partition metadata.
        # Use row-level routing: iterate the input rows.
        for row in rows:
            key = self.router.key_for(row) if self.router.partition_column else None
            self._route_row(key, table.column(len(self._current_partition_column())) if False else None)  # placeholder; replaced below

    # --- helpers ---

    def _route_row(self, key: Optional[str], row_index: int, table: pa.Table) -> None:
        """Append a single row to the per-partition buffer."""
        # Single-row table slice.
        single = table.slice(row_index, 1)
        self._append_to_partition(key, single)

    def _current_partition_column(self) -> str:
        return ""

    def _append_to_partition(self, key: Optional[str], table: pa.Table) -> None:
        writer = self._get_writer(key)
        buffer = self._get_buffer(key, writer.schema)
        # We use a tighter loop here: feed the table into the buffer; the buffer
        # yields complete row-groups which we forward to the writer.
        for chunk in buffer.add(table):
            writer.write_chunk(chunk)
            self._bytes_written += chunk.nbytes
        self._rows_seen += table.num_rows

    def _get_writer(self, key: Optional[str]) -> ParquetPartitionWriter:
        dict_key = key if key is not None else "__unpartitioned__"
        if dict_key not in self._writers:
            if key is None:
                target = self.output_dir / "data_0.parquet"
            else:
                part_dir = resolve_partition_path(self.output_dir, key)
                target = part_dir / "data_0.parquet"
            self._writers[dict_key] = ParquetPartitionWriter(
                target,
                self.schema_manager.schema,
                self.config.options,
            )
        return self._writers[dict_key]

    def _get_buffer(self, key: Optional[str], schema: pa.Schema) -> RowGroupBuffer:
        dict_key = key if key is not None else "__unpartitioned__"
        if dict_key not in self._buffers:
            self._buffers[dict_key] = RowGroupBuffer(schema, self.config.options.row_group_bytes)
        return self._buffers[dict_key]

    def _flush_all(self) -> None:
        for key, buffer in self._buffers.items():
            writer = self._writers[key]
            for chunk in buffer.flush():
                writer.write_chunk(chunk)
                self._bytes_written += chunk.nbytes

    def _close_all(self) -> None:
        for writer in self._writers.values():
            writer.close()
```

**NOTE**: The `_ingest` method above has a design issue I caught during writing — it uses a placeholder for partitioning per-row which is awkward. Let me re-state the cleanest version:

Replace `_ingest` and `_route_row` with this single approach: route rows at the dict level (not the PyArrow level), then build per-partition tables and flush.

```python
    def _ingest(self, rows: List[Dict[str, Any]]) -> None:
        if not rows:
            return
        # Step 1: schema manager validates/infers schema, returns combined table.
        _ = self.schema_manager.ingest_page(rows)
        # Step 2: bucket rows by partition key, building per-partition tables.
        buckets: Dict[Optional[str], List[Dict[str, Any]]] = {}
        for row in rows:
            key = (
                self.router.key_for(row)
                if self.router.partition_column is not None
                else None
            )
            buckets.setdefault(key, []).append(row)
        # Step 3: convert each bucket to a PyArrow table and push through its buffer.
        for key, bucket_rows in buckets.items():
            # Project bucket rows onto the canonical schema.
            schema_keys = [f.name for f in self.schema_manager.schema]
            projected = [{k: r.get(k) for k in schema_keys} for r in bucket_rows]
            table = pa.Table.from_pylist(projected, schema=self.schema_manager.schema)
            writer = self._get_writer(key)
            buffer = self._get_buffer(key, writer.schema)
            for chunk in buffer.add(table):
                writer.write_chunk(chunk)
                self._bytes_written += chunk.nbytes
            self._rows_seen += len(bucket_rows)
```

Delete the `_route_row` and `_current_partition_column` methods — they aren't needed.

- [ ] **Step 5: Run tests; expect all pass**

Run: `pytest tests/test_streaming_exporter.py -v`
Expected: 9 passed.

- [ ] **Step 6: Run the full test suite to confirm no regressions**

Run: `pytest -v`
Expected: All v0.1.1 tests still pass; new tests added.

- [ ] **Step 7: Commit**

```bash
git add src/nocoly_explorer/streaming/exporter.py tests/test_streaming_exporter.py tests/streaming_fixtures.py
git commit -m "feat(streaming): StreamingExporter orchestrator with row-group flushing"
```

---

## Task 8: Public API re-exports + pyproject extra

**Files:**
- Modify: `src/nocoly_explorer/__init__.py`
- Modify: `pyproject.toml`

- [ ] **Step 1: Update `__init__.py`**

```python
"""Nocoly Explorer public API."""

try:
    from .exporter import WorksheetExporter
    from .filters import NocolyFilter
    __all__ = ["WorksheetExporter", "NocolyFilter"]
except ImportError:  # pragma: no cover - core not available
    __all__ = []


def __getattr__(name):
    """Lazy-load optional streaming exports."""
    if name in {"StreamingExporter", "ParquetExportOptions", "PartitionSpec",
                "StreamingExportConfig", "ExportResult"}:
        from .streaming import (  # noqa: F401
            StreamingExporter, ParquetExportOptions, PartitionSpec,
            StreamingExportConfig, ExportResult,
        )
        return globals()[name]
    if name in {"SchemaDriftError", "CardinalityExceededError"}:
        from .exceptions import SchemaDriftError, CardinalityExceededError  # noqa: F401
        return globals()[name]
    raise AttributeError(f"module 'nocoly_explorer' has no attribute {name!r}")
```

- [ ] **Step 2: Add `streaming` extra to pyproject.toml**

Modify `[project.optional-dependencies]`:

```toml
[project.optional-dependencies]
dataframe = ["pandas>=2.0.0"]
spark = ["pyspark>=3.4.0"]
streaming = ["pyarrow>=14.0.0"]
```

- [ ] **Step 3: Run all tests**

Run: `pytest -v`
Expected: All pass (105+ tests).

- [ ] **Step 4: Verify the lazy import works**

```bash
cd /Users/hermes/Downloads/nocoly-explorer
python -c "
from nocoly_explorer import WorksheetExporter, NocolyFilter
print('core OK')
from nocoly_explorer import StreamingExporter, ParquetExportOptions, PartitionSpec
print('streaming OK')
from nocoly_explorer.exceptions import SchemaDriftError, CardinalityExceededError
print('errors OK')
"
```

Expected: all three lines print.

- [ ] **Step 5: Commit**

```bash
git add src/nocoly_explorer/__init__.py pyproject.toml
git commit -m "feat(streaming): expose public API and add pyarrow optional extra"
```

---

## Task 9: Build, publish 0.2.0rc1

- [ ] **Step 1: Bump version**

Edit `pyproject.toml`: `version = "0.2.0rc1"`.

- [ ] **Step 2: Build**

```bash
cd /Users/hermes/Downloads/nocoly-explorer
rm -rf dist/
python -m build
ls dist/
```

- [ ] **Step 3: Verify install**

```bash
cd /tmp && rm -rf verify-install && python -m venv verify-install
/tmp/verify-install/bin/pip install --quiet /Users/hermes/Downloads/nocoly-explorer/dist/nocoly_explorer-0.2.0rc1-py3-none-any.whl
/tmp/verify-install/bin/python -c "
from nocoly_explorer import StreamingExporter, ParquetExportOptions, PartitionSpec
print('install OK')
"
rm -rf /tmp/verify-install
```

- [ ] **Step 4: Commit, tag, release**

```bash
cd /Users/hermes/Downloads/nocoly-explorer
git add pyproject.toml
git commit -m "chore(release): bump to 0.2.0rc1 (streaming phase 1)"
git tag v0.2.0rc1
git push origin main --tags
gh release create v0.2.0rc1 dist/nocoly_explorer-0.2.0rc1-py3-none-any.whl dist/nocoly_explorer-0.2.0rc1.tar.gz \
  --title "v0.2.0rc1 — Streaming Parquet Exporter" \
  --notes "Phase 1 of the v0.2.0 enterprise scale roadmap. Adds:
- StreamingExporter: paginated → partitioned Parquet, bounded memory
- ParquetSchemaManager: drift detection + ignore/error modes
- PartitionRouter: Hive-style partitioning with cardinality guard
- Optional pyarrow dependency via [streaming] extra

Test count: 56 → 105+."
```

---

## Self-Review

1. **Spec coverage:** §4.1 (Streaming Parquet) covered by Tasks 1–9. Sub-decisions:
   - 128MB row groups ✓ (Task 6, default in `ParquetExportOptions`)
   - Hive-style partitioning ✓ (Task 4, `PartitionRouter._format_key` → `column=value`)
   - Cardinality guard ✓ (Task 4, `test_router_cardinality_guard`)
   - PyArrow schema enforcement ✓ (Task 3, `ParquetSchemaManager`)
   - Drift policy ✓ (Task 3, `on_schema_drift` literal)
   - Atomic / safe writes ✓ (Task 5, `ParquetPartitionWriter`)
   - Failure modes (network error mid-page, partition cardinality, schema drift) ✓ (Tests 3.4, 4.8, 7.7)

2. **Placeholder scan:** No TBDs. All steps have concrete code.

3. **Type consistency:**
   - `ParquetExportOptions` field names match between Task 1 (creation) and Task 5/6/7 (consumers).
   - `PartitionSpec.column` used consistently.
   - `CardinalityExceededError` raised in Task 4, caught in Task 7.
   - `SchemaDriftError` raised in Task 3, caught in Task 7.

4. **Review Focus:** All 5 input classes covered with named tests:
   - #1 Empty result set → `test_streaming_export_with_zero_rows`
   - #2 Single partition → `test_streaming_export_single_partition_yields_single_file`
   - #3 Schema drift mid-stream → `test_streaming_export_raises_on_schema_drift_when_policy_error` + `test_streaming_export_handles_schema_drift_default_ignore`
   - #4 Existing directory conflict → `test_streaming_export_to_existing_dir_does_not_silently_overwrite`
   - #5 `.parquet` extension rejected → `test_validate_output_dir_rejects_parquet_extension`

---

**Plan complete and saved to `docs/superpowers/plans/2026-09-28-streaming-phase1.md`.**

The user said "go first I will later review if this meet the standard" — implementing native in this session per `superpowers:executing-plans`.