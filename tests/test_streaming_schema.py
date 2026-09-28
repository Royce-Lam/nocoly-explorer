"""Tests for ParquetSchemaManager."""

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
    assert set(mgr.schema.names) == {"a", "b"}
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
    assert "d" not in mgr.schema.names
    assert mgr.drift_seen is True
    assert set(table.column_names) == {"a", "b"}


def test_extra_column_raises_when_policy_is_error():
    mgr = ParquetSchemaManager(ParquetExportOptions(on_schema_drift="error"))
    mgr.ingest_page([{"a": 1, "b": "x"}])
    with pytest.raises(SchemaDriftError):
        mgr.ingest_page([{"a": 2, "b": "y", "d": "extra"}])


def test_missing_column_in_subsequent_page_filled_with_null():
    mgr = ParquetSchemaManager(ParquetExportOptions())
    mgr.ingest_page([{"a": 1, "b": "x", "c": 9}])
    table = mgr.ingest_page([{"a": 2, "b": "y"}])
    assert set(mgr.schema.names) == {"a", "b", "c"}
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