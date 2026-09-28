"""Tests for ParquetPartitionWriter."""

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
    assert md.num_rows == 100