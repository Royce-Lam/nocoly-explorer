"""Tests for RowGroupBuffer."""

from __future__ import annotations

import pyarrow as pa
import pytest

from nocoly_explorer.streaming.buffer import RowGroupBuffer


def _table(n: int = 100) -> pa.Table:
    return pa.table({
        "a": list(range(n)),
        "b": [f"row-{i}" for i in range(n)],
    })


def test_buffer_yields_when_threshold_exceeded():
    schema = pa.schema([("a", pa.int64()), ("b", pa.string())])
    buf = RowGroupBuffer(schema, target_bytes=512)
    yielded = []
    for table in buf.add(_table(500)):
        yielded.append(table)
    assert len(yielded) >= 1
    assert all(t.num_rows > 0 for t in yielded)


def test_buffer_keeps_residual_rows():
    schema = pa.schema([("a", pa.int64()), ("b", pa.string())])
    buf = RowGroupBuffer(schema, target_bytes=2048)
    # add() is a generator; consume with list() to trigger accumulation.
    yielded = list(buf.add(_table(5)))
    assert yielded == []


def test_buffer_flush_emits_residual():
    schema = pa.schema([("a", pa.int64()), ("b", pa.string())])
    buf = RowGroupBuffer(schema, target_bytes=1_000_000)
    list(buf.add(_table(5)))
    flushed = list(buf.flush())
    assert len(flushed) == 1
    assert flushed[0].num_rows == 5


def test_buffer_total_bytes_tracks_estimate():
    schema = pa.schema([("a", pa.int64()), ("b", pa.string())])
    buf = RowGroupBuffer(schema, target_bytes=1024 * 1024)
    # add() is a generator; bytes accumulate only when iterated (or when the
    # threshold is crossed). Force iteration with list().
    list(buf.add(_table(100)))
    assert buf.total_bytes > 0


def test_buffer_handles_empty_input():
    schema = pa.schema([("a", pa.int64())])
    buf = RowGroupBuffer(schema, target_bytes=1024)
    assert list(buf.add(pa.table({}))) == []
    assert list(buf.flush()) == []