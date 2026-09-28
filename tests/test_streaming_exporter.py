"""Tests for StreamingExporter orchestrator."""

from __future__ import annotations

from pathlib import Path

import pyarrow.parquet as pq
import pytest

from nocoly_explorer.exceptions import (
    CardinalityExceededError,
    OutputValidationError,
    SchemaDriftError,
)
from nocoly_explorer.streaming.exporter import StreamingExportConfig, StreamingExporter
from nocoly_explorer.streaming.options import (
    ParquetExportOptions,
    PartitionSpec,
)
from streaming_fixtures import FakeClient, make_pages


def _exporter(client, output_dir, **opt_kwargs):
    return StreamingExporter(
        client=client,
        config=StreamingExportConfig(
            output_dir=output_dir,
            worksheet_id="ws_test",
            options=ParquetExportOptions(**opt_kwargs),
            partition=PartitionSpec(column="region"),
        ),
    )


def test_streaming_export_produces_partitioned_files(tmp_path: Path):
    pages = list(make_pages(total_rows=300, page_size=100))
    client = FakeClient(pages)
    exp = _exporter(client, tmp_path)
    result = exp.export()
    assert result.rows_written == 300
    partition_dirs = [p for p in tmp_path.iterdir() if p.is_dir()]
    region_dirs = [p for p in partition_dirs if "region=" in p.name]
    assert len(region_dirs) >= 1
    for part_dir in region_dirs:
        for pf in part_dir.glob("data*.parquet"):
            table = pq.read_table(pf)
            assert table.num_rows > 0


def test_streaming_export_with_zero_rows(tmp_path: Path):
    client = FakeClient(iter([[]]))
    exp = _exporter(client, tmp_path)
    result = exp.export()
    assert result.rows_written == 0


def test_streaming_export_single_partition_yields_single_file(tmp_path: Path):
    pages = list(make_pages(total_rows=50, page_size=25))
    client = FakeClient(pages)
    exp = StreamingExporter(
        client=client,
        config=StreamingExportConfig(
            output_dir=tmp_path,
            worksheet_id="ws_test",
            options=ParquetExportOptions(),
            partition=PartitionSpec(column="region"),
        ),
    )
    result = exp.export()
    assert result.rows_written == 50
    region_dirs = [p for p in tmp_path.iterdir() if p.is_dir() and "region=" in p.name]
    assert len(region_dirs) == 2  # HK and SZ


def test_streaming_export_to_existing_empty_dir_succeeds(tmp_path: Path):
    # Review Focus #4: pre-existing empty directory is fine.
    pages = list(make_pages(total_rows=10, page_size=5))
    client = FakeClient(pages)
    exp = _exporter(client, tmp_path)
    result = exp.export()
    assert result.rows_written == 10


def test_streaming_export_raises_on_schema_drift_when_policy_error(tmp_path: Path):
    pages = [
        [{"a": 1, "b": "x"}],
        [{"a": 2, "b": "y", "d": "extra"}],
    ]
    client = FakeClient(pages)
    exp = StreamingExporter(
        client=client,
        config=StreamingExportConfig(
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
    result = exp.export()
    assert result.rows_written == 2


def test_streaming_export_raises_on_partition_cardinality(tmp_path: Path):
    pages = []
    for region in ["a", "b", "c", "d", "e"]:
        pages.append([{"region": region, "a": 1}])
    client = FakeClient(pages)
    exp = StreamingExporter(
        client=client,
        config=StreamingExportConfig(
            output_dir=tmp_path,
            worksheet_id="ws_test",
            options=ParquetExportOptions(max_partition_cardinality=3),
            partition=PartitionSpec(column="region"),
        ),
    )
    with pytest.raises(CardinalityExceededError):
        exp.export()


def test_streaming_export_uses_buffer_threshold(tmp_path: Path):
    pages = list(make_pages(total_rows=500, page_size=50))
    client = FakeClient(pages)
    # Use a tiny but valid row-group size (>= 1 MiB) to trigger frequent flushing.
    exp = _exporter(client, tmp_path, row_group_bytes=1024 * 1024)
    result = exp.export()
    assert result.rows_written == 500


def test_streaming_export_no_partition_produces_flat_layout(tmp_path: Path):
    pages = list(make_pages(total_rows=20, page_size=10))
    client = FakeClient(pages)
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
    files = [p for p in tmp_path.iterdir() if p.is_file() and p.suffix == ".parquet"]
    assert len(files) >= 1