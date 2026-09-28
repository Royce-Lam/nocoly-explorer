"""Tests for streaming options and partition spec."""

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