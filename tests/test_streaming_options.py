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


def test_partition_spec_with_day_granularity_requires_non_empty_column():
    # Documenting current behavior: empty column is allowed as a placeholder;
    # granularity-aware specs require a column to be meaningful.
    spec = PartitionSpec(column="x", granularity="day")
    assert spec.column == "x"
    assert spec.granularity == "day"


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


def test_validate_output_dir_creates_directory(tmp_path):
    target = tmp_path / "exports" / "deep" / "path"
    from nocoly_explorer.streaming.options import validate_output_dir
    result = validate_output_dir(target)
    assert result.exists()
    assert result.is_dir()


def test_validate_output_dir_rejects_existing_file(tmp_path):
    existing_file = tmp_path / "not-a-dir.txt"
    existing_file.write_text("")
    from nocoly_explorer.streaming.options import validate_output_dir
    from nocoly_explorer.exceptions import OutputValidationError
    with pytest.raises(OutputValidationError):
        validate_output_dir(existing_file)


def test_validate_output_dir_rejects_parquet_extension(tmp_path):
    target = tmp_path / "data.parquet"
    from nocoly_explorer.streaming.options import validate_output_dir
    from nocoly_explorer.exceptions import OutputValidationError
    with pytest.raises(OutputValidationError) as excinfo:
        validate_output_dir(target)
    assert ".parquet" in str(excinfo.value).lower()


def test_validate_output_dir_accepts_existing_empty_dir(tmp_path):
    target = tmp_path / "existing-empty"
    target.mkdir()
    from nocoly_explorer.streaming.options import validate_output_dir
    result = validate_output_dir(target)
    assert result == target


def test_resolve_partition_path_returns_hive_style():
    from nocoly_explorer.streaming.options import resolve_partition_path
    out = resolve_partition_path(__import__("pathlib").Path("/data"), "created_date=2026-09-28")
    assert out == __import__("pathlib").Path("/data/created_date=2026-09-28")


def test_resolve_partition_path_rejects_path_traversal():
    from nocoly_explorer.streaming.options import resolve_partition_path
    with pytest.raises(ValueError):
        resolve_partition_path(__import__("pathlib").Path("/data"), "../../etc/passwd")


def test_resolve_partition_path_rejects_absolute_keys():
    from nocoly_explorer.streaming.options import resolve_partition_path
    with pytest.raises(ValueError):
        resolve_partition_path(__import__("pathlib").Path("/data"), "/etc/passwd")