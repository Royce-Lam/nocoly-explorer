"""Tests for OutputFormatter behaviors."""

from __future__ import annotations

from pathlib import Path

import pytest

from nocoly_explorer.exceptions import OutputValidationError
from nocoly_explorer.output import OutputFormatter


def test_file_output_requires_path(tmp_path: Path):
    formatter = OutputFormatter(runtime_mode="standard")
    with pytest.raises(OutputValidationError):
        formatter.format(rows=[], output_type="file")


def test_csv_output_returns_string():
    formatter = OutputFormatter(runtime_mode="standard")
    rows = [{"Name": "Alice", "Status": "Active"}]

    csv_text = formatter.format(rows=rows, output_type="csv")

    assert "Name,Status" in csv_text
    assert "Alice,Active" in csv_text


def test_dataframe_output_uses_pandas(monkeypatch):
    formatter = OutputFormatter(runtime_mode="standard")
    rows = [{"Name": "Bob"}]

    class DummyPandas:
        def __init__(self):
            self.payload = None

        def DataFrame(self, payload):
            self.payload = payload
            return {"called_with": payload}

    dummy_pd = DummyPandas()
    monkeypatch.setattr("nocoly_explorer.output.pd", dummy_pd)

    result = formatter.format(rows=rows, output_type="dataframe")

    assert result == {"called_with": rows}
    assert dummy_pd.payload == rows


def test_file_export_json(tmp_path: Path):
    formatter = OutputFormatter(runtime_mode="standard")
    rows = [{"Name": "Eve"}]
    target = tmp_path / "out.json"

    path = formatter.format(
        rows=rows,
        output_type="file",
        file_path=str(target),
        file_format="json",
    )

    assert Path(path).exists()
    assert target.read_text(encoding="utf-8").strip().startswith("[")


def test_csv_output_handles_mismatched_keys():
    """Rows with different keys must not crash (I1)."""
    formatter = OutputFormatter(runtime_mode="standard")
    rows = [{"a": 1, "b": 2}, {"a": 3, "c": 4}]
    csv_text = formatter.format(rows=rows, output_type="csv")
    # union of keys (a, b, c) — first-row order preserved
    assert "a,b,c" in csv_text
    assert "1,2," in csv_text
    assert "3,,4" in csv_text


def test_csv_output_preserves_column_order_from_first_row():
    formatter = OutputFormatter(runtime_mode="standard")
    rows = [{"z": 1, "a": 2, "m": 3}, {"z": 4, "a": 5, "m": 6}]
    csv_text = formatter.format(rows=rows, output_type="csv")
    # fieldnames header must be in first-row order
    assert csv_text.splitlines()[0] == "z,a,m"


def test_csv_output_renders_none_and_missing_keys_consistently():
    """None and missing keys both render as empty string (I4)."""
    formatter = OutputFormatter(runtime_mode="standard")
    rows = [{"a": 1, "b": None}, {"a": 2}]  # second row missing 'b'
    csv_text = formatter.format(rows=rows, output_type="csv")
    lines = csv_text.splitlines()
    assert lines[0] == "a,b"
    assert lines[1] == "1,"  # None → empty
    assert lines[2] == "2,"  # missing key → empty (consistent)


def test_string_output_handles_datetime_and_decimal():
    """Datetime/Decimal values must serialize to JSON (I2)."""
    import datetime
    import decimal
    formatter = OutputFormatter(runtime_mode="standard")
    rows = [
        {"date": datetime.datetime(2025, 1, 1), "amount": decimal.Decimal("10.50")},
    ]
    text = formatter.format(rows=rows, output_type="string")
    assert "2025-01-01" in text
    assert "10.5" in text


def test_json_output_handles_datetime_and_decimal():
    """`json` output returns a list of dicts (raw passthrough); values are not
    coerced — callers serialize themselves. Datetime/Decimal coverage belongs on
    the `string` output path (test above)."""
    formatter = OutputFormatter(runtime_mode="standard")
    import datetime
    rows = [{"date": datetime.datetime(2025, 1, 1)}]
    result = formatter.format(rows=rows, output_type="json")
    assert isinstance(result, list)
    assert result[0]["date"] is rows[0]["date"]  # passthrough reference


def test_parquet_export_missing_pyarrow_raises_clear_error(tmp_path, monkeypatch):
    """Missing pyarrow should give an actionable error message (M9)."""
    formatter = OutputFormatter(runtime_mode="standard")
    target = tmp_path / "out.parquet"

    # Simulate pandas without pyarrow engine
    class FakeDf:
        def to_parquet(self, path, index):
            raise ImportError("Missing optional dependency 'pyarrow'.")

    class FakePandas:
        def DataFrame(self, rows):
            return FakeDf()

    monkeypatch.setattr("nocoly_explorer.output.pd", FakePandas())

    with pytest.raises(OutputValidationError) as excinfo:
        formatter.format(
            rows=[{"a": 1}], output_type="file",
            file_path=str(target), file_format="parquet",
        )
    assert "pyarrow" in str(excinfo.value).lower() or "fastparquet" in str(excinfo.value).lower()


def test_file_output_is_atomic(tmp_path, monkeypatch):
    """File writes must go through tmp + replace to avoid partial writes (I7)."""
    import os as _os
    formatter = OutputFormatter(runtime_mode="standard")
    target = tmp_path / "out.json"
    replaced = []

    real_replace = _os.replace

    def tracked_replace(src, dst):
        replaced.append((src, dst))
        return real_replace(src, dst)

    monkeypatch.setattr("nocoly_explorer.output.os.replace", tracked_replace)

    formatter.format(
        rows=[{"a": 1}], output_type="file",
        file_path=str(target), file_format="json",
    )
    assert len(replaced) == 1, "file output should atomic-replace once"
    src, dst = replaced[0]
    assert str(dst) == str(target)


def test_file_format_mismatch_with_extension_logs_warning(tmp_path, caplog):
    """file_format='json' + .csv path emits a warning (I8)."""
    import logging
    formatter = OutputFormatter(runtime_mode="standard")
    target = tmp_path / "data.csv"
    with caplog.at_level(logging.WARNING):
        formatter.format(
            rows=[{"a": 1}], output_type="file",
            file_path=str(target), file_format="json",
        )
    assert any("format" in r.message.lower() or "extension" in r.message.lower()
               for r in caplog.records)
