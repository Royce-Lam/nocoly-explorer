"""Output formatting helpers."""

from __future__ import annotations

import csv
import datetime
import decimal
import json
import logging
import os
import uuid
from io import StringIO
from pathlib import Path
from typing import Any, Dict, List, Sequence

from .detector import RuntimeMode
from .exceptions import OutputValidationError

try:
    import pandas as pd  # type: ignore
except ModuleNotFoundError:  # pragma: no cover - optional dependency
    pd = None  # type: ignore

try:
    from pyspark.sql import SparkSession  # type: ignore
except ModuleNotFoundError:  # pragma: no cover - optional dependency
    SparkSession = None  # type: ignore

_LOGGER = logging.getLogger(__name__)


def _json_default(obj: Any) -> Any:
    """JSON encoder fallback for non-natively-serializable values."""
    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    if isinstance(obj, datetime.time):
        return obj.isoformat()
    if isinstance(obj, datetime.timedelta):
        return obj.total_seconds()
    if isinstance(obj, decimal.Decimal):
        return float(obj) if obj.is_finite() else str(obj)
    if isinstance(obj, uuid.UUID):
        return str(obj)
    if isinstance(obj, bytes):
        try:
            return obj.decode("utf-8")
        except UnicodeDecodeError:
            return obj.hex()
    if isinstance(obj, set):
        return sorted(obj, key=repr)
    if isinstance(obj, frozenset):
        return sorted(obj, key=repr)
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


def _json_dumps(obj: Any) -> str:
    return json.dumps(obj, indent=2, default=_json_default, ensure_ascii=False)


class OutputFormatter:
    """Convert raw worksheet rows into caller-requested shapes."""

    def __init__(self, runtime_mode: RuntimeMode):
        self.runtime_mode = runtime_mode
        self._logger = _LOGGER

    def format(
        self,
        rows: Sequence[Dict[str, Any]],
        output_type: str,
        file_path: str | None = None,
        file_format: str | None = None,
    ) -> Any:
        normalized = (output_type or "dataframe").lower()
        if normalized in {"dataframe", "pandas"}:
            return self._to_pandas(rows)
        if normalized in {"spark", "pyspark"}:
            return self._to_spark(rows)
        if normalized == "json":
            return list(rows)
        if normalized == "string":
            return _json_dumps(list(rows))
        if normalized == "csv":
            return self._to_csv_string(rows)
        if normalized == "file":
            return self._export_to_file(rows, file_path=file_path, file_format=file_format)
        raise OutputValidationError(f"Unsupported output_type: {output_type}")

    def _to_pandas(self, rows: Sequence[Dict[str, Any]]):
        if pd is None:
            raise OutputValidationError(
                "pandas is required for dataframe output. Install pandas>=2.0."
            )
        return pd.DataFrame(list(rows))

    def _to_spark(self, rows: Sequence[Dict[str, Any]]):
        if SparkSession is None:
            raise OutputValidationError(
                "PySpark is required for spark output. Install pyspark or run on Databricks."
            )
        spark = SparkSession.builder.getOrCreate()
        return spark.createDataFrame(list(rows))

    def _collect_fieldnames(self, rows: Sequence[Dict[str, Any]]) -> List[str]:
        """Union of all keys, preserving first-row order, then appending new keys."""
        seen: List[str] = []
        seen_set = set()
        for row in rows:
            for key in row.keys():
                if key not in seen_set:
                    seen.append(key)
                    seen_set.add(key)
        return seen

    def _to_csv_string(self, rows: Sequence[Dict[str, Any]]) -> str:
        if not rows:
            return ""
        buffer = StringIO()
        fieldnames = self._collect_fieldnames(rows)
        writer = csv.DictWriter(buffer, fieldnames=fieldnames, extrasaction="ignore", restval="")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
        return buffer.getvalue()

    def _export_to_file(
        self,
        rows: Sequence[Dict[str, Any]],
        file_path: str | None,
        file_format: str | None,
    ) -> str:
        if not file_path:
            raise OutputValidationError("file_path is required when output_type='file'.")
        path = Path(file_path)
        fmt = (file_format or path.suffix.lstrip(".") or "json").lower()
        path.parent.mkdir(parents=True, exist_ok=True)

        # Warn when explicit format conflicts with path extension (I8).
        if (
            file_format
            and path.suffix
            and file_format.lower() != path.suffix.lstrip(".").lower()
        ):
            self._logger.warning(
                "file_format=%r disagrees with path extension %r; using file_format.",
                file_format,
                path.suffix,
            )

        tmp_path = path.with_suffix(path.suffix + ".tmp")
        try:
            if fmt == "json":
                tmp_path.write_text(_json_dumps(list(rows)), encoding="utf-8")
            elif fmt == "csv":
                tmp_path.write_text(self._to_csv_string(rows), encoding="utf-8")
            elif fmt == "parquet":
                if pd is None:
                    raise OutputValidationError(
                        "pandas is required for parquet export — install pandas>=1.0."
                    )
                try:
                    df = pd.DataFrame(list(rows))
                    df.to_parquet(tmp_path, index=False)
                except (ImportError, ValueError) as exc:
                    # pandas re-raises missing engine as ImportError; some versions as ValueError.
                    raise OutputValidationError(
                        "parquet export requires a parquet engine — install pyarrow or fastparquet "
                        "(e.g. `pip install pyarrow`)."
                    ) from exc
            else:
                raise OutputValidationError(
                    f"Unsupported file_format '{fmt}'. Use json, csv, or parquet."
                )
            os.replace(tmp_path, path)
        except Exception:
            if tmp_path.exists():
                try:
                    tmp_path.unlink()
                except OSError:
                    pass
            raise
        return str(path)