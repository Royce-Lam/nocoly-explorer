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


def _project_rows_to_schema(
    rows: Sequence[Dict[str, Any]], schema: pa.Schema
) -> List[Dict[str, Any]]:
    """Project rows onto a schema: drop unknown fields, fill missing with None."""
    schema_keys = [f.name for f in schema]
    projected = [{k: row.get(k) for k in schema_keys} for row in rows]
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
            if self._schema is not None:
                return self._schema.empty_table()
            return pa.table({})

        if self._schema is None:
            if self._explicit_schema is not None:
                self._schema = self._explicit_schema
            else:
                self._schema = _infer_schema_from_rows(rows)

        row_keys: set = set()
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