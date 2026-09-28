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