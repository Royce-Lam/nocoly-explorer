"""Configuration objects for streaming Parquet exports."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Optional

_VALID_GRANULARITIES = ("day", "month", "year")
_VALID_DRIFT_POLICIES = ("ignore", "error")
_VALID_COMPRESSIONS = ("snappy", "gzip", "zstd", "lz4", "brotli", None)


@dataclass(frozen=True, slots=True)
class PartitionSpec:
    """How to partition the output Parquet dataset."""

    column: str
    granularity: Optional[Literal["day", "month", "year"]] = None

    def __post_init__(self) -> None:
        if not self.column:
            raise ValueError("PartitionSpec.column must be a non-empty string")
        if self.granularity is not None and self.granularity not in _VALID_GRANULARITIES:
            raise ValueError(
                f"PartitionSpec.granularity must be one of {_VALID_GRANULARITIES} or None; "
                f"got {self.granularity!r}"
            )


@dataclass(frozen=True, slots=True)
class ParquetExportOptions:
    """Tunables for the streaming exporter."""

    row_group_bytes: int = 128 * 1024 * 1024
    max_partition_cardinality: int = 10_000
    on_schema_drift: Literal["ignore", "error"] = "ignore"
    compression: Optional[str] = "snappy"
    write_statistics: bool = True

    def __post_init__(self) -> None:
        if self.row_group_bytes < 1024 * 1024:
            raise ValueError(
                f"row_group_bytes must be >= 1 MiB (got {self.row_group_bytes})"
            )
        if self.max_partition_cardinality < 1:
            raise ValueError(
                f"max_partition_cardinality must be >= 1 (got {self.max_partition_cardinality})"
            )
        if self.on_schema_drift not in _VALID_DRIFT_POLICIES:
            raise ValueError(
                f"on_schema_drift must be one of {_VALID_DRIFT_POLICIES}; "
                f"got {self.on_schema_drift!r}"
            )
        if self.compression not in _VALID_COMPRESSIONS:
            raise ValueError(
                f"compression must be one of {_VALID_COMPRESSIONS}; "
                f"got {self.compression!r}"
            )