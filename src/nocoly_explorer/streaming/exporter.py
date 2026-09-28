"""Streaming Parquet exporter for Nocoly worksheet data."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Protocol, Sequence

import pyarrow as pa

from ..exceptions import OutputValidationError
from .buffer import RowGroupBuffer
from .options import (
    ParquetExportOptions,
    PartitionSpec,
    resolve_partition_path,
    validate_output_dir,
)
from .partitions import PartitionRouter
from .schema import ParquetSchemaManager
from .writer import ParquetPartitionWriter

_LOGGER = logging.getLogger("nocoly_explorer.streaming")


class WorksheetClientLike(Protocol):
    """Minimum interface needed from a worksheet client."""

    def fetch_rows(
        self,
        *,
        page_size: int = 200,
        max_pages: int = 1000,
        filter_criteria: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]: ...


@dataclass(slots=True)
class StreamingExportConfig:
    output_dir: Path
    worksheet_id: str
    options: ParquetExportOptions = field(default_factory=ParquetExportOptions)
    partition: Optional[PartitionSpec] = None


@dataclass(slots=True)
class ExportResult:
    rows_written: int
    partitions: Dict[str, int]
    output_dir: Path
    bytes_written: int = 0


_UNPARTITIONED_KEY = "__unpartitioned__"


class StreamingExporter:
    """Stream pages from a worksheet client to Parquet files on disk."""

    def __init__(self, client: WorksheetClientLike, config: StreamingExportConfig) -> None:
        self.client = client
        self.config = config
        self.output_dir = validate_output_dir(config.output_dir)
        self.schema_manager = ParquetSchemaManager(config.options)
        if config.partition is not None:
            self.router = PartitionRouter(
                config.partition,
                partition_column=config.partition.column,
                max_cardinality=config.options.max_partition_cardinality,
            )
        else:
            self.router = PartitionRouter(
                PartitionSpec(column=""),  # placeholder; not used since partition_column=None
                partition_column=None,
                max_cardinality=config.options.max_partition_cardinality,
            )
        self._writers: Dict[str, ParquetPartitionWriter] = {}
        self._buffers: Dict[str, RowGroupBuffer] = {}
        self._rows_seen = 0
        self._bytes_written = 0

    def export(
        self,
        *,
        page_size: int = 200,
        max_pages: int = 1000,
        filter_criteria: Optional[Dict[str, Any]] = None,
    ) -> ExportResult:
        rows = self.client.fetch_rows(
            page_size=page_size,
            max_pages=max_pages,
            filter_criteria=filter_criteria,
        )
        # The client may return either a flat list or a list of pages.
        if isinstance(rows, list) and rows and isinstance(rows[0], list):
            for page in rows:
                self._ingest(page)
        else:
            self._ingest(rows)
        self._flush_all()
        self._close_all()
        return ExportResult(
            rows_written=self._rows_seen,
            partitions={k: w.rows_written for k, w in self._writers.items()},
            output_dir=self.output_dir,
            bytes_written=self._bytes_written,
        )

    def _ingest(self, rows: Sequence[Dict[str, Any]]) -> None:
        if not rows:
            return
        # Step 1: schema manager validates/infers schema.
        self.schema_manager.ingest_page(rows)
        # Step 2: bucket rows by partition key.
        buckets: Dict[Optional[str], List[Dict[str, Any]]] = {}
        for row in rows:
            key = (
                self.router.key_for(row)
                if self.router.partition_column is not None
                else None
            )
            buckets.setdefault(key, []).append(row)
        # Step 3: convert each bucket to a PyArrow table and push through buffer.
        schema = self.schema_manager.schema
        schema_keys = [f.name for f in schema]
        for key, bucket_rows in buckets.items():
            projected = [{k: r.get(k) for k in schema_keys} for r in bucket_rows]
            table = pa.Table.from_pylist(projected, schema=schema)
            writer = self._get_writer(key)
            buffer = self._get_buffer(key, writer.schema)
            for chunk in buffer.add(table):
                writer.write_chunk(chunk)
                self._bytes_written += chunk.nbytes
            self._rows_seen += len(bucket_rows)

    def _get_writer(self, key: Optional[str]) -> ParquetPartitionWriter:
        dict_key = key if key is not None else _UNPARTITIONED_KEY
        if dict_key not in self._writers:
            if key is None:
                target = self.output_dir / "data_0.parquet"
            else:
                part_dir = resolve_partition_path(self.output_dir, key)
                target = part_dir / "data_0.parquet"
            self._writers[dict_key] = ParquetPartitionWriter(
                target,
                self.schema_manager.schema,
                self.config.options,
            )
        return self._writers[dict_key]

    def _get_buffer(self, key: Optional[str], schema: pa.Schema) -> RowGroupBuffer:
        dict_key = key if key is not None else _UNPARTITIONED_KEY
        if dict_key not in self._buffers:
            self._buffers[dict_key] = RowGroupBuffer(schema, self.config.options.row_group_bytes)
        return self._buffers[dict_key]

    def _flush_all(self) -> None:
        for key, buffer in self._buffers.items():
            writer = self._writers[key]
            for chunk in buffer.flush():
                writer.write_chunk(chunk)
                self._bytes_written += chunk.nbytes

    def _close_all(self) -> None:
        for writer in self._writers.values():
            writer.close()