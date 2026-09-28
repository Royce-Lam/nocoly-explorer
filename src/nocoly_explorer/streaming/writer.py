"""Per-partition Parquet writer with append-mode row-group flushing."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import pyarrow as pa
import pyarrow.parquet as pq

from .options import ParquetExportOptions

_LOGGER = logging.getLogger(__name__)


class ParquetPartitionWriter:
    """Wraps pyarrow.parquet.ParquetWriter for append-mode writes to one file."""

    def __init__(
        self,
        path: Path,
        schema: pa.Schema,
        options: ParquetExportOptions,
    ) -> None:
        self.path = path
        self.schema = schema
        self.options = options
        self._writer: Optional[pq.ParquetWriter] = None
        self.rows_written: int = 0
        self.bytes_written: int = 0

    def _ensure_writer(self) -> pq.ParquetWriter:
        if self._writer is None:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self._writer = pq.ParquetWriter(
                str(self.path),
                self.schema,
                compression=self.options.compression,
                write_statistics=self.options.write_statistics,
                use_dictionary=True,
            )
        return self._writer

    def write_chunk(self, table: pa.Table) -> None:
        if table.num_rows == 0:
            return
        writer = self._ensure_writer()
        writer.write_table(table)
        self.rows_written += table.num_rows
        self.bytes_written += table.nbytes

    def close(self) -> None:
        if self._writer is not None:
            self._writer.close()
            self._writer = None
            _LOGGER.debug(
                "streaming_parquet.partition_closed",
                extra={"path": str(self.path), "rows": self.rows_written},
            )