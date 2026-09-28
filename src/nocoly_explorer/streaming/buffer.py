"""Buffer rows into row-group-sized chunks for parquet writing."""

from __future__ import annotations

from typing import Iterator, List

import pyarrow as pa


class RowGroupBuffer:
    """Accumulates PyArrow tables and yields them once size threshold is hit.

    Side effects (accumulation, byte tracking) happen eagerly on `add()`;
    only the *yield* of full row-groups is lazy (via generator).
    """

    def __init__(self, schema: pa.Schema, target_bytes: int) -> None:
        self._schema = schema
        self._target_bytes = target_bytes
        self._accumulated_tables: List[pa.Table] = []
        self._accumulated_bytes: int = 0
        self.total_bytes: int = 0

    def add(self, table: pa.Table) -> Iterator[pa.Table]:
        if table.num_rows == 0:
            return
        self._accumulated_tables.append(table)
        self._accumulated_bytes += table.nbytes
        self.total_bytes += table.nbytes
        if self._accumulated_bytes >= self._target_bytes:
            yield from self._drain()

    def flush(self) -> Iterator[pa.Table]:
        if self._accumulated_tables:
            yield from self._drain()

    def _drain(self) -> Iterator[pa.Table]:
        if not self._accumulated_tables:
            return
        combined = pa.concat_tables(self._accumulated_tables, promote_options="default")
        self._accumulated_tables = []
        self._accumulated_bytes = 0
        yield combined