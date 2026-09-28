"""Arq worker function that runs a single export job."""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Any, Dict

from ..async_client import AsyncWorksheetClient
from ..streaming import (
    PartitionSpec,
    ParquetExportOptions,
    StreamingExportConfig,
    StreamingExporter,
)
from .state import JobState

_LOGGER = logging.getLogger("nocoly_explorer.service.worker")


async def run_job(redis: Any, job_id: str, params: Dict[str, Any]) -> Dict[str, Any]:
    """Execute one export job. Returns a dict result for Arq."""
    state = JobState(redis)
    await state.set_status(job_id, "running", progress_pct=0.0)

    sink_cfg = params.get("output") or {}
    sink = sink_cfg.get("sink", "parquet_local")
    output_path = Path(sink_cfg["path"]) if sink_cfg.get("path") else None
    if sink == "parquet_local" and output_path is None:
        await state.set_status(job_id, "failed", error="output.path required for parquet_local")
        return {"status": "failed", "error": "output.path required"}

    partition_col = sink_cfg.get("partition_by")
    partition_gran = sink_cfg.get("partition_granularity")

    host = params["host"]
    worksheet_id = params["worksheet_id"]
    auth_token = params.get("auth_token", "")
    page_size = params.get("page_size", 200)
    max_pages = params.get("max_pages", 1000)
    concurrency = params.get("concurrency", 8)

    rows_written = 0
    artifact_path: str = ""

    try:
        if sink == "parquet_local":
            output_path.mkdir(parents=True, exist_ok=True)
            partition = (
                PartitionSpec(column=partition_col, granularity=partition_gran)
                if partition_col
                else None
            )
            options = ParquetExportOptions()

            # Fetch all pages asynchronously in this event loop, then write
            # Parquet synchronously. This keeps the StreamingExporter's sync
            # API and avoids cross-thread event-loop issues with aiohttp.
            async with AsyncWorksheetClient(
                base_url=host,
                auth_token=auth_token,
                worksheet_id=worksheet_id,
                concurrency=concurrency,
            ) as aclient:
                all_rows = await aclient.fetch_all_async(
                    page_size=page_size,
                    max_pages=max_pages,
                )

                # Cooperative cancellation between pages.
                if await state.is_cancel_requested(job_id):
                    await state.set_status(job_id, "cancelled", error="cancel requested")
                    return {"status": "cancelled"}

                await state.set_status(job_id, "running", progress_pct=50.0, rows_fetched=len(all_rows))

            # Sync Parquet write happens outside the async context.
            exporter = StreamingExporter(
                client=_SyncClientAdapter(all_rows),
                config=StreamingExportConfig(
                    output_dir=output_path,
                    worksheet_id=worksheet_id,
                    options=options,
                    partition=partition,
                ),
            )
            result = exporter.export()
            rows_written = result.rows_written
            artifact_path = str(output_path)

            await state.set_status(
                job_id, "succeeded", progress_pct=100.0, rows_fetched=rows_written
            )
            await state.set_result(
                job_id, artifact_path, rows_written=rows_written
            )
            return {
                "status": "succeeded",
                "rows_written": rows_written,
                "artifact_path": artifact_path,
            }

        else:
            await state.set_status(job_id, "failed", error=f"unsupported sink: {sink}")
            return {"status": "failed", "error": f"unsupported sink: {sink}"}

    except Exception as exc:
        _LOGGER.exception("worker.run_job failed", extra={"job_id": job_id})
        await state.set_status(job_id, "failed", error=str(exc)[:500])
        return {"status": "failed", "error": str(exc)[:500]}


class _SyncClientAdapter:
    """In-memory WorksheetClient that returns pre-fetched rows to the sync exporter."""

    def __init__(self, rows):
        self._rows = list(rows)

    def fetch_rows(self, **_kwargs):
        return self._rows


async def arq_startup(ctx: Dict[str, Any]) -> None:
    pass


async def arq_shutdown(ctx: Dict[str, Any]) -> None:
    pass


def make_arq_settings(redis_url: str):
    """Build arq WorkerSettings for the worker process."""
    from arq import WorkerSettings
    from arq.connections import RedisSettings

    class _Settings(WorkerSettings):
        redis_settings = RedisSettings.from_dsn(redis_url)
        functions = [run_job]
        on_startup = arq_startup
        on_shutdown = arq_shutdown

    return _Settings