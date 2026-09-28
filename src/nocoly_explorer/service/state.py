"""Redis-backed job state for the FastAPI service."""

from __future__ import annotations

import json
from typing import Any, Dict, Optional

from .schemas import JobResult, JobStatus


def _status_key(job_id: str) -> str:
    return f"job:{job_id}:status"


def _result_key(job_id: str) -> str:
    return f"job:{job_id}:result"


def _cancel_key(job_id: str) -> str:
    return f"job:{job_id}:cancel"


def _params_key(job_id: str) -> str:
    return f"job:{job_id}:params"


class JobState:
    """Async Redis-backed job state manager."""

    def __init__(self, redis: Any) -> None:
        self.redis = redis

    async def create_job(self, job_id: str, params: Dict[str, Any]) -> None:
        await self.redis.set(_params_key(job_id), json.dumps(params, default=str))
        await self.redis.set(
            _status_key(job_id),
            json.dumps({"job_id": job_id, "status": "queued"}),
        )

    async def set_status(
        self,
        job_id: str,
        status: str,
        *,
        progress_pct: Optional[float] = None,
        rows_fetched: Optional[int] = None,
        error: Optional[str] = None,
    ) -> None:
        existing_raw = await self.redis.get(_status_key(job_id))
        existing: Dict[str, Any] = json.loads(existing_raw) if existing_raw else {"job_id": job_id}
        existing["job_id"] = job_id
        existing["status"] = status
        if progress_pct is not None:
            existing["progress_pct"] = progress_pct
        if rows_fetched is not None:
            existing["rows_fetched"] = rows_fetched
        if error is not None:
            existing["error"] = error
        await self.redis.set(_status_key(job_id), json.dumps(existing))

    async def get_status(self, job_id: str) -> Optional[JobStatus]:
        raw = await self.redis.get(_status_key(job_id))
        if raw is None:
            return None
        return JobStatus(**json.loads(raw))

    async def set_result(
        self, job_id: str, artifact_path: str, *, rows_written: int
    ) -> None:
        await self.redis.set(
            _result_key(job_id),
            json.dumps(
                {
                    "job_id": job_id,
                    "status": "succeeded",
                    "artifact_path": artifact_path,
                    "rows_written": rows_written,
                }
            ),
        )

    async def get_result(self, job_id: str) -> Optional[JobResult]:
        raw = await self.redis.get(_result_key(job_id))
        if raw is None:
            return None
        return JobResult(**json.loads(raw))

    async def get_params(self, job_id: str) -> Optional[Dict[str, Any]]:
        raw = await self.redis.get(_params_key(job_id))
        if raw is None:
            return None
        return json.loads(raw)

    async def request_cancel(self, job_id: str) -> None:
        await self.redis.set(_cancel_key(job_id), "1")

    async def is_cancel_requested(self, job_id: str) -> bool:
        raw = await self.redis.get(_cancel_key(job_id))
        return raw is not None