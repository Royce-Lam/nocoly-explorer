"""FastAPI service layer for Nocoly Explorer jobs."""

from __future__ import annotations

import uuid
from typing import Any, Awaitable, Callable, Optional

from fastapi import Depends, FastAPI, HTTPException, Request, status

from .auth import require_api_key
from .schemas import HealthResponse, JobResult, JobStatus, JobSubmission
from .state import JobState

__all__ = [
    "create_app",
    "JobState",
    "JobSubmission",
    "JobStatus",
    "JobResult",
    "HealthResponse",
]


EnqueueFunc = Callable[[str, dict], Awaitable[Any]]
RedisFactory = Callable[[], Any]


def create_app(
    *,
    redis_url: Optional[str] = None,
    redis: Optional[Any] = None,
    redis_factory: Optional[RedisFactory] = None,
    api_key: Optional[str] = None,
    enqueue_func: Optional[EnqueueFunc] = None,
) -> FastAPI:
    """Create the FastAPI app.

    Exactly one of `redis_url`, `redis`, or `redis_factory` is required.
    A factory is recommended for tests using fakeredis (each request gets a
    fresh client bound to the current event loop).
    """
    if redis_factory is None and redis is None and not redis_url:
        raise ValueError(
            "One of redis_url, redis, or redis_factory must be provided"
        )
    if redis_factory is None and redis is None:
        import redis.asyncio as aioredis
        redis_client_singleton = aioredis.from_url(redis_url)
    elif redis_factory is None:
        redis_client_singleton = redis
    else:
        redis_client_singleton = None

    auth_dep = require_api_key(api_key)
    app = FastAPI(title="nocoly-explorer", version="0.2.0")
    app.state.redis_singleton = redis_client_singleton
    app.state.redis_factory = redis_factory
    app.state.enqueue_func = enqueue_func

    def _get_redis(request: Request) -> Any:
        if request.app.state.redis_factory is not None:
            return request.app.state.redis_factory()
        return request.app.state.redis_singleton

    def _get_state(request: Request) -> JobState:
        return JobState(_get_redis(request))

    @app.get("/healthz", response_model=HealthResponse, tags=["meta"])
    async def healthz() -> HealthResponse:
        return HealthResponse(status="ok")

    @app.get("/readyz", response_model=HealthResponse, tags=["meta"])
    async def readyz(request: Request) -> HealthResponse:
        try:
            await _get_redis(request).ping()
            return HealthResponse(status="ok")
        except Exception:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Redis unreachable",
            )

    @app.post("/jobs", tags=["jobs"])
    async def submit_job(
        submission: JobSubmission,
        request: Request,
        _auth: None = Depends(auth_dep),
    ) -> dict:
        job_id = uuid.uuid4().hex
        params = submission.model_dump(mode="json")
        state = _get_state(request)
        await state.create_job(job_id, params)
        if app.state.enqueue_func is not None:
            await app.state.enqueue_func(job_id, params)
        return {"job_id": job_id}

    @app.get("/jobs/{job_id}", response_model=JobStatus, tags=["jobs"])
    async def get_job_status(
        job_id: str,
        request: Request,
        _auth: None = Depends(auth_dep),
    ) -> JobStatus:
        s = await _get_state(request).get_status(job_id)
        if s is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Job {job_id!r} not found",
            )
        return s

    @app.get("/jobs/{job_id}/result", response_model=JobResult, tags=["jobs"])
    async def get_job_result(
        job_id: str,
        request: Request,
        _auth: None = Depends(auth_dep),
    ) -> JobResult:
        state = _get_state(request)
        s = await state.get_status(job_id)
        if s is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Job {job_id!r} not found",
            )
        if s.status != "succeeded":
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Job {job_id!r} is {s.status!r}; result not available",
            )
        r = await state.get_result(job_id)
        if r is None:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Job {job_id!r} succeeded but result missing",
            )
        return r

    @app.post("/jobs/{job_id}/cancel", tags=["jobs"])
    async def cancel_job(
        job_id: str,
        request: Request,
        _auth: None = Depends(auth_dep),
    ) -> dict:
        state = _get_state(request)
        s = await state.get_status(job_id)
        if s is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Job {job_id!r} not found",
            )
        await state.request_cancel(job_id)
        return {"status": "cancellation_requested"}

    return app