"""Tests for the Arq worker function."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest


@pytest.fixture
def redis():
    import fakeredis.aioredis
    return fakeredis.aioredis.FakeRedis()


@pytest.fixture
def mock_pages():
    return [
        [{"id": i, "v": i * 2} for i in range(0, 5)],
        [{"id": i, "v": i * 2} for i in range(5, 10)],
        [{"id": i, "v": i * 2} for i in range(10, 12)],
    ]


@pytest.mark.asyncio
async def test_run_job_parquet_export_to_local_sink(redis, mock_pages, tmp_path: Path):
    from nocoly_explorer.service.worker import run_job
    from nocoly_explorer.service.state import JobState
    from aiohttp import web
    from aiohttp.test_utils import TestServer

    pages = mock_pages

    async def handler(request):
        page = int(request.query.get("page", "1"))
        page_size = int(request.query.get("page_size", "200"))
        if page > len(pages):
            return web.json_response({"rows": [], "has_more": False})
        rows = pages[page - 1]
        return web.json_response(
            {"rows": rows[:page_size], "has_more": page < len(pages)}
        )

    app = web.Application()
    app.router.add_get("/worksheets/ws/rows", handler)
    async with TestServer(app) as server:
        state = JobState(redis)
        output_path = tmp_path / "out"
        params = {
            "host": str(server.make_url("")),
            "worksheet_id": "ws",
            "auth_token": "test",
            "output": {"sink": "parquet_local", "path": str(output_path)},
            "page_size": 5,
            "max_pages": 100,
            "concurrency": 2,
        }
        await state.create_job("job-test-1", params)

        result = await run_job(redis=redis, job_id="job-test-1", params=params)

        assert result["status"] == "succeeded"
        assert result["rows_written"] == 12

        parquet_files = list(output_path.rglob("data*.parquet"))
        assert len(parquet_files) >= 1
        import pyarrow.parquet as pq
        total = sum(pq.read_table(f).num_rows for f in parquet_files)
        assert total == 12


@pytest.mark.asyncio
async def test_run_job_records_failure_on_error(redis, tmp_path: Path):
    from nocoly_explorer.service.worker import run_job
    from nocoly_explorer.service.state import JobState

    state = JobState(redis)
    output_path = tmp_path / "out"
    params = {
        "host": "http://127.0.0.1:1",
        "worksheet_id": "ws",
        "auth_token": "test",
        "output": {"sink": "parquet_local", "path": str(output_path)},
        "page_size": 10,
        "max_pages": 5,
        "concurrency": 2,
    }
    await state.create_job("job-fail", params)
    result = await run_job(redis=redis, job_id="job-fail", params=params)
    assert result["status"] == "failed"
    assert "error" in result
    status = await state.get_status("job-fail")
    assert status.status == "failed"
    assert status.error is not None


@pytest.mark.asyncio
async def test_run_job_honors_cancel(redis, mock_pages, tmp_path: Path):
    from nocoly_explorer.service.worker import run_job
    from nocoly_explorer.service.state import JobState
    from aiohttp import web
    from aiohttp.test_utils import TestServer

    pages = mock_pages

    async def handler(request):
        await asyncio.sleep(0.1)
        page = int(request.query.get("page", "1"))
        if page > len(pages):
            return web.json_response({"rows": [], "has_more": False})
        return web.json_response(
            {"rows": pages[page - 1], "has_more": page < len(pages)}
        )

    app = web.Application()
    app.router.add_get("/worksheets/ws/rows", handler)
    async with TestServer(app) as server:
        state = JobState(redis)
        await state.create_job("job-cancel", {"host": "x", "worksheet_id": "y"})
        await state.request_cancel("job-cancel")
        params = {
            "host": str(server.make_url("")),
            "worksheet_id": "ws",
            "auth_token": "test",
            "output": {"sink": "parquet_local", "path": str(tmp_path / "out")},
            "page_size": 5,
            "max_pages": 100,
            "concurrency": 2,
        }
        result = await run_job(redis=redis, job_id="job-cancel", params=params)
        assert result["status"] == "cancelled"