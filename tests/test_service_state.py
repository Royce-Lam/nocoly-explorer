"""Tests for JobState (Redis-backed state manager)."""

from __future__ import annotations

import pytest


@pytest.fixture
def redis():
    import fakeredis.aioredis
    return fakeredis.aioredis.FakeRedis()


@pytest.mark.asyncio
async def test_create_job_initializes_status(redis):
    from nocoly_explorer.service.state import JobState
    state = JobState(redis)
    await state.create_job("job-1", {"host": "https://x", "worksheet_id": "ws"})
    status = await state.get_status("job-1")
    assert status is not None
    assert status.job_id == "job-1"
    assert status.status == "queued"


@pytest.mark.asyncio
async def test_get_status_returns_none_for_unknown_job(redis):
    from nocoly_explorer.service.state import JobState
    state = JobState(redis)
    assert await state.get_status("nope") is None


@pytest.mark.asyncio
async def test_set_status_updates_progress(redis):
    from nocoly_explorer.service.state import JobState
    state = JobState(redis)
    await state.create_job("job-2", {})
    await state.set_status("job-2", "running", progress_pct=42.0, rows_fetched=1000)
    s = await state.get_status("job-2")
    assert s.status == "running"
    assert s.progress_pct == 42.0
    assert s.rows_fetched == 1000


@pytest.mark.asyncio
async def test_set_status_preserves_unset_fields(redis):
    from nocoly_explorer.service.state import JobState
    state = JobState(redis)
    await state.create_job("job-3", {})
    await state.set_status("job-3", "running", progress_pct=10.0)
    await state.set_status("job-3", "running", rows_fetched=50)
    s = await state.get_status("job-3")
    assert s.progress_pct == 10.0
    assert s.rows_fetched == 50


@pytest.mark.asyncio
async def test_set_result_then_get_result(redis):
    from nocoly_explorer.service.state import JobState
    state = JobState(redis)
    await state.create_job("job-4", {})
    await state.set_status("job-4", "succeeded")
    await state.set_result("job-4", "/tmp/out", rows_written=12345)
    result = await state.get_result("job-4")
    assert result is not None
    assert result.artifact_path == "/tmp/out"
    assert result.rows_written == 12345


@pytest.mark.asyncio
async def test_get_result_returns_none_for_unknown_job(redis):
    from nocoly_explorer.service.state import JobState
    state = JobState(redis)
    assert await state.get_result("nope") is None


@pytest.mark.asyncio
async def test_cancel_request_lifecycle(redis):
    from nocoly_explorer.service.state import JobState
    state = JobState(redis)
    await state.create_job("job-5", {})
    assert not await state.is_cancel_requested("job-5")
    await state.request_cancel("job-5")
    assert await state.is_cancel_requested("job-5")


@pytest.mark.asyncio
async def test_set_status_records_error_message(redis):
    from nocoly_explorer.service.state import JobState
    state = JobState(redis)
    await state.create_job("job-6", {})
    await state.set_status("job-6", "failed", error="boom")
    s = await state.get_status("job-6")
    assert s.status == "failed"
    assert s.error == "boom"