"""Tests for WorksheetClient HTTP behavior."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
import requests

from nocoly_explorer.auth import CredentialPair
from nocoly_explorer.client import WorksheetClient
from nocoly_explorer.exceptions import NocolyError


def _client(**overrides):
    creds = CredentialPair(app_key="k", app_sign="s")
    kwargs = dict(
        host="https://example.com",
        worksheet_id="ws_1",
        credentials=creds,
        max_retries=3,
        timeout_seconds=30.0,
    )
    kwargs.update(overrides)
    return WorksheetClient(**kwargs)


def test_max_pages_zero_raises():
    client = _client()
    with pytest.raises(ValueError):
        client.fetch_rows(max_pages=0)


def test_max_pages_negative_raises():
    client = _client()
    with pytest.raises(ValueError):
        client.fetch_rows(max_pages=-5)


def test_page_size_must_be_positive():
    client = _client()
    with pytest.raises(ValueError):
        client.fetch_rows(page_size=0)


def test_page_size_excessive_raises():
    client = _client()
    with pytest.raises(ValueError):
        client.fetch_rows(page_size=10_000)


class _FakeResponse:
    def __init__(self, *, status_code=200, json_data=None, headers=None, text=""):
        self.status_code = status_code
        self.ok = 200 <= status_code < 300
        self._json = json_data
        self.text = text
        self.headers = headers or {}

    def json(self):
        return self._json


def _patched_session(client, responses):
    """Replace client._session.post with a sequence-returning mock."""
    iter_resp = iter(responses)
    post = MagicMock(side_effect=lambda *a, **kw: next(iter_resp))
    client._session.post = post
    return post


def test_backoff_caps_at_max_wait(monkeypatch):
    """Exponential backoff must not exceed max_wait_seconds (I5)."""
    sleeps = []
    monkeypatch.setattr("nocoly_explorer.client.time.sleep", lambda s: sleeps.append(s))

    client = _client(max_retries=10)
    # Always fail
    post = _patched_session(client, [
        _FakeResponse(status_code=500, text="boom") for _ in range(11)
    ])
    client._session.post = post

    with pytest.raises(NocolyError):
        client._execute({})

    # Largest single sleep must be <= 30s (default max_wait).
    assert max(sleeps) <= 30.0, f"sleep grew unbounded: {max(sleeps)}"
    # And the schedule is non-decreasing then capped
    for i in range(1, len(sleeps)):
        assert sleeps[i] >= sleeps[i - 1]


def test_backoff_max_wait_configurable(monkeypatch):
    sleeps = []
    monkeypatch.setattr("nocoly_explorer.client.time.sleep", lambda s: sleeps.append(s))

    client = _client(max_retries=8, max_wait_seconds=2.0)
    post = _patched_session(client, [
        _FakeResponse(status_code=500, text="boom") for _ in range(9)
    ])
    client._session.post = post
    with pytest.raises(NocolyError):
        client._execute({})
    assert max(sleeps) <= 2.0


def test_retry_after_header_honored(monkeypatch):
    """When server returns Retry-After, use it instead of exponential backoff."""
    sleeps = []
    monkeypatch.setattr("nocoly_explorer.client.time.sleep", lambda s: sleeps.append(s))

    client = _client(max_retries=3)
    # First two responses are 429 with Retry-After: 7 ; third succeeds
    responses = [
        _FakeResponse(status_code=429, headers={"Retry-After": "7"}, text="slow down"),
        _FakeResponse(status_code=429, headers={"Retry-After": "7"}, text="slow down"),
        _FakeResponse(status_code=200, json_data={"rows": [{"a": 1}], "has_more": False}),
    ]
    _patched_session(client, responses)
    client._session.post = client._session.post

    result = client._execute({})
    assert result == {"rows": [{"a": 1}], "has_more": False}
    # Sleep should include 7 (from Retry-After) and probably cap
    assert any(s >= 7 for s in sleeps), f"expected Retry-After honored; sleeps were {sleeps}"


def test_retry_after_http_date_honored(monkeypatch):
    """Retry-After: HTTP-date format is also accepted (I6)."""
    import datetime
    sleeps = []
    monkeypatch.setattr("nocoly_explorer.client.time.sleep", lambda s: sleeps.append(s))

    client = _client(max_retries=2)
    future = (
        datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=3)
    ).strftime("%a, %d %b %Y %H:%M:%S GMT")
    responses = [
        _FakeResponse(status_code=503, headers={"Retry-After": future}, text="down"),
        _FakeResponse(status_code=200, json_data={"rows": [], "has_more": False}),
    ]
    _patched_session(client, responses)
    client._session.post = client._session.post

    result = client._execute({})
    assert result == {"rows": [], "has_more": False}
    # Should sleep around 3 seconds (date-based)
    assert any(2 <= s <= 5 for s in sleeps), f"expected ~3s sleep from date; sleeps were {sleeps}"


def test_empty_rows_with_has_more_true_logs_warning(monkeypatch):
    """Empty rows + has_more=True is suspicious; we should log + keep paging up to cap."""
    import logging
    logs = []
    client = _client(max_retries=0)

    responses = [
        _FakeResponse(status_code=200, json_data={"rows": [], "has_more": True}),
        _FakeResponse(status_code=200, json_data={"rows": [{"a": 1}], "has_more": False}),
    ]
    _patched_session(client, responses)
    client._session.post = client._session.post

    # attach a capturing handler
    handler = logging.Handler()
    handler.emit = lambda record: logs.append(record.getMessage())
    client._logger.addHandler(handler)
    client._logger.setLevel(logging.DEBUG)
    rows = client.fetch_rows(page_size=10, max_pages=5)
    assert rows == [{"a": 1}]
    assert any("empty" in m.lower() for m in logs), f"expected warning; got {logs}"


def test_request_exception_eventually_raises(monkeypatch):
    """Transport errors retry then raise NocolyError."""
    client = _client(max_retries=2)
    # max_retries=2 means up to 3 attempts before raising
    client._session.post = MagicMock(
        side_effect=[
            requests.ConnectionError("boom"),
            requests.ConnectionError("boom"),
            requests.ConnectionError("boom"),
        ]
    )
    with pytest.raises(NocolyError):
        client._execute({})


def test_retry_response_uses_persistent_headers(monkeypatch):
    """Headers must include HAP-AppKey/HAP-Sign/HAP-AuthType on every attempt."""
    client = _client(max_retries=2)
    responses = [
        _FakeResponse(status_code=500, text="x"),
        _FakeResponse(status_code=200, json_data={"rows": [], "has_more": False}),
    ]
    captured = []

    def fake_post(url, json, headers, timeout, verify):
        captured.append(headers)
        return next(iter(responses))

    iter_resp = iter(responses)
    client._session.post = MagicMock(side_effect=lambda *a, **kw: next(iter_resp))

    client._execute({})
    assert all(h.get("HAP-AppKey") == "k" for h in captured)