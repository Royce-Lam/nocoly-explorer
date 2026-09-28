"""Tests for Bearer-token API key authentication."""

from __future__ import annotations

import pytest


def test_no_api_key_means_auth_disabled():
    from nocoly_explorer.service.auth import require_api_key
    dep = require_api_key(api_key=None)
    assert dep is not None


def test_api_key_required_when_configured():
    from fastapi import HTTPException
    from nocoly_explorer.service.auth import _check_api_key
    assert _check_api_key("Bearer secret-key", "secret-key") is None
    with pytest.raises(HTTPException) as exc:
        _check_api_key("Bearer wrong-key", "secret-key")
    assert exc.value.status_code == 401


def test_api_key_case_insensitive_scheme():
    from nocoly_explorer.service.auth import _check_api_key
    assert _check_api_key("bearer secret-key", "secret-key") is None


def test_api_key_rejects_non_bearer_scheme():
    from fastapi import HTTPException
    from nocoly_explorer.service.auth import _check_api_key
    with pytest.raises(HTTPException):
        _check_api_key("Basic secret-key", "secret-key")


def test_api_key_rejects_missing_header():
    from fastapi import HTTPException
    from nocoly_explorer.service.auth import _check_api_key
    with pytest.raises(HTTPException) as exc:
        _check_api_key(None, "secret-key")
    assert exc.value.status_code == 401