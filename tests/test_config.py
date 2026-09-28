"""Tests for configuration loading."""

from __future__ import annotations

import json
import os
import tempfile

import pytest

from nocoly_explorer.config import (
    PackageConfig,
    _discover_config_path,
    _load_from_path,
    load_package_config,
)


def _write_config(suffix: str, payload: dict) -> str:
    f = tempfile.NamedTemporaryFile(suffix=suffix, mode="w", delete=False)
    json.dump(payload, f)
    f.close()
    return f.name


def test_load_default_returns_empty_config_when_no_path(monkeypatch):
    monkeypatch.delenv("NOCOLY_CONFIG", raising=False)
    cfg = load_package_config()
    assert isinstance(cfg, PackageConfig)
    assert cfg.env_mode is None
    assert cfg.default_output_type == "dataframe"


def test_load_reads_explicit_config(monkeypatch):
    path = _write_config(".json", {"env_mode": "databricks", "max_retries": 7})
    try:
        monkeypatch.setenv("NOCOLY_CONFIG", path)
        cfg = load_package_config()
        assert cfg.env_mode == "databricks"
        assert cfg.max_retries == 7
    finally:
        os.unlink(path)


def test_load_caches_result(monkeypatch):
    """Calling load_package_config twice within the same process returns the same instance (M5)."""
    path = _write_config(".json", {"max_retries": 9})
    try:
        monkeypatch.setenv("NOCOLY_CONFIG", path)
        first = load_package_config()
        second = load_package_config()
        assert first is second
    finally:
        os.unlink(path)


def test_cache_respects_env_var_change(monkeypatch):
    """Changing NOCOLY_CONFIG and re-calling should pick up the new file."""
    path1 = _write_config(".json", {"max_retries": 1})
    path2 = _write_config(".json", {"max_retries": 2})
    try:
        monkeypatch.setenv("NOCOLY_CONFIG", path1)
        c1 = load_package_config()
        monkeypatch.setenv("NOCOLY_CONFIG", path2)
        c2 = load_package_config()
        assert c1.max_retries == 1
        assert c2.max_retries == 2
    finally:
        os.unlink(path1)
        os.unlink(path2)


def test_load_from_path_clear_error_for_unsupported_extension():
    """M6: unknown extensions should raise a clear, actionable error."""
    bad = tempfile.NamedTemporaryFile(suffix=".yaml", delete=False)
    bad.close()
    try:
        with pytest.raises(ValueError) as excinfo:
            _load_from_path(__import__("pathlib").Path(bad.name))
        msg = str(excinfo.value)
        assert ".yaml" in msg
        assert ".json" in msg or ".toml" in msg  # mentions supported formats
    finally:
        os.unlink(bad.name)


def test_discover_config_path_returns_none_when_nothing_found(monkeypatch):
    """Discovery should return None if no file is found, not crash (M6)."""
    monkeypatch.delenv("NOCOLY_CONFIG", raising=False)
    tmp = tempfile.mkdtemp()
    monkeypatch.chdir(tmp)
    monkeypatch.setenv("XDG_CONFIG_HOME", tmp)
    result = _discover_config_path(None)
    assert result is None


def test_bad_value_type_raises_valueerror(monkeypatch):
    """M6: bad types should give a clear ValueError mentioning the field."""
    path = _write_config(".json", {"max_retries": "not-an-int"})
    try:
        monkeypatch.setenv("NOCOLY_CONFIG", path)
        with pytest.raises(ValueError) as excinfo:
            load_package_config()
        assert "max_retries" in str(excinfo.value) or "int" in str(excinfo.value).lower()
    finally:
        os.unlink(path)