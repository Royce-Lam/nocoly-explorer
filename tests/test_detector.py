"""Tests for environment detection."""

from __future__ import annotations

from nocoly_explorer.config import PackageConfig
from nocoly_explorer.detector import EnvironmentDetector
from nocoly_explorer.exceptions import EnvironmentDetectionError


def _config_with_env_mode(mode):
    return PackageConfig(env_mode=mode)


def test_detector_returns_databricks_when_forced():
    det = EnvironmentDetector(_config_with_env_mode("databricks"))
    assert det.detect() == "databricks"


def test_detector_returns_standard_when_forced():
    det = EnvironmentDetector(_config_with_env_mode("standard"))
    assert det.detect() == "standard"


def test_detector_raises_for_unknown_env_mode():
    """Typos like 'databrics' must not silently downgrade to standard."""
    det = EnvironmentDetector(_config_with_env_mode("databrics"))
    with __import__("pytest").raises(EnvironmentDetectionError):
        det.detect()


def test_detector_raises_for_empty_env_mode():
    det = EnvironmentDetector(_config_with_env_mode(""))
    with __import__("pytest").raises(EnvironmentDetectionError):
        det.detect()


def test_detector_raises_for_uppercase_env_mode():
    """Mode strings are case-sensitive lowercase."""
    det = EnvironmentDetector(_config_with_env_mode("DATABRICKS"))
    with __import__("pytest").raises(EnvironmentDetectionError):
        det.detect()


def test_detector_none_env_mode_falls_back_to_heuristic(monkeypatch):
    monkeypatch.delenv("DATABRICKS_RUNTIME_VERSION", raising=False)
    monkeypatch.delenv("DB_IS_DRIVER", raising=False)
    monkeypatch.delenv("DATABRICKS_RUNTIME_CLUSTER_ID", raising=False)
    det = EnvironmentDetector(PackageConfig())
    assert det.detect() == "standard"