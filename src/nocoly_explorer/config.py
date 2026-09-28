"""Configuration loading utilities."""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional

try:  # Python 3.11+
    import tomllib  # type: ignore[attr-defined]
except ModuleNotFoundError:  # pragma: no cover - fallback for <3.11
    tomllib = None  # type: ignore


_LOGGER = logging.getLogger(__name__)

CONFIG_ENV_VAR = "NOCOLY_CONFIG"
DEFAULT_CONFIG_FILENAMES = (
    "nocoly.toml",
    "nocoly.json",
    "config.toml",
    "config.json",
)
DEFAULT_CONFIG_DIRS = (
    Path(os.environ.get("XDG_CONFIG_HOME", Path.home() / ".config")) / "nocoly",
    Path.cwd(),
)

_SUPPORTED_CONFIG_SUFFIXES = (".json", ".toml")


@dataclass(slots=True)
class DatabricksSecretMapping:
    """Secret scope/key metadata for Databricks authentication."""

    scope: Optional[str] = None
    app_key_secret: Optional[str] = None
    app_sign_secret: Optional[str] = None


@dataclass(slots=True)
class StaticCredentials:
    """Static credentials provided via config."""

    app_key: Optional[str] = None
    app_sign: Optional[str] = None
    env_prefix: Optional[str] = None


@dataclass(slots=True)
class PackageConfig:
    """Resolved package configuration."""

    env_mode: Optional[str] = None  # Force "databricks" or "standard"
    default_output_type: str = "dataframe"
    request_timeout_seconds: float = 30.0
    max_retries: int = 3
    databricks: DatabricksSecretMapping = field(default_factory=DatabricksSecretMapping)
    static_credentials: StaticCredentials = field(default_factory=StaticCredentials)

    def as_dict(self) -> Dict[str, Any]:
        """Return a JSON-serializable representation."""

        return {
            "env_mode": self.env_mode,
            "default_output_type": self.default_output_type,
            "request_timeout_seconds": self.request_timeout_seconds,
            "max_retries": self.max_retries,
            "databricks": {
                "scope": self.databricks.scope,
                "app_key_secret": self.databricks.app_key_secret,
                "app_sign_secret": self.databricks.app_sign_secret,
            },
            "static_credentials": {
                "app_key": self.static_credentials.app_key,
                "app_sign": self.static_credentials.app_sign,
                "env_prefix": self.static_credentials.env_prefix,
            },
        }


def _coerce_int(value: Any, field_name: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be an integer, got bool")
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"{field_name} must be coercible to int (got {type(value).__name__}: {value!r})"
        ) from exc


def _coerce_float(value: Any, field_name: str) -> float:
    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be a number, got bool")
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"{field_name} must be coercible to float (got {type(value).__name__}: {value!r})"
        ) from exc


def _load_from_path(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(path)
    suffix = path.suffix.lower()
    if suffix == ".json":
        return json.loads(path.read_text(encoding="utf-8"))
    if suffix == ".toml":
        if not tomllib:
            raise RuntimeError("TOML config provided but tomllib is unavailable")
        return tomllib.loads(path.read_text(encoding="utf-8"))
    raise ValueError(
        f"Unsupported config format: {suffix!r} for path {path}. "
        f"Supported formats: {_SUPPORTED_CONFIG_SUFFIXES}."
    )


def _discover_config_path(explicit: Optional[str]) -> Optional[Path]:
    if explicit:
        return Path(explicit).expanduser()
    for directory in DEFAULT_CONFIG_DIRS:
        for filename in DEFAULT_CONFIG_FILENAMES:
            try:
                candidate = directory / filename
                if candidate.exists():
                    return candidate
            except OSError:
                # PermissionError etc. — keep searching.
                continue
    return None


# Simple module-level cache so repeated `WorksheetExporter()` instantiations
# within the same process don't re-stat config files. Invalidated whenever
# `NOCOLY_CONFIG` changes between calls.
_CONFIG_CACHE: Dict[Optional[str], PackageConfig] = {}


def load_package_config() -> PackageConfig:
    """Load configuration from env / file system.

    Results are cached per NOCOLY_CONFIG value so repeated calls within the
    same process don't repeat filesystem I/O.
    """
    explicit = os.environ.get(CONFIG_ENV_VAR)
    if explicit in _CONFIG_CACHE:
        return _CONFIG_CACHE[explicit]

    candidate_path = _discover_config_path(explicit)
    if not candidate_path:
        cfg = PackageConfig()
        _CONFIG_CACHE[explicit] = cfg
        return cfg

    raw = _load_from_path(candidate_path)
    databricks_section = raw.get("databricks", {}) if isinstance(raw, dict) else {}
    static_section = raw.get("static_credentials", {}) if isinstance(raw, dict) else {}

    cfg = PackageConfig(
        env_mode=raw.get("env_mode"),
        default_output_type=raw.get("default_output_type", "dataframe"),
        request_timeout_seconds=_coerce_float(
            raw.get("request_timeout_seconds", 30.0), "request_timeout_seconds"
        ),
        max_retries=_coerce_int(raw.get("max_retries", 3), "max_retries"),
        databricks=DatabricksSecretMapping(
            scope=databricks_section.get("scope"),
            app_key_secret=databricks_section.get("app_key_secret"),
            app_sign_secret=databricks_section.get("app_sign_secret"),
        ),
        static_credentials=StaticCredentials(
            app_key=static_section.get("app_key"),
            app_sign=static_section.get("app_sign"),
            env_prefix=static_section.get("env_prefix"),
        ),
    )
    _CONFIG_CACHE[explicit] = cfg
    return cfg