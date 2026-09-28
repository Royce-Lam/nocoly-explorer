"""Runtime environment detection helpers."""

from __future__ import annotations

import os
from typing import Literal, Optional

from .config import PackageConfig
from .exceptions import EnvironmentDetectionError

RuntimeMode = Literal["databricks", "standard"]

_VALID_MODES = {"databricks", "standard"}


class EnvironmentDetector:
    """Determine whether we are running inside Databricks."""

    def __init__(self, config: PackageConfig):
        self._config = config

    def detect(self) -> RuntimeMode:
        """Return the runtime mode based on config/env heuristics.

        Raises EnvironmentDetectionError when config.env_mode is set to a
        value other than None, "databricks", or "standard" — silently falling
        back to "standard" on a typo would mask configuration mistakes.
        """
        forced = self._config.env_mode
        if forced is not None:
            if forced not in _VALID_MODES:
                raise EnvironmentDetectionError(
                    f"Unsupported env_mode {forced!r}; expected one of {sorted(_VALID_MODES)} or None."
                )
            return forced  # type: ignore[return-value]

        if self._looks_like_databricks():
            return "databricks"
        return "standard"

    @staticmethod
    def _looks_like_databricks() -> bool:
        """Heuristics for Databricks detection."""

        env_indicators = (
            "DATABRICKS_RUNTIME_VERSION",
            "DB_IS_DRIVER",
            "DATABRICKS_RUNTIME_CLUSTER_ID",
        )
        if any(os.environ.get(var) for var in env_indicators):
            return True
        try:
            import pyspark  # noqa: F401  # pragma: no cover - import check only
            import dbruntime  # noqa: F401  # type: ignore
        except ModuleNotFoundError:
            return False
        return True


def ensure_mode(mode: Optional[str]) -> RuntimeMode:
    """Validate/normalize a runtime mode string."""

    if mode == "databricks":
        return "databricks"
    if mode == "standard":
        return "standard"
    raise EnvironmentDetectionError(f"Unsupported runtime mode: {mode}")
