"""Credential provider strategies."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Optional, Protocol

from .config import PackageConfig
from .exceptions import MissingCredentialsError

_LOGGER = logging.getLogger(__name__)


@dataclass(slots=True)
class CredentialPair:
    """Container for app credentials."""

    app_key: str
    app_sign: str


class CredentialProvider(Protocol):
    """Protocol for credential providers."""

    def get_credentials(self) -> CredentialPair:
        ...


class DatabricksCredentialProvider:
    """Resolve credentials from Databricks secret scopes."""

    def __init__(
        self,
        scope: str,
        app_key_secret: str,
        app_sign_secret: str,
    ) -> None:
        self.scope = scope
        self.app_key_secret = app_key_secret
        self.app_sign_secret = app_sign_secret

    def _get_dbutils(self):
        try:
            from pyspark.dbutils import DBUtils  # type: ignore
            from pyspark.sql import SparkSession
        except ModuleNotFoundError as exc:  # pragma: no cover - depends on runtime
            raise MissingCredentialsError("pyspark is required for Databricks auth") from exc

        spark = SparkSession.builder.getOrCreate()
        return DBUtils(spark)

    def get_credentials(self) -> CredentialPair:
        dbutils = self._get_dbutils()
        app_key = dbutils.secrets.get(scope=self.scope, key=self.app_key_secret)
        app_sign = dbutils.secrets.get(scope=self.scope, key=self.app_sign_secret)
        if not app_key or not app_sign:
            raise MissingCredentialsError("Databricks secrets returned empty values")
        return CredentialPair(app_key=app_key, app_sign=app_sign)


class StandardCredentialProvider:
    """Resolve credentials from explicit args, env vars, or config."""

    def __init__(
        self,
        explicit_app_key: Optional[str],
        explicit_app_sign: Optional[str],
        config: PackageConfig,
        env_prefix: Optional[str] = None,
    ) -> None:
        self.explicit_app_key = explicit_app_key or config.static_credentials.app_key
        self.explicit_app_sign = explicit_app_sign or config.static_credentials.app_sign
        self.env_prefix = env_prefix or config.static_credentials.env_prefix

    def _build_env_keys(self) -> tuple[str, str]:
        if self.env_prefix:
            prefix = self.env_prefix.upper()
            return (
                f"NOCOLY_{prefix}_APP_KEY",
                f"NOCOLY_{prefix}_APP_SIGN",
            )
        return ("NOCOLY_APP_KEY", "NOCOLY_APP_SIGN")

    def _read_env(self) -> tuple[Optional[str], Optional[str]]:
        """Read credentials from the environment.

        Looks at NOCOLY_<PREFIX>_APP_KEY / NOCOLY_<PREFIX>_APP_SIGN when an
        env_prefix is set, or NOCOLY_APP_KEY / NOCOLY_APP_SIGN otherwise.
        The lookup is strictly scoped to the configured prefix — no silent
        fall-back to unprefixed env vars, which could otherwise leak credentials
        from a different environment.
        """
        primary_key, primary_sign = self._build_env_keys()
        return (
            os.environ.get(primary_key),
            os.environ.get(primary_sign),
        )

    def get_credentials(self) -> CredentialPair:
        app_key = self.explicit_app_key
        app_sign = self.explicit_app_sign
        if not app_key or not app_sign:
            env_key, env_sign = self._read_env()
            app_key = app_key or env_key
            app_sign = app_sign or env_sign
        if not app_key or not app_sign:
            prefix_hint = (
                f"NOCOLY_{self.env_prefix.upper()}_APP_KEY/NOCOLY_{self.env_prefix.upper()}_APP_SIGN"
                if self.env_prefix
                else "NOCOLY_APP_KEY/NOCOLY_APP_SIGN"
            )
            raise MissingCredentialsError(
                f"App key/sign must be provided via parameters, config, or environment variables "
                f"(expected env vars: {prefix_hint})."
            )
        return CredentialPair(app_key=app_key, app_sign=app_sign)