"""Public façade for downloading worksheet data."""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Union

from .auth import (
    CredentialPair,
    DatabricksCredentialProvider,
    StandardCredentialProvider,
)
from .client import WorksheetClient
from .config import PackageConfig, load_package_config
from .detector import EnvironmentDetector, RuntimeMode
from .exceptions import MissingCredentialsError, OutputValidationError
from .filters import FilterExpression, ensure_filter_dict
from .output import OutputFormatter


class WorksheetExporter:
    """High-level orchestrator for worksheet downloads."""

    def __init__(self, config: PackageConfig | None = None) -> None:
        self.config = config or load_package_config()
        self._detector = EnvironmentDetector(self.config)

    def export(
        self,
        *,
        host: str,
        worksheet_id: str,
        output_type: str | None = None,
        filter_criteria: Optional[Union[Dict[str, Any], FilterExpression]] = None,
        columns: Optional[List[str]] = None,
        sorts: Optional[List[Dict[str, Any]]] = None,
        page_size: int = 200,
        max_pages: int = 1000,
        view_id: str = "",
        file_path: Optional[str] = None,
        file_format: Optional[str] = None,
        env_prefix: Optional[str] = None,
        app_key: Optional[str] = None,
        app_sign: Optional[str] = None,
        databricks_scope: Optional[str] = None,
        databricks_app_key_secret: Optional[str] = None,
        databricks_app_sign_secret: Optional[str] = None,
        verify_ssl: bool = True,
        logger: Optional[logging.Logger] = None,
    ) -> Any:
        """Download worksheet rows and format them as requested."""

        if not host:
            raise ValueError("host is required")
        if not worksheet_id:
            raise ValueError("worksheet_id is required")

        runtime_mode = self._detector.detect()
        credentials = self._resolve_credentials(
            runtime_mode=runtime_mode,
            app_key=app_key,
            app_sign=app_sign,
            env_prefix=env_prefix,
            databricks_scope=databricks_scope,
            databricks_app_key_secret=databricks_app_key_secret,
            databricks_app_sign_secret=databricks_app_sign_secret,
        )

        client = WorksheetClient(
            host=host,
            worksheet_id=worksheet_id,
            credentials=credentials,
            timeout_seconds=self.config.request_timeout_seconds,
            max_retries=self.config.max_retries,
            verify_ssl=verify_ssl,
            logger=logger,
        )
        normalized_filters = ensure_filter_dict(filter_criteria)

        rows = client.fetch_rows(
            columns=columns,
            filter_criteria=normalized_filters,
            sorts=sorts,
            page_size=page_size,
            view_id=view_id,
            max_pages=max_pages,
        )

        formatter = OutputFormatter(runtime_mode)
        resolved_output_type = output_type or self.config.default_output_type
        return formatter.format(
            rows=rows,
            output_type=resolved_output_type,
            file_path=file_path,
            file_format=file_format,
        )

    def _resolve_credentials(
        self,
        *,
        runtime_mode: RuntimeMode,
        app_key: Optional[str],
        app_sign: Optional[str],
        env_prefix: Optional[str],
        databricks_scope: Optional[str],
        databricks_app_key_secret: Optional[str],
        databricks_app_sign_secret: Optional[str],
    ) -> CredentialPair:
        if runtime_mode == "databricks":
            mapping = self.config.databricks
            scope = databricks_scope or mapping.scope
            key_secret = databricks_app_key_secret or mapping.app_key_secret
            sign_secret = databricks_app_sign_secret or mapping.app_sign_secret
            if not scope or not key_secret or not sign_secret:
                raise MissingCredentialsError(
                    "Databricks mode requires scope + secret names. Provide them via args or config."
                )
            provider = DatabricksCredentialProvider(
                scope=scope,
                app_key_secret=key_secret,
                app_sign_secret=sign_secret,
            )
            return provider.get_credentials()

        provider = StandardCredentialProvider(
            explicit_app_key=app_key,
            explicit_app_sign=app_sign,
            env_prefix=env_prefix,
            config=self.config,
        )
        return provider.get_credentials()
