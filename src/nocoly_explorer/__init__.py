"""Nocoly Explorer public API."""

from .exporter import WorksheetExporter
from .filters import NocolyFilter

__all__ = ["WorksheetExporter", "NocolyFilter", "StreamingExporter",
           "ParquetExportOptions", "PartitionSpec",
           "StreamingExportConfig", "ExportResult",
           "SchemaDriftError", "CardinalityExceededError",
           "AsyncWorksheetClient", "AsyncClientError", "PaginationLimitExceeded"]

# Lazy re-exports: keep pyarrow and aiohttp optional.
_STREAMING_EXPORTS = {
    "StreamingExporter",
    "ParquetExportOptions",
    "PartitionSpec",
    "StreamingExportConfig",
    "ExportResult",
}
_ERROR_EXPORTS = {
    "SchemaDriftError",
    "CardinalityExceededError",
    "AsyncClientError",
    "PaginationLimitExceeded",
}
_ASYNC_EXPORTS = {
    "AsyncWorksheetClient",
    "AsyncClientError",
    "PaginationLimitExceeded",
}


def __getattr__(name):
    if name in _STREAMING_EXPORTS:
        from .streaming import (
            StreamingExporter,
            ParquetExportOptions,
            PartitionSpec,
            StreamingExportConfig,
            ExportResult,
        )
        namespace = {
            "StreamingExporter": StreamingExporter,
            "ParquetExportOptions": ParquetExportOptions,
            "PartitionSpec": PartitionSpec,
            "StreamingExportConfig": StreamingExportConfig,
            "ExportResult": ExportResult,
        }
        return namespace[name]
    if name in _ERROR_EXPORTS:
        from .exceptions import (
            SchemaDriftError,
            CardinalityExceededError,
            AsyncClientError,
            PaginationLimitExceeded,
        )
        return {
            "SchemaDriftError": SchemaDriftError,
            "CardinalityExceededError": CardinalityExceededError,
            "AsyncClientError": AsyncClientError,
            "PaginationLimitExceeded": PaginationLimitExceeded,
        }[name]
    if name in _ASYNC_EXPORTS:
        from .async_client import (
            AsyncWorksheetClient,
            AsyncClientError,
            PaginationLimitExceeded,
        )
        return {
            "AsyncWorksheetClient": AsyncWorksheetClient,
            "AsyncClientError": AsyncClientError,
            "PaginationLimitExceeded": PaginationLimitExceeded,
        }[name]
    raise AttributeError(f"module 'nocoly_explorer' has no attribute {name!r}")