"""Nocoly Explorer public API."""

from .exporter import WorksheetExporter
from .filters import NocolyFilter

__all__ = ["WorksheetExporter", "NocolyFilter", "StreamingExporter",
           "ParquetExportOptions", "PartitionSpec",
           "StreamingExportConfig", "ExportResult",
           "SchemaDriftError", "CardinalityExceededError"]

# Lazy re-exports: keep pyarrow optional. If a user only needs the v0.1.1 API,
# importing nocoly_explorer must not pull in pyarrow.
_STREAMING_EXPORTS = {
    "StreamingExporter",
    "ParquetExportOptions",
    "PartitionSpec",
    "StreamingExportConfig",
    "ExportResult",
}
_ERROR_EXPORTS = {"SchemaDriftError", "CardinalityExceededError"}


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
        from .exceptions import SchemaDriftError, CardinalityExceededError
        return {"SchemaDriftError": SchemaDriftError,
                "CardinalityExceededError": CardinalityExceededError}[name]
    raise AttributeError(f"module 'nocoly_explorer' has no attribute {name!r}")