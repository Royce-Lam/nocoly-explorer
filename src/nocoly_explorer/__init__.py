"""Nocoly Explorer public API."""

from .exporter import WorksheetExporter
from .filters import NocolyFilter

__all__ = [
    "WorksheetExporter", "NocolyFilter",
    "StreamingExporter", "ParquetExportOptions", "PartitionSpec",
    "StreamingExportConfig", "ExportResult",
    "SchemaDriftError", "CardinalityExceededError",
    "AsyncWorksheetClient", "AsyncClientError", "PaginationLimitExceeded",
    "create_app", "JobSubmission", "JobStatus", "JobResult",
    "HealthResponse", "OutputSpec", "JobState", "run_job",
]

# Lazy re-exports: keep pyarrow, aiohttp, fastapi optional.
_STREAMING_EXPORTS = {
    "StreamingExporter", "ParquetExportOptions", "PartitionSpec",
    "StreamingExportConfig", "ExportResult",
}
_ERROR_EXPORTS = {
    "SchemaDriftError", "CardinalityExceededError",
    "AsyncClientError", "PaginationLimitExceeded",
}
_ASYNC_EXPORTS = {
    "AsyncWorksheetClient", "AsyncClientError", "PaginationLimitExceeded",
}
_SERVICE_EXPORTS = {
    "create_app", "JobSubmission", "JobStatus", "JobResult",
    "HealthResponse", "OutputSpec", "JobState", "run_job",
}


def __getattr__(name):
    if name in _STREAMING_EXPORTS:
        from .streaming import (
            StreamingExporter, ParquetExportOptions, PartitionSpec,
            StreamingExportConfig, ExportResult,
        )
        return {
            "StreamingExporter": StreamingExporter,
            "ParquetExportOptions": ParquetExportOptions,
            "PartitionSpec": PartitionSpec,
            "StreamingExportConfig": StreamingExportConfig,
            "ExportResult": ExportResult,
        }[name]
    if name in _ERROR_EXPORTS:
        from .exceptions import (
            SchemaDriftError, CardinalityExceededError,
            AsyncClientError, PaginationLimitExceeded,
        )
        return {
            "SchemaDriftError": SchemaDriftError,
            "CardinalityExceededError": CardinalityExceededError,
            "AsyncClientError": AsyncClientError,
            "PaginationLimitExceeded": PaginationLimitExceeded,
        }[name]
    if name in _ASYNC_EXPORTS:
        from .async_client import (
            AsyncWorksheetClient, AsyncClientError, PaginationLimitExceeded,
        )
        return {
            "AsyncWorksheetClient": AsyncWorksheetClient,
            "AsyncClientError": AsyncClientError,
            "PaginationLimitExceeded": PaginationLimitExceeded,
        }[name]
    if name in _SERVICE_EXPORTS:
        from .service import create_app, JobState
        from .service.schemas import (
            JobSubmission, JobStatus, JobResult,
            HealthResponse, OutputSpec,
        )
        from .service.worker import run_job
        return {
            "create_app": create_app,
            "JobState": JobState,
            "JobSubmission": JobSubmission,
            "JobStatus": JobStatus,
            "JobResult": JobResult,
            "HealthResponse": HealthResponse,
            "OutputSpec": OutputSpec,
            "run_job": run_job,
        }[name]
    raise AttributeError(f"module 'nocoly_explorer' has no attribute {name!r}")