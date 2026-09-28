"""Custom exception hierarchy for the Nocoly explorer package."""

from __future__ import annotations


class NocolyError(Exception):
    """Base class for all custom errors."""


class MissingCredentialsError(NocolyError):
    """Raised when app_key/app_sign cannot be resolved."""


class OutputValidationError(NocolyError):
    """Raised when output parameters are inconsistent."""


class EnvironmentDetectionError(NocolyError):
    """Raised when the runtime mode cannot be determined."""
