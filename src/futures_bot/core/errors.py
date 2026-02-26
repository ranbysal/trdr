"""Custom exceptions for futures_bot."""

from __future__ import annotations


class FuturesBotError(Exception):
    """Base exception for project-specific failures."""


class ConfigurationError(FuturesBotError):
    """Raised when configuration is invalid or missing."""


class ValidationError(FuturesBotError):
    """Raised when deterministic validation checks fail."""
