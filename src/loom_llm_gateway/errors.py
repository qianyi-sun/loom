"""Gateway-specific errors (separate from loom.errors so Gateway doesn't leak
implementation details across the HTTP boundary).
"""

from __future__ import annotations


class GatewayError(Exception):
    """Base for Gateway-internal errors."""


class ProviderError(GatewayError):
    """Underlying LiteLLM/provider call failed."""

    def __init__(self, *, provider: str, kind: str, message: str) -> None:
        super().__init__(message)
        self.provider = provider
        self.kind = kind


class RateCardNotFoundError(GatewayError):
    """No rate card matched the model/provider/tier/region tuple."""


class InvalidLoomMetadataError(GatewayError):
    """The `loom` block in the request body is missing required fields."""
