"""Immutable Pipeline v1 data contracts.

The package deliberately contains no controller or worker execution logic.  It
is the shared, closed vocabulary used by the service, scheduler, and workers.
"""

from loom.pipeline.keys import (
    canonical_digest,
    canonical_document,
    canonical_identity,
    canonical_uuid5,
)

__all__ = [
    "canonical_digest",
    "canonical_document",
    "canonical_identity",
    "canonical_uuid5",
]
