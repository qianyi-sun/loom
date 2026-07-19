"""Derive critical object inventory from the shared read-only DB snapshot."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime

from loom_cli.rollout.readonly_database_authority import ReadonlyDatabaseEvidence

from .config import OperatorConfig
from .rollout_checkpoint import (
    ImmutableObjectInventory,
    build_immutable_inventory,
)


class ReadonlyLifecycleInventoryProvider:
    """Read exact pinned-object versions from the shared DB snapshot authority."""

    def __init__(
        self,
        config: OperatorConfig,
        *,
        evidence_source: Callable[[], ReadonlyDatabaseEvidence],
    ) -> None:
        if config.environment != "staging" or config.namespace != "loom-staging":
            raise ValueError("critical checkpoint inventory is staging-only")
        self._config = config
        self._evidence_source = evidence_source

    def __call__(self, created_at: datetime) -> ImmutableObjectInventory:
        evidence = self._evidence_source()
        return build_immutable_inventory(
            environment=self._config.environment,
            namespace=self._config.namespace,
            mutation_epoch=evidence.mutation_epoch,
            schema_revision=evidence.schema_revision,
            created_at=created_at,
            objects=evidence.immutable_objects,
        )


__all__ = ["ReadonlyLifecycleInventoryProvider"]
