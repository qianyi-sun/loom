"""Production detached backup/rehearsal composition for the service worker."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timedelta

from loom_cli.cluster_backup_guard import DEFAULT_BACKUP_MAX_AGE_HOURS

from .backup import BackupCreator, SubprocessBackupCommandRunner, VerifiedBackup
from .checkpoint_inventory_provider import KubernetesLifecycleInventoryProvider
from .checkpoint_lease import CriticalCheckpointEvidence, inspect_critical_checkpoint
from .config import OperatorConfig
from .detached_preflight_runner import DetachedPreflightBackupRunner
from .installed_deep_preflight_factory import build_installed_deep_preflight_composition
from .model import PreflightRequest
from .policy import sanitized_child_environment
from .store import RequestStore

# Preserve the existing two-hour launch margin and another two-hour hard
# cushion for backup/rehearsal runtime before the 24-hour manifest age bound.
_RESTORE_VERIFIED_LEASE_TTL = timedelta(hours=DEFAULT_BACKUP_MAX_AGE_HOURS - 4)


def build_installed_detached_preflight_runner(
    config: OperatorConfig,
    *,
    service_uid: int,
    service_gid: int,
    store: RequestStore,
    now: Callable[[], datetime],
) -> DetachedPreflightBackupRunner:
    """Build the exact Tier 3 worker callback; no legacy backup-only fallback."""
    if service_uid < 0 or service_gid < 0:
        raise ValueError("detached preflight service identity is invalid")
    child_environment = sanitized_child_environment(config, service_uid=service_uid)
    command_runner = SubprocessBackupCommandRunner()
    inventory_source = KubernetesLifecycleInventoryProvider(
        config,
        runner=command_runner,
        environment=child_environment,
    )
    creator = BackupCreator(
        config,
        service_uid=service_uid,
        runner=command_runner,
        now=now,
        object_inventory_provider=inventory_source,
    )
    authority = build_installed_deep_preflight_composition(
        config,
        service_uid=service_uid,
        service_gid=service_gid,
        store=store,
        now=now,
    ).authority()

    def inspect(
        backup: VerifiedBackup,
        request: PreflightRequest,
    ) -> CriticalCheckpointEvidence:
        return inspect_critical_checkpoint(
            backup,
            request_id=request.request_id,
            environment=request.environment,
            namespace=request.namespace,
            expected_owner_uid=service_uid,
            now=now(),
        )

    return DetachedPreflightBackupRunner(
        creator=creator,
        store=store,
        rehearsal_store=store,
        load_assessment=store.read_preflight_assessment,
        orchestrator=authority.detached_orchestrator(),
        inspect_checkpoint=inspect,
        now=now,
        lease_ttl=_RESTORE_VERIFIED_LEASE_TTL,
    )


__all__ = ["build_installed_detached_preflight_runner"]
