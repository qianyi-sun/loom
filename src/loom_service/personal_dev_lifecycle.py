"""Service-owned background loop for durable personal-dev reconciliation."""

from __future__ import annotations

import asyncio
import logging
import os
import ssl
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from loom.dev_instance_provisioner import OwnerAccessSnapshot
from loom.dev_instance_runtime import KubectlClient
from loom.personal_dev_capacity import (
    CapacityManagerPersonalDevProjector,
    PersonalDevCapacityInstaller,
    PersonalDevCapacityManagerConnection,
    PersonalDevCapacityProjector,
)
from loom.personal_dev_capacity_runtime import (
    KubectlPersonalDevCapacityInstaller,
    PersonalDevCapacityRuntimeConfig,
    PersonalDevCapacityStatusReader,
    PsycopgPersonalDevCapacityDatabase,
    parse_pool_capabilities,
)
from loom.personal_dev_environment import (
    PersonalDevLifecycleLimits,
    PersonalDevReconciliationClaim,
)
from loom.personal_dev_environment_store import SqlAlchemyPersonalDevEnvironmentAuthority
from loom.personal_dev_reconciler import (
    PersonalDevEnvironmentReconciler,
    PersonalDevPreparationExecutor,
)
from loom.personal_dev_runtime import (
    PersonalDevAcceptanceInterlock,
    PersonalDevAcceptanceInterlockError,
    PersonalDevAcceptanceRuntimeBinding,
    parse_personal_dev_acceptance_runtime_binding,
)
from loom_capacity_agent.client import (
    DemandReporterTLSFiles,
    build_reporter_tls_context,
    read_owner_only_bytes,
)
from loom_service.config import LoomServiceSettings
from loom_service.dev_instance_access import load_owner_access_snapshot_by_binding

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class PersonalDevCapacityRuntime:
    installer: PersonalDevCapacityInstaller
    projector: CapacityManagerPersonalDevProjector
    status_reader: PersonalDevCapacityStatusReader
    acceptance_interlock: PersonalDevAcceptanceInterlock | None


def build_personal_dev_capacity_runtime(
    settings: LoomServiceSettings,
) -> PersonalDevCapacityRuntime | None:
    """Build both trusted sides of personal capacity projection or fail startup."""

    if not settings.dev_instances_enabled:
        return None
    acceptance_binding: PersonalDevAcceptanceRuntimeBinding | None = None
    if settings.personal_dev_builder_enabled:
        try:
            acceptance_binding = parse_personal_dev_acceptance_runtime_binding(
                settings.personal_dev_acceptance_binding_json,
                settings.personal_dev_acceptance_plan_sha256,
            )
        except PersonalDevAcceptanceInterlockError as exc:
            raise RuntimeError("personal-dev acceptance binding is invalid") from exc
    if settings.dev_instance_database_admin_url is None:
        raise RuntimeError(
            "LOOM_SVC_DEV_INSTANCE_DATABASE_ADMIN_URL is required when dev instances are enabled"
        )
    kubectl_path = settings.dev_instance_kubectl_path
    if not kubectl_path.is_file() or not os.access(kubectl_path, os.X_OK):
        raise RuntimeError("configured dev-instance kubectl executable is unavailable")
    agent_tls_files = DemandReporterTLSFiles(
        ca_file=settings.personal_dev_capacity_ca_file,
        certificate_file=settings.personal_dev_capacity_certificate_file,
        private_key_file=settings.personal_dev_capacity_private_key_file,
    )
    lifecycle_tls_files = DemandReporterTLSFiles(
        ca_file=settings.personal_dev_capacity_lifecycle_ca_file,
        certificate_file=settings.personal_dev_capacity_lifecycle_certificate_file,
        private_key_file=settings.personal_dev_capacity_lifecycle_private_key_file,
    )
    connection = PersonalDevCapacityManagerConnection(
        manager_origin=settings.personal_dev_capacity_manager_origin,
        bearer_token_file=(settings.personal_dev_capacity_lifecycle_bearer_token_file),
        tls_files=lifecycle_tls_files,
    )
    try:
        capabilities = parse_pool_capabilities(
            settings.personal_dev_capacity_pool_capabilities_json
        )
        config = PersonalDevCapacityRuntimeConfig(
            manager_origin=settings.personal_dev_capacity_manager_origin,
            tls_files=agent_tls_files,
            trusted_agent_image=settings.personal_dev_capacity_agent_image,
            pool_capabilities=capabilities,
            poll_interval_seconds=(settings.personal_dev_capacity_agent_poll_interval_sec),
            max_attempts=settings.personal_dev_capacity_agent_max_attempts,
            manager_namespace=settings.personal_dev_capacity_manager_namespace,
            manager_pod_label_key=settings.personal_dev_capacity_manager_pod_label_key,
            manager_pod_label=settings.personal_dev_capacity_manager_pod_label,
            manager_port=settings.personal_dev_capacity_manager_port,
            database_namespace=settings.personal_dev_capacity_database_namespace,
            database_pod_label_key=settings.personal_dev_capacity_database_pod_label_key,
            database_pod_label=settings.personal_dev_capacity_database_pod_label,
            database_port=settings.personal_dev_capacity_database_port,
            dns_namespace=settings.personal_dev_capacity_dns_namespace,
            dns_pod_label_key=settings.personal_dev_capacity_dns_pod_label_key,
            dns_pod_label=settings.personal_dev_capacity_dns_pod_label,
            dns_port=settings.personal_dev_capacity_dns_port,
        )
    except (TypeError, ValueError) as exc:
        raise RuntimeError("personal-dev capacity runtime configuration is invalid") from exc
    try:
        # Validate the lower-authority identity at startup even though its
        # client is constructed later inside an independently installed pod.
        build_reporter_tls_context(agent_tls_files)
        agent_certificate = read_owner_only_bytes(agent_tls_files.certificate_file)
        agent_private_key = read_owner_only_bytes(agent_tls_files.private_key_file)
        lifecycle_certificate = read_owner_only_bytes(lifecycle_tls_files.certificate_file)
        lifecycle_private_key = read_owner_only_bytes(lifecycle_tls_files.private_key_file)
    except (OSError, ssl.SSLError, TypeError, ValueError) as exc:
        raise RuntimeError("personal-dev capacity runtime credentials are invalid") from exc
    if agent_certificate == lifecycle_certificate or agent_private_key == lifecycle_private_key:
        raise RuntimeError("personal-dev lifecycle and reporter agent must use distinct identities")
    kubectl = KubectlClient(
        str(kubectl_path),
        context=settings.dev_instance_kube_context,
        field_manager="loom-personal-dev-capacity",
    )
    installer = KubectlPersonalDevCapacityInstaller(
        kubectl=kubectl,
        database=PsycopgPersonalDevCapacityDatabase(str(settings.dev_instance_database_admin_url)),
        config=config,
    )
    try:
        projector = CapacityManagerPersonalDevProjector.from_files(connection)
    except (OSError, ssl.SSLError, TypeError, ValueError) as exc:
        raise RuntimeError("personal-dev capacity runtime credentials are invalid") from exc
    return PersonalDevCapacityRuntime(
        installer=installer,
        projector=projector,
        status_reader=PersonalDevCapacityStatusReader(
            kubectl=kubectl,
            database_admin_url=str(settings.dev_instance_database_admin_url),
            projector=projector,
        ),
        acceptance_interlock=(
            PersonalDevAcceptanceInterlock.from_binding(
                projector=projector,
                binding=acceptance_binding,
            )
            if acceptance_binding is not None
            else None
        ),
    )


class SessionPersonalDevReconciliationAuthority:
    """Give every durable transition a short independent DB session."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        limits: PersonalDevLifecycleLimits,
    ) -> None:
        self._session_factory = session_factory
        self._limits = limits

    async def _call(self, method: str, **kwargs: Any) -> Any:
        async with self._session_factory() as session:
            authority = SqlAlchemyPersonalDevEnvironmentAuthority(
                session,
                limits=self._limits,
            )
            return await getattr(authority, method)(**kwargs)

    async def claim_next_reconciliation(self, **kwargs: Any) -> Any:
        return await self._call("claim_next_reconciliation", **kwargs)

    async def begin_activation(self, **kwargs: Any) -> Any:
        return await self._call("begin_activation", **kwargs)

    async def heartbeat_reconciliation(self, **kwargs: Any) -> Any:
        return await self._call("heartbeat_reconciliation", **kwargs)

    async def fail_pre_activation(self, **kwargs: Any) -> Any:
        return await self._call("fail_pre_activation", **kwargs)

    async def complete_activation(self, **kwargs: Any) -> Any:
        return await self._call("complete_activation", **kwargs)

    async def prepare_capacity_projection(self, **kwargs: Any) -> Any:
        return await self._call("prepare_capacity_projection", **kwargs)

    async def refresh_capacity_projection_epoch(self, **kwargs: Any) -> Any:
        return await self._call("refresh_capacity_projection_epoch", **kwargs)

    async def record_capacity_projection(self, **kwargs: Any) -> Any:
        return await self._call("record_capacity_projection", **kwargs)

    async def advance_destroy_checkpoint(self, **kwargs: Any) -> Any:
        return await self._call("advance_destroy_checkpoint", **kwargs)


def personal_dev_access_loader(
    session_factory: async_sessionmaker[AsyncSession],
) -> Callable[[PersonalDevReconciliationClaim], Awaitable[OwnerAccessSnapshot]]:
    async def load(claim: PersonalDevReconciliationClaim) -> OwnerAccessSnapshot:
        async with session_factory() as session:
            return await load_owner_access_snapshot_by_binding(
                session,
                owner_user_id=claim.operation.owner_user_id,
                owner_team_id=claim.operation.owner_team_id,
                binding=claim.attempt.access_binding,
            )

    return load


async def personal_dev_reconcile_run_loop(
    *,
    session_factory: async_sessionmaker[AsyncSession],
    executor: PersonalDevPreparationExecutor,
    capacity_installer: PersonalDevCapacityInstaller,
    capacity_projector: PersonalDevCapacityProjector,
    limits: PersonalDevLifecycleLimits,
    reconciler_id: str,
    lease_seconds: int,
    poll_interval_seconds: float,
) -> None:
    if poll_interval_seconds <= 0:
        raise ValueError("personal-dev reconcile poll interval must be positive")
    authority = SessionPersonalDevReconciliationAuthority(
        session_factory,
        limits=limits,
    )
    reconciler = PersonalDevEnvironmentReconciler(
        authority=authority,
        executor=executor,
        capacity_installer=capacity_installer,
        capacity_projector=capacity_projector,
        access_loader=personal_dev_access_loader(session_factory),
        reconciler_id=reconciler_id,
        lease_seconds=lease_seconds,
    )
    while True:
        try:
            progressed = await reconciler.reconcile_once(now=datetime.now(UTC))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error(
                "personal_dev_reconcile_iteration_failed",
                extra={"error_type": type(exc).__name__},
            )
            progressed = False
        if not progressed:
            await asyncio.sleep(poll_interval_seconds)


__all__ = [
    "PersonalDevCapacityRuntime",
    "SessionPersonalDevReconciliationAuthority",
    "build_personal_dev_capacity_runtime",
    "personal_dev_access_loader",
    "personal_dev_reconcile_run_loop",
]
