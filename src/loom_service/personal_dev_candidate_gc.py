"""Service-owned loop for bounded personal-dev artifact garbage collection."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from loom.personal_dev_builder_tools import (
    AsyncBoundedCommandRunner,
    SkopeoPersonalDevRegistryArtifactCollector,
)
from loom.personal_dev_candidate import PersonalDevCandidateLimits
from loom.personal_dev_candidate_gc import (
    PersonalDevArtifactGcCoordinator,
    PersonalDevArtifactGcManifest,
    S3PersonalDevArtifactCollector,
)
from loom.personal_dev_candidate_store import SqlAlchemyPersonalDevCandidateStore
from loom_service.config import LoomServiceSettings
from loom_service.personal_dev_builder import (
    _required_executable,
    _required_registry_auth_file,
)

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class PersonalDevArtifactCollector:
    objects: S3PersonalDevArtifactCollector
    registry: SkopeoPersonalDevRegistryArtifactCollector

    async def collect(self, manifest: PersonalDevArtifactGcManifest) -> None:
        await self.objects.collect(manifest)
        await self.registry.collect(manifest)


def build_personal_dev_artifact_collector(
    settings: LoomServiceSettings,
    *,
    minio_client: Any,
) -> PersonalDevArtifactCollector | None:
    if not (
        settings.dev_instances_enabled
        and settings.personal_dev_builder_enabled
    ):
        return None
    if (
        type(settings.personal_dev_candidate_gc_retention_sec) is not int
        or settings.personal_dev_candidate_gc_retention_sec < 0
        or type(settings.personal_dev_candidate_gc_lease_sec) is not int
        or settings.personal_dev_candidate_gc_lease_sec <= 0
        or settings.personal_dev_candidate_gc_poll_interval_sec <= 0
    ):
        raise RuntimeError("personal-dev artifact GC settings are invalid")
    registry_auth_file = _required_registry_auth_file(
        settings.personal_dev_builder_registry_auth_file
    )
    return PersonalDevArtifactCollector(
        objects=S3PersonalDevArtifactCollector(
            minio_client,
            expected_bucket=settings.artifacts_bucket,
        ),
        registry=SkopeoPersonalDevRegistryArtifactCollector(
            runner=AsyncBoundedCommandRunner(
                environment={
                    "DOCKER_CONFIG": str(registry_auth_file.parent),
                    "HOME": "/tmp",
                    "PATH": (
                        "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
                    ),
                    "TMPDIR": "/tmp",
                }
            ),
            skopeo_executable=_required_executable(
                settings.personal_dev_builder_skopeo_path,
                label="registry cleanup",
            ),
            registry_auth_file=registry_auth_file,
            expected_registry_prefix=settings.personal_dev_builder_registry_prefix,
        ),
    )


class SessionPersonalDevArtifactGcAuthority:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        limits: PersonalDevCandidateLimits,
    ) -> None:
        self._session_factory = session_factory
        self._limits = limits

    async def _call(self, method: str, **kwargs: Any) -> Any:
        async with self._session_factory() as session:
            store = SqlAlchemyPersonalDevCandidateStore(session, limits=self._limits)
            return await getattr(store, method)(**kwargs)

    async def claim_next_artifact_gc(self, **kwargs: Any) -> Any:
        return await self._call("claim_next_artifact_gc", **kwargs)

    async def mark_next_artifact_gc(self, **kwargs: Any) -> Any:
        return await self._call("mark_next_artifact_gc", **kwargs)

    async def heartbeat_artifact_gc(self, **kwargs: Any) -> Any:
        return await self._call("heartbeat_artifact_gc", **kwargs)

    async def finish_artifact_gc(self, **kwargs: Any) -> Any:
        return await self._call("finish_artifact_gc", **kwargs)


async def personal_dev_artifact_gc_run_loop(
    *,
    session_factory: async_sessionmaker[AsyncSession],
    collector: PersonalDevArtifactCollector,
    limits: PersonalDevCandidateLimits,
    collector_id: str,
    retention_seconds: int,
    lease_seconds: int,
    poll_interval_seconds: float,
) -> None:
    if poll_interval_seconds <= 0:
        raise ValueError("personal-dev artifact GC poll interval must be positive")
    coordinator = PersonalDevArtifactGcCoordinator(
        authority=SessionPersonalDevArtifactGcAuthority(
            session_factory,
            limits=limits,
        ),
        collector=collector,
        collector_id=collector_id,
        retention_seconds=retention_seconds,
        lease_seconds=lease_seconds,
    )
    while True:
        try:
            progressed = await coordinator.collect_once(now=datetime.now(UTC))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error(
                "personal_dev_artifact_gc_iteration_failed",
                extra={"error_type": type(exc).__name__},
            )
            progressed = False
        if not progressed:
            await asyncio.sleep(poll_interval_seconds)


__all__ = [
    "PersonalDevArtifactCollector",
    "SessionPersonalDevArtifactGcAuthority",
    "build_personal_dev_artifact_collector",
    "personal_dev_artifact_gc_run_loop",
]
