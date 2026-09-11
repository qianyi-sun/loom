"""Management's native demand adapter for the existing whole-attempt coordinator.

This adapter never allocates capacity or launches a build process. It requires
installed native consumers before service intake may select it. The coordinator
owns lease heartbeat, finalization and unconditional cleanup after build().
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from types import MappingProxyType
from typing import Protocol
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from loom.personal_dev_build_demand import personal_build_work_identity
from loom.personal_dev_build_platform_requests import (
    _installation,
    _member,
    cancel_platform_requests,
    stage_platform_requests,
)
from loom.personal_dev_build_runtime_installation import PersonalBuildRuntimeInstallation
from loom.personal_dev_builder import verify_personal_dev_build_source
from loom.personal_dev_builder_exporter import PersonalDevAcceptedArtifactResolver
from loom.personal_dev_builder_runtime import PersonalDevBuildPublicationExporter
from loom.personal_dev_candidate import (
    PERSONAL_DEV_PLATFORMS,
    CandidateRegistration,
    PersonalDevPlatform,
)
from loom_capacity_agent.build_admission import BuildOutcomeReceiptV1
from loom_capacity_executor.native_build_source import _settled_io
from loom_capacity_manager.build_value_contracts import PersonalBuildMemberV1
from loom_capacity_manager.contracts import canonical_digest


class NativePlatformOutcomeReader(PersonalDevAcceptedArtifactResolver, Protocol):
    async def observe(self, registration: CandidateRegistration, *, platform: PersonalDevPlatform) -> BuildOutcomeReceiptV1 | None: ...


class NativeBuildPublicationExporter(PersonalDevBuildPublicationExporter, Protocol):
    @property
    def accepted_artifact_resolver(self) -> PersonalDevAcceptedArtifactResolver | None: ...


@dataclass(frozen=True, slots=True)
class NativePersonalDevBuildExecutor:
    session_factory: async_sessionmaker[AsyncSession]
    member: PersonalBuildMemberV1
    runtime: PersonalBuildRuntimeInstallation
    outcomes: NativePlatformOutcomeReader
    exporter: NativeBuildPublicationExporter
    wait_timeout_seconds: float = 3600
    poll_interval_seconds: float = 2

    def __post_init__(self) -> None:
        member = _member(self.member)
        _installation(member, self.runtime)
        object.__setattr__(self, "member", member)
        if self.exporter.accepted_artifact_resolver is not self.outcomes:
            raise ValueError("native platform publication requires the same accepted artifact resolver")
        for value, maximum in ((self.wait_timeout_seconds, 7200), (self.poll_interval_seconds, 60)):
            if type(value) not in (int, float) or not 0 < value <= maximum:
                raise ValueError("native platform waiter timing is invalid")
        if self.poll_interval_seconds >= self.wait_timeout_seconds:
            raise ValueError("native platform polling must fit within its wait deadline")

    def _result(self, receipt: BuildOutcomeReceiptV1, registration: CandidateRegistration,
        platform: PersonalDevPlatform,
    ) -> str:
        receipt = BuildOutcomeReceiptV1.model_validate_json(receipt.model_dump_json())
        request, config = receipt.request, self.member.configuration
        binding = request.claim.binding
        if (receipt.request_digest != canonical_digest(request)
            or request.claim.request_id != personal_build_work_identity(registration, platform)[1]
            or binding.pool_id != ("gb10" if platform == "linux/arm64" else "oldlab")
            or binding.subject_id != config.subject_id or binding.subject_incarnation != config.subject_incarnation
            or binding.deployment_generation != config.deployment_generation
            or binding.candidate_generation != config.candidate_generation
            or binding.candidate != self.runtime.candidate):
            raise ValueError("native platform waiter outcome binding changed")
        return request.result

    async def build(self, registration: CandidateRegistration, *, source_archive: Path) -> Mapping[str, object]:
        async with asyncio.timeout(self.wait_timeout_seconds):
            # No feature-controlled extraction or host build. Cancellation settles
            # this read before the coordinator can remove its source workspace.
            await _settled_io(verify_personal_dev_build_source, registration.candidate, source_archive)
            async with asyncio.timeout(30), self.session_factory.begin() as session:
                await stage_platform_requests(session, registration, member=self.member, runtime=self.runtime,
                    platforms=PERSONAL_DEV_PLATFORMS, now=datetime.now(UTC))
            pending = set(PERSONAL_DEV_PLATFORMS)
            while pending:
                for platform in sorted(pending):
                    receipt = await self.outcomes.observe(registration, platform=platform)
                    if receipt is None:
                        continue
                    result = self._result(receipt, registration, platform)
                    if result != "artifact-ready":
                        raise RuntimeError(f"native personal build {platform} ended {result}")
                    pending.remove(platform)
                if pending:
                    await asyncio.sleep(self.poll_interval_seconds)
            # The trusted exporter independently rechecks live accepted outcomes,
            # verifies both OCI archives, scans, and publishes immutable results.
            if self.exporter.accepted_artifact_resolver is not self.outcomes:
                raise ValueError("native platform publication resolver changed")
            return await self.exporter.publish(registration)

    async def cleanup(self, registration: CandidateRegistration) -> None:
        """Close this exact lease's demand, including when cleanup wins staging.

        Physical workers and capacity holds remain under protected manager
        recovery. Logical cancellation is not a physical-release receipt.
        """
        async with asyncio.timeout(30), self.session_factory.begin() as session:
            await cancel_platform_requests(session, registration, member=self.member, runtime=self.runtime,
                platforms=PERSONAL_DEV_PLATFORMS, now=datetime.now(UTC))


@dataclass(frozen=True, slots=True)
class NativePersonalDevBuildExecutorRouter:
    """Route global whole-attempt claims to their installed owner scope.

    A service reload constructs a new router; in-flight coordinators retain this
    immutable map so cleanup cannot silently switch installation generations.
    Unknown owners never fall back to a legacy or another owner's executor.
    """

    executors: Mapping[UUID, NativePersonalDevBuildExecutor]

    def __post_init__(self) -> None:
        entries = dict(self.executors)
        if (not 1 <= len(entries) <= 64
            or any(owner != executor.member.owner_id for owner, executor in entries.items())
            or len({(executor.member.configuration.subject_id, executor.member.configuration.subject_incarnation)
                for executor in entries.values()}) != len(entries)):
            raise ValueError("native build owner routing is invalid")
        object.__setattr__(self, "executors", MappingProxyType(entries))

    def _owner(self, registration: CandidateRegistration) -> NativePersonalDevBuildExecutor:
        executor = self.executors.get(registration.candidate.owner_user_id)
        if executor is None:
            raise ValueError("native personal build owner installation is unavailable")
        return executor

    async def build(self, registration: CandidateRegistration, *, source_archive: Path) -> Mapping[str, object]:
        return await self._owner(registration).build(registration, source_archive=source_archive)

    async def cleanup(self, registration: CandidateRegistration) -> None:
        await self._owner(registration).cleanup(registration)
