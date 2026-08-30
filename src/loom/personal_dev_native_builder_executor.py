"""Management-side executor for durable native personal-dev build grants."""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from loom.db.schema import PersonalDevNativeBuildGrant
from loom.personal_dev_candidate import CandidateRegistration, PersonalDevPlatform
from loom.personal_dev_native_builder_store import (
    NativeBuilderGrantPolicy,
    cancel_native_build_grant,
    get_native_build_grant,
    issue_native_build_grant,
)

_NATIVE_PLATFORM: PersonalDevPlatform = "linux/arm64"


class NativeBuilderGrantAuthority(Protocol):
    async def issue(
        self,
        registration: CandidateRegistration,
        policy: NativeBuilderGrantPolicy,
        now: datetime,
    ) -> PersonalDevNativeBuildGrant: ...

    async def get(
        self,
        attempt_id: UUID,
        attempt_lease_epoch: int,
        platform: PersonalDevPlatform,
    ) -> PersonalDevNativeBuildGrant | None: ...

    async def cancel(
        self,
        attempt_id: UUID,
        attempt_lease_epoch: int,
        platform: PersonalDevPlatform,
        now: datetime,
    ) -> bool: ...


class SqlAlchemyNativeBuilderGrantAuthority:
    """Bind durable grant functions to one short-lived database session."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def issue(
        self,
        registration: CandidateRegistration,
        policy: NativeBuilderGrantPolicy,
        now: datetime,
    ) -> PersonalDevNativeBuildGrant:
        return await issue_native_build_grant(self._session, registration, policy, now)

    async def get(
        self,
        attempt_id: UUID,
        attempt_lease_epoch: int,
        platform: PersonalDevPlatform,
    ) -> PersonalDevNativeBuildGrant | None:
        return await get_native_build_grant(
            self._session,
            attempt_id,
            attempt_lease_epoch,
            platform,
        )

    async def cancel(
        self,
        attempt_id: UUID,
        attempt_lease_epoch: int,
        platform: PersonalDevPlatform,
        now: datetime,
    ) -> bool:
        return await cancel_native_build_grant(
            self._session,
            attempt_id,
            attempt_lease_epoch,
            platform,
            now,
        )


NativeBuilderGrantAuthorityFactory = Callable[
    [AsyncSession],
    NativeBuilderGrantAuthority,
]
NativeBuilderGrantPolicyFactory = Callable[
    [CandidateRegistration],
    NativeBuilderGrantPolicy,
]


def _attempt_binding(registration: CandidateRegistration) -> tuple[UUID, int]:
    attempt = registration.build_attempt
    if (
        attempt is None
        or attempt.candidate_id != registration.candidate.id
        or attempt.state != "running"
        or attempt.lease_epoch <= 0
    ):
        raise ValueError("personal-dev native builder attempt is unavailable")
    return attempt.id, attempt.lease_epoch


def _grant_matches(
    grant: PersonalDevNativeBuildGrant,
    registration: CandidateRegistration,
) -> bool:
    attempt_id, lease_epoch = _attempt_binding(registration)
    return (
        grant.candidate_id == registration.candidate.id
        and grant.attempt_id == attempt_id
        and grant.attempt_lease_epoch == lease_epoch
        and grant.platform == _NATIVE_PLATFORM
    )


@dataclass(slots=True)
class NativeAgentPersonalDevPlatformBuildExecutor:
    """Issue and wait for one exact arm64 grant without holding a DB session."""

    session_factory: async_sessionmaker[AsyncSession]
    policy_factory: NativeBuilderGrantPolicyFactory
    wait_timeout_seconds: float
    poll_interval_seconds: float = 1.0
    authority_factory: NativeBuilderGrantAuthorityFactory = (
        SqlAlchemyNativeBuilderGrantAuthority
    )

    def __post_init__(self) -> None:
        if not callable(self.session_factory) or not callable(self.policy_factory):
            raise ValueError("personal-dev native builder executor authority is invalid")
        if not callable(self.authority_factory):
            raise ValueError("personal-dev native builder executor store is invalid")
        if (
            not isinstance(self.wait_timeout_seconds, (int, float))
            or isinstance(self.wait_timeout_seconds, bool)
            or not 0 < self.wait_timeout_seconds <= 7200
            or not isinstance(self.poll_interval_seconds, (int, float))
            or isinstance(self.poll_interval_seconds, bool)
            or self.poll_interval_seconds <= 0
        ):
            raise ValueError("personal-dev native builder executor timing is invalid")

    async def _issue(
        self,
        registration: CandidateRegistration,
        policy: NativeBuilderGrantPolicy,
    ) -> PersonalDevNativeBuildGrant:
        async with self.session_factory() as session:
            return await self.authority_factory(session).issue(
                registration,
                policy,
                datetime.now(UTC),
            )

    async def _get(
        self,
        registration: CandidateRegistration,
    ) -> PersonalDevNativeBuildGrant | None:
        attempt_id, lease_epoch = _attempt_binding(registration)
        async with self.session_factory() as session:
            return await self.authority_factory(session).get(
                attempt_id,
                lease_epoch,
                _NATIVE_PLATFORM,
            )

    async def build_platform(
        self,
        registration: CandidateRegistration,
        *,
        source_archive: Path,
    ) -> None:
        del source_archive
        _attempt_binding(registration)
        policy = self.policy_factory(registration)
        try:
            try:
                async with asyncio.timeout(self.wait_timeout_seconds):
                    issued = await self._issue(registration, policy)
                    if not _grant_matches(issued, registration):
                        raise RuntimeError(
                            "personal-dev native builder grant binding is invalid"
                        )
                    while True:
                        grant = await self._get(registration)
                        if grant is None or not _grant_matches(grant, registration):
                            raise RuntimeError(
                                "personal-dev native builder grant is unavailable"
                            )
                        if grant.id != issued.id:
                            raise RuntimeError(
                                "personal-dev native builder grant identity changed"
                            )
                        if grant.state == "succeeded":
                            return
                        if grant.state == "failed":
                            raise RuntimeError("personal-dev native builder grant failed")
                        if grant.state == "cancelled":
                            raise RuntimeError(
                                "personal-dev native builder grant was cancelled"
                            )
                        if grant.state not in {"queued", "running"}:
                            raise RuntimeError(
                                "personal-dev native builder grant state is invalid"
                            )
                        await asyncio.sleep(self.poll_interval_seconds)
            except TimeoutError:
                await self.cleanup_platform(registration)
                raise TimeoutError(
                    "personal-dev native builder grant deadline expired"
                ) from None
        except asyncio.CancelledError:
            with contextlib.suppress(Exception):
                await self.cleanup_platform(registration)
            raise

    async def cleanup_platform(self, registration: CandidateRegistration) -> None:
        attempt_id, lease_epoch = _attempt_binding(registration)
        async with self.session_factory() as session:
            await self.authority_factory(session).cancel(
                attempt_id,
                lease_epoch,
                _NATIVE_PLATFORM,
                datetime.now(UTC),
            )


__all__ = [
    "NativeAgentPersonalDevPlatformBuildExecutor",
    "NativeBuilderGrantAuthority",
    "NativeBuilderGrantAuthorityFactory",
    "NativeBuilderGrantPolicyFactory",
    "SqlAlchemyNativeBuilderGrantAuthority",
]
