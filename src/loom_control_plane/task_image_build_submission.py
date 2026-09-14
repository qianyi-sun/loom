"""Transaction-owning, one-invocation builder submission composition.

The provider is still disabled in deployment and has no production command
runner. This coordinator deliberately does not bind, release, cancel, or certify
cleanup. A returned job number is only a hint for authoritative reconciliation.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from loom.db.schema import TaskImageBuildGrant
from loom_control_plane.task_image_build_environment import (
    BuildEnvironmentDisabledError,
    SlurmBuildEnvironmentProvider,
    SlurmBuildGrantV2,
)
from loom_control_plane.task_image_build_grants import (
    TaskImageBuildGrantConflictError,
    _stored_grant,
    begin_task_image_build_submission,
)


class TaskImageBuildSubmissionUncertainError(RuntimeError):
    """The consumed invocation may have created a held job; never resubmit."""


@dataclass(frozen=True, slots=True)
class TaskImageBuildSubmissionReceipt:
    """Advisory transport receipt, not a Slurm binding or release authority."""

    grant_id: UUID
    reported_job_id: str


def _utc_now() -> datetime:
    return datetime.now(UTC)


class TaskImageBuildSubmissionCoordinator:
    """Own and finish the DB transaction before entering an external provider.

    Only the winner of the locked issued-to-submitting transition may invoke
    the provider. A definite rollback before commit leaves authority unconsumed;
    committed consumption survives lost acknowledgement, restart, provider error
    and cancellation. Even a crash after commit but before the actual send must
    recover through inventory, not submission.

    The eventual fixed helper must separately enforce its principal, grant
    deadline, pinned commands and outstanding-command fence. Returning or
    cancelling this coroutine does not prove that a remote command has settled.
    """

    def __init__(
        self,
        *,
        session_factory: async_sessionmaker[AsyncSession],
        environment: str,
        provider: SlurmBuildEnvironmentProvider,
        clock: Callable[[], datetime] = _utc_now,
    ) -> None:
        if re.fullmatch(r"[A-Za-z0-9_.-]+", environment) is None:
            raise ValueError("builder coordinator environment is invalid")
        self.session_factory = session_factory
        self.environment = environment
        self.provider = provider
        self.clock = clock

    async def submit_once(self, grant_id: UUID) -> TaskImageBuildSubmissionReceipt:
        if not self.provider.policy.enabled or self.provider.policy.activation_blockers:
            raise BuildEnvironmentDisabledError("rootless builder provider is disabled")
        # Separate session and transaction scopes so a commit/acknowledgement
        # failure cannot skip session cleanup in the combined factory context.
        async with self.session_factory() as session, session.begin():
            row = await session.scalar(
                select(TaskImageBuildGrant)
                .where(TaskImageBuildGrant.id == grant_id)
                .with_for_update()
            )
            if row is None or row.environment != self.environment:
                raise TaskImageBuildGrantConflictError("builder grant is outside coordinator scope")
            grant = _stored_grant(row)
            if not isinstance(grant, SlurmBuildGrantV2):
                raise TaskImageBuildGrantConflictError("builder submission requires V2 authority")
            # Check exact provider/release/resource policy before consuming. The
            # provider repeats this validation at its own boundary.
            self.provider.render_submission(grant)
            now = self.clock()
            if now < grant.authority.issued_at:
                raise TaskImageBuildGrantConflictError("builder grant is not yet valid")
            await begin_task_image_build_submission(session, grant_id=grant_id, now=now)

        # Both commit AND session close have succeeded. Do not move any external
        # call into the transaction above, or put this call in a retry loop.
        if not grant.authority.issued_at <= self.clock() < grant.authority.expires_at:
            raise TaskImageBuildGrantConflictError(
                "builder grant outside validity after invocation commit"
            )
        try:
            job_id = await self.provider.submit_once(grant)
            if (
                not isinstance(job_id, str)
                or re.fullmatch(r"[1-9][0-9]{0,9}", job_id) is None
                or int(job_id) >= 0xFFFFFFFF
            ):
                raise ValueError("invalid submission receipt")
        except Exception:
            # Untrusted process output must not be copied into service errors.
            # CancelledError/BaseException propagate, retaining durable consumption.
            raise TaskImageBuildSubmissionUncertainError(
                "builder submission outcome requires authoritative inventory; do not retry"
            ) from None
        return TaskImageBuildSubmissionReceipt(grant_id=grant_id, reported_job_id=job_id)


__all__ = [
    "TaskImageBuildSubmissionCoordinator",
    "TaskImageBuildSubmissionReceipt",
    "TaskImageBuildSubmissionUncertainError",
]
