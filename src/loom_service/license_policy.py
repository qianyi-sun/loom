"""Service-side helpers for team license policy previews.

Control-plane trial submit remains the enforcement backstop. The service uses
the same policy inputs earlier so benchmark readiness, task preview counts, and
batch creation agree before fan-out reaches control-plane.
"""

from __future__ import annotations

from collections.abc import Iterable
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import TeamQuota
from loom.license_policy import DEFAULT_LICENSE_ALLOWLIST


def license_block_reason(task_license: str) -> str:
    return f"{task_license} not in team license allowlist"


async def load_team_license_allowlist(
    session: AsyncSession,
    team_id: UUID | None,
) -> tuple[str, ...] | None:
    """Return the current team's allowlist, or None when no team applies.

    Some legacy/dev teams can exist without a TeamQuota row. Control-plane
    upserts a quota row at submit time, so read-side previews use the same
    default allowlist instead of treating missing quota as unrestricted.
    """
    if team_id is None:
        return None
    allowlist = (
        await session.execute(
            select(TeamQuota.license_allowlist).where(
                TeamQuota.team_id == team_id,
            ),
        )
    ).scalar_one_or_none()
    if allowlist is None:
        return DEFAULT_LICENSE_ALLOWLIST
    return tuple(str(item) for item in allowlist)


def sorted_license_block_reasons(licenses: Iterable[str]) -> tuple[str, ...]:
    return tuple(license_block_reason(license_) for license_ in sorted(set(licenses)))
