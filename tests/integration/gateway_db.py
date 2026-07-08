"""Shared DB helpers for gateway integration tests (#86).

Gateway fixtures seed ``Team`` rows and often ``TeamQuota`` rows. Teardown
must delete child ``team_quotas`` rows before ``teams`` or Postgres raises
``team_quotas_team_id_fkey``.
"""

from __future__ import annotations

from uuid import UUID

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session

from loom.db.schema import Team, TeamQuota


def delete_all_teams_and_quotas(session: Session) -> None:
    """Delete all team_quotas rows, then all teams (FK order)."""
    session.execute(delete(TeamQuota))
    session.execute(delete(Team))


def delete_team_and_quota(session: Session, team_id: UUID) -> None:
    """Delete one team's quota row (if any), then the team row."""
    session.execute(delete(TeamQuota).where(TeamQuota.team_id == team_id))
    session.execute(delete(Team).where(Team.id == team_id))


def delete_teams_by_name(session: Session, name_pattern: str) -> None:
    """Delete teams (and their quotas) whose name matches a SQL LIKE pattern."""
    team_ids = session.scalars(
        select(Team.id).where(Team.name.like(name_pattern)),
    ).all()
    for team_id in team_ids:
        session.execute(delete(TeamQuota).where(TeamQuota.team_id == team_id))
    session.execute(delete(Team).where(Team.name.like(name_pattern)))


async def delete_teams_by_name_async(
    session: AsyncSession,
    name_pattern: str,
) -> None:
    """Async variant of :func:`delete_teams_by_name`."""
    team_ids = (
        await session.scalars(
            select(Team.id).where(Team.name.like(name_pattern)),
        )
    ).all()
    for team_id in team_ids:
        await session.execute(
            delete(TeamQuota).where(TeamQuota.team_id == team_id),
        )
    await session.execute(delete(Team).where(Team.name.like(name_pattern)))
