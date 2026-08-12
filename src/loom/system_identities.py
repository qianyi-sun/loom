"""Reserved database identities owned by Loom deployments.

These values are deliberately stable across releases. Callers must match the
identifier and expected name, not discover a system identity by display name.
"""

from __future__ import annotations

from uuid import UUID

from sqlalchemy import func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import TeamMembership, Token, User, UserSession

TASKSET_FENCE_CANARY_TEAM_ID = UUID("2c9506e1-7d5e-4b49-b532-4b8f0a3f5ea9")
TASKSET_FENCE_CANARY_TEAM_NAME = "loom-system-taskset-fence-canary"

PIPELINE_ACCEPTANCE_CONTROLLER_USER_ID = UUID("00000000-0000-4000-8000-000000001216")
PIPELINE_ACCEPTANCE_CONTROLLER_USERNAME = "loom-pipeline-acceptance-controller"


async def assert_pipeline_controller_identity(session: AsyncSession) -> int:
    """Fail startup if the reserved controller becomes human-authenticatable."""

    user = await session.get(User, PIPELINE_ACCEPTANCE_CONTROLLER_USER_ID)
    if (
        user is None
        or user.username != PIPELINE_ACCEPTANCE_CONTROLLER_USERNAME
        or user.username_normalized != PIPELINE_ACCEPTANCE_CONTROLLER_USERNAME
        or user.email is not None
        or user.password_hash is not None
        or user.password_set_at is not None
        or user.is_platform_admin
        or user.disabled_at is not None
    ):
        raise RuntimeError("reserved Pipeline controller identity collision or drift")
    authority_rows = (
        await session.execute(
            select(func.count())
            .select_from(User)
            .where(
                User.id == PIPELINE_ACCEPTANCE_CONTROLLER_USER_ID,
                or_(
                    User.username != PIPELINE_ACCEPTANCE_CONTROLLER_USERNAME,
                    User.username_normalized != PIPELINE_ACCEPTANCE_CONTROLLER_USERNAME,
                ),
            )
        )
    ).scalar_one()
    memberships = (
        await session.execute(
            select(func.count()).select_from(TeamMembership).where(
                TeamMembership.user_id == PIPELINE_ACCEPTANCE_CONTROLLER_USER_ID
            )
        )
    ).scalar_one()
    sessions = (
        await session.execute(
            select(func.count()).select_from(UserSession).where(
                UserSession.user_id == PIPELINE_ACCEPTANCE_CONTROLLER_USER_ID
            )
        )
    ).scalar_one()
    tokens = (
        await session.execute(
            select(func.count()).select_from(Token).where(
                Token.created_by_user_id == PIPELINE_ACCEPTANCE_CONTROLLER_USER_ID
            )
        )
    ).scalar_one()
    if authority_rows or memberships or sessions or tokens:
        raise RuntimeError("reserved Pipeline controller identity has login authority")
    return 1
