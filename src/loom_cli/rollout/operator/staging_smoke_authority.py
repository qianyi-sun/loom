"""Single staging admin-smoke authority shared by rehearsal and final gates."""

from __future__ import annotations

from loom.models.types import ModelSpec
from loom_cli.rollout.admin_smoke_contract import AdminSmokeAuthority
from loom_cli.rollout.steps.s13_smoke import (
    DEFAULT_CURRENT_GB10_REQUIRED_WORKER_POOL,
    DEFAULT_CURRENT_GB10_SMOKE_TASK_ID,
)

from .config import OperatorConfig

_ADMIN_ACTOR = "codex-v1-release-gate"
_STAGING_AGENT = "direct-completion"
_STAGING_AGENT_MODEL = ModelSpec(
    provider="yibu",
    name="gpt-4o-mini",
    source="local-server",
    local_server="yibu",
    max_output_tokens=64,
)


def staging_smoke_authority(config: OperatorConfig) -> AdminSmokeAuthority:
    if config.environment != "staging" or config.namespace != "loom-staging":
        raise ValueError("staging smoke authority escaped staging")
    return AdminSmokeAuthority(
        represented_username=config.smoke_on_behalf_username,
        team_id=config.smoke_on_behalf_team_id,
        admin_actor=_ADMIN_ACTOR,
        task_id=DEFAULT_CURRENT_GB10_SMOKE_TASK_ID,
        required_worker_pool=DEFAULT_CURRENT_GB10_REQUIRED_WORKER_POOL,
        agent=_STAGING_AGENT,
        agent_model=_STAGING_AGENT_MODEL,
    )


__all__ = ["staging_smoke_authority"]
