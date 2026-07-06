"""Ordered inventory of rollout steps (#340).

The driver's step list. Exported so both the CLI wire-up and tests can
build the same sequence without duplicating knowledge.
"""

from __future__ import annotations

from loom_cli.rollout.steps.base import Step
from loom_cli.rollout.steps.s00_resolve_target import ResolveTargetStep
from loom_cli.rollout.steps.s01_worktree import WorktreeStep
from loom_cli.rollout.steps.s02_build_images import BuildImagesStep
from loom_cli.rollout.steps.s03_kind_load_images import KindLoadImagesStep
from loom_cli.rollout.steps.s04_gb10_prep import GB10PrepStep
from loom_cli.rollout.steps.s05_backup import BackupStep
from loom_cli.rollout.steps.s06_audit import AuditStep
from loom_cli.rollout.steps.s07_render import RenderStep
from loom_cli.rollout.steps.s08_preflight import PreflightStep
from loom_cli.rollout.steps.s09_migrate import MigrateStep
from loom_cli.rollout.steps.s10_env_state import EnvStateStep
from loom_cli.rollout.steps.s11_cluster_up import ClusterUpStep
from loom_cli.rollout.steps.s12_production_defaults import ProductionDefaultsStep
from loom_cli.rollout.steps.s12_release_gate import ReleaseGateStep
from loom_cli.rollout.steps.s13_smoke import SmokeStep
from loom_cli.rollout.steps.s99_summary import SummaryStep


def default_step_sequence() -> list[Step]:
    """Return the standard 15-step + summary rollout sequence.

    The order is significant: dependencies flow left-to-right (worktree
    is required by build; build by kind-load; render by migrate; etc.).
    """
    return [
        ResolveTargetStep(),
        WorktreeStep(),
        BuildImagesStep(),
        KindLoadImagesStep(),
        GB10PrepStep(),
        BackupStep(),
        AuditStep(),
        RenderStep(),
        PreflightStep(),
        MigrateStep(),
        EnvStateStep(),
        ClusterUpStep(),
        ProductionDefaultsStep(),
        ReleaseGateStep(),
        SmokeStep(),
        SummaryStep(),
    ]


__all__ = ["default_step_sequence"]
