"""Candidate CLI arguments for broker-bound backup traversal limits."""

from __future__ import annotations

from loom_cli.rollout.context import RolloutContext


def backup_traversal_limit_argv(ctx: RolloutContext) -> list[str]:
    limits = ctx.backup_traversal_limits()
    if limits is None:
        return []
    max_files, max_entries, max_total_bytes = limits
    return [
        "--backup-max-files",
        str(max_files),
        "--backup-max-entries",
        str(max_entries),
        "--backup-max-total-bytes",
        str(max_total_bytes),
    ]


__all__ = ["backup_traversal_limit_argv"]
