"""Object-storage byte accounting for TaskSet team quotas (#242 sub-plan 7)."""

from __future__ import annotations

import re
from typing import Any
from uuid import UUID

_TASKSET_SLUG_RE = re.compile(r"^[a-z0-9](?:[a-z0-9_-]*[a-z0-9])?$")


def _validated_taskset_slug(slug: str) -> str:
    if not isinstance(slug, str) or not _TASKSET_SLUG_RE.fullmatch(slug):
        raise ValueError("task set slug must use the canonical lowercase slug grammar")
    return slug


def _canonical_job_id(job_id: UUID) -> UUID:
    if not isinstance(job_id, UUID):
        raise ValueError("materialization job id must be a UUID")
    return job_id


def _canonical_epoch(epoch: int) -> int:
    if type(epoch) is not int or epoch < 0:
        raise ValueError("materialization epoch must be a nonnegative integer")
    return epoch


def taskset_root(*, team_id: UUID | str, slug: str) -> str:
    """Return the canonical, delimiter-terminated TaskSet root."""
    return f"tasksets/user/{team_id}/{_validated_taskset_slug(slug)}/"


def generation_root(*, team_id: UUID | str, slug: str) -> str:
    """Return the canonical, delimiter-terminated generated-output root."""
    return f"{taskset_root(team_id=team_id, slug=slug)}materializations/"


def generation_prefix(
    *,
    team_id: UUID | str,
    slug: str,
    job_id: UUID,
    epoch: int,
) -> str:
    """Return one exact, DB-derived staged-generation prefix."""
    return (
        f"{generation_root(team_id=team_id, slug=slug)}{_canonical_job_id(job_id)}/"
        f"{_canonical_epoch(epoch)}/"
    )


def generated_tasks_prefix(
    *,
    team_id: UUID | str,
    slug: str,
    job_id: UUID,
    epoch: int,
) -> str:
    """Return the canonical generated task-data prefix for one lease."""
    return f"{generation_prefix(team_id=team_id, slug=slug, job_id=job_id, epoch=epoch)}tasks/"


def prefix_storage_bytes(
    client: Any,
    *,
    bucket: str,
    prefix: str,
) -> int:
    """Sum object sizes under an S3 prefix."""
    total = 0
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            total += int(obj.get("Size", 0))
    return total


def team_taskset_storage_bytes(
    client: Any,
    *,
    bucket: str,
    team_id: UUID | str,
) -> int:
    """Total bytes stored for a team's TaskSet blobs."""
    prefix = f"tasksets/user/{team_id}/"
    return prefix_storage_bytes(client, bucket=bucket, prefix=prefix)


def team_storage_baseline_excluding_task_set(
    client: Any,
    *,
    bucket: str,
    team_id: UUID | str,
    slug: str,
) -> int:
    """Team storage excluding one TaskSet prefix (for rebuild rematerialization)."""
    team_prefix = f"tasksets/user/{team_id}/"
    task_prefix = f"{team_prefix}{slug}/"
    team_total = prefix_storage_bytes(client, bucket=bucket, prefix=team_prefix)
    task_bytes = prefix_storage_bytes(client, bucket=bucket, prefix=task_prefix)
    return max(0, team_total - task_bytes)
