"""Object-storage byte accounting for TaskSet team quotas (#242 sub-plan 7)."""

from __future__ import annotations

from typing import Any
from uuid import UUID


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
