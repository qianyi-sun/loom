"""DRF SQL claim query (spec §2.6).

One ordered query with `FOR UPDATE SKIP LOCKED` that atomically transitions
the chosen trial from `queued` to `claimed`.

Ordering (most → least important):
1. Lowest `in_flight_count / fair_share_weight` (Dominant Resource Fairness)
2. Highest `submit_priority`
3. Oldest `submitted_at`

`mounted_fs` is intentionally omitted from the eligibility predicate — per
spec §3.1.1 v1's derive_requires_caps does not emit a mounted_fs requirement.
If we add it later, the matching predicate goes here and the helper signature
grows the corresponding parameter.
"""

from __future__ import annotations

import json
from typing import Any
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.engine import RowMapping
from sqlalchemy.ext.asyncio import AsyncSession

_CLAIM_SQL = text("""
WITH next AS (
  SELECT t.id
    FROM trials t
    JOIN team_quotas q ON q.team_id = t.team_id
   WHERE t.state = 'queued'
     AND t.attempt_count < q.max_attempts
     AND (t.next_attempt_at IS NULL OR t.next_attempt_at <= NOW())
     AND t.requires_caps->>'os' = ANY(:worker_os)
     AND (
       COALESCE(t.requires_caps->>'cpu_arch', 'x86_64') = 'any'
       OR COALESCE(t.requires_caps->>'cpu_arch', 'x86_64') = ANY(:worker_cpu_arches)
     )
     AND t.requires_caps->>'gpu_vendor' = ANY(:worker_gpu_vendors)
     AND (t.requires_caps->'network_policies') <@ (:worker_network_policies)::jsonb
     AND EXISTS (
       SELECT 1
         FROM workers w
        WHERE w.id = (:worker_id)::uuid
          AND w.status = 'active'
          AND w.drain_state = 'active'
     )
   ORDER BY
       (q.in_flight_count * 1.0) / NULLIF(q.fair_share_weight, 0) ASC,
       t.submit_priority DESC,
       t.submitted_at ASC
   LIMIT 1
   FOR UPDATE OF t SKIP LOCKED
)
UPDATE trials t
   SET state = 'claimed',
       worker_id = :worker_id,
       claimed_at = NOW(),
       attempt_count = attempt_count + 1
  FROM next
 WHERE t.id = next.id
 RETURNING t.id, t.team_id, t.task_id, t.config, t.requires_caps,
           t.attempt_count, t.provider_connection_id;
""")


async def claim_one(
    session: AsyncSession,
    *,
    worker_id: UUID,
    worker_os: list[str],
    worker_cpu_arches: list[str],
    worker_gpu_vendors: list[str],
    worker_network_policies: list[str],
) -> RowMapping | None:
    """Claim the next eligible trial. Returns the claimed trial row (RowMapping)
    or None if the queue is empty / nothing matches caps.
    """
    params: dict[str, Any] = {
        "worker_id": worker_id,
        "worker_os": worker_os,
        "worker_cpu_arches": worker_cpu_arches,
        "worker_gpu_vendors": worker_gpu_vendors,
        "worker_network_policies": json.dumps(worker_network_policies),
    }
    result = await session.execute(_CLAIM_SQL, params)
    return result.mappings().one_or_none()
