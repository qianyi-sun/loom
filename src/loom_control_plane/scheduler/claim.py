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

#672 family-runs: a trial whose `family_key` is set is only claimable
when the matching `batch_family_state` row is `pending` and its
``task_sequence[current_index]`` equals the trial's task_id. The claim
also flips the family state from `pending` to `running` in the same
transaction so a second concurrent claim can't race the family gate.
"""

from __future__ import annotations

import json
from typing import Any
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.engine import RowMapping
from sqlalchemy.ext.asyncio import AsyncSession

# The family predicate uses ``task_sequence[current_index + 1]`` because
# Postgres arrays are 1-indexed while ``current_index`` counts from 0.
_CLAIM_SQL = text("""
WITH next AS (
  SELECT t.id, t.family_key, t.batch_id
    FROM trials t
    JOIN team_quotas q ON q.team_id = t.team_id
   WHERE t.state = 'queued'
     AND t.attempt_count < q.max_attempts_ceiling
     AND (t.next_attempt_at IS NULL OR t.next_attempt_at <= NOW())
     AND t.requires_caps->>'os' = ANY(:worker_os)
     AND (
       COALESCE(t.requires_caps->>'cpu_arch', 'x86_64') = 'any'
       OR COALESCE(t.requires_caps->>'cpu_arch', 'x86_64') = ANY(:worker_cpu_arches)
     )
     AND t.requires_caps->>'gpu_vendor' = ANY(:worker_gpu_vendors)
     AND (t.requires_caps->'network_policies') <@ (:worker_network_policies)::jsonb
     AND (
       t.family_key IS NULL
       OR EXISTS (
         SELECT 1
           FROM batch_family_state bfs
          WHERE bfs.batch_id = t.batch_id
            AND bfs.family_key = t.family_key
            AND bfs.state = 'pending'
            AND bfs.task_sequence[bfs.current_index + 1] = t.task_id
       )
     )
     AND EXISTS (
       SELECT 1
         FROM workers w
        WHERE w.id = (:worker_id)::uuid
          AND w.status = 'active'
          AND w.drain_state = 'active'
          AND (
            NULLIF(t.requires_caps->>'worker_pool', '') IS NULL
            OR w.pool_name = t.requires_caps->>'worker_pool'
          )
          -- #892: fence claims on a Slurm pool under active prod-pressure
          -- drain. GB10 pools are already fenced via w.drain_state; Slurm
          -- pools keep a single writer (the external actor), so the claim
          -- path reads the intent directly to stop new claims immediately.
          AND NOT EXISTS (
            SELECT 1
              FROM worker_pool_autoscaler_policies p
             WHERE p.pool_name = w.pool_name
               AND p.actuator = 'slurm'
               AND p.prod_pressure_state->>'state' = 'draining'
          )
     )
   ORDER BY
       (q.in_flight_count * 1.0) / NULLIF(q.fair_share_weight, 0) ASC,
       t.submit_priority DESC,
       t.submitted_at ASC
   LIMIT 1
   FOR UPDATE OF t SKIP LOCKED
),
-- Same-transaction family gate: flip the picked family to 'running' so a
-- concurrent claim on a sibling task blocks on the predicate above.
family_lock AS (
  UPDATE batch_family_state bfs
     SET state = 'running',
         updated_at = NOW()
    FROM next n
   WHERE n.family_key IS NOT NULL
     AND bfs.batch_id = n.batch_id
     AND bfs.family_key = n.family_key
     AND bfs.state = 'pending'
  RETURNING bfs.batch_id, bfs.family_key, bfs.state_uri
)
UPDATE trials t
   SET state = 'claimed',
       worker_id = :worker_id,
       claimed_at = NOW(),
       pre_start_heartbeat_at = NULL,
       failure_reason = NULL,
       failure_message = NULL,
       attempt_count = attempt_count + 1
  FROM next
 WHERE t.id = next.id
 RETURNING t.id, t.team_id, t.task_id, t.config, t.requires_caps,
           t.attempt_count, t.provider_connection_id,
           t.family_key, t.batch_id,
           (SELECT state_uri FROM family_lock) AS family_state_uri,
           (SELECT b.family_run_spec
              FROM batches b
             WHERE b.id = t.batch_id) AS family_run_spec;
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
