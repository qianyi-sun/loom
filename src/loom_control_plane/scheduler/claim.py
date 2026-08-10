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

import hmac
import json
import secrets
from dataclasses import dataclass
from hashlib import sha256
from typing import Any
from uuid import UUID, uuid4

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
            (
              NULLIF(t.requires_caps->>'worker_pool', '') IS NOT NULL
              AND w.pool_name = t.requires_caps->>'worker_pool'
            )
            OR (
              NULLIF(t.requires_caps->>'worker_pool', '') IS NULL
              AND (
                t.autoscaler_pool_name IS NULL
                OR w.pool_name = t.autoscaler_pool_name
              )
            )
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
          AND (
            NOT :enforce_shared_slot
            OR w.max_concurrent > (
              (SELECT count(*) FROM trials active_trial
                WHERE active_trial.worker_id = w.id
                  AND active_trial.state IN ('claimed','running'))
              +
              (SELECT count(*) FROM execution_attempts active_attempt
                WHERE active_attempt.worker_id = w.id
                  AND active_attempt.state IN ('claimed','running'))
            )
          )
          AND NOT EXISTS (
            SELECT 1
              FROM pipeline_acceptance_preflight_prerequisites fence
             WHERE fence.worker_id = w.id
               AND fence.fence_state = 'active'
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
    enforce_shared_slot: bool = False,
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
        "enforce_shared_slot": enforce_shared_slot,
    }
    result = await session.execute(_CLAIM_SQL, params)
    return result.mappings().one_or_none()


@dataclass(frozen=True)
class WorkClaimConflictError(Exception):
    reason: str


_WORKER_CLAIM_GUARD_SQL = text("""
SELECT w.id,
       w.max_concurrent,
       w.status,
       w.drain_state,
       w.supported_work_kinds,
       w.capability_snapshot_digest,
       w.auth_token_hash,
       w.lease_epoch,
       (
         (SELECT count(*) FROM trials t
           WHERE t.worker_id = w.id AND t.state IN ('claimed','running'))
         +
         (SELECT count(*) FROM execution_attempts a
           WHERE a.worker_id = w.id AND a.state IN ('claimed','running'))
       ) AS active_count
  FROM workers w
 WHERE w.id = (:worker_id)::uuid
 FOR UPDATE
""")


_WORK_CLAIM_SQL = text("""
WITH candidates AS (
  SELECT 'trial'::text AS work_kind,
         t.id,
         t.team_id,
         t.submit_priority,
         t.submitted_at AS queued_at,
         q.in_flight_count,
         q.fair_share_weight,
         t.family_key,
         t.batch_id
    FROM trials t
    JOIN team_quotas q ON q.team_id = t.team_id
    JOIN workers w ON w.id = (:worker_id)::uuid
   WHERE 'trial' = ANY((:supported_work_kinds)::text[])
     AND t.state = 'queued'
     AND t.attempt_count < q.max_attempts_ceiling
     AND (t.next_attempt_at IS NULL OR t.next_attempt_at <= NOW())
     AND t.requires_caps->>'os' = ANY(:worker_os)
     AND (COALESCE(t.requires_caps->>'cpu_arch', 'x86_64') = 'any'
          OR COALESCE(t.requires_caps->>'cpu_arch', 'x86_64') = ANY(:worker_cpu_arches))
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
     AND (
       (
         NULLIF(t.requires_caps->>'worker_pool', '') IS NOT NULL
         AND w.pool_name = t.requires_caps->>'worker_pool'
       )
       OR (
         NULLIF(t.requires_caps->>'worker_pool', '') IS NULL
         AND (t.autoscaler_pool_name IS NULL OR w.pool_name = t.autoscaler_pool_name)
       )
     )
     AND NOT EXISTS (
       SELECT 1
         FROM worker_pool_autoscaler_policies p
        WHERE p.pool_name = w.pool_name
          AND p.actuator = 'slurm'
          AND p.prod_pressure_state->>'state' = 'draining'
     )
     AND NOT EXISTS (
       SELECT 1 FROM pipeline_acceptance_preflight_prerequisites fence
        WHERE fence.worker_id = w.id AND fence.fence_state = 'active'
     )
  UNION ALL
  SELECT 'execution_attempt'::text AS work_kind,
         a.id,
         r.team_id,
         0 AS submit_priority,
         a.queued_at,
         q.in_flight_count,
         q.fair_share_weight,
         NULL::text AS family_key,
         NULL::uuid AS batch_id
    FROM execution_attempts a
    JOIN pipeline_stage_runs s ON s.id = a.stage_run_id
    JOIN pipeline_runs r ON r.id = s.pipeline_run_id
    JOIN pipeline_budget_ledgers ledger ON ledger.pipeline_run_id = r.id
    JOIN team_quotas q ON q.team_id = r.team_id
    JOIN workers w ON w.id = (:worker_id)::uuid
   WHERE 'execution_attempt' = ANY((:supported_work_kinds)::text[])
     AND a.state = 'queued'
     AND s.state = 'queued'
     AND s.resolved_execution_spec_json IS NOT NULL
     AND s.execution_spec_digest IS NOT NULL
     AND s.resolved_input_bindings_json IS NOT NULL
     AND s.resource_profile_json IS NOT NULL
     AND s.resource_profile_digest IS NOT NULL
     AND s.image_runtime_contract_json IS NOT NULL
     AND s.image_runtime_contract_digest IS NOT NULL
     AND EXISTS (
       SELECT 1
         FROM jsonb_array_elements(w.capabilities) capability
        WHERE capability->>'os' = 'linux'
          AND COALESCE(capability->>'cpu_arch', 'x86_64') =
              s.image_runtime_contract_json->>'cpu_arch'
          AND capability->>'gpu_vendor' =
              s.image_runtime_contract_json->>'gpu_vendor'
          AND (
            (s.resource_profile_json->>'network_profile' = 'none'
             AND capability->'network_policies' ? 'no-network')
            OR
            (s.resource_profile_json->>'network_profile' = 'gateway'
             AND capability->'network_policies' ? 'allowlist')
          )
     )
     AND r.state IN ('submitted','running')
     AND ledger.terminal_cause IS NULL
     AND ledger.wall_deadline_at > NOW()
     AND (
       NOT EXISTS (
         SELECT 1 FROM pipeline_acceptance_preflight_prerequisites any_fence
          WHERE any_fence.worker_id = w.id AND any_fence.fence_state = 'active'
       )
       OR EXISTS (
         SELECT 1 FROM pipeline_acceptance_preflight_prerequisites fence
          WHERE fence.worker_id = w.id
            AND fence.fence_state = 'active'
            AND fence.pipeline_run_id = r.id
            AND fence.worker_capability_snapshot_digest = w.capability_snapshot_digest
            AND fence.worker_lease_epoch = w.lease_epoch
            AND (
              (s.node_key LIKE '%acceptance_preflight_cold'
               AND fence.state = 'satisfied')
              OR
              (s.node_key LIKE '%acceptance_preflight_warm'
               AND fence.state = 'consumed')
            )
       )
     )
), picked AS (
  SELECT work_kind, id, family_key, batch_id
    FROM candidates
   ORDER BY (in_flight_count * 1.0) / NULLIF(fair_share_weight, 0) ASC,
            submit_priority DESC,
            queued_at ASC,
            work_kind ASC,
            id ASC
   LIMIT 1
), family_lock AS (
  UPDATE batch_family_state bfs
     SET state = 'running', updated_at = NOW()
    FROM picked p
   WHERE p.work_kind = 'trial'
     AND p.family_key IS NOT NULL
     AND bfs.batch_id = p.batch_id
     AND bfs.family_key = p.family_key
     AND bfs.state = 'pending'
  RETURNING bfs.batch_id, bfs.family_key
), claimed_trial AS (
  UPDATE trials t
     SET state = 'claimed', worker_id = (:worker_id)::uuid, claimed_at = NOW(),
         pre_start_heartbeat_at = NULL, failure_reason = NULL, failure_message = NULL,
         attempt_count = attempt_count + 1
    FROM picked p
   WHERE p.work_kind = 'trial' AND t.id = p.id AND t.state = 'queued'
     AND (
       p.family_key IS NULL
       OR EXISTS (
         SELECT 1 FROM family_lock locked
          WHERE locked.batch_id = p.batch_id
            AND locked.family_key = p.family_key
       )
     )
  RETURNING 'trial'::text AS work_kind, t.id, t.team_id, NULL::uuid AS stage_run_id,
            NULL::uuid AS pipeline_run_id, NULL::uuid AS claim_id,
            NULL::bigint AS lease_epoch, NULL::timestamptz AS lease_expires_at,
            t.task_id, t.config, t.requires_caps, t.attempt_count,
            t.provider_connection_id, t.family_key, t.batch_id
), claimed_attempt AS (
  UPDATE execution_attempts a
     SET state = 'claimed', worker_id = (:worker_id)::uuid,
         claim_id = (:claim_id)::uuid, lease_epoch = a.lease_epoch + 1,
         lease_token_digest = :lease_token_digest,
         lease_expires_at = NOW() + INTERVAL '60 seconds', claimed_at = NOW(),
         version = a.version + 1
    FROM picked p, pipeline_stage_runs s, pipeline_runs r
   WHERE p.work_kind = 'execution_attempt' AND a.id = p.id AND a.state = 'queued'
     AND s.id = a.stage_run_id AND s.state = 'queued'
     AND r.id = s.pipeline_run_id AND r.state IN ('submitted','running')
  RETURNING 'execution_attempt'::text AS work_kind, a.id,
            r.team_id, a.stage_run_id, r.id AS pipeline_run_id, a.claim_id,
            a.lease_epoch, a.lease_expires_at, NULL::text AS task_id,
            NULL::jsonb AS config, NULL::jsonb AS requires_caps,
            a.attempt_number, NULL::uuid AS provider_connection_id,
            NULL::text AS family_key, NULL::uuid AS batch_id
), acceptance_consume AS (
  UPDATE pipeline_acceptance_preflight_prerequisites fence
     SET state = 'consumed',
         consumed_attempt_id = a.id,
         consumed_at = NOW(),
         version = fence.version + 1
    FROM claimed_attempt a, pipeline_stage_runs s
   WHERE s.id = a.stage_run_id
     AND s.node_key LIKE '%acceptance_preflight_cold'
     AND fence.pipeline_run_id = a.pipeline_run_id
     AND fence.fence_state = 'active'
     AND fence.state = 'satisfied'
  RETURNING a.id AS execution_attempt_id
), stage_claim AS (
  UPDATE pipeline_stage_runs s
     SET state = 'claimed', claimed_at = NOW(), version = s.version + 1
   FROM claimed_attempt a
   WHERE s.id = a.stage_run_id AND s.state = 'queued'
     AND (
       s.node_key NOT LIKE '%acceptance_preflight_cold'
       OR EXISTS (
         SELECT 1 FROM acceptance_consume consumed
          WHERE consumed.execution_attempt_id = a.id
       )
     )
  RETURNING s.id
)
SELECT * FROM claimed_trial
UNION ALL
SELECT a.* FROM claimed_attempt a JOIN stage_claim s ON s.id = a.stage_run_id
""")


async def claim_work(
    session: AsyncSession,
    *,
    worker_id: UUID,
    capability_snapshot_digest: str,
    worker_token_hash: bytes,
    supported_work_kinds: list[str],
    free_slots: int,
    worker_os: list[str],
    worker_cpu_arches: list[str],
    worker_gpu_vendors: list[str],
    worker_network_policies: list[str],
) -> tuple[RowMapping, str | None] | None:
    """Atomically select one Trial or ExecutionAttempt from the shared queue."""

    guard = (
        await session.execute(_WORKER_CLAIM_GUARD_SQL, {"worker_id": worker_id})
    ).mappings().one_or_none()
    if guard is None:
        raise WorkClaimConflictError("worker_unknown")
    if guard["status"] != "active" or guard["drain_state"] != "active":
        raise WorkClaimConflictError("worker_not_active")
    if guard["auth_token_hash"] is None or not hmac.compare_digest(
        bytes(guard["auth_token_hash"]), worker_token_hash
    ):
        raise WorkClaimConflictError("worker_identity_mismatch")
    if guard["capability_snapshot_digest"] != capability_snapshot_digest:
        raise WorkClaimConflictError("capability_snapshot_drift")
    registered_kinds = list(guard["supported_work_kinds"] or ["trial"])
    if supported_work_kinds != registered_kinds:
        raise WorkClaimConflictError("supported_work_kinds_drift")
    if free_slots < 1 or free_slots > int(guard["max_concurrent"]):
        raise WorkClaimConflictError("invalid_free_slots")
    if int(guard["active_count"]) >= int(guard["max_concurrent"]):
        raise WorkClaimConflictError("worker_capacity_exhausted")

    raw_lease_token = secrets.token_urlsafe(32)
    params: dict[str, Any] = {
        "worker_id": worker_id,
        "supported_work_kinds": registered_kinds,
        "worker_os": worker_os,
        "worker_cpu_arches": worker_cpu_arches,
        "worker_gpu_vendors": worker_gpu_vendors,
        "worker_network_policies": json.dumps(worker_network_policies),
        "claim_id": uuid4(),
        "lease_token_digest": sha256(raw_lease_token.encode()).hexdigest(),
    }
    result = (await session.execute(_WORK_CLAIM_SQL, params)).mappings().one_or_none()
    if result is None:
        return None
    if result["work_kind"] == "execution_attempt":
        event = (
            await session.execute(
                text("""
                    UPDATE pipeline_runs
                       SET state = CASE WHEN state = 'submitted' THEN 'running' ELSE state END,
                           started_at = COALESCE(started_at, NOW()),
                           next_event_seq = next_event_seq + 1,
                           version = version + 1
                     WHERE id = (:run_id)::uuid
                 RETURNING next_event_seq - 1 AS seq
                """),
                {"run_id": result["pipeline_run_id"]},
            )
        ).mappings().one()
        await session.execute(
            text("""
                INSERT INTO pipeline_events (
                    pipeline_run_id, seq, stage_run_id, execution_attempt_id,
                    event_type, actor_kind, actor_id, payload_json
                ) VALUES (
                    (:run_id)::uuid, :seq, (:stage_id)::uuid, (:attempt_id)::uuid,
                    'execution_attempt_claimed', 'worker', (:worker_id)::text,
                    jsonb_build_object('claim_id', (:claim_id)::text,
                                       'lease_epoch', :lease_epoch)
                )
            """),
            {
                "run_id": result["pipeline_run_id"],
                "seq": event["seq"],
                "stage_id": result["stage_run_id"],
                "attempt_id": result["id"],
                "worker_id": worker_id,
                "claim_id": result["claim_id"],
                "lease_epoch": result["lease_epoch"],
            },
        )
    return result, raw_lease_token if result["work_kind"] == "execution_attempt" else None
