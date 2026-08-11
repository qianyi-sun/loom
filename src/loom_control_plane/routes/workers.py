"""Worker-facing endpoints: claim, register, heartbeat."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

from fastapi import APIRouter, Header, HTTPException, Request, Response
from fastapi.responses import JSONResponse
from pydantic import ValidationError
from sqlalchemy import insert, text, update

from loom.auth import verify_bearer_token
from loom.db.schema import Worker
from loom.models.capabilities import Capabilities
from loom.models.result import FailureReason
from loom.pipeline.keys import canonical_digest
from loom.pipeline.work_protocol import (
    AcceptancePreflightGrantV1,
    ArtifactInputDescriptorV1,
    ExecutionAttemptClaimV1,
    StageRequestGrantV1,
    TrialClaimV1,
    WorkClaimRequestV1,
    WorkClaimV1,
)
from loom_control_plane.metrics import CLAIM_LATENCY_SEC
from loom_control_plane.scheduler.claim import (
    WorkClaimConflictError,
    claim_one,
    claim_work,
)

router = APIRouter()

_WORKER_HEARTBEAT_STATUSES = {"active", "idle-exit", "shutting-down"}

_REQUEUE_TRIAL_RETRY_SQL = text("""
UPDATE trials t
   SET state = 'queued',
       worker_id = NULL,
       failure_reason = (:failure_reason)::text,
       failure_message = (:failure_message)::text,
       attempt_count = CASE
           WHEN (:failure_reason)::text = 'node_setup_health'
           THEN GREATEST(t.attempt_count - 1, 0)
           ELSE t.attempt_count
       END,
       next_attempt_at = NOW() + (:retry_after_sec)::double precision
                         * INTERVAL '1 second'
  FROM team_quotas q
 WHERE t.id = (:trial_id)::uuid
   AND t.worker_id = (:worker_id)::uuid
   AND t.state = 'claimed'
   AND t.started_at IS NULL
   AND q.team_id = t.team_id
   AND (
       (:failure_reason)::text = 'node_setup_health'
       OR t.attempt_count < q.max_attempts_ceiling
   )
 RETURNING t.id;
""")

_PRE_START_HEARTBEAT_SQL = text("""
UPDATE trials
   SET pre_start_heartbeat_at = NOW()
 WHERE id = (:trial_id)::uuid
   AND worker_id = (:worker_id)::uuid
   AND state = 'claimed'
   AND started_at IS NULL
 RETURNING id, pre_start_heartbeat_at;
""")


@router.post("/trials/claim")
async def claim_trial(
    request: Request,
    payload: dict[str, Any],
    authorization: str | None = Header(default=None),
) -> Response:
    async with request.app.state.session_factory() as session:
        ctx = await verify_bearer_token(session, authorization)
    if ctx is None or "worker:claim" not in ctx.scopes:
        raise HTTPException(status_code=401, detail="not authorized to claim")

    try:
        worker_id = UUID(payload["worker_id"])
        caps = payload["caps"]
    except (KeyError, ValueError) as exc:
        raise HTTPException(
            status_code=400, detail=f"worker_id + caps required: {exc}",
        ) from exc

    worker_os = sorted({c["os"] for c in caps})
    worker_cpu_arches = sorted({c.get("cpu_arch", "x86_64") for c in caps})
    worker_gpu = sorted({c["gpu_vendor"] for c in caps})
    worker_network = sorted({
        p for c in caps for p in c["network_policies"]
    })

    import time as _time
    t0 = _time.perf_counter()
    async with request.app.state.session_factory() as session:
        row = await claim_one(
            session,
            worker_id=worker_id,
            worker_os=worker_os, worker_cpu_arches=worker_cpu_arches,
            worker_gpu_vendors=worker_gpu,
            worker_network_policies=worker_network,
            enforce_shared_slot=True,
        )
        await session.commit()
    elapsed = _time.perf_counter() - t0

    if row is None:
        CLAIM_LATENCY_SEC.labels(result="miss").observe(elapsed)
        return Response(status_code=204)
    CLAIM_LATENCY_SEC.labels(result="hit").observe(elapsed)

    pc_id = row["provider_connection_id"]
    # #672 family-runs: propagate the family gate so the worker can
    # bind-mount the resolved state_uri before starting the sandbox.
    family_run_spec = row.get("family_run_spec") if hasattr(row, "get") else row["family_run_spec"]
    family_state_uri = row.get("family_state_uri") if hasattr(row, "get") else row["family_state_uri"]
    family_key = row.get("family_key") if hasattr(row, "get") else row["family_key"]
    return JSONResponse({
        "trial_id": str(row["id"]),
        "team_id": str(row["team_id"]),
        "task_id": row["task_id"],
        "config": row["config"],
        "requires_caps": row["requires_caps"],
        "attempt_count": row["attempt_count"],
        "provider_connection_id": (
            str(pc_id) if pc_id is not None else None
        ),
        "family_key": family_key,
        "family_state_uri": family_state_uri,
        "family_run_spec": family_run_spec,
        "state": "claimed",
    })


@router.post("/work/claim")
async def claim_any_work(
    request: Request,
    payload: WorkClaimRequestV1,
    authorization: str | None = Header(default=None),
) -> Response:
    async with request.app.state.session_factory() as session:
        ctx = await verify_bearer_token(session, authorization)
        if ctx is None or "worker:claim" not in ctx.scopes:
            raise HTTPException(status_code=401, detail="not authorized to claim")
        worker = (
            await session.execute(
                text("SELECT capabilities FROM workers WHERE id=(:worker_id)::uuid"),
                {"worker_id": payload.worker_id},
            )
        ).mappings().one_or_none()
        if worker is None:
            raise HTTPException(status_code=409, detail="worker_unknown")
        caps = list(worker["capabilities"])
        worker_os = sorted({item["os"] for item in caps})
        worker_cpu_arches = sorted({item.get("cpu_arch", "x86_64") for item in caps})
        worker_gpu = sorted({item["gpu_vendor"] for item in caps})
        worker_network = sorted(
            {policy for item in caps for policy in item["network_policies"]}
        )
        try:
            claimed = await claim_work(
                session,
                worker_id=payload.worker_id,
                capability_snapshot_digest=payload.capability_snapshot_digest,
                worker_token_hash=ctx.token_hash,
                supported_work_kinds=list(payload.supported_work_kinds),
                free_slots=payload.free_slots,
                worker_os=worker_os,
                worker_cpu_arches=worker_cpu_arches,
                worker_gpu_vendors=worker_gpu,
                worker_network_policies=worker_network,
            )
        except WorkClaimConflictError as exc:
            await session.rollback()
            raise HTTPException(status_code=409, detail=exc.reason) from exc
        if claimed is None:
            await session.rollback()
            return Response(status_code=204)
        row, lease_token = claimed
        claim_payload: TrialClaimV1 | ExecutionAttemptClaimV1
        if row["work_kind"] == "trial":
            family_row = None
            if row["batch_id"] is not None and row["family_key"] is not None:
                family_row = (
                    await session.execute(
                        text("""
                            SELECT b.family_run_spec, bfs.state_uri
                              FROM batches b
                              LEFT JOIN batch_family_state bfs
                                ON bfs.batch_id=b.id AND bfs.family_key=:family_key
                             WHERE b.id=(:batch_id)::uuid
                        """),
                        {"batch_id": row["batch_id"], "family_key": row["family_key"]},
                    )
                ).mappings().one_or_none()
            claim_payload = TrialClaimV1(
                trial_id=row["id"],
                team_id=row["team_id"],
                task_id=row["task_id"],
                config=row["config"],
                requires_caps=row["requires_caps"],
                attempt_count=row["attempt_count"],
                provider_connection_id=row["provider_connection_id"],
                family_key=row["family_key"],
                family_state_uri=family_row["state_uri"] if family_row else None,
                family_run_spec=family_row["family_run_spec"] if family_row else None,
                state="claimed",
            )
        else:
            assert lease_token is not None
            attempt_row = (
                await session.execute(
                    text("""
                        SELECT a.id, a.attempt_number, a.claim_id, a.lease_epoch,
                               a.lease_expires_at, a.stage_request_bytes,
                               a.stage_request_digest, a.resumed_checkpoint_artifact_id,
                               s.id AS stage_run_id, s.node_key, s.shard_key,
                               s.resolved_execution_spec_json, s.execution_spec_digest,
                               s.resource_profile_json, s.resource_profile_digest,
                               s.image_runtime_contract_json,
                               s.image_runtime_contract_digest,
                               s.resolved_input_bindings_json,
                               s.request_renderer_json, s.provider_connection_ref,
                               s.secret_refs, r.id AS pipeline_run_id, r.team_id,
                               r.recipe_digest, r.graph_spec_digest,
                               p.authorization_id,
                               p.authorization_snapshot_sha256,
                               p.candidate_sha256, p.preflight_input_set_id,
                               p.sealed_input_descriptor_set_sha256,
                               p.exclusive_fence_id, p.policy_id,
                               p.policy_config_sha256, p.policy_activation_epoch,
                               p.slurm_cluster_id, p.slurm_cluster_config_sha256,
                               p.slurm_allocation_id
                          FROM execution_attempts a
                          JOIN pipeline_stage_runs s ON s.id=a.stage_run_id
                          JOIN pipeline_runs r ON r.id=s.pipeline_run_id
                          LEFT JOIN pipeline_acceptance_preflight_prerequisites p
                            ON p.pipeline_run_id=r.id AND p.fence_state='active'
                         WHERE a.id=(:attempt_id)::uuid
                    """),
                    {"attempt_id": row["id"]},
                )
            ).mappings().one()
            spec = attempt_row["resolved_execution_spec_json"]
            node = spec["container_node"]
            renderer = attempt_row["request_renderer_json"]
            stage_request = None
            if attempt_row["stage_request_bytes"] is not None:
                if renderer is None:
                    raise HTTPException(status_code=409, detail="claim_contract_mismatch")
                stage_request = StageRequestGrantV1(
                    renderer_name=renderer["name"],
                    renderer_version=renderer["version"],
                    renderer_digest=renderer["digest"],
                    canonical_jcs_lf=bytes(attempt_row["stage_request_bytes"]).decode("utf-8"),
                    stage_request_sha256=attempt_row["stage_request_digest"],
                    size_bytes=len(attempt_row["stage_request_bytes"]),
                )
            acceptance = None
            if attempt_row["exclusive_fence_id"] is not None:
                phase = "cold" if node["node_key"].endswith("_cold") else "warm"
                variant = node["node_key"].removesuffix(
                    f"_acceptance_preflight_{phase}"
                )
                acceptance = AcceptancePreflightGrantV1(
                    authorization_id=attempt_row["authorization_id"],
                    authorization_snapshot_sha256=(
                        attempt_row["authorization_snapshot_sha256"]
                    ),
                    action="matrix",
                    candidate_sha256=attempt_row["candidate_sha256"],
                    preflight_input_set_id=attempt_row["preflight_input_set_id"],
                    prerequisite_pipeline_run_id=attempt_row["pipeline_run_id"],
                    exclusive_fence_id=attempt_row["exclusive_fence_id"],
                    node_key=node["node_key"],
                    backend_variant_id=variant,
                    cache_expectation=(
                        "cold_after_eviction" if phase == "cold" else "warm_reuse_only"
                    ),
                    sealed_input_descriptor_set_sha256=(
                        attempt_row["sealed_input_descriptor_set_sha256"]
                    ),
                    policy_id=attempt_row["policy_id"],
                    policy_config_sha256=attempt_row["policy_config_sha256"],
                    policy_activation_epoch=attempt_row["policy_activation_epoch"],
                    slurm_cluster_id=attempt_row["slurm_cluster_id"],
                    slurm_cluster_config_sha256=(
                        attempt_row["slurm_cluster_config_sha256"]
                    ),
                    slurm_allocation_id=attempt_row["slurm_allocation_id"],
                    image_runtime_contract_digest=(
                        attempt_row["image_runtime_contract_digest"]
                    ),
                    resource_profile_digest=attempt_row["resource_profile_digest"],
                    network_profile="none",
                    renderer_digest=renderer["digest"],
                )
            resume_checkpoint = None
            if attempt_row["resumed_checkpoint_artifact_id"] is not None:
                checkpoint_row = (
                    await session.execute(
                        text("""
                            SELECT id,artifact_type,content_hash,manifest_sha256,
                                   stored_size_bytes,unpacked_size_bytes,file_count
                              FROM artifacts WHERE id=(:artifact_id)::uuid
                        """),
                        {"artifact_id": attempt_row["resumed_checkpoint_artifact_id"]},
                    )
                ).mappings().one_or_none()
                if checkpoint_row is None or any(
                    checkpoint_row[name] is None
                    for name in (
                        "manifest_sha256",
                        "stored_size_bytes",
                        "unpacked_size_bytes",
                        "file_count",
                    )
                ):
                    raise HTTPException(status_code=409, detail="resume_checkpoint_drift")
                resume_checkpoint = ArtifactInputDescriptorV1(
                    artifact_id=checkpoint_row["id"],
                    artifact_type=checkpoint_row["artifact_type"],
                    content_sha256=checkpoint_row["content_hash"],
                    manifest_sha256=checkpoint_row["manifest_sha256"],
                    stored_size_bytes=checkpoint_row["stored_size_bytes"],
                    unpacked_size_bytes=checkpoint_row["unpacked_size_bytes"],
                    file_count=checkpoint_row["file_count"],
                )
            claim_payload = ExecutionAttemptClaimV1(
                execution_attempt_id=attempt_row["id"],
                pipeline_run_id=attempt_row["pipeline_run_id"],
                stage_run_id=attempt_row["stage_run_id"],
                team_id=attempt_row["team_id"],
                node_key=attempt_row["node_key"],
                shard_key=attempt_row["shard_key"],
                attempt_number=attempt_row["attempt_number"],
                claim_id=attempt_row["claim_id"],
                lease_epoch=attempt_row["lease_epoch"],
                lease_token=lease_token,
                lease_expires_at=attempt_row["lease_expires_at"],
                recipe_digest=attempt_row["recipe_digest"],
                run_graph_digest=attempt_row["graph_spec_digest"],
                execution_spec_snapshot=spec,
                execution_spec_digest=attempt_row["execution_spec_digest"],
                image=node["image"],
                argv=node["argv"],
                workdir=node["workdir"],
                resource_profile_snapshot=attempt_row["resource_profile_json"],
                resource_profile_digest=attempt_row["resource_profile_digest"],
                network_profile=node["network_profile"],
                image_runtime_contract_snapshot=(
                    attempt_row["image_runtime_contract_json"]
                ),
                image_runtime_contract_digest=(
                    attempt_row["image_runtime_contract_digest"]
                ),
                input_bindings=attempt_row["resolved_input_bindings_json"],
                outputs=node["outputs"],
                checkpoint=node["checkpoint"],
                fanout_commit=node["fanout_commit"],
                stage_request=stage_request,
                acceptance_preflight=acceptance,
                provider_connection_ref=attempt_row["provider_connection_ref"],
                secret_refs=list(attempt_row["secret_refs"]),
                resume_checkpoint=resume_checkpoint,
                timeout_seconds=node["timeout_seconds"],
                cancellation_poll_seconds=5,
                cancellation_grace_seconds=30,
            )
        envelope = WorkClaimV1(
            schema_version="loom.work-claim.v1",
            work_kind=row["work_kind"],
            payload=claim_payload,
        )
        await session.commit()
    return JSONResponse(envelope.model_dump(mode="json", exclude_none=False))


@router.post("/trials/{trial_id}/retry")
async def requeue_trial_retry(
    trial_id: UUID,
    request: Request,
    payload: dict[str, Any],
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    async with request.app.state.session_factory() as session:
        ctx = await verify_bearer_token(session, authorization)
    if ctx is None or "worker:report" not in ctx.scopes:
        raise HTTPException(status_code=401, detail="not authorized to report")

    try:
        worker_id = UUID(payload["worker_id"])
    except (KeyError, ValueError) as exc:
        raise HTTPException(
            status_code=400, detail=f"worker_id required: {exc}",
        ) from exc

    failure_reason_str = payload.get("failure_reason")
    if not isinstance(failure_reason_str, str):
        raise HTTPException(
            status_code=400,
            detail="failure_reason must be a string",
        )
    try:
        failure_reason = FailureReason(failure_reason_str)
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail=f"invalid failure_reason {failure_reason_str!r}",
        ) from exc

    failure_message = payload.get("failure_message")
    if failure_message is not None and not isinstance(failure_message, str):
        raise HTTPException(
            status_code=400,
            detail="failure_message must be a string",
        )

    try:
        retry_after_sec = float(payload["retry_after_sec"])
    except (KeyError, TypeError, ValueError) as exc:
        raise HTTPException(
            status_code=400,
            detail=f"retry_after_sec required: {exc}",
        ) from exc
    if retry_after_sec < 0:
        raise HTTPException(
            status_code=400,
            detail="retry_after_sec must be >= 0",
        )

    async with request.app.state.session_factory() as session:
        row = (
            await session.execute(
                _REQUEUE_TRIAL_RETRY_SQL,
                {
                    "trial_id": trial_id,
                    "worker_id": worker_id,
                    "failure_reason": failure_reason.value,
                    "failure_message": failure_message,
                    "retry_after_sec": retry_after_sec,
                },
            )
        ).mappings().one_or_none()
        await session.commit()

    if row is None:
        raise HTTPException(
            status_code=409,
            detail=(
                "worker lost claim, trial has started, or retry transition "
                "is not allowed"
            ),
        )
    return {"trial_id": str(row["id"]), "state": "queued"}


@router.post("/trials/{trial_id}/pre-start-heartbeat")
async def pre_start_heartbeat(
    trial_id: UUID,
    request: Request,
    payload: dict[str, Any],
    authorization: str | None = Header(default=None),
) -> dict[str, str]:
    async with request.app.state.session_factory() as session:
        ctx = await verify_bearer_token(session, authorization)
    if ctx is None or "worker:report" not in ctx.scopes:
        raise HTTPException(status_code=401, detail="not authorized to report")

    try:
        worker_id = UUID(payload["worker_id"])
    except (KeyError, ValueError) as exc:
        raise HTTPException(
            status_code=400, detail=f"worker_id required: {exc}",
        ) from exc

    async with request.app.state.session_factory() as session:
        row = (
            await session.execute(
                _PRE_START_HEARTBEAT_SQL,
                {"trial_id": trial_id, "worker_id": worker_id},
            )
        ).mappings().one_or_none()
        await session.commit()

    if row is None:
        raise HTTPException(
            status_code=409,
            detail="worker lost claim, trial has started, or trial is not claimed",
        )
    return {
        "trial_id": str(row["id"]),
        "pre_start_heartbeat_at": row["pre_start_heartbeat_at"].isoformat(),
    }


@router.post("/workers/register")
async def register_worker(
    request: Request,
    payload: dict[str, Any],
    authorization: str | None = Header(default=None),
) -> dict[str, Any]:
    async with request.app.state.session_factory() as session:
        ctx = await verify_bearer_token(session, authorization)
    if ctx is None or "worker:report" not in ctx.scopes:
        raise HTTPException(status_code=401, detail="not authorized to register")

    # Bug 5 fix: validate each capabilities entry against the Capabilities
    # Pydantic model so garbage (typo'd OS, unknown gpu_vendor, etc.) is
    # rejected at the boundary rather than silently mis-matching DRF claim
    # queries later.
    raw_caps = payload.get("capabilities")
    if not isinstance(raw_caps, list) or not raw_caps:
        raise HTTPException(
            status_code=400,
            detail="capabilities must be a non-empty list",
        )
    try:
        validated_caps = [
            Capabilities.model_validate(c).model_dump(mode="json")
            for c in raw_caps
        ]
    except ValidationError as exc:
        raise HTTPException(
            status_code=400, detail=f"invalid capabilities: {exc.errors()}",
        ) from exc

    raw_max_concurrent = payload.get("max_concurrent", 1)
    try:
        max_concurrent = int(raw_max_concurrent)
    except (TypeError, ValueError) as exc:
        raise HTTPException(
            status_code=400,
            detail="max_concurrent must be a positive integer",
        ) from exc
    if max_concurrent < 1:
        raise HTTPException(
            status_code=400,
            detail="max_concurrent must be a positive integer",
        )

    raw_pool_name = payload.get("pool_name", "default")
    pool_name = str(raw_pool_name).strip()
    if not pool_name:
        raise HTTPException(
            status_code=400,
            detail="pool_name must be a non-empty string",
        )

    raw_work_kinds = payload.get("supported_work_kinds")
    if raw_work_kinds is None:
        supported_work_kinds = ["trial"]
    elif raw_work_kinds in (["trial"], ["trial", "execution_attempt"]):
        supported_work_kinds = list(raw_work_kinds)
    else:
        raise HTTPException(
            status_code=400,
            detail=(
                "supported_work_kinds must be ['trial'] or "
                "['trial', 'execution_attempt']"
            ),
        )

    cache_field_names = (
        "input_cache_capacity_bytes",
        "input_cache_reserved_bytes",
        "input_cache_ready_bytes",
    )
    raw_cache_values = tuple(payload.get(name) for name in cache_field_names)
    if any(value is not None for value in raw_cache_values):
        if any(value is None or isinstance(value, bool) for value in raw_cache_values):
            raise HTTPException(
                status_code=400, detail="input cache registration fields must be integers"
            )
        try:
            raw_capacity, raw_reserved, raw_ready = raw_cache_values
            assert raw_capacity is not None
            assert raw_reserved is not None
            assert raw_ready is not None
            capacity_bytes = int(raw_capacity)
            reserved_bytes = int(raw_reserved)
            ready_bytes = int(raw_ready)
        except (TypeError, ValueError) as exc:
            raise HTTPException(
                status_code=400, detail="input cache registration fields must be integers"
            ) from exc
        if not (
            0 <= reserved_bytes <= capacity_bytes
            and 0 <= ready_bytes <= capacity_bytes
        ):
            raise HTTPException(status_code=400, detail="input_cache_capacity_drift")
    else:
        capacity_bytes = reserved_bytes = ready_bytes = 0

    capability_identity: dict[str, Any] = {
            "capabilities": validated_caps,
            "max_concurrent": max_concurrent,
            "pool_name": pool_name,
            "supported_work_kinds": supported_work_kinds,
    }
    if any(value is not None for value in raw_cache_values):
        capability_identity.update(
            input_cache_capacity_bytes=capacity_bytes,
            input_cache_reserved_bytes=reserved_bytes,
            input_cache_ready_bytes=ready_bytes,
        )
    capability_snapshot_digest = canonical_digest(capability_identity)
    supplied_digest = payload.get("capability_snapshot_digest")
    if supplied_digest is not None and supplied_digest != capability_snapshot_digest:
        raise HTTPException(status_code=409, detail="capability_snapshot_mismatch")

    worker_id = uuid4()
    async with request.app.state.session_factory() as session:
        await session.execute(insert(Worker).values(
            id=worker_id,
            hostname=payload.get("hostname", "unknown"),
            version=payload.get("version", "unknown"),
            capabilities=validated_caps,
            supported_work_kinds=supported_work_kinds,
            capability_snapshot_digest=capability_snapshot_digest,
            auth_token_hash=ctx.token_hash,
            max_concurrent=max_concurrent,
            pool_name=pool_name,
            input_cache_capacity_bytes=capacity_bytes,
            input_cache_reserved_bytes=reserved_bytes,
            input_cache_ready_bytes=ready_bytes,
            registered_at=datetime.now(UTC),
            last_seen_at=datetime.now(UTC),
            status="active",
        ))
        await session.commit()

    return {
        "worker_id": str(worker_id),
        "capability_snapshot_digest": capability_snapshot_digest,
        "supported_work_kinds": supported_work_kinds,
        "input_cache_capacity_bytes": capacity_bytes,
        "input_cache_reserved_bytes": reserved_bytes,
        "input_cache_ready_bytes": ready_bytes,
        "heartbeat_interval_sec": 5,
        "claim_poll_interval_sec": 1.0,
        "drain_timeout_sec": 600,
    }


@router.post("/workers/{worker_id}/heartbeat")
async def heartbeat(
    worker_id: UUID,
    request: Request,
    payload: dict[str, Any] | None = None,
    authorization: str | None = Header(default=None),
) -> dict[str, str]:
    async with request.app.state.session_factory() as session:
        ctx = await verify_bearer_token(session, authorization)
    if ctx is None or "worker:report" not in ctx.scopes:
        raise HTTPException(status_code=401, detail="not authorized")

    status = None
    if payload is not None:
        raw_status = payload.get("status")
        if raw_status is not None:
            if raw_status not in _WORKER_HEARTBEAT_STATUSES:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "invalid worker heartbeat status: "
                        f"{raw_status!r}"
                    ),
                )
            status = raw_status

    values: dict[str, Any] = {"last_seen_at": datetime.now(UTC)}
    if status is not None:
        values["status"] = status
    async with request.app.state.session_factory() as session:
        await session.execute(update(Worker).where(Worker.id == worker_id).values(
            **values,
        ))
        await session.commit()
    return {"status": "ok"}
