from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import httpx
import pytest
from fastapi import FastAPI
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.pipeline.artifact_commit import (
    ArtifactCommitManifestV1,
    ArtifactManifestV1,
    RootArtifactRecordV1,
    StoredFileV1,
)
from loom.pipeline.keys import canonical_digest, canonical_document
from loom.pipeline.state import (
    RetryClass,
    StageResultOutputV1,
    StageResultProvenanceV1,
    StageResultV1,
)
from loom_control_plane.artifact_commit_runtime import ExecutionAttemptCompletionService
from loom_control_plane.routes import execution_attempts


@dataclass(frozen=True, slots=True)
class _Seed:
    team_id: UUID
    worker_id: UUID
    run_id: UUID
    stage_id: UUID
    attempt_id: UUID
    claim_id: UUID
    worker_token: str
    lease_token: str


def _spec(*, digest: str) -> dict[str, object]:
    return {
        "schema_version": "loom.execution-spec.v1",
        "recipe_digest": digest,
        "run_graph_digest": digest,
        "node_key": "rollout",
        "shard_key": "singleton",
        "container_node": {
            "node_kind": "container",
            "node_key": "rollout",
            "image": "registry.example.com/loom/behavior@sha256:" + "2" * 64,
            "argv": ["/opt/loom/venv/bin/python", "-m", "behavior"],
            "workdir": "/workspace",
            "resource_profile": "behavior-gpu-oldlab@1",
            "network_profile": "none",
            "needs": [],
            "inputs": [],
            "outputs": [
                {
                    "name": "rollout",
                    "artifact_type": "behavior_rollout_bundle.v1",
                    "required": True,
                    "role": "artifact",
                    "producer": "container",
                    "max_bytes": 4096,
                }
            ],
            "request_renderer": None,
            "checkpoint": None,
            "fanout": None,
            "fanout_commit": None,
            "timeout_seconds": 300,
            "max_attempts": 3,
            "failure_policy": "fail_run",
        },
        "image_runtime_contract_digest": digest,
        "resource_profile_digest": digest,
        "execution_variant_id": "oldlab-rtx5080-2gpu",
        "gpu_backend_selection_sha256": digest,
        "resolved_image_manifest_digest": "sha256:" + "2" * 64,
        "network_profile": "none",
        "resolved_input_bindings_digest": canonical_digest([]),
        "fanout_source_manifest_digest": None,
        "fanout_item_digest": None,
        "fanout_parameters_digest": None,
        "request_renderer_lock_digest": None,
        "control_binding_snapshots": [],
    }


async def _seed_failure(
    sessions: async_sessionmaker,
    *,
    attempt_number: int,
) -> _Seed:
    seed = _Seed(
        team_id=uuid4(),
        worker_id=uuid4(),
        run_id=uuid4(),
        stage_id=uuid4(),
        attempt_id=uuid4(),
        claim_id=uuid4(),
        worker_token="worker-" + uuid4().hex + uuid4().hex,
        lease_token="lease-" + uuid4().hex + uuid4().hex,
    )
    now = datetime.now(UTC)
    digest = "sha256:" + "1" * 64
    spec = _spec(digest=digest)
    request = {
        "schema_version": "behavior.stage-request.v1",
        "budget": {
            "provider": None,
            "gpu_seconds_limit": 2 * (300 + 35),
            "final_output_bytes_limit": 4096,
            "checkpoint_bytes_limit": 0,
            "timeout_seconds": 300,
            "max_attempts": 3,
        },
    }
    async with sessions() as session, session.begin():
        await session.execute(
            text("INSERT INTO teams(id,name) VALUES (:id,:name)"),
            {"id": seed.team_id, "name": f"terminal-{seed.team_id}"},
        )
        await session.execute(
            text("INSERT INTO team_quotas(team_id,in_flight_count) VALUES (:id,0)"),
            {"id": seed.team_id},
        )
        token_hash = hashlib.sha256(seed.worker_token.encode()).digest()
        await session.execute(
            text("""
                INSERT INTO tokens(token_hash,type,scopes,issued_at)
                VALUES (:hash,'worker',ARRAY['worker:report']::text[],now())
            """),
            {"hash": token_hash},
        )
        await session.execute(
            text("""
                INSERT INTO workers(id,hostname,version,capabilities,supported_work_kinds,
                    auth_token_hash,pool_name,registered_at,last_seen_at,status)
                VALUES (:id,'terminal-worker','test','[]'::jsonb,
                    ARRAY['trial','execution_attempt']::text[],:token,'behavior-gpu-oldlab',
                    now(),now(),'active')
            """),
            {"id": seed.worker_id, "token": token_hash},
        )
        await session.execute(
            text("""
                INSERT INTO pipeline_runs(id,team_id,submission_policy,recipe_name,recipe_version,
                    recipe_digest,graph_spec_json,graph_spec_digest,parameters_json,
                    parameters_digest,resolved_inputs_json,budget_json,request_digest,
                    idempotency_key,state,started_at)
                VALUES (:id,:team,'ordinary','terminal-test',1,:digest,'{}'::jsonb,:digest,
                    '{}'::jsonb,:digest,'[]'::jsonb,'{}'::jsonb,:digest,:key,'running',:now)
            """),
            {
                "id": seed.run_id,
                "team": seed.team_id,
                "digest": digest,
                "key": f"terminal-{seed.run_id}",
                "now": now,
            },
        )
        await session.execute(
            text("""
                INSERT INTO pipeline_stage_runs(id,pipeline_run_id,node_key,shard_key,node_kind,
                    state,resolved_execution_spec_json,resolved_execution_spec_bytes,
                    execution_spec_digest,resource_profile_json,resource_profile_digest,
                    image_runtime_contract_json,image_runtime_contract_digest,
                    resolved_input_bindings_json,resolved_input_bindings_digest,failure_policy,
                    ready_at,claimed_at,started_at,attempt_count)
                VALUES (:id,:run,'rollout','singleton','container','running',CAST(:spec AS jsonb),
                    :spec_bytes,:spec_digest,'{}'::jsonb,:digest,'{}'::jsonb,:digest,
                    '[]'::jsonb,:bindings,'fail_run',:now,:now,:now,:attempt_number)
            """),
            {
                "id": seed.stage_id,
                "run": seed.run_id,
                "spec": json.dumps(spec),
                "spec_bytes": canonical_document(spec),
                "spec_digest": canonical_digest(spec),
                "digest": digest,
                "bindings": canonical_digest([]),
                "now": now,
                "attempt_number": attempt_number,
            },
        )
        await session.execute(
            text("""
                INSERT INTO execution_attempts(id,stage_run_id,attempt_number,state,worker_id,
                    claim_id,lease_epoch,lease_token_digest,lease_expires_at,heartbeat_runtime_seconds,
                    stage_request_json,stage_request_bytes,stage_request_digest,
                    container_id,input_view_digest,queued_at,claimed_at,started_at,runtime_started_at)
                VALUES (:id,:stage,:number,'running',:worker,:claim,1,:lease,:expires,12.2,
                    CAST(:request AS jsonb),:request_bytes,:request_digest,
                    'container-test',:input_digest,:now,:now,:now,:now)
            """),
            {
                "id": seed.attempt_id,
                "stage": seed.stage_id,
                "number": attempt_number,
                "worker": seed.worker_id,
                "claim": seed.claim_id,
                "lease": hashlib.sha256(seed.lease_token.encode()).hexdigest(),
                "expires": now + timedelta(minutes=5),
                "request": json.dumps(request),
                "request_bytes": canonical_document(request),
                "request_digest": canonical_digest(request),
                "input_digest": canonical_digest([]),
                "now": now,
            },
        )
        await session.execute(
            text("""
                INSERT INTO pipeline_budget_ledgers(pipeline_run_id,provider_limit_microusd,
                    gpu_limit_seconds,gpu_reserved_seconds,artifact_limit_bytes,
                    artifact_reserved_bytes,stage_run_limit,stage_runs_created,attempt_limit,
                    attempts_created,wall_deadline_at)
                VALUES (:run,0,10000,670,10000,4096,1,1,3,:attempt_number,:deadline)
            """),
            {
                "run": seed.run_id,
                "attempt_number": attempt_number,
                "deadline": now + timedelta(hours=1),
            },
        )
        for kind, key, amount in (
            ("gpu", f"gpu:{seed.attempt_id}", 670),
            ("artifact", f"artifact:final:{seed.attempt_id}", 4096),
        ):
            await session.execute(
                text("""
                    INSERT INTO pipeline_budget_reservations(pipeline_run_id,
                        execution_attempt_id,kind,reservation_key,request_digest,reserved_amount)
                    VALUES (:run,:attempt,:kind,:key,:digest,:amount)
                """),
                {
                    "run": seed.run_id,
                    "attempt": seed.attempt_id,
                    "kind": kind,
                    "key": key,
                    "digest": digest,
                    "amount": amount,
                },
            )
    return seed


async def _cleanup(sessions: async_sessionmaker, seed: _Seed) -> None:
    async with sessions() as session, session.begin():
        await session.execute(text("DELETE FROM pipeline_runs WHERE id=:id"), {"id": seed.run_id})
        await session.execute(
            text("DELETE FROM tokens WHERE type='worker' AND token_hash=:hash"),
            {"hash": hashlib.sha256(seed.worker_token.encode()).digest()},
        )
        await session.execute(text("DELETE FROM workers WHERE id=:id"), {"id": seed.worker_id})
        await session.execute(
            text("DELETE FROM team_quotas WHERE team_id=:id"), {"id": seed.team_id}
        )
        await session.execute(text("DELETE FROM teams WHERE id=:id"), {"id": seed.team_id})


async def _add_committed_ready_output(
    sessions: async_sessionmaker,
    seed: _Seed,
) -> tuple[UUID, UUID, StageResultV1, str, int]:
    digest = "sha256:" + "1" * 64
    async with sessions() as session:
        spec_digest = (
            await session.execute(
                text("SELECT execution_spec_digest FROM pipeline_stage_runs WHERE id=:id"),
                {"id": seed.stage_id},
            )
        ).scalar_one()
    result = StageResultV1(
        schema_version="loom.stage-result.v1",
        domain_outcome="success",
        reason_code="episode_complete",
        retry_class=RetryClass.NONE,
        inputs=[],
        outputs=[StageResultOutputV1(name="rollout", artifact_type="behavior_rollout_bundle.v1")],
        metrics={"step_count": 1},
        provenance=StageResultProvenanceV1(
            pipeline_run_id=seed.run_id,
            stage_run_id=seed.stage_id,
            execution_attempt_id=seed.attempt_id,
            recipe_digest=digest,
            execution_spec_digest=spec_digest,
            image_digest="sha256:" + "2" * 64,
        ),
        error=None,
    )
    result_digest = canonical_digest(result)
    payload = b"{}\n"
    payload_digest = "sha256:" + hashlib.sha256(payload).hexdigest()
    upload_id, artifact_id = uuid4(), uuid4()
    stored = StoredFileV1(
        file_index=0,
        relative_path="artifact.json",
        role="semantic_document",
        archive_format="none",
        media_type="application/json",
        size_bytes=len(payload),
        sha256=payload_digest,
    )
    item = ArtifactManifestV1(
        artifact_id=artifact_id,
        artifact_name="rollout",
        artifact_type="behavior_rollout_bundle.v1",
        content_sha256=payload_digest,
        stored_size_bytes=len(payload),
        unpacked_size_bytes=len(payload),
        file_count=1,
        stored_files=[stored],
        lineage_artifact_ids=[],
        lineage_digests=[],
    )
    root = ArtifactCommitManifestV1(
        session_id=upload_id,
        commit_kind="final_output",
        producer_identity={"execution_attempt_id": str(seed.attempt_id)},
        artifacts=[
            RootArtifactRecordV1(
                artifact_id=artifact_id,
                artifact_name="rollout",
                artifact_type="behavior_rollout_bundle.v1",
                manifest_sha256=canonical_digest(item),
                content_sha256=payload_digest,
                stored_files=[stored],
            )
        ],
        total_bytes=len(payload),
        input_lineage_artifact_ids=[],
        input_lineage_digests=[],
        request_digest="sha256:" + "9" * 64,
    )
    now = datetime.now(UTC)
    async with sessions() as session, session.begin():
        await session.execute(
            text("""
                INSERT INTO artifact_upload_sessions(id,team_id,commit_kind,pipeline_run_id,
                    pipeline_stage_run_id,execution_attempt_id,attempt_number,idempotency_key,
                    request_digest,stage_result_json,stage_result_digest,inventory_digest,prefix,
                    state,expected_total_max_bytes,actual_total_bytes,canonical_manifest_json,
                    manifest_sha256,committed_marker_sha256,expires_at,committed_ready_at)
                VALUES (:id,:team,'final_output',:run,:stage,:attempt,1,:key,:request_digest,
                    CAST(:result AS jsonb),:result_digest,:inventory,:prefix,'committed_ready',
                    4096,:bytes,CAST(:manifest AS jsonb),:manifest_digest,:marker,:expires,:now)
            """),
            {
                "id": upload_id,
                "team": seed.team_id,
                "run": seed.run_id,
                "stage": seed.stage_id,
                "attempt": seed.attempt_id,
                "key": f"output-{upload_id}",
                "request_digest": root.request_digest,
                "result": json.dumps(result.model_dump(mode="json")),
                "result_digest": result_digest,
                "inventory": "sha256:" + "8" * 64,
                "prefix": f"private/{seed.team_id}/{upload_id}/",
                "bytes": len(payload),
                "manifest": json.dumps(root.model_dump(mode="json")),
                "manifest_digest": canonical_digest(root),
                "marker": "sha256:" + "7" * 64,
                "expires": now + timedelta(hours=1),
                "now": now,
            },
        )
        await session.execute(
            text("""
                INSERT INTO artifact_upload_files(session_id,file_index,
                    preallocated_artifact_id,relative_path,artifact_name,artifact_type,producer,
                    media_type,role,archive_format,expected_max_bytes,expected_sha256,
                    expected_size,computed_sha256,actual_size,state)
                VALUES (:session,0,:artifact,'artifact.json','rollout',
                    'behavior_rollout_bundle.v1','container','application/json',
                    'semantic_document','none',4096,:digest,:bytes,:digest,:bytes,'verified')
            """),
            {
                "session": upload_id,
                "artifact": artifact_id,
                "digest": payload_digest,
                "bytes": len(payload),
            },
        )
    return upload_id, artifact_id, result, result_digest, len(payload)


@pytest.mark.parametrize(
    ("attempt_number", "expected_stage", "next_attempt"),
    [(1, "retry_wait", True), (3, "failed", False)],
)
async def test_failed_attempt_converges_stage_budget_and_retry_without_attempt_four(
    postgres_url: str,
    attempt_number: int,
    expected_stage: str,
    next_attempt: bool,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    seed = await _seed_failure(sessions, attempt_number=attempt_number)
    app = FastAPI()
    app.include_router(execution_attempts.router)
    app.state.session_factory = sessions
    payload = {
        "exit_code": 22,
        "retry_class": "infrastructure_transient",
        "reason_code": "stage_helper_transient",
        "redacted_message": "helper unavailable",
        "stage_result": None,
        "stage_result_sha256": None,
        "teardown_observed": True,
        "resources": {
            "container_absent": True,
            "cgroup_empty": True,
            "network_absent": True,
            "step_jwt_revoked": True,
            "runtime_secret_mount_absent": True,
            "scratch_absent": True,
            "outputs_absent": True,
            "input_views_absent": True,
            "active_upload_session_ids": [],
        },
    }
    headers = {
        "Authorization": f"Bearer {seed.worker_token}",
        "X-Loom-Claim-Id": str(seed.claim_id),
        "X-Loom-Lease-Epoch": "1",
        "X-Loom-Lease-Token": seed.lease_token,
        "X-Loom-Request-Id": str(uuid4()),
    }
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://control-plane"
        ) as client:
            response = await client.post(
                f"/execution-attempts/{seed.attempt_id}/failed", json=payload, headers=headers
            )
        assert response.status_code == 200, response.text
        async with sessions() as session:
            row = (
                (
                    await session.execute(
                        text("""
                        SELECT a.state AS attempt_state,a.cleanup_acknowledged_at,
                               s.state AS stage_state,s.reason_code,s.next_attempt_at,
                               l.gpu_reserved_seconds,l.gpu_settled_seconds,
                               l.artifact_reserved_bytes,
                               (SELECT count(*) FROM execution_attempts extra
                                 WHERE extra.stage_run_id=s.id AND extra.attempt_number=4) AS attempt_four,
                               (SELECT count(*) FROM pipeline_budget_reservations reservation
                                 WHERE reservation.execution_attempt_id=a.id
                                   AND reservation.state='released') AS released
                          FROM execution_attempts a
                          JOIN pipeline_stage_runs s ON s.id=a.stage_run_id
                          JOIN pipeline_budget_ledgers l ON l.pipeline_run_id=s.pipeline_run_id
                         WHERE a.id=:attempt
                    """),
                        {"attempt": seed.attempt_id},
                    )
                )
                .mappings()
                .one()
            )
        assert row["attempt_state"] == "failed"
        assert row["cleanup_acknowledged_at"] is not None
        assert row["stage_state"] == expected_stage
        assert (row["next_attempt_at"] is not None) is next_attempt
        assert row["reason_code"] == (None if next_attempt else "stage_helper_transient")
        assert row["gpu_reserved_seconds"] == 0
        assert row["gpu_settled_seconds"] == 26
        assert row["artifact_reserved_bytes"] == 0
        assert row["released"] == 1
        assert row["attempt_four"] == 0
    finally:
        await _cleanup(sessions, seed)
        await engine.dispose()


async def test_completed_attempt_atomically_publishes_output_and_converges_stage_budget(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    seed = await _seed_failure(sessions, attempt_number=1)
    upload_id, artifact_id, result, result_digest, output_bytes = await _add_committed_ready_output(
        sessions, seed
    )
    app = FastAPI()
    app.include_router(execution_attempts.router)
    app.state.session_factory = sessions
    app.state.execution_attempt_completion_service = ExecutionAttemptCompletionService()
    payload = {
        "exit_code": 0,
        "stage_result": result.model_dump(mode="json"),
        "stage_result_sha256": result_digest,
        "final_output_upload_session_id": str(upload_id),
    }
    headers = {
        "Authorization": f"Bearer {seed.worker_token}",
        "X-Loom-Claim-Id": str(seed.claim_id),
        "X-Loom-Lease-Epoch": "1",
        "X-Loom-Lease-Token": seed.lease_token,
        "X-Loom-Request-Id": str(uuid4()),
    }
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://control-plane"
        ) as client:
            response = await client.post(
                f"/execution-attempts/{seed.attempt_id}/complete", json=payload, headers=headers
            )
        assert response.status_code == 200, response.text
        async with sessions() as session:
            row = (
                (
                    await session.execute(
                        text("""
                        SELECT a.state AS attempt_state,s.state AS stage_state,
                               s.domain_outcome,s.reason_code,l.gpu_reserved_seconds,
                               l.gpu_settled_seconds,l.artifact_reserved_bytes,
                               l.artifact_settled_bytes,q.in_flight_count,
                               (SELECT state FROM artifact_upload_sessions WHERE id=:upload)
                                   AS upload_state,
                               (SELECT count(*) FROM artifacts WHERE id=:artifact) AS artifact_count,
                               (SELECT count(*) FROM pipeline_budget_reservations reservation
                                 WHERE reservation.execution_attempt_id=a.id
                                   AND reservation.state='settled') AS settled
                          FROM execution_attempts a
                          JOIN pipeline_stage_runs s ON s.id=a.stage_run_id
                          JOIN pipeline_runs r ON r.id=s.pipeline_run_id
                          JOIN pipeline_budget_ledgers l ON l.pipeline_run_id=r.id
                          JOIN team_quotas q ON q.team_id=r.team_id
                         WHERE a.id=:attempt
                    """),
                        {
                            "attempt": seed.attempt_id,
                            "upload": upload_id,
                            "artifact": artifact_id,
                        },
                    )
                )
                .mappings()
                .one()
            )
        assert row["attempt_state"] == "succeeded"
        assert row["stage_state"] == "succeeded"
        assert row["domain_outcome"] == "success"
        assert row["reason_code"] == "episode_complete"
        assert row["gpu_reserved_seconds"] == 0
        assert row["gpu_settled_seconds"] == 26
        assert row["artifact_reserved_bytes"] == 0
        assert row["artifact_settled_bytes"] == output_bytes
        assert row["in_flight_count"] == 0
        assert row["upload_state"] == "committed"
        assert row["artifact_count"] == 1
        assert row["settled"] == 2
    finally:
        await _cleanup(sessions, seed)
        await engine.dispose()
