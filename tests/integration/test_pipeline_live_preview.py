from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from io import BytesIO
from pathlib import Path
from uuid import UUID, uuid4

import httpx
import pytest
from fastapi import FastAPI
from PIL import Image
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import PipelineLivePreviewFrame, PipelineLivePreviewGeneration
from loom.pipeline.keys import canonical_digest, canonical_document
from loom_control_plane.live_preview import reconcile_live_previews
from loom_control_plane.routes import execution_attempts
from loom_service.routes import pipeline


@dataclass(frozen=True)
class _Seed:
    team_id: UUID
    other_team_id: UUID
    worker_id: UUID
    run_id: UUID
    stage_id: UUID
    attempt_id: UUID
    claim_id: UUID
    worker_token: str
    team_token: str
    other_team_token: str
    lease_token: str


def _jpeg(*, color: tuple[int, int, int] = (12, 34, 56)) -> bytes:
    output = BytesIO()
    Image.new("RGB", (672, 448), color).save(
        output,
        format="JPEG",
        progressive=False,
        optimize=False,
    )
    return output.getvalue()


def _execution_spec() -> dict[str, object]:
    digest = "sha256:" + "1" * 64
    image = "registry.example.com/loom/behavior@sha256:" + "2" * 64
    bindings: list[object] = []
    return {
        "schema_version": "loom.execution-spec.v1",
        "recipe_digest": digest,
        "run_graph_digest": "sha256:" + "3" * 64,
        "node_key": "rollout",
        "shard_key": "singleton",
        "container_node": {
            "node_kind": "container",
            "node_key": "rollout",
            "image": image,
            "argv": ["/opt/behavior/bin/rollout"],
            "workdir": "/workspace",
            "resource_profile": "behavior-offline-none@1",
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
                    "max_bytes": 1048576,
                }
            ],
            "request_renderer": None,
            "checkpoint": None,
            "fanout": None,
            "fanout_commit": None,
            "timeout_seconds": 300,
            "max_attempts": 2,
            "failure_policy": "fail_run",
        },
        "image_runtime_contract_digest": "sha256:" + "4" * 64,
        "resource_profile_digest": "sha256:" + "5" * 64,
        "execution_variant_id": "cpu-data-x86_64",
        "gpu_backend_selection_sha256": None,
        "resolved_image_manifest_digest": "sha256:" + "6" * 64,
        "network_profile": "none",
        "resolved_input_bindings_digest": canonical_digest(bindings),
        "fanout_source_manifest_digest": None,
        "fanout_item_digest": None,
        "fanout_parameters_digest": None,
        "request_renderer_lock_digest": None,
        "control_binding_snapshots": [],
    }


async def _seed(session_factory: async_sessionmaker) -> _Seed:
    seed = _Seed(
        team_id=uuid4(),
        other_team_id=uuid4(),
        worker_id=uuid4(),
        run_id=uuid4(),
        stage_id=uuid4(),
        attempt_id=uuid4(),
        claim_id=uuid4(),
        worker_token="worker-" + uuid4().hex + uuid4().hex,
        team_token="team-" + uuid4().hex,
        other_team_token="other-" + uuid4().hex,
        lease_token="lease-" + uuid4().hex + uuid4().hex,
    )
    spec = _execution_spec()
    digest = "sha256:" + "7" * 64
    now = datetime.now(UTC)
    worker_hash = hashlib.sha256(seed.worker_token.encode()).digest()
    lease_hash = hashlib.sha256(seed.lease_token.encode()).hexdigest()
    async with session_factory() as session, session.begin():
        for team_id, name in (
            (seed.team_id, f"preview-{seed.team_id}"),
            (seed.other_team_id, f"preview-other-{seed.other_team_id}"),
        ):
            await session.execute(
                text("INSERT INTO teams(id,name) VALUES (:id,:name)"),
                {"id": team_id, "name": name},
            )
            await session.execute(
                text("INSERT INTO team_quotas(team_id) VALUES (:id)"), {"id": team_id}
            )
        for raw, token_type, scopes, team_id in (
            (seed.worker_token, "worker", ["worker:report"], None),
            (seed.team_token, "team", ["read:own"], seed.team_id),
            (seed.other_team_token, "team", ["read:own"], seed.other_team_id),
        ):
            await session.execute(
                text("""
                    INSERT INTO tokens(token_hash,type,scopes,team_id,issued_at)
                    VALUES (:hash,:type,:scopes,:team,now())
                """),
                {
                    "hash": hashlib.sha256(raw.encode()).digest(),
                    "type": token_type,
                    "scopes": scopes,
                    "team": team_id,
                },
            )
        await session.execute(
            text("""
                INSERT INTO workers(
                    id,hostname,version,capabilities,supported_work_kinds,
                    auth_token_hash,pool_name,registered_at,last_seen_at,status
                ) VALUES (
                    :id,'preview-worker','test','[]'::jsonb,
                    ARRAY['trial','execution_attempt']::text[],
                    :token,'behavior-cpu-data',now(),now(),'active'
                )
            """),
            {"id": seed.worker_id, "token": worker_hash},
        )
        await session.execute(
            text("""
                INSERT INTO pipeline_runs(
                    id,team_id,submission_policy,recipe_name,recipe_version,recipe_digest,
                    graph_spec_json,graph_spec_digest,parameters_json,parameters_digest,
                    resolved_inputs_json,budget_json,request_digest,idempotency_key,state,started_at
                ) VALUES (
                    :id,:team,'ordinary','preview-test',1,:digest,'{}'::jsonb,:digest,
                    '{}'::jsonb,:digest,'[]'::jsonb,'{}'::jsonb,:digest,:key,'running',:now
                )
            """),
            {
                "id": seed.run_id,
                "team": seed.team_id,
                "digest": digest,
                "key": f"preview-{seed.run_id}",
                "now": now,
            },
        )
        await session.execute(
            text("""
                INSERT INTO pipeline_stage_runs(
                    id,pipeline_run_id,node_key,shard_key,node_kind,state,
                    resolved_execution_spec_json,resolved_execution_spec_bytes,
                    execution_spec_digest,resource_profile_json,resource_profile_digest,
                    resolved_input_bindings_json,resolved_input_bindings_digest,
                    failure_policy,ready_at,claimed_at,started_at,attempt_count
                ) VALUES (
                    :id,:run,'rollout','singleton','container','running',CAST(:spec AS jsonb),
                    :spec_bytes,:spec_digest,'{}'::jsonb,:digest,'[]'::jsonb,:bindings,
                    'fail_run',:now,:now,:now,1
                )
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
            },
        )
        await session.execute(
            text("""
                INSERT INTO execution_attempts(
                    id,stage_run_id,attempt_number,state,worker_id,claim_id,lease_epoch,
                    lease_token_digest,lease_expires_at,queued_at,claimed_at,started_at
                ) VALUES (
                    :id,:stage,1,'running',:worker,:claim,1,:lease,:expires,:now,:now,:now
                )
            """),
            {
                "id": seed.attempt_id,
                "stage": seed.stage_id,
                "worker": seed.worker_id,
                "claim": seed.claim_id,
                "lease": lease_hash,
                "expires": now + timedelta(minutes=10),
                "now": now,
            },
        )
        await session.execute(
            text("""
                INSERT INTO pipeline_live_preview_generations(
                    execution_attempt_id,generation,team_id,pipeline_run_id,
                    pipeline_stage_run_id,worker_id,claim_id,lease_epoch,state,expires_at
                ) VALUES (:attempt,:attempt,:team,:run,:stage,:worker,:claim,1,'waiting',:expires)
            """),
            {
                "attempt": seed.attempt_id,
                "team": seed.team_id,
                "run": seed.run_id,
                "stage": seed.stage_id,
                "worker": seed.worker_id,
                "claim": seed.claim_id,
                "expires": now + timedelta(minutes=5),
            },
        )
    return seed


def _claim_headers(seed: _Seed, frame: bytes, *, step: int = 7) -> dict[str, str]:
    digest = "sha256:" + hashlib.sha256(frame).hexdigest()
    return {
        "Authorization": f"Bearer {seed.worker_token}",
        "X-Loom-Claim-Id": str(seed.claim_id),
        "X-Loom-Lease-Epoch": "1",
        "X-Loom-Lease-Token": seed.lease_token,
        "Idempotency-Key": f"preview-{seed.attempt_id}-0",
        "X-Loom-Preview-Step": str(step),
        "If-Match": f'"{digest}"',
        "Content-Type": "image/jpeg",
    }


@pytest.mark.asyncio
async def test_claim_bound_publish_same_origin_read_and_replay(postgres_url: str) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    seed = await _seed(sessions)
    cp = FastAPI()
    cp.include_router(execution_attempts.router)
    cp.state.session_factory = sessions
    service = FastAPI()
    service.include_router(pipeline.router, prefix="/api/v1")
    service.state.session_factory = sessions
    frame = _jpeg()
    publish_path = f"/api/v1/execution-attempts/{seed.attempt_id}/live-preview/frames/0"
    read_root = (
        f"/api/v1/pipeline-runs/{seed.run_id}/stages/{seed.stage_id}"
        f"/attempts/{seed.attempt_id}/live-preview"
    )
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=cp), base_url="http://cp"
        ) as client:
            response = await client.put(
                publish_path, headers=_claim_headers(seed, frame), content=frame
            )
            assert response.status_code == 201, response.text
            assert response.json()["idempotent_replay"] is False
            replay = await client.put(
                publish_path, headers=_claim_headers(seed, frame), content=frame
            )
            assert replay.status_code == 200
            assert replay.json()["idempotent_replay"] is True
            changed = _jpeg(color=(90, 80, 70))
            conflict = await client.put(
                publish_path, headers=_claim_headers(seed, changed), content=changed
            )
            assert conflict.status_code == 409
            assert conflict.json() == {"detail": "preview_replay_conflict"}

        auth = {"Authorization": f"Bearer {seed.team_token}"}
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=service), base_url="http://service"
        ) as client:
            metadata = await client.get(read_root, headers=auth)
            assert metadata.status_code == 200, metadata.text
            assert metadata.json() == {
                "schema_version": "loom.behavior-stage1-live-preview.v1",
                "state": "live",
                "attempt_id": str(seed.attempt_id),
                "generation": str(seed.attempt_id),
                "latest_sequence": 0,
                "latest_step_idx": 7,
                "received_at": metadata.json()["received_at"],
                "retry_after_ms": 500,
            }
            frame_url = f"{read_root}/frames/0"
            fetched = await client.get(frame_url, headers=auth)
            expected_etag = f'"sha256:{hashlib.sha256(frame).hexdigest()}"'
            assert fetched.status_code == 200
            assert fetched.content == frame
            assert fetched.headers["content-type"] == "image/jpeg"
            assert fetched.headers["cache-control"] == "private, no-store"
            assert fetched.headers["content-security-policy"] == "default-src 'none'; sandbox"
            assert fetched.headers["x-content-type-options"] == "nosniff"
            assert fetched.headers["content-length"] == str(len(frame))
            assert fetched.headers["etag"] == expected_etag
            head = await client.head(frame_url, headers=auth)
            assert head.status_code == 200 and head.content == b""
            assert head.headers["content-length"] == str(len(frame))
            unchanged = await client.get(
                frame_url, headers={**auth, "If-None-Match": expected_etag}
            )
            assert unchanged.status_code == 304 and unchanged.content == b""
            ranged = await client.get(frame_url, headers={**auth, "Range": "bytes=0-3"})
            assert ranged.status_code == 416
            enumerated = await client.get(f"{read_root}/frames/1", headers=auth)
            assert enumerated.status_code == 404
            cross_team = await client.get(
                read_root,
                headers={"Authorization": f"Bearer {seed.other_team_token}"},
            )
            assert cross_team.status_code == 404
            payloads = " ".join(
                [metadata.text, fetched.text, ranged.text, enumerated.text, cross_team.text]
            )
            assert seed.lease_token not in payloads
            assert "object_key" not in payloads and "minio" not in payloads
    finally:
        async with engine.begin() as connection:
            await connection.execute(
                text("DELETE FROM pipeline_runs WHERE id=:id"), {"id": seed.run_id}
            )
            await connection.execute(
                text("DELETE FROM workers WHERE id=:id"), {"id": seed.worker_id}
            )
            await connection.execute(
                text("DELETE FROM tokens WHERE token_hash IN (:a,:b,:c)"),
                {
                    "a": hashlib.sha256(seed.worker_token.encode()).digest(),
                    "b": hashlib.sha256(seed.team_token.encode()).digest(),
                    "c": hashlib.sha256(seed.other_team_token.encode()).digest(),
                },
            )
            await connection.execute(
                text("DELETE FROM team_quotas WHERE team_id IN (:a,:b)"),
                {"a": seed.team_id, "b": seed.other_team_id},
            )
            await connection.execute(
                text("DELETE FROM teams WHERE id IN (:a,:b)"),
                {"a": seed.team_id, "b": seed.other_team_id},
            )
        await engine.dispose()


@pytest.mark.asyncio
async def test_fence_replacement_ttl_and_cancel_reconciliation_purge_bytes(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    seed = await _seed(sessions)
    frame = _jpeg()
    cp = FastAPI()
    cp.include_router(execution_attempts.router)
    cp.state.session_factory = sessions
    service = FastAPI()
    service.include_router(pipeline.router, prefix="/api/v1")
    service.state.session_factory = sessions
    publish_path = f"/api/v1/execution-attempts/{seed.attempt_id}/live-preview/frames/0"
    read_root = (
        f"/api/v1/pipeline-runs/{seed.run_id}/stages/{seed.stage_id}"
        f"/attempts/{seed.attempt_id}/live-preview"
    )
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=cp), base_url="http://cp"
        ) as client:
            accepted = await client.put(
                publish_path, headers=_claim_headers(seed, frame), content=frame
            )
            assert accepted.status_code == 201
        replacement = uuid4()
        async with sessions() as session, session.begin():
            await session.execute(
                text("UPDATE execution_attempts SET claim_id=:claim WHERE id=:id"),
                {"claim": replacement, "id": seed.attempt_id},
            )
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=service), base_url="http://service"
        ) as client:
            stale = await client.get(
                read_root, headers={"Authorization": f"Bearer {seed.team_token}"}
            )
            assert stale.status_code == 404
        async with sessions() as session, session.begin():
            assert await reconcile_live_previews(session) == 1
        async with sessions() as session:
            generation = await session.get(PipelineLivePreviewGeneration, seed.attempt_id)
            assert generation is not None
            assert generation.state == "ended"
            assert generation.purge_reason == "claim_replaced"
            assert generation.frame_count == 0 and generation.total_bytes == 0
            assert (
                await session.execute(
                    select(PipelineLivePreviewFrame).where(
                        PipelineLivePreviewFrame.execution_attempt_id == seed.attempt_id
                    )
                )
            ).scalar_one_or_none() is None

        # Idempotent reconciliation retains the bounded cleanup proof without
        # creating a second deletion or exposing stale bytes.
        async with sessions() as session, session.begin():
            assert await reconcile_live_previews(session) == 0
    finally:
        async with engine.begin() as connection:
            await connection.execute(
                text("DELETE FROM pipeline_runs WHERE id=:id"), {"id": seed.run_id}
            )
            await connection.execute(
                text("DELETE FROM workers WHERE id=:id"), {"id": seed.worker_id}
            )
            await connection.execute(
                text("DELETE FROM tokens WHERE token_hash IN (:a,:b,:c)"),
                {
                    "a": hashlib.sha256(seed.worker_token.encode()).digest(),
                    "b": hashlib.sha256(seed.team_token.encode()).digest(),
                    "c": hashlib.sha256(seed.other_team_token.encode()).digest(),
                },
            )
            await connection.execute(
                text("DELETE FROM team_quotas WHERE team_id IN (:a,:b)"),
                {"a": seed.team_id, "b": seed.other_team_id},
            )
            await connection.execute(
                text("DELETE FROM teams WHERE id IN (:a,:b)"),
                {"a": seed.team_id, "b": seed.other_team_id},
            )
        await engine.dispose()


def test_preview_backend_is_ephemeral_and_not_artifact_storage() -> None:
    migration = (
        Path(__file__).parents[2] / "migrations/versions/0093_pipeline_live_preview.py"
    ).read_text()
    lowered = migration.lower()
    assert "pipeline_live_preview_frames" in lowered
    assert "artifacts" not in lowered
    assert "artifact_files" not in lowered
    assert "object_key" not in lowered
    assert "bucket" not in lowered
    assert "credential" not in lowered
