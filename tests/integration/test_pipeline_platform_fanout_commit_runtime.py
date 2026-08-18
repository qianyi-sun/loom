from __future__ import annotations

import json
from uuid import uuid4

from sqlalchemy import text

from loom.db.schema import ArtifactUploadSession, ExecutionAttempt
from loom.pipeline.artifact_commit import (
    ArtifactCommitManifestV1,
    ArtifactCommitService,
    UploadAuthV1,
    UploadSessionGrantV1,
)
from loom.pipeline.keys import canonical_digest, canonical_document
from loom.pipeline.spec import FanoutManifestV1, PlatformFanoutIndexV1, RunGraphSpecV1
from loom.pipeline.state import StageResultV1
from loom.pipeline.work_protocol import ExecutionCompleteV1, FinalOutputPrepareRequestV1
from loom.trajectory.storage import FakeObjectStore
from loom_control_plane.artifact_commit_runtime import (
    ExecutionAttemptCompletionService,
    FinalOutputRouteService,
    SqlArtifactCommitRepository,
)
from loom_pipeline_orchestrator.fanout_runtime import FanoutExpansionRuntime
from loom_pipeline_orchestrator.repository import FrozenReadiness
from tests.integration.pipeline_artifact_testkit import chunks, digest
from tests.integration.pipeline_orchestrator_fixtures import DIGEST, OrchestratorSeed


async def test_platform_fanout_manifest_is_synthesized_inside_final_marker(
    orchestrator_seed: OrchestratorSeed,
) -> None:
    seed = orchestrator_seed
    node = {
        "node_kind": "container",
        "node_key": "root",
        "image": "registry.example.com/loom/pipeline@sha256:" + "b" * 64,
        "argv": ["produce"],
        "workdir": "/workspace",
        "resource_profile": "pipeline-test-cpu-none@1",
        "network_profile": "none",
        "needs": [],
        "inputs": [],
        "outputs": [
            {
                "name": "index",
                "artifact_type": "loom.platform-fanout-index.v1",
                "required": True,
                "role": "artifact",
                "producer": "container",
                "max_bytes": 4096,
            },
            {
                "name": "item",
                "artifact_type": "example.item.v1",
                "required": False,
                "role": "artifact",
                "producer": "container",
                "max_bytes": 4096,
            },
            {
                "name": "manifest",
                "artifact_type": "loom.fanout-manifest.v1",
                "required": True,
                "role": "fanout_manifest",
                "producer": "platform",
                "max_bytes": 16_777_216,
            },
        ],
        "request_renderer": None,
        "checkpoint": None,
        "fanout": None,
        "fanout_commit": {
            "index_output_name": "index",
            "manifest_output_name": "manifest",
            "items_pointer": "/items",
            "item_binding_name": "item",
            "max_items": 2,
        },
        "timeout_seconds": 60,
        "max_attempts": 3,
        "failure_policy": "fail_run",
    }
    consumer = {
        "node_kind": "container",
        "node_key": "consume",
        "image": "registry.example.com/loom/pipeline@sha256:" + "b" * 64,
        "argv": ["consume"],
        "workdir": "/workspace",
        "resource_profile": "pipeline-test-cpu-none@1",
        "network_profile": "none",
        "needs": ["root"],
        "inputs": [
            {
                "source": "fanout_item",
                "binding_name": "item",
                "artifact_type": "example.item.v1",
            }
        ],
        "outputs": [],
        "request_renderer": None,
        "checkpoint": None,
        "fanout": {
            "source": "stage_output",
            "manifest_stage_key": "root",
            "manifest_output_name": "manifest",
            "items_pointer": "/items",
            "shard_key_pointer": "/shard_key",
            "item_binding_name": "item",
            "item_artifact_type": "example.item.v1",
            "max_items": 2,
        },
        "fanout_commit": None,
        "timeout_seconds": 60,
        "max_attempts": 3,
        "failure_policy": "fail_run",
    }
    finalize = {
        "node_kind": "container",
        "node_key": "finalize",
        "image": "registry.example.com/loom/pipeline@sha256:" + "b" * 64,
        "argv": ["finalize"],
        "workdir": "/workspace",
        "resource_profile": "pipeline-test-cpu-none@1",
        "network_profile": "none",
        "needs": ["root"],
        "inputs": [
            {
                "source": "terminal_outputs",
                "binding_name": "manifests",
                "artifact_type": "loom.fanout-manifest.v1",
                "stage_keys": ["root"],
                "output_name": "manifest",
                "match_outcomes": ["generated"],
            }
        ],
        "outputs": [],
        "request_renderer": {
            "name": "finalize",
            "version": 1,
            "digest": DIGEST,
            "max_bytes": 65_536,
            "terminal_stage_keys": ["root"],
        },
        "checkpoint": None,
        "fanout": None,
        "fanout_commit": None,
        "timeout_seconds": 60,
        "max_attempts": 3,
        "failure_policy": "fail_run",
    }
    graph = RunGraphSpecV1.model_validate(
        {
            "schema_version": "loom.run-graph.v1",
            "recipe": {"name": "orchestrator-fixture", "version": 1, "digest": DIGEST},
            "inputs": [],
            "parameters": {},
            "budget": {
                "max_provider_cost_usd": "0",
                "max_gpu_seconds": 0,
                "max_wall_seconds": 600,
                "max_artifact_bytes": 100_000,
                "max_stage_runs": 8,
                "max_attempts_total": 8,
            },
            "nodes": [node, consumer, finalize],
        }
    )
    async with seed.sessions() as db, db.begin():
        await db.execute(
            text("""
                UPDATE pipeline_runs
                   SET graph_spec_json=CAST(:graph AS jsonb),
                       graph_spec_digest=:graph_digest,
                       budget_json=CAST(:budget AS jsonb)
                 WHERE id=:run_id
            """),
            {
                "run_id": seed.run_id,
                "graph": json.dumps(graph.model_dump(mode="json", exclude_none=False)),
                "graph_digest": canonical_digest(graph),
                "budget": json.dumps(graph.budget.model_dump(mode="json")),
            },
        )
    lease = (await seed.repository.claim_runs(controller_id="fanout-commit"))[0]
    assert await seed.repository.initialize_run(lease) == 2
    candidate = next(
        candidate
        for candidate in await seed.repository.readiness_candidates(lease)
        if candidate.node_key == "root"
    )
    spec = {
        "schema_version": "loom.execution-spec.v1",
        "recipe_digest": DIGEST,
        "container_node": node,
        "resolved_image_manifest_digest": "sha256:" + "b" * 64,
    }
    frozen = FrozenReadiness(
        input_bindings_json=[],
        input_bindings_digest=canonical_digest([]),
        execution_spec_json=spec,
        execution_spec_bytes=canonical_document(spec),
        execution_spec_digest=canonical_digest(spec),
    )
    await seed.repository.freeze_readiness(
        lease,
        stage_run_id=candidate.stage_run_id,
        frozen=frozen,
    )
    attempt_id = uuid4()
    await seed.repository.create_attempt(
        lease,
        stage_run_id=candidate.stage_run_id,
        attempt_id=attempt_id,
        stage_request_json=None,
        stage_request_bytes=None,
        stage_request_digest=None,
        reservations=(),
    )
    async with seed.sessions() as db:
        attempt = await db.get(ExecutionAttempt, attempt_id)
    assert attempt is not None

    index_payload = canonical_document(
        PlatformFanoutIndexV1.model_validate(
            {
                "schema_version": "loom.platform-fanout-index.v1",
                "items": [
                    {"shard_key": "a", "output_name": "out_a"},
                    {"shard_key": "b", "output_name": "out_b"},
                ],
            }
        )
    )
    output_payloads = {
        "index": index_payload,
        "out_a": canonical_document({"schema_version": "example.item.v1", "slot": "a"}),
        "out_b": canonical_document({"schema_version": "example.item.v1", "slot": "b"}),
    }
    stage_result = StageResultV1.model_validate(
        {
            "schema_version": "loom.stage-result.v1",
            "domain_outcome": "generated",
            "reason_code": "generated",
            "retry_class": "none",
            "inputs": [],
            "outputs": [
                {"name": "index", "artifact_type": "loom.platform-fanout-index.v1"},
                {"name": "out_a", "artifact_type": "example.item.v1"},
                {"name": "out_b", "artifact_type": "example.item.v1"},
            ],
            "metrics": {},
            "provenance": {
                "pipeline_run_id": seed.run_id,
                "stage_run_id": candidate.stage_run_id,
                "execution_attempt_id": attempt_id,
                "recipe_digest": DIGEST,
                "execution_spec_digest": canonical_digest(spec),
                "image_digest": "sha256:" + "b" * 64,
            },
            "error": None,
        }
    )
    request = FinalOutputPrepareRequestV1(
        schema_version="loom.final-output-prepare.v1",
        stage_result=stage_result,
        stage_result_sha256=canonical_digest(stage_result),
        files=[
            {
                "output_name": name,
                "relative_path": f"artifacts/{name}/artifact.json",
                "size_bytes": len(payload),
                "sha256": digest(payload),
            }
            for name, payload in output_payloads.items()
        ],
    )
    store = FakeObjectStore()
    sql_repository = SqlArtifactCommitRepository(
        session_factory=seed.sessions,
        store=store,
        bucket="artifacts",
    )
    service = ArtifactCommitService(
        store=store,
        bucket="artifacts",
        repository=sql_repository,
    )
    route = FinalOutputRouteService(service=service, session_factory=seed.sessions)
    grant = UploadSessionGrantV1.model_validate_json(
        canonical_document(
            await route.prepare(attempt=attempt, request=request, request_id=uuid4())
        )
    )
    auth = UploadAuthV1(upload_token=grant.upload_token)
    assert [plan.producer for plan in grant.files] == [
        "container",
        "container",
        "container",
        "platform",
    ]
    for plan in grant.files:
        if plan.producer != "container":
            continue
        payload = output_payloads[plan.artifact_name]
        receipt = await service.write_part(
            session_id=grant.upload_session_id,
            file_index=plan.file_index,
            part_number=1,
            content_length=len(payload),
            content_sha256=digest(payload),
            body=chunks(payload),
            auth=auth,
        )
        await service.complete_file(
            session_id=grant.upload_session_id,
            file_index=plan.file_index,
            ordered_parts=[receipt],
            auth=auth,
        )

    first = await route.commit(
        attempt=attempt,
        session_id=grant.upload_session_id,
        upload_token=grant.upload_token,
        request_id=uuid4(),
        request={},
    )
    second = await route.commit(
        attempt=attempt,
        session_id=grant.upload_session_id,
        upload_token=grant.upload_token,
        request_id=uuid4(),
        request={},
    )
    assert first == second
    async with seed.sessions() as db:
        upload = await db.get(ArtifactUploadSession, grant.upload_session_id)
    assert upload is not None
    assert upload.state == "committed_ready"
    assert upload.canonical_manifest_json is not None
    assert upload.manifest_sha256 == first["manifest_sha256"]
    assert upload.committed_marker_sha256 == first["committed_marker_sha256"]
    manifest = ArtifactCommitManifestV1.model_validate_json(
        canonical_document(upload.canonical_manifest_json)
    )
    records = {record.artifact_name: record for record in manifest.artifacts}
    assert set(records) == {"index", "manifest", "out_a", "out_b"}
    platform_bytes = store.objects[
        (
            "artifacts",
            next(
                key
                for bucket, key in store.objects
                if bucket == "artifacts"
                and key.endswith(f"artifacts/{records['manifest'].artifact_id}/artifact.json")
            ),
        )
    ]
    platform = FanoutManifestV1.model_validate_json(platform_bytes)
    assert [item.shard_key for item in platform.items] == ["a", "b"]
    assert [item.artifact_bindings[0].artifact_id for item in platform.items] == [
        records["out_a"].artifact_id,
        records["out_b"].artifact_id,
    ]
    report = ExecutionCompleteV1(
        exit_code=0,
        stage_result=stage_result,
        stage_result_sha256=canonical_digest(stage_result),
        final_output_upload_session_id=grant.upload_session_id,
    )
    worker_id = uuid4()
    claim_id = uuid4()
    async with seed.sessions() as db, db.begin():
        persisted_attempt = await db.get(ExecutionAttempt, attempt_id)
        assert persisted_attempt is not None
        committed_bytes = await ExecutionAttemptCompletionService().complete(
            attempt=persisted_attempt,
            report=report,
            session=db,
        )
        await db.execute(
            text("""
                INSERT INTO workers (
                    id, hostname, version, capabilities, supported_work_kinds,
                    pool_name, registered_at, last_seen_at, status
                ) VALUES (
                    :id, 'fanout-fixture', 'test', '[]'::jsonb,
                    ARRAY['trial','execution_attempt']::text[],
                    'behavior-gpu-oldlab', clock_timestamp(), clock_timestamp(), 'active'
                )
            """),
            {"id": worker_id},
        )
        await db.execute(
            text("""
                UPDATE pipeline_stage_runs
                   SET state='succeeded', domain_outcome='generated',
                       reason_code='generated', finished_at=clock_timestamp()
                 WHERE id=:stage_id
            """),
            {"stage_id": candidate.stage_run_id},
        )
        await db.execute(
            text("""
                UPDATE execution_attempts
                   SET state='succeeded', worker_id=:worker_id, claim_id=:claim_id,
                       lease_token_digest=:lease_digest,
                       lease_expires_at=clock_timestamp() + interval '5 minutes',
                       claimed_at=clock_timestamp(), started_at=clock_timestamp(),
                       exit_code=0, retry_class='none', reason_code='generated',
                       result_manifest_json=CAST(:result AS jsonb),
                       result_manifest_digest=:result_digest, finished_at=clock_timestamp()
                 WHERE id=:attempt_id
            """),
            {
                "attempt_id": attempt_id,
                "worker_id": worker_id,
                "claim_id": claim_id,
                "lease_digest": "1" * 64,
                "result": json.dumps(stage_result.model_dump(mode="json")),
                "result_digest": canonical_digest(stage_result),
            },
        )
    assert committed_bytes["control"] == len(platform_bytes)
    sources = await seed.repository.fanout_source_candidates(lease)
    assert len(sources) == 1
    assert sources[0].node_key == "consume"
    assert sources[0].source_stage_run_id == candidate.stage_run_id
    expanded = await FanoutExpansionRuntime(
        repository=seed.repository,
        store=store,
        bucket="artifacts",
    ).reconcile(lease)
    assert expanded == 2
    assert await seed.repository.fanout_source_candidates(lease) == ()
    async with seed.sessions() as db:
        child_keys = list(
            (
                await db.execute(
                    text("""
                        SELECT shard_key FROM pipeline_stage_runs
                         WHERE pipeline_run_id=:run_id AND node_key='consume'
                         ORDER BY shard_key
                    """),
                    {"run_id": seed.run_id},
                )
            ).scalars()
        )
    assert child_keys == ["a", "b"]
    assert await seed.repository.reconcile_dependencies_and_gates(lease) == 3
    candidates = await seed.repository.readiness_candidates(lease)
    child_candidates = [candidate for candidate in candidates if candidate.node_key == "consume"]
    assert [candidate.shard_key for candidate in child_candidates] == ["a", "b"]
    assert [
        candidate.fanout_item_json["shard_key"]
        for candidate in child_candidates
        if candidate.fanout_item_json is not None
    ] == ["a", "b"]
    assert [
        candidate.ordinary_input_bindings_json[0]["items"][0]["artifact_id"]
        for candidate in child_candidates
        if candidate.ordinary_input_bindings_json is not None
    ] == [
        str(records["out_a"].artifact_id),
        str(records["out_b"].artifact_id),
    ]
    terminal_candidate = next(
        candidate for candidate in candidates if candidate.node_key == "finalize"
    )
    assert terminal_candidate.terminal_snapshot is not None
    assert terminal_candidate.terminal_snapshot.stages_json[0]["node_key"] == "root"
    assert terminal_candidate.terminal_snapshot.stages_json[0]["terminal_state"] == "succeeded"
    assert terminal_candidate.ordinary_input_bindings_json is not None
    assert terminal_candidate.ordinary_input_bindings_json[0]["items"][0]["artifact_id"] == str(
        records["manifest"].artifact_id
    )
    await seed.repository.release(lease)
    async with seed.sessions() as db, db.begin():
        await db.execute(
            text("DELETE FROM pipeline_runs WHERE id=:id"), {"id": seed.run_id}
        )
        await db.execute(text("DELETE FROM workers WHERE id=:id"), {"id": worker_id})
