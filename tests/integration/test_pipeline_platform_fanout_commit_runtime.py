from __future__ import annotations

from uuid import uuid4

from loom.db.schema import ArtifactUploadSession, ExecutionAttempt
from loom.pipeline.artifact_commit import (
    ArtifactCommitManifestV1,
    ArtifactCommitService,
    UploadAuthV1,
    UploadSessionGrantV1,
)
from loom.pipeline.keys import canonical_digest, canonical_document
from loom.pipeline.spec import FanoutManifestV1, PlatformFanoutIndexV1
from loom.pipeline.state import StageResultV1
from loom.pipeline.work_protocol import FinalOutputPrepareRequestV1
from loom.trajectory.storage import FakeObjectStore
from loom_control_plane.artifact_commit_runtime import (
    FinalOutputRouteService,
    SqlArtifactCommitRepository,
)
from loom_pipeline_orchestrator.repository import FrozenReadiness
from tests.integration.pipeline_artifact_testkit import chunks, digest
from tests.integration.pipeline_orchestrator_fixtures import DIGEST, OrchestratorSeed


async def test_platform_fanout_manifest_is_synthesized_inside_final_marker(
    orchestrator_seed: OrchestratorSeed,
) -> None:
    seed = orchestrator_seed
    lease = (await seed.repository.claim_runs(controller_id="fanout-commit"))[0]
    await seed.repository.initialize_run(lease)
    candidate = (await seed.repository.readiness_candidates(lease))[0]
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
    spec = {
        "schema_version": "loom.execution-spec.v1",
        "recipe_digest": DIGEST,
        "container_node": node,
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
    await seed.repository.release(lease)
