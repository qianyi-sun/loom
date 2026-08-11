from __future__ import annotations

from collections.abc import Iterator
from uuid import UUID, uuid4

from loom.pipeline.artifact_commit import (
    ArtifactCommitService,
    AuthoritativeArtifactDocumentV1,
    InputMaterializationProducerV1,
)
from loom.pipeline.input_materialization import (
    InputMaterializationRequestV1,
    MaterializationDeclarationV1,
    MaterializationOutputV1,
    MaterializationResultBindingV1,
    MaterializedArtifactDocumentV1,
    PipelineInputMaterializationService,
)
from loom.pipeline.keys import canonical_digest
from loom.pipeline.spec import RecipeIdentityV1
from loom.trajectory.storage import FakeObjectStore


async def test_materialization_document_is_service_generated_and_atomic() -> None:
    producer = InputMaterializationProducerV1(
        commit_kind="input_materialization",
        team_id=uuid4(),
        pipeline_input_materialization_id=uuid4(),
        actor_user_id=uuid4(),
    )
    raw = {
        "producer": producer,
        "artifact_id": uuid4(),
        "artifact_name": "taskset",
        "artifact_type": "behavior.taskset-snapshot.v1",
        "relative_path": "artifact.json",
        "semantic_document": {"schema_version": "behavior.taskset-snapshot.v1"},
        "max_bytes": 1024,
    }
    declaration = AuthoritativeArtifactDocumentV1(
        **raw, declaration_digest=canonical_digest(raw, persisted=False)
    )
    result = await ArtifactCommitService(
        store=FakeObjectStore(), bucket="artifacts"
    ).commit_authoritative_document(declaration=declaration)
    assert result.state == "committed" and result.artifacts[0].safety_state == "verified_internal"


class _TwoOutputMaterializer:
    render_count = 0

    def declare(
        self, *, frozen_request: InputMaterializationRequestV1
    ) -> MaterializationDeclarationV1:
        del frozen_request
        raw = {
            "outputs": [
                MaterializationOutputV1(
                    logical_name="children",
                    artifact_type="loom.fanout-manifest.v1",
                    max_bytes=1024,
                    lineage_source_ids=[],
                ),
                MaterializationOutputV1(
                    logical_name="taskset",
                    artifact_type="behavior.taskset-snapshot.v1",
                    max_bytes=1024,
                    lineage_source_ids=[],
                ),
            ],
            "result_bindings": [
                MaterializationResultBindingV1(
                    graph_input_name="task_instances", logical_name="children"
                )
            ],
            "source_artifact_refs": [],
        }
        return MaterializationDeclarationV1(
            **raw,
            materialization_identity_digest=canonical_digest(raw, persisted=False),
        )

    def render(
        self,
        *,
        declaration: MaterializationDeclarationV1,
        artifact_ids: dict[str, UUID],
    ) -> Iterator[MaterializedArtifactDocumentV1]:
        del declaration
        self.render_count += 1
        yield MaterializedArtifactDocumentV1(
            logical_name="children",
            value={"items": [], "artifact_id": str(artifact_ids["children"])},
        )
        yield MaterializedArtifactDocumentV1(
            logical_name="taskset",
            value={"tasks": [], "artifact_id": str(artifact_ids["taskset"])},
        )


async def test_two_output_materialization_streams_distinct_artifacts_and_replays() -> None:
    source_snapshot: dict[str, object] = {"sources": []}
    parameters: dict[str, object] = {"shards": 1}
    request = InputMaterializationRequestV1(
        schema_version="loom.input-materialization-request.v1",
        materialization_id=uuid4(),
        team_id=uuid4(),
        actor_user_id=uuid4(),
        recipe=RecipeIdentityV1(
            name="behavior-recovery",
            version=1,
            digest="sha256:" + "a" * 64,
        ),
        source_snapshot=source_snapshot,
        source_snapshot_digest=canonical_digest(source_snapshot),
        parameters=parameters,
        parameters_digest=canonical_digest(parameters),
    )
    producer = InputMaterializationProducerV1(
        commit_kind="input_materialization",
        team_id=request.team_id,
        pipeline_input_materialization_id=request.materialization_id,
        actor_user_id=request.actor_user_id,
    )
    store = FakeObjectStore()
    materializer = _TwoOutputMaterializer()
    service = PipelineInputMaterializationService(
        artifact_committer=ArtifactCommitService(store=store, bucket="artifacts"),
        materializers={"behavior-recovery": materializer},
    )

    first, declaration = await service.materialize(
        frozen_request=request,
        producer=producer,
        materializer_kind="behavior-recovery",
        idempotency_key="materialize:one",
    )
    replay, replay_declaration = await service.materialize(
        frozen_request=request,
        producer=producer,
        materializer_kind="behavior-recovery",
        idempotency_key="materialize:one",
    )

    assert declaration == replay_declaration
    assert first == replay
    assert materializer.render_count == 1
    assert len(first.artifacts) == 2
    assert len({item.id for item in first.artifacts}) == 2
    assert len({item.manifest_sha256 for item in first.artifacts}) == 2
    artifact_documents = [
        key
        for bucket, key in store.objects
        if bucket == "artifacts" and key.endswith("/artifact.json")
    ]
    assert len(artifact_documents) == 2
