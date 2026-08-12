from __future__ import annotations

from collections.abc import Iterator
from uuid import UUID

import pytest

from loom.pipeline.artifact_commit import (
    ArtifactCommitService,
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
from loom_service import behavior_pipeline_adapter
from loom_service.pipeline_api_service import _behavior_unknown_input_is_admissible


class _AtomicBehaviorShape:
    def declare(
        self, *, frozen_request: InputMaterializationRequestV1
    ) -> MaterializationDeclarationV1:
        del frozen_request
        raw = {
            "outputs": [
                MaterializationOutputV1(
                    logical_name="task_instance_000000000000",
                    artifact_type="behavior_task_instance.v1",
                    max_bytes=1024,
                    lineage_source_ids=[],
                ),
                MaterializationOutputV1(
                    logical_name="task_instances",
                    artifact_type="loom.fanout-manifest.v1",
                    max_bytes=1024,
                    lineage_source_ids=[],
                ),
                MaterializationOutputV1(
                    logical_name="task_set",
                    artifact_type="behavior_taskset_snapshot.v1",
                    max_bytes=1024,
                    lineage_source_ids=[],
                ),
            ],
            "result_bindings": [
                MaterializationResultBindingV1(
                    graph_input_name="task_set", logical_name="task_set"
                ),
                MaterializationResultBindingV1(
                    graph_input_name="task_instances", logical_name="task_instances"
                ),
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
        del artifact_ids
        for output in declaration.outputs:
            yield MaterializedArtifactDocumentV1(
                logical_name=output.logical_name,
                value={"schema_version": output.artifact_type},
            )


async def test_atomic_behavior_batch_commits_all_three_shapes_and_replays() -> None:
    source: dict[str, object] = {"source": "frozen"}
    parameters: dict[str, object] = {"episodes_per_instance": 1, "seed_base": 0}
    request = InputMaterializationRequestV1(
        schema_version="loom.input-materialization-request.v1",
        materialization_id=UUID(int=1),
        team_id=UUID(int=2),
        actor_user_id=UUID(int=3),
        recipe=RecipeIdentityV1(name="behavior-recovery", version=1, digest="sha256:" + "a" * 64),
        source_snapshot=source,
        source_snapshot_digest=canonical_digest(source),
        parameters=parameters,
        parameters_digest=canonical_digest(parameters),
    )
    producer = InputMaterializationProducerV1(
        commit_kind="input_materialization",
        team_id=request.team_id,
        pipeline_input_materialization_id=request.materialization_id,
        actor_user_id=request.actor_user_id,
    )
    service = PipelineInputMaterializationService(
        artifact_committer=ArtifactCommitService(store=FakeObjectStore(), bucket="artifacts"),
        materializers={"behavior-recovery": _AtomicBehaviorShape()},
    )
    first, _ = await service.materialize(
        frozen_request=request,
        producer=producer,
        materializer_kind="behavior-recovery",
        idempotency_key="behavior-batch",
    )
    replay, _ = await service.materialize(
        frozen_request=request,
        producer=producer,
        materializer_kind="behavior-recovery",
        idempotency_key="behavior-batch",
    )
    assert first == replay
    assert [item.artifact_type for item in first.artifacts] == [
        "behavior_task_instance.v1",
        "loom.fanout-manifest.v1",
        "behavior_taskset_snapshot.v1",
    ]


def test_startup_installs_one_explicit_pipeline_adapter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from types import SimpleNamespace

    app = SimpleNamespace(state=SimpleNamespace())
    marker = object()
    observed: dict[str, object] = {}

    def build(*, settings: object, recipe_registry: object) -> object:
        observed.update(settings=settings, recipe_registry=recipe_registry)
        return marker

    monkeypatch.setattr(behavior_pipeline_adapter, "build_behavior_pipeline_public_adapter", build)
    settings = object()
    behavior_pipeline_adapter.install_behavior_pipeline_public_adapter(app=app, settings=settings)
    assert app.state.pipeline_public_adapter is marker
    assert observed["settings"] is settings
    assert observed["recipe_registry"] is app.state.pipeline_recipe_registry


async def test_unknown_safety_exception_is_exactly_same_team_official_import() -> None:
    from types import SimpleNamespace

    team_id = UUID(int=101)
    import_id = UUID(int=102)
    upload_id = UUID(int=103)
    artifact_id = UUID(int=104)
    digest = "sha256:" + "a" * 64
    artifact = SimpleNamespace(
        id=artifact_id,
        team_id=team_id,
        artifact_type="behavior_dataset_snapshot.v1",
        safety_state="unknown",
        producer_kind="input_import",
        pipeline_input_import_id=import_id,
        artifact_upload_session_id=upload_id,
        manifest_sha256=digest,
        provenance={"root_manifest_sha256": digest, "marker_sha256": digest},
    )
    imported = SimpleNamespace(
        team_id=team_id,
        kind="dataset",
        target_artifact_type="behavior_dataset_snapshot.v1",
        trust_class="internal_trusted",
        state="committed",
        recipe_name="behavior-recovery",
        recipe_version=1,
        recipe_digest=digest,
        committed_artifact_id=artifact_id,
        artifact_upload_session_id=upload_id,
    )
    upload = SimpleNamespace(
        id=upload_id,
        state="committed",
        manifest_sha256=digest,
        committed_marker_sha256=digest,
    )

    class Session:
        async def get(self, model: object, identity: UUID) -> object:
            del model
            return imported if identity == import_id else upload

    assert await _behavior_unknown_input_is_admissible(
        Session(),  # type: ignore[arg-type]
        artifact=artifact,  # type: ignore[arg-type]
        team_id=team_id,
        recipe_name="behavior-recovery",
        recipe_version=1,
        recipe_digest=digest,
        input_name="dataset",
    )
    assert not await _behavior_unknown_input_is_admissible(
        Session(),  # type: ignore[arg-type]
        artifact=artifact,  # type: ignore[arg-type]
        team_id=team_id,
        recipe_name="other-recipe",
        recipe_version=1,
        recipe_digest=digest,
        input_name="dataset",
    )
