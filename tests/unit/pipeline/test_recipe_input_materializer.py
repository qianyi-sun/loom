from __future__ import annotations

from uuid import UUID

import pytest

from loom.integrations.behavior.contracts import MOP_REQUIRED_COLUMNS
from loom.pipeline.behavior_materialization import BehaviorRecipeInputMaterializer
from loom.pipeline.input_materialization import InputMaterializationRequestV1
from loom.pipeline.keys import canonical_digest
from loom.pipeline.spec import RecipeIdentityV1

DIGEST = "sha256:" + "a" * 64


def _request(
    *, engine_ids: list[int] | None = None, episodes: int = 1
) -> InputMaterializationRequestV1:
    team_id = UUID("00000000-0000-0000-0000-000000000001")
    user_id = UUID("00000000-0000-0000-0000-000000000002")
    ids = engine_ids or [37]
    dataset_ids = [7]
    cards = [
        {
            "behavior_task_id": 7,
            "task_name": "collect-cans",
            "relative_path": "agentic_sweep/task_cards/task-0007.md",
            "sha256": DIGEST,
            "size_bytes": 10,
        }
    ]
    demos = [
        {
            "behavior_task_id": 7,
            "episodes": [
                {
                    "episode_id": "episode_00070000",
                    "files": [
                        {
                            "camera": camera,
                            "relative_path": (
                                f"videos/task-0007/observation.images.rgb.{camera}/"
                                "episode_00070000.mp4"
                            ),
                            "sha256": DIGEST,
                            "size_bytes": 10,
                            "frame_count": 30,
                        }
                        for camera in ("head", "left_wrist", "right_wrist")
                    ],
                }
            ],
        }
    ]
    test_sets = [
        {
            "behavior_task_id": 7,
            "task_name": "collect-cans",
            "engine_task_instance_ids": ids,
        }
    ]
    bank_files = [
        {
            "behavior_task_id": 7,
            "relative_path": "banks/task-0007/task_7_training_bank.npz",
            "sha256": DIGEST,
            "size_bytes": 10,
            "row_count": 2,
        }
    ]
    coverage = [
        {
            "behavior_task_id": 7,
            "annotated_subtask_count": 1,
            "event_row_count": 1,
            "temporal_row_count": 1,
        }
    ]
    dataset = {
        "kind": "dataset",
        "omnigibson_version": "3.8",
        "dataset_schema_version": "behavior_dataset_snapshot.v1",
        "omnigibson_dataset_layout_version": "behavior_1k_runtime_v1",
        "omnigibson_dataset_root": "payload/omnigibson",
        "runtime_roots": [
            {
                "name": "behavior-1k-assets",
                "relative_path": "omnigibson/behavior-1k-assets",
                "revision": "r1",
                "tree_sha256": DIGEST,
            },
            {
                "name": "2025-challenge-task-instances",
                "relative_path": "omnigibson/2025-challenge-task-instances",
                "revision": "r1",
                "tree_sha256": DIGEST,
                "episodes_jsonl_sha256": DIGEST,
                "test_instances_csv_sha256": DIGEST,
                "scenes_tree_sha256": DIGEST,
            },
        ],
        "task_universe_sha256": canonical_digest(dataset_ids, persisted=False),
        "task_count": 1,
        "agentic_task_cards": cards,
        "agentic_task_cards_sha256": canonical_digest(cards, persisted=False),
        "agentic_demo_video_sets": demos,
        "agentic_demo_video_sets_sha256": canonical_digest(demos, persisted=False),
        "test_instance_sets": test_sets,
        "test_instance_sets_sha256": canonical_digest(test_sets, persisted=False),
    }
    mop = {
        "kind": "mop_bank",
        "bank_schema_version": "training_bank_v2_pickle_free",
        "column_contract_version": "behavior_mop_bank_npz_v1",
        "pose_dim": 28,
        "action_dim": 23,
        "task_universe_sha256": canonical_digest(dataset_ids, persisted=False),
        "source_revision": "r1",
        "sampling_mode": "event_and_temporal",
        "required_columns": MOP_REQUIRED_COLUMNS,
        "row_count": 2,
        "bank_files": bank_files,
        "bank_files_sha256": canonical_digest(bank_files, persisted=False),
        "task_coverage": coverage,
        "task_coverage_sha256": canonical_digest(coverage, persisted=False),
    }
    refs = {
        name: {
            "artifact_id": f"00000000-0000-0000-0000-00000000000{index}",
            "artifact_type": artifact_type,
            "manifest_sha256": DIGEST,
            "content_sha256": DIGEST,
        }
        for index, (name, artifact_type) in enumerate(
            (
                ("dataset", "behavior_dataset_snapshot.v1"),
                ("policy", "behavior_policy_checkpoint.v1"),
                ("mop_bank", "behavior_mop_bank.v1"),
            ),
            start=3,
        )
    }
    source = {
        "source_task_set": {
            "task_set_id": "ts/team/behavior",
            "owning_team_id": str(team_id),
            "manifest_generation": 1,
            "manifest_sha256": DIGEST,
            "intents": ["evaluation", "trajectory_generation"],
            "evaluation_ready": True,
        },
        "tasks": [
            {
                "loom_task_id": "task-0007",
                "behavior_task_id": 7,
                "task_name": "collect-cans",
                "semantic_task_id": "behavior/7",
                "task_checksum": DIGEST,
                "source_bddl_path": "omnigibson/2025-challenge-task-instances/task.bddl",
                "eligible_eval_instance_ids": list(range(len(ids))),
                "engine_task_instance_ids": ids,
                "task_bundle": {"kind": "object", "object_sha256": DIGEST, "size_bytes": 10},
            }
        ],
        "companion_inputs": refs,
        "dataset_compatibility": dataset,
        "mop_bank_compatibility": mop,
        "control_event_id": "00000000-0000-0000-0000-000000000006",
        "loom_commit_sha": "b" * 40,
        "caller_idempotency_key": "materialize-test",
    }
    parameters = {"episodes_per_instance": episodes, "seed_base": 0}
    return InputMaterializationRequestV1(
        schema_version="loom.input-materialization-request.v1",
        materialization_id=UUID("00000000-0000-0000-0000-000000000007"),
        team_id=team_id,
        actor_user_id=user_id,
        recipe=RecipeIdentityV1(name="behavior-recovery", version=1, digest=DIGEST),
        source_snapshot=source,
        source_snapshot_digest=canonical_digest(source),
        parameters=parameters,
        parameters_digest=canonical_digest(parameters),
    )


def test_selector_zero_resolves_engine_37_and_fanout_matches_embedded_child() -> None:
    request = _request()
    materializer = BehaviorRecipeInputMaterializer()
    declaration = materializer.declare(frozen_request=request)
    artifact_ids = {
        output.logical_name: UUID(int=index + 20)
        for index, output in enumerate(declaration.outputs)
    }
    documents = list(materializer.render(declaration=declaration, artifact_ids=artifact_ids))
    child = documents[0].value["payload"]
    fanout = documents[-2].value
    snapshot = documents[-1].value["payload"]
    assert child["eval_instance_index"] == 0
    assert child["engine_task_instance_id"] == 37
    assert child["demo_id"] == 70370
    assert child["demo_stem"] == "episode_00070370"
    assert fanout["items"][0]["shard_key"] == child["task_instance_identity"]
    assert snapshot["task_instances"][0]["payload"] == child
    assert [item.graph_input_name for item in declaration.result_bindings] == [
        "task_set",
        "task_instances",
    ]


def test_zero_or_over_200_children_fails_before_render() -> None:
    with pytest.raises(ValueError, match=r"1\.\.200"):
        BehaviorRecipeInputMaterializer().declare(
            frozen_request=_request(engine_ids=list(range(21)), episodes=10)
        )


def test_selector_order_changes_identity() -> None:
    first = BehaviorRecipeInputMaterializer()
    first_request = _request(engine_ids=[37, 4])
    first_declaration = first.declare(frozen_request=first_request)
    first_docs = list(
        first.render(
            declaration=first_declaration,
            artifact_ids={
                item.logical_name: UUID(int=index + 50)
                for index, item in enumerate(first_declaration.outputs)
            },
        )
    )
    second = BehaviorRecipeInputMaterializer()
    second_request = _request(engine_ids=[4, 37])
    second_declaration = second.declare(frozen_request=second_request)
    second_docs = list(
        second.render(
            declaration=second_declaration,
            artifact_ids={
                item.logical_name: UUID(int=index + 50)
                for index, item in enumerate(second_declaration.outputs)
            },
        )
    )
    assert (
        first_docs[0].value["payload"]["task_instance_identity"]
        != second_docs[1].value["payload"]["task_instance_identity"]
    )
