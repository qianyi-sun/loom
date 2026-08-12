from __future__ import annotations

import csv
import io
from pathlib import Path
from uuid import uuid4

import numpy as np
import pytest
from pydantic import ValidationError

from loom.pipeline.behavior_input_import import (
    BehaviorInputImportManifestV1,
    build_artifact_document,
    validate_mop_npz,
    validate_test_instances_csv,
)
from loom.pipeline.keys import canonical_digest


def _policy_manifest() -> dict[str, object]:
    files = [
        {
            "path": "checkpoint/weights.bin",
            "sha256": "a" * 64,
            "size_bytes": 3,
            "media_type": "application/octet-stream",
        }
    ]
    tree = [
        {
            "relative_path": "weights.bin",
            "sha256": "sha256:" + "a" * 64,
            "size_bytes": 3,
        }
    ]
    return {
        "schema_version": "behavior.input-import.v1",
        "kind": "policy",
        "name": "pi-policy",
        "version": "1",
        "upstream": {"type": "artifact", "locator": "owner/policy", "revision": "r1"},
        "compatibility": {
            "kind": "policy",
            "architecture": "pi_behavior_b1k_fast",
            "action_dim": 23,
            "state_dim": 23,
            "robot_action_dim": 25,
            "checkpoint_format": "openpi_checkpoint_directory_v1",
            "checkpoint_root": "payload/checkpoint",
            "checkpoint_tree_sha256": canonical_digest(tree, persisted=False),
            "model_identifier": "pi0-behavior-r1",
            "vla_interface_version": "behavior_b1k_websocket_v1",
            "controller_adapter_version": "r1pro_25_to_pi23_v1",
        },
        "files": files,
    }


def test_policy_manifest_is_closed_and_recomputes_checkpoint_tree() -> None:
    manifest = BehaviorInputImportManifestV1.model_validate(_policy_manifest())
    document = build_artifact_document(
        manifest,
        actor_user_id=uuid4(),
        control_event_id=uuid4(),
        recipe_digest="sha256:" + "b" * 64,
        loom_commit_sha="c" * 40,
    )
    assert document["schema_version"] == "behavior_policy_checkpoint.v1"
    assert document["files"][0]["relative_path"] == "payload/checkpoint/weights.bin"
    assert "input-manifest.json" not in str(document)

    drifted = _policy_manifest()
    drifted["compatibility"] = {
        **drifted["compatibility"],  # type: ignore[arg-type]
        "checkpoint_tree_sha256": "sha256:" + "0" * 64,
    }
    with pytest.raises(ValidationError, match="checkpoint_tree_sha256"):
        BehaviorInputImportManifestV1.model_validate(drifted)


def test_manifest_rejects_kind_union_and_inventory_case_collisions() -> None:
    wrong = _policy_manifest()
    wrong["kind"] = "dataset"
    with pytest.raises(ValidationError, match="kind"):
        BehaviorInputImportManifestV1.model_validate(wrong)

    collided = _policy_manifest()
    collided["files"] = [
        *collided["files"],  # type: ignore[misc]
        {
            "path": "Checkpoint/weights.bin",
            "sha256": "a" * 64,
            "size_bytes": 3,
            "media_type": "application/octet-stream",
        },
    ]
    with pytest.raises(ValidationError):
        BehaviorInputImportManifestV1.model_validate(collided)


def test_test_instance_csv_preserves_selector_to_engine_mapping() -> None:
    from loom.integrations.behavior.contracts import TestInstanceSetV1

    expected = [
        TestInstanceSetV1(
            behavior_task_id=7,
            task_name="collect-cans",
            engine_task_instance_ids=[37, 4, 99],
        )
    ]
    output = io.StringIO(newline="")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(["task_id", "task_name", "test_instances"])
    writer.writerow([0, "collect-cans", "37,4,99"])
    validate_test_instances_csv(output.getvalue().encode(), expected)
    with pytest.raises(ValueError, match="selector order"):
        validate_test_instances_csv(
            output.getvalue().replace("37,4,99", "4,37,99").encode(), expected
        )


def test_mop_builder_emits_pickle_free_closed_npz(tmp_path: Path) -> None:
    from loom.integrations.behavior.recovery.mop.bank_builder import (
        build_training_bank,
        fixed_unicode_scalar,
    )

    destination = tmp_path / "bank.npz"
    zeros3 = np.zeros((2, 3), dtype=np.float32)
    quat = np.asarray([[0, 0, 0, 1], [0, 0, 0, 1]], dtype=np.float32)
    columns = {
        "episode_id": [1, 1],
        "step": [0, 1],
        "kind": ["event", "temporal"],
        "object": ["can", "can"],
        "category": ["container", "container"],
        "manip_object": ["can", "can"],
        "corrected_end_step": [0, 1],
        "stage_frac": [0.0, 1.0],
        "joint_positions": np.zeros((2, 28), dtype=np.float32),
        "base_rel": zeros3,
        "standoff_left": [0.0, 0.0],
        "standoff_right": [0.0, 0.0],
        "eef_rel_pos_left": zeros3,
        "eef_rel_quat_left": quat,
        "eef_rel_pos_right": zeros3,
        "eef_rel_quat_right": quat,
    }
    build_training_bank(
        destination,
        behavior_task_id=7,
        source_revision="r1",
        source_inputs=[
            {
                "relative_path": "banks/task-0007/sources/source.json",
                "sha256": "sha256:" + "c" * 64,
                "size_bytes": 3,
            }
        ],
        columns=columns,
    )
    validate_mop_npz(
        destination.read_bytes(), behavior_task_id=7, row_count=2, source_revision="r1"
    )

    astral = chr(0x1F642) * 4096
    assert str(fixed_unicode_scalar(astral)[()]) == astral
    with pytest.raises(ValueError, match="4096 Unicode scalar"):
        fixed_unicode_scalar(astral + "x")
