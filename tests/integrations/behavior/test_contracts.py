from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from pathlib import Path
from uuid import UUID

import pytest
from pydantic import ValidationError

from loom.integrations.behavior.canonical_json import load_canonical_document
from loom.integrations.behavior.cli import main
from loom.integrations.behavior.contracts import (
    ARTIFACT_MODELS,
    BEHAVIOR_ARTIFACT_TYPES,
    SCHEMA_MODELS,
    BehaviorStage,
    ProviderAssetManifestV1,
    StageRequestV1,
    StageResultDocumentV1,
    validate_behavior_stage_result,
)
from loom.pipeline.keys import canonical_digest, canonical_document
from loom.pipeline.renderers.behavior_stage_request import render
from loom.pipeline.spec import BindingSetV1, RequestRendererLockV1
from loom.pipeline.state import StageResultV1

ROOT = Path(__file__).resolve().parents[3]
FIXTURES = Path(__file__).with_name("fixtures") / "contracts"
DIGEST_0 = "sha256:" + "0" * 64
DIGEST_1 = "sha256:" + "1" * 64
DIGEST_2 = "sha256:" + "2" * 64
DIGEST_3 = "sha256:" + "3" * 64
IMAGE = "registry.example.com/loom/behavior@sha256:" + "4" * 64
RUN_ID = UUID("00000000-0000-0000-0000-000000000001")
STAGE_RUN_ID = UUID("00000000-0000-0000-0000-000000000002")
ATTEMPT_ID = UUID("00000000-0000-0000-0000-000000000003")
ARTIFACT_ID = UUID("00000000-0000-0000-0000-000000000004")


def binding(*, cardinality: str = "one", count: int = 1) -> dict[str, object]:
    items = [
        {
            "artifact_id": str(UUID(int=4 + index)),
            "content_sha256": DIGEST_0,
            "file_count": 1,
            "item_key": "singleton" if cardinality == "one" else f"item-{index}",
            "manifest_sha256": DIGEST_1,
            "stored_size_bytes": 10,
            "unpacked_size_bytes": 20,
        }
        for index in range(count)
    ]
    return {
        "binding_name": "task_instance",
        "artifact_type": "behavior_task_instance.v1",
        "cardinality": cardinality,
        "items": items,
    }


def request_value(
    *,
    stage: str = "rollout",
    parameters: dict[str, object] | None = None,
    inputs: list[dict[str, object]] | None = None,
) -> dict[str, object]:
    selected_inputs = [binding()] if inputs is None else inputs
    resolved_digest = canonical_digest(selected_inputs)
    provider: dict[str, int] | None = None
    control: dict[str, object] | None = None
    if stage == "offline_judge":
        provider = {
            "provider_request_limit_per_attempt": 256,
            "provider_cost_limit_microusd_per_attempt": 30_000_000,
            "per_call_timeout_seconds": 60,
        }
        control = {
            "logical_name": "behavior_offline_judge",
            "kind": "judge_profile",
            "node_key": "offline_judge",
            "object_id": "00000000-0000-0000-0000-000000000010",
            "version": 1,
            "snapshot_sha256": DIGEST_0,
            "provider_asset_manifest_sha256": DIGEST_1,
            "judge_profile_id": "00000000-0000-0000-0000-000000000011",
            "judge_profile_version": 1,
            "agent": "codex",
            "agent_version": "0.146.0",
            "provider": "openai",
            "model": "gpt-5.6-sol",
        }
    if stage == "recovery" and parameters and parameters.get("stream") == "primitive":
        provider = {
            "provider_request_limit_per_attempt": 512,
            "provider_cost_limit_microusd_per_attempt": 30_000_000,
            "per_call_timeout_seconds": 600,
        }
        control = {
            "logical_name": "behavior_recovery_primitive",
            "kind": "provider",
            "node_key": "recovery_primitive",
            "object_id": "00000000-0000-0000-0000-000000000012",
            "version": 1,
            "snapshot_sha256": DIGEST_0,
            "provider_asset_manifest_sha256": DIGEST_1,
        }
    checkpoint_bytes = (
        16_777_216
        if stage == "recovery" and parameters and parameters.get("stream") == "mop"
        else 0
    )
    preimage = {
        "attempt_id": str(ATTEMPT_ID),
        "execution_spec_digest": DIGEST_2,
        "resolved_input_bindings_digest": resolved_digest,
        "stage_run_id": str(STAGE_RUN_ID),
    }
    return {
        "schema_version": "behavior.stage-request.v1",
        "stage": stage,
        "run_id": str(RUN_ID),
        "stage_run_id": str(STAGE_RUN_ID),
        "attempt_id": str(ATTEMPT_ID),
        "idempotency_key": canonical_digest(preimage, persisted=False),
        "inputs": selected_inputs,
        "parameters": parameters
        if parameters is not None
        else {
            "eval_instance_index": 0,
            "episode_index": 0,
            "seed": 7,
            "record_depth": False,
            "recording_fps": 30,
        },
        "budget": {
            "provider": provider,
            "gpu_seconds_limit": 0,
            "final_output_bytes_limit": 16_777_216,
            "checkpoint_bytes_limit": checkpoint_bytes,
            "timeout_seconds": 60,
            "max_attempts": 2,
        },
        "provenance": {
            "recipe_digest": DIGEST_0,
            "resolved_input_bindings_digest": resolved_digest,
            "execution_spec_digest": DIGEST_2,
            "image_digest": IMAGE,
            "loom_commit_sha": "5" * 40,
            "control_binding": control,
            "compatibility_manifest_sha256": DIGEST_3,
        },
        "orchestration": None,
    }


@pytest.mark.parametrize(
    ("stage", "parameters"),
    [
        ("input_preflight", {}),
        (
            "rollout",
            {
                "eval_instance_index": 0,
                "episode_index": 1,
                "seed": 7,
                "record_depth": False,
                "recording_fps": 30,
            },
        ),
        ("offline_judge", {"inspection_mode": "whole_episode_predicate_log"}),
        ("failure_materialize", {}),
        ("frame_author", {}),
        ("recovery", {"stream": "primitive", "sample_id": str(UUID(int=20))}),
        ("recovery", {"stream": "mop", "sample_id": str(UUID(int=21))}),
        ("dataset_build", {"format": "lerobot_v2.1"}),
    ],
)
def test_stage_request_round_trips_every_nonaggregate_parameter_branch(
    stage: str, parameters: dict[str, object]
) -> None:
    value = request_value(stage=stage, parameters=parameters)
    request = StageRequestV1.model_validate_json(json.dumps(value))

    assert request.model_dump(mode="json", exclude_none=False) == value
    assert request.budget.model_dump(mode="json", exclude_none=False).keys() == {
        "provider",
        "gpu_seconds_limit",
        "final_output_bytes_limit",
        "checkpoint_bytes_limit",
        "timeout_seconds",
        "max_attempts",
    }


def test_request_reuses_pipeline_binding_and_stage_result_types() -> None:
    assert StageResultDocumentV1 is StageResultV1
    parsed = StageRequestV1.model_validate_json(json.dumps(request_value()))
    assert isinstance(parsed.inputs[0], BindingSetV1)

    many = StageRequestV1.model_validate_json(
        json.dumps(request_value(inputs=[binding(cardinality="many", count=0)]))
    )
    assert many.inputs[0].items == []
    assert many.inputs[0].cardinality == "many"


def test_request_rejects_digest_idempotency_provider_and_extra_field_drift() -> None:
    value = request_value()
    value["idempotency_key"] = DIGEST_0
    with pytest.raises(ValidationError, match="idempotency_key"):
        StageRequestV1.model_validate_json(json.dumps(value))

    value = request_value()
    value["provenance"]["resolved_input_bindings_digest"] = DIGEST_0  # type: ignore[index]
    with pytest.raises(ValidationError, match="resolved_input_bindings_digest"):
        StageRequestV1.model_validate_json(json.dumps(value))

    value = request_value()
    value["budget"]["provider"] = {  # type: ignore[index]
        "provider_request_limit_per_attempt": 1,
        "provider_cost_limit_microusd_per_attempt": 1,
        "per_call_timeout_seconds": 1,
    }
    with pytest.raises(ValidationError, match="Provider-null"):
        StageRequestV1.model_validate_json(json.dumps(value))

    primitive = request_value(
        stage="recovery",
        parameters={"stream": "primitive", "sample_id": str(UUID(int=22))},
    )
    primitive["budget"]["provider"]["provider_request_limit_per_attempt"] = 511  # type: ignore[index]
    with pytest.raises(ValidationError, match="exactly 512"):
        StageRequestV1.model_validate_json(json.dumps(primitive))

    offline = request_value(
        stage="offline_judge",
        parameters={"inspection_mode": "whole_episode_predicate_log"},
    )
    offline["budget"]["provider"]["provider_request_limit_per_attempt"] = 257  # type: ignore[index]
    with pytest.raises(ValidationError, match="fixed profile caps"):
        StageRequestV1.model_validate_json(json.dumps(offline))

    non_nfc = request_value()
    non_nfc["inputs"][0]["items"][0]["item_key"] = "e\u0301"  # type: ignore[index]
    with pytest.raises(ValidationError, match="must already be NFC"):
        StageRequestV1.model_validate_json(json.dumps(non_nfc))

    value = request_value()
    value["unexpected"] = True
    with pytest.raises(ValidationError, match="Extra inputs"):
        StageRequestV1.model_validate_json(json.dumps(value))


def test_renderer_computes_the_attempt_scoped_idempotency_key() -> None:
    value = request_value()
    render_input = {
        key: item for key, item in value.items() if key not in {"schema_version", "idempotency_key"}
    }
    rendered = render(render_input)
    assert rendered.idempotency_key == value["idempotency_key"]
    assert rendered.model_dump(mode="json", exclude_none=False) == value


def test_aggregate_requires_exact_platform_orchestration_and_dataset_build_does_not() -> None:
    value = request_value(stage="aggregate", parameters={})
    value["orchestration"] = {
        "schema_version": "behavior.terminal-stage-set.v1",
        "pipeline_run_id": str(RUN_ID),
        "run_graph_digest": DIGEST_0,
        "snapshot_id": "00000000-0000-0000-0000-000000000099",
        "terminal_stage_keys": [
            "input_preflight",
            "rollout",
            "offline_judge",
            "failure_materialize",
            "frame_author",
            "recovery_mop",
            "recovery_primitive",
        ],
        "stages": [],
    }
    parsed = StageRequestV1.model_validate_json(json.dumps(value))
    assert parsed.orchestration is not None

    wrong = deepcopy(value)
    wrong["orchestration"]["terminal_stage_keys"][-2:] = [  # type: ignore[index]
        "recovery_primitive",
        "recovery_mop",
    ]
    with pytest.raises(ValidationError, match="strategy expansion"):
        StageRequestV1.model_validate_json(json.dumps(wrong))

    dataset_build = request_value(stage="dataset_build", parameters={"format": "lerobot_v2.1"})
    StageRequestV1.model_validate_json(json.dumps(dataset_build))


def test_mop_checkpoint_budget_is_per_checkpoint_and_provider_null() -> None:
    value = request_value(
        stage="recovery",
        parameters={"stream": "mop", "sample_id": str(UUID(int=30))},
    )
    parsed = StageRequestV1.model_validate_json(json.dumps(value))
    assert parsed.budget.checkpoint_bytes_limit == 16_777_216
    assert parsed.budget.provider is None


def stage_result_value(
    *, domain_outcome: str | None, reason_code: str, retry_class: str
) -> dict[str, object]:
    return {
        "schema_version": "loom.stage-result.v1",
        "domain_outcome": domain_outcome,
        "reason_code": reason_code,
        "retry_class": retry_class,
        "inputs": [],
        "outputs": [],
        "metrics": {},
        "provenance": {
            "pipeline_run_id": str(RUN_ID),
            "stage_run_id": str(STAGE_RUN_ID),
            "execution_attempt_id": str(ATTEMPT_ID),
            "recipe_digest": DIGEST_0,
            "execution_spec_digest": DIGEST_1,
            "image_digest": DIGEST_2,
        },
        "error": None
        if domain_outcome is not None
        else {"code": reason_code, "message": "redacted failure"},
    }


def test_behavior_stage_result_exit_outcome_and_retry_mapping_is_closed() -> None:
    success = stage_result_value(
        domain_outcome="rollout_failure", reason_code="task_failed", retry_class="none"
    )
    validate_behavior_stage_result(success, stage=BehaviorStage.ROLLOUT, exit_code=0)
    with pytest.raises(ValueError, match="not legal"):
        validate_behavior_stage_result(success, stage=BehaviorStage.FRAME_AUTHOR, exit_code=0)

    transient = stage_result_value(
        domain_outcome=None,
        reason_code="provider_429",
        retry_class="provider_transient",
    )
    validate_behavior_stage_result(transient, stage=BehaviorStage.OFFLINE_JUDGE, exit_code=21)
    transient["reason_code"] = "provider_timeout"
    with pytest.raises(ValueError, match="non-canonical retry reason"):
        validate_behavior_stage_result(transient, stage=BehaviorStage.OFFLINE_JUDGE, exit_code=21)


def test_all_registered_artifacts_have_closed_models_and_current_schemas() -> None:
    assert set(ARTIFACT_MODELS) == BEHAVIOR_ARTIFACT_TYPES
    schema_root = ROOT / "src/loom/integrations/behavior/schemas"
    for name, model in SCHEMA_MODELS.items():
        assert (schema_root / f"{name}.json").read_bytes() == canonical_document(
            model.model_json_schema(mode="validation")
        )


def test_renderer_lock_pins_exact_entrypoint_files_and_hashes() -> None:
    lock_path = (
        ROOT / "src/loom/integrations/behavior/schemas/behavior_stage_request.renderer-lock.v1.json"
    )
    lock = RequestRendererLockV1.model_validate(load_canonical_document(lock_path))
    expected_paths = [
        "src/loom/pipeline/renderers/behavior_stage_request.py",
        "src/loom/pipeline/renderers/behavior_stage_request_models.py",
        "src/loom/pipeline/renderers/schemas/behavior.stage-request.v1.json",
    ]
    assert lock.entrypoint == "loom.pipeline.renderers.behavior_stage_request:render"
    assert [item.repo_path for item in lock.files] == expected_paths
    for item in lock.files:
        assert (
            item.sha256
            == "sha256:" + hashlib.sha256((ROOT / item.repo_path).read_bytes()).hexdigest()
        )


def test_provider_asset_manifest_requires_the_fixed_slot_inventory() -> None:
    files = [
        {"relative_path": path, "sha256": DIGEST_0, "size_bytes": 1}
        for path in [
            "inspect_rollout.md",
            "looking.md",
            "mcp-lock.json",
            "runner-lock.json",
            "seed.schema.json",
            "skill_vocabulary.md",
            "system.md",
            "tools/mosaic.py",
            "validate_outputs.py",
        ]
    ]
    value = {
        "schema_version": "behavior.provider-assets.v1",
        "logical_name": "behavior_offline_judge",
        "files": files,
    }
    ProviderAssetManifestV1.model_validate(value)
    bad = deepcopy(value)
    bad["files"] = files[:-1]
    with pytest.raises(ValidationError, match="fixed slot file set"):
        ProviderAssetManifestV1.model_validate(bad)


def test_cli_validates_the_canonical_rollout_fixture_read_only(
    capsys: pytest.CaptureFixture[str],
) -> None:
    fixture = FIXTURES / "rollout_request.json"
    before = fixture.read_bytes()
    assert main(["validate", "--kind", "request", "--file", str(fixture)]) == 0
    assert capsys.readouterr().out == "valid request: behavior.stage-request.v1\n"
    assert fixture.read_bytes() == before


def test_canonical_reader_rejects_pretty_json_and_duplicate_keys(tmp_path: Path) -> None:
    pretty = tmp_path / "pretty.json"
    pretty.write_text('{\n  "a": 1\n}\n', encoding="utf-8")
    with pytest.raises(ValueError, match="not canonical"):
        load_canonical_document(pretty)

    duplicate = tmp_path / "duplicate.json"
    duplicate.write_bytes(b'{"a":1,"a":2}\n')
    with pytest.raises(ValueError, match="duplicate JSON key"):
        load_canonical_document(duplicate)


def test_stage_enum_is_exact() -> None:
    assert [stage.value for stage in BehaviorStage] == [
        "input_preflight",
        "rollout",
        "offline_judge",
        "failure_materialize",
        "frame_author",
        "recovery",
        "aggregate",
        "dataset_build",
    ]
