from __future__ import annotations

import json
from copy import deepcopy
from typing import Any
from uuid import UUID

import pytest
from pydantic import ValidationError

from loom.pipeline.keys import canonical_digest, canonical_document
from loom.pipeline.spec import ContainerNodeV1, PlatformFanoutIndexV1
from loom.pipeline.state import (
    RetryClass,
    StageResultInputV1,
    StageResultProvenanceV1,
    StageResultV1,
    validate_stage_result,
)

DIGEST_0 = "sha256:" + "0" * 64
DIGEST_1 = "sha256:" + "1" * 64
DIGEST_2 = "sha256:" + "2" * 64
IMAGE = "registry.example.com/loom/pipeline@sha256:" + "3" * 64
RUN_ID = UUID("00000000-0000-0000-0000-000000000001")
STAGE_ID = UUID("00000000-0000-0000-0000-000000000002")
ATTEMPT_ID = UUID("00000000-0000-0000-0000-000000000003")
ARTIFACT_ID = UUID("00000000-0000-0000-0000-000000000004")


def parse_result(value: dict[str, Any]) -> StageResultV1:
    """Parse the same JSON representation read from a StageResult document."""

    return StageResultV1.model_validate_json(json.dumps(value))


def node_value(*, fanout_commit: bool = False) -> dict[str, Any]:
    outputs: list[dict[str, Any]] = [
        {
            "name": "report",
            "artifact_type": "behavior.report.v1",
            "required": True,
            "role": "artifact",
            "producer": "container",
            "max_bytes": 1_000,
        },
        {
            "name": "debug",
            "artifact_type": "behavior.debug.v1",
            "required": False,
            "role": "artifact",
            "producer": "container",
            "max_bytes": 500,
        },
    ]
    commit = None
    if fanout_commit:
        outputs = [
            {
                "name": "fanout_index",
                "artifact_type": "loom.platform-fanout-index.v1",
                "required": True,
                "role": "artifact",
                "producer": "container",
                "max_bytes": 1_000,
            },
            {
                "name": "fanout_manifest",
                "artifact_type": "loom.fanout-manifest.v1",
                "required": True,
                "role": "fanout_manifest",
                "producer": "platform",
                "max_bytes": 2_000,
            },
            {
                "name": "case",
                "artifact_type": "behavior.case.v1",
                "required": False,
                "role": "artifact",
                "producer": "container",
                "max_bytes": 3_000,
            },
        ]
        commit = {
            "index_output_name": "fanout_index",
            "manifest_output_name": "fanout_manifest",
            "items_pointer": "/items",
            "item_binding_name": "case",
            "max_items": 10,
        }
    return {
        "node_kind": "container",
        "node_key": "judge",
        "image": IMAGE,
        "argv": ["judge"],
        "workdir": "/workspace",
        "resource_profile": "cpu@1",
        "network_profile": "none",
        "needs": [],
        "inputs": [],
        "outputs": outputs,
        "request_renderer": None,
        "checkpoint": None,
        "fanout": None,
        "fanout_commit": commit,
        "timeout_seconds": 60,
        "max_attempts": 2,
        "failure_policy": "fail_run",
    }


def expected_input() -> StageResultInputV1:
    return StageResultInputV1(
        binding_name="case",
        item_key="singleton",
        artifact_id=ARTIFACT_ID,
        artifact_type="behavior.case.v1",
        manifest_sha256=DIGEST_0,
    )


def expected_provenance() -> StageResultProvenanceV1:
    return StageResultProvenanceV1(
        pipeline_run_id=RUN_ID,
        stage_run_id=STAGE_ID,
        execution_attempt_id=ATTEMPT_ID,
        recipe_digest=DIGEST_0,
        execution_spec_digest=DIGEST_1,
        image_digest=DIGEST_2,
    )


def success_value() -> dict[str, Any]:
    return {
        "schema_version": "loom.stage-result.v1",
        "domain_outcome": "authored",
        "reason_code": "completed",
        "retry_class": "none",
        "inputs": [expected_input().model_dump(mode="json")],
        "outputs": [
            {"name": "debug", "artifact_type": "behavior.debug.v1"},
            {"name": "report", "artifact_type": "behavior.report.v1"},
        ],
        "metrics": {"latency_ms": 12.5, "tokens": 42},
        "provenance": expected_provenance().model_dump(mode="json"),
        "error": None,
    }


def failure_value() -> dict[str, Any]:
    value = success_value()
    value.update(
        {
            "domain_outcome": None,
            "reason_code": "provider_timeout",
            "retry_class": "provider_transient",
            "outputs": [],
            "error": {"code": "provider_timeout", "message": "provider timed out"},
        }
    )
    return value


def test_stage_result_round_trips_as_a_closed_strict_document() -> None:
    value = success_value()
    result = parse_result(value)

    assert result.model_dump(mode="json", exclude_none=False) == value
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        parse_result({**value, "unexpected": True})
    missing = deepcopy(value)
    del missing["provenance"]
    with pytest.raises(ValidationError, match="provenance"):
        parse_result(missing)


def test_stage_result_persisted_bytes_and_digest_are_stable() -> None:
    result = parse_result(success_value())

    assert canonical_document(result).endswith(b"\n")
    assert canonical_digest(result) == (
        "sha256:45e3a791d1596662b02d2554da7eea7bbbdaf723aecc612dd9ab4122313fce8a"
    )


def test_stage_result_normalizes_identifiers_but_preserves_free_form_error_message() -> None:
    value = success_value()
    value["domain_outcome"] = "e\u0301"
    value["inputs"][0]["item_key"] = "A\u030a"
    result = parse_result(value)
    assert result.domain_outcome == "\u00e9"
    assert result.inputs[0].item_key == "\u00c5"

    failed = failure_value()
    failed["error"]["message"] = "e\u0301"
    parsed_error = parse_result(failed).error
    assert parsed_error is not None
    assert parsed_error.message == "e\u0301"
    assert parsed_error.message != "\u00e9"


def test_stage_result_outputs_are_unique_and_bytewise_sorted() -> None:
    unsorted = success_value()
    unsorted["outputs"] = list(reversed(unsorted["outputs"]))
    with pytest.raises(ValidationError, match="unique and bytewise sorted"):
        parse_result(unsorted)

    duplicate = success_value()
    duplicate["outputs"].append(deepcopy(duplicate["outputs"][0]))
    with pytest.raises(ValidationError, match="unique and bytewise sorted"):
        parse_result(duplicate)


@pytest.mark.parametrize("metric", [float("nan"), float("inf"), float("-inf")])
def test_stage_result_rejects_non_finite_metrics(metric: float) -> None:
    value = success_value()
    value["metrics"] = {"score": metric}
    with pytest.raises(ValidationError, match="metric values must be finite"):
        parse_result(value)


def test_stage_result_rejects_identifier_metric_keys_and_too_many_metrics() -> None:
    uuid_key = success_value()
    uuid_key["metrics"] = {str(RUN_ID): 1}
    with pytest.raises(ValidationError, match="metric keys cannot be IDs"):
        parse_result(uuid_key)

    too_many = success_value()
    too_many["metrics"] = {f"metric_{index}": index for index in range(129)}
    with pytest.raises(ValidationError, match="metrics exceeds 128 keys"):
        parse_result(too_many)

    unsafe_integer = success_value()
    unsafe_integer["metrics"] = {"count": 2**53}
    with pytest.raises(ValidationError, match="interoperable JSON range"):
        parse_result(unsafe_integer)


def test_metric_keys_are_nfc_normalized_and_collisions_are_rejected() -> None:
    decomposed = success_value()
    decomposed["metrics"] = {"e\u0301": 1}
    assert parse_result(decomposed).metrics == {"\u00e9": 1}

    collision = success_value()
    collision["metrics"] = {"e\u0301": 1, "\u00e9": 2}
    with pytest.raises(ValidationError, match="collide after NFC normalization"):
        parse_result(collision)


def test_result_semantics_keep_domain_outcomes_separate_from_platform_failures() -> None:
    domain_with_error = success_value()
    domain_with_error["retry_class"] = "contract_error"
    domain_with_error["error"] = {"code": "bad_result", "message": "bad"}
    with pytest.raises(ValidationError, match="domain outcome requires retry_class=none"):
        parse_result(domain_with_error)

    unexplained = failure_value()
    unexplained["retry_class"] = "none"
    unexplained["error"] = None
    with pytest.raises(ValidationError, match="must explain its failure"):
        parse_result(unexplained)


def test_validate_stage_result_accepts_exact_success_claim_and_outputs() -> None:
    result = parse_result(success_value())
    node = ContainerNodeV1.model_validate(node_value())

    assert (
        validate_stage_result(
            result,
            exit_code=0,
            node=node,
            expected_inputs=[expected_input()],
            expected_provenance=expected_provenance(),
        )
        is result
    )

def test_validate_stage_result_requires_result_for_every_exit() -> None:
    node = ContainerNodeV1.model_validate(node_value())
    kwargs = {
        "node": node,
        "expected_inputs": [expected_input()],
        "expected_provenance": expected_provenance(),
    }
    with pytest.raises(ValueError, match="rc=0 requires StageResult"):
        validate_stage_result(None, exit_code=0, **kwargs)
    assert validate_stage_result(None, exit_code=1, **kwargs) is None


def test_validate_stage_result_binds_frozen_inputs_and_provenance() -> None:
    result = parse_result(success_value())
    node = ContainerNodeV1.model_validate(node_value())

    with pytest.raises(ValueError, match="inputs do not match"):
        validate_stage_result(
            result,
            exit_code=0,
            node=node,
            expected_inputs=[],
            expected_provenance=expected_provenance(),
        )
    other_provenance = expected_provenance().model_copy(
        update={"execution_attempt_id": UUID("00000000-0000-0000-0000-000000000099")}
    )
    with pytest.raises(ValueError, match="provenance does not match"):
        validate_stage_result(
            result,
            exit_code=0,
            node=node,
            expected_inputs=[expected_input()],
            expected_provenance=other_provenance,
        )


def test_validate_stage_result_enforces_exit_code_semantics() -> None:
    success = parse_result(success_value())
    failure = parse_result(failure_value())
    node = ContainerNodeV1.model_validate(node_value())
    kwargs = {
        "node": node,
        "expected_inputs": [expected_input()],
        "expected_provenance": expected_provenance(),
    }

    with pytest.raises(ValueError, match="rc=0 requires a valid domain result"):
        validate_stage_result(failure, exit_code=0, **kwargs)
    with pytest.raises(ValueError, match="nonzero exit cannot report a domain outcome"):
        validate_stage_result(success, exit_code=1, **kwargs)
    assert validate_stage_result(failure, exit_code=1, **kwargs) is failure


@pytest.mark.parametrize(
    ("outputs", "message"),
    [
        ([{"name": "debug", "artifact_type": "behavior.debug.v1"}], "missing required"),
        ([{"name": "report", "artifact_type": "behavior.other.v1"}], "undeclared output"),
        ([{"name": "other", "artifact_type": "behavior.report.v1"}], "undeclared output"),
    ],
)
def test_validate_stage_result_matches_declared_outputs(
    outputs: list[dict[str, str]], message: str
) -> None:
    value = success_value()
    value["outputs"] = outputs
    result = parse_result(value)

    with pytest.raises(ValueError, match=message):
        validate_stage_result(
            result,
            exit_code=0,
            node=ContainerNodeV1.model_validate(node_value()),
            expected_inputs=[expected_input()],
            expected_provenance=expected_provenance(),
        )


def test_fanout_stage_result_lists_index_and_dynamic_items_but_not_platform_manifest() -> None:
    node = ContainerNodeV1.model_validate(node_value(fanout_commit=True))
    value = success_value()
    value["outputs"] = [
        {"name": "fanout_index", "artifact_type": "loom.platform-fanout-index.v1"},
        {"name": "item_001", "artifact_type": "behavior.case.v1"},
        {"name": "item_002", "artifact_type": "behavior.case.v1"},
    ]
    result = parse_result(value)
    index = PlatformFanoutIndexV1.model_validate(
        {
            "items": [
                {"shard_key": "case-001", "output_name": "item_001"},
                {"shard_key": "case-002", "output_name": "item_002"},
            ],
            "schema_version": "loom.platform-fanout-index.v1",
        }
    )
    assert (
        validate_stage_result(
            result,
            exit_code=0,
            node=node,
            expected_inputs=[expected_input()],
            expected_provenance=expected_provenance(),
            fanout_index=index,
        )
        is result
    )

    unexpected = success_value()
    unexpected["outputs"] = [
        {"name": "evil_name", "artifact_type": "behavior.case.v1"},
        {"name": "fanout_index", "artifact_type": "loom.platform-fanout-index.v1"},
    ]
    with pytest.raises(ValueError, match="do not match the fanout index"):
        validate_stage_result(
            parse_result(unexpected),
            exit_code=0,
            node=node,
            expected_inputs=[expected_input()],
            expected_provenance=expected_provenance(),
            fanout_index=index,
        )

    platform = success_value()
    platform["outputs"] = [
        {"name": "fanout_index", "artifact_type": "loom.platform-fanout-index.v1"},
        {"name": "fanout_manifest", "artifact_type": "loom.fanout-manifest.v1"},
    ]
    with pytest.raises(ValueError):
        validate_stage_result(
            parse_result(platform),
            exit_code=0,
            node=node,
            expected_inputs=[expected_input()],
            expected_provenance=expected_provenance(),
        )


def test_retry_class_is_closed() -> None:
    assert {item.value for item in RetryClass} == {
        "none",
        "contract_error",
        "provider_transient",
        "infrastructure_transient",
        "internal_defect",
        "cancelled",
    }
