from __future__ import annotations

import json
from dataclasses import replace

import pytest

from loom_cli.rollout.admin_smoke_contract import (
    AdminSmokeAuthority,
    AdminSmokeContract,
    decode_json_object,
)


def _authority() -> AdminSmokeAuthority:
    return AdminSmokeAuthority(
        represented_username="devansh",
        team_id="11111111-1111-4111-8111-111111111111",
        admin_actor="loom-staging-rollout",
        task_id="loom-smoke/gb10-oracle-hello-world",
        required_worker_pool="gb10-arm64",
        agent="oracle",
    )


def test_authority_round_trips_and_builds_exact_payload() -> None:
    authority = _authority()
    contract = AdminSmokeContract(authority)

    assert AdminSmokeAuthority.from_record(authority.to_record()) == authority
    assert contract.submission_payload(batch_name="rehearsal-abc123") == {
        "name": "rehearsal-abc123",
        "represented_username": "devansh",
        "team_id": "11111111-1111-4111-8111-111111111111",
        "task_filter": {"task_ids": ["loom-smoke/gb10-oracle-hello-world"]},
        "trial_config": {"agent_name": "oracle", "agent_model": None},
        "n_per_task": 1,
        "required_worker_pools": ["gb10-arm64"],
    }

    unpooled = AdminSmokeContract(replace(authority, required_worker_pool=None))
    assert "required_worker_pools" not in unpooled.submission_payload(
        batch_name="full-cluster-smoke"
    )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("team_id", "11111111-1111-1111-8111-111111111111"),
        ("task_id", "../unsafe/task"),
        ("required_worker_pool", "gb10 arm64"),
        ("agent", "oracle;sh"),
    ],
)
def test_authority_rejects_unsafe_inputs(field: str, value: str) -> None:
    with pytest.raises(ValueError, match="admin smoke"):
        replace(_authority(), **{field: value})


def test_contract_validates_identity_catalog_and_existing_batch() -> None:
    contract = AdminSmokeContract(_authority())
    assert (
        contract.validate_admin_identity({"credential_type": "admin_bearer_token", "scopes": []})
        is None
    )
    assert contract.validate_benchmark_catalog({"items": [{"id": "loom-smoke"}]}) is None
    assert (
        contract.existing_batch_id(
            {
                "items": [
                    {
                        "id": "batch-1",
                        "name": "rehearsal-abc123",
                        "team_id": _authority().team_id,
                        "submitted_by_user": {
                            "username": "Devansh",
                            "team_id": _authority().team_id,
                        },
                        "task_filter": {"task_ids": [_authority().task_id]},
                    }
                ]
            },
            batch_name="rehearsal-abc123",
        )
        == "batch-1"
    )


def test_contract_classifies_nonrecoverable_and_terminal_results() -> None:
    contract = AdminSmokeContract(_authority())
    failed = {
        "state": "finished",
        "result_status": "all_failed",
        "failure_reason": "fanout_submit_failed",
        "failure_message": "Bearer loom_admin_" + "s" * 32,
        "fanout_errors": [{"reason": "required_worker_pool_incompatible"}],
    }
    failure = contract.nonrecoverable_failure(failed)
    assert failure is not None
    assert "required_worker_pool_incompatible" in failure
    assert "loom_admin_" not in failure

    succeeded = {
        "state": "finished",
        "result_status": "succeeded",
        "expected_trial_count": 1,
        "trial_summary": {"succeeded": 1},
        "submitted_by_user": {
            "username": "DEVANSH",
            "team_id": _authority().team_id,
        },
    }
    assert contract.validate_terminal_batch(succeeded) is None


def test_decode_json_object_rejects_non_objects_and_invalid_json() -> None:
    assert decode_json_object(json.dumps({"status": "ok"}).encode()) == {"status": "ok"}
    assert decode_json_object(b"[]") is None
    assert decode_json_object(b"not-json") is None
