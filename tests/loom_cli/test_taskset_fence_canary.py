from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path

import pytest


def _contract(**overrides: object) -> dict[str, object]:
    return {
        "candidate_sha": "a" * 40,
        "image_tag": "staging-aaaaaaa",
        "task_set_id": "ts/9ec52425-99d5-421c-a90b-9e21e215f424/fence-canary",
        "expected_task_checksum": "b" * 64,
        "authorization_token": "deployment-only-secret",
        **overrides,
    }


def test_contract_rejects_invalid_candidate_sha_without_echoing_input() -> None:
    from loom_cli.taskset_fence_canary import (
        TaskSetFenceCanaryContract,
        TaskSetFenceCanaryContractError,
    )

    with pytest.raises(TaskSetFenceCanaryContractError) as exc_info:
        TaskSetFenceCanaryContract.from_mapping(
            {
                "candidate_sha": "not-a-candidate-sha",
                "image_tag": "staging-deadbee",
                "task_set_id": "ts/9ec52425-99d5-421c-a90b-9e21e215f424/fence-canary",
                "expected_task_checksum": "a" * 64,
                "authorization_token": "not-for-evidence",
            },
        )

    assert str(exc_info.value) == "invalid candidate identity"
    assert "not-a-candidate-sha" not in str(exc_info.value)


@pytest.mark.parametrize(
    ("payload", "message", "secret_value"),
    [
        (_contract(image_tag="staging-deadbee"), "invalid candidate identity", None),
        (_contract(task_set_id="not-a-task-set-id"), "invalid disposable task set", None),
        (_contract(expected_task_checksum="not-a-checksum"), "invalid expected task checksum", None),
        (_contract(authorization_token=""), "missing canary authorization", None),
        (
            _contract(owner="operator-owned-value"),
            "invalid canary contract fields",
            "operator-owned-value",
        ),
        (
            _contract(storage_prefix="s3://private/unsafe"),
            "invalid canary contract fields",
            "s3://private/unsafe",
        ),
    ],
)
def test_contract_rejects_unapproved_or_malformed_values(
    payload: dict[str, object],
    message: str,
    secret_value: str | None,
) -> None:
    from loom_cli.taskset_fence_canary import (
        TaskSetFenceCanaryContract,
        TaskSetFenceCanaryContractError,
    )

    with pytest.raises(TaskSetFenceCanaryContractError) as exc_info:
        TaskSetFenceCanaryContract.from_mapping(payload)

    assert str(exc_info.value) == message
    if secret_value is not None:
        assert secret_value not in str(exc_info.value)


def test_contract_requires_the_deployed_canary_capability() -> None:
    from loom_cli.taskset_fence_canary import (
        TaskSetFenceCanaryAuthorizationError,
        TaskSetFenceCanaryContract,
        validate_contract_authorization,
    )

    contract = TaskSetFenceCanaryContract.from_mapping(_contract())
    validate_contract_authorization(
        contract,
        configured_token="deployment-only-secret",
    )

    with pytest.raises(TaskSetFenceCanaryAuthorizationError) as exc_info:
        validate_contract_authorization(contract, configured_token="different-secret")

    assert str(exc_info.value) == "canary authorization rejected"
    assert "different-secret" not in str(exc_info.value)


def _rollout_inputs(*, sha: str = "a" * 40, image_tag: str = "staging-aaaaaaa") -> dict[str, object]:
    return {
        "environment": "staging",
        "namespace": "loom-staging",
        "rollout_root": "/data/loom-staging",
        "resolved_sha": sha,
        "image_tag": image_tag,
    }


def test_deployment_runner_uses_rollout_owned_candidate_and_evidence_path(
    tmp_path: Path,
) -> None:
    from loom_cli.cluster_taskset_fence_canary import run_staging_fence_canary

    rollout_root = tmp_path / "loom-staging"
    rollout_dir = rollout_root / "rollouts" / "candidate"
    rollout_dir.mkdir(parents=True)
    (rollout_dir / "inputs.json").write_text(json.dumps(_rollout_inputs()))
    (rollout_dir / "state.json").write_text(json.dumps({"status": "done"}))
    observed: dict[str, object] = {}

    def runner(command: list[str], payload: str) -> subprocess.CompletedProcess[str]:
        observed["command"] = command
        observed["payload"] = json.loads(payload)
        return subprocess.CompletedProcess(
            command,
            0,
            stdout=json.dumps({
                "schema_version": 1,
                "candidate_sha": "a" * 40,
                "image_tag": "staging-aaaaaaa",
                "task_set_id": "ts/9ec52425-99d5-421c-a90b-9e21e215f424/fence-canary",
                "winner": {
                    "job_id": "9ec52425-99d5-421c-a90b-9e21e215f424",
                    "lease_epoch": 3,
                    "owner_fingerprint": "sha256:0123456789ab",
                    "published_generation": 3,
                    "outcome": "published",
                },
                "loser": {
                    "job_id": "9ec52425-99d5-421c-a90b-9e21e215f424",
                    "lease_epoch": 1,
                    "owner_fingerprint": "sha256:abcdef012345",
                    "outcome": "fenced_before_publish",
                    "gc_eligible": True,
                },
                "published_task": {"task_count": 1, "checksum": "b" * 64},
                "stale_cas_outcome": "LeaseLost",
                "timestamps": {
                    "a_staged_at": "2030-01-01T00:00:00Z",
                    "b_published_at": "2030-01-01T00:00:01Z",
                    "a_lease_lost_at": "2030-01-01T00:00:02Z",
                },
            }),
            stderr="",
        )

    evidence_path = run_staging_fence_canary(
        rollout_dir=rollout_dir,
        task_set_id="ts/9ec52425-99d5-421c-a90b-9e21e215f424/fence-canary",
        expected_task_checksum="b" * 64,
        authorization_token="deployment-only-secret",
        rollout_root=rollout_root,
        runner=runner,
    )

    assert evidence_path == rollout_dir / "canaries/taskset-lease-fencing/evidence.json"
    assert json.loads(evidence_path.read_text())["candidate_sha"] == "a" * 40
    assert observed["command"] == [
        "kubectl",
        "-n",
        "loom-staging",
        "exec",
        "deploy/loom-service",
        "-c",
        "loom-service",
        "-i",
        "--",
        "python3",
        "-m",
        "loom_cli.taskset_fence_canary",
        "--internal",
    ]
    payload = observed["payload"]
    assert payload == {
        "candidate_sha": "a" * 40,
        "image_tag": "staging-aaaaaaa",
        "task_set_id": "ts/9ec52425-99d5-421c-a90b-9e21e215f424/fence-canary",
        "expected_task_checksum": "b" * 64,
        "authorization_token": "deployment-only-secret",
    }
    assert "owner" not in payload
    assert "prefix" not in payload


@pytest.mark.parametrize(
    "inputs",
    [
        {**_rollout_inputs(), "environment": "production"},
        {**_rollout_inputs(), "image_tag": "staging-deadbee"},
    ],
)
def test_deployment_runner_rejects_noncanonical_or_production_rollouts(
    tmp_path: Path,
    inputs: dict[str, object],
) -> None:
    from loom_cli.cluster_taskset_fence_canary import (
        TaskSetFenceCanaryDeploymentError,
        run_staging_fence_canary,
    )

    rollout_root = tmp_path / "loom-staging"
    rollout_dir = rollout_root / "rollouts" / "candidate"
    rollout_dir.mkdir(parents=True)
    (rollout_dir / "inputs.json").write_text(json.dumps(inputs))
    (rollout_dir / "state.json").write_text(json.dumps({"status": "done"}))

    with pytest.raises(TaskSetFenceCanaryDeploymentError) as exc_info:
        run_staging_fence_canary(
            rollout_dir=rollout_dir,
            task_set_id="ts/9ec52425-99d5-421c-a90b-9e21e215f424/fence-canary",
            expected_task_checksum="b" * 64,
            authorization_token="deployment-only-secret",
            rollout_root=rollout_root,
            runner=lambda *_args: pytest.fail("runner must not be called"),
        )

    assert str(exc_info.value) == "rollout is not an eligible staging candidate"


def test_internal_runner_has_no_public_mode_and_rejects_production(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from loom_cli import taskset_fence_canary

    assert taskset_fence_canary.main([]) == 2
    assert capsys.readouterr().out == ""

    monkeypatch.setenv("LOOM_ENV", "production")
    monkeypatch.setattr("sys.stdin.read", lambda: json.dumps(_contract()))
    assert taskset_fence_canary.main(["--internal"]) == 2
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "production" not in captured.err.lower()


def test_cluster_surface_exposes_only_the_disposable_contract_fields() -> None:
    from loom_cli.cluster_taskset_fence_canary import add_subparser

    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    add_subparser(sub)
    args = parser.parse_args([
        "taskset-fence-canary",
        "--rollout-dir", "/data/loom-staging/rollouts/candidate",
        "--task-set-id", "ts/9ec52425-99d5-421c-a90b-9e21e215f424/fence-canary",
        "--expected-task-checksum", "b" * 64,
    ])

    assert vars(args).keys() == {
        "command",
        "rollout_dir",
        "task_set_id",
        "expected_task_checksum",
        "handler",
    }


def test_deployment_runner_rejects_evidence_with_different_winner_and_loser_jobs() -> None:
    from loom_cli.cluster_taskset_fence_canary import (
        TaskSetFenceCanaryDeploymentError,
        _validate_evidence,
    )
    from loom_cli.taskset_fence_canary import TaskSetFenceCanaryContract

    contract = TaskSetFenceCanaryContract.from_mapping(_contract())
    evidence = {
        "schema_version": 1,
        "candidate_sha": contract.candidate_sha,
        "image_tag": contract.image_tag,
        "task_set_id": contract.task_set_id,
        "winner": {
            "job_id": "9ec52425-99d5-421c-a90b-9e21e215f424",
            "lease_epoch": 3,
            "owner_fingerprint": "sha256:0123456789ab",
            "published_generation": 3,
            "outcome": "published",
        },
        "loser": {
            "job_id": "1ec52425-99d5-421c-a90b-9e21e215f424",
            "lease_epoch": 1,
            "owner_fingerprint": "sha256:abcdef012345",
            "outcome": "fenced_before_publish",
            "gc_eligible": True,
        },
        "published_task": {"task_count": 1, "checksum": contract.expected_task_checksum},
        "stale_cas_outcome": "LeaseLost",
        "timestamps": {
            "a_staged_at": "2030-01-01T00:00:00Z",
            "b_published_at": "2030-01-01T00:00:01Z",
            "a_lease_lost_at": "2030-01-01T00:00:02Z",
        },
    }

    with pytest.raises(TaskSetFenceCanaryDeploymentError) as exc_info:
        _validate_evidence(evidence, contract=contract)

    assert str(exc_info.value) == "internal canary evidence was rejected"
