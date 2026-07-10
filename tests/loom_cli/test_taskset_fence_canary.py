from __future__ import annotations

import argparse
import json
import subprocess
from collections.abc import Callable
from pathlib import Path

import pytest


def _contract(**overrides: object) -> dict[str, object]:
    return {
        "candidate_sha": "a" * 40,
        "image_tag": "staging-aaaaaaa",
        "task_set_id": "ts/9ec52425-99d5-421c-a90b-9e21e215f424/fence-canary",
        "expected_task_checksum": "b" * 64,
        "authorization_token": "deployment-only-secret",
        "nonce": "n" * 43,
        **overrides,
    }


def _runner_evidence(
    *,
    candidate_sha: str = "a" * 40,
    image_tag: str = "staging-aaaaaaa",
    task_set_id: str = "ts/9ec52425-99d5-421c-a90b-9e21e215f424/fence-canary",
    expected_task_checksum: str = "b" * 64,
) -> dict[str, object]:
    return {
        "schema_version": 1,
        "candidate_sha": candidate_sha,
        "image_tag": image_tag,
        "task_set_id": task_set_id,
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
        "published_task": {"task_count": 1, "checksum": expected_task_checksum},
        "stale_cas_outcome": "LeaseLost",
        "timestamps": {
            "a_staged_at": "2030-01-01T00:00:00Z",
            "b_published_at": "2030-01-01T00:00:01Z",
            "a_lease_lost_at": "2030-01-01T00:00:02Z",
        },
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
                "nonce": "n" * 43,
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


def _release_manifest(
    *,
    sha: str = "a" * 40,
    image_tag: str = "staging-aaaaaaa",
    digest: str = "sha256:" + "1" * 64,
) -> dict[str, object]:
    return {
        "release": {
            "environment": "staging",
            "git_sha": sha,
            "image_tag": image_tag,
        },
        "cluster_config": {"namespace": "loom-staging"},
        "rendered_manifest": {
            "deployment_image_identities": {
                "loom-service": {
                    "loom-service": {
                        "image": f"loom-service:{image_tag}",
                        "repo_digest": f"registry.example/loom-service@{digest}",
                        "image_id": digest,
                    },
                },
            },
        },
    }


def _live_deployment(
    *,
    image_tag: str = "staging-aaaaaaa",
    generation: int = 7,
    observed_generation: int = 7,
    ready_replicas: int = 1,
    updated_replicas: int = 1,
) -> dict[str, object]:
    return {
        "metadata": {"name": "loom-service", "generation": generation},
        "spec": {
            "replicas": 1,
            "selector": {"matchLabels": {"app": "loom-service"}},
            "template": {
                "spec": {
                    "containers": [
                        {"name": "loom-service", "image": f"loom-service:{image_tag}"},
                    ],
                },
            },
        },
        "status": {
            "observedGeneration": observed_generation,
            "readyReplicas": ready_replicas,
            "updatedReplicas": updated_replicas,
            "replicas": 1,
        },
    }


def _live_pod(
    *,
    name: str = "loom-service-abc",
    image_tag: str = "staging-aaaaaaa",
    digest: str = "sha256:" + "1" * 64,
    ready: bool = True,
) -> dict[str, object]:
    return {
        "metadata": {
            "name": name,
            "uid": f"{name}-uid",
            "labels": {"app": "loom-service"},
        },
        "spec": {
            "containers": [
                {"name": "loom-service", "image": f"loom-service:{image_tag}"},
            ],
        },
        "status": {
            "phase": "Running",
            "conditions": [{"type": "Ready", "status": "True" if ready else "False"}],
            "containerStatuses": [
                {
                    "name": "loom-service",
                    "image": f"loom-service:{image_tag}",
                    "imageID": f"docker-pullable://registry.example/loom-service@{digest}",
                },
            ],
        },
    }


def _write_candidate_rollout(rollout_dir: Path) -> None:
    rollout_dir.mkdir(parents=True, exist_ok=True)
    (rollout_dir / "inputs.json").write_text(json.dumps(_rollout_inputs()))
    (rollout_dir / "state.json").write_text(json.dumps({"status": "done"}))
    manifest_path = rollout_dir / "14-release-gate/release-manifest-staging-aaaaaaa.json"
    manifest_path.parent.mkdir(parents=True)
    manifest_path.write_text(json.dumps(_release_manifest()))


def _live_runner(
    *,
    evidence: dict[str, object] | None = None,
    on_internal: Callable[[list[str], str], subprocess.CompletedProcess[str]] | None = None,
) -> Callable[[list[str], str], subprocess.CompletedProcess[str]]:
    def runner(command: list[str], payload: str) -> subprocess.CompletedProcess[str]:
        if command[-5:] == ["get", "deployment", "loom-service", "-o", "json"]:
            return subprocess.CompletedProcess(command, 0, json.dumps(_live_deployment()), "")
        if "pods" in command:
            return subprocess.CompletedProcess(
                command,
                0,
                json.dumps({"items": [_live_pod()]}),
                "",
            )
        if "--authorize" in command:
            return subprocess.CompletedProcess(command, 0, '{"status":"authorized"}', "")
        if on_internal is not None:
            return on_internal(command, payload)
        return subprocess.CompletedProcess(command, 0, json.dumps(evidence), "")

    return runner


@pytest.mark.parametrize(
    ("manifest", "deployment", "pods"),
    [
        (
            _release_manifest(sha="c" * 40),
            _live_deployment(),
            {"items": [_live_pod()]},
        ),
        (
            _release_manifest(),
            _live_deployment(observed_generation=6),
            {"items": [_live_pod()]},
        ),
        (
            _release_manifest(),
            _live_deployment(),
            {"items": [_live_pod(ready=False)]},
        ),
        (
            _release_manifest(),
            _live_deployment(),
            {"items": [_live_pod(), _live_pod(name="loom-service-old", image_tag="staging-old")]},
        ),
        (
            _release_manifest(),
            _live_deployment(),
            {"items": [_live_pod(digest="sha256:" + "9" * 64)]},
        ),
    ],
)
def test_live_target_parser_rejects_stale_or_noncandidate_deployments(
    manifest: dict[str, object],
    deployment: dict[str, object],
    pods: dict[str, object],
) -> None:
    from loom_cli.cluster_taskset_fence_canary import (
        TaskSetFenceCanaryDeploymentError,
        _select_live_target,
    )

    with pytest.raises(TaskSetFenceCanaryDeploymentError) as exc_info:
        _select_live_target(
            manifest,
            candidate_sha="a" * 40,
            image_tag="staging-aaaaaaa",
            deployment=deployment,
            pods=pods,
        )

    assert str(exc_info.value) == "live staging target was rejected"


def test_deployment_runner_uses_rollout_owned_candidate_and_evidence_path(
    tmp_path: Path,
) -> None:
    from loom_cli.cluster_taskset_fence_canary import run_staging_fence_canary

    rollout_root = tmp_path / "loom-staging"
    rollout_dir = rollout_root / "rollouts" / "candidate"
    _write_candidate_rollout(rollout_dir)
    observed: list[tuple[list[str], str]] = []

    def capture_internal(command: list[str], payload: str) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(command, 0, json.dumps(_runner_evidence()), "")

    base_runner = _live_runner(on_internal=capture_internal)

    def runner(command: list[str], payload: str) -> subprocess.CompletedProcess[str]:
        observed.append((command, payload))
        return base_runner(command, payload)

    evidence_path = run_staging_fence_canary(
        rollout_dir=rollout_dir,
        task_set_id="ts/9ec52425-99d5-421c-a90b-9e21e215f424/fence-canary",
        expected_task_checksum="b" * 64,
        authorization_token="deployment-only-secret",
        rollout_root=rollout_root,
        runner=runner,
        nonce_factory=lambda: "n" * 43,
    )

    assert evidence_path == rollout_dir / "canaries/taskset-lease-fencing/evidence.json"
    published = json.loads(evidence_path.read_text())
    assert published["candidate_sha"] == "a" * 40
    assert published["schema_version"] == 2
    assert published["live_target"] == {
        "deployment_generation": 7,
        "service_image_digest": "sha256:" + "1" * 64,
    }
    internal_commands = [
        (command, json.loads(payload))
        for command, payload in observed
        if "exec" in command
    ]
    assert [command for command, _payload in internal_commands] == [
        [
            "kubectl", "--context", "kind-loom-staging", "-n", "loom-staging",
            "exec", "pod/loom-service-abc", "-c", "loom-service", "-i", "--",
            "python3", "-m", "loom_cli.taskset_fence_canary", "--internal", "--authorize",
        ],
        [
            "kubectl", "--context", "kind-loom-staging", "-n", "loom-staging",
            "exec", "pod/loom-service-abc", "-c", "loom-service", "-i", "--",
            "python3", "-m", "loom_cli.taskset_fence_canary", "--internal",
        ],
    ]
    payload = internal_commands[0][1]
    assert payload == {
        "candidate_sha": "a" * 40,
        "image_tag": "staging-aaaaaaa",
        "task_set_id": "ts/9ec52425-99d5-421c-a90b-9e21e215f424/fence-canary",
        "expected_task_checksum": "b" * 64,
        "authorization_token": "deployment-only-secret",
        "nonce": "n" * 43,
    }
    assert "owner" not in payload
    assert "prefix" not in payload
    assert internal_commands[1][1] == payload


@pytest.mark.parametrize(
    ("inputs", "state"),
    [
        ({**_rollout_inputs(), "environment": "production"}, {"status": "done"}),
        ({**_rollout_inputs(), "image_tag": "staging-deadbee"}, {"status": "done"}),
        (_rollout_inputs(), {"status": "running"}),
    ],
)
def test_deployment_runner_rejects_noncanonical_or_production_rollouts(
    tmp_path: Path,
    inputs: dict[str, object],
    state: dict[str, object],
) -> None:
    from loom_cli.cluster_taskset_fence_canary import (
        TaskSetFenceCanaryDeploymentError,
        run_staging_fence_canary,
    )

    rollout_root = tmp_path / "loom-staging"
    rollout_dir = rollout_root / "rollouts" / "candidate"
    rollout_dir.mkdir(parents=True)
    (rollout_dir / "inputs.json").write_text(json.dumps(inputs))
    (rollout_dir / "state.json").write_text(json.dumps(state))

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


@pytest.mark.parametrize(
    "timestamps",
    [
        {
            "a_staged_at": "not-a-rfc3339-timeZ",
            "b_published_at": "2030-01-01T00:00:01Z",
            "a_lease_lost_at": "2030-01-01T00:00:02Z",
        },
        {
            "a_staged_at": "2030-01-01T00:00:02Z",
            "b_published_at": "2030-01-01T00:00:01Z",
            "a_lease_lost_at": "2030-01-01T00:00:00Z",
        },
    ],
)
def test_deployment_runner_rejects_malformed_or_inverted_evidence_timestamps(
    timestamps: dict[str, str],
) -> None:
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
            "job_id": "9ec52425-99d5-421c-a90b-9e21e215f424",
            "lease_epoch": 1,
            "owner_fingerprint": "sha256:abcdef012345",
            "outcome": "fenced_before_publish",
            "gc_eligible": True,
        },
        "published_task": {"task_count": 1, "checksum": contract.expected_task_checksum},
        "stale_cas_outcome": "LeaseLost",
        "timestamps": timestamps,
    }

    with pytest.raises(TaskSetFenceCanaryDeploymentError) as exc_info:
        _validate_evidence(evidence, contract=contract)

    assert str(exc_info.value) == "internal canary evidence was rejected"


@pytest.mark.parametrize(
    "failure",
    [
        subprocess.TimeoutExpired(
            ["kubectl", "exec"],
            900,
            output="private runner stdout",
            stderr="private runner stderr",
        ),
        OSError("private command failure"),
    ],
)
def test_deployment_runner_translates_kubectl_execution_exceptions(
    tmp_path: Path,
    failure: BaseException,
) -> None:
    from loom_cli.cluster_taskset_fence_canary import (
        TaskSetFenceCanaryDeploymentError,
        run_staging_fence_canary,
    )

    rollout_root = tmp_path / "loom-staging"
    rollout_dir = rollout_root / "rollouts" / "candidate"
    _write_candidate_rollout(rollout_dir)

    def fail_internal(_command: list[str], _payload: str) -> subprocess.CompletedProcess[str]:
        raise failure

    with pytest.raises(TaskSetFenceCanaryDeploymentError) as exc_info:
        run_staging_fence_canary(
            rollout_dir=rollout_dir,
            task_set_id="ts/9ec52425-99d5-421c-a90b-9e21e215f424/fence-canary",
            expected_task_checksum="b" * 64,
            authorization_token="deployment-only-secret",
            rollout_root=rollout_root,
            runner=_live_runner(on_internal=fail_internal),
        )

    assert str(exc_info.value) == "internal canary runner failed"
    assert "private" not in str(exc_info.value)


def test_deployment_runner_rejects_existing_evidence_before_remote_execution(
    tmp_path: Path,
) -> None:
    from loom_cli.cluster_taskset_fence_canary import (
        TaskSetFenceCanaryDeploymentError,
        run_staging_fence_canary,
    )

    rollout_root = tmp_path / "loom-staging"
    rollout_dir = rollout_root / "rollouts" / "candidate"
    _write_candidate_rollout(rollout_dir)
    evidence_path = rollout_dir / "canaries/taskset-lease-fencing/evidence.json"
    evidence_path.parent.mkdir(parents=True)
    evidence_path.write_text(json.dumps(_runner_evidence()))

    with pytest.raises(TaskSetFenceCanaryDeploymentError) as exc_info:
        run_staging_fence_canary(
            rollout_dir=rollout_dir,
            task_set_id="ts/9ec52425-99d5-421c-a90b-9e21e215f424/fence-canary",
            expected_task_checksum="b" * 64,
            authorization_token="deployment-only-secret",
            rollout_root=rollout_root,
            runner=lambda *_args: pytest.fail("existing evidence must stop before exec"),
        )

    assert str(exc_info.value) == "canary evidence already exists"


def test_deployment_runner_discards_interrupted_private_evidence_file(
    tmp_path: Path,
) -> None:
    from loom_cli.cluster_taskset_fence_canary import run_staging_fence_canary

    rollout_root = tmp_path / "loom-staging"
    rollout_dir = rollout_root / "rollouts" / "candidate"
    _write_candidate_rollout(rollout_dir)
    interrupted = rollout_dir / "canaries/taskset-lease-fencing/.evidence.interrupted.tmp"
    interrupted.parent.mkdir(parents=True)
    interrupted.write_text("{partial")

    evidence_path = run_staging_fence_canary(
        rollout_dir=rollout_dir,
        task_set_id="ts/9ec52425-99d5-421c-a90b-9e21e215f424/fence-canary",
        expected_task_checksum="b" * 64,
        authorization_token="deployment-only-secret",
        rollout_root=rollout_root,
        runner=_live_runner(evidence=_runner_evidence()),
    )

    assert json.loads(evidence_path.read_text())["candidate_sha"] == "a" * 40
    assert not interrupted.exists()


def test_deployment_runner_rejects_rollout_path_replacement_after_validation(
    tmp_path: Path,
) -> None:
    from loom_cli.cluster_taskset_fence_canary import (
        TaskSetFenceCanaryDeploymentError,
        run_staging_fence_canary,
    )

    rollout_root = tmp_path / "loom-staging"
    rollout_dir = rollout_root / "rollouts" / "candidate"
    _write_candidate_rollout(rollout_dir)
    replacement = tmp_path / "replacement"
    replacement.mkdir()

    def replace_after_execution(
        command: list[str],
        _payload: str,
    ) -> subprocess.CompletedProcess[str]:
        original = rollout_dir.with_name("candidate-original")
        rollout_dir.rename(original)
        rollout_dir.symlink_to(replacement, target_is_directory=True)
        return subprocess.CompletedProcess(
            command,
            0,
            stdout=json.dumps(_runner_evidence()),
            stderr="",
        )

    with pytest.raises(TaskSetFenceCanaryDeploymentError) as exc_info:
        run_staging_fence_canary(
            rollout_dir=rollout_dir,
            task_set_id="ts/9ec52425-99d5-421c-a90b-9e21e215f424/fence-canary",
            expected_task_checksum="b" * 64,
            authorization_token="deployment-only-secret",
            rollout_root=rollout_root,
            runner=_live_runner(on_internal=replace_after_execution),
        )

    assert str(exc_info.value) == "rollout evidence path changed"
    assert not (replacement / "canaries/taskset-lease-fencing/evidence.json").exists()
