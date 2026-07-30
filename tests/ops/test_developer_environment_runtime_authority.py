from __future__ import annotations

import json
from typing import Any

import pytest
from scripts.ops import developer_environment_runtime_authority as runtime


def _binding() -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    environment = {
        "state_root": "/srv/loom/developer-environments/denv-test",
        "runtime_id": "dev-test",
        "compose_project": "loom-env-test",
        "candidate_root": "/shared_work/loom/candidates/environments/denv-test",
        "evidence_root": "/srv/loom/developer-environments/denv-test/evidence",
        "runtime_root": "/shared_work/loom/runtime/environments/denv-test",
        "ports": {"control_plane": 23003},
        "env_id": "denv-test",
        "state": "active",
        "resource_generation": 2,
        "service_user": "loom_dev_test",
    }
    candidate = {
        "candidate_id": "cand-" + "a" * 40,
        "candidate_tree": "b" * 40,
        "image_digests": {
            "amd64": "sha256:" + "d" * 64,
            "arm64": "sha256:" + "e" * 64,
        },
    }
    deployment = {
        "deployment_id": "dep-" + "9" * 32,
        "env_id": environment["env_id"],
        "candidate_id": candidate["candidate_id"],
        "phase": "committed",
        "applied_resource_generation": 2,
        "expected_resource_generation": 1,
        "worker_runtime_bindings": {
            "domains": {
                "oldlab": {"runtime_image_id": "sha256:" + "1" * 64},
                "gb10": {"runtime_image_id": "sha256:" + "2" * 64},
            }
        },
    }
    snapshot = {
        "generation": 9,
        "payload_sha256": "f" * 64,
        "deployments": [deployment],
    }
    return snapshot, environment, candidate, deployment


def test_profile_selects_runtime_ids_not_candidate_config_ids() -> None:
    profile = runtime._profile(*_binding())

    assert profile.worker_image_ids == {
        "oldlab": "sha256:" + "1" * 64,
        "gb10": "sha256:" + "2" * 64,
    }
    assert profile.worker_image_id("oldlab") != profile.worker_image_id("gb10")


@pytest.mark.parametrize("failure", ("missing", "same"))
def test_profile_fails_closed_without_two_distinct_config_ids(failure: str) -> None:
    snapshot, environment, candidate, deployment = _binding()
    if failure == "missing":
        deployment["worker_runtime_bindings"]["domains"].pop("gb10")
    else:
        deployment["worker_runtime_bindings"]["domains"]["gb10"]["runtime_image_id"] = deployment[
            "worker_runtime_bindings"
        ]["domains"]["oldlab"]["runtime_image_id"]

    with pytest.raises(runtime.RuntimeAuthorityError, match="worker image binding"):
        runtime._profile(snapshot, environment, candidate, deployment)


def test_rollback_receipt_reports_effective_candidate_worker_ids(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    snapshot, environment, requested, deployment = _binding()
    effective = {
        **requested,
        "candidate_id": "cand-" + "1" * 40,
        "candidate_sha": "2" * 40,
        "candidate_tree": "3" * 40,
        "image_digests": {
            "amd64": "sha256:" + "4" * 64,
            "arm64": "sha256:" + "5" * 64,
        },
    }
    effective_deployment = {
        "deployment_id": "dep-" + "4" * 32,
        "env_id": environment["env_id"],
        "candidate_id": effective["candidate_id"],
        "phase": "committed",
        "applied_resource_generation": 2,
        "worker_runtime_bindings": {
            "domains": {
                "oldlab": {"runtime_image_id": "sha256:" + "6" * 64},
                "gb10": {"runtime_image_id": "sha256:" + "7" * 64},
            }
        },
    }
    snapshot["deployments"].append(effective_deployment)
    request = {
        "deployment_id": "dep-" + "6" * 32,
        "env_id": environment["env_id"],
        "runtime_id": environment["runtime_id"],
        "candidate_id": requested["candidate_id"],
        "candidate_sha": "7" * 40,
        "candidate_tree": requested["candidate_tree"],
        "resource_generation": 2,
        "registry_generation": snapshot["generation"],
        "registry_snapshot_sha256": snapshot["payload_sha256"],
        "payload_sha256": "8" * 64,
    }
    captured: list[dict[str, Any]] = []
    monkeypatch.setattr(runtime.os, "geteuid", lambda: 0)
    monkeypatch.setattr(runtime, "_request", lambda _deployment_id, _action: request)
    monkeypatch.setattr(runtime, "_snapshot", lambda: snapshot)
    monkeypatch.setattr(
        runtime,
        "_binding",
        lambda _snapshot, _request: (environment, requested, deployment, effective),
    )
    reconciled: list[dict[str, Any]] = []
    monkeypatch.setattr(
        runtime,
        "_reconcile",
        lambda _snapshot, _environment, candidate, _deployment: reconciled.append(candidate),
    )
    monkeypatch.setattr(runtime, "_activate", lambda _environment: {"status": "ready"})
    monkeypatch.setattr(
        runtime,
        "_check",
        lambda *_args: {
            "status": "prepared",
            "runtime_id": environment["runtime_id"],
        },
    )
    monkeypatch.setattr(
        runtime,
        "_atomic_write",
        lambda _path, raw: captured.append(json.loads(raw)),
    )

    receipt = runtime.execute("rollback", request["deployment_id"])

    assert reconciled == [effective]
    assert receipt["worker_image_ids"] == {
        "oldlab": "sha256:" + "6" * 64,
        "gb10": "sha256:" + "7" * 64,
    }
    assert captured == [receipt]
