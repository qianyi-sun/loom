from __future__ import annotations

import copy
import json
import os
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest
from scripts.ops import developer_environment_registry as registry
from scripts.ops import developer_sandbox_live_acceptance as live_acceptance
from scripts.ops import developer_sandbox_platform_health_authority as authority

REPO_ROOT = Path(__file__).resolve().parents[2]
CONFIG = REPO_ROOT / "deploy/developer-sandboxes/platform-health-authority.toml"
SESSION = "1" * 32


def _register(principal: str, index: int) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "kind": registry.REGISTER_KIND,
        "principal_id": principal,
        "idempotency_key": f"registration-key-{index:04d}",
        "display_name": f"Developer {index}",
    }


def _candidate(
    environment: registry.EnvironmentRecord,
    index: int,
    *,
    sha: str | None = None,
) -> dict[str, Any]:
    digit = format(index + 1, "x")
    return {
        "schema_version": 1,
        "kind": registry.CANDIDATE_KIND,
        "principal_id": environment.principal_id,
        "idempotency_key": f"candidate-key-{index:04d}",
        "env_id": environment.env_id,
        "candidate_sha": sha or digit * 40,
        "candidate_tree": format(index + 5, "x") * 40,
        "bundle_sha256": format(index + 9, "x") * 64,
        "bundle_size": 1024 + index,
        "image_digests": {
            "amd64": "sha256:" + format(index + 1, "x") * 64,
            "arm64": "sha256:" + format(index + 5, "x") * 64,
        },
    }


def _active_source(tmp_path: Path, count: int = 4) -> dict[str, Any]:
    registry_root = tmp_path / "registry"
    registry_root.mkdir()
    store = registry.DeveloperEnvironmentRegistry(registry_root / "registry.sqlite3")
    for index in range(count):
        environment = store.register(
            _register(f"oidc:example:developer-{index}", index),
        )
        candidate = store.import_candidate(_candidate(environment, index))
        deployment = store.begin_deployment(
            {
                "schema_version": 1,
                "kind": registry.DEPLOY_KIND,
                "principal_id": environment.principal_id,
                "idempotency_key": f"deployment-key-{index:04d}",
                "env_id": environment.env_id,
                "candidate_id": candidate.candidate_id,
                "expected_resource_generation": 1,
            },
        )
        for expected, following in zip(
            registry.DEPLOY_PHASES[:-1],
            registry.DEPLOY_PHASES[1:],
            strict=True,
        ):
            if following == "committed":
                store.prepare_deployment_finalization(
                    deployment.deployment_id,
                    principal_id=environment.principal_id,
                    expected_resource_generation=1,
                )
                store.record_deployment_finalization(
                    deployment.deployment_id,
                    principal_id=environment.principal_id,
                    expected_resource_generation=1,
                    evidence={
                        "capacity_finalize_receipt_sha256": "a" * 64,
                        "capacity_finalize_check_receipt_sha256": "b" * 64,
                        "runtime_reconcile_receipt_sha256": "c" * 64,
                        "runtime_prepare_check_receipt_sha256": "d" * 64,
                        "acceptance_probe_receipt_sha256": "e" * 64,
                    },
                )
            deployment = store.advance_deployment(
                deployment.deployment_id,
                principal_id=environment.principal_id,
                expected_phase=expected,
                next_phase=following,
                expected_resource_generation=1,
            )
    return store.snapshot()


def _projection(tmp_path: Path, count: int = 4) -> dict[str, Any]:
    return live_acceptance._acceptance_registry_snapshot(
        _active_source(tmp_path, count=count),
    )


def _state(projection: dict[str, Any]) -> dict[str, Any]:
    candidates = {
        row["runtime_id"]: {
            "sha": row["candidate_sha"],
            "tree": row["candidate_tree"],
        }
        for row in projection["environments"]
    }
    return {
        "schema_version": 2,
        "session_id": SESSION,
        "registry_snapshot": projection,
        "candidates": candidates,
        "submit_host": "trt-eai-oldlab-2",
        "status": "running",
        "completed_phases": [],
    }


def _config(tmp_path: Path) -> authority.Config:
    checked = authority.load_config(CONFIG)
    return replace(
        checked,
        acceptance_state_root=tmp_path / "acceptance",
        authority_state_root=tmp_path / "health",
        registry_snapshot=tmp_path / "registry" / "current-snapshot.json",
    )


def test_config_is_full_twenty_node_inventory_including_node_seven() -> None:
    config = authority.load_config(CONFIG)
    assert len(config.nodes) == 20
    assert len(config.oldlab_nodes) == 5
    assert len(config.gb10_nodes) == 15
    assert "trt-gb10-7" in config.gb10_nodes
    assert authority.EXCLUDED_NODES == frozenset()
    assert config.registry_snapshot == registry.SYSTEM_SNAPSHOT


def test_config_rejects_missing_registry_authority(tmp_path: Path) -> None:
    raw = CONFIG.read_text(encoding="utf-8").replace(
        'registry_snapshot = "/var/lib/loom-developer-environment-registry/current-snapshot.json"\n',
        "",
    )
    path = tmp_path / "config.toml"
    path.write_text(raw, encoding="utf-8")
    with pytest.raises(authority.PlatformHealthError, match="invalid shape"):
        authority.load_config(path)


def test_dynamic_registry_cohort_supports_four_environments(tmp_path: Path) -> None:
    projection = _projection(tmp_path)
    environments = authority._registry_environments(projection)
    assert len(environments) == 4
    assert tuple(environments) == authority._sandboxes(projection)
    assert len({row["service_user"] for row in environments.values()}) == 4
    assert len({row["slurm_account"] for row in environments.values()}) == 4
    assert len({row["slurm_qos"] for row in environments.values()}) == 4
    assert len({row["ports"]["control_plane"] for row in environments.values()}) == 4


def test_registry_projection_tamper_fails_closed(tmp_path: Path) -> None:
    projection = _projection(tmp_path)
    projection["environments"][0]["slurm_account"] = "forged"
    unsigned = {key: value for key, value in projection.items() if key != "payload_sha256"}
    projection["payload_sha256"] = live_acceptance.hashlib.sha256(
        live_acceptance._canonical_digest_bytes(unsigned),
    ).hexdigest()
    with pytest.raises(authority.PlatformHealthError, match="registry snapshot"):
        authority._validated_registry_snapshot(projection)


def test_acceptance_state_exact_binds_current_registry_and_fourth_environment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    projection = _projection(tmp_path)
    state = _state(projection)
    config = _config(tmp_path)
    monkeypatch.setattr(
        authority,
        "_secure_json",
        lambda *_args, **_kwargs: (state, authority._canonical(state)),
    )
    monkeypatch.setattr(authority, "_current_registry_snapshot", lambda _config: projection)
    assert authority._acceptance_state(config, SESSION) == state


def test_acceptance_state_rejects_stale_registry_generation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    projection = _projection(tmp_path)
    state = _state(projection)
    current = copy.deepcopy(projection)
    current["generation"] += 1
    unsigned = {key: value for key, value in current.items() if key != "payload_sha256"}
    current["payload_sha256"] = live_acceptance.hashlib.sha256(
        live_acceptance._canonical_digest_bytes(unsigned),
    ).hexdigest()
    config = _config(tmp_path)
    monkeypatch.setattr(
        authority,
        "_secure_json",
        lambda *_args, **_kwargs: (state, authority._canonical(state)),
    )
    monkeypatch.setattr(authority, "_current_registry_snapshot", lambda _config: current)
    with pytest.raises(authority.PlatformHealthError, match="state binding"):
        authority._acceptance_state(config, SESSION)


def test_node_request_and_transport_envelope_are_dynamic_registry_bound(
    tmp_path: Path,
) -> None:
    state = _state(_projection(tmp_path))
    config = _config(tmp_path)
    request = authority._node_request(
        config,
        state=state,
        checkpoint="mixed_non_loom",
        node="trt-gb10-7",
        since_at="2026-07-29T00:00:00Z",
    )
    assert request["registry_snapshot"] == state["registry_snapshot"]
    assert set(request["candidates"]) == set(authority._sandboxes(state["registry_snapshot"]))
    envelope = json.loads(
        authority._request_envelope(request, node="trt-gb10-7"),
    )
    first = next(iter(state["candidates"]))
    assert envelope["sandbox"] == first
    assert envelope["candidate_sha"] == state["candidates"][first]["sha"]


def test_observe_node_rejects_registry_projection_tamper_before_commands(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = _state(_projection(tmp_path))
    config = _config(tmp_path)
    request = authority._node_request(
        config,
        state=state,
        checkpoint="baseline",
        node="trt-gb10-7",
        since_at="2026-07-29T00:00:00Z",
    )
    request["registry_snapshot"]["payload_sha256"] = "0" * 64
    monkeypatch.setattr(authority, "_require_root", lambda: None)
    with pytest.raises(authority.PlatformHealthError, match="registry snapshot"):
        authority.observe_node(authority._canonical(request))


def test_exact_active_job_matrix_scales_with_dynamic_cohort(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path)
    state = _state(_projection(tmp_path))
    candidates = state["candidates"]
    nodes = {node: {"active_jobs": []} for node in config.nodes}
    for index, sandbox in enumerate(candidates):
        oldlab = config.oldlab_nodes[index]
        gb10 = config.gb10_nodes[index]
        nodes[oldlab]["active_jobs"].append(
            {
                "sandbox": sandbox,
                "node": config.host_aliases[oldlab],
                "host": config.host_aliases[oldlab],
            },
        )
        nodes[gb10]["active_jobs"].append(
            {
                "sandbox": sandbox,
                "node": gb10,
                "host": config.host_aliases[gb10],
            },
        )
    monkeypatch.setattr(
        authority,
        "_load_capacity_policy",
        lambda _pool: {"values": {}},
    )
    monkeypatch.setattr(authority, "_verify_mixed_job_policy", lambda *_args, **_kwargs: None)
    jobs = authority._exact_active_jobs(config, nodes, candidates)
    assert len(jobs) == 8
    assert {
        (
            job["sandbox"],
            "oldlab" if "oldlab" in str(job["node"]) else "gb10",
        )
        for job in jobs
    } == {(sandbox, pool) for sandbox in candidates for pool in authority.POOLS}


def test_exact_active_job_matrix_rejects_missing_fourth_pair(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(tmp_path)
    state = _state(_projection(tmp_path))
    candidates = state["candidates"]
    nodes = {node: {"active_jobs": []} for node in config.nodes}
    monkeypatch.setattr(
        authority,
        "_load_capacity_policy",
        lambda _pool: {"values": {}},
    )
    monkeypatch.setattr(authority, "_verify_mixed_job_policy", lambda *_args, **_kwargs: None)
    with pytest.raises(authority.PlatformHealthError, match="exact active"):
        authority._exact_active_jobs(config, nodes, candidates)


def test_job_readback_uses_registry_slurm_account_qos_and_user() -> None:
    environment = {
        "slurm_user": "loom-env-user",
        "slurm_account": "loom-env-account",
        "slurm_qos": "loom-env-qos",
    }
    sha = "a" * 40
    output = (
        "JobId=42 JobName=loom-dynamic-a-"
        + sha[:12]
        + "-work UserId=loom-env-user(32000) Account=loom-env-account "
        "QOS=loom-env-qos JobState=RUNNING NodeList=trt-gb10-7 NumNodes=1 "
        "NumCPUs=4 Comment=loom-cgroup-v1:pids=512 Shared=OK "
        "AllocTRES=cpu=4,mem=16G,gres/gpu=1 MinMemoryNode=16G"
    )

    def run(*_args: object, **_kwargs: object) -> Any:
        return type("Result", (), {"returncode": 0, "stdout": output.encode(), "stderr": b""})()

    row = authority._job_readback(
        sandbox="dynamic-a",
        candidate_sha=sha,
        environment=environment,
        job_id="42",
        expected_node="trt-gb10-7",
        run=run,
    )
    assert row["account"] == "loom-env-account"
    assert row["qos"] == "loom-env-qos"
    assert row["user"] == "loom-env-user"


def test_job_readback_rejects_string_derived_legacy_qos() -> None:
    environment = {
        "slurm_user": "loom-env-user",
        "slurm_account": "loom-env-account",
        "slurm_qos": "loom-env-qos",
    }
    sha = "a" * 40
    output = (
        "JobId=42 JobName=loom-dynamic-a-"
        + sha[:12]
        + "-work UserId=loom-env-user(32000) Account=loom-env-account "
        "QOS=loom-dev JobState=RUNNING NodeList=trt-gb10-7 NumNodes=1 "
        "NumCPUs=4 Comment=loom-cgroup-v1:pids=512 Shared=OK "
        "AllocTRES=cpu=4,mem=16G,gres/gpu=1 MinMemoryNode=16G"
    )

    def run(*_args: object, **_kwargs: object) -> Any:
        return type("Result", (), {"returncode": 0, "stdout": output.encode(), "stderr": b""})()

    with pytest.raises(authority.PlatformHealthError, match="candidate identity"):
        authority._job_readback(
            sandbox="dynamic-a",
            candidate_sha=sha,
            environment=environment,
            job_id="42",
            expected_node="trt-gb10-7",
            run=run,
        )


def test_platform_health_sudoers_is_group_scoped_and_not_person_scoped() -> None:
    text = (
        REPO_ROOT
        / "deploy/developer-sandboxes/loom-developer-sandbox-platform-health-authority.sudoers"
    ).read_text(encoding="utf-8")
    assert text.count("%loom-developers ") == 9
    assert "qianyi ALL=" not in text


def test_platform_health_service_can_read_fixed_registry_authority() -> None:
    text = (
        REPO_ROOT
        / "deploy/developer-sandboxes/loom-developer-sandbox-platform-health-authority.service"
    ).read_text(encoding="utf-8")
    assert "/var/lib/loom-developer-environment-registry" in text


def test_source_has_no_fixed_three_developer_names_or_profile_loader() -> None:
    text = Path(authority.__file__).read_text(encoding="utf-8")
    for name in ("qianyi", "hongjian", "devansh"):
        assert name not in text
    assert "load_profiles" not in text
    assert "loom-dev-{sandbox}" not in text
    assert "loom-sandbox-{sandbox}" not in text


def test_platform_health_requires_registry_identity_labels() -> None:
    text = Path(authority.__file__).read_text(encoding="utf-8")
    for label in (
        "loom.env_id",
        "loom.resource_generation",
        "loom.candidate_id",
        "loom.candidate_tree",
        "loom.registry_generation",
        "loom.registry_payload_sha256",
    ):
        assert label in text


def test_registry_snapshot_secure_read_rejects_noncanonical_bytes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = _active_source(tmp_path)
    path = tmp_path / "snapshot.json"
    path.write_text(json.dumps(source, indent=2), encoding="utf-8")
    path.chmod(0o600)
    config = replace(_config(tmp_path), registry_snapshot=path)
    monkeypatch.setattr(authority, "ROOT_UID", os.getuid())
    monkeypatch.setattr(authority, "ROOT_GID", os.getgid())
    with pytest.raises(authority.PlatformHealthError, match="registry snapshot"):
        authority._current_registry_snapshot(config)
