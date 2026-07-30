from __future__ import annotations

import copy
import hashlib
import json
import os
import re
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


def _slice_identity() -> dict[str, Any]:
    return {
        "cluster": "trt-gb10",
        "node": "trt-gb10-7",
        "job_id": "42",
        "job_start_time": "2026-07-30T00:00:00",
        "account": "loom-env-account",
        "env_id": "denv-dynamic-a",
        "resource_generation": 2,
        "runtime_id": "dynamic-a",
        "candidate_id": "cand-" + "b" * 40,
        "candidate_sha": "a" * 40,
        "candidate_tree": "c" * 40,
    }


def _slice_limits() -> dict[str, str]:
    return {
        "cpu_max": "400000 100000",
        "memory_max": str(16 * 1024**3),
        "memory_swap_max_source": "max",
        "pids_max": "512",
        "cpuset_cpus": "0-3",
        "cpuset_mems": "0",
    }


def _slice_receipt(
    *,
    identity: dict[str, Any] | None = None,
    limits: dict[str, str] | None = None,
    unit_bytes: bytes = b"[Slice]\nCPUQuota=400%\n",
) -> dict[str, Any]:
    bound_identity = identity or _slice_identity()
    bound_limits = limits or _slice_limits()
    unit, identity_sha256 = authority._systemd_slice_identity(bound_identity)
    unsigned = {
        "schema_version": 1,
        "kind": "loom.slurm-systemd-slice-receipt",
        "systemd_slice": unit,
        "slice_identity_sha256": identity_sha256,
        "unit_sha256": hashlib.sha256(unit_bytes).hexdigest(),
        "job_id": bound_identity["job_id"],
        "job_start_time": bound_identity["job_start_time"],
        "cluster": bound_identity["cluster"],
        "node_list": bound_identity["node"],
        "account": bound_identity["account"],
        "env_id": bound_identity["env_id"],
        "resource_generation": bound_identity["resource_generation"],
        "runtime_id": bound_identity["runtime_id"],
        "candidate_id": bound_identity["candidate_id"],
        "candidate_sha": bound_identity["candidate_sha"],
        "candidate_tree": bound_identity["candidate_tree"],
        **bound_limits,
        "memory_swap_max_effective": "0",
        "gpu_tres": "cpu=4,mem=16G,gres/gpu=1",
        "gpu_detail": "gpu(IDX:0)",
    }
    return {
        **unsigned,
        "payload_sha256": hashlib.sha256(
            json.dumps(unsigned, sort_keys=True, separators=(",", ":")).encode("ascii"),
        ).hexdigest(),
    }


def _canonical_slice_path(unit: str) -> str:
    match = re.fullmatch(
        r"loom-job-([1-9][0-9]*)-[0-9a-f]{40}\.slice",
        unit,
    )
    assert match is not None
    return f"/loom.slice/loom-job.slice/loom-job-{match.group(1)}.slice/{unit}"


def _mixed_job(*, systemd: bool) -> tuple[dict[str, Any], dict[str, Any]]:
    policy = {
        "requested_cpus": 4,
        "requested_memory_mib": 16 * 1024,
        "job_pids_max": 512,
        "gpu_tres": "gres/gpu=1",
        "container_cpus": 1,
        "container_memory_mib": 1024,
        "container_pids": 64,
    }
    identity = _slice_identity()
    receipt = _slice_receipt(identity=identity)
    unit = str(receipt["systemd_slice"])
    job_path = "/system.slice/slurmstepd.scope/job_42"
    slice_path = _canonical_slice_path(unit)
    container_parent = unit if systemd else job_path
    observed_parent = slice_path if systemd else job_path
    containers: list[dict[str, Any]] = []
    for index, role in enumerate(authority.ROLES):
        container_id = format(index + 1, "x") * 12
        gpu_ids = ["0"] if index == 0 else []
        containers.append(
            {
                "container_id": container_id,
                "name": f"dynamic-a-{role}",
                "role": role,
                "sandbox": "dynamic-a",
                "candidate_sha": "a" * 40,
                "job_id": "42",
                "compose_project": "dynamic-a-gb10",
                "identity_labels": {
                    "loom.sandbox": "dynamic-a",
                    "loom.candidate_sha": "a" * 40,
                    "loom.slurm_job_id": "42",
                    "loom.compose_project": "dynamic-a-gb10",
                    "loom.env_id": "denv-dynamic-a",
                    "loom.resource_generation": "2",
                    "loom.candidate_id": "cand-" + "b" * 40,
                    "loom.candidate_tree": "c" * 40,
                    "loom.registry_generation": "7",
                    "loom.registry_payload_sha256": "d" * 64,
                },
                "compose_networks": ["dynamic-a-gb10_default"],
                "pid": 1000 + index,
                "cgroup_parent": container_parent,
                "observed_cgroup_path": (f"{observed_parent}/docker-{container_id}.scope"),
                "limits": {
                    "cpu_cores": 1,
                    "memory_bytes": 1024**3,
                    "pids": 64,
                    "gpu_count": len(gpu_ids),
                    "gpu_ids": gpu_ids,
                },
            },
        )
    cgroup = {
        "layout_version": ("systemd-mirror-v1" if systemd else "cgroupfs-job-v1"),
        "job_path": job_path,
        "container_parent": container_parent,
        "slurm_job_id": "42",
        "slurm_pid_cgroup_paths": [f"{job_path}/step_batch.scope"],
        "controllers": ["cpu", "memory", "pids"],
        "delegated_controllers": ["cpu", "memory", "pids"],
        "delegated": True,
        "cpu_cores_max": 4,
        "memory_bytes_max": 16 * 1024**3,
        "pids_max": 512,
        "pids_current": 8,
        "systemd_slice_receipt": receipt if systemd else None,
        "systemd_slice_live": (
            {
                "path": slice_path,
                "cpu_cores_max": 4,
                "memory_bytes_max": 16 * 1024**3,
                "memory_swap_bytes_max": 0,
                "pids_max": 512,
                "cpuset_cpus": "0-3",
                "cpuset_mems": "0",
            }
            if systemd
            else None
        ),
    }
    job = {
        "job_id": "42",
        "job_start_time": identity["job_start_time"],
        "job_name": "loom-dynamic-a-aaaaaaaaaaaa-trt-gb10-7",
        "sandbox": "dynamic-a",
        "env_id": identity["env_id"],
        "resource_generation": identity["resource_generation"],
        "candidate_id": identity["candidate_id"],
        "candidate_sha": identity["candidate_sha"],
        "candidate_tree": identity["candidate_tree"],
        "registry_generation": 7,
        "registry_payload_sha256": "d" * 64,
        "account": identity["account"],
        "qos": "loom-env-qos",
        "user": "loom-env-user",
        "node": identity["node"],
        "host": "gx10-0faf",
        "state": "RUNNING",
        "allocation": {
            "cpu_cores": 4,
            "memory_bytes": 16 * 1024**3,
            "pids": 512,
            "gpu_count": 1,
            "tres": "cpu=4,mem=16G,gres/gpu=1",
            "gpu_detail": "gpu(IDX:0)",
            "exclusive": False,
        },
        "compose_project": "dynamic-a-gb10",
        "compose_networks": ["dynamic-a-gb10_default"],
        "cgroup": cgroup,
        "containers": containers,
        "aggregate_limits": {
            "cpu_cores": 4,
            "memory_bytes": 4 * 1024**3,
            "pids": 256,
            "gpu_count": 1,
        },
        "device_probe": {
            "method": "docker-nvidia-smi-and-device-denial-v1",
            "allocated_ids": ["0"],
            "all_allocated_usable": True,
            "unallocated_denied": True,
            "allocated_probe_container_ids": [containers[0]["container_id"]],
            "denial_probe_container_ids": [item["container_id"] for item in containers[1:]],
        },
    }
    return job, policy


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
        "NumCPUs=4 StartTime=2026-07-30T00:00:00 "
        "Comment=loom-cgroup-v1:pids=512 Shared=OK "
        "AllocTRES=cpu=4,mem=16G,gres/gpu=1 GresDetail=gpu(IDX:0) MinMemoryNode=16G"
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
        "NumCPUs=4 StartTime=2026-07-30T00:00:00 "
        "Comment=loom-cgroup-v1:pids=512 Shared=OK "
        "AllocTRES=cpu=4,mem=16G,gres/gpu=1 GresDetail=gpu(IDX:0) MinMemoryNode=16G"
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


@pytest.mark.parametrize(
    "start_field",
    ("", "StartTime=unknown "),
)
def test_job_readback_rejects_missing_or_invalid_start_identity(
    start_field: str,
) -> None:
    sha = "a" * 40
    output = (
        "JobId=42 JobName=loom-dynamic-a-"
        + sha[:12]
        + "-work UserId=loom-env-user(32000) Account=loom-env-account "
        "QOS=loom-env-qos JobState=RUNNING NodeList=trt-gb10-7 NumNodes=1 "
        f"NumCPUs=4 {start_field}Comment=loom-cgroup-v1:pids=512 Shared=OK "
        "AllocTRES=cpu=4,mem=16G,gres/gpu=1 "
        "GresDetail=gpu(IDX:0) MinMemoryNode=16G"
    )

    def run(*_args: object, **_kwargs: object) -> Any:
        return type("Result", (), {"returncode": 0, "stdout": output.encode(), "stderr": b""})()

    with pytest.raises(authority.PlatformHealthError, match="candidate identity"):
        authority._job_readback(
            sandbox="dynamic-a",
            candidate_sha=sha,
            environment={
                "slurm_user": "loom-env-user",
                "slurm_account": "loom-env-account",
                "slurm_qos": "loom-env-qos",
            },
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


def test_container_cgroup_layout_accepts_both_reviewed_layouts() -> None:
    job_path = "/system.slice/slurmstepd.scope/job_42"
    assert authority._container_cgroup_layout(
        f"{job_path}/docker-deadbeef.scope",
        job_path,
    ) == ("cgroupfs-job-v1", job_path)
    unit = "loom-job-42-" + "a" * 40 + ".slice"
    slice_path = _canonical_slice_path(unit)
    assert authority._container_cgroup_layout(
        f"{slice_path}/docker-deadbeef.scope",
        unit,
    ) == ("systemd-mirror-v1", slice_path)


@pytest.mark.parametrize(
    ("observed", "parent"),
    (
        ("/system.slice/docker-deadbeef.scope", "/system.slice/job_42"),
        (
            "/loom.slice/loom-job.slice/docker-deadbeef.scope",
            "loom-job-42-" + "a" * 40 + ".slice",
        ),
        (
            "/foreign.slice/loom-job-42.slice/loom-job-42-" + "a" * 40 + ".slice/docker.scope",
            "loom-job-42-" + "a" * 40 + ".slice",
        ),
        (
            "/loom.slice/loom-job.slice/loom-job-42.slice/loom-job-42-"
            + "a" * 40
            + ".slice/docker.scope",
            "loom-job-43-" + "b" * 40 + ".slice",
        ),
    ),
)
def test_container_cgroup_layout_rejects_escape_or_foreign_slice(
    observed: str,
    parent: str,
) -> None:
    with pytest.raises(authority.PlatformHealthError):
        authority._container_cgroup_layout(observed, parent)


def test_systemd_slice_receipt_binds_identity_limits_and_unit_digest(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    unit_bytes = b"[Slice]\nCPUQuota=400%\n"
    identity = _slice_identity()
    limits = _slice_limits()
    receipt = _slice_receipt(
        identity=identity,
        limits=limits,
        unit_bytes=unit_bytes,
    )
    unit = str(receipt["systemd_slice"])

    def read(path: Path, **_kwargs: object) -> bytes:
        if path == authority.SYSTEMD_SLICE_RECEIPT_ROOT / f"{unit}.json":
            return authority._canonical(receipt)
        if path == authority.SYSTEMD_UNIT_ROOT / unit:
            return unit_bytes
        raise AssertionError(path)

    monkeypatch.setattr(authority, "_read_secure_bytes", read)
    assert (
        authority._load_systemd_slice_receipt(
            unit,
            identity=identity,
            limits=limits,
            expected_gpu_tres="cpu=4,mem=16G,gres/gpu=1",
            expected_gpu_detail="gpu(IDX:0)",
        )
        == receipt
    )


def test_systemd_slice_receipt_rejects_unit_digest_drift(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    identity = _slice_identity()
    limits = _slice_limits()
    receipt = _slice_receipt(identity=identity, limits=limits)
    unit = str(receipt["systemd_slice"])

    def read(path: Path, **_kwargs: object) -> bytes:
        if path == authority.SYSTEMD_SLICE_RECEIPT_ROOT / f"{unit}.json":
            return authority._canonical(receipt)
        if path == authority.SYSTEMD_UNIT_ROOT / unit:
            return b"foreign unit\n"
        raise AssertionError(path)

    monkeypatch.setattr(authority, "_read_secure_bytes", read)
    with pytest.raises(authority.PlatformHealthError, match="unit digest"):
        authority._load_systemd_slice_receipt(
            unit,
            identity=identity,
            limits=limits,
            expected_gpu_tres="cpu=4,mem=16G,gres/gpu=1",
            expected_gpu_detail="gpu(IDX:0)",
        )


def test_systemd_slice_live_limits_are_closed_against_slurm(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    receipt = _slice_receipt()
    observed = {
        "cpu.max": "400000 100000",
        "memory.max": str(16 * 1024**3),
        "memory.swap.max": "0",
        "pids.max": "512",
        "cpuset.cpus.effective": "0-3",
        "cpuset.mems.effective": "0",
    }
    monkeypatch.setattr(
        authority,
        "_read_cgroup_limit",
        lambda _path, name: observed[name],
    )
    slice_path = _canonical_slice_path("loom-job-42-" + "a" * 40 + ".slice")
    assert authority._systemd_slice_live_evidence(
        slice_path,
        receipt=receipt,
    ) == {
        "path": slice_path,
        "cpu_cores_max": 4,
        "memory_bytes_max": 16 * 1024**3,
        "memory_swap_bytes_max": 0,
        "pids_max": 512,
        "cpuset_cpus": "0-3",
        "cpuset_mems": "0",
    }


@pytest.mark.parametrize(
    ("control", "value"),
    (
        ("cpu.max", "500000 100000"),
        ("memory.max", str(17 * 1024**3)),
        ("memory.swap.max", "1"),
        ("pids.max", "513"),
        ("cpuset.cpus.effective", "0-4"),
        ("cpuset.mems.effective", "0-1"),
    ),
)
def test_systemd_slice_live_limits_reject_any_weaker_controller(
    control: str,
    value: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed = {
        "cpu.max": "400000 100000",
        "memory.max": str(16 * 1024**3),
        "memory.swap.max": "0",
        "pids.max": "512",
        "cpuset.cpus.effective": "0-3",
        "cpuset.mems.effective": "0",
    }
    observed[control] = value
    monkeypatch.setattr(
        authority,
        "_read_cgroup_limit",
        lambda _path, name: observed[name],
    )
    with pytest.raises(authority.PlatformHealthError, match="weaker than Slurm"):
        authority._systemd_slice_live_evidence(
            _canonical_slice_path("loom-job-42-" + "a" * 40 + ".slice"),
            receipt=_slice_receipt(),
        )


@pytest.mark.parametrize(
    "mutation",
    ("job-reuse", "foreign-env", "limit-drift", "swap-not-clamped", "payload-drift"),
)
def test_systemd_slice_receipt_rejects_stale_foreign_or_drifted_binding(
    mutation: str,
) -> None:
    identity = _slice_identity()
    limits = _slice_limits()
    receipt = _slice_receipt(identity=identity, limits=limits)
    expected_identity = copy.deepcopy(identity)
    expected_limits = copy.deepcopy(limits)
    if mutation == "job-reuse":
        expected_identity["job_start_time"] = "2026-07-30T00:00:01"
    elif mutation == "foreign-env":
        receipt["env_id"] = "denv-foreign-x"
    elif mutation == "limit-drift":
        receipt["memory_max"] = str(32 * 1024**3)
    elif mutation == "swap-not-clamped":
        receipt["memory_swap_max_source"] = "4294967296"
        receipt["memory_swap_max_effective"] = "4294967296"
    else:
        receipt["payload_sha256"] = "0" * 64
    if mutation in {"foreign-env", "limit-drift", "swap-not-clamped"}:
        unsigned = {key: value for key, value in receipt.items() if key != "payload_sha256"}
        receipt["payload_sha256"] = hashlib.sha256(
            json.dumps(unsigned, sort_keys=True, separators=(",", ":")).encode("ascii"),
        ).hexdigest()
    with pytest.raises(authority.PlatformHealthError, match="receipt binding"):
        authority._validated_systemd_slice_receipt_payload(
            receipt,
            identity=expected_identity,
            limits=expected_limits,
            expected_gpu_tres="cpu=4,mem=16G,gres/gpu=1",
            expected_gpu_detail="gpu(IDX:0)",
        )


@pytest.mark.parametrize("systemd", (False, True))
def test_mixed_job_validator_accepts_cgroupfs_and_systemd_mirror(
    systemd: bool,
) -> None:
    job, policy = _mixed_job(systemd=systemd)
    authority._verify_mixed_job_policy(job, policy=policy)


def test_systemd_mirror_rejects_live_limit_weaker_than_slurm() -> None:
    job, policy = _mixed_job(systemd=True)
    job["cgroup"]["systemd_slice_live"]["memory_bytes_max"] = 32 * 1024**3
    with pytest.raises(authority.PlatformHealthError, match="systemd mirror"):
        authority._verify_mixed_job_policy(job, policy=policy)


def test_systemd_mirror_rejects_reused_job_receipt() -> None:
    job, policy = _mixed_job(systemd=True)
    job["job_start_time"] = "2026-07-30T00:00:01"
    with pytest.raises(authority.PlatformHealthError, match="receipt binding"):
        authority._verify_mixed_job_policy(job, policy=policy)


def test_gpu_detail_must_exactly_bind_docker_allocated_ids() -> None:
    job, policy = _mixed_job(systemd=True)
    allocated = next(item for item in job["containers"] if item["limits"]["gpu_ids"])
    allocated["limits"]["gpu_ids"] = ["1"]
    job["device_probe"]["allocated_ids"] = ["1"]
    with pytest.raises(authority.PlatformHealthError, match="mixed job"):
        authority._verify_mixed_job_policy(job, policy=policy)


@pytest.mark.parametrize(
    "detail",
    (
        "gpu(IDX:0),gpu(IDX:0)",
        "gpu(IDX:0-5000)",
        "gpu(IDX:MIG-GPU-deadbeef)",
        "gpu:MIG(IDX:0)",
        "gpu(IDX:0,1)",
        "gpu(UUID:GPU-deadbeef)",
    ),
)
def test_gpu_detail_rejects_duplicates_ranges_and_mig_ambiguity(
    detail: str,
) -> None:
    with pytest.raises(authority.PlatformHealthError, match="GPU detail"):
        authority._gpu_detail_ids(detail)


def test_gpu_detail_parses_bounded_exact_index_ranges() -> None:
    assert authority._gpu_detail_ids(
        "gpu:a100:2(IDX:0-1),gpu:a100:1(IDX:3)",
    ) == {"0", "1", "3"}
