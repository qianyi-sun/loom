from __future__ import annotations

import copy
import importlib.util
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest
from jsonschema import Draft202012Validator

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts/ops/nonexclusive_slurm_acceptance.py"
SCHEMA = REPO_ROOT / "docs/evidence/nonexclusive-slurm-acceptance-v1.schema.json"


def _load_module() -> Any:
    spec = importlib.util.spec_from_file_location("nonexclusive_acceptance", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


ACCEPTANCE = _load_module()


def _run(*args: str | Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(SCRIPT), *map(str, args)],
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )


def _evidence() -> dict[str, Any]:
    candidate = "a" * 40
    sandbox = "staging"
    job_id = "1234"
    project = "loom-staging-1234"
    job_path = "/slurm/job_1234"
    labels = {
        "loom.sandbox": sandbox,
        "loom.candidate_sha": candidate,
        "loom.slurm_job_id": job_id,
        "loom.compose_project": project,
    }
    containers = []
    for index, role in enumerate(("worker", "trial", "verifier", "sidecar"), start=1):
        containers.append(
            {
                "role": role,
                "container_id": f"{index:012x}",
                "name": f"{project}-{role}",
                "pid": 1000 + index,
                "labels": labels.copy(),
                "cgroup_parent": job_path,
                "cgroup_path": f"{job_path}/docker/{index:012x}",
                "limits": {
                    "cpu_cores": 2,
                    "memory_bytes": 2_000_000_000,
                    "pids": 128,
                },
                "device_ids": ["GPU-0"] if role == "trial" else [],
            },
        )

    sandboxes = ["development", "staging", "production"]
    checks = [
        {
            "source": source,
            "target": target,
            "resource": resource,
            "denied": True,
        }
        for source in sandboxes
        for target in sandboxes
        if source != target
        for resource in ("worker_identity", "object_store", "result_path")
    ]
    checkpoints = [
        {
            "event": event,
            "observed_within_seconds": 30,
            "live_containers": 0,
            "live_jobs": 0,
            "durable_trial_state": True,
            "retryable_interrupted_trials": True,
        }
        for event in (
            "cancellation",
            "ttl_expiry",
            "worker_crash",
            "submit_host_restart",
        )
    ]
    return {
        "schema_version": 1,
        "collected_at": "2026-07-27T14:00:00Z",
        "candidate_sha": candidate,
        "sandbox": sandbox,
        "node": {
            "hostname": "oldlab1.internal",
            "slurm_node_name": "oldlab1",
        },
        "job": {
            "job_id": job_id,
            "candidate_sha": candidate,
            "sandbox": sandbox,
            "node": "oldlab1",
            "compose_project": project,
            "allocation": {
                "cpu_cores": 12,
                "memory_bytes": 16_000_000_000,
                "pids": 1024,
                "gpu_ids": ["GPU-0"],
                "tres": "cpu=12,mem=16000M,gres/gpu=1",
            },
        },
        "containers": containers,
        "cgroup": {
            "job_path": job_path,
            "controllers": ["cpu", "memory", "pids"],
            "delegated": True,
            "cpu_cores_max": 10,
            "memory_bytes_max": 12_000_000_000,
            "pids_max": 800,
        },
        "aggregate_caps": {
            "cpu_cores": 8,
            "memory_bytes": 8_000_000_000,
            "pids": 512,
        },
        "devices": {
            "allocated_ids": ["GPU-0"],
            "all_allocated_usable": True,
            "unallocated_denied": True,
        },
        "headroom": {
            "duration_seconds": 1800,
            "required_duration_seconds": 1800,
            "sample_count": 60,
            "required_sample_count": 30,
            "min_free_cpu_cores": 8,
            "required_free_cpu_cores": 4,
            "min_free_memory_bytes": 32_000_000_000,
            "required_free_memory_bytes": 16_000_000_000,
            "max_pid_usage_ratio": 0.45,
            "max_allowed_pid_usage_ratio": 0.7,
            "observed_peak_concurrency": 2,
            "reviewed_max_concurrency": 2,
            "kube_api_healthy": True,
            "minio_quorum_healthy": True,
            "longhorn_healthy": True,
            "within_reviewed_envelope": True,
        },
        "negative_isolation": {
            "sandboxes": sandboxes,
            "checks": checks,
        },
        "soak": {
            "duration_seconds": 14_400,
            "required_duration_seconds": 14_400,
            "sample_count": 240,
            "required_sample_count": 120,
            "workloads": [
                "loom",
                "non_loom_slurm",
                "kubernetes",
                "minio",
                "longhorn",
            ],
            "trial_success_ratio": 0.99,
            "minimum_trial_success_ratio": 0.95,
            "resource_envelope_breaches": 0,
            "kube_api_healthy": True,
            "minio_quorum_healthy": True,
            "longhorn_healthy": True,
            "non_loom_slurm_healthy": True,
        },
        "cleanup": {
            "max_cleanup_seconds": 60,
            "checkpoints": checkpoints,
        },
    }


def _failures(evidence: dict[str, Any]) -> list[str]:
    schema = json.loads(SCHEMA.read_text(encoding="utf-8"))
    return ACCEPTANCE.verify_evidence(evidence, schema)


def test_schema_is_valid_and_complete_fixture_passes() -> None:
    schema = json.loads(SCHEMA.read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(schema)

    assert _failures(_evidence()) == []


@pytest.mark.parametrize(
    ("path", "value"),
    [
        (("collected_at",), "not-a-timestamp"),
        (("headroom", "required_duration_seconds"), 1),
        (("headroom", "required_sample_count"), 1),
        (("headroom", "required_free_cpu_cores"), 0),
        (("headroom", "required_free_memory_bytes"), 0),
        (("headroom", "max_allowed_pid_usage_ratio"), 1),
        (("soak", "required_duration_seconds"), 1),
        (("soak", "required_sample_count"), 1),
        (("soak", "minimum_trial_success_ratio"), 0),
        (("cleanup", "max_cleanup_seconds"), 301),
    ],
)
def test_policy_thresholds_cannot_be_weakened(
    path: tuple[str, ...],
    value: Any,
) -> None:
    evidence = _evidence()
    target: dict[str, Any] = evidence
    for part in path[:-1]:
        target = target[part]
    target[path[-1]] = value

    assert _failures(evidence)


def test_slurm_tres_must_bind_cpu_memory_and_gpu() -> None:
    evidence = _evidence()
    evidence["job"]["allocation"]["tres"] = "cpu=12,mem=16000M"

    assert any("GPU allocation" in item for item in _failures(evidence))


def test_plan_is_explicitly_read_only_and_lists_stop_rules() -> None:
    completed = _run("plan")

    assert completed.returncode == 0, completed.stderr
    plan = json.loads(completed.stdout)
    assert plan["mode"] == "repo_only_read_only"
    assert plan["mutations_supported"] is False
    assert len(plan["stop_rules"]) >= 7
    assert "cleanup_checkpoints" in plan["required_evidence"]


@pytest.mark.parametrize(
    "field",
    [
        "candidate_sha",
        "sandbox",
        "node",
        "job",
        "containers",
        "cgroup",
        "aggregate_caps",
        "devices",
        "headroom",
        "negative_isolation",
        "soak",
        "cleanup",
    ],
)
def test_missing_acceptance_domain_fails_closed(field: str) -> None:
    evidence = _evidence()
    del evidence[field]

    failures = _failures(evidence)

    assert failures
    assert failures[0].startswith("schema violation")


@pytest.mark.parametrize(
    ("mutate", "expected"),
    [
        (
            lambda item: item["job"].__setitem__("candidate_sha", "b" * 40),
            "job candidate SHA",
        ),
        (
            lambda item: item["containers"][0]["labels"].__setitem__(
                "loom.sandbox",
                "production",
            ),
            "container labels",
        ),
        (
            lambda item: item["containers"][0].__setitem__(
                "cgroup_path",
                "/slurm/job_999/docker/escape",
            ),
            "not inside the Slurm job cgroup",
        ),
        (
            lambda item: item["containers"][0].__setitem__(
                "cgroup_parent",
                "/slurm",
            ),
            "cgroup parent",
        ),
    ],
)
def test_candidate_label_and_cgroup_identity_fail_closed(
    mutate: Any,
    expected: str,
) -> None:
    evidence = _evidence()
    mutate(evidence)

    assert any(expected in failure for failure in _failures(evidence))


def test_container_identity_must_be_unique_and_paths_cannot_traverse() -> None:
    evidence = _evidence()
    evidence["containers"][1]["container_id"] = evidence["containers"][0]["container_id"]
    evidence["containers"][2]["cgroup_path"] = (
        f"{evidence['cgroup']['job_path']}/../job_999/docker/escape"
    )

    failures = _failures(evidence)

    assert any("container_id values must be unique" in item for item in failures)
    assert any("not inside the Slurm job cgroup" in item for item in failures)


@pytest.mark.parametrize("field", ["cpu_cores", "memory_bytes", "pids"])
def test_aggregate_cap_overallocation_fails_closed(field: str) -> None:
    evidence = _evidence()
    evidence["aggregate_caps"][field] = evidence["job"]["allocation"][field] + 1

    failures = _failures(evidence)

    assert any(f"aggregate {field} exceeds Slurm allocation" in item for item in failures)


def test_devices_require_allocation_match_usability_and_denial() -> None:
    evidence = _evidence()
    evidence["containers"][0]["device_ids"] = ["GPU-9"]

    failures = _failures(evidence)

    assert any("outside the Slurm allocation" in item for item in failures)

    evidence = _evidence()
    evidence["devices"]["unallocated_denied"] = False
    failures = _failures(evidence)
    assert any("schema violation at devices.unallocated_denied" in item for item in failures)


def test_headroom_requires_duration_samples_health_and_reviewed_envelope() -> None:
    evidence = _evidence()
    evidence["headroom"]["duration_seconds"] = 10
    evidence["headroom"]["observed_peak_concurrency"] = 3

    failures = _failures(evidence)

    assert any("headroom observation duration" in item for item in failures)
    assert any("observed concurrency" in item for item in failures)

    evidence = _evidence()
    evidence["headroom"]["kube_api_healthy"] = False
    failures = _failures(evidence)
    assert any("headroom.kube_api_healthy" in item for item in failures)


def test_negative_isolation_requires_every_ordered_pair_and_denial() -> None:
    evidence = _evidence()
    evidence["negative_isolation"]["checks"][-1] = copy.deepcopy(
        evidence["negative_isolation"]["checks"][0],
    )

    failures = _failures(evidence)

    assert any("negative isolation matrix" in item for item in failures)

    evidence = _evidence()
    evidence["sandbox"] = "candidate"
    evidence["job"]["sandbox"] = "candidate"
    for container in evidence["containers"]:
        container["labels"]["loom.sandbox"] = "candidate"

    assert any("absent from the isolation matrix" in item for item in _failures(evidence))


def test_soak_requires_duration_workload_health_and_success() -> None:
    evidence = _evidence()
    evidence["soak"]["duration_seconds"] = 60
    evidence["soak"]["trial_success_ratio"] = 0.5

    failures = _failures(evidence)

    assert any("soak duration" in item for item in failures)
    assert any("success ratio" in item for item in failures)

    evidence = _evidence()
    evidence["soak"]["workloads"].pop()
    failures = _failures(evidence)
    assert any("soak.workloads" in item for item in failures)


def test_cleanup_requires_all_events_no_orphans_and_retryable_state() -> None:
    evidence = _evidence()
    evidence["cleanup"]["checkpoints"][-1]["event"] = "cancellation"

    failures = _failures(evidence)

    assert any("cleanup checkpoints" in item for item in failures)

    evidence = _evidence()
    evidence["cleanup"]["checkpoints"][0]["live_containers"] = 1
    evidence["cleanup"]["checkpoints"][1]["retryable_interrupted_trials"] = False
    failures = _failures(evidence)
    assert any("cleanup.checkpoints.0.live_containers" in item for item in failures)
    assert any("cleanup.checkpoints.1.retryable_interrupted_trials" in item for item in failures)


def test_secret_like_input_is_rejected_without_echoing_value(tmp_path: Path) -> None:
    evidence = _evidence()
    evidence["api_token"] = "loom_api_DO_NOT_ECHO_123456"
    source = tmp_path / "unsafe.json"
    source.write_text(json.dumps(evidence), encoding="utf-8")

    completed = _run("verify", "--evidence", source)

    assert completed.returncode == 1
    assert "secret-like field" in completed.stdout
    assert "DO_NOT_ECHO" not in completed.stdout


def test_collect_only_canonicalizes_complete_offline_evidence(tmp_path: Path) -> None:
    source = tmp_path / "observed.json"
    output = tmp_path / "evidence.json"
    source.write_text(json.dumps(_evidence()), encoding="utf-8")

    completed = _run("collect", "--input", source, "--output", output)

    assert completed.returncode == 0, completed.stdout
    assert json.loads(completed.stdout)["status"] == "pass"
    assert json.loads(output.read_text(encoding="utf-8")) == _evidence()
    assert "import subprocess" not in SCRIPT.read_text(encoding="utf-8")


def test_collect_does_not_write_partial_evidence(tmp_path: Path) -> None:
    source = tmp_path / "incomplete.json"
    output = tmp_path / "evidence.json"
    evidence = _evidence()
    del evidence["cleanup"]
    source.write_text(json.dumps(evidence), encoding="utf-8")

    completed = _run("collect", "--input", source, "--output", output)

    assert completed.returncode == 1
    assert not output.exists()


def test_collect_never_overwrites_an_existing_artifact(tmp_path: Path) -> None:
    source = tmp_path / "observed.json"
    output = tmp_path / "evidence.json"
    source.write_text(json.dumps(_evidence()), encoding="utf-8")
    output.write_text("preserve-me\n", encoding="utf-8")

    completed = _run("collect", "--input", source, "--output", output)

    assert completed.returncode == 1
    assert output.read_text(encoding="utf-8") == "preserve-me\n"
    assert "cannot create output artifact" in completed.stdout
