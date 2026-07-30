from __future__ import annotations

import copy
import hashlib
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


def _gate6_registry_snapshot(
    candidates: dict[str, dict[str, str]],
) -> dict[str, Any]:
    registry = ACCEPTANCE.live_acceptance.environment_registry
    environments: list[dict[str, Any]] = []
    candidate_rows: list[dict[str, Any]] = []
    deployments: list[dict[str, Any]] = []
    finalizations: list[dict[str, Any]] = []
    for index, (sandbox, candidate) in enumerate(sorted(candidates.items()), start=1):
        env_id = f"denv-{index:08d}"
        principal_id = f"github:{1000 + index}"
        resources = registry.DeveloperEnvironmentRegistry._dynamic_resources(
            env_id,
            sandbox,
        )
        identity = {
            "principal_id": principal_id,
            "env_id": env_id,
            "lifecycle_epoch": 1,
            "repository_id": "qianyi-sun/loom",
            "candidate_sha": candidate["sha"],
            "candidate_tree": candidate["tree"],
            "bundle_sha256": str(index) * 64,
        }
        candidate_id = f"cand-{registry._digest(identity)[:40]}"
        environments.append(
            {
                "env_id": env_id,
                "principal_id": principal_id,
                "display_name": sandbox,
                "layout_version": "dynamic-v1",
                "runtime_id": sandbox,
                "state": "active",
                "resource_generation": 2,
                "lifecycle_epoch": 1,
                **resources,
                "uid": 32_000 + index,
                "gid": 32_000 + index,
                "ports": {
                    name: 30_000 + index * 100 + offset
                    for offset, name in enumerate(registry.PORT_NAMES)
                },
                "current_candidate_id": candidate_id,
                "created_at": "2026-07-28T00:00:00Z",
            },
        )
        candidate_rows.append(
            {
                "candidate_id": candidate_id,
                **identity,
                "bundle_size": 1024,
                "bundle_path": (
                    f"/var/lib/loom-developer-environments/candidates/"
                    f"{candidate_id}/candidate.bundle"
                ),
                "image_digests": {
                    "amd64": f"sha256:{candidate['sha']}{candidate['sha'][:24]}",
                    "arm64": f"sha256:{candidate['tree']}{candidate['tree'][:24]}",
                },
                "imported_at": "2026-07-28T00:00:00Z",
            },
        )
        deployment_id = f"dep-{index:032x}"
        applied_registry_payload_sha256 = f"{index + 5:x}" * 64
        finalization_unsigned = {
            "deployment_id": deployment_id,
            "env_id": env_id,
            "principal_id": principal_id,
            "candidate_id": candidate_id,
            "candidate_sha": candidate["sha"],
            "candidate_tree": candidate["tree"],
            "applied_resource_generation": 2,
            "applied_registry_generation": 41,
            "applied_registry_payload_sha256": applied_registry_payload_sha256,
            "capacity_finalize_receipt_sha256": "a" * 64,
            "capacity_finalize_check_receipt_sha256": "b" * 64,
            "runtime_reconcile_receipt_sha256": "c" * 64,
            "runtime_prepare_check_receipt_sha256": "d" * 64,
            "acceptance_probe_receipt_sha256": "e" * 64,
            "created_at": "2026-07-28T00:00:01Z",
        }
        finalization = {
            **finalization_unsigned,
            "payload_sha256": registry._digest(finalization_unsigned),
        }
        finalizations.append(finalization)
        deployments.append(
            {
                "deployment_id": deployment_id,
                "principal_id": principal_id,
                "env_id": env_id,
                "candidate_id": candidate_id,
                "expected_resource_generation": 1,
                "applied_resource_generation": 2,
                "applied_registry_generation": 41,
                "applied_registry_payload_sha256": applied_registry_payload_sha256,
                "finalization_payload_sha256": finalization["payload_sha256"],
                "phase": "committed",
                "previous_candidate_id": None,
                "request_digest": str(index + 4) * 64,
                "created_at": "2026-07-28T00:00:00Z",
                "updated_at": "2026-07-28T00:00:01Z",
            },
        )
    unsigned = {
        "schema_version": 1,
        "kind": "loom.developer-environment.registry-snapshot",
        "generation": 42,
        "environments": sorted(
            environments,
            key=lambda environment: (environment["env_id"], environment["runtime_id"]),
        ),
        "candidates": sorted(
            candidate_rows,
            key=lambda candidate: candidate["candidate_id"],
        ),
        "deployments": sorted(
            deployments,
            key=lambda deployment: deployment["deployment_id"],
        ),
        "deployment_finalizations": finalizations,
    }
    source = {
        **unsigned,
        "payload_sha256": hashlib.sha256(
            ACCEPTANCE._canonical_bytes(unsigned),
        ).hexdigest(),
    }
    return ACCEPTANCE.live_acceptance._acceptance_registry_snapshot(source)


def _gate6_sources() -> tuple[
    dict[str, Any],
    dict[str, Any],
    dict[tuple[str, str], dict[str, Any]],
]:
    candidates = {
        "qianyi": {"sha": "a" * 40, "tree": "b" * 40},
        "hongjian": {"sha": "c" * 40, "tree": "d" * 40},
        "devansh": {"sha": "e" * 40, "tree": "f" * 40},
        "quinn": {"sha": "1" * 40, "tree": "2" * 40},
    }
    registry_snapshot = _gate6_registry_snapshot(candidates)
    sandboxes = [environment["runtime_id"] for environment in registry_snapshot["environments"]]
    environments = {
        environment["runtime_id"]: environment for environment in registry_snapshot["environments"]
    }
    checks = [
        {"source": source, "target": target, "resource": resource, "denied": True}
        for source in sandboxes
        for target in sandboxes
        if source != target
        for resource in ("worker_identity", "object_store", "result_path")
    ]
    live: dict[str, Any] = {
        "schema_version": 2,
        "session": {"id": "1" * 32},
        "candidates": candidates,
        "registry_snapshot": registry_snapshot,
        "state_machine": [{"phase": index} for index in range(33)],
        "topology": {
            "eligible_nodes": [
                *ACCEPTANCE.GATE6_POOL_NODES["oldlab"],
                *ACCEPTANCE.GATE6_POOL_NODES["gb10"],
            ],
            "excluded_nodes": [],
        },
        "cross_sandbox_negative": checks,
    }
    platform_jobs: list[dict[str, Any]] = []
    devices: list[dict[str, Any]] = []
    pair_headroom: list[dict[str, Any]] = []
    matrices: dict[tuple[str, str], dict[str, Any]] = {}
    for sandbox_index, sandbox in enumerate(sandboxes, start=1):
        candidate = candidates[sandbox]
        for pool_index, pool in enumerate(ACCEPTANCE.GATE6_POOLS, start=1):
            job_id = str(1000 + sandbox_index * 10 + pool_index)
            node = (
                ACCEPTANCE.GATE6_POOL_NODES["oldlab"][0]
                if pool == "oldlab"
                else ACCEPTANCE.GATE6_POOL_NODES["gb10"][0]
            )
            host = f"{pool}-{sandbox}-host"
            job_path = f"/slurm/job_{job_id}"
            project = f"loom-{sandbox}-{job_id}"
            gpu_ids = [] if pool == "oldlab" else ["GPU-0"]
            containers = []
            for role_index, role in enumerate(
                ("worker", "trial", "verifier", "sidecar"),
                start=1,
            ):
                container_gpu_ids = gpu_ids if role == "trial" else []
                containers.append(
                    {
                        "container_id": f"{sandbox_index}{pool_index}{role_index:010d}",
                        "name": f"{project}-{role}",
                        "role": role,
                        "sandbox": sandbox,
                        "candidate_sha": candidate["sha"],
                        "job_id": job_id,
                        "compose_project": project,
                        "compose_networks": [f"{project}_default"],
                        "identity_labels": {
                            "loom.sandbox": sandbox,
                            "loom.candidate_sha": candidate["sha"],
                            "loom.slurm_job_id": job_id,
                            "loom.compose_project": project,
                        },
                        "pid": 2000 + role_index,
                        "cgroup_parent": job_path,
                        "observed_cgroup_path": (
                            f"{job_path}/docker/{sandbox_index}{pool_index}{role_index:010d}"
                        ),
                        "limits": {
                            "cpu_cores": 1,
                            "memory_bytes": 1_000_000_000,
                            "pids": 128,
                            "gpu_count": len(container_gpu_ids),
                            "gpu_ids": container_gpu_ids,
                        },
                    },
                )
            allocation = {
                "cpu_cores": 8 if pool == "oldlab" else 16,
                "memory_bytes": 32_000_000_000 if pool == "oldlab" else 92_000_000_000,
                "pids": 32_768 if pool == "oldlab" else 65_536,
                "gpu_count": len(gpu_ids),
                "tres": (
                    "cpu=8,mem=32000M" if pool == "oldlab" else "cpu=16,mem=92000M,gres/gpu=1"
                ),
                "exclusive": False,
            }
            platform_jobs.append(
                {
                    "job_id": job_id,
                    "job_name": f"loom-{sandbox}-{candidate['sha'][:12]}-{node}",
                    "sandbox": sandbox,
                    "candidate_sha": candidate["sha"],
                    "account": environments[sandbox]["slurm_account"],
                    "user": environments[sandbox]["slurm_user"],
                    "node": node,
                    "state": "RUNNING",
                    "allocation": allocation,
                    "compose_project": project,
                    "compose_networks": [f"{project}_default"],
                    "cgroup": {
                        "job_path": job_path,
                        "slurm_job_id": job_id,
                        "slurm_pid_cgroup_paths": [f"{job_path}/step_batch"],
                        "controllers": ["cpu", "memory", "pids"],
                        "delegated": True,
                        "cpu_cores_max": allocation["cpu_cores"],
                        "memory_bytes_max": allocation["memory_bytes"],
                        "pids_max": allocation["pids"],
                    },
                    "containers": containers,
                    "aggregate_limits": {
                        "cpu_cores": 4,
                        "memory_bytes": 4_000_000_000,
                        "pids": 512,
                        "gpu_count": len(gpu_ids),
                    },
                },
            )
            devices.append(
                {
                    "sandbox": sandbox,
                    "pool": pool,
                    "job_id": job_id,
                    "node": node,
                    "host": host,
                    "allocated_ids": gpu_ids,
                    "all_allocated_usable": True,
                    "unallocated_denied": True,
                    "proof": {
                        "method": (
                            "docker-no-device-exposure-v1"
                            if pool == "oldlab"
                            else "docker-nvidia-smi-and-device-denial-v1"
                        ),
                        "allocated_probe_container_ids": (
                            [] if pool == "oldlab" else [containers[1]["container_id"]]
                        ),
                        "denial_probe_container_ids": [containers[0]["container_id"]],
                        "observed_at": "2026-07-28T04:00:00Z",
                    },
                },
            )
            pair_headroom.append(
                {
                    "sandbox": sandbox,
                    "pool": pool,
                    "min_free_cpu_cores": 8,
                    "min_free_memory_bytes": 32_000_000_000,
                    "max_pid_usage_ratio": 0.4,
                    "observed_peak_concurrency": 1,
                    "within_reviewed_envelope": True,
                },
            )
            nodes = ACCEPTANCE.GATE6_POOL_NODES[pool]
            matrix_rows = [
                {
                    "node": matrix_node,
                    "sandbox": sandbox,
                    "state": "COMPLETED",
                    "account": environments[sandbox]["slurm_account"],
                    "qos": environments[sandbox]["slurm_qos"],
                    "sbatch_verified": True,
                    "srun_verified": True,
                    "nonexclusive": True,
                    "explicit_nodelist": matrix_node,
                    "gpu_verified": True,
                    "compute_check": {
                        "sandbox": sandbox,
                        "account": environments[sandbox]["slurm_account"],
                        "candidate_sha": candidate["sha"],
                        "candidate_tree": candidate["tree"],
                        "pool": pool,
                        "concurrency": ACCEPTANCE.GATE6_POOL_CONCURRENCY[pool],
                        "cgroup_guard_verified": True,
                        "compose_verified": True,
                    },
                }
                for matrix_node in nodes
            ]
            matrices[(sandbox, pool)] = {
                "schema_version": 1,
                "artifact_type": "developer-sandbox-slurm-allocation-matrix",
                "candidate_sha": candidate["sha"],
                "candidate_tree": candidate["tree"],
                "sandbox": sandbox,
                "cluster": ACCEPTANCE.GATE6_CLUSTERS[pool],
                "expected_pool": pool,
                "expected_concurrency": ACCEPTANCE.GATE6_POOL_CONCURRENCY[pool],
                "account": environments[sandbox]["slurm_account"],
                "qos": environments[sandbox]["slurm_qos"],
                "allowed_nodes": list(nodes),
                "candidate_binding": {
                    "repository": {
                        "candidate_sha": candidate["sha"],
                        "candidate_tree": candidate["tree"],
                    },
                },
                "runtime_attestation": {
                    "sandbox": sandbox,
                    "candidate_sha": candidate["sha"],
                    "candidate_tree": candidate["tree"],
                    "domain": pool,
                    "env_id": environments[sandbox]["env_id"],
                    "resource_generation": 2,
                    "registry_generation": registry_snapshot["generation"],
                    "registry_payload_sha256": registry_snapshot["source_registry"][
                        "payload_sha256"
                    ],
                },
                "nodes": matrix_rows,
                "closed_world_verified": True,
            }
    cleanup = [
        {
            "event": event,
            "checkpoint": event,
            "job_ids": ["1"],
            "terminal_states": ["COMPLETED"],
            "observed_within_seconds": 30,
            "maximum_cleanup_seconds": 300,
            "live_containers": 0,
            "live_jobs": 0,
            "durable_trial_state": True,
            "retryable_interrupted_trials": True,
            "observed_at": "2026-07-28T04:00:00Z",
        }
        for event in (
            "cancellation",
            "ttl_expiry",
            "worker_crash",
            "submit_host_restart",
        )
    ]
    soak = {
        "started_at": "2026-07-28T00:00:00Z",
        "completed_at": "2026-07-28T04:00:00Z",
        "duration_seconds": 14_400,
        "sample_count": 120,
        "required_duration_seconds": 14_400,
        "required_sample_count": 120,
        "workloads": ["loom", "non_loom_slurm", "kubernetes", "minio", "longhorn"],
        "trial_success_ratio": 0.99,
        "minimum_trial_success_ratio": 0.95,
        "resource_envelope_breaches": 0,
        "kube_api_healthy": True,
        "minio_quorum_healthy": True,
        "longhorn_healthy": True,
        "non_loom_slurm_healthy": True,
        "pair_headroom": pair_headroom,
    }
    platform = {
        "schema_version": 1,
        "kind": "loom.developer-sandbox.platform-health-evidence",
        "session_id": live["session"]["id"],
        "candidates": candidates,
        "checkpoints": [
            {"checkpoint": "mixed_non_loom", "observed_at": "2026-07-28T00:00:00Z"},
        ],
        "mixed_jobs": platform_jobs,
        "gate6_observations": {
            "soak": soak,
            "device_isolation": devices,
            "cleanup": cleanup,
        },
        "payload_sha256": "9" * 64,
    }
    return live, platform, matrices


def _build_gate6(
    live: dict[str, Any],
    platform: dict[str, Any],
    matrices: dict[tuple[str, str], dict[str, Any]],
) -> tuple[dict[str, Any], dict[tuple[str, str], dict[str, Any]]]:
    schema = json.loads(SCHEMA.read_text(encoding="utf-8"))
    return ACCEPTANCE.build_gate6_bundle(live, platform, matrices, schema)


def test_gate6_bridge_verifies_dynamic_pairs_and_unchanged_gb10_v1() -> None:
    live, platform, matrices = _gate6_sources()

    bundle, artifacts = _build_gate6(live, platform, matrices)

    assert bundle["status"] == "pass"
    assert bundle["state_machine_phase_count"] == 33
    assert len(bundle["allocation_matrices"]) == 8
    assert len(artifacts) == 8
    assert bundle["registry_generation"] == 42
    schema = json.loads(SCHEMA.read_text(encoding="utf-8"))
    assert all(
        ACCEPTANCE.verify_evidence(artifacts[(sandbox, "gb10")], schema) == []
        for sandbox in live["candidates"]
    )


@pytest.mark.parametrize("attack", ("partial_phases", "node7_omitted", "excluded_node"))
def test_gate6_bridge_rejects_partial_or_excluded_live_evidence(attack: str) -> None:
    live, platform, matrices = _gate6_sources()
    if attack == "partial_phases":
        live["state_machine"].pop()
    elif attack == "node7_omitted":
        live["topology"]["eligible_nodes"].remove("trt-gb10-7")
    else:
        live["topology"]["excluded_nodes"] = ["trt-gb10-7"]

    with pytest.raises(ACCEPTANCE.AcceptanceError):
        _build_gate6(live, platform, matrices)


def test_gate6_bridge_rejects_candidate_or_matrix_mismatch() -> None:
    live, platform, matrices = _gate6_sources()
    matrices[("qianyi", "gb10")]["candidate_sha"] = "f" * 40

    with pytest.raises(ACCEPTANCE.AcceptanceError, match="matrix binding"):
        _build_gate6(live, platform, matrices)


@pytest.mark.parametrize("attack", ("source_digest", "stale_projection"))
def test_gate6_bridge_rejects_tampered_or_stale_registry_snapshot(
    attack: str,
) -> None:
    live, platform, matrices = _gate6_sources()
    snapshot = live["registry_snapshot"]
    if attack == "source_digest":
        snapshot["source_registry"]["payload_sha256"] = "0" * 64
    else:
        snapshot["generation"] -= 1
        unsigned = {key: value for key, value in snapshot.items() if key != "payload_sha256"}
        snapshot["payload_sha256"] = hashlib.sha256(
            ACCEPTANCE._canonical_bytes(unsigned),
        ).hexdigest()

    with pytest.raises(ACCEPTANCE.AcceptanceError, match="registry snapshot"):
        _build_gate6(live, platform, matrices)


def test_gate6_bridge_cannot_route_gb10_through_zero_device_branch() -> None:
    live, platform, matrices = _gate6_sources()
    gb10 = next(
        row
        for row in platform["gate6_observations"]["device_isolation"]
        if row["sandbox"] == "qianyi" and row["pool"] == "gb10"
    )
    gb10["allocated_ids"] = []

    with pytest.raises(ACCEPTANCE.AcceptanceError, match="gb10 v1"):
        _build_gate6(live, platform, matrices)


def test_gate6_bridge_rejects_oldlab_missing_device_denial() -> None:
    live, platform, matrices = _gate6_sources()
    oldlab = next(
        row
        for row in platform["gate6_observations"]["device_isolation"]
        if row["sandbox"] == "qianyi" and row["pool"] == "oldlab"
    )
    oldlab["unallocated_denied"] = False

    with pytest.raises(ACCEPTANCE.AcceptanceError, match="zero-device"):
        _build_gate6(live, platform, matrices)


def test_gate6_bridge_rejects_missing_native_field() -> None:
    live, platform, matrices = _gate6_sources()
    del platform["mixed_jobs"][0]["containers"][0]["identity_labels"]

    with pytest.raises(ACCEPTANCE.AcceptanceError, match="incomplete"):
        _build_gate6(live, platform, matrices)

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
