from __future__ import annotations

import hashlib
import json
import os
import subprocess
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from jsonschema import Draft202012Validator
from scripts.ops import developer_sandbox_platform_health_authority as authority

CONFIG = Path("deploy/developer-sandboxes/platform-health-authority.toml")
LIVE_SCHEMA = Path("docs/evidence/developer-sandbox-live-acceptance.schema.json")
SESSION = "1" * 32
CANDIDATES = {
    "qianyi": {"sha": "a" * 40, "tree": "1" * 40},
    "hongjian": {"sha": "b" * 40, "tree": "2" * 40},
    "devansh": {"sha": "c" * 40, "tree": "3" * 40},
}


def _iso(offset: int) -> str:
    return (
        (datetime(2026, 7, 29, tzinfo=UTC) + timedelta(seconds=offset))
        .isoformat()
        .replace(
            "+00:00",
            "Z",
        )
    )


def _platform_health() -> dict[str, Any]:
    return {
        "k3s": {"readyz": True, "node_count": 3, "ready_node_count": 3},
        "minio": {
            "replicas": 4,
            "ready_replicas": 4,
            "quorum_healthy": True,
            "pdb_expected_pods": 4,
            "pdb_current_healthy": 4,
            "pdb_desired_healthy": 3,
            "pdb_disruptions_allowed": 1,
        },
        "longhorn": {
            "volume_count": 2,
            "healthy_volume_count": 2,
            "pod_count": 8,
            "ready_pod_count": 8,
        },
    }


def _job(sandbox: str, node: str, job_id: str) -> dict[str, Any]:
    pool = "oldlab" if "oldlab-" in node else "gb10"
    host = next(
        host for alias, host in authority.EXPECTED_HOST_ALIASES.items() if node in {alias, host}
    )
    policy = authority._load_capacity_policy(pool)["values"]
    job_path = f"/system.slice/slurmstepd.scope/job_{job_id}"
    compose_project = f"loom-{sandbox}-{job_id}"
    compose_networks = [f"{compose_project}_default"]
    containers = [
        {
            "container_id": f"{int(job_id) * 16 + index:064x}",
            "name": f"{compose_project}-{role}",
            "role": role,
            "sandbox": sandbox,
            "candidate_sha": CANDIDATES[sandbox]["sha"],
            "job_id": job_id,
            "compose_project": compose_project,
            "identity_labels": {
                "loom.sandbox": sandbox,
                "loom.candidate_sha": CANDIDATES[sandbox]["sha"],
                "loom.slurm_job_id": job_id,
                "loom.compose_project": compose_project,
            },
            "compose_networks": compose_networks,
            "pid": int(job_id) * 16 + index,
            "cgroup_parent": job_path,
            "observed_cgroup_path": f"{job_path}/docker/{int(job_id) * 16 + index}",
            "limits": {
                "cpu_cores": policy["container_cpus"],
                "memory_bytes": policy["container_memory_mib"] * 1024**2,
                "pids": policy["container_pids"],
                "gpu_count": 1 if pool == "gb10" and role == "worker" else 0,
                "gpu_ids": ["GPU-test"] if pool == "gb10" and role == "worker" else [],
            },
        }
        for index, role in enumerate(authority.ROLES, start=1)
    ]
    return {
        "job_id": job_id,
        "job_name": f"loom-{sandbox}-{CANDIDATES[sandbox]['sha'][:12]}-{node}",
        "sandbox": sandbox,
        "candidate_sha": CANDIDATES[sandbox]["sha"],
        "account": f"loom-dev-{sandbox}",
        "user": f"loom-sandbox-{sandbox}",
        "node": node,
        "host": host,
        "state": "RUNNING",
        "allocation": {
            "cpu_cores": policy["requested_cpus"],
            "memory_bytes": policy["requested_memory_mib"] * 1024**2,
            "pids": policy["job_pids_max"],
            "gpu_count": 1 if policy["gpu_tres"] else 0,
            "tres": (
                f"cpu={policy['requested_cpus']},"
                f"mem={policy['requested_memory_mib']}M"
                + (f",gres/{policy['gpu_tres'].replace(':', '=')}" if policy["gpu_tres"] else "")
            ),
            "exclusive": False,
        },
        "compose_project": compose_project,
        "compose_networks": compose_networks,
        "cgroup": {
            "job_path": job_path,
            "slurm_job_id": job_id,
            "slurm_pid_cgroup_paths": [f"{job_path}/step_batch"],
            "controllers": ["cpu", "memory", "pids"],
            "delegated_controllers": ["cpu", "memory", "pids"],
            "delegated": True,
            "cpu_cores_max": policy["requested_cpus"],
            "memory_bytes_max": policy["requested_memory_mib"] * 1024**2,
            "pids_max": policy["job_pids_max"],
            "pids_current": 64,
        },
        "containers": containers,
        "aggregate_limits": {
            "cpu_cores": len(authority.ROLES) * policy["container_cpus"],
            "memory_bytes": (len(authority.ROLES) * policy["container_memory_mib"] * 1024**2),
            "pids": len(authority.ROLES) * policy["container_pids"],
            "gpu_count": 1 if pool == "gb10" else 0,
        },
        "device_probe": {
            "method": (
                "docker-nvidia-smi-and-device-denial-v1"
                if pool == "gb10"
                else "docker-no-device-exposure-v1"
            ),
            "allocated_ids": ["GPU-test"] if pool == "gb10" else [],
            "all_allocated_usable": True,
            "unallocated_denied": True,
            "allocated_probe_container_ids": (
                [containers[0]["container_id"]] if pool == "gb10" else []
            ),
            "denial_probe_container_ids": [
                item["container_id"] for item in (containers[1:] if pool == "gb10" else containers)
            ],
        },
    }


def _receipts(config: authority.Config) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    checkpoint_offsets = {
        "baseline": 0,
        "mixed_non_loom": 100,
        "cancel_cleanup": 15_000,
        "ttl_cleanup": 15_100,
        "submit_host_restart": 15_200,
        "worker_crash": 15_300,
        "final_drain": 15_400,
    }
    for sequence, checkpoint in enumerate(authority.CHECKPOINTS, start=1):
        nodes: dict[str, Any] = {}
        for index, node in enumerate(config.nodes):
            active: list[dict[str, Any]] = []
            if checkpoint == "mixed_non_loom":
                for sandbox_index, sandbox in enumerate(authority.SANDBOXES):
                    expected_oldlab = config.oldlab_nodes[sandbox_index]
                    expected_gb10 = config.gb10_nodes[sandbox_index]
                    if node == expected_oldlab:
                        active.append(
                            _job(
                                sandbox,
                                config.host_aliases[node],
                                str(100 + sandbox_index),
                            ),
                        )
                    if node == expected_gb10:
                        active.append(_job(sandbox, node, str(200 + sandbox_index)))
            terminal: list[dict[str, Any]] = []
            if checkpoint == "cancel_cleanup" and node == config.oldlab_nodes[0]:
                terminal.append(
                    {
                        "job_id": "301",
                        "job_name": f"loom-qianyi-{CANDIDATES['qianyi']['sha'][:12]}-cancel",
                        "state": "CANCELLED",
                        "node": config.host_aliases[node],
                        "sandbox": "qianyi",
                        "candidate_sha": CANDIDATES["qianyi"]["sha"],
                        "ended_at": _iso(14_990),
                        "elapsed_seconds": 60,
                    },
                )
            if checkpoint == "ttl_cleanup" and node == config.oldlab_nodes[0]:
                terminal.append(
                    {
                        "job_id": "303",
                        "job_name": f"loom-qianyi-{CANDIDATES['qianyi']['sha'][:12]}-ttl",
                        "state": "TIMEOUT",
                        "node": config.host_aliases[node],
                        "sandbox": "qianyi",
                        "candidate_sha": CANDIDATES["qianyi"]["sha"],
                        "ended_at": _iso(15_090),
                        "elapsed_seconds": 60,
                    },
                )
            if checkpoint == "submit_host_restart" and node == config.oldlab_nodes[0]:
                terminal.append(
                    {
                        "job_id": "304",
                        "job_name": f"loom-qianyi-{CANDIDATES['qianyi']['sha'][:12]}-restart",
                        "state": "COMPLETED",
                        "node": config.host_aliases[node],
                        "sandbox": "qianyi",
                        "candidate_sha": CANDIDATES["qianyi"]["sha"],
                        "ended_at": _iso(15_190),
                        "elapsed_seconds": 60,
                    },
                )
            if checkpoint == "worker_crash" and node == config.oldlab_nodes[1]:
                terminal.append(
                    {
                        "job_id": "302",
                        "job_name": (
                            f"loom-hongjian-{CANDIDATES['hongjian']['sha'][:12]}-crash"
                        ),
                        "state": "FAILED",
                        "node": config.host_aliases[node],
                        "sandbox": "hongjian",
                        "candidate_sha": CANDIDATES["hongjian"]["sha"],
                        "ended_at": _iso(15_290),
                        "elapsed_seconds": 60,
                    },
                )
            nodes[node] = {
                "schema_version": 1,
                "kind": "loom.developer-sandbox.platform-health-node-observation",
                "session_id": SESSION,
                "checkpoint": checkpoint,
                "checkpoint_group": authority.CHECKPOINT_GROUPS[checkpoint],
                "node": node,
                "host": config.host_aliases[node],
                "pool": "oldlab" if node.startswith("oldlab-") else "gb10",
                "observed_at": _iso(checkpoint_offsets[checkpoint]),
                "capacity": {
                    "cpu_cores_total": 12 if node in config.oldlab_nodes else 20,
                    "cpu_ticks_total": 10_000 + sequence * 100,
                    "cpu_ticks_idle": 8_000 + sequence * 70,
                    "memory_bytes_total": (
                        58 * 1024**3 if node in config.oldlab_nodes else 115000 * 1024**2
                    ),
                    "memory_bytes_available": 32 * 1024**3,
                },
                "io": {
                    "read_bytes_total": index * 1000 + sequence * 100,
                    "write_bytes_total": index * 2000 + sequence * 200,
                },
                "active_jobs": active,
                "terminal_jobs": terminal,
                "non_loom_slurm": {
                    "controller_healthy": True,
                    "running_job_count": 1,
                    "running_job_ids": ["900"],
                },
                "orphan_container_ids": [],
            }
        receipt: dict[str, Any] = {
            "schema_version": 1,
            "kind": "loom.developer-sandbox.platform-health-checkpoint",
            "session_id": SESSION,
            "sequence": sequence,
            "checkpoint": checkpoint,
            "checkpoint_group": authority.CHECKPOINT_GROUPS[checkpoint],
            "candidates": CANDIDATES,
            "collector_host": config.collector_host,
            "acceptance_checkpoint_times": {
                sandbox: _iso(checkpoint_offsets[checkpoint] - 2) for sandbox in authority.SANDBOXES
            },
            "collection_started_at": _iso(checkpoint_offsets[checkpoint] - 1),
            "observed_at": _iso(checkpoint_offsets[checkpoint]),
            "excluded_nodes": [],
            "nodes": nodes,
            "platform_health": _platform_health(),
        }
        receipt["payload_sha256"] = authority._digest(receipt)
        rows.append(receipt)
    return rows


def _samples(config: authority.Config, receipts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    previous: str | None = None
    trial_batches = [
        {
            "sandbox": sandbox,
            "pool": pool,
            "batch_id": f"{index + 100:08x}-0000-4000-8000-{index + 100:012x}",
            "candidate_sha": CANDIDATES[sandbox]["sha"],
            "candidate_tree": CANDIDATES[sandbox]["tree"],
            "phase_started_at": _iso(100),
            "phase_completed_at": _iso(300),
        }
        for index, (sandbox, pool) in enumerate(
            (
                ("qianyi", "oldlab"),
                ("qianyi", "gb10"),
                ("hongjian", "oldlab"),
                ("hongjian", "gb10"),
                ("devansh", "oldlab"),
                ("devansh", "gb10"),
            ),
            start=1,
        )
    ]
    terminal_trials = [
        {
            "trial_id": f"{index:08x}-0000-4000-8000-{index:012x}",
            "batch_id": next(
                row["batch_id"]
                for row in trial_batches
                if row["sandbox"] == sandbox and row["pool"] == pool
            ),
            "batch_created_at": _iso(150),
            "expected_trial_count": 1,
            "state": "succeeded",
            "attempt_count": 1,
            "retry_count": 0,
            "finished_at": _iso(200),
            "worker_id": f"{index + 200:08x}-0000-4000-8000-{index + 200:012x}",
            "slurm_job_id": str(100 + sandbox_index)
            if pool == "oldlab"
            else str(200 + sandbox_index),
            "sandbox": sandbox,
            "pool": pool,
            "candidate_sha": CANDIDATES[sandbox]["sha"],
        }
        for index, (sandbox_index, sandbox, pool) in enumerate(
            (
                (0, "qianyi", "oldlab"),
                (0, "qianyi", "gb10"),
                (1, "hongjian", "oldlab"),
                (1, "hongjian", "gb10"),
                (2, "devansh", "oldlab"),
                (2, "devansh", "gb10"),
            ),
            start=1,
        )
    ]
    for sequence in range(1, 121):
        observed_at = _iso(100 + (sequence - 1) * 122)
        nodes: dict[str, Any] = {}
        for node in config.nodes:
            source = deepcopy(receipts[1]["nodes"][node])
            source.update(
                {
                    "schema_version": 1,
                    "kind": "loom.developer-sandbox.platform-health-node-observation",
                    "session_id": SESSION,
                    "checkpoint": "mixed_non_loom",
                    "checkpoint_group": "during",
                    "node": node,
                    "host": config.host_aliases[node],
                    "pool": "oldlab" if node.startswith("oldlab-") else "gb10",
                    "observed_at": observed_at,
                    "orphan_container_ids": [],
                },
            )
            source["capacity"]["cpu_ticks_total"] += sequence * 100
            source["capacity"]["cpu_ticks_idle"] += sequence * 70
            source["io"]["read_bytes_total"] += sequence * 100
            source["io"]["write_bytes_total"] += sequence * 200
            nodes[node] = source
        row: dict[str, Any] = {
            "schema_version": 1,
            "kind": "loom.developer-sandbox.platform-health-soak-sample",
            "session_id": SESSION,
            "sequence": sequence,
            "previous_sha256": previous,
            "candidates": CANDIDATES,
            "collector_host": config.collector_host,
            "collection_started_at": observed_at,
            "observed_at": observed_at,
            "excluded_nodes": [],
            "nodes": nodes,
            "platform_health": _platform_health(),
            "trial_batches": deepcopy(trial_batches),
            "trial_database_authorities": [
                {
                    "sandbox": sandbox,
                    "candidate_sha": CANDIDATES[sandbox]["sha"],
                    "candidate_tree": CANDIDATES[sandbox]["tree"],
                    "compose_project": f"loom-sandbox-{sandbox}",
                    "container_id": f"{index + 900:064x}",
                    "compose_config_sha256": f"{index + 800:064x}",
                    "created_at": _iso(50),
                    "started_at": _iso(60),
                    "lifecycle_updated_at": _iso(70),
                    "desired_sha256": f"{index + 700:064x}",
                    "lifecycle_sha256": f"{index + 600:064x}",
                    "combined_receipt_sha256": f"{index + 500:064x}",
                }
                for index, sandbox in enumerate(authority.SANDBOXES, start=1)
            ],
            "trial_outcomes": deepcopy(terminal_trials) if sequence >= 2 else [],
        }
        row["payload_sha256"] = authority._digest(row)
        previous = row["payload_sha256"]
        rows.append(row)
    return rows


def _rehash_samples(samples: list[dict[str, Any]]) -> None:
    previous: str | None = None
    for sequence, sample in enumerate(samples, start=1):
        sample["sequence"] = sequence
        sample["previous_sha256"] = previous
        sample["payload_sha256"] = authority._digest(
            {key: value for key, value in sample.items() if key != "payload_sha256"},
        )
        previous = sample["payload_sha256"]


def test_checked_in_config_covers_all_infrastructure_and_capacity_nodes() -> None:
    config = authority.load_config(CONFIG)

    assert config.collector_host == "trt-eai-oldlab-2"
    assert config.namespace == "loom-staging"
    assert len(config.nodes) == 20
    assert config.oldlab_nodes == tuple(f"oldlab-{index}" for index in range(1, 6))
    assert config.gb10_nodes == tuple(f"trt-gb10-{index}" for index in range(1, 16))
    assert "trt-gb10-7" in config.nodes
    assert "trt-gb10-7" in config.capacity_gb10_nodes
    assert len(config.capacity_gb10_nodes) == 15
    assert tuple(config.host_aliases) == config.nodes
    assert config.host_aliases == authority.EXPECTED_HOST_ALIASES
    assert config.host_aliases["trt-gb10-7"] == "gx10-0faf"
    assert len(set(config.host_aliases.values())) == len(config.nodes)


def test_node_request_is_canonical_candidate_and_fixed_host_bound() -> None:
    config = authority.load_config(CONFIG)
    state = {"session_id": SESSION, "candidates": CANDIDATES}

    request = authority._node_request(
        config,
        state=state,
        checkpoint="mixed_non_loom",
        node="trt-gb10-1",
        since_at=_iso(0),
    )
    envelope = json.loads(
        authority._request_envelope(request, node="trt-gb10-1"),
    )

    assert request["expected_host"] == "gx10-01c7"
    assert request["expected_slurm_node"] == "trt-gb10-1"
    assert request["candidates"] == CANDIDATES
    assert envelope["action"] == "observe-platform-health-node"
    assert envelope["node"] == "trt-gb10-1"
    assert envelope["domain"] == "gb10"
    assert envelope["sandbox"] == "qianyi"
    assert envelope["candidate_sha"] == CANDIDATES["qianyi"]["sha"]
    assert envelope["payload_kind"] == "platform-health-node-json"

    oldlab = authority._node_request(
        config,
        state=state,
        checkpoint="mixed_non_loom",
        node="oldlab-1",
        since_at=_iso(0),
    )
    assert oldlab["expected_slurm_node"] == "trt-eai-oldlab-1"

    node_seven = authority._node_request(
        config,
        state=state,
        checkpoint="mixed_non_loom",
        node="trt-gb10-7",
        since_at=_iso(0),
    )
    node_seven_envelope = json.loads(
        authority._request_envelope(node_seven, node="trt-gb10-7"),
    )
    assert node_seven["expected_host"] == "gx10-0faf"
    assert node_seven["expected_slurm_node"] == "trt-gb10-7"
    assert node_seven_envelope["action"] == "observe-platform-health-node"
    assert node_seven_envelope["node"] == "trt-gb10-7"


@pytest.mark.parametrize(
    "attack",
    ("session", "schema", "digest", "recorded_at", "noncanonical"),
)
def test_soak_batch_manifest_rejects_replayed_or_noncanonical_checkpoint(
    attack: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = authority.load_config(CONFIG)
    state = {"session_id": SESSION, "candidates": CANDIDATES}

    def secure_json(path: Path, *, label: str) -> tuple[dict[str, Any], bytes]:
        sandbox = next(item for item in authority.SANDBOXES if item in path.name)
        index = authority.SANDBOXES.index(sandbox)
        payload = {
            "schema_version": 2,
            "session_id": SESSION,
            "sandbox": sandbox,
            "candidate_sha": CANDIDATES[sandbox]["sha"],
            "candidate_tree": CANDIDATES[sandbox]["tree"],
            "phase": "mixed_non_loom",
            "phase_started_at": _iso(100),
            "recorded_at": _iso(300),
            "status": "pass",
            "evidence_sha256": f"{index + 1:064x}",
            "trial_batches": {
                "oldlab": f"{index + 1:08x}-0000-4000-8000-{index + 1:012x}",
                "gb10": f"{index + 11:08x}-0000-4000-8000-{index + 11:012x}",
            },
        }
        if sandbox == "qianyi":
            if attack == "session":
                payload["session_id"] = "2" * 32
            elif attack == "schema":
                payload["schema_version"] = 1
            elif attack == "digest":
                payload["evidence_sha256"] = "bad"
            elif attack == "recorded_at":
                payload["recorded_at"] = _iso(50)
        raw = authority._canonical(payload)
        if sandbox == "qianyi" and attack == "noncanonical":
            raw = json.dumps(payload, indent=2).encode()
        return payload, raw

    monkeypatch.setattr(authority, "_secure_json", secure_json)

    with pytest.raises(authority.PlatformHealthError, match=r"manifest|window"):
        authority._soak_trial_batch_manifest(config, state)


def test_trial_outcome_readback_uses_each_fixed_sandbox_database_and_batch_manifest(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = authority.load_config(CONFIG)
    trial_batches = _samples(config, _receipts(config))[0]["trial_batches"]
    container_ids = {
        sandbox: f"{index:064x}"
        for index, sandbox in enumerate(authority.SANDBOXES, start=1)
    }
    database_authorities = {
        row["sandbox"]: row
        for row in _samples(config, _receipts(config))[0]["trial_database_authorities"]
    }
    sandbox_by_container = {value: key for key, value in container_ids.items()}
    monkeypatch.setattr(
        authority,
        "_sandbox_postgres_container",
        lambda sandbox, _candidate, *, run: (
            container_ids[sandbox],
            database_authorities[sandbox],
        ),
    )
    calls: list[tuple[str, ...]] = []

    def run(argv: tuple[str, ...], **_kwargs: object) -> subprocess.CompletedProcess[bytes]:
        calls.append(argv)
        sandbox = sandbox_by_container[argv[2]]
        sandbox_index = authority.SANDBOXES.index(sandbox)
        row = {
            "trial_id": f"{sandbox_index + 1:08x}-0000-4000-8000-{sandbox_index + 1:012x}",
            "batch_id": next(
                item["batch_id"]
                for item in trial_batches
                if item["sandbox"] == sandbox and item["pool"] == "oldlab"
            ),
            "batch_created_at": _iso(150),
            "expected_trial_count": 1,
            "state": "succeeded",
            "attempt_count": 2,
            "retry_count": 1,
            "finished_at": _iso(200),
            "worker_id": (
                f"{sandbox_index + 20:08x}-0000-4000-8000-{sandbox_index + 20:012x}"
            ),
            "slurm_job_id": str(100 + sandbox_index),
            "sandbox": sandbox,
            "pool": "oldlab",
            "candidate_sha": CANDIDATES[sandbox]["sha"],
        }
        return subprocess.CompletedProcess(argv, 0, json.dumps(row).encode() + b"\n", b"")

    outcomes, observed_authorities = authority._trial_outcomes(
        CANDIDATES,
        trial_batches,
        run=run,
    )

    assert len(outcomes) == 3
    assert {row["sandbox"] for row in outcomes} == set(authority.SANDBOXES)
    assert observed_authorities == [
        database_authorities[sandbox] for sandbox in authority.SANDBOXES
    ]
    assert all(call[:2] == ("/usr/bin/docker", "exec") for call in calls)
    assert all("LEFT JOIN workers" in call[-1] for call in calls)
    assert all("LEFT JOIN slurm_worker_jobs" in call[-1] for call in calls)
    assert not any("kubectl" in argument for call in calls for argument in call)


def test_sandbox_postgres_container_is_candidate_receipt_and_compose_label_bound(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    container_id = "a" * 64
    verified: list[tuple[str, str, str]] = []
    profile = next(
        item for item in authority.load_profiles() if item.sandbox == "qianyi"
    )
    receipt = SimpleNamespace(payload_sha256="b" * 64)
    monkeypatch.setattr(
        authority,
        "verify_combined_receipt",
        lambda profile, sha, tree: (
            verified.append((profile.sandbox, sha, tree)),
            receipt,
        )[1],
    )
    monkeypatch.setattr(
        authority,
        "_load_host_json",
        lambda path, _label: (
            {"desired": True}
            if path == profile.desired_file
            else {
                "schema_version": 1,
                "sandbox": "qianyi",
                "compose_project": "loom-sandbox-qianyi",
                "candidate_sha": CANDIDATES["qianyi"]["sha"],
                "candidate_tree": CANDIDATES["qianyi"]["tree"],
                "source_repo": str(
                    profile.candidate_root / CANDIDATES["qianyi"]["sha"],
                ),
                "updated_at": _iso(90),
            }
        ),
    )
    monkeypatch.setattr(authority, "_validate_desired_binding", lambda *_args, **_kwargs: None)

    def run(argv: tuple[str, ...], **_kwargs: object) -> subprocess.CompletedProcess[bytes]:
        if argv[1] == "ps":
            return subprocess.CompletedProcess(argv, 0, f"{container_id}\n".encode(), b"")
        assert argv[1:] == ("inspect", container_id)
        payload = [
            {
                "Config": {
                    "Labels": {
                        "com.docker.compose.project": "loom-sandbox-qianyi",
                        "com.docker.compose.service": "postgres",
                        "com.docker.compose.project.working_dir": str(
                            profile.candidate_root
                            / CANDIDATES["qianyi"]["sha"]
                            / "deploy",
                        ),
                        "com.docker.compose.project.config_files": str(
                            profile.candidate_root
                            / CANDIDATES["qianyi"]["sha"]
                            / "deploy/docker-compose.dev.yml",
                        ),
                        "com.docker.compose.config-hash": "c" * 64,
                    },
                },
                "Id": container_id,
                "Created": _iso(50),
                "State": {
                    "Running": True,
                    "Health": {"Status": "healthy"},
                    "StartedAt": _iso(60),
                },
            },
        ]
        return subprocess.CompletedProcess(argv, 0, json.dumps(payload).encode(), b"")

    observed, database_authority = authority._sandbox_postgres_container(
        "qianyi",
        CANDIDATES["qianyi"],
        run=run,
    )

    assert observed == container_id
    assert database_authority["container_id"] == container_id
    assert database_authority["lifecycle_updated_at"] == _iso(90)
    assert verified == [
        ("qianyi", CANDIDATES["qianyi"]["sha"], CANDIDATES["qianyi"]["tree"]),
    ]


def test_sandbox_postgres_rejects_candidate_root_as_compose_working_dir(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    container_id = "a" * 64
    profile = next(
        item for item in authority.load_profiles() if item.sandbox == "qianyi"
    )
    monkeypatch.setattr(
        authority,
        "verify_combined_receipt",
        lambda *_args, **_kwargs: SimpleNamespace(payload_sha256="b" * 64),
    )
    monkeypatch.setattr(authority, "_validate_desired_binding", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        authority,
        "_load_host_json",
        lambda path, _label: (
            {"desired": True}
            if path == profile.desired_file
            else {
                "schema_version": 1,
                "sandbox": "qianyi",
                "compose_project": "loom-sandbox-qianyi",
                "candidate_sha": CANDIDATES["qianyi"]["sha"],
                "candidate_tree": CANDIDATES["qianyi"]["tree"],
                "source_repo": str(
                    profile.candidate_root / CANDIDATES["qianyi"]["sha"],
                ),
                "updated_at": _iso(90),
            }
        ),
    )

    def run(argv: tuple[str, ...], **_kwargs: object) -> subprocess.CompletedProcess[bytes]:
        if argv[1] == "ps":
            return subprocess.CompletedProcess(argv, 0, f"{container_id}\n".encode(), b"")
        payload = [
            {
                "Config": {
                    "Labels": {
                        "com.docker.compose.project": "loom-sandbox-qianyi",
                        "com.docker.compose.service": "postgres",
                        "com.docker.compose.project.working_dir": str(
                            profile.candidate_root / CANDIDATES["qianyi"]["sha"],
                        ),
                        "com.docker.compose.project.config_files": str(
                            profile.candidate_root
                            / CANDIDATES["qianyi"]["sha"]
                            / "deploy/docker-compose.dev.yml",
                        ),
                        "com.docker.compose.config-hash": "c" * 64,
                    },
                },
                "Id": container_id,
                "Created": _iso(50),
                "State": {
                    "Running": True,
                    "Health": {"Status": "healthy"},
                    "StartedAt": _iso(60),
                },
            },
        ]
        return subprocess.CompletedProcess(argv, 0, json.dumps(payload).encode(), b"")

    with pytest.raises(authority.PlatformHealthError, match="container binding"):
        authority._sandbox_postgres_container(
            "qianyi",
            CANDIDATES["qianyi"],
            run=run,
        )


def test_sandbox_postgres_rejects_f_receipt_with_h_active_lifecycle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = next(
        item for item in authority.load_profiles() if item.sandbox == "qianyi"
    )
    monkeypatch.setattr(
        authority,
        "verify_combined_receipt",
        lambda *_args, **_kwargs: SimpleNamespace(payload_sha256="b" * 64),
    )
    monkeypatch.setattr(authority, "_validate_desired_binding", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        authority,
        "_load_host_json",
        lambda path, _label: (
            {"desired": True}
            if path == profile.desired_file
            else {
                "schema_version": 1,
                "sandbox": "qianyi",
                "compose_project": "loom-sandbox-qianyi",
                "candidate_sha": "f" * 40,
                "candidate_tree": "e" * 40,
                "source_repo": str(profile.candidate_root / ("f" * 40)),
                "updated_at": _iso(90),
            }
        ),
    )

    with pytest.raises(authority.PlatformHealthError, match="lifecycle candidate"):
        authority._sandbox_postgres_container(
            "qianyi",
            CANDIDATES["qianyi"],
            run=lambda *_args, **_kwargs: pytest.fail("Docker must not run"),
        )


def test_capacity_node_seven_allows_noninvasive_health_observation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = authority.load_config(CONFIG)
    request = authority._node_request(
        config,
        state={"session_id": SESSION, "candidates": CANDIDATES},
        checkpoint="baseline",
        node="trt-gb10-7",
        since_at=_iso(0),
    )
    monkeypatch.setattr(authority, "ROOT_UID", os.getuid())
    monkeypatch.setattr(authority, "_parse_meminfo", lambda _raw: (128 * 1024**3, 64 * 1024**3))
    monkeypatch.setattr(authority, "_parse_cpu_stat", lambda _raw: (10_000, 8_000))
    monkeypatch.setattr(authority, "_parse_diskstats", lambda _raw: (1_000, 2_000))
    monkeypatch.setattr(Path, "read_text", lambda *_args, **_kwargs: "")
    monkeypatch.setattr(os, "cpu_count", lambda: 20)
    monkeypatch.setattr(
        authority,
        "_container_observations",
        lambda *_args, **_kwargs: ([], []),
    )
    monkeypatch.setattr(authority, "_terminal_jobs", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(
        authority,
        "_non_loom_slurm_health",
        lambda *_args, **_kwargs: {
            "controller_healthy": True,
            "running_job_count": 1,
            "running_job_ids": ["900"],
        },
    )

    result = authority.observe_node(
        authority._canonical(request),
        clock=lambda: datetime(2026, 7, 29, tzinfo=UTC),
        hostname=lambda: "gx10-0faf",
    )

    assert result["node"] == "trt-gb10-7"
    assert result["host"] == "gx10-0faf"
    assert result["pool"] == "gb10"
    assert result["active_jobs"] == []
    assert result["terminal_jobs"] == []


def test_complete_receipts_produce_reusable_trusted_evidence() -> None:
    config = authority.load_config(CONFIG)
    receipts = _receipts(config)

    final = authority._verify_checkpoints(
        config,
        receipts,
        require_complete=True,
        samples=_samples(config, receipts),
    )

    assert final is not None
    assert all(receipt["excluded_nodes"] == [] for receipt in receipts)
    assert all("trt-gb10-7" in receipt["nodes"] for receipt in receipts)
    assert final["session_id"] == SESSION
    assert final["candidates"] == CANDIDATES
    assert len(final["mixed_jobs"]) == 6
    assert final["zero_orphans"] is True
    assert final["gate6_observations"]["soak"]["sample_count"] == 120
    assert final["gate6_observations"]["soak"]["duration_seconds"] >= 14_400
    assert final["gate6_observations"]["soak"]["trial_success_numerator"] == 6
    assert final["gate6_observations"]["soak"]["trial_success_denominator"] == 6
    assert final["gate6_observations"]["soak"]["trial_success_ratio"] == 1.0
    assert len(final["gate6_observations"]["soak"]["trial_outcomes"]) == 6
    assert len(final["gate6_observations"]["soak"]["pair_headroom"]) == 6
    assert len(final["gate6_observations"]["device_isolation"]) == 6
    assert {row["event"] for row in final["gate6_observations"]["cleanup"]} == set(
        authority.GATE6_CLEANUP_EVENTS,
    )
    assert len(final["node_intervals"]) == 20
    assert "trt-gb10-7" in final["node_intervals"]
    assert final["policy_capacity"]["oldlab"]["max_slots"] == 20
    assert final["policy_capacity"]["oldlab"]["job_pids_max"] == 32768
    assert final["policy_capacity"]["gb10"]["max_slots"] == 120
    assert final["policy_capacity"]["gb10"]["reserved_cpu_cores_per_node"] == 4
    assert final["oldlab_capacity_recommendation"]["values"] == final["policy_capacity"]["oldlab"]
    assert authority.DIGEST_RE.fullmatch(
        final["oldlab_capacity_recommendation"]["source_sha256"],
    )
    assert (
        authority._timestamp(
            final["expires_at"],
            label="test expiry",
        )
        - authority._timestamp(
            final["completed_at"],
            label="test completion",
        )
        == authority.PLATFORM_HEALTH_EVIDENCE_TTL
    )
    assert authority.DIGEST_RE.fullmatch(final["payload_sha256"])


def test_final_authority_matches_live_acceptance_platform_schema() -> None:
    config = authority.load_config(CONFIG)
    receipts = _receipts(config)
    final = authority._verify_checkpoints(
        config,
        receipts,
        require_complete=True,
        samples=_samples(config, receipts),
    )
    assert final is not None
    schema = json.loads(LIVE_SCHEMA.read_text(encoding="utf-8"))
    validator = Draft202012Validator(schema).evolve(
        schema=schema["$defs"]["platformHealthAuthority"],
    )

    assert list(validator.iter_errors(final)) == []


def test_four_hour_soak_allows_policy_bound_job_rotation() -> None:
    config = authority.load_config(CONFIG)
    receipts = _receipts(config)
    samples = _samples(config, receipts)
    rotated = _job("qianyi", config.host_aliases[config.oldlab_nodes[0]], "777")
    samples[60]["nodes"][config.oldlab_nodes[0]]["active_jobs"] = [rotated]
    _rehash_samples(samples)

    soak = authority._verify_soak_samples(config, samples)

    assert soak["duration_seconds"] >= authority.SOAK_REQUIRED_DURATION_SECONDS
    assert soak["sample_count"] == authority.SOAK_REQUIRED_SAMPLE_COUNT
    assert (
        next(
            row
            for row in soak["pair_headroom"]
            if row["sandbox"] == "qianyi" and row["pool"] == "oldlab"
        )["observed_peak_concurrency"]
        == 1
    )


def test_four_hour_soak_rejects_trial_success_ratio_below_policy() -> None:
    config = authority.load_config(CONFIG)
    receipts = _receipts(config)
    samples = _samples(config, receipts)
    for sample in samples[1:]:
        sample["trial_outcomes"][0]["state"] = "failed"
    _rehash_samples(samples)

    with pytest.raises(authority.PlatformHealthError, match="success ratio is below policy"):
        authority._verify_soak_samples(config, samples)


def test_four_hour_soak_rejects_trial_outcome_without_observed_candidate_job() -> None:
    config = authority.load_config(CONFIG)
    receipts = _receipts(config)
    samples = _samples(config, receipts)
    for sample in samples[1:]:
        sample["trial_outcomes"][0]["slurm_job_id"] = "999"
    _rehash_samples(samples)

    with pytest.raises(authority.PlatformHealthError, match="observed candidate job"):
        authority._verify_soak_samples(config, samples)


def test_four_hour_soak_rejects_zero_trial_denominator() -> None:
    config = authority.load_config(CONFIG)
    receipts = _receipts(config)
    samples = _samples(config, receipts)
    for sample in samples:
        sample["trial_outcomes"] = []
    _rehash_samples(samples)

    with pytest.raises(authority.PlatformHealthError, match="census is incomplete"):
        authority._verify_soak_samples(config, samples)


def test_four_hour_soak_rejects_unattributed_terminal_trial_from_fixed_batch() -> None:
    config = authority.load_config(CONFIG)
    receipts = _receipts(config)
    samples = _samples(config, receipts)
    for sample in samples[1:]:
        sample["trial_outcomes"][0]["worker_id"] = None
        sample["trial_outcomes"][0]["slurm_job_id"] = None
    _rehash_samples(samples)

    with pytest.raises(authority.PlatformHealthError, match="outcome binding is invalid"):
        authority._verify_soak_samples(config, samples)


def test_four_hour_soak_rejects_running_trial_even_with_successes() -> None:
    config = authority.load_config(CONFIG)
    receipts = _receipts(config)
    samples = _samples(config, receipts)
    running = deepcopy(samples[1]["trial_outcomes"][0])
    running.update(
        {
            "trial_id": "00000099-0000-4000-8000-000000000099",
            "state": "running",
            "finished_at": None,
            "expected_trial_count": 2,
        },
    )
    for sample in samples[1:]:
        sample["trial_outcomes"][0]["expected_trial_count"] = 2
        sample["trial_outcomes"].append(deepcopy(running))
    _rehash_samples(samples)

    with pytest.raises(authority.PlatformHealthError, match="not terminal-complete"):
        authority._verify_soak_samples(config, samples)


def test_early_finished_failed_trial_is_included_in_fixed_batch_denominator() -> None:
    config = authority.load_config(CONFIG)
    receipts = _receipts(config)
    samples = _samples(config, receipts)
    early_failed = deepcopy(samples[1]["trial_outcomes"][0])
    early_failed.update(
        {
            "trial_id": "00000098-0000-4000-8000-000000000098",
            "state": "failed",
            "batch_created_at": _iso(50),
            "finished_at": _iso(75),
        },
    )
    for sample in samples:
        for manifest in sample["trial_batches"]:
            if manifest["batch_id"] == early_failed["batch_id"]:
                manifest["phase_started_at"] = _iso(0)
        for outcome in sample["trial_outcomes"]:
            if outcome["batch_id"] == early_failed["batch_id"]:
                outcome["batch_created_at"] = _iso(50)
                outcome["expected_trial_count"] = 2
        early_failed["expected_trial_count"] = 2
        sample["trial_outcomes"].append(deepcopy(early_failed))
    _rehash_samples(samples)

    with pytest.raises(authority.PlatformHealthError, match="success ratio is below policy"):
        authority._verify_soak_samples(config, samples)


def test_four_hour_soak_rejects_partial_fixed_batch_fanout() -> None:
    config = authority.load_config(CONFIG)
    receipts = _receipts(config)
    samples = _samples(config, receipts)
    for sample in samples[1:]:
        sample["trial_outcomes"][0]["expected_trial_count"] = 10
    _rehash_samples(samples)

    with pytest.raises(authority.PlatformHealthError, match="census is incomplete"):
        authority._verify_soak_samples(config, samples)


def test_four_hour_soak_preserves_terminal_trial_retry_attribution() -> None:
    config = authority.load_config(CONFIG)
    receipts = _receipts(config)
    samples = _samples(config, receipts)
    for sample in samples[1:]:
        sample["trial_outcomes"][0]["attempt_count"] = 2
        sample["trial_outcomes"][0]["retry_count"] = 1
    _rehash_samples(samples)

    soak = authority._verify_soak_samples(config, samples)
    qianyi_oldlab = next(
        row
        for row in soak["trial_outcomes"]
        if row["sandbox"] == "qianyi" and row["pool"] == "oldlab"
    )

    assert qianyi_oldlab["retried_trial_count"] == 1
    assert qianyi_oldlab["retry_attempt_count"] == 1


@pytest.mark.parametrize(
    "attack",
    [
        "missing_node",
        "replay",
        "foreign_job",
        "weak_headroom",
        "weak_free_cpu",
        "missing_cancel",
        "orphan",
        "forged_parent",
        "compose_reuse",
        "network_reuse",
        "extra_capacity_job",
        "missing_ttl",
        "missing_restart",
        "slow_cleanup",
        "missing_node7_soak",
        "short_soak",
        "forged_gpu_proof",
        "forged_device_container_id",
    ],
)
def test_trusted_evidence_attacks_fail_closed(attack: str) -> None:
    config = authority.load_config(CONFIG)
    receipts = _receipts(config)
    samples = _samples(config, receipts)
    if attack == "missing_node":
        del receipts[1]["nodes"]["trt-gb10-15"]
    elif attack == "replay":
        receipts[2]["observed_at"] = receipts[1]["observed_at"]
    elif attack == "foreign_job":
        receipts[1]["nodes"][config.oldlab_nodes[0]]["active_jobs"][0]["sandbox"] = "foreign"
    elif attack == "weak_headroom":
        receipts[1]["nodes"][config.oldlab_nodes[0]]["capacity"]["memory_bytes_available"] = 1
    elif attack == "weak_free_cpu":
        baseline = receipts[0]["nodes"][config.oldlab_nodes[0]]["capacity"]
        receipts[1]["nodes"][config.oldlab_nodes[0]]["capacity"]["cpu_ticks_idle"] = (
            baseline["cpu_ticks_idle"] + 25
        )
    elif attack == "missing_cancel":
        receipts[2]["nodes"][config.oldlab_nodes[0]]["terminal_jobs"] = []
    elif attack == "orphan":
        receipts[-1]["nodes"][config.oldlab_nodes[0]]["active_jobs"] = [
            _job("qianyi", config.host_aliases[config.oldlab_nodes[0]], "999"),
        ]
    elif attack == "forged_parent":
        job = receipts[1]["nodes"][config.oldlab_nodes[0]]["active_jobs"][0]
        job["cgroup"]["job_path"] = "/system.slice/slurmstepd.scope/job_999"
    elif attack == "compose_reuse":
        first = receipts[1]["nodes"][config.oldlab_nodes[0]]["active_jobs"][0]
        second = receipts[1]["nodes"][config.oldlab_nodes[1]]["active_jobs"][0]
        second["compose_project"] = first["compose_project"]
    elif attack == "network_reuse":
        first = receipts[1]["nodes"][config.oldlab_nodes[0]]["active_jobs"][0]
        second = receipts[1]["nodes"][config.oldlab_nodes[1]]["active_jobs"][0]
        second["compose_networks"] = first["compose_networks"]
    elif attack == "extra_capacity_job":
        receipts[1]["nodes"]["trt-gb10-7"]["active_jobs"] = [
            _job("qianyi", "trt-gb10-7", "999"),
        ]
    elif attack == "missing_ttl":
        receipts[3]["nodes"][config.oldlab_nodes[0]]["terminal_jobs"] = []
    elif attack == "missing_restart":
        receipts[4]["nodes"][config.oldlab_nodes[0]]["terminal_jobs"] = []
    elif attack == "slow_cleanup":
        receipts[2]["nodes"][config.oldlab_nodes[0]]["terminal_jobs"][0]["ended_at"] = _iso(
            10_000,
        )
    elif attack == "missing_node7_soak":
        del samples[0]["nodes"]["trt-gb10-7"]
    elif attack == "short_soak":
        samples = samples[:-1]
    elif attack == "forged_gpu_proof":
        receipts[1]["nodes"][config.gb10_nodes[0]]["active_jobs"][0]["device_probe"][
            "all_allocated_usable"
        ] = False
    else:
        receipts[1]["nodes"][config.gb10_nodes[0]]["active_jobs"][0]["device_probe"][
            "allocated_probe_container_ids"
        ] = ["f" * 64]

    with pytest.raises(authority.PlatformHealthError):
        authority._verify_checkpoints(
            config,
            receipts,
            require_complete=True,
            samples=samples,
        )


@pytest.mark.parametrize("attack", ["missing", "extra", "duplicate", "drift"])
def test_checkpoint_candidate_mapping_is_closed_and_monotonic(attack: str) -> None:
    config = authority.load_config(CONFIG)
    receipts = _receipts(config)
    candidates = json.loads(json.dumps(receipts[2]["candidates"]))
    receipts[2]["candidates"] = candidates
    if attack == "missing":
        del candidates["devansh"]
    elif attack == "extra":
        candidates["foreign"] = {"sha": "d" * 40, "tree": "4" * 40}
    elif attack == "duplicate":
        candidates["devansh"] = dict(candidates["qianyi"])
    else:
        candidates["devansh"] = {"sha": "d" * 40, "tree": "4" * 40}

    with pytest.raises(authority.PlatformHealthError, match="checkpoint receipt"):
        authority._verify_checkpoints(config, receipts, require_complete=True)


def test_container_set_requires_exact_roles_cgroup_and_positive_limits(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    policy = authority._load_capacity_policy("oldlab")["values"]
    containers = []
    for index, role in enumerate(authority.ROLES, start=1):
        compose_project = "loom-qianyi-123"
        labels = {
            "loom.sandbox": "qianyi",
            "loom.candidate_sha": CANDIDATES["qianyi"]["sha"],
            "loom.slurm_job_id": "123",
            "loom.compose_project": compose_project,
            "com.docker.compose.project": compose_project,
        }
        if role == "worker":
            labels["com.docker.compose.service"] = "worker"
        elif role == "sidecar":
            labels["com.docker.compose.service"] = "sandbox-link"
        elif role == "verifier":
            labels["loom.task_sidecar"] = "verifier"
        else:
            labels["loom.trial_id"] = "trial-1"
        containers.append(
            {
                "Id": f"{index:064x}",
                "Name": f"/{compose_project}-{role}",
                "Config": {"Labels": labels},
                "HostConfig": {
                    "CgroupParent": "/slurm/job_123",
                    "NanoCpus": policy["container_cpus"] * 1_000_000_000,
                    "Memory": policy["container_memory_mib"] * 1024**2,
                    "PidsLimit": policy["container_pids"],
                    "DeviceRequests": [],
                },
                "State": {"Status": "running", "Pid": 1000 + index},
                "NetworkSettings": {"Networks": {f"{compose_project}_default": {}}},
            },
        )
    monkeypatch.setattr(
        authority,
        "_docker_container_ids",
        lambda *_args, **_kwargs: tuple(item["Id"] for item in containers),
    )
    monkeypatch.setattr(authority, "_proc_cgroup", lambda pid: f"/slurm/job_123/docker/{pid}")
    monkeypatch.setattr(
        authority,
        "_json_command",
        lambda argv, **_kwargs: [
            next(item for item in containers if item["Id"] == argv[-1]),
        ],
    )
    monkeypatch.setattr(
        authority,
        "_job_readback",
        lambda **_kwargs: {
            "job_id": "123",
            "job_name": "loom-qianyi-aaaaaaaaaaaa-node",
            "sandbox": "qianyi",
            "candidate_sha": CANDIDATES["qianyi"]["sha"],
            "account": "loom-dev-qianyi",
            "user": "loom-sandbox-qianyi",
            "node": "oldlab-1",
            "state": "RUNNING",
            "allocation": {
                "cpu_cores": policy["requested_cpus"],
                "memory_bytes": policy["requested_memory_mib"] * 1024**2,
                "pids": policy["job_pids_max"],
                "gpu_count": 0,
                "tres": "cpu=8,mem=32000M",
                "exclusive": False,
            },
        },
    )
    monkeypatch.setattr(
        authority,
        "_slurm_job_pid_cgroups",
        lambda *_args, **_kwargs: ("/slurm/job_123/step_batch",),
    )
    monkeypatch.setattr(
        authority,
        "_read_cgroup_limit",
        lambda _path, name: {
            "cgroup.controllers": "cpu memory pids",
            "cgroup.subtree_control": "cpu memory pids",
            "cpu.max": f"{policy['requested_cpus'] * 100000} 100000",
            "memory.max": str(policy["requested_memory_mib"] * 1024**2),
            "pids.max": str(policy["job_pids_max"]),
            "pids.current": "64",
        }[name],
    )
    monkeypatch.setattr(authority, "_probe_command", lambda *_args, **_kwargs: (True, ""))

    jobs, orphans = authority._container_observations(
        CANDIDATES,
        expected_node="oldlab-1",
        expected_host="trt-eai-oldlab-1",
        checkpoint="mixed_non_loom",
        policy=policy,
        run=lambda *_args, **_kwargs: None,  # type: ignore[arg-type,return-value]
    )

    assert orphans == []
    assert len(jobs) == 1
    assert [item["role"] for item in jobs[0]["containers"]] == sorted(authority.ROLES)

    containers[0]["HostConfig"]["Memory"] = 0
    with pytest.raises(authority.PlatformHealthError, match="limits are not finite"):
        authority._container_observations(
            CANDIDATES,
            expected_node="oldlab-1",
            expected_host="trt-eai-oldlab-1",
            checkpoint="mixed_non_loom",
            policy=policy,
            run=lambda *_args, **_kwargs: None,  # type: ignore[arg-type,return-value]
        )


@pytest.mark.parametrize(
    ("stdout", "message"),
    [
        ("PID JOBID STEPID LOCALID GLOBALID\n", "PID set"),
        ("PID JOBID STEPID LOCALID GLOBALID\n123 77 batch 0\n", "malformed"),
        ("PID JOBID STEPID LOCALID GLOBALID\n123 78 batch 0 0\n", "identity"),
    ],
)
def test_slurm_listpids_fails_closed(
    stdout: str,
    message: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def run(argv: tuple[str, ...], **_kwargs: Any) -> subprocess.CompletedProcess[bytes]:
        return subprocess.CompletedProcess(argv, 0, stdout=stdout.encode(), stderr=b"")

    monkeypatch.setattr(authority, "_proc_cgroup", lambda _pid: "/slurm/job_77/step_batch")
    with pytest.raises(authority.PlatformHealthError, match=message):
        authority._slurm_job_pid_cgroups("77", run=run)


def test_capacity_policy_source_is_closed_and_digest_bound(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = Path("deploy/developer-sandboxes/shared-capacity-policies/oldlab.toml")
    copied = tmp_path / authority.CAPACITY_POLICY_SOURCES["oldlab"]
    copied.parent.mkdir(parents=True)
    copied.write_bytes(source.read_bytes())
    monkeypatch.setattr(authority, "CAPACITY_SOURCE_ROOT", tmp_path)

    loaded = authority._load_capacity_policy("oldlab")
    assert loaded["source_sha256"] == hashlib.sha256(copied.read_bytes()).hexdigest()

    copied.write_text(
        source.read_text(encoding="utf-8") + "\nunexpected_policy_drift = true\n",
        encoding="utf-8",
    )
    with pytest.raises(authority.PlatformHealthError, match="invalid"):
        authority._load_capacity_policy("oldlab")


def test_recovery_rejects_foreign_journal_without_overwrite(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = authority.load_config(CONFIG)
    config = authority.Config(
        **{
            **{field: getattr(config, field) for field in config.__dataclass_fields__},
            "authority_state_root": tmp_path / "authority",
        },
    )
    monkeypatch.setattr(authority, "ROOT_UID", os.getuid())
    monkeypatch.setattr(authority, "ROOT_GID", os.getgid())
    session_root = config.authority_state_root / "sessions" / SESSION
    for path in (
        config.authority_state_root,
        config.authority_state_root / "sessions",
        session_root,
    ):
        path.mkdir(mode=0o700, exist_ok=True)
    (session_root / "receipts").mkdir(mode=0o700)
    foreign = {
        "schema_version": 1,
        "kind": "foreign",
        "session_id": SESSION,
    }
    (session_root / "journal.json").write_bytes(authority._canonical(foreign))
    (session_root / "journal.json").chmod(0o600)
    monkeypatch.setattr(
        authority,
        "_secure_json",
        lambda path, **_kwargs: (
            json.loads(path.read_bytes()),
            path.read_bytes(),
        ),
    )

    with pytest.raises(authority.PlatformHealthError, match="recovery binding"):
        authority._recover_transaction(config, session_root, SESSION)

    assert json.loads((session_root / "journal.json").read_bytes()) == foreign


def test_soak_sample_transaction_recovers_prepared_root_owned_write(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    checked_in = authority.load_config(CONFIG)
    config = authority.Config(
        **{
            **{field: getattr(checked_in, field) for field in checked_in.__dataclass_fields__},
            "authority_state_root": tmp_path / "authority",
        },
    )
    monkeypatch.setattr(authority, "ROOT_UID", os.getuid())
    monkeypatch.setattr(authority, "ROOT_GID", os.getgid())
    monkeypatch.setattr(
        authority,
        "_secure_json",
        lambda path, **_kwargs: (json.loads(path.read_bytes()), path.read_bytes()),
    )
    session_root = config.authority_state_root / "sessions" / SESSION
    for path in (
        config.authority_state_root,
        config.authority_state_root / "sessions",
        session_root,
        session_root / "samples",
    ):
        path.mkdir(mode=0o700, exist_ok=True)
    sample = _samples(config, _receipts(config))[0]
    destination = session_root / "samples" / "0001.json"
    journal = {
        "schema_version": 1,
        "kind": "loom.developer-sandbox.platform-health-soak-transaction",
        "session_id": SESSION,
        "sequence": 1,
        "sample_path": str(destination),
        "sample_sha256": sample["payload_sha256"],
        "sample": sample,
        "phase": "prepared",
    }
    journal_path = session_root / "sample-journal.json"
    journal_path.write_bytes(authority._canonical(journal))
    journal_path.chmod(0o600)

    authority._recover_sample_transaction(config, session_root, SESSION, CANDIDATES)

    assert json.loads(destination.read_bytes()) == sample
    assert json.loads(journal_path.read_bytes())["phase"] == "committed"
    assert (destination.stat().st_mode & 0o777) == 0o600


def test_deploy_assets_have_fixed_root_only_surface() -> None:
    service = Path(
        "deploy/developer-sandboxes/loom-developer-sandbox-platform-health-authority.service",
    ).read_text()
    sudoers = Path(
        "deploy/developer-sandboxes/loom-developer-sandbox-platform-health-authority.sudoers",
    ).read_text()

    assert "User=root" in service
    assert "ProtectSystem=strict" in service
    assert "verify-current" in service
    assert " NOSETENV:" not in sudoers
    assert "NOPASSWD:NOSETENV:" in sudoers
    assert " sample --session-id [0-9a-f]* --execute" in sudoers
    assert "--checkpoint ttl_cleanup --execute" in sudoers
    assert "--checkpoint submit_host_restart --execute" in sudoers
    assert "--checkpoint final_drain --execute" in sudoers
