from __future__ import annotations

import json
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from scripts.ops import developer_sandbox_platform_health_authority as authority

CONFIG = Path("deploy/developer-sandboxes/platform-health-authority.toml")
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


def _job(sandbox: str, node: str, job_id: str) -> dict[str, Any]:
    return {
        "job_id": job_id,
        "job_name": f"loom-{sandbox}-{CANDIDATES[sandbox]['sha'][:12]}-{node}",
        "sandbox": sandbox,
        "candidate_sha": CANDIDATES[sandbox]["sha"],
        "account": f"loom-dev-{sandbox}",
        "user": f"loom-sandbox-{sandbox}",
        "node": node,
        "state": "RUNNING",
        "allocation": {
            "cpu_cores": 8,
            "memory_bytes": 16 * 1024**3,
            "pids": 4096,
            "gpu_count": 1 if node.startswith("trt-gb10") else 0,
            "tres": "cpu=8,mem=16G",
            "exclusive": False,
        },
        "compose_project": f"loom-{sandbox}-{job_id}",
        "cgroup": {
            "job_path": f"/slurm/job_{job_id}",
            "controllers": ["cpu", "memory", "pids"],
            "cpu_cores_max": 8,
            "memory_bytes_max": 16 * 1024**3,
            "pids_max": 4096,
        },
        "containers": [],
        "aggregate_limits": {
            "cpu_cores": 4,
            "memory_bytes": 8 * 1024**3,
            "pids": 1024,
            "gpu_count": 1 if node.startswith("trt-gb10") else 0,
        },
    }


def _receipts(config: authority.Config) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for sequence, checkpoint in enumerate(authority.CHECKPOINTS, start=1):
        nodes: dict[str, Any] = {}
        for index, node in enumerate(config.nodes):
            active: list[dict[str, Any]] = []
            if checkpoint == "mixed_non_loom":
                for sandbox_index, sandbox in enumerate(authority.SANDBOXES):
                    expected_oldlab = config.oldlab_nodes[sandbox_index]
                    expected_gb10 = config.gb10_nodes[sandbox_index]
                    if node == expected_oldlab:
                        active.append(_job(sandbox, node, str(100 + sandbox_index)))
                    if node == expected_gb10:
                        active.append(_job(sandbox, node, str(200 + sandbox_index)))
            terminal: list[dict[str, str]] = []
            if checkpoint == "cancel_cleanup" and node == config.oldlab_nodes[0]:
                terminal.append(
                    {
                        "job_id": "301",
                        "job_name": "cancel",
                        "state": "CANCELLED",
                        "node": node,
                        "sandbox": "qianyi",
                        "candidate_sha": CANDIDATES["qianyi"]["sha"],
                    },
                )
            if checkpoint == "worker_crash" and node == config.oldlab_nodes[1]:
                terminal.append(
                    {
                        "job_id": "302",
                        "job_name": "crash",
                        "state": "FAILED",
                        "node": node,
                        "sandbox": "hongjian",
                        "candidate_sha": CANDIDATES["hongjian"]["sha"],
                    },
                )
            nodes[node] = {
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
            }
        rows.append(
            {
                "schema_version": 1,
                "kind": "loom.developer-sandbox.platform-health-checkpoint",
                "session_id": SESSION,
                "sequence": sequence,
                "checkpoint": checkpoint,
                "checkpoint_group": authority.CHECKPOINT_GROUPS[checkpoint],
                "candidates": CANDIDATES,
                "collector_host": config.collector_host,
                "observed_at": _iso(sequence * 100),
                "excluded_nodes": ["trt-gb10-7"],
                "nodes": nodes,
                "platform_health": {"healthy": True},
                "payload_sha256": str(sequence) * 64,
            },
        )
    return rows


def test_checked_in_config_is_closed_and_excludes_gb10_7() -> None:
    config = authority.load_config(CONFIG)

    assert config.collector_host == "trt-eai-oldlab-2"
    assert config.namespace == "loom-staging"
    assert len(config.nodes) == 19
    assert config.oldlab_nodes == tuple(f"oldlab-{index}" for index in range(1, 6))
    assert "trt-gb10-7" not in config.nodes
    assert tuple(config.host_aliases) == config.nodes
    assert config.host_aliases == authority.EXPECTED_HOST_ALIASES
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
    assert request["candidates"] == CANDIDATES
    assert envelope["action"] == "observe-platform-health-node"
    assert envelope["node"] == "trt-gb10-1"
    assert envelope["domain"] == "gb10"
    assert envelope["sandbox"] == "qianyi"
    assert envelope["candidate_sha"] == CANDIDATES["qianyi"]["sha"]
    assert envelope["payload_kind"] == "platform-health-node-json"


def test_complete_receipts_produce_reusable_trusted_evidence() -> None:
    config = authority.load_config(CONFIG)

    final = authority._verify_checkpoints(
        config,
        _receipts(config),
        require_complete=True,
    )

    assert final is not None
    assert final["session_id"] == SESSION
    assert final["candidates"] == CANDIDATES
    assert len(final["mixed_jobs"]) == 6
    assert final["zero_orphans"] is True
    assert len(final["node_intervals"]) == 19
    assert final["policy_capacity"]["oldlab"]["max_slots"] == 20
    assert final["policy_capacity"]["gb10"]["max_slots"] == 112
    assert final["policy_capacity"]["gb10"]["reserved_cpu_cores_per_node"] == 4
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


@pytest.mark.parametrize(
    "attack",
    ["missing_node", "replay", "foreign_job", "weak_headroom", "missing_cancel", "orphan"],
)
def test_trusted_evidence_attacks_fail_closed(attack: str) -> None:
    config = authority.load_config(CONFIG)
    receipts = _receipts(config)
    if attack == "missing_node":
        del receipts[1]["nodes"]["trt-gb10-15"]
    elif attack == "replay":
        receipts[2]["observed_at"] = receipts[1]["observed_at"]
    elif attack == "foreign_job":
        receipts[1]["nodes"][config.oldlab_nodes[0]]["active_jobs"][0]["sandbox"] = "foreign"
    elif attack == "weak_headroom":
        receipts[1]["nodes"][config.oldlab_nodes[0]]["capacity"]["memory_bytes_available"] = 1
    elif attack == "missing_cancel":
        receipts[2]["nodes"][config.oldlab_nodes[0]]["terminal_jobs"] = []
    else:
        receipts[-1]["nodes"][config.oldlab_nodes[0]]["active_jobs"] = [
            _job("qianyi", config.oldlab_nodes[0], "999"),
        ]

    with pytest.raises(authority.PlatformHealthError):
        authority._verify_checkpoints(config, receipts, require_complete=True)


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
    containers = []
    for index, role in enumerate(authority.ROLES, start=1):
        labels = {
            "loom.sandbox": "qianyi",
            "loom.candidate_sha": CANDIDATES["qianyi"]["sha"],
            "loom.slurm_job_id": "123",
            "loom.compose_project": "loom-qianyi-123",
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
                "Config": {"Labels": labels},
                "HostConfig": {
                    "CgroupParent": "/slurm/job_123",
                    "NanoCpus": 1_000_000_000,
                    "Memory": 1024**3,
                    "PidsLimit": 128,
                    "DeviceRequests": [],
                },
                "State": {"Status": "running", "Pid": 1000 + index},
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
                "cpu_cores": 8,
                "memory_bytes": 8 * 1024**3,
                "pids": 1024,
                "gpu_count": 0,
                "tres": "cpu=8,mem=8G",
                "exclusive": False,
            },
        },
    )
    monkeypatch.setattr(
        authority,
        "_read_cgroup_limit",
        lambda _path, name: {
            "cgroup.controllers": "cpu memory pids",
            "cpu.max": "800000 100000",
            "memory.max": str(8 * 1024**3),
            "pids.max": "1024",
        }[name],
    )

    jobs, orphans = authority._container_observations(
        CANDIDATES,
        expected_node="oldlab-1",
        checkpoint="mixed_non_loom",
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
            checkpoint="mixed_non_loom",
            run=lambda *_args, **_kwargs: None,  # type: ignore[arg-type,return-value]
        )


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
    assert "--checkpoint final_drain --execute" in sudoers
