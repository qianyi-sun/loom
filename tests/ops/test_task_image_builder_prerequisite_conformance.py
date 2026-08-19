from __future__ import annotations

import copy
import importlib.util
import json
import stat
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest
from jsonschema import Draft202012Validator

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts/ops/task_image_builder_prerequisite_conformance.py"
POLICY = ROOT / "deploy/task-image-builder/prerequisites-v1.toml"
SCHEMA = ROOT / "docs/evidence/task-image-builder-prerequisite-conformance-v1.schema.json"


def _load_module() -> ModuleType:
    spec = importlib.util.spec_from_file_location("task_builder_prerequisite_test", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


CONFORMANCE = _load_module()


def _evidence() -> dict[str, Any]:
    policy = CONFORMANCE.load_policy(POLICY)
    collected_at = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    clusters = []
    for cluster in policy.raw["clusters"]:
        architecture = cluster["architecture"]
        runtime = policy.runtime["architectures"][architecture]
        nodes = []
        for node_index, node_name in enumerate(cluster["builder_nodes"], start=10):
            address = (
                f"192.0.2.{node_index}"
                if cluster["id"] == "oldlab"
                else f"198.51.100.{node_index}"
            )
            physical_name = f"host-{node_name}"
            nodes.append(
                {
                    "name": node_name,
                    "architecture": architecture,
                    "slurm_identity": {
                        "node_name": node_name,
                        "node_hostname": physical_name,
                        "node_addr": address,
                        "resolved_addresses": [address],
                        "local_hostnames": [physical_name],
                        "local_addresses": [address],
                    },
                    "identity": {
                        "user": "loom-builder",
                        "uid": 993,
                        "group": "loom-task-builder",
                        "gid": 980,
                        "supplementary_groups": [],
                        "subuid_start": 3_000_000,
                        "subuid_count": 65_536,
                        "subgid_start": 3_000_000,
                        "subgid_count": 65_536,
                        "newuidmap_setuid_root": True,
                        "newgidmap_setuid_root": True,
                    },
                    "kernel": {
                        "cgroup_version": 2,
                        "controllers": ["cpu", "cpuset", "io", "memory", "pids"],
                        "unprivileged_user_namespaces": True,
                        "pidfd_open": True,
                        "sealed_memfd": True,
                        "clone3_into_cgroup": True,
                        "bpffs_mounted_root_only": True,
                    },
                    "runtime": {
                        "release": "rootless-runtime-v1",
                        "binary_sha256": runtime["binaries"],
                        "snapshotter": "fuse-overlayfs",
                        "network_driver": "slirp4netns",
                        "rootlesskit_flags": [
                            "--disable-host-loopback",
                            "--ipv6",
                            "--slirp4netns-sandbox=true",
                            "--slirp4netns-seccomp=true",
                        ],
                        "insecure_entitlements": [],
                    },
                    "storage": {
                        "filesystem": "xfs",
                        "project_quota": True,
                        "empty_job_root": True,
                        "cleanup_supported": True,
                    },
                    "network": {
                        "ipv4_default_deny": True,
                        "ipv6_default_deny": True,
                        "ingress_bytes_per_second": 52_428_800,
                        "egress_bytes_per_second": 52_428_800,
                        "ingress_packets_per_second": 25_000,
                        "egress_packets_per_second": 25_000,
                        "concurrent_flows": 1_024,
                        "new_flows_per_second": 200,
                        "dns_queries_per_second": 100,
                    },
                    "forbidden_paths_present": [],
                    "node_guard": {
                        "installed": False,
                        "active": False,
                    },
                }
            )
        clusters.append(
            {
                "id": cluster["id"],
                "slurm_cluster": cluster["slurm_cluster"],
                "controller": cluster["controller"],
                "architecture": architecture,
                "controller_identity": {
                    "user": "loom-builder",
                    "uid": 993,
                    "group": "loom-task-builder",
                    "gid": 980,
                    "home": "/nonexistent",
                    "shell": "/usr/sbin/nologin",
                    "supplementary_groups": [],
                },
                "slurm": {
                    "task_plugin": "task/cgroup",
                    "proctrack_type": "proctrack/cgroup",
                    "cgroup_version": 2,
                    "constrain_cores": True,
                    "constrain_ram_space": True,
                    "constrain_swap_space": True,
                    "constrain_devices": True,
                    "trial_partition": {
                        "name": cluster["trial_partition"],
                        "priority_tier": 100,
                        "nodes": cluster["builder_nodes"],
                    },
                    "builder_partition": {
                        "name": "loom-task-builder",
                        "priority_tier": 200,
                        "nodes": cluster["builder_nodes"],
                    },
                    "qos": {
                        "name": cluster["slurm_qos"],
                        "flags": ["DenyOnLimit"],
                        "max_jobs_per_user": 1,
                        "max_submit_jobs_per_user": 1,
                        "max_wall": "02:00:00",
                        "group_tres": {
                            "cpu": 8,
                            "memory_mib": 32_768,
                            "nodes": 1,
                        },
                    },
                    "association": {
                        "user": "loom-builder",
                        "account": "loom-task-builder",
                        "partition": "loom-task-builder",
                        "qos": [cluster["slurm_qos"]],
                        "default_qos": cluster["slurm_qos"],
                    },
                    "legacy_builder": {
                        "qos": {
                            "name": policy.raw["legacy_guard"]["qos"],
                            "flags": ["DenyOnLimit"],
                            "priority": 0,
                            "max_jobs_per_user": 1,
                            "max_submit_jobs_per_user": 1,
                            "max_wall": "04:00:00",
                            "group_tres": {},
                        },
                        "association": {
                            "cluster": cluster["slurm_cluster"],
                            "account": policy.raw["legacy_guard"]["account"],
                            "user": policy.raw["legacy_guard"]["user"],
                            "qos": sorted(
                                [
                                    cluster["legacy_base_qos"],
                                    policy.raw["legacy_guard"]["qos"],
                                ]
                            ),
                            "default_qos": cluster["legacy_base_qos"],
                        },
                        "reservation": {
                            "name": policy.raw["legacy_guard"]["reservation"],
                            "node": cluster["legacy_reservation_node"],
                            "partition": cluster["legacy_reservation_partition"],
                            "users": [policy.raw["legacy_guard"]["user"]],
                            "accounts": [policy.raw["legacy_guard"]["account"]],
                            "state": "ACTIVE",
                            "flags": ["IGNORE_JOBS", "SPEC_NODES"],
                        },
                    },
                },
                "nodes": nodes,
            }
        )
    return {
        "schema": "loom.task-image-builder-prerequisite-conformance/v1",
        "schema_version": 1,
        "collected_at": collected_at,
        "policy_version": policy.raw["policy_version"],
        "policy_sha256": policy.digest,
        "production_certification_allowed": False,
        "certified_nodes": [],
        "control_plane_services": policy.raw["control_plane_services"],
        "clusters": clusters,
    }


def _failures(evidence: dict[str, Any]) -> list[str]:
    return CONFORMANCE.verify_evidence(evidence, CONFORMANCE.load_policy(POLICY))


def test_schema_and_complete_phase_one_evidence_are_valid_but_not_certifiable() -> None:
    schema = json.loads(SCHEMA.read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(schema)
    policy = CONFORMANCE.load_policy(POLICY)

    assert _failures(_evidence()) == []
    assert CONFORMANCE.certification_blockers(policy) == ("phase2_guard_provider_release_missing",)


@pytest.mark.parametrize(
    ("mutate", "expected"),
    [
        (lambda item: item.__setitem__("policy_sha256", "0" * 64), "policy digest"),
        (lambda item: item["clusters"].pop(), "cluster set"),
        (
            lambda item: item["clusters"][0].__setitem__("architecture", "aarch64"),
            "architecture",
        ),
        (
            lambda item: item["clusters"][0]["controller_identity"].__setitem__(
                "uid", 992
            ),
            "controller identity",
        ),
        (
            lambda item: item["clusters"][0]["slurm"].__setitem__("constrain_ram_space", False),
            "cgroup constraints",
        ),
        (
            lambda item: item["clusters"][0]["slurm"]["builder_partition"].__setitem__(
                "priority_tier", 100
            ),
            "higher priority tier",
        ),
        (
            lambda item: item["clusters"][0]["slurm"]["builder_partition"]["nodes"].pop(),
            "builder partition nodes",
        ),
        (
            lambda item: item["clusters"][0]["slurm"]["qos"]["group_tres"].__setitem__("cpu", 16),
            "QoS",
        ),
        (
            lambda item: item["clusters"][0]["slurm"]["association"].__setitem__(
                "user", "loom-rollout"
            ),
            "association",
        ),
        (
            lambda item: item["clusters"][0]["slurm"]["legacy_builder"]["qos"].__setitem__(
                "max_wall", "08:00:00"
            ),
            "legacy builder",
        ),
        (
            lambda item: item["clusters"][0]["slurm"]["legacy_builder"][
                "reservation"
            ].__setitem__("node", "trt-eai-oldlab-5"),
            "legacy builder",
        ),
        (
            lambda item: item["clusters"][0]["nodes"][0]["slurm_identity"].__setitem__(
                "node_name", "trt-eai-oldlab-4"
            ),
            "Slurm host binding",
        ),
        (
            lambda item: item["clusters"][0]["nodes"][0]["slurm_identity"].__setitem__(
                "resolved_addresses", ["192.0.2.200"]
            ),
            "Slurm host binding",
        ),
        (
            lambda item: item["clusters"][0]["nodes"][0]["slurm_identity"].__setitem__(
                "local_hostnames", ["foreign-host"]
            ),
            "Slurm host binding",
        ),
        (
            lambda item: item["clusters"][0]["nodes"][0]["identity"].__setitem__(
                "subuid_start", 100_000
            ),
            "identity",
        ),
        (
            lambda item: item["clusters"][0]["nodes"][0]["runtime"]["binary_sha256"].__setitem__(
                "buildkitd", "f" * 64
            ),
            "runtime binaries",
        ),
        (
            lambda item: item["clusters"][0]["nodes"][0]["runtime"].__setitem__(
                "rootlesskit_flags", ["--ipv6"]
            ),
            "rootless runtime policy",
        ),
        (
            lambda item: item["clusters"][0]["nodes"][0]["storage"].__setitem__(
                "project_quota", False
            ),
            "storage",
        ),
        (
            lambda item: item["clusters"][0]["nodes"][0]["kernel"].__setitem__(
                "bpffs_mounted_root_only", False
            ),
            "kernel",
        ),
        (
            lambda item: item["clusters"][0]["nodes"][0]["network"].__setitem__(
                "ipv6_default_deny", False
            ),
            "network policy",
        ),
        (
            lambda item: item["clusters"][0]["nodes"][0]["forbidden_paths_present"].append(
                "/var/run/docker.sock"
            ),
            "forbidden host path",
        ),
        (
            lambda item: item["clusters"][0]["nodes"][0]["node_guard"].__setitem__(
                "installed", True
            ),
            "Phase 2 node guard",
        ),
        (
            lambda item: item["control_plane_services"].__setitem__("publication_signer", False),
            "control-plane services",
        ),
        (
            lambda item: item.__setitem__("certified_nodes", ["trt-gb10-1"]),
            "certified_nodes",
        ),
        (
            lambda item: item.__setitem__("production_certification_allowed", True),
            "production certification",
        ),
    ],
)
def test_evidence_drift_fails_closed(mutate: Any, expected: str) -> None:
    evidence = _evidence()
    mutate(evidence)

    assert any(expected in failure for failure in _failures(evidence))


def test_node_set_must_be_complete_unique_and_bound_to_the_cluster() -> None:
    evidence = _evidence()
    evidence["clusters"][1]["nodes"].pop()
    evidence["clusters"][0]["nodes"].append(copy.deepcopy(evidence["clusters"][0]["nodes"][0]))

    failures = _failures(evidence)

    assert any("node evidence set" in item for item in failures)
    assert any("duplicate node evidence" in item for item in failures)


def test_evidence_timestamp_must_be_fresh() -> None:
    evidence = _evidence()
    evidence["collected_at"] = "2020-01-01T00:00:00Z"

    assert any("freshness window" in item for item in _failures(evidence))


def test_cli_plan_is_read_only_and_verify_rejects_secret_like_evidence(tmp_path: Path) -> None:
    plan = subprocess.run(
        [sys.executable, str(SCRIPT), "plan", "--policy", str(POLICY)],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert plan.returncode == 0, plan.stderr
    payload = json.loads(plan.stdout)
    assert payload["mutations_supported"] is False
    assert payload["production_certification_allowed"] is False
    assert payload["certified_nodes"] == []

    evidence = _evidence()
    evidence["clusters"][0]["api_token"] = "Bearer secret-value"
    evidence_path = tmp_path / "evidence.json"
    evidence_path.write_text(json.dumps(evidence), encoding="utf-8")
    verified = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "verify",
            "--policy",
            str(POLICY),
            "--evidence",
            str(evidence_path),
        ],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert verified.returncode == 2
    assert "secret-like" in verified.stderr
    assert "secret-value" not in verified.stderr


def test_cli_verify_reports_valid_prerequisites_and_closed_certification(
    tmp_path: Path,
) -> None:
    evidence_path = tmp_path / "evidence.json"
    evidence_path.write_text(json.dumps(_evidence()), encoding="utf-8")

    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "verify",
            "--policy",
            str(POLICY),
            "--evidence",
            str(evidence_path),
        ],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    report = json.loads(result.stdout)
    assert report["prerequisites_valid"] is True
    assert report["production_certification_allowed"] is False
    assert report["certification_blockers"] == [
        "phase2_guard_provider_release_missing",
    ]
    assert report["certified_nodes"] == []


def test_cli_canonicalize_writes_once_with_owner_only_mode(tmp_path: Path) -> None:
    evidence = _evidence()
    evidence_path = tmp_path / "evidence.json"
    evidence_path.write_text(json.dumps(evidence, indent=2), encoding="utf-8")
    output = tmp_path / "canonical.json"

    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "canonicalize",
            "--policy",
            str(POLICY),
            "--evidence",
            str(evidence_path),
            "--output",
            str(output),
        ],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert output.read_bytes() == (
        json.dumps(evidence, sort_keys=True, separators=(",", ":")).encode("utf-8") + b"\n"
    )
    assert stat.S_IMODE(output.stat().st_mode) == 0o600

    repeated = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "canonicalize",
            "--policy",
            str(POLICY),
            "--evidence",
            str(evidence_path),
            "--output",
            str(output),
        ],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert repeated.returncode == 2
    assert "already exists" in repeated.stderr
