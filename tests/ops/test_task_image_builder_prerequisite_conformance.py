from __future__ import annotations

import base64
import copy
import functools
import hashlib
import importlib.util
import json
import os
import stat
import subprocess
import sys
import tomllib
from datetime import UTC, datetime
from pathlib import Path
from types import ModuleType, SimpleNamespace
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


def _legacy_evidence() -> dict[str, Any]:
    policy = CONFORMANCE.load_policy(POLICY)
    collected_at = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    clusters = []
    for cluster in policy.raw["clusters"]:
        architecture = cluster["architecture"]
        runtime = policy.runtime["architectures"][architecture]
        nodes = []
        for node_index, node_name in enumerate(cluster["builder_nodes"], start=10):
            address = (
                f"192.0.2.{node_index}" if cluster["id"] == "oldlab" else f"198.51.100.{node_index}"
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


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _fixture_policy() -> dict[str, Any]:
    return tomllib.loads(POLICY.read_text(encoding="utf-8"))


def _fixture_release() -> dict[str, Any]:
    return json.loads(
        (ROOT / "deploy/task-image-builder/host-release-v1.json").read_text(encoding="utf-8")
    )


def _fixture_runtime() -> dict[str, Any]:
    return json.loads(
        (ROOT / "deploy/task-image-builder/rootless-runtime-v1.json").read_text(encoding="utf-8")
    )


def _path_digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _fingerprint(value: object) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _authority_binding() -> Any:
    return CONFORMANCE.authority.load_authority_binding(ROOT)


def _fixture_cluster(cluster_id: str) -> dict[str, Any]:
    matches = [item for item in _fixture_policy()["clusters"] if item["id"] == cluster_id]
    assert len(matches) == 1
    return matches[0]


def _candidate_digest() -> str:
    components = {
        "policy": _path_digest(POLICY),
        "release": _path_digest(ROOT / "deploy/task-image-builder/host-release-v1.json"),
        "runtime": _path_digest(ROOT / "deploy/task-image-builder/rootless-runtime-v1.json"),
        **_authority_binding().as_dict(),
    }
    return _fingerprint(components)


def _slurm_candidate_digest() -> str:
    return _fingerprint(
        {
            "policy": _path_digest(POLICY),
            **_authority_binding().as_dict(),
        }
    )


def _host_candidate_digest() -> str:
    return _fingerprint(
        {
            "policy": _path_digest(POLICY),
            "release": _path_digest(ROOT / "deploy/task-image-builder/host-release-v1.json"),
            "runtime": _path_digest(ROOT / "deploy/task-image-builder/rootless-runtime-v1.json"),
            **_authority_binding().as_dict(),
        }
    )


def _maintenance_candidate_digest() -> str:
    return _fingerprint(
        {
            "policy": _path_digest(POLICY),
            **_authority_binding().as_dict(),
        }
    )


def _event_chain(items: list[tuple[str, dict[str, Any]]]) -> list[dict[str, Any]]:
    previous = "0" * 64
    events: list[dict[str, Any]] = []
    for sequence, (event_type, data) in enumerate(items):
        event = {
            "sequence": sequence,
            "type": event_type,
            "previous_hash": previous,
            "data": data,
        }
        event["event_hash"] = _fingerprint(event)
        previous = event["event_hash"]
        events.append(event)
    return events


def _receipt(document: dict[str, Any]) -> dict[str, Any]:
    transported = json.loads(_canonical(document))
    return {
        "sha256": hashlib.sha256(_canonical(transported) + b"\n").hexdigest(),
        "document": transported,
    }


def _legacy_receipt_state(cluster: dict[str, Any]) -> dict[str, Any]:
    return {
        "qos": {
            "name": "loom-task-image-builder",
            "flags": ["DenyOnLimit"],
            "priority": 0,
            "max_jobs_per_user": 1,
            "max_submit_jobs_per_user": 1,
            "max_wall": "04:00:00",
            "group_tres": {},
        },
        "association": {
            "cluster": cluster["slurm_cluster"],
            "account": "loom-staging",
            "user": "loom-rollout",
            "qos": sorted([cluster["legacy_base_qos"], "loom-task-image-builder"]),
            "default_qos": cluster["legacy_base_qos"],
        },
        "reservation": {
            "name": "loom-task-image-builder",
            "node": cluster["legacy_reservation_node"],
            "node_count": 1,
            "partition": cluster["legacy_reservation_partition"],
            "users": ["loom-rollout"],
            "accounts": ["loom-staging"],
            "state": "ACTIVE",
            "flags": ["IGNORE_JOBS", "SPEC_NODES"],
        },
    }


def _slurm_state(cluster: dict[str, Any]) -> dict[str, Any]:
    return {
        "partition": {
            "name": cluster["builder_partition"],
            "line": cluster["builder_partition_line"],
        },
        "account": {"name": cluster["slurm_account"]},
        "qos": {
            "name": cluster["slurm_qos"],
            "flags": ["DenyOnLimit"],
            "priority": 0,
            "max_jobs_per_user": 1,
            "max_submit_jobs_per_user": 1,
            "max_wall": "02:00:00",
            "group_tres": {"cpu": 8, "memory_mib": 32_768, "nodes": 1},
        },
        "association": {
            "cluster": cluster["slurm_cluster"],
            "account": cluster["slurm_account"],
            "user": "loom-builder",
            "partition": cluster["builder_partition"],
            "qos": [cluster["slurm_qos"]],
            "default_qos": cluster["slurm_qos"],
        },
        "legacy": _legacy_receipt_state(cluster),
    }


def _slurm_receipt(cluster_id: str, operation_id: str) -> dict[str, Any]:
    cluster = _fixture_cluster(cluster_id)
    pre_state = _slurm_state(cluster)
    post_state = copy.deepcopy(pre_state)
    legacy_fingerprint = _fingerprint(pre_state["legacy"])
    events = _event_chain(
        [
            ("pre_state", {"state": copy.deepcopy(pre_state)}),
            (
                "intent",
                {
                    "action": "apply",
                    "delegate": str(
                        ROOT / "deploy/slurm/converge-loom-task-image-builder-prerequisites.sh"
                    ),
                    "cluster_id": cluster_id,
                },
            ),
            (
                "post_state",
                {
                    "state": copy.deepcopy(post_state),
                    "readback_error": None,
                    "created_objects": [],
                },
            ),
            ("converged", {"returncode": 0, "legacy_unchanged": True}),
        ]
    )
    return {
        "schema": "loom.task-image-builder-slurm-receipt/v1",
        "operation_id": operation_id,
        "cluster_id": cluster_id,
        "candidate_digest": _slurm_candidate_digest(),
        "policy_digest": _path_digest(POLICY),
        "controller_digest": _path_digest(
            ROOT / "deploy/slurm/install-loom-task-image-builder-controller-identity.sh"
        ),
        "cluster_digest": _fingerprint(cluster),
        **_authority_binding().as_dict(),
        "production_certification_allowed": False,
        "certified_nodes": [],
        "blockers": ["phase2_guard_provider_release_missing"],
        "pre_state": pre_state,
        "post_state": post_state,
        "legacy_pre_fingerprint": legacy_fingerprint,
        "legacy_post_fingerprint": legacy_fingerprint,
        "created_objects": [],
        "durable_config_backup_digest": "1" * 64,
        "command_outcome": {"returncode": 0, "stdout": "", "stderr": ""},
        "post_readback_error": None,
        "terminal_state": "converged",
        "events": events,
    }


def _quota_state() -> dict[str, Any]:
    resources = _fixture_policy()["resource_profile"]
    return {
        "storage_root_exists": True,
        "storage_root_uid": 993,
        "storage_root_gid": 980,
        "storage_root_mode": 0o700,
        "storage_root_entries": [],
        "project_id": 300_993,
        "project_inherit": True,
        "block_used": 0,
        "block_soft_limit": 0,
        "block_hard_limit": resources["scratch_bytes"] // 1024,
        "inode_used": 0,
        "inode_soft_limit": 0,
        "inode_hard_limit": 1_000_000,
    }


def _host_receipt(cluster_id: str, node_name: str, operation_id: str) -> dict[str, Any]:
    cluster = _fixture_cluster(cluster_id)
    cgroup_contents = (
        "CgroupPlugin=autodetect\n"
        "ConstrainCores=yes\n"
        "ConstrainRAMSpace=yes\n"
        "ConstrainSwapSpace=yes\n"
        "ConstrainDevices=yes\n"
    )
    cgroup = {
        "kind": "regular",
        "payload_b64": base64.b64encode(cgroup_contents.encode()).decode(),
        "sha256": hashlib.sha256(cgroup_contents.encode()).hexdigest(),
        "mode": 0o644,
        "uid": 0,
        "gid": 0,
    }
    facts = {
        "architecture": cluster["architecture"],
        "slurm_node": node_name,
        "bundle_digest": "2" * 64,
        "packages": {
            "libsubid4": "1:4.13+dfsg1-4ubuntu3.2",
            "uidmap": "1:4.13+dfsg1-4ubuntu3.2",
            "quota": "4.06-1build6",
        },
        "helpers_exact": True,
        "identity_exact": True,
        "runtime_exact": True,
        "quota_exact": True,
        "quota_state": _quota_state(),
        "storage_exact": True,
        "kernel_exact": True,
        "forbidden_sockets_absent": True,
    }
    binding = {
        "operation_id": operation_id,
        "cluster_id": cluster_id,
        "slurm_node": node_name,
        "candidate_digest": _host_candidate_digest(),
        "policy_digest": _path_digest(POLICY),
        "release_digest": _path_digest(ROOT / "deploy/task-image-builder/host-release-v1.json"),
        "cluster_digest": _fingerprint(cluster),
        **_authority_binding().as_dict(),
        "bundle_digest": "2" * 64,
    }
    events = _event_chain(
        [
            (
                "pre_state",
                {
                    "binding": binding,
                    "facts": copy.deepcopy(facts),
                    "cgroup": copy.deepcopy(cgroup),
                },
            ),
            ("intent", {"changes": []}),
            (
                "post_state",
                {"facts": copy.deepcopy(facts), "cgroup": copy.deepcopy(cgroup)},
            ),
            (
                "host_prepared",
                {"activation_required": True, "created_inert_artifacts": []},
            ),
        ]
    )
    return {
        "schema": "loom.task-image-builder-host-receipt/v1",
        "operation_id": operation_id,
        "cluster_id": cluster_id,
        "slurm_node": node_name,
        "candidate_digest": _host_candidate_digest(),
        "policy_digest": _path_digest(POLICY),
        "release_digest": _path_digest(ROOT / "deploy/task-image-builder/host-release-v1.json"),
        "cluster_digest": _fingerprint(cluster),
        **_authority_binding().as_dict(),
        "production_certification_allowed": False,
        "certified_nodes": [],
        "blockers": ["phase2_guard_provider_release_missing"],
        "bundle_digest": "2" * 64,
        "pre_state": copy.deepcopy(facts),
        "post_state": copy.deepcopy(facts),
        "cgroup_prestate": copy.deepcopy(cgroup),
        "cgroup_poststate": copy.deepcopy(cgroup),
        "created_inert_artifacts": [],
        "activation_required": True,
        "rollback_verified": None,
        "rollback_source_state": None,
        "terminal_state": "host_prepared",
        "failure": None,
        "events": events,
    }


def _command(
    command: list[str],
    *,
    returncode: int = 0,
    stdout: str = "",
) -> dict[str, Any]:
    return {
        "command": command,
        "returncode": returncode,
        "stdout": stdout,
        "stderr": "" if returncode == 0 else "denied by policy",
    }


def _maintenance_receipt(
    cluster_id: str,
    node_name: str,
    operation_id: str,
    job_id: str,
) -> dict[str, Any]:
    cluster = _fixture_cluster(cluster_id)
    reason = f"loom-task-builder-phase1/host-release-v1/{operation_id}"
    cgroup_contents = (
        "CgroupPlugin=autodetect\n"
        "ConstrainCores=yes\n"
        "ConstrainRAMSpace=yes\n"
        "ConstrainSwapSpace=yes\n"
        "ConstrainDevices=yes\n"
    )
    daemon_state = {
        "state": "active",
        "cgroup_config": {
            "path": "/etc/slurm/cgroup.conf",
            "sha256": hashlib.sha256(cgroup_contents.encode()).hexdigest(),
            "contents": cgroup_contents,
        },
    }
    cgroup_path = f"/slurm/uid_993/job_{job_id}/step_batch"
    controls = {
        "cpuset_cpus_effective": "0-7",
        "cpuset_cpu_count": 8,
        "memory_max": 32_768 * 1024 * 1024,
        "memory_swap_max": 0,
        "devices": {
            "cgroup_path": cgroup_path,
            "programs": [
                {
                    "id": 19,
                    "attach_type": "cgroup_device",
                    "attach_flags": "multi",
                    "name": "loom_devices",
                }
            ],
        },
    }
    cleanup_absence = {
        "processes_absent": True,
        "mounts_absent": True,
        "job_directory_absent": True,
    }
    smoke = {
        "job_id": job_id,
        "allocation": {"node": node_name, "sole_first_allocation": True},
        "cgroup": controls,
        "cgroup_path": cgroup_path,
        "cleanup": cleanup_absence,
    }
    reservation_name = "loom_task_builder_maintenance_" + operation_id.replace("-", "")
    unrelated = (
        "ReservationName=legacy_operator_hold Nodes=legacy-node Users=operator State=ACTIVE\n"
    )
    created = (
        f"ReservationName={reservation_name} Nodes={node_name} Users=loom-builder State=ACTIVE\n"
    )
    reservation = {
        "name": reservation_name,
        "prior_readback": _command(
            ["/usr/bin/scontrol", "show", "reservation", "--oneliner"],
            stdout=unrelated,
        ),
        "prior_absence": {"name": reservation_name, "absent": True},
        "create": _command(
            [
                "/usr/bin/scontrol",
                "create",
                "reservation",
                f"Name={reservation_name}",
                f"Nodes={node_name}",
                "Users=loom-builder",
                "StartTime=now",
                "Duration=00:15:00",
            ]
        ),
        "create_readback": _command(
            ["/usr/bin/scontrol", "show", "reservation", "--oneliner"],
            stdout=unrelated + created,
        ),
        "binding": {
            "name": reservation_name,
            "node": node_name,
            "state": "ACTIVE",
            "user": "loom-builder",
        },
        "delete": _command(
            [
                "/usr/bin/scontrol",
                "delete",
                "reservation",
                f"Name={reservation_name}",
            ]
        ),
        "delete_readback": _command(
            ["/usr/bin/scontrol", "show", "reservation", "--oneliner"],
            stdout=unrelated,
        ),
        "absence": {"name": reservation_name, "absent": True},
    }
    admission_arguments = [
        f"--account={cluster['slurm_account']}",
        f"--qos={cluster['slurm_qos']}",
        f"--partition={cluster['builder_partition']}",
        "--cpus-per-task=8",
        "--mem=32768M",
        "--time=02:00:00",
    ]
    admission = {
        "builder": _command(
            [
                "/usr/sbin/runuser",
                "--user",
                "loom-builder",
                "--",
                "/usr/bin/sbatch",
                "--test-only",
                *admission_arguments,
                "--wrap=/usr/bin/true",
            ]
        ),
        "rollout_rejected": _command(
            [
                "/usr/sbin/runuser",
                "--user",
                "loom-rollout",
                "--",
                "/usr/bin/sbatch",
                "--test-only",
                *admission_arguments,
                "--wrap=/usr/bin/true",
            ],
            returncode=1,
        ),
    }
    accounting = {
        "readback": _command(
            [
                "/usr/bin/sacct",
                "--noheader",
                "--parsable2",
                "--jobs",
                job_id,
                "--format=JobIDRaw,State,ExitCode",
            ],
            stdout=f"{job_id}|COMPLETED|0:0\n",
        ),
        "top_level": {"job_id": job_id, "state": "COMPLETED", "exit_code": "0:0"},
    }
    observed_evidence = {
        "schema": "loom.task-image-builder-maintenance-smoke/v1",
        "operation_id": operation_id,
        "job_id": job_id,
        "cgroup_path": cgroup_path,
        "controls": controls,
    }
    release = {
        **_command(
            [
                str(ROOT / "scripts/ops/task_image_builder_node_maintenance.py"),
                "--internal-smoke",
                "release",
                job_id,
                operation_id,
            ],
            stdout='{"state":"released"}\n',
        ),
        "outcome": "released",
    }
    cleanup = {
        **_command(
            [
                str(ROOT / "scripts/ops/task_image_builder_node_maintenance.py"),
                "--internal-smoke",
                "cleanup",
                job_id,
                operation_id,
            ],
            stdout=(
                '{"job_directory_absent":true,"mounts_absent":true,'
                '"processes_absent":true,"state":"absent"}\n'
            ),
        ),
        **cleanup_absence,
    }
    observations = {
        "daemon": {"restart": daemon_state, "check": copy.deepcopy(daemon_state)},
        "admission": admission,
        "reservation": reservation,
        "smoke": smoke,
        "emergency_containment": None,
    }
    pre_state = {"state": "IDLE", "reason": "none", "allocated_tres": "cpu=0,mem=0M"}
    events = _event_chain(
        [
            ("pre_state_recorded", {"pre_state": copy.deepcopy(pre_state)}),
            ("drained", {"reason": reason}),
            ("idle", {}),
            ("host_preflighted", {}),
            ("host_applied", {}),
            ("daemon_restarted", {}),
            ("readback_verified", {}),
            ("admission_verified", {}),
            (
                "reservation_created",
                {
                    "name": reservation_name,
                    "create": reservation["create"],
                    "create_readback": reservation["create_readback"],
                    "binding": reservation["binding"],
                },
            ),
            ("smoke_queued", {"job_id": job_id}),
            ("smoke_pending", {"job_id": job_id}),
            ("smoke_running", {"job_id": job_id}),
            (
                "smoke_observed",
                {"job_id": job_id, "evidence": observed_evidence},
            ),
            ("smoke_released", {"job_id": job_id, "release": release}),
            ("smoke_completed", {"job_id": job_id, "accounting": accounting}),
            ("smoke_cleaned", {"job_id": job_id, "cleanup": cleanup}),
            (
                "reservation_deleted",
                {
                    "name": reservation_name,
                    "delete": reservation["delete"],
                    "delete_readback": reservation["delete_readback"],
                    "absence": reservation["absence"],
                },
            ),
            ("prepared", {"job_id": job_id}),
        ]
    )
    return {
        "schema": "loom.task-image-builder-node-maintenance/v1",
        "operation_id": operation_id,
        "cluster_id": cluster_id,
        "slurm_node": node_name,
        "candidate_digest": _maintenance_candidate_digest(),
        "policy_digest": _path_digest(POLICY),
        **_authority_binding().as_dict(),
        "production_certification_allowed": False,
        "certified_nodes": [],
        "blockers": ["phase2_guard_provider_release_missing"],
        "pre_state": pre_state,
        "observations": observations,
        "terminal_state": "prepared",
        "failure": None,
        "events": events,
    }


def _metadata(observed_at: str) -> dict[str, Any]:
    policy = _fixture_policy()
    return {
        "observed_at": observed_at,
        "candidate_sha256": _candidate_digest(),
        "policy_version": policy["policy_version"],
        "policy_sha256": _fingerprint(policy),
        "policy_file_sha256": _path_digest(POLICY),
        "release_name": "host-release-v1",
        "release_sha256": _path_digest(ROOT / "deploy/task-image-builder/host-release-v1.json"),
        "runtime_manifest_sha256": _path_digest(
            ROOT / "deploy/task-image-builder/rootless-runtime-v1.json"
        ),
        **_authority_binding().as_dict(),
    }


def _operation(index: int) -> str:
    return f"00000000-0000-4000-8000-{index:012d}"


def _phase2_absence() -> dict[str, Any]:
    names = (
        "loom-task-builder-allocation-supervisor",
        "loom-task-builder-node-guard",
        "loom-task-builder-provider",
    )
    unit_stdout = "LoadState=not-found\nActiveState=inactive\nFragmentPath=\n"
    return {
        "installed": False,
        "active": False,
        "artifacts": [{"path": f"/usr/libexec/{name}", "present": False} for name in names],
        "unit_readback": [
            {
                "name": f"{name}.service",
                "command": [
                    "/usr/bin/systemctl",
                    "show",
                    "--no-pager",
                    "--property=LoadState",
                    "--property=ActiveState",
                    "--property=FragmentPath",
                    f"{name}.service",
                ],
                "returncode": 0,
                "stdout": unit_stdout,
                "stderr": "",
            }
            for name in names
        ],
        "process_readback": [
            {
                "name": name,
                "command": ["/usr/bin/pgrep", "-f", f"(^|/){name}( |$)"],
                "returncode": 1,
                "stdout": "",
                "stderr": "",
            }
            for name in names
        ],
    }


def _controller_cluster(
    cluster: dict[str, Any],
    observed_at: str,
    ordinal: int,
) -> dict[str, Any]:
    state = _slurm_state(cluster)
    qos = copy.deepcopy(state["qos"])
    qos.pop("priority")
    association = copy.deepcopy(state["association"])
    association.pop("cluster")
    legacy = copy.deepcopy(state["legacy"])
    legacy["reservation"].pop("node_count")
    return {
        "id": cluster["id"],
        "slurm_cluster": cluster["slurm_cluster"],
        "controller": cluster["controller"],
        "architecture": cluster["architecture"],
        **_metadata(observed_at),
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
            "qos": qos,
            "association": association,
            "legacy_builder": legacy,
        },
        "slurm_receipt": _receipt(_slurm_receipt(cluster["id"], _operation(ordinal))),
    }


def _node_evidence(
    cluster: dict[str, Any],
    node_name: str,
    index: int,
    ordinal: int,
    observed_at: str,
) -> dict[str, Any]:
    policy = _fixture_policy()
    release = _fixture_release()
    runtime = _fixture_runtime()
    architecture = cluster["architecture"]
    debian_architecture = release["architecture_map"][architecture]
    runtime_binaries = runtime["architectures"][architecture]["binaries"]
    address = f"192.0.2.{index}" if cluster["id"] == "oldlab" else f"198.51.100.{index}"
    physical = f"host-{cluster['id']}-{index}"
    cgroup_contents = (
        "CgroupPlugin=autodetect\n"
        "ConstrainCores=yes\n"
        "ConstrainRAMSpace=yes\n"
        "ConstrainSwapSpace=yes\n"
        "ConstrainDevices=yes\n"
    )
    operation_id = _operation(ordinal)
    host_document = _host_receipt(cluster["id"], node_name, operation_id)
    maintenance_document = _maintenance_receipt(
        cluster["id"], node_name, operation_id, str(10_000 + ordinal)
    )
    controllers_contents = "cpu cpuset io memory pids\n"
    delegation_contents = "io pids\n"
    source = f"/dev/mapper/builder-{cluster['id']}-{index}"
    cleanup = next(
        event["data"]["cleanup"]
        for event in maintenance_document["events"]
        if event["type"] == "smoke_cleaned"
    )
    installed_packages = [
        {
            "name": name,
            "version": package["version"],
            "architecture": package["architecture"],
            "filename": package["filename"],
            "size": package["size"],
            "artifact_sha256": package["sha256"],
        }
        for name, package in sorted(release["packages"][debian_architecture].items())
    ]
    dynamic_readback = {
        name: {
            "command": [
                "/usr/bin/readelf",
                "-d",
                f"/opt/loom-task-builder/releases/rootless-runtime-v1/bin/{name}",
            ],
            "returncode": 0,
            "stdout": "There is no dynamic section in this file.\n",
            "stderr": "",
        }
        for name in sorted(runtime_binaries)
    }
    network_keys = (
        "ipv4_default_deny",
        "ipv6_default_deny",
        "ingress_bytes_per_second",
        "egress_bytes_per_second",
        "ingress_packets_per_second",
        "egress_packets_per_second",
        "concurrent_flows",
        "new_flows_per_second",
        "dns_queries_per_second",
    )
    return {
        "name": node_name,
        "architecture": architecture,
        **_metadata(observed_at),
        "slurm_identity": {
            "node_name": node_name,
            "node_hostname": physical,
            "node_addr": address,
            "resolved_addresses": [address],
            "resolution": {"query": address, "addresses": [address]},
            "local_hostnames": [physical, f"{physical}.example.test"],
            "local_addresses": [address],
            "readback": {
                "command": ["/usr/bin/scontrol", "show", "node", node_name, "-o"],
                "returncode": 0,
                "stdout": (
                    f"NodeName={node_name} NodeAddr={address} NodeHostName={physical} "
                    "AvailableFeatures=(null) ActiveFeatures=(null)\n"
                ),
                "stderr": "",
            },
        },
        "identity": {
            "user": "loom-builder",
            "uid": 993,
            "group": "loom-task-builder",
            "gid": 980,
            "home": "/nonexistent",
            "shell": "/usr/sbin/nologin",
            "supplementary_groups": [],
            "subuid_start": 3_000_000,
            "subuid_count": 65_536,
            "subgid_start": 3_000_000,
            "subgid_count": 65_536,
            "newuidmap_setuid_root": True,
            "newgidmap_setuid_root": True,
        },
        "packages": {
            "source": {
                "os_id": "ubuntu",
                "version_id": "24.04",
                "suite": "noble-updates",
                "component": "main",
                "signer_fingerprint": "F6ECB3762474EDA9D21B7022871920D1991BC93C",
                "keyring_sha256": (
                    "80a36b0a6de2f69f49d2df75ef473ccde121e9e190b9ea01d20a4f63778d5c31"
                ),
            },
            "installed": installed_packages,
            "helpers": [
                {
                    "path": "/usr/bin/newgidmap",
                    "uid": 0,
                    "gid": 0,
                    "mode": "4755",
                    "sha256": "3" * 64,
                    "file_capabilities": [],
                },
                {
                    "path": "/usr/bin/newuidmap",
                    "uid": 0,
                    "gid": 0,
                    "mode": "4755",
                    "sha256": "4" * 64,
                    "file_capabilities": [],
                },
            ],
        },
        "kernel": {
            "cgroup_version": 2,
            "controllers": ["cpu", "cpuset", "io", "memory", "pids"],
            "unprivileged_user_namespaces": True,
            "pidfd_open": True,
            "sealed_memfd": True,
            "clone3_into_cgroup": True,
            "bpffs_mounted_root_only": True,
            "cgroup_filesystem": "cgroup2",
            "delegated_controllers": ["io", "pids"],
            "slurm_cgroup_readback": {
                "path": "/etc/slurm/cgroup.conf",
                "sha256": hashlib.sha256(cgroup_contents.encode()).hexdigest(),
                "contents": cgroup_contents,
            },
            "raw": {
                "unprivileged_user_namespaces": _command(
                    [
                        "/usr/sbin/sysctl",
                        "--values",
                        "kernel.unprivileged_userns_clone",
                    ],
                    stdout="1\n",
                ),
                "pidfd_open": {"pid": 4242, "flags": 0, "outcome": "opened"},
                "sealed_memfd": {
                    "required_seals": 15,
                    "observed_seals": 15,
                    "outcome": "sealed",
                },
                "clone3_into_cgroup": {
                    "flags": "CLONE_INTO_CGROUP",
                    "cgroup_fd": -1,
                    "returncode": -1,
                    "errno": 9,
                    "errno_name": "EBADF",
                },
                "cgroup_mount": _command(
                    [
                        "/usr/bin/findmnt",
                        "--json",
                        "--target",
                        "/sys/fs/cgroup",
                        "--output",
                        "TARGET,SOURCE,FSTYPE,OPTIONS",
                    ],
                    stdout=json.dumps(
                        {
                            "filesystems": [
                                {
                                    "target": "/sys/fs/cgroup",
                                    "source": "cgroup2",
                                    "fstype": "cgroup2",
                                    "options": "rw,nosuid,nodev,noexec,relatime",
                                }
                            ]
                        }
                    ),
                ),
                "bpffs_mount": _command(
                    [
                        "/usr/bin/findmnt",
                        "--json",
                        "--target",
                        "/sys/fs/bpf",
                        "--output",
                        "TARGET,SOURCE,FSTYPE,OPTIONS",
                    ],
                    stdout=json.dumps(
                        {
                            "filesystems": [
                                {
                                    "target": "/sys/fs/bpf",
                                    "source": "bpf",
                                    "fstype": "bpf",
                                    "options": "rw,nosuid,nodev,noexec,relatime,mode=700",
                                }
                            ]
                        }
                    ),
                ),
                "bpffs_metadata": {
                    "path": "/sys/fs/bpf",
                    "uid": 0,
                    "gid": 0,
                    "mode": "0700",
                },
                "controllers": {
                    "path": "/sys/fs/cgroup/cgroup.controllers",
                    "contents": controllers_contents,
                    "sha256": hashlib.sha256(controllers_contents.encode()).hexdigest(),
                },
                "delegation": {
                    "path": "/sys/fs/cgroup/cgroup.subtree_control",
                    "contents": delegation_contents,
                    "sha256": hashlib.sha256(delegation_contents.encode()).hexdigest(),
                },
            },
        },
        "runtime": {
            "release": "rootless-runtime-v1",
            "manifest_sha256": _path_digest(
                ROOT / "deploy/task-image-builder/rootless-runtime-v1.json"
            ),
            "binary_sha256": runtime_binaries,
            "dependency_sha256": {},
            "elf_dynamic_readback": dynamic_readback,
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
            "filesystem": "ext4",
            "project_quota": True,
            "empty_job_root": True,
            "cleanup_supported": True,
            "mountpoint": "/var/lib/loom-task-builder",
            "source": source,
            "mount_options": ["prjquota", "rw"],
            "dedicated": True,
            "quota": _quota_state(),
            "raw": {
                "findmnt": _command(
                    [
                        "/usr/bin/findmnt",
                        "--json",
                        "--target",
                        "/var/lib/loom-task-builder",
                        "--output",
                        "TARGET,SOURCE,FSTYPE,OPTIONS",
                    ],
                    stdout=json.dumps(
                        {
                            "filesystems": [
                                {
                                    "target": "/var/lib/loom-task-builder",
                                    "source": source,
                                    "fstype": "ext4",
                                    "options": "rw,prjquota",
                                }
                            ]
                        }
                    ),
                ),
                "lsblk": _command(
                    ["/usr/bin/lsblk", "--noheadings", "--output", "TYPE", source],
                    stdout="lvm\n",
                ),
                "jobs_root": {
                    "path": "/var/lib/loom-task-builder/jobs",
                    "uid": 993,
                    "gid": 980,
                    "mode": "0700",
                    "entries": [],
                },
                "lsattr": _command(
                    [
                        "/usr/bin/lsattr",
                        "-pd",
                        "/var/lib/loom-task-builder/jobs",
                    ],
                    stdout=(
                        "300993 --------------P------- "
                        "/var/lib/loom-task-builder/jobs\n"
                    ),
                ),
                "repquota": _command(
                    [
                        "/usr/sbin/repquota",
                        "-v",
                        "-n",
                        "-p",
                        "-P",
                        "-O",
                        "csv",
                        "/var/lib/loom-task-builder",
                    ],
                    stdout=(
                        "Project,BlockStatus,FileStatus,BlockUsed,BlockSoftLimit,"
                        "BlockHardLimit,BlockGrace,FileUsed,FileSoftLimit,"
                        "FileHardLimit,FileGrace\n"
                        "300993,--,--,0,0,104857600,0,0,0,1000000,0\n"
                    ),
                ),
                "cleanup": copy.deepcopy(cleanup),
            },
        },
        "network": {key: policy["network"][key] for key in network_keys},
        "forbidden_paths_present": [],
        "node_guard": _phase2_absence(),
        "host_receipt": _receipt(host_document),
        "maintenance_receipt": _receipt(maintenance_document),
    }


@functools.lru_cache(maxsize=1)
def _complete_evidence() -> dict[str, Any]:
    policy = _fixture_policy()
    observed_at = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    clusters: list[dict[str, Any]] = []
    ordinal = 1
    for cluster in sorted(policy["clusters"], key=lambda item: item["id"]):
        controller = _controller_cluster(cluster, observed_at, ordinal)
        nodes: list[dict[str, Any]] = []
        for index, node_name in enumerate(sorted(cluster["builder_nodes"]), start=10):
            ordinal += 1
            nodes.append(_node_evidence(cluster, node_name, index, ordinal, observed_at))
        controller["nodes"] = nodes
        clusters.append(controller)
        ordinal += 1
    return {
        "schema": "loom.task-image-builder-prerequisite-conformance/v1",
        "schema_version": 1,
        "collected_at": observed_at,
        "candidate_sha256": _candidate_digest(),
        "policy_version": policy["policy_version"],
        "policy_sha256": _fingerprint(policy),
        "policy_file_sha256": _path_digest(POLICY),
        "release_name": "host-release-v1",
        "release_sha256": _path_digest(ROOT / "deploy/task-image-builder/host-release-v1.json"),
        "runtime_manifest_sha256": _path_digest(
            ROOT / "deploy/task-image-builder/rootless-runtime-v1.json"
        ),
        **_authority_binding().as_dict(),
        "production_certification_allowed": False,
        "certified_nodes": [],
        "blockers": ["phase2_guard_provider_release_missing"],
        "control_plane_services": policy["control_plane_services"],
        "clusters": clusters,
    }


def _evidence() -> dict[str, Any]:
    return copy.deepcopy(_complete_evidence())


def _failures(evidence: dict[str, Any]) -> list[str]:
    return CONFORMANCE.verify_evidence(evidence, CONFORMANCE.load_policy(POLICY))


def _refresh_receipt(receipt: dict[str, Any], *, rechain: bool = False) -> None:
    document = receipt["document"]
    if rechain:
        previous = "0" * 64
        for sequence, raw_event in enumerate(document["events"]):
            event = dict(raw_event)
            event.pop("event_hash", None)
            event["sequence"] = sequence
            event["previous_hash"] = previous
            event_hash = hashlib.sha256(_canonical(event)).hexdigest()
            event["event_hash"] = event_hash
            document["events"][sequence] = event
            previous = event_hash
    receipt["sha256"] = hashlib.sha256(_canonical(document) + b"\n").hexdigest()


def _mutate_candidate_digest(item: dict[str, Any]) -> None:
    item["candidate_sha256"] = "f" * 64


def _mutate_cluster_policy_file_digest(item: dict[str, Any]) -> None:
    item["clusters"][0]["policy_file_sha256"] = "f" * 64


def _mutate_node_release_digest(item: dict[str, Any]) -> None:
    item["clusters"][0]["nodes"][0]["release_sha256"] = "f" * 64


def _mutate_package_signature(item: dict[str, Any]) -> None:
    item["clusters"][0]["nodes"][0]["packages"]["source"]["signer_fingerprint"] = "A" * 40


def _mutate_helper_digest(item: dict[str, Any]) -> None:
    item["clusters"][0]["nodes"][0]["packages"]["helpers"][0]["mode"] = "4775"


def _mutate_runtime_manifest_digest(item: dict[str, Any]) -> None:
    item["clusters"][0]["nodes"][0]["runtime"]["manifest_sha256"] = "f" * 64


def _mutate_runtime_needed_entry(item: dict[str, Any]) -> None:
    readbacks = item["clusters"][0]["nodes"][0]["runtime"]["elf_dynamic_readback"]
    first_name = sorted(readbacks)[0]
    readbacks[first_name]["stdout"] = " 0x0000000000000001 (NEEDED) Shared library: [libc.so.6]\n"


def _mutate_cgroup_readback_digest(item: dict[str, Any]) -> None:
    item["clusters"][0]["nodes"][0]["kernel"]["slurm_cgroup_readback"]["sha256"] = "f" * 64


def _mutate_slurm_legacy_receipt(item: dict[str, Any]) -> None:
    receipt = item["clusters"][0]["slurm_receipt"]
    receipt["document"]["legacy_pre_fingerprint"] = "f" * 64
    _refresh_receipt(receipt)


def _mutate_slurm_pre_legacy_state(item: dict[str, Any]) -> None:
    receipt = item["clusters"][0]["slurm_receipt"]
    document = receipt["document"]
    document["pre_state"]["legacy"]["reservation"]["node"] = "forged-pre-state-node"
    document["events"][0]["data"] = {"state": document["pre_state"]}
    _refresh_receipt(receipt, rechain=True)


def _mutate_slurm_command_failure(item: dict[str, Any]) -> None:
    receipt = item["clusters"][0]["slurm_receipt"]
    receipt["document"]["command_outcome"]["returncode"] = 1
    _refresh_receipt(receipt)


def _mutate_slurm_post_readback_failure(item: dict[str, Any]) -> None:
    receipt = item["clusters"][0]["slurm_receipt"]
    receipt["document"]["post_readback_error"] = "readback failed"
    _refresh_receipt(receipt)


def _mutate_slurm_event_binding(item: dict[str, Any]) -> None:
    receipt = item["clusters"][0]["slurm_receipt"]
    converged = next(
        event for event in receipt["document"]["events"] if event["type"] == "converged"
    )
    converged["data"]["legacy_unchanged"] = False
    _refresh_receipt(receipt, rechain=True)


def _mutate_host_receipt_terminal(item: dict[str, Any]) -> None:
    receipt = item["clusters"][0]["nodes"][0]["host_receipt"]
    receipt["document"]["terminal_state"] = "rolled_back"
    _refresh_receipt(receipt)


def _mutate_host_receipt_inert_boundary(item: dict[str, Any]) -> None:
    receipt = item["clusters"][0]["nodes"][0]["host_receipt"]
    receipt["document"]["production_certification_allowed"] = True
    _refresh_receipt(receipt)


def _mutate_host_receipt_rollback_claim(item: dict[str, Any]) -> None:
    receipt = item["clusters"][0]["nodes"][0]["host_receipt"]
    receipt["document"]["rollback_verified"] = True
    _refresh_receipt(receipt)


def _mutate_host_receipt_bundle_binding(item: dict[str, Any]) -> None:
    receipt = item["clusters"][0]["nodes"][0]["host_receipt"]
    receipt["document"]["post_state"]["bundle_digest"] = "f" * 64
    _refresh_receipt(receipt)


def _mutate_host_receipt_event_binding(item: dict[str, Any]) -> None:
    receipt = item["clusters"][0]["nodes"][0]["host_receipt"]
    prepared = next(
        event for event in receipt["document"]["events"] if event["type"] == "host_prepared"
    )
    prepared["data"]["activation_required"] = False
    _refresh_receipt(receipt, rechain=True)


def _mutate_maintenance_receipt_terminal(item: dict[str, Any]) -> None:
    receipt = item["clusters"][0]["nodes"][0]["maintenance_receipt"]
    receipt["document"]["terminal_state"] = "applying"
    _refresh_receipt(receipt)


def _mutate_maintenance_receipt_inert_boundary(item: dict[str, Any]) -> None:
    receipt = item["clusters"][0]["nodes"][0]["maintenance_receipt"]
    receipt["document"]["certified_nodes"] = ["trt-gb10-1"]
    _refresh_receipt(receipt)


def _mutate_maintenance_emergency_containment(item: dict[str, Any]) -> None:
    receipt = item["clusters"][0]["nodes"][0]["maintenance_receipt"]
    receipt["document"]["observations"]["emergency_containment"] = {"state": "failed"}
    _refresh_receipt(receipt)


def _mutate_maintenance_daemon_inactive(item: dict[str, Any]) -> None:
    receipt = item["clusters"][0]["nodes"][0]["maintenance_receipt"]
    receipt["document"]["observations"]["daemon"]["restart"]["state"] = "inactive"
    receipt["document"]["observations"]["daemon"]["check"]["state"] = "inactive"
    _refresh_receipt(receipt)


def _mutate_maintenance_admission_exclusive(item: dict[str, Any]) -> None:
    receipt = item["clusters"][0]["nodes"][0]["maintenance_receipt"]
    receipt["document"]["observations"]["admission"]["builder"]["command"].append("--exclusive")
    _refresh_receipt(receipt)


def _mutate_maintenance_admission_override(item: dict[str, Any]) -> None:
    receipt = item["clusters"][0]["nodes"][0]["maintenance_receipt"]
    receipt["document"]["observations"]["admission"]["builder"]["command"].append(
        "--account=loom-staging"
    )
    _refresh_receipt(receipt)


def _mutate_maintenance_reservation_delete_failure(item: dict[str, Any]) -> None:
    receipt = item["clusters"][0]["nodes"][0]["maintenance_receipt"]
    receipt["document"]["observations"]["reservation"]["delete"]["returncode"] = 1
    _refresh_receipt(receipt)


def _mutate_maintenance_reservation_command(item: dict[str, Any]) -> None:
    receipt = item["clusters"][0]["nodes"][0]["maintenance_receipt"]
    receipt["document"]["observations"]["reservation"]["create"]["command"] = ["/bin/true"]
    created = next(
        event for event in receipt["document"]["events"] if event["type"] == "reservation_created"
    )
    created["data"]["create"]["command"] = ["/bin/true"]
    _refresh_receipt(receipt, rechain=True)


def _mutate_maintenance_reservation_prior_raw(item: dict[str, Any]) -> None:
    receipt = item["clusters"][0]["nodes"][0]["maintenance_receipt"]
    reservation = receipt["document"]["observations"]["reservation"]
    reservation["prior_readback"]["stdout"] += (
        f"ReservationName={reservation['name']} Nodes=forged-node Users=loom-builder State=ACTIVE\n"
    )
    _refresh_receipt(receipt)


def _mutate_maintenance_reservation_create_raw(item: dict[str, Any]) -> None:
    receipt = item["clusters"][0]["nodes"][0]["maintenance_receipt"]
    document = receipt["document"]
    reservation = document["observations"]["reservation"]
    reservation["create_readback"]["stdout"] = (
        "ReservationName=legacy_operator_hold Nodes=legacy-node Users=operator State=ACTIVE\n"
    )
    created = next(event for event in document["events"] if event["type"] == "reservation_created")
    created["data"]["create_readback"] = reservation["create_readback"]
    _refresh_receipt(receipt, rechain=True)


def _mutate_maintenance_reservation_delete_raw(item: dict[str, Any]) -> None:
    receipt = item["clusters"][0]["nodes"][0]["maintenance_receipt"]
    document = receipt["document"]
    reservation = document["observations"]["reservation"]
    reservation["delete_readback"]["stdout"] += (
        f"ReservationName={reservation['name']} Nodes={item['clusters'][0]['nodes'][0]['name']} "
        "Users=loom-builder State=ACTIVE\n"
    )
    deleted = next(event for event in document["events"] if event["type"] == "reservation_deleted")
    deleted["data"]["delete_readback"] = reservation["delete_readback"]
    _refresh_receipt(receipt, rechain=True)


def _mutate_maintenance_smoke_release_failure(item: dict[str, Any]) -> None:
    receipt = item["clusters"][0]["nodes"][0]["maintenance_receipt"]
    released = next(
        event for event in receipt["document"]["events"] if event["type"] == "smoke_released"
    )
    released["data"]["release"]["returncode"] = 1
    released["data"]["release"]["outcome"] = "failed"
    _refresh_receipt(receipt, rechain=True)


def _mutate_maintenance_prestate_incomplete(item: dict[str, Any]) -> None:
    receipt = item["clusters"][0]["nodes"][0]["maintenance_receipt"]
    del receipt["document"]["pre_state"]["allocated_tres"]
    recorded = next(
        event for event in receipt["document"]["events"] if event["type"] == "pre_state_recorded"
    )
    recorded["data"]["pre_state"] = receipt["document"]["pre_state"]
    _refresh_receipt(receipt, rechain=True)


def _mutate_maintenance_reservation_event_binding(item: dict[str, Any]) -> None:
    receipt = item["clusters"][0]["nodes"][0]["maintenance_receipt"]
    deleted = next(
        event for event in receipt["document"]["events"] if event["type"] == "reservation_deleted"
    )
    deleted["data"] = {"name": "wrong-reservation"}
    _refresh_receipt(receipt, rechain=True)


def _mutate_smoke_device_program(item: dict[str, Any]) -> None:
    receipt = item["clusters"][0]["nodes"][0]["maintenance_receipt"]
    document = receipt["document"]
    document["observations"]["smoke"]["cgroup"]["devices"]["programs"][0]["attach_type"] = (
        "cgroup_inet_ingress"
    )
    observed = next(event for event in document["events"] if event["type"] == "smoke_observed")
    observed["data"]["evidence"]["controls"]["devices"]["programs"][0]["attach_type"] = (
        "cgroup_inet_ingress"
    )
    _refresh_receipt(receipt, rechain=True)


def _mutate_smoke_accounting(item: dict[str, Any]) -> None:
    receipt = item["clusters"][0]["nodes"][0]["maintenance_receipt"]
    completed = next(
        event for event in receipt["document"]["events"] if event["type"] == "smoke_completed"
    )
    completed["data"]["accounting"]["top_level"]["state"] = "RUNNING"
    _refresh_receipt(receipt, rechain=True)


def _mutate_smoke_accounting_readback(item: dict[str, Any]) -> None:
    receipt = item["clusters"][0]["nodes"][0]["maintenance_receipt"]
    completed = next(
        event for event in receipt["document"]["events"] if event["type"] == "smoke_completed"
    )
    job_id = completed["data"]["job_id"]
    completed["data"]["accounting"]["readback"]["stdout"] = f"{job_id}|RUNNING|0:0\n"
    _refresh_receipt(receipt, rechain=True)


def _mutate_smoke_accounting_command(item: dict[str, Any]) -> None:
    receipt = item["clusters"][0]["nodes"][0]["maintenance_receipt"]
    completed = next(
        event for event in receipt["document"]["events"] if event["type"] == "smoke_completed"
    )
    completed["data"]["accounting"]["readback"]["command"] = ["/bin/true"]
    _refresh_receipt(receipt, rechain=True)


def _mutate_smoke_cleanup_command(item: dict[str, Any]) -> None:
    receipt = item["clusters"][0]["nodes"][0]["maintenance_receipt"]
    cleaned = next(
        event for event in receipt["document"]["events"] if event["type"] == "smoke_cleaned"
    )
    cleaned["data"]["cleanup"]["command"] = ["/bin/true"]
    _refresh_receipt(receipt, rechain=True)


def _mutate_smoke_cleanup(item: dict[str, Any]) -> None:
    receipt = item["clusters"][0]["nodes"][0]["maintenance_receipt"]
    document = receipt["document"]
    document["observations"]["smoke"]["cleanup"]["job_directory_absent"] = False
    cleaned = next(event for event in document["events"] if event["type"] == "smoke_cleaned")
    cleaned["data"]["cleanup"]["job_directory_absent"] = False
    _refresh_receipt(receipt, rechain=True)


def _mutate_smoke_surviving_process(item: dict[str, Any]) -> None:
    receipt = item["clusters"][0]["nodes"][0]["maintenance_receipt"]
    document = receipt["document"]
    document["observations"]["smoke"]["cleanup"]["processes_absent"] = False
    cleaned = next(event for event in document["events"] if event["type"] == "smoke_cleaned")
    cleaned["data"]["cleanup"]["processes_absent"] = False
    _refresh_receipt(receipt, rechain=True)


def _mutate_smoke_surviving_mount(item: dict[str, Any]) -> None:
    receipt = item["clusters"][0]["nodes"][0]["maintenance_receipt"]
    document = receipt["document"]
    document["observations"]["smoke"]["cleanup"]["mounts_absent"] = False
    cleaned = next(event for event in document["events"] if event["type"] == "smoke_cleaned")
    cleaned["data"]["cleanup"]["mounts_absent"] = False
    _refresh_receipt(receipt, rechain=True)


def _mutate_node_observation_age(item: dict[str, Any]) -> None:
    item["clusters"][0]["nodes"][0]["observed_at"] = "2020-01-01T00:00:00Z"


def _mutate_kernel_raw_sysctl(item: dict[str, Any]) -> None:
    kernel = item["clusters"][0]["nodes"][0]["kernel"]
    kernel["raw"]["unprivileged_user_namespaces"]["stdout"] = "0\n"


def _mutate_kernel_raw_clone3(item: dict[str, Any]) -> None:
    kernel = item["clusters"][0]["nodes"][0]["kernel"]
    delegation = kernel["raw"]["delegation"]
    delegation["contents"] = "cpu io pids\n"
    delegation["sha256"] = hashlib.sha256(delegation["contents"].encode()).hexdigest()


def _mutate_storage_raw_quota(item: dict[str, Any]) -> None:
    storage = item["clusters"][0]["nodes"][0]["storage"]
    storage["raw"]["repquota"]["stdout"] = storage["raw"]["repquota"]["stdout"].replace(
        ",104857600,", ",0,"
    )


def _mutate_storage_raw_jobs_root(item: dict[str, Any]) -> None:
    storage = item["clusters"][0]["nodes"][0]["storage"]
    storage["raw"]["findmnt"]["stdout"] = storage["raw"]["findmnt"]["stdout"].replace(
        storage["source"], "/dev/mapper/forged-source"
    )


def _mutate_storage_raw_cleanup(item: dict[str, Any]) -> None:
    storage = item["clusters"][0]["nodes"][0]["storage"]
    storage["raw"]["cleanup"]["command"] = ["/bin/true", "a", "b", "c", "d"]


def _mutate_slurm_loopback_binding(item: dict[str, Any]) -> None:
    binding = item["clusters"][0]["nodes"][0]["slurm_identity"]
    binding["resolved_addresses"] = ["127.0.0.1"]
    binding["local_addresses"] = ["127.0.0.1"]


def _mutate_slurm_resolution_query(item: dict[str, Any]) -> None:
    binding = item["clusters"][0]["nodes"][0]["slurm_identity"]
    binding["resolution"]["query"] = "forged-node-address.example.test"


def _mutate_slurm_advertised_builder_feature(item: dict[str, Any]) -> None:
    binding = item["clusters"][0]["nodes"][0]["slurm_identity"]
    binding["readback"]["stdout"] = binding["readback"]["stdout"].replace(
        "AvailableFeatures=(null)",
        "AvailableFeatures=loom_rootless_buildkit",
    )


def _mutate_phase2_artifact_present(item: dict[str, Any]) -> None:
    guard = item["clusters"][0]["nodes"][0]["node_guard"]
    guard["artifacts"][0]["present"] = True


def _mutate_phase2_unit_loaded(item: dict[str, Any]) -> None:
    guard = item["clusters"][0]["nodes"][0]["node_guard"]
    guard["unit_readback"][0]["stdout"] = (
        "LoadState=loaded\nActiveState=inactive\n"
        "FragmentPath=/etc/systemd/system/loom-task-builder-node-guard.service\n"
    )


def _mutate_phase2_process_running(item: dict[str, Any]) -> None:
    guard = item["clusters"][0]["nodes"][0]["node_guard"]
    guard["process_readback"][0].update({"returncode": 0, "stdout": "4242\n"})


def _mutate_receipt_operation_identity(item: dict[str, Any]) -> None:
    cluster = item["clusters"][0]
    node = cluster["nodes"][0]
    operation_id = "not-a-uuid"
    host = node["host_receipt"]
    host["document"] = _host_receipt(cluster["id"], node["name"], operation_id)
    _refresh_receipt(host)
    maintenance = node["maintenance_receipt"]
    maintenance["document"] = _maintenance_receipt(
        cluster["id"],
        node["name"],
        operation_id,
        "1902",
    )
    _refresh_receipt(maintenance)


def _mutate_slurm_receipt_operation_identity(item: dict[str, Any]) -> None:
    cluster = item["clusters"][0]
    receipt = cluster["slurm_receipt"]
    receipt["document"] = _slurm_receipt(cluster["id"], "not-a-uuid")
    _refresh_receipt(receipt)


def test_schema_rejects_the_legacy_boolean_only_envelope_without_raw_receipts() -> None:
    schema = json.loads(SCHEMA.read_text(encoding="utf-8"))
    validator = Draft202012Validator(schema)

    locations = {
        ".".join(str(part) for part in error.absolute_path)
        for error in validator.iter_errors(_legacy_evidence())
    }

    assert "" in locations
    assert "clusters.0" in locations
    assert "clusters.0.nodes.0" in locations


def test_schema_and_complete_phase_one_evidence_are_valid_but_not_certifiable() -> None:
    schema = json.loads(SCHEMA.read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(schema)
    policy = CONFORMANCE.load_policy(POLICY)

    assert _failures(_evidence()) == []
    assert CONFORMANCE.certification_blockers(policy) == ("phase2_guard_provider_release_missing",)


@pytest.mark.parametrize(
    ("old", "new"),
    [
        (
            'unconditional_blockers = ["phase2_guard_provider_release_missing"]',
            'unconditional_blockers = ["caller-selected-blocker"]',
        ),
        ('user = "loom-builder"', 'user = "caller-selected-builder"'),
        ('group = "loom-task-builder"', 'group = "caller-selected-group"'),
        ("uid = 993", "uid = 994"),
        ("gid = 980", "gid = 981"),
        ("subid_start = 3000000", "subid_start = 4000000"),
        ("subid_count = 65536", "subid_count = 32768"),
        ('home = "/nonexistent"', 'home = "/tmp"'),
        ('shell = "/usr/sbin/nologin"', 'shell = "/bin/sh"'),
        (
            'forbidden_supplementary_groups = ["docker", "root", "sudo"]',
            'forbidden_supplementary_groups = ["docker", "root"]',
        ),
        ('controller = "TRT-EAI-OLDLAB-1"', 'controller = "caller-controller"'),
        ('architecture = "x86_64"', 'architecture = "aarch64"'),
        ('  "trt-eai-oldlab-5",', '  "trt-eai-oldlab-7",'),
        ('legacy_base_qos = "normal"', 'legacy_base_qos = "caller-qos"'),
        (
            'legacy_reservation_node = "trt-eai-oldlab-6"',
            'legacy_reservation_node = "trt-eai-oldlab-5"',
        ),
        (
            'legacy_reservation_partition = "all"',
            'legacy_reservation_partition = "loom-staging"',
        ),
    ],
)
def test_policy_loader_rejects_mutated_phase_one_authority(
    tmp_path: Path,
    old: str,
    new: str,
) -> None:
    source = POLICY.read_text(encoding="utf-8")
    assert source.count(old) == 1
    policy_path = tmp_path / POLICY.name
    policy_path.write_text(source.replace(old, new, 1), encoding="utf-8")
    for source_path in (
        ROOT / "deploy/task-image-builder/rootless-runtime-v1.json",
        ROOT / "deploy/task-image-builder/host-release-v1.json",
    ):
        (tmp_path / source_path.name).write_bytes(source_path.read_bytes())

    with pytest.raises(CONFORMANCE.ConformanceError, match="policy"):
        CONFORMANCE.load_policy(policy_path)


@pytest.mark.parametrize(
    ("mutate", "expected"),
    [
        (_mutate_candidate_digest, "candidate digest"),
        (_mutate_cluster_policy_file_digest, "policy digest"),
        (_mutate_node_release_digest, "release digest"),
        (_mutate_package_signature, "package/source signature"),
        (_mutate_helper_digest, "UID-map helper"),
        (_mutate_runtime_manifest_digest, "runtime/dependency"),
        (_mutate_runtime_needed_entry, "runtime/dependency"),
        (_mutate_cgroup_readback_digest, "cgroup readback"),
        (_mutate_slurm_legacy_receipt, "legacy Slurm fingerprints"),
        (_mutate_slurm_pre_legacy_state, "legacy Slurm fingerprints"),
        (_mutate_slurm_command_failure, "Slurm convergence receipt"),
        (_mutate_slurm_post_readback_failure, "Slurm convergence receipt"),
        (_mutate_slurm_event_binding, "Slurm receipt event binding"),
        (_mutate_host_receipt_terminal, "host receipt terminal"),
        (_mutate_host_receipt_inert_boundary, "host receipt inert boundary"),
        (_mutate_host_receipt_rollback_claim, "host receipt terminal"),
        (_mutate_host_receipt_bundle_binding, "host receipt raw facts"),
        (_mutate_host_receipt_event_binding, "host receipt event binding"),
        (_mutate_maintenance_receipt_terminal, "maintenance receipt terminal"),
        (
            _mutate_maintenance_receipt_inert_boundary,
            "maintenance receipt inert boundary",
        ),
        (_mutate_maintenance_emergency_containment, "emergency containment"),
        (_mutate_maintenance_daemon_inactive, "cgroup daemon readback"),
        (_mutate_maintenance_admission_exclusive, "Slurm admission"),
        (_mutate_maintenance_admission_override, "Slurm admission"),
        (_mutate_maintenance_reservation_delete_failure, "reservation lifecycle"),
        (_mutate_maintenance_reservation_command, "reservation lifecycle"),
        (_mutate_maintenance_reservation_prior_raw, "reservation lifecycle"),
        (_mutate_maintenance_reservation_create_raw, "reservation lifecycle"),
        (_mutate_maintenance_reservation_delete_raw, "reservation lifecycle"),
        (_mutate_maintenance_smoke_release_failure, "smoke release"),
        (_mutate_maintenance_prestate_incomplete, "maintenance pre-state"),
        (_mutate_maintenance_reservation_event_binding, "reservation event chain"),
        (_mutate_smoke_device_program, "device containment"),
        (_mutate_smoke_accounting, "smoke accounting"),
        (_mutate_smoke_accounting_readback, "smoke accounting"),
        (_mutate_smoke_accounting_command, "smoke accounting"),
        (_mutate_smoke_cleanup, "smoke cleanup"),
        (_mutate_smoke_surviving_process, "smoke cleanup"),
        (_mutate_smoke_surviving_mount, "smoke cleanup"),
        (_mutate_smoke_cleanup_command, "smoke cleanup"),
        (_mutate_node_observation_age, "observation freshness"),
        (_mutate_kernel_raw_sysctl, "cgroup readback"),
        (_mutate_kernel_raw_clone3, "cgroup readback"),
        (_mutate_storage_raw_quota, "mount/quota raw readback"),
        (_mutate_storage_raw_jobs_root, "mount/quota raw readback"),
        (_mutate_storage_raw_cleanup, "mount/quota raw readback"),
        (_mutate_slurm_loopback_binding, "Slurm host binding"),
        (_mutate_slurm_resolution_query, "Slurm host binding"),
        (_mutate_slurm_advertised_builder_feature, "Slurm host binding"),
        (_mutate_phase2_artifact_present, "node_guard.artifacts"),
        (_mutate_phase2_unit_loaded, "Phase 2 node guard"),
        (_mutate_phase2_process_running, "Phase 2 node guard"),
        (_mutate_receipt_operation_identity, "operation identity"),
        (_mutate_slurm_receipt_operation_identity, "operation identity"),
    ],
)
def test_raw_collector_and_receipt_facts_are_semantically_verified(
    mutate: Any,
    expected: str,
) -> None:
    evidence = _evidence()
    mutate(evidence)

    assert any(expected in failure for failure in _failures(evidence))


@pytest.mark.parametrize(
    ("mutate", "expected"),
    [
        (lambda item: item.__setitem__("policy_sha256", "0" * 64), "policy digest"),
        (
            lambda item: item.__setitem__("authority_manifest_sha256", "0" * 64),
            "authority manifest digest",
        ),
        (lambda item: item["clusters"].pop(), "cluster set"),
        (
            lambda item: item["clusters"][0].__setitem__("architecture", "x86_64"),
            "architecture",
        ),
        (
            lambda item: item["clusters"][0]["controller_identity"].__setitem__("uid", 992),
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
            lambda item: item["clusters"][0]["slurm"]["legacy_builder"]["reservation"].__setitem__(
                "node", "trt-eai-oldlab-5"
            ),
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


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("access_key", "redacted"),
        ("note", "sk-placeholder-secret-12345"),
        ("note", "github_pat_PLACEHOLDER12345"),
        ("note", "loom_api_PLACEHOLDER12345"),
        ("note", "--token"),
    ],
)
def test_cli_verify_rejects_additional_secret_forms_without_echo(
    tmp_path: Path,
    field: str,
    value: str,
) -> None:
    evidence = _evidence()
    evidence["clusters"][0][field] = value
    evidence_path = tmp_path / "secret-like-evidence.json"
    evidence_path.write_text(json.dumps(evidence), encoding="utf-8")

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

    assert result.returncode == 2
    assert "secret-like" in result.stderr
    assert value not in result.stderr


@pytest.mark.parametrize(
    "value",
    [
        "Authorization: Basic QWxhZGRpbjpvcGVuIHNlc2FtZQ==",
        (
            'Authorization: Digest username="loom", realm="builder", '
            'nonce="abcdef0123456789", response="0123456789abcdef"'
        ),
        "-----BEGIN OPENSSH PRIVATE KEY-----\nplaceholder\n-----END OPENSSH PRIVATE KEY-----",
        "AKIAIOSFODNN7EXAMPLE",
        "AIzaSyD-9tSrke72PouQMnMX-a7eZSW0jkFMBWY",
        "ya29.a0AfH6SMBplaceholderGoogleOAuthCredential",
        "xoxb-" + "123456789012-123456789012-abcdefghijklmnopqrstuvwxyzABCD",
        "sk_" + "live_" + "51M3mABCDEFGHIJKLMNopqrstuvwxyz",
        "whsec_0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ",
    ],
)
def test_cli_verify_rejects_authorization_pem_and_provider_tokens_without_echo(
    tmp_path: Path,
    value: str,
) -> None:
    evidence = _evidence()
    evidence["clusters"][0]["observation_note"] = value
    evidence_path = tmp_path / "provider-secret-evidence.json"
    evidence_path.write_text(json.dumps(evidence), encoding="utf-8")

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

    assert result.returncode == 2
    assert result.stdout == ""
    assert "secret-like" in result.stderr
    assert value not in result.stderr


def test_cli_verify_rejects_invalid_utf8_without_traceback(tmp_path: Path) -> None:
    evidence_path = tmp_path / "invalid-utf8.json"
    evidence_path.write_bytes(b'{"schema":"broken","value":"\xff"}\n')

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

    assert result.returncode == 2
    assert result.stdout == ""
    assert result.stderr == "error: input is not valid JSON\n"


def test_verifier_reads_input_through_a_no_follow_descriptor(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    evidence_path = tmp_path / "evidence.json"
    evidence_path.write_bytes(b"{}\n")
    real_open = os.open
    opened_flags: list[int] = []

    def recording_open(path: Path, flags: int, mode: int = 0o777) -> int:
        opened_flags.append(flags)
        return real_open(path, flags, mode)

    monkeypatch.setattr(CONFORMANCE.os, "open", recording_open)

    assert CONFORMANCE._read_regular(evidence_path) == b"{}\n"
    assert len(opened_flags) == 1
    assert opened_flags[0] & os.O_NOFOLLOW


def test_cli_verify_rejects_unpaired_fifo_without_blocking(tmp_path: Path) -> None:
    evidence_path = tmp_path / "evidence.fifo"
    os.mkfifo(evidence_path, mode=0o600)

    try:
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
            timeout=2,
        )
    except subprocess.TimeoutExpired:
        pytest.fail("verifier FIFO blocked before descriptor type validation")

    assert result.returncode == 2
    assert result.stdout == ""
    assert result.stderr == "error: input must be a regular non-symlink file\n"


def test_verifier_rejects_input_metadata_changes_during_descriptor_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    evidence_path = tmp_path / "evidence.json"
    evidence_path.write_bytes(b"{}\n")
    real_fstat = os.fstat
    calls = 0

    def changing_fstat(descriptor: int) -> object:
        nonlocal calls
        calls += 1
        metadata = real_fstat(descriptor)
        if calls == 1:
            return metadata
        return SimpleNamespace(
            st_mode=metadata.st_mode,
            st_dev=metadata.st_dev,
            st_ino=metadata.st_ino,
            st_size=metadata.st_size,
            st_mtime_ns=metadata.st_mtime_ns + 1,
            st_ctime_ns=metadata.st_ctime_ns,
        )

    monkeypatch.setattr(CONFORMANCE.os, "fstat", changing_fstat)

    with pytest.raises(CONFORMANCE.ConformanceError, match="changed while being read"):
        CONFORMANCE._read_regular(evidence_path)

    assert calls == 2


def test_cli_verify_rejects_json_above_the_nesting_limit(tmp_path: Path) -> None:
    evidence_path = tmp_path / "too-deep.json"
    evidence_path.write_bytes(b"[" * 65 + b"0" + b"]" * 65)

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

    assert result.returncode == 2
    assert result.stdout == ""
    assert result.stderr == "error: input exceeds the JSON nesting limit\n"


def test_cli_verify_converts_parser_recursion_to_a_generic_error(tmp_path: Path) -> None:
    evidence_path = tmp_path / "recursive.json"
    evidence_path.write_bytes(b"[" * 2_048 + b"0" + b"]" * 2_048)

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

    assert result.returncode == 2
    assert result.stdout == ""
    assert result.stderr == "error: input is not valid JSON\n"
    assert "Traceback" not in result.stderr


@pytest.mark.parametrize(
    ("target_name", "expected"),
    [
        ("rootless-runtime-v1.json", "rootless runtime manifest is invalid"),
        ("host-release-v1.json", "host release is invalid"),
    ],
)
def test_cli_plan_converts_policy_json_recursion_to_a_generic_error(
    tmp_path: Path,
    target_name: str,
    expected: str,
) -> None:
    policy_path = tmp_path / POLICY.name
    policy_path.write_bytes(POLICY.read_bytes())
    for source in (
        ROOT / "deploy/task-image-builder/rootless-runtime-v1.json",
        ROOT / "deploy/task-image-builder/host-release-v1.json",
    ):
        destination = tmp_path / source.name
        if source.name == target_name:
            destination.write_bytes(b"[" * 2_048 + b"0" + b"]" * 2_048)
        else:
            destination.write_bytes(source.read_bytes())

    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "plan",
            "--policy",
            str(policy_path),
        ],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 2
    assert result.stdout == ""
    assert result.stderr == f"error: {expected}\n"
    assert "Traceback" not in result.stderr


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
    assert report["blockers"] == ["phase2_guard_provider_release_missing"]
    assert report["certified_nodes"] == []


def test_cli_verify_accepts_assembled_evidence_above_two_mib(tmp_path: Path) -> None:
    evidence = _evidence()
    receipt = evidence["clusters"][0]["slurm_receipt"]
    receipt["document"]["command_outcome"]["stdout"] = "x" * (2 * 1024 * 1024)
    _refresh_receipt(receipt)
    payload = _canonical(evidence) + b"\n"
    assert 2 * 1024 * 1024 < len(payload) < 16 * 1024 * 1024
    evidence_path = tmp_path / "large-assembled-evidence.json"
    evidence_path.write_bytes(payload)

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
    assert json.loads(result.stdout)["prerequisites_valid"] is True


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


def test_canonical_output_is_owner_readable_under_restrictive_umask(
    tmp_path: Path,
) -> None:
    output = tmp_path / "canonical.json"
    evidence = _evidence()

    previous_umask = os.umask(0o777)
    try:
        CONFORMANCE._write_canonical(output, evidence)
    finally:
        os.umask(previous_umask)

    assert stat.S_IMODE(output.stat().st_mode) == 0o600
