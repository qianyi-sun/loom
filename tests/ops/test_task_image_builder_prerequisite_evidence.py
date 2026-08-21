from __future__ import annotations

import base64
import hashlib
import importlib.util
import json
import os
import stat
import subprocess
import sys
import tomllib
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any

import pytest
from jsonschema import Draft202012Validator

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts/ops/task_image_builder_prerequisite_evidence.py"
POLICY = ROOT / "deploy/task-image-builder/prerequisites-v1.toml"
RELEASE = ROOT / "deploy/task-image-builder/host-release-v1.json"
RUNTIME = ROOT / "deploy/task-image-builder/rootless-runtime-v1.json"
SCHEMA = ROOT / "docs/evidence/task-image-builder-prerequisite-conformance-v1.schema.json"
PHASE2_NAMES = (
    "loom-task-builder-allocation-supervisor",
    "loom-task-builder-node-guard",
    "loom-task-builder-provider",
)


def _load_module() -> ModuleType:
    spec = importlib.util.spec_from_file_location("task_builder_evidence_test", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


EVIDENCE = _load_module()


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")


def _sha_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha_path(path: Path) -> str:
    return _sha_bytes(path.read_bytes())


def _fingerprint(value: object) -> str:
    return _sha_bytes(_canonical(value))


def _authority_binding() -> Any:
    return EVIDENCE.authority.load_authority_binding(ROOT)


def _events(items: list[tuple[str, Mapping[str, object]]]) -> list[dict[str, object]]:
    previous = "0" * 64
    events: list[dict[str, object]] = []
    for sequence, (event_type, data) in enumerate(items):
        event: dict[str, object] = {
            "sequence": sequence,
            "type": event_type,
            "previous_hash": previous,
            "data": dict(data),
        }
        event["event_hash"] = _fingerprint(event)
        previous = str(event["event_hash"])
        events.append(event)
    return events


def _write_receipt(path: Path, document: Mapping[str, object]) -> None:
    path.write_bytes(_canonical(document) + b"\n")
    path.chmod(0o600)


def _policy() -> dict[str, Any]:
    return tomllib.loads(POLICY.read_text(encoding="utf-8"))


def _release() -> dict[str, Any]:
    return json.loads(RELEASE.read_text(encoding="utf-8"))


def _runtime() -> dict[str, Any]:
    return json.loads(RUNTIME.read_text(encoding="utf-8"))


def _cluster(cluster_id: str) -> dict[str, Any]:
    matches = [item for item in _policy()["clusters"] if item["id"] == cluster_id]
    assert len(matches) == 1
    return matches[0]


def _slurm_candidate_digest() -> str:
    components = {
        "policy": _sha_path(POLICY),
        **_authority_binding().as_dict(),
    }
    return _fingerprint(components)


def _host_candidate_digest() -> str:
    components = {
        "policy": _sha_path(POLICY),
        "release": _sha_path(RELEASE),
        "runtime": _sha_path(RUNTIME),
        **_authority_binding().as_dict(),
    }
    return _fingerprint(components)


def _maintenance_candidate_digest() -> str:
    return _fingerprint(
        {
            "policy": _sha_path(POLICY),
            **_authority_binding().as_dict(),
        }
    )


def _legacy(cluster: Mapping[str, Any]) -> dict[str, object]:
    policy = _policy()
    guard = policy["legacy_guard"]
    return {
        "qos": {
            "name": guard["qos"],
            "flags": ["DenyOnLimit"],
            "priority": 0,
            "max_jobs_per_user": 1,
            "max_submit_jobs_per_user": 1,
            "max_wall": "04:00:00",
            "group_tres": {},
        },
        "association": {
            "cluster": cluster["slurm_cluster"],
            "account": guard["account"],
            "user": guard["user"],
            "qos": sorted([cluster["legacy_base_qos"], guard["qos"]]),
            "default_qos": cluster["legacy_base_qos"],
        },
        "reservation": {
            "name": guard["reservation"],
            "node": cluster["legacy_reservation_node"],
            "node_count": 1,
            "partition": cluster["legacy_reservation_partition"],
            "users": [guard["user"]],
            "accounts": [guard["account"]],
            "state": "ACTIVE",
            "flags": ["IGNORE_JOBS", "SPEC_NODES"],
        },
    }


def _slurm_receipt(cluster_id: str, operation_id: str) -> dict[str, object]:
    cluster = _cluster(cluster_id)
    resources = _policy()["resource_profile"]
    legacy = _legacy(cluster)
    state: dict[str, object] = {
        "partition": {
            "name": cluster["builder_partition"],
            "line": cluster["builder_partition_line"],
        },
        "account": {"name": cluster["slurm_account"]},
        "qos": {
            "name": cluster["slurm_qos"],
            "flags": ["DenyOnLimit"],
            "priority": 0,
            "max_jobs_per_user": resources["max_jobs_per_user"],
            "max_submit_jobs_per_user": resources["max_submit_jobs_per_user"],
            "max_wall": resources["wall_time"],
            "group_tres": {
                "cpu": resources["cpus"],
                "memory_mib": resources["memory_mib"],
                "nodes": 1,
            },
        },
        "association": {
            "cluster": cluster["slurm_cluster"],
            "account": cluster["slurm_account"],
            "user": "loom-builder",
            "partition": cluster["builder_partition"],
            "qos": [cluster["slurm_qos"]],
            "default_qos": cluster["slurm_qos"],
        },
        "legacy": legacy,
    }
    legacy_fingerprint = _fingerprint(legacy)
    events = _events(
        [
            ("pre_state", {"state": state}),
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
                {"state": state, "readback_error": None, "created_objects": []},
            ),
            ("converged", {"returncode": 0, "legacy_unchanged": True}),
        ]
    )
    return {
        "schema": "loom.task-image-builder-slurm-receipt/v1",
        "operation_id": operation_id,
        "cluster_id": cluster_id,
        "candidate_digest": _slurm_candidate_digest(),
        "policy_digest": _sha_path(POLICY),
        "controller_digest": _sha_path(
            ROOT / "deploy/slurm/install-loom-task-image-builder-controller-identity.sh"
        ),
        "cluster_digest": _fingerprint(cluster),
        **_authority_binding().as_dict(),
        "production_certification_allowed": False,
        "certified_nodes": [],
        "blockers": ["phase2_guard_provider_release_missing"],
        "pre_state": state,
        "post_state": state,
        "legacy_pre_fingerprint": legacy_fingerprint,
        "legacy_post_fingerprint": legacy_fingerprint,
        "created_objects": [],
        "durable_config_backup_digest": "1" * 64,
        "command_outcome": {"returncode": 0, "stdout": "", "stderr": ""},
        "post_readback_error": None,
        "terminal_state": "converged",
        "events": events,
    }


def _quota_state() -> dict[str, object]:
    resources = _policy()["resource_profile"]
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
        "inode_hard_limit": resources["scratch_inodes"],
    }


def _host_receipt(
    cluster_id: str,
    node_name: str,
    operation_id: str,
) -> dict[str, object]:
    cluster = _cluster(cluster_id)
    cgroup_payload = (
        "CgroupPlugin=autodetect\n"
        "ConstrainCores=yes\n"
        "ConstrainRAMSpace=yes\n"
        "ConstrainSwapSpace=yes\n"
        "ConstrainDevices=yes\n"
    )
    cgroup = {
        "kind": "regular",
        "payload_b64": base64.b64encode(cgroup_payload.encode("utf-8")).decode("ascii"),
        "sha256": _sha_bytes(cgroup_payload.encode("utf-8")),
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
    events = _events(
        [
            (
                "pre_state",
                {
                    "binding": {
                        "operation_id": operation_id,
                        "cluster_id": cluster_id,
                        "slurm_node": node_name,
                        "candidate_digest": _host_candidate_digest(),
                        "policy_digest": _sha_path(POLICY),
                    "release_digest": _sha_path(RELEASE),
                    "cluster_digest": _fingerprint(cluster),
                    **_authority_binding().as_dict(),
                    "bundle_digest": facts["bundle_digest"],
                    },
                    "facts": facts,
                    "cgroup": cgroup,
                },
            ),
            ("intent", {"changes": []}),
            ("post_state", {"facts": facts, "cgroup": cgroup}),
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
        "policy_digest": _sha_path(POLICY),
        "release_digest": _sha_path(RELEASE),
        "cluster_digest": _fingerprint(cluster),
        **_authority_binding().as_dict(),
        "production_certification_allowed": False,
        "certified_nodes": [],
        "blockers": ["phase2_guard_provider_release_missing"],
        "bundle_digest": facts["bundle_digest"],
        "pre_state": facts,
        "post_state": facts,
        "cgroup_prestate": cgroup,
        "cgroup_poststate": cgroup,
        "created_inert_artifacts": [],
        "activation_required": True,
        "rollback_verified": None,
        "rollback_source_state": None,
        "terminal_state": "host_prepared",
        "failure": None,
        "events": events,
    }


def _command(command: list[str], *, returncode: int = 0, stdout: str = "") -> dict[str, object]:
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
) -> dict[str, object]:
    cluster = _cluster(cluster_id)
    resources = _policy()["resource_profile"]
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
            "sha256": _sha_bytes(cgroup_contents.encode("utf-8")),
            "contents": cgroup_contents,
        },
    }
    cgroup_path = f"/slurm/uid_993/job_{job_id}/step_batch"
    controls = {
        "cpuset_cpus_effective": "0-7",
        "cpuset_cpu_count": 8,
        "memory_max": resources["memory_mib"] * 1024 * 1024,
        "memory_swap_max": resources["swap_bytes"],
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
    unrelated_reservation = (
        "ReservationName=legacy_operator_hold Nodes=legacy-node Users=operator State=ACTIVE\n"
    )
    created_reservation = (
        f"ReservationName={reservation_name} Nodes={node_name} Users=loom-builder State=ACTIVE\n"
    )
    reservation = {
        "name": reservation_name,
        "prior_readback": _command(
            ["/usr/bin/scontrol", "show", "reservation", "--oneliner"],
            stdout=unrelated_reservation,
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
            stdout=unrelated_reservation + created_reservation,
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
            stdout=unrelated_reservation,
        ),
        "absence": {"name": reservation_name, "absent": True},
    }
    admission_args = [
        f"--account={cluster['slurm_account']}",
        f"--qos={cluster['slurm_qos']}",
        f"--partition={cluster['builder_partition']}",
        f"--cpus-per-task={resources['cpus']}",
        f"--mem={resources['memory_mib']}M",
        f"--time={resources['wall_time']}",
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
                *admission_args,
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
                *admission_args,
                "--wrap=/usr/bin/true",
            ],
            returncode=1,
        ),
    }
    observations = {
        "daemon": {"restart": daemon_state, "check": daemon_state},
        "admission": admission,
        "reservation": reservation,
        "smoke": smoke,
        "emergency_containment": None,
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
    event_data: list[tuple[str, Mapping[str, object]]] = [
        (
            "pre_state_recorded",
            {"pre_state": {"state": "IDLE", "reason": "none", "allocated_tres": "cpu=0,mem=0M"}},
        ),
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
        ("smoke_observed", {"job_id": job_id, "evidence": observed_evidence}),
        (
            "smoke_released",
            {
                "job_id": job_id,
                "release": {
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
                },
            },
        ),
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
    return {
        "schema": "loom.task-image-builder-node-maintenance/v1",
        "operation_id": operation_id,
        "cluster_id": cluster_id,
        "slurm_node": node_name,
        "candidate_digest": _maintenance_candidate_digest(),
        "policy_digest": _sha_path(POLICY),
        **_authority_binding().as_dict(),
        "production_certification_allowed": False,
        "certified_nodes": [],
        "blockers": ["phase2_guard_provider_release_missing"],
        "pre_state": {"state": "IDLE", "reason": "none", "allocated_tres": "cpu=0,mem=0M"},
        "observations": observations,
        "terminal_state": "prepared",
        "failure": None,
        "events": _events(event_data),
    }


def _controller_observation() -> dict[str, object]:
    return {
        "user": "loom-builder",
        "uid": 993,
        "group": "loom-task-builder",
        "gid": 980,
        "home": "/nonexistent",
        "shell": "/usr/sbin/nologin",
        "supplementary_groups": [],
    }


def _node_observation(
    cluster_id: str,
    node_name: str,
    index: int,
    operation_id: str,
    job_id: str,
) -> dict[str, object]:
    cluster = _cluster(cluster_id)
    runtime = _runtime()["architectures"][cluster["architecture"]]
    release = _release()
    debian_arch = release["architecture_map"][cluster["architecture"]]
    address = f"192.0.2.{index}" if cluster_id == "oldlab" else f"198.51.100.{index}"
    physical = f"oldlab-host-{index}" if cluster_id == "oldlab" else f"gx10-{index:02d}"
    packages = []
    for name in ("libsubid4", "uidmap", "quota"):
        package = release["packages"][debian_arch][name]
        packages.append(
            {
                "name": name,
                "version": package["version"],
                "architecture": package["architecture"],
                "filename": package["filename"],
                "size": package["size"],
                "artifact_sha256": package["sha256"],
            }
        )
    cgroup_contents = (
        "CgroupPlugin=autodetect\n"
        "ConstrainCores=yes\n"
        "ConstrainRAMSpace=yes\n"
        "ConstrainSwapSpace=yes\n"
        "ConstrainDevices=yes\n"
    )
    controllers_contents = "cpu cpuset io memory pids\n"
    delegation_contents = "io pids\n"
    source = f"/dev/mapper/builder-{index}"
    findmnt_stdout = json.dumps(
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
    )
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
        "processes_absent": True,
        "mounts_absent": True,
        "job_directory_absent": True,
    }
    observation: dict[str, Any] = {
        "slurm_identity": {
            "node_name": node_name,
            "node_hostname": physical,
            "node_addr": address,
            "resolved_addresses": [address],
            "local_hostnames": [physical, f"{physical}.example.test"],
            "local_addresses": [address],
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
                "suite": release["ubuntu"]["suite"],
                "component": "main",
                "signer_fingerprint": release["ubuntu"]["signer_fingerprint"],
                "keyring_sha256": release["ubuntu"]["keyring_sha256"],
            },
            "installed": packages,
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
                "sha256": _sha_bytes(cgroup_contents.encode("utf-8")),
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
                    "sha256": _sha_bytes(controllers_contents.encode()),
                },
                "delegation": {
                    "path": "/sys/fs/cgroup/cgroup.subtree_control",
                    "contents": delegation_contents,
                    "sha256": _sha_bytes(delegation_contents.encode()),
                },
            },
        },
        "runtime": {
            "release": _runtime()["release"],
            "manifest_sha256": _sha_path(RUNTIME),
            "binary_sha256": runtime["binaries"],
            "dependency_sha256": {},
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
                    stdout=findmnt_stdout,
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
                "cleanup": cleanup,
            },
        },
        "forbidden_paths_present": [],
    }
    _add_phase2_and_runtime_readback(observation, cluster_id, node_name)
    return observation


def _operation(index: int) -> str:
    return f"00000000-0000-4000-8000-{index:012d}"


def _phase2_absence() -> dict[str, object]:
    unit_stdout = "LoadState=not-found\nActiveState=inactive\nFragmentPath=\n"
    return {
        "installed": False,
        "active": False,
        "artifacts": [{"path": f"/usr/libexec/{name}", "present": False} for name in PHASE2_NAMES],
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
            for name in PHASE2_NAMES
        ],
        "process_readback": [
            {
                "name": name,
                "command": [
                    "/usr/bin/pgrep",
                    "-f",
                    f"(^|/){name}( |$)",
                ],
                "returncode": 1,
                "stdout": "",
                "stderr": "",
            }
            for name in PHASE2_NAMES
        ],
    }


def _static_elf_readback(cluster_id: str) -> dict[str, dict[str, object]]:
    release_name = str(_runtime()["release"])
    binary_names = sorted(
        _runtime()["architectures"][_cluster(cluster_id)["architecture"]]["binaries"]
    )
    return {
        name: {
            "command": [
                "/usr/bin/readelf",
                "-d",
                f"/opt/loom-task-builder/releases/{release_name}/bin/{name}",
            ],
            "returncode": 0,
            "stdout": "There is no dynamic section in this file.\n",
            "stderr": "",
        }
        for name in binary_names
    }


def _add_phase2_and_runtime_readback(
    observation: dict[str, Any],
    cluster_id: str,
    node_name: str,
) -> None:
    identity = observation["slurm_identity"]
    address = identity["node_addr"]
    identity["resolution"] = {"query": address, "addresses": [address]}
    identity["readback"] = {
        "command": ["/usr/bin/scontrol", "show", "node", node_name, "-o"],
        "returncode": 0,
        "stdout": (
            f"NodeName={node_name} NodeAddr={address} "
            f"NodeHostName={identity['node_hostname']} "
            "AvailableFeatures=(null) ActiveFeatures=(null)\n"
        ),
        "stderr": "",
    }
    observation["runtime"]["elf_dynamic_readback"] = _static_elf_readback(cluster_id)
    observation["node_guard"] = _phase2_absence()


def _collect_fixture(
    tmp_path: Path,
    *,
    observed_at: datetime | None = None,
) -> tuple[list[Path], list[Path]]:
    now = observed_at or datetime.now(UTC).replace(microsecond=0)
    controllers: list[Path] = []
    nodes: list[Path] = []
    ordinal = 1
    for cluster_id in ("oldlab", "gb10"):
        cluster = _cluster(cluster_id)
        slurm_path = tmp_path / f"{cluster_id}-slurm.json"
        _write_receipt(slurm_path, _slurm_receipt(cluster_id, _operation(ordinal)))
        controller_output = tmp_path / f"{cluster_id}-controller.json"
        EVIDENCE.collect_controller(
            ROOT,
            POLICY,
            RELEASE,
            cluster_id,
            slurm_path,
            controller_output,
            observation=_controller_observation(),
            observed_at=now,
            required_owner=os.geteuid(),
        )
        controllers.append(controller_output)
        for node_index, node_name in enumerate(cluster["builder_nodes"], start=10):
            ordinal += 1
            operation_id = _operation(ordinal)
            host_path = tmp_path / f"{node_name}-host.json"
            maintenance_path = tmp_path / f"{node_name}-maintenance.json"
            _write_receipt(
                host_path,
                _host_receipt(cluster_id, node_name, operation_id),
            )
            _write_receipt(
                maintenance_path,
                _maintenance_receipt(
                    cluster_id,
                    node_name,
                    operation_id,
                    str(1_000 + ordinal),
                ),
            )
            node_output = tmp_path / f"{node_name}-node.json"
            EVIDENCE.collect_node(
                ROOT,
                POLICY,
                RELEASE,
                cluster_id,
                node_name,
                host_path,
                maintenance_path,
                node_output,
                observation=_node_observation(
                    cluster_id,
                    node_name,
                    node_index,
                    operation_id,
                    str(1_000 + ordinal),
                ),
                observed_at=now,
                required_owner=os.geteuid(),
            )
            nodes.append(node_output)
        ordinal += 1
    return controllers, nodes


def complete_evidence(tmp_path: Path) -> dict[str, Any]:
    controllers, nodes = _collect_fixture(tmp_path)
    output = tmp_path / "assembled.json"
    EVIDENCE.assemble(
        ROOT,
        POLICY,
        RELEASE,
        controllers,
        nodes,
        output,
        collected_at=datetime.now(UTC).replace(microsecond=0),
        required_owner=os.geteuid(),
    )
    return json.loads(output.read_text(encoding="utf-8"))


def test_cli_exposes_separate_read_only_collection_and_assembly_commands() -> None:
    result = subprocess.run(
        [sys.executable, str(SCRIPT), "--help"],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert "collect-controller" in result.stdout
    assert "collect-node" in result.stdout
    assert "assemble" in result.stdout


def test_collectors_transport_exact_receipts_and_raw_host_readback_owner_only(
    tmp_path: Path,
) -> None:
    cluster_id = "oldlab"
    node_name = "trt-eai-oldlab-3"
    operation_id = _operation(41)
    slurm_path = tmp_path / "slurm.json"
    host_path = tmp_path / "host.json"
    maintenance_path = tmp_path / "maintenance.json"
    _write_receipt(slurm_path, _slurm_receipt(cluster_id, _operation(40)))
    _write_receipt(host_path, _host_receipt(cluster_id, node_name, operation_id))
    _write_receipt(
        maintenance_path,
        _maintenance_receipt(cluster_id, node_name, operation_id, "1041"),
    )
    inputs = {
        path: (path.read_bytes(), path.stat().st_mtime_ns)
        for path in (POLICY, RELEASE, RUNTIME, slurm_path, host_path, maintenance_path)
    }
    observed_at = datetime.now(UTC).replace(microsecond=0)
    controller_output = tmp_path / "controller.json"
    node_output = tmp_path / "node.json"

    controller = EVIDENCE.collect_controller(
        ROOT,
        POLICY,
        RELEASE,
        cluster_id,
        slurm_path,
        controller_output,
        observation=_controller_observation(),
        observed_at=observed_at,
        required_owner=os.geteuid(),
    )
    node = EVIDENCE.collect_node(
        ROOT,
        POLICY,
        RELEASE,
        cluster_id,
        node_name,
        host_path,
        maintenance_path,
        node_output,
        observation=_node_observation(cluster_id, node_name, 10, operation_id, "1041"),
        observed_at=observed_at,
        required_owner=os.geteuid(),
    )

    assert controller["schema"] == "loom.task-image-builder-controller-evidence/v1"
    assert controller["cluster"]["controller_identity"] == _controller_observation()
    assert controller["cluster"]["slurm_receipt"]["document"] == json.loads(
        slurm_path.read_text(encoding="utf-8")
    )
    assert controller["cluster"]["slurm_receipt"]["sha256"] == _sha_path(slurm_path)
    assert node["schema"] == "loom.task-image-builder-node-evidence/v1"
    expected_packages = _node_observation(
        cluster_id, node_name, 10, operation_id, "1041"
    )["packages"]
    expected_packages["installed"] = sorted(
        expected_packages["installed"], key=lambda item: item["name"]
    )
    assert node["node"]["packages"] == expected_packages
    assert node["node"]["runtime"]["dependency_sha256"] == {}
    assert node["node"]["kernel"]["slurm_cgroup_readback"]["contents"].endswith(
        "ConstrainDevices=yes\n"
    )
    assert node["node"]["storage"]["quota"] == _quota_state()
    assert node["node"]["host_receipt"]["document"] == json.loads(
        host_path.read_text(encoding="utf-8")
    )
    assert node["node"]["maintenance_receipt"]["document"] == json.loads(
        maintenance_path.read_text(encoding="utf-8")
    )
    assert stat.S_IMODE(controller_output.stat().st_mode) == 0o600
    assert stat.S_IMODE(node_output.stat().st_mode) == 0o600
    assert all(
        (path.read_bytes(), path.stat().st_mtime_ns) == before for path, before in inputs.items()
    )


def test_node_collector_preserves_additional_cgroup_v2_controllers(
    tmp_path: Path,
) -> None:
    cluster_id = "oldlab"
    node_name = "trt-eai-oldlab-3"
    operation_id = _operation(42)
    host_path = tmp_path / "host.json"
    maintenance_path = tmp_path / "maintenance.json"
    _write_receipt(host_path, _host_receipt(cluster_id, node_name, operation_id))
    _write_receipt(
        maintenance_path,
        _maintenance_receipt(cluster_id, node_name, operation_id, "1042"),
    )
    observation = _node_observation(cluster_id, node_name, 10, operation_id, "1042")
    observation["kernel"]["controllers"] = [
        "cpu",
        "cpuset",
        "hugetlb",
        "io",
        "memory",
        "pids",
    ]
    controllers = observation["kernel"]["raw"]["controllers"]
    controllers["contents"] = "cpu cpuset hugetlb io memory pids\n"
    controllers["sha256"] = _sha_bytes(controllers["contents"].encode())

    node = EVIDENCE.collect_node(
        ROOT,
        POLICY,
        RELEASE,
        cluster_id,
        node_name,
        host_path,
        maintenance_path,
        tmp_path / "node.json",
        observation=observation,
        observed_at=datetime.now(UTC),
        required_owner=os.geteuid(),
    )

    assert node["node"]["kernel"]["controllers"] == [
        "cpu",
        "cpuset",
        "hugetlb",
        "io",
        "memory",
        "pids",
    ]


def test_node_collector_binds_phase_two_absence_static_elf_and_resolution(
    tmp_path: Path,
) -> None:
    cluster_id = "oldlab"
    node_name = "trt-eai-oldlab-3"
    operation_id = _operation(43)
    host_path = tmp_path / "host.json"
    maintenance_path = tmp_path / "maintenance.json"
    _write_receipt(host_path, _host_receipt(cluster_id, node_name, operation_id))
    _write_receipt(
        maintenance_path,
        _maintenance_receipt(cluster_id, node_name, operation_id, "1043"),
    )
    observation = _node_observation(cluster_id, node_name, 10, operation_id, "1043")
    _add_phase2_and_runtime_readback(observation, cluster_id, node_name)

    node = EVIDENCE.collect_node(
        ROOT,
        POLICY,
        RELEASE,
        cluster_id,
        node_name,
        host_path,
        maintenance_path,
        tmp_path / "node.json",
        observation=observation,
        observed_at=datetime.now(UTC),
        required_owner=os.geteuid(),
    )["node"]

    assert node["node_guard"] == _phase2_absence()
    assert node["runtime"]["elf_dynamic_readback"] == _static_elf_readback(cluster_id)
    assert node["slurm_identity"]["resolution"] == {
        "query": "192.0.2.10",
        "addresses": ["192.0.2.10"],
    }


@pytest.mark.parametrize(
    ("mutation", "expected"),
    [
        ("phase2_artifact", "Phase 2 node guard"),
        ("phase2_unit", "Phase 2 unit"),
        ("phase2_process", "Phase 2 process"),
        ("builder_feature", "alias binding"),
        ("dynamic_needed", "runtime/dependency"),
        ("resolution_query", "alias binding"),
        ("kernel_raw_sysctl", "kernel raw command"),
        ("storage_raw_jobs_owner", "mount/quota raw"),
        ("storage_raw_cleanup", "mount/quota raw"),
    ],
)
def test_node_collector_rejects_forged_raw_absence_and_runtime_facts(
    tmp_path: Path,
    mutation: str,
    expected: str,
) -> None:
    cluster_id = "oldlab"
    node_name = "trt-eai-oldlab-3"
    operation_id = _operation(44)
    host_path = tmp_path / "host.json"
    maintenance_path = tmp_path / "maintenance.json"
    _write_receipt(host_path, _host_receipt(cluster_id, node_name, operation_id))
    _write_receipt(
        maintenance_path,
        _maintenance_receipt(cluster_id, node_name, operation_id, "1044"),
    )
    observation = _node_observation(cluster_id, node_name, 10, operation_id, "1044")
    if mutation == "phase2_artifact":
        observation["node_guard"]["artifacts"][0]["present"] = True
    elif mutation == "phase2_unit":
        observation["node_guard"]["unit_readback"][0]["stdout"] = (
            "LoadState=loaded\nActiveState=inactive\n"
            "FragmentPath=/etc/systemd/system/loom-task-builder-node-guard.service\n"
        )
    elif mutation == "phase2_process":
        observation["node_guard"]["process_readback"][0].update(
            {"returncode": 0, "stdout": "4242\n"}
        )
    elif mutation == "builder_feature":
        readback = observation["slurm_identity"]["readback"]
        readback["stdout"] = readback["stdout"].replace(
            "AvailableFeatures=(null)",
            "AvailableFeatures=loom_rootless_buildkit",
        )
    elif mutation == "dynamic_needed":
        readbacks = observation["runtime"]["elf_dynamic_readback"]
        readbacks[sorted(readbacks)[0]]["stdout"] = (
            " 0x0000000000000001 (NEEDED) Shared library: [libc.so.6]\n"
        )
    elif mutation == "kernel_raw_sysctl":
        observation["kernel"]["raw"]["unprivileged_user_namespaces"]["stdout"] = "0\n"
    elif mutation == "storage_raw_jobs_owner":
        observation["storage"]["raw"]["jobs_root"]["uid"] = 0
    elif mutation == "storage_raw_cleanup":
        observation["storage"]["raw"]["cleanup"]["command"] = ["/bin/true"]
    else:
        observation["slurm_identity"]["resolution"]["query"] = "forged-node-address.example.test"

    with pytest.raises(EVIDENCE.EvidenceError, match=expected):
        EVIDENCE.collect_node(
            ROOT,
            POLICY,
            RELEASE,
            cluster_id,
            node_name,
            host_path,
            maintenance_path,
            tmp_path / f"{mutation}.json",
            observation=observation,
            observed_at=datetime.now(UTC),
            required_owner=os.geteuid(),
        )


def test_node_collector_builds_production_observation_from_system_boundaries(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cluster_id = "oldlab"
    node_name = "trt-eai-oldlab-3"
    operation_id = _operation(45)
    host_path = tmp_path / "host.json"
    maintenance_path = tmp_path / "maintenance.json"
    _write_receipt(host_path, _host_receipt(cluster_id, node_name, operation_id))
    _write_receipt(
        maintenance_path,
        _maintenance_receipt(cluster_id, node_name, operation_id, "1045"),
    )

    runtime = _runtime()
    binaries = runtime["architectures"]["x86_64"]["binaries"]
    release_root = Path("/opt/loom-task-builder/releases/rootless-runtime-v1")
    binary_payloads = {
        release_root / "bin" / name: f"static-elf:{name}".encode() for name in binaries
    }
    digest_overrides = {payload: binaries[path.name] for path, payload in binary_payloads.items()}
    runtime_receipt = (
        _canonical(
            {
                "schema": "loom.task-image-builder-installed-runtime/v1",
                "release": "rootless-runtime-v1",
                "architecture": "x86_64",
                "manifest_sha256": _sha_path(RUNTIME),
                "binary_sha256": binaries,
            }
        )
        + b"\n"
    )
    special_files = {
        Path("/sys/fs/cgroup/cgroup.controllers"): b"cpu cpuset io memory pids\n",
        Path("/sys/fs/cgroup/cgroup.subtree_control"): b"io pids\n",
        Path("/etc/subuid"): b"loom-builder:3000000:65536\n",
        Path("/etc/subgid"): b"loom-builder:3000000:65536\n",
        Path("/usr/bin/newgidmap"): b"newgidmap-static-helper",
        Path("/usr/bin/newuidmap"): b"newuidmap-static-helper",
        release_root / "receipt.json": runtime_receipt,
        **binary_payloads,
    }
    real_read_regular = EVIDENCE._read_regular

    def observed_read_regular(path: Path, label: str, **kwargs: Any) -> bytes:
        if path in special_files:
            return special_files[path]
        return real_read_regular(path, label, **kwargs)

    real_sha = EVIDENCE._sha

    def observed_sha(payload: bytes) -> str:
        return digest_overrides.get(payload, real_sha(payload))

    real_path_stat = Path.stat

    def observed_path_stat(
        path: Path,
        *,
        follow_symlinks: bool = True,
    ) -> object:
        if path in {Path("/usr/bin/newgidmap"), Path("/usr/bin/newuidmap")}:
            return SimpleNamespace(st_uid=0, st_gid=0, st_mode=stat.S_IFREG | 0o4755)
        return real_path_stat(path, follow_symlinks=follow_symlinks)

    phase2_paths = {str(path) for path in EVIDENCE.PHASE2_ARTIFACT_PATHS}
    real_lstat = os.lstat

    def observed_lstat(path: object, *args: object, **kwargs: object) -> object:
        if str(path) in phase2_paths:
            raise FileNotFoundError(str(path))
        if str(path) == "/sys/fs/bpf":
            return SimpleNamespace(st_uid=0, st_gid=0, st_mode=stat.S_IFDIR | 0o700)
        if str(path) == "/var/lib/loom-task-builder/jobs":
            return SimpleNamespace(st_uid=993, st_gid=980, st_mode=stat.S_IFDIR | 0o700)
        return real_lstat(path, *args, **kwargs)

    commands: list[tuple[str, ...]] = []
    scontrol_stdout = (
        f"NodeName={node_name} NodeAddr=192.0.2.10 "
        "NodeHostName=oldlab-host-10 "
        "AvailableFeatures=(null) ActiveFeatures=(null)\n"
    )

    def observed_run(command: Any, label: str) -> Any:
        vector = tuple(command)
        commands.append(vector)
        if vector == ("/usr/bin/uname", "-m"):
            return EVIDENCE.CommandResult(0, "x86_64\n", "")
        if vector == ("/usr/bin/scontrol", "show", "node", node_name, "-o"):
            return EVIDENCE.CommandResult(0, scontrol_stdout, "")
        if vector == ("/bin/hostname", "--short"):
            return EVIDENCE.CommandResult(0, "oldlab-host-10\n", "")
        if vector == ("/bin/hostname", "--fqdn"):
            return EVIDENCE.CommandResult(0, "oldlab-host-10.example.test\n", "")
        if vector == ("/bin/hostname", "--all-ip-addresses"):
            return EVIDENCE.CommandResult(0, "192.0.2.10\n", "")
        if vector == ("/usr/bin/getent", "passwd", "loom-builder"):
            return EVIDENCE.CommandResult(
                0,
                "loom-builder:x:993:980::/nonexistent:/usr/sbin/nologin\n",
                "",
            )
        if vector == ("/usr/bin/getent", "group", "loom-task-builder"):
            return EVIDENCE.CommandResult(0, "loom-task-builder:x:980:\n", "")
        if vector == ("/usr/bin/id", "-G", "-n", "loom-builder"):
            return EVIDENCE.CommandResult(0, "loom-task-builder\n", "")
        if vector[:2] == ("/usr/sbin/getcap", "-n"):
            return EVIDENCE.CommandResult(0, "", "")
        if vector[:2] == ("/usr/bin/readelf", "-d"):
            return EVIDENCE.CommandResult(
                0,
                "There is no dynamic section in this file.\n",
                "",
            )
        if vector[:2] == ("/usr/bin/systemctl", "show"):
            return EVIDENCE.CommandResult(
                0,
                "LoadState=not-found\nActiveState=inactive\nFragmentPath=\n",
                "",
            )
        if vector[:2] == ("/usr/bin/pgrep", "-f"):
            return EVIDENCE.CommandResult(1, "", "")
        if vector == (
            "/usr/sbin/sysctl",
            "--values",
            "kernel.unprivileged_userns_clone",
        ):
            return EVIDENCE.CommandResult(0, "1\n", "")
        if vector == (
            "/usr/bin/findmnt",
            "--json",
            "--target",
            "/sys/fs/cgroup",
            "--output",
            "TARGET,SOURCE,FSTYPE,OPTIONS",
        ):
            return EVIDENCE.CommandResult(
                0,
                json.dumps(
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
                "",
            )
        if vector == (
            "/usr/bin/findmnt",
            "--json",
            "--target",
            "/sys/fs/bpf",
            "--output",
            "TARGET,SOURCE,FSTYPE,OPTIONS",
        ):
            return EVIDENCE.CommandResult(
                0,
                json.dumps(
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
                "",
            )
        if vector == (
            "/usr/bin/findmnt",
            "--json",
            "--target",
            "/var/lib/loom-task-builder",
            "--output",
            "TARGET,SOURCE,FSTYPE,OPTIONS",
        ):
            return EVIDENCE.CommandResult(
                0,
                json.dumps(
                    {
                        "filesystems": [
                            {
                                "target": "/var/lib/loom-task-builder",
                                "source": "/dev/mapper/builder-oldlab-10",
                                "fstype": "ext4",
                                "options": "rw,prjquota",
                            }
                        ]
                    }
                ),
                "",
            )
        if vector == (
            "/usr/bin/lsblk",
            "--noheadings",
            "--output",
            "TYPE",
            "/dev/mapper/builder-oldlab-10",
        ):
            return EVIDENCE.CommandResult(0, "lvm\n", "")
        if vector == (
            "/usr/bin/lsattr",
            "-pd",
            "/var/lib/loom-task-builder/jobs",
        ):
            return EVIDENCE.CommandResult(
                0,
                "300993 --------------P------- /var/lib/loom-task-builder/jobs\n",
                "",
            )
        if vector == (
            "/usr/sbin/repquota",
            "-v",
            "-n",
            "-p",
            "-P",
            "-O",
            "csv",
            "/var/lib/loom-task-builder",
        ):
            return EVIDENCE.CommandResult(
                0,
                "Project,BlockStatus,FileStatus,BlockUsed,BlockSoftLimit,"
                "BlockHardLimit,BlockGrace,FileUsed,FileSoftLimit,FileHardLimit,FileGrace\n"
                "300993,--,--,0,0,104857600,0,0,0,1000000,0\n",
                "",
            )
        raise AssertionError(f"unexpected system command for {label}: {vector!r}")

    monkeypatch.setattr(EVIDENCE, "_read_regular", observed_read_regular)
    monkeypatch.setattr(EVIDENCE, "_sha", observed_sha)
    monkeypatch.setattr(EVIDENCE.Path, "stat", observed_path_stat)
    monkeypatch.setattr(EVIDENCE.os, "lstat", observed_lstat)
    monkeypatch.setattr(EVIDENCE, "_run", observed_run)
    monkeypatch.setattr(EVIDENCE, "_directory_entries", lambda path: [], raising=False)
    monkeypatch.setattr(
        EVIDENCE,
        "_probe_pidfd_open",
        lambda: {"pid": 4242, "flags": 0, "outcome": "opened"},
        raising=False,
    )
    monkeypatch.setattr(
        EVIDENCE,
        "_probe_sealed_memfd",
        lambda: {"required_seals": 15, "observed_seals": 15, "outcome": "sealed"},
        raising=False,
    )
    monkeypatch.setattr(
        EVIDENCE,
        "_probe_clone3_into_cgroup",
        lambda: {
            "flags": "CLONE_INTO_CGROUP",
            "cgroup_fd": -1,
            "returncode": -1,
            "errno": 9,
            "errno_name": "EBADF",
        },
        raising=False,
    )

    node = EVIDENCE.collect_node(
        ROOT,
        POLICY,
        RELEASE,
        cluster_id,
        node_name,
        host_path,
        maintenance_path,
        tmp_path / "system-observed-node.json",
        observation=None,
        observed_at=datetime.now(UTC),
        required_owner=os.geteuid(),
    )["node"]

    assert node["identity"]["home"] == "/nonexistent"
    assert node["identity"]["shell"] == "/usr/sbin/nologin"
    assert node["slurm_identity"]["readback"]["stdout"] == scontrol_stdout
    assert node["slurm_identity"]["resolution"] == {
        "query": "192.0.2.10",
        "addresses": ["192.0.2.10"],
    }
    assert node["runtime"]["dependency_sha256"] == {}
    assert set(node["runtime"]["elf_dynamic_readback"]) == set(binaries)
    assert node["kernel"]["raw"]["pidfd_open"]["outcome"] == "opened"
    assert node["kernel"]["raw"]["sealed_memfd"]["outcome"] == "sealed"
    assert node["kernel"]["raw"]["clone3_into_cgroup"]["errno_name"] == "EBADF"
    assert node["kernel"]["raw"]["delegation"]["contents"] == "io pids\n"
    assert node["storage"]["raw"]["jobs_root"] == {
        "path": "/var/lib/loom-task-builder/jobs",
        "uid": 993,
        "gid": 980,
        "mode": "0700",
        "entries": [],
    }
    assert node["storage"]["raw"]["cleanup"]["returncode"] == 0
    assert node["storage"]["quota"] == _quota_state()
    assert all(not artifact["present"] for artifact in node["node_guard"]["artifacts"])
    assert ("/usr/bin/scontrol", "show", "node", node_name, "-o") in commands
    assert any(command[:2] == ("/usr/bin/systemctl", "show") for command in commands)
    assert ("/usr/bin/lsblk", "--noheadings", "--output", "TYPE", "/dev/mapper/builder-oldlab-10") in commands
    assert any(command[:2] == ("/usr/sbin/repquota", "-v") for command in commands)


def test_assemble_sorts_exact_two_cluster_inventories_and_stays_inert(
    tmp_path: Path,
) -> None:
    controllers, nodes = _collect_fixture(tmp_path)
    output = tmp_path / "evidence.json"

    result = EVIDENCE.assemble(
        ROOT,
        POLICY,
        RELEASE,
        list(reversed(controllers)),
        list(reversed(nodes)),
        output,
        collected_at=datetime.now(UTC).replace(microsecond=0),
        required_owner=os.geteuid(),
    )

    assert [cluster["id"] for cluster in result["clusters"]] == ["gb10", "oldlab"]
    for cluster in result["clusters"]:
        assert [node["name"] for node in cluster["nodes"]] == sorted(
            _cluster(cluster["id"])["builder_nodes"]
        )
    assert result["production_certification_allowed"] is False
    assert result["certified_nodes"] == []
    assert result["blockers"] == ["phase2_guard_provider_release_missing"]
    assert stat.S_IMODE(output.stat().st_mode) == 0o600
    schema = json.loads(SCHEMA.read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(schema)
    assert list(Draft202012Validator(schema).iter_errors(result)) == []


def test_assemble_rejects_forged_slurm_pre_legacy_state(tmp_path: Path) -> None:
    controllers, nodes = _collect_fixture(tmp_path)
    controller_path = controllers[0]
    fragment = json.loads(controller_path.read_text(encoding="utf-8"))
    receipt = fragment["cluster"]["slurm_receipt"]
    document = receipt["document"]
    document["pre_state"]["legacy"]["reservation"]["node"] = "forged-pre-state-node"
    document["events"][0]["data"] = {"state": document["pre_state"]}
    document["events"] = _events([(event["type"], event["data"]) for event in document["events"]])
    receipt["sha256"] = _sha_bytes(_canonical(document) + b"\n")
    controller_path.write_bytes(_canonical(fragment) + b"\n")

    with pytest.raises(EVIDENCE.EvidenceError, match="legacy Slurm fingerprints"):
        EVIDENCE.assemble(
            ROOT,
            POLICY,
            RELEASE,
            controllers,
            nodes,
            tmp_path / "forged-pre-state.json",
            collected_at=datetime.now(UTC).replace(microsecond=0),
            required_owner=os.geteuid(),
        )


@pytest.mark.parametrize(
    ("mutation", "expected"),
    [
        ("missing_node", "node evidence set"),
        ("duplicate_node", "duplicate node"),
        ("duplicate_cluster", "duplicate controller"),
        ("mixed_policy", "policy digest"),
        ("mixed_release", "release digest"),
        ("mixed_authority", "authority manifest digest"),
        ("stale", "freshness"),
        ("prepared_without_terminal_receipt", "terminal"),
        ("foreign_drain", "foreign drain"),
        ("surviving_job_directory", "cleanup"),
        ("surviving_process", "cleanup"),
        ("surviving_mount", "cleanup"),
        ("nonterminal_smoke", "accounting"),
        ("malformed_package", "package"),
        ("loopback_binding", "alias binding"),
        ("invalid_operation_identity", "operation identity"),
        ("overridden_admission", "admission"),
        ("forged_reservation_command", "reservation"),
        ("forged_reservation_prior_raw", "reservation"),
        ("forged_reservation_create_raw", "reservation"),
        ("forged_reservation_delete_raw", "reservation"),
        ("forged_accounting_command", "accounting"),
        ("forged_cleanup_command", "cleanup"),
        ("secret", "secret-like"),
    ],
)
def test_assemble_rejects_incomplete_mixed_stale_or_unsafe_facts(
    tmp_path: Path,
    mutation: str,
    expected: str,
) -> None:
    fixture_root = tmp_path / mutation
    fixture_root.mkdir()
    controllers, nodes = _collect_fixture(fixture_root)
    if mutation == "missing_node":
        nodes.pop()
    elif mutation == "duplicate_node":
        nodes.append(nodes[0])
    elif mutation == "duplicate_cluster":
        controllers.append(controllers[0])
    else:
        target = nodes[0]
        value = json.loads(target.read_text(encoding="utf-8"))
        if mutation == "mixed_policy":
            value["policy_file_sha256"] = "9" * 64
        elif mutation == "mixed_release":
            value["release_sha256"] = "9" * 64
        elif mutation == "mixed_authority":
            value["authority_manifest_sha256"] = "9" * 64
        elif mutation == "stale":
            value["observed_at"] = (
                (datetime.now(UTC) - timedelta(hours=2))
                .replace(microsecond=0)
                .isoformat()
                .replace("+00:00", "Z")
            )
        elif mutation == "prepared_without_terminal_receipt":
            receipt = value["node"]["maintenance_receipt"]
            receipt["document"]["terminal_state"] = "applying"
            receipt["sha256"] = _sha_bytes(_canonical(receipt["document"]) + b"\n")
        elif mutation == "foreign_drain":
            receipt = value["node"]["maintenance_receipt"]
            receipt["document"]["pre_state"] = {
                "state": "DRAIN",
                "reason": "operator-maintenance",
                "allocated_tres": "cpu=0,mem=0M",
            }
            receipt["document"]["events"][0]["data"]["pre_state"] = receipt["document"]["pre_state"]
            receipt["document"]["events"] = _events(
                [(event["type"], event["data"]) for event in receipt["document"]["events"]]
            )
            receipt["sha256"] = _sha_bytes(_canonical(receipt["document"]) + b"\n")
        elif mutation in {"surviving_job_directory", "surviving_process", "surviving_mount"}:
            receipt = value["node"]["maintenance_receipt"]
            fact = {
                "surviving_job_directory": "job_directory_absent",
                "surviving_process": "processes_absent",
                "surviving_mount": "mounts_absent",
            }[mutation]
            receipt["document"]["observations"]["smoke"]["cleanup"][fact] = False
            cleaned = next(
                event for event in receipt["document"]["events"] if event["type"] == "smoke_cleaned"
            )
            cleaned["data"]["cleanup"][fact] = False
            receipt["document"]["events"] = _events(
                [(event["type"], event["data"]) for event in receipt["document"]["events"]]
            )
            receipt["sha256"] = _sha_bytes(_canonical(receipt["document"]) + b"\n")
        elif mutation == "nonterminal_smoke":
            receipt = value["node"]["maintenance_receipt"]
            completed = next(
                event
                for event in receipt["document"]["events"]
                if event["type"] == "smoke_completed"
            )
            completed["data"]["accounting"]["top_level"]["state"] = "RUNNING"
            receipt["document"]["events"] = _events(
                [(event["type"], event["data"]) for event in receipt["document"]["events"]]
            )
            receipt["sha256"] = _sha_bytes(_canonical(receipt["document"]) + b"\n")
        elif mutation == "malformed_package":
            value["node"]["packages"]["installed"] = [1, 2, 3]
        elif mutation == "loopback_binding":
            binding = value["node"]["slurm_identity"]
            binding["resolved_addresses"] = ["127.0.0.1"]
            binding["local_addresses"] = ["127.0.0.1"]
        elif mutation == "invalid_operation_identity":
            cluster_id = value["cluster_id"]
            node_name = value["node"]["name"]
            operation_id = "not-a-uuid"
            host = _host_receipt(cluster_id, node_name, operation_id)
            maintenance = _maintenance_receipt(
                cluster_id,
                node_name,
                operation_id,
                "1999",
            )
            value["node"]["host_receipt"] = {
                "sha256": _sha_bytes(_canonical(host) + b"\n"),
                "document": host,
            }
            value["node"]["maintenance_receipt"] = {
                "sha256": _sha_bytes(_canonical(maintenance) + b"\n"),
                "document": maintenance,
            }
        elif mutation == "overridden_admission":
            receipt = value["node"]["maintenance_receipt"]
            receipt["document"]["observations"]["admission"]["builder"]["command"].append(
                "--account=loom-staging"
            )
            receipt["sha256"] = _sha_bytes(_canonical(receipt["document"]) + b"\n")
        elif mutation == "forged_reservation_command":
            receipt = value["node"]["maintenance_receipt"]
            receipt["document"]["observations"]["reservation"]["create"]["command"] = ["/bin/true"]
            created = next(
                event
                for event in receipt["document"]["events"]
                if event["type"] == "reservation_created"
            )
            created["data"]["create"]["command"] = ["/bin/true"]
            receipt["document"]["events"] = _events(
                [(event["type"], event["data"]) for event in receipt["document"]["events"]]
            )
            receipt["sha256"] = _sha_bytes(_canonical(receipt["document"]) + b"\n")
        elif mutation == "forged_reservation_prior_raw":
            receipt = value["node"]["maintenance_receipt"]
            reservation = receipt["document"]["observations"]["reservation"]
            reservation["prior_readback"]["stdout"] += (
                f"ReservationName={reservation['name']} Nodes=forged-node "
                "Users=loom-builder State=ACTIVE\n"
            )
            receipt["sha256"] = _sha_bytes(_canonical(receipt["document"]) + b"\n")
        elif mutation == "forged_reservation_create_raw":
            receipt = value["node"]["maintenance_receipt"]
            document = receipt["document"]
            reservation = document["observations"]["reservation"]
            reservation["create_readback"]["stdout"] = (
                "ReservationName=legacy_operator_hold Nodes=legacy-node "
                "Users=operator State=ACTIVE\n"
            )
            created = next(
                event for event in document["events"] if event["type"] == "reservation_created"
            )
            created["data"]["create_readback"] = reservation["create_readback"]
            document["events"] = _events(
                [(event["type"], event["data"]) for event in document["events"]]
            )
            receipt["sha256"] = _sha_bytes(_canonical(document) + b"\n")
        elif mutation == "forged_reservation_delete_raw":
            receipt = value["node"]["maintenance_receipt"]
            document = receipt["document"]
            reservation = document["observations"]["reservation"]
            reservation["delete_readback"]["stdout"] += (
                f"ReservationName={reservation['name']} Nodes={value['node']['name']} "
                "Users=loom-builder State=ACTIVE\n"
            )
            deleted = next(
                event for event in document["events"] if event["type"] == "reservation_deleted"
            )
            deleted["data"]["delete_readback"] = reservation["delete_readback"]
            document["events"] = _events(
                [(event["type"], event["data"]) for event in document["events"]]
            )
            receipt["sha256"] = _sha_bytes(_canonical(document) + b"\n")
        elif mutation == "forged_accounting_command":
            receipt = value["node"]["maintenance_receipt"]
            completed = next(
                event
                for event in receipt["document"]["events"]
                if event["type"] == "smoke_completed"
            )
            completed["data"]["accounting"]["readback"]["command"] = ["/bin/true"]
            receipt["document"]["events"] = _events(
                [(event["type"], event["data"]) for event in receipt["document"]["events"]]
            )
            receipt["sha256"] = _sha_bytes(_canonical(receipt["document"]) + b"\n")
        elif mutation == "forged_cleanup_command":
            receipt = value["node"]["maintenance_receipt"]
            cleaned = next(
                event for event in receipt["document"]["events"] if event["type"] == "smoke_cleaned"
            )
            cleaned["data"]["cleanup"]["command"] = ["/bin/true"]
            receipt["document"]["events"] = _events(
                [(event["type"], event["data"]) for event in receipt["document"]["events"]]
            )
            receipt["sha256"] = _sha_bytes(_canonical(receipt["document"]) + b"\n")
        else:
            value["api_token"] = "Bearer super-secret-value"
        target.write_bytes(_canonical(value) + b"\n")

    with pytest.raises(EVIDENCE.EvidenceError, match=expected):
        EVIDENCE.assemble(
            ROOT,
            POLICY,
            RELEASE,
            controllers,
            nodes,
            tmp_path / f"{mutation}.json",
            collected_at=datetime.now(UTC).replace(microsecond=0),
            required_owner=os.geteuid(),
        )


def test_collectors_refuse_existing_output_and_secret_bearing_receipts(
    tmp_path: Path,
) -> None:
    slurm = _slurm_receipt("oldlab", _operation(70))
    slurm["command_outcome"]["stdout"] = "api_key=do-not-print-this"
    slurm_path = tmp_path / "slurm.json"
    _write_receipt(slurm_path, slurm)
    output = tmp_path / "output.json"

    with pytest.raises(EVIDENCE.EvidenceError, match="secret-like") as secret:
        EVIDENCE.collect_controller(
            ROOT,
            POLICY,
            RELEASE,
            "oldlab",
            slurm_path,
            output,
            observation=_controller_observation(),
            observed_at=datetime.now(UTC),
            required_owner=os.geteuid(),
        )
    assert "do-not-print-this" not in str(secret.value)

    _write_receipt(slurm_path, _slurm_receipt("oldlab", _operation(70)))
    output.write_text("owned by caller\n", encoding="utf-8")
    with pytest.raises(EVIDENCE.EvidenceError, match="already exists"):
        EVIDENCE.collect_controller(
            ROOT,
            POLICY,
            RELEASE,
            "oldlab",
            slurm_path,
            output,
            observation=_controller_observation(),
            observed_at=datetime.now(UTC),
            required_owner=os.geteuid(),
        )


def test_collector_output_is_owner_readable_under_restrictive_umask(
    tmp_path: Path,
) -> None:
    slurm_path = tmp_path / "slurm.json"
    output = tmp_path / "controller.json"
    _write_receipt(slurm_path, _slurm_receipt("oldlab", _operation(71)))

    previous_umask = os.umask(0o777)
    try:
        EVIDENCE.collect_controller(
            ROOT,
            POLICY,
            RELEASE,
            "oldlab",
            slurm_path,
            output,
            observation=_controller_observation(),
            observed_at=datetime.now(UTC),
            required_owner=os.geteuid(),
        )
    finally:
        os.umask(previous_umask)

    assert stat.S_IMODE(output.stat().st_mode) == 0o600
