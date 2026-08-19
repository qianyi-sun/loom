from __future__ import annotations

import json
import re
import tomllib
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
POLICY_PATH = ROOT / "deploy/task-image-builder/prerequisites-v1.toml"
RUNTIME_PATH = ROOT / "deploy/task-image-builder/rootless-runtime-v1.json"
SHA256_RE = re.compile(r"^[a-f0-9]{64}$")


def _policy() -> dict[str, Any]:
    with POLICY_PATH.open("rb") as handle:
        return tomllib.load(handle)


def _runtime() -> dict[str, Any]:
    return json.loads(RUNTIME_PATH.read_text(encoding="utf-8"))


def test_phase_one_policy_is_dynamic_bounded_and_cannot_certify_production() -> None:
    policy = _policy()

    assert policy["schema"] == "loom.task-image-builder-prerequisites/v1"
    assert policy["policy_version"] == "task-image-builder-prerequisites-v1"
    assert policy["production_certification_allowed"] is False
    assert policy["certified_nodes"] == []
    assert policy["unconditional_blockers"] == [
        "phase2_guard_provider_release_missing",
    ]

    identity = policy["identity"]
    assert identity == {
        "user": "loom-builder",
        "group": "loom-task-builder",
        "uid": 993,
        "gid": 980,
        "subid_start": 3_000_000,
        "subid_count": 65_536,
        "home": "/nonexistent",
        "shell": "/usr/sbin/nologin",
        "forbidden_supplementary_groups": ["docker", "root", "sudo"],
    }

    resources = policy["resource_profile"]
    assert resources["cpus"] == 8
    assert resources["memory_mib"] == 32_768
    assert resources["pids"] == 4_096
    assert resources["scratch_bytes"] == 107_374_182_400
    assert resources["scratch_inodes"] == 1_000_000
    assert resources["wall_time"] == "02:00:00"
    assert resources["swap_bytes"] == 0
    assert resources["max_jobs_per_user"] == 1
    assert resources["max_submit_jobs_per_user"] == 1

    cgroup = policy["cgroup"]
    assert cgroup == {
        "version": 2,
        "task_plugin": "task/cgroup",
        "proctrack_type": "proctrack/cgroup",
        "constrain_cores": True,
        "constrain_ram_space": True,
        "constrain_swap_space": True,
        "constrain_devices": True,
        "required_delegated_controllers": ["io", "pids"],
    }

    runtime = policy["runtime"]
    assert runtime["manifest"] == "rootless-runtime-v1.json"
    assert runtime["snapshotter"] == "fuse-overlayfs"
    assert runtime["network_driver"] == "slirp4netns"
    assert runtime["rootlesskit_flags"] == [
        "--disable-host-loopback",
        "--ipv6",
        "--slirp4netns-sandbox=true",
        "--slirp4netns-seccomp=true",
    ]
    assert runtime["insecure_entitlements"] == []
    assert runtime["forbidden_paths"] == [
        "/run/containerd/containerd.sock",
        "/var/run/docker.sock",
    ]

    storage = policy["storage"]
    assert storage["required_quota_kinds"] == ["project"]
    assert storage["allowed_filesystems"] == ["ext4", "xfs"]
    assert storage["cache_enabled"] is False
    assert storage["cleanup_required"] is True

    assert policy["control_plane_services"] == {
        "repository_scoped_renewable_credential_broker": True,
        "reference_aware_registry_retention": True,
        "publication_signer": True,
        "publication_verification_keyset_lifecycle": True,
    }

    network = policy["network"]
    for key in (
        "ingress_bytes_per_second",
        "egress_bytes_per_second",
        "ingress_packets_per_second",
        "egress_packets_per_second",
        "concurrent_flows",
        "new_flows_per_second",
        "dns_queries_per_second",
    ):
        assert network[key] > 0
    assert network["ipv4_default_deny"] is True
    assert network["ipv6_default_deny"] is True
    assert network["bpffs_required"] is True

    clusters = {item["id"]: item for item in policy["clusters"]}
    assert set(clusters) == {"oldlab", "gb10"}
    assert clusters["oldlab"]["architecture"] == "x86_64"
    assert clusters["oldlab"]["controller"] == "TRT-EAI-OLDLAB-1"
    assert clusters["oldlab"]["trial_partition"] == "loom-staging"
    assert clusters["oldlab"]["trial_priority_tier"] == 100
    assert clusters["oldlab"]["builder_nodes"] == [
        "trt-eai-oldlab-3",
        "trt-eai-oldlab-4",
        "trt-eai-oldlab-5",
    ]
    assert clusters["oldlab"]["builder_nodes_expression"] == "trt-eai-oldlab-[3-5]"
    assert clusters["oldlab"]["slurm_config_owner"] == "trt"
    assert clusters["oldlab"]["slurm_config_group"] == "sharedwork"
    assert clusters["oldlab"]["slurm_config_mode"] == "0664"
    assert clusters["oldlab"]["trial_partition_anchor"] == (
        "PartitionName=loom-staging Nodes=trt-eai-oldlab-[3-5] Default=NO "
        "MaxTime=2-00:00:00 State=UP PriorityTier=100 AllowGroups=loom-rollout "
        "OverSubscribe=NO"
    )
    assert clusters["gb10"]["architecture"] == "aarch64"
    assert clusters["gb10"]["controller"] == "gx10-01c7"
    assert clusters["gb10"]["trial_partition"] == "gb10"
    assert clusters["gb10"]["trial_priority_tier"] == 100
    assert clusters["gb10"]["builder_nodes"] == [f"trt-gb10-{number}" for number in range(1, 16)]
    assert clusters["gb10"]["builder_nodes_expression"] == "trt-gb10-[1-15]"
    assert clusters["gb10"]["slurm_config_owner"] == "root"
    assert clusters["gb10"]["slurm_config_group"] == "root"
    assert clusters["gb10"]["slurm_config_mode"] == "0644"
    assert clusters["gb10"]["trial_partition_anchor"] == (
        "PartitionName=gb10 Nodes=trt-gb10-[1-15] Default=YES "
        "MaxTime=1-00:00:00 State=UP PriorityTier=100"
    )
    for cluster in clusters.values():
        assert cluster["builder_partition"] == "loom-task-builder"
        assert cluster["builder_priority_tier"] == 200
        assert cluster["builder_priority_tier"] > cluster["trial_priority_tier"]
        assert cluster["slurm_account"] == "loom-task-builder"
        assert cluster["slurm_qos"] == "loom-task-image-builder"
        assert cluster["builder_partition_line"] == (
            f"PartitionName=loom-task-builder Nodes={cluster['builder_nodes_expression']} "
            "Default=NO MaxTime=02:00:00 State=UP PriorityTier=200 "
            "AllowAccounts=loom-task-builder AllowGroups=loom-task-builder "
            "OverSubscribe=NO"
        )

    serialized = json.dumps(policy, sort_keys=True).lower()
    for forbidden_key in ('"exclusive"', '"nodelist"', '"reservation"'):
        assert forbidden_key not in serialized


def test_runtime_manifest_pins_only_native_rootless_binaries() -> None:
    manifest = _runtime()

    assert manifest["schema"] == "loom.task-image-builder-rootless-runtime/v1"
    assert manifest["release"] == "rootless-runtime-v1"
    assert manifest["versions"] == {
        "buildkit": "v0.32.2",
        "rootlesskit": "v3.1.0",
        "slirp4netns": "v1.3.4",
        "fuse-overlayfs": "v1.17",
    }
    architectures = manifest["architectures"]
    assert set(architectures) == {"x86_64", "aarch64"}
    expected_binaries = {
        "buildctl",
        "buildkit-runc",
        "buildkitd",
        "fuse-overlayfs",
        "rootlessctl",
        "rootlesskit",
        "slirp4netns",
    }
    expected_platforms = {"x86_64": "linux-amd64", "aarch64": "linux-arm64"}
    for architecture, release in architectures.items():
        assert release["platform"] == expected_platforms[architecture]
        assert len(release["artifacts"]) == 4
        assert set(release["binaries"]) == expected_binaries
        for artifact in release["artifacts"]:
            assert artifact["name"]
            assert artifact["url"].startswith("https://github.com/")
            assert SHA256_RE.fullmatch(artifact["sha256"])
        for name, digest in release["binaries"].items():
            assert name == Path(name).name
            assert SHA256_RE.fullmatch(digest)
            assert "qemu" not in name
            assert "cni" not in name
