from __future__ import annotations

import json
import re
import tomllib
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
POLICY_PATH = ROOT / "deploy/task-image-builder/prerequisites-v1.toml"
RUNTIME_PATH = ROOT / "deploy/task-image-builder/rootless-runtime-v1.json"
HOST_RELEASE_PATH = ROOT / "deploy/task-image-builder/host-release-v2.json"
SHA256_RE = re.compile(r"^[a-f0-9]{64}$")


def _policy() -> dict[str, Any]:
    with POLICY_PATH.open("rb") as handle:
        return tomllib.load(handle)


def _runtime() -> dict[str, Any]:
    return json.loads(RUNTIME_PATH.read_text(encoding="utf-8"))


def _host_release() -> dict[str, Any]:
    return json.loads(HOST_RELEASE_PATH.read_text(encoding="utf-8"))


def test_phase_one_policy_is_dynamic_bounded_and_cannot_certify_production() -> None:
    policy = _policy()

    assert policy["schema"] == "loom.task-image-builder-prerequisites/v1"
    assert policy["policy_version"] == "task-image-builder-prerequisites-v1"
    assert policy["host_release_manifest"] == "host-release-v2.json"
    assert not (ROOT / "deploy/task-image-builder/host-release-v1.json").exists()
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
    expected_qos = {
        "oldlab": "loom-task-image-builder-rootless-oldlab",
        "gb10": "loom-task-image-builder-rootless-gb10",
    }
    assert policy["legacy_guard"] == {
        "qos": "loom-task-image-builder",
        "reservation": "loom-task-image-builder",
        "account": "loom-staging",
        "user": "loom-rollout",
        "max_jobs_per_user": 1,
        "max_submit_jobs_per_user": 1,
        "max_wall": "04:00:00",
    }
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
    assert clusters["oldlab"]["legacy_base_qos"] == "normal"
    assert clusters["oldlab"]["legacy_reservation_node"] == "trt-eai-oldlab-6"
    assert clusters["oldlab"]["legacy_reservation_partition"] == "all"
    assert clusters["oldlab"]["trial_partition_anchor"] == (
        "PartitionName=loom-staging Nodes=trt-eai-oldlab-[3-5] Default=NO "
        "MaxTime=2-00:00:00 State=UP PriorityTier=100 AllowGroups=loom-rollout "
        "OverSubscribe=NO"
    )
    assert clusters["gb10"]["architecture"] == "aarch64"
    assert clusters["gb10"]["controller"] == "gx10-01c7"
    assert clusters["gb10"]["trial_partition"] == "loom-staging"
    assert clusters["gb10"]["trial_priority_tier"] == 100
    assert clusters["gb10"]["builder_nodes"] == [f"trt-gb10-{number}" for number in range(1, 16)]
    assert clusters["gb10"]["builder_nodes_expression"] == "trt-gb10-[1-15]"
    assert clusters["gb10"]["slurm_config_owner"] == "root"
    assert clusters["gb10"]["slurm_config_group"] == "root"
    assert clusters["gb10"]["slurm_config_mode"] == "0644"
    assert clusters["gb10"]["legacy_base_qos"] == "loom-staging"
    assert clusters["gb10"]["legacy_reservation_node"] == "trt-gb10-2"
    assert clusters["gb10"]["legacy_reservation_partition"] == "gb10"
    assert clusters["gb10"]["trial_partition_anchor"] == (
        "PartitionName=loom-staging Nodes=trt-gb10-[1-15] Default=NO "
        "MaxTime=1-00:00:00 State=UP PriorityTier=100 "
        "AllowAccounts=loom-staging AllowQos=loom-staging OverSubscribe=NO"
    )
    for cluster_id, cluster in clusters.items():
        assert cluster["builder_partition"] == "loom-task-builder"
        assert cluster["builder_priority_tier"] == 200
        assert cluster["builder_priority_tier"] > cluster["trial_priority_tier"]
        assert cluster["slurm_account"] == "loom-task-builder"
        assert cluster["slurm_qos"] == expected_qos[cluster_id]
        assert cluster["slurm_qos"] != policy["legacy_guard"]["qos"]
        assert cluster["builder_partition_line"] == (
            f"PartitionName=loom-task-builder Nodes={cluster['builder_nodes_expression']} "
            "Default=NO MaxTime=02:00:00 State=UP PriorityTier=200 "
            "AllowAccounts=loom-task-builder AllowGroups=loom-task-builder "
            "OverSubscribe=NO"
        )

    rootless_serialized = json.dumps(policy["clusters"], sort_keys=True).lower()
    for forbidden_key in ('"exclusive"', '"nodelist"', '"reservation"'):
        assert forbidden_key not in rootless_serialized


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


def test_host_release_pins_signed_packages_and_site_preconditions() -> None:
    release = _host_release()
    policy = _policy()

    assert release["schema"] == "loom.task-image-builder-host-release/v2"
    assert release["release"] == "host-release-v2"
    assert release["runtime_manifest"] == "rootless-runtime-v1.json"
    assert release["ubuntu"] == {
        "os_id": "ubuntu",
        "version_id": "24.04",
        "snapshot": "20260820T000000Z",
        "component": "main",
        "signer_fingerprint": "F6ECB3762474EDA9D21B7022871920D1991BC93C",
        "keyring_name": "ubuntu-archive-keyring.gpg",
        "keyring_sha256": (
            "80a36b0a6de2f69f49d2df75ef473ccde121e9e190b9ea01d20a4f63778d5c31"
        ),
    }
    assert release["repositories"] == {
        "amd64": {
            "base_url": "https://snapshot.ubuntu.com/ubuntu/20260820T000000Z",
            "indexes": {
                "noble": {
                    "inrelease_path": "dists/noble/InRelease",
                    "inrelease_size": 255_850,
                    "inrelease_sha256": (
                        "cdb2f31d809f589719a53c6ad15f255b27569c4059542ada282aaa21b8e164b0"
                    ),
                    "packages_path": "dists/noble/main/binary-amd64/Packages.xz",
                    "packages_size": 1_401_160,
                    "packages_sha256": (
                        "2a6a199e1031a5c279cb346646d594993f35b1c03dd4a82aaa0323980dd92451"
                    ),
                },
                "noble-updates": {
                    "inrelease_path": "dists/noble-updates/InRelease",
                    "inrelease_size": 126_125,
                    "inrelease_sha256": (
                        "79d2a1c90ce4f14c98867053190c64a9018ac993702fe5146081873f3da526bf"
                    ),
                    "packages_path": (
                        "dists/noble-updates/main/binary-amd64/Packages.xz"
                    ),
                    "packages_size": 1_215_608,
                    "packages_sha256": (
                        "f43e6d13c95ac3db303163064a024de9718e61191b26f89584288d83842e8419"
                    ),
                },
            },
        },
        "arm64": {
            "base_url": "https://snapshot.ubuntu.com/ubuntu/20260820T000000Z",
            "indexes": {
                "noble": {
                    "inrelease_path": "dists/noble/InRelease",
                    "inrelease_size": 255_850,
                    "inrelease_sha256": (
                        "cdb2f31d809f589719a53c6ad15f255b27569c4059542ada282aaa21b8e164b0"
                    ),
                    "packages_path": "dists/noble/main/binary-arm64/Packages.xz",
                    "packages_size": 1_376_632,
                    "packages_sha256": (
                        "4a1901e6124fb0a111f5dffc8f5c14474f449e2ecfa71f2eaf0b29917edb53f9"
                    ),
                },
                "noble-updates": {
                    "inrelease_path": "dists/noble-updates/InRelease",
                    "inrelease_size": 126_125,
                    "inrelease_sha256": (
                        "79d2a1c90ce4f14c98867053190c64a9018ac993702fe5146081873f3da526bf"
                    ),
                    "packages_path": (
                        "dists/noble-updates/main/binary-arm64/Packages.xz"
                    ),
                    "packages_size": 1_262_792,
                    "packages_sha256": (
                        "573cec116d4f4effc0b2cacbff28fd542182debd30d94ec40be996286690fba5"
                    ),
                },
            },
        },
    }
    assert release["architecture_map"] == {"x86_64": "amd64", "aarch64": "arm64"}

    expected_packages = {
        "amd64": {
            "libsubid4": (
                "noble-updates",
                "1:4.13+dfsg1-4ubuntu3.2",
                "pool/main/s/shadow/libsubid4_4.13+dfsg1-4ubuntu3.2_amd64.deb",
                23_442,
                "ba97fd28c53560a8d2a2261e8f75a7ab4112535b12f9fe1d50970c30051da0da",
            ),
            "uidmap": (
                "noble-updates",
                "1:4.13+dfsg1-4ubuntu3.2",
                "pool/main/s/shadow/uidmap_4.13+dfsg1-4ubuntu3.2_amd64.deb",
                26_006,
                "a80cb7f72dd18c73cbb0b07b7fbe855504f26bfafae072a9b3d125c89d499b9e",
            ),
            "quota": (
                "noble",
                "4.06-1build6",
                "pool/main/q/quota/quota_4.06-1build6_amd64.deb",
                211_338,
                "55cc08283cd16b26ce305c01252d92989ee561ea47d2d781958ea6a27d5a7e25",
            ),
        },
        "arm64": {
            "libsubid4": (
                "noble-updates",
                "1:4.13+dfsg1-4ubuntu3.2",
                "pool/main/s/shadow/libsubid4_4.13+dfsg1-4ubuntu3.2_arm64.deb",
                23_534,
                "00916edc15862421e803bec7e69d548c6ce281badf5d449498085a3b3710639f",
            ),
            "uidmap": (
                "noble-updates",
                "1:4.13+dfsg1-4ubuntu3.2",
                "pool/main/s/shadow/uidmap_4.13+dfsg1-4ubuntu3.2_arm64.deb",
                26_650,
                "052b1852a9ab03d931398a9d0060ef7c312f1b48bc4f4ee4533649bb958b634a",
            ),
            "quota": (
                "noble",
                "4.06-1build6",
                "pool/main/q/quota/quota_4.06-1build6_arm64.deb",
                216_482,
                "2ff4f684f177690caac079d636fa3effdce44e3aa4f6f81f1e24e9ec3e9263b8",
            ),
        },
    }
    for architecture, packages in expected_packages.items():
        assert set(release["packages"][architecture]) == set(packages)
        for package, (source_suite, version, filename, size, digest) in packages.items():
            assert release["packages"][architecture][package] == {
                "package": package,
                "source_suite": source_suite,
                "version": version,
                "architecture": architecture,
                "filename": filename,
                "size": size,
                "sha256": digest,
            }
            assert SHA256_RE.fullmatch(digest)

    storage = policy["storage"]
    assert storage["mountpoint"] == "/var/lib/loom-task-builder"
    assert storage["root"] == "/var/lib/loom-task-builder/jobs"
    assert storage["project_id"] == 300_993
    assert storage["site_filesystem"] == "ext4"
    assert storage["required_mount_options"] == ["prjquota"]
    assert storage["automatic_block_device_changes"] is False
    assert storage["reject_root_filesystem"] is True
    assert storage["reject_network_filesystem"] is True

    clusters = {item["id"]: item for item in policy["clusters"]}
    assert clusters["oldlab"]["cgroup_transition"] == "shared_symlink_to_node_local"
    assert clusters["oldlab"]["cgroup_observed_path"] == "/shared_work/cgroup.conf"
    assert clusters["oldlab"]["cgroup_observed_sha256"] == (
        "a4a31fa25902b407f1c2d865d5667128725aad5bbaa47c1e2b701c226fff8a2f"
    )
    assert clusters["gb10"]["cgroup_transition"] == "node_local"
    assert clusters["gb10"]["cgroup_observed_path"] == "/etc/slurm/cgroup.conf"
    assert clusters["gb10"]["cgroup_observed_sha256"] == (
        "333f28cf5d91fd40515551b239ce4e421b92244d047e5c25b260bca1af2ac10b"
    )

    serialized = json.dumps({"release": release, "storage": storage}, sort_keys=True).lower()
    for forbidden in ("docker.sock", '"reservation"', '"exclusive"', '"nodelist"'):
        assert forbidden not in serialized
