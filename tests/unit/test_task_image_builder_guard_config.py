from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from loom_task_image_builder_guard.config import GuardConfig
from loom_task_image_builder_guard.errors import GuardError
from loom_task_image_builder_guard.safeio import read_stable_file

SHA_A = "1" * 64
SHA_B = "2" * 64
SHA_C = "3" * 64
SHA_D = "4" * 64
SHA_E = "5" * 64
SHA_F = "6" * 64


def _document() -> dict[str, object]:
    return {
        "schema": "loom.task-image-builder-node-guard-config/v1",
        "cluster_id": "oldlab",
        "cpu_arch": "x86_64",
        "node_name": "trt-eai-oldlab-3",
        "identity": {
            "uid": 993,
            "gid": 980,
            "forbidden_supplementary_gids": [0, 27, 128],
            "supervisor_path": "/usr/local/libexec/loom-task-builder-supervisor",
            "supervisor_sha256": SHA_A,
        },
        "protocol": {
            "socket_path": "/run/loom-task-image-builder-guard/guard.sock",
            "socket_mode": 432,
            "socket_gid": 980,
            "max_packet_bytes": 4096,
            "max_pending_peers": 16,
            "requests_per_second": 32,
            "ack_timeout_seconds": 5,
        },
        "authority": {
            "base_url": "https://loom-task-image-authority.loom.svc:8445",
            "ca_path": "/etc/loom/task-image-builder-guard/client-ca.pem",
            "cert_path": "/etc/loom/task-image-builder-guard/client.pem",
            "key_path": "/etc/loom/task-image-builder-guard/client-key.pem",
            "bearer_path": "/etc/loom/task-image-builder-guard/node-bearer",
            "timeout_seconds": 10,
            "max_response_bytes": 65536,
        },
        "commands": {
            "scontrol": {"path": "/usr/bin/scontrol", "sha256": SHA_A},
            "sacct": {"path": "/usr/bin/sacct", "sha256": SHA_B},
            "bpftool": {
                "path": "/opt/loom-task-image-builder-guard/release/bpftool",
                "sha256": SHA_C,
            },
        },
        "slurm": {
            "cluster_name": "trt-oldlab",
            "request_sha256": SHA_D,
            "account": "loom-task-builder",
            "partition": "loom-task-builder",
            "qos": "loom-task-image-builder-rootless-oldlab",
            "feature": "loom_rootless_buildkit",
            "cpus": 8,
            "memory_mib": 32768,
            "wall_time": "02:00:00",
        },
        "containment": {
            "cgroup_root": "/sys/fs/cgroup",
            "bpffs_root": "/sys/fs/bpf/loom-task-image-builder",
            "ledger_root": "/var/lib/loom-task-image-builder-guard/ledger",
            "bpf_object_path": (
                "/opt/loom-task-image-builder-guard/release/guard-network-v1.bpf.o"
            ),
            "pids_max": 4096,
            "io_limits": [
                {
                    "device": "8:1",
                    "rbps": 104857600,
                    "wbps": 104857600,
                    "riops": 10000,
                    "wiops": 10000,
                }
            ],
            "containment_policy_sha256": SHA_E,
            "resource_profile_sha256": SHA_F,
            "bpf_program_sha256": SHA_A,
            "bpf_map_schema_sha256": SHA_B,
        },
        "service": {
            "attestation_interval_seconds": 15,
            "attestation_lifetime_seconds": 60,
            "max_ledger_entries": 128,
        },
    }


def _write_config(path: Path, document: dict[str, object]) -> None:
    path.write_text(json.dumps(document), encoding="ascii")
    path.chmod(0o600)


def test_loads_exact_native_guard_configuration(tmp_path: Path) -> None:
    path = tmp_path / "guard.json"
    _write_config(path, _document())

    config = GuardConfig.from_file(path)

    assert (config.cluster_id, config.cpu_arch, config.node_name) == (
        "oldlab",
        "x86_64",
        "trt-eai-oldlab-3",
    )
    assert config.identity.forbidden_supplementary_gids == (0, 27, 128)
    assert config.identity.supervisor_path == Path(
        "/usr/local/libexec/loom-task-builder-supervisor"
    )
    assert config.slurm.cluster_name == "trt-oldlab"
    assert config.commands.bpftool.sha256 == SHA_C
    assert config.containment.io_limits[0].device == "8:1"
    assert config.service.attestation_interval_seconds == 15


@pytest.mark.parametrize(
    ("mutation", "code"),
    [
        (lambda value: value.update({"extra": True}), "config_fields_invalid"),
        (
            lambda value: value["authority"].update(  # type: ignore[union-attr]
                {"base_url": "http://authority.invalid:8445"}
            ),
            "config_authority_invalid",
        ),
        (
            lambda value: value.update({"cpu_arch": "arm64"}),
            "config_native_pair_invalid",
        ),
        (
            lambda value: value["identity"].update(  # type: ignore[union-attr]
                {"supervisor_sha256": "0" * 64}
            ),
            "config_digest_invalid",
        ),
        (
            lambda value: value["commands"]["sacct"].update(  # type: ignore[index,union-attr]
                {"path": "sacct"}
            ),
            "config_path_invalid",
        ),
        (
            lambda value: value["slurm"].update(  # type: ignore[union-attr]
                {"cluster_name": "foreign-cluster"}
            ),
            "config_slurm_invalid",
        ),
        (
            lambda value: value["protocol"].update(  # type: ignore[union-attr]
                {"max_packet_bytes": 0}
            ),
            "config_limit_invalid",
        ),
        (
            lambda value: value["service"].update(  # type: ignore[union-attr]
                {"attestation_interval_seconds": 60}
            ),
            "config_attestation_invalid",
        ),
    ],
)
def test_rejects_configuration_that_broadens_guard_authority(
    tmp_path: Path,
    mutation: object,
    code: str,
) -> None:
    document = _document()
    assert callable(mutation)
    mutation(document)
    path = tmp_path / "guard.json"
    _write_config(path, document)

    with pytest.raises(GuardError) as caught:
        GuardConfig.from_file(path)

    assert caught.value.code == code
    assert str(caught.value) == code


def test_rejects_duplicate_json_keys(tmp_path: Path) -> None:
    path = tmp_path / "guard.json"
    path.write_text(
        '{"schema":"loom.task-image-builder-node-guard-config/v1",'
        '"schema":"loom.task-image-builder-node-guard-config/v1"}',
        encoding="ascii",
    )
    path.chmod(0o600)

    with pytest.raises(GuardError) as caught:
        GuardConfig.from_file(path)

    assert caught.value.code == "config_json_invalid"


def test_stable_file_rejects_symlink_and_wrong_mode(tmp_path: Path) -> None:
    target = tmp_path / "target"
    target.write_bytes(b"secret")
    target.chmod(0o644)
    link = tmp_path / "link"
    link.symlink_to(target)

    for path in (target, link):
        with pytest.raises(GuardError) as caught:
            read_stable_file(
                path,
                uid=os.geteuid(),
                gid=os.getegid(),
                mode=0o600,
                maximum=64,
            )
        assert caught.value.code == "safe_file_invalid"


def test_stable_file_rejects_payload_over_limit(tmp_path: Path) -> None:
    path = tmp_path / "bounded"
    path.write_bytes(b"12345")
    path.chmod(0o600)

    with pytest.raises(GuardError) as caught:
        read_stable_file(
            path,
            uid=os.geteuid(),
            gid=os.getegid(),
            mode=0o600,
            maximum=4,
        )

    assert caught.value.code == "safe_file_too_large"


def test_guard_error_never_reflects_secret_or_unbounded_detail() -> None:
    error = GuardError("authority_failed", detail="loom_tibp_secret-value")

    assert str(error) == "authority_failed"
    assert repr(error) == "GuardError('authority_failed')"
