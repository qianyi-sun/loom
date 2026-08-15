from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from uuid import UUID

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from loom_capacity_executor.config import ExecutorConfigError, PoolExecutorConfig
from loom_capacity_manager.ownership import public_key_fingerprint


@dataclass(frozen=True)
class ExecutorFiles:
    config: Path
    bearer: Path


def _private(path: Path, payload: str) -> None:
    path.write_text(payload, encoding="utf-8")
    path.chmod(0o600)


def executor_files(tmp_path: Path, *, pool_id: str = "oldlab") -> ExecutorFiles:
    state = tmp_path / f"{pool_id}-state"
    state.mkdir(mode=0o700)
    bearer = tmp_path / f"{pool_id}.bearer"
    _private(bearer, "test-bearer\n")
    tls_ca = tmp_path / f"{pool_id}.ca"
    tls_cert = tmp_path / f"{pool_id}.cert"
    tls_key = tmp_path / f"{pool_id}.tls-key"
    for path in (tls_ca, tls_cert, tls_key):
        _private(path, "test-tls\n")
    private_key = Ed25519PrivateKey.from_private_bytes(
        (b"\x11" if pool_id == "gb10" else b"\x12") * 32
    )
    key = tmp_path / f"{pool_id}.key"
    key.write_bytes(private_key.private_bytes_raw())
    key.chmod(0o600)
    config = tmp_path / f"{pool_id}.json"
    _private(
        config,
        json.dumps(
            {
                "pool_id": pool_id,
                "pool_generation": 1,
                "executor_id": f"{pool_id}-executor",
                "executor_incarnation": str(
                    UUID("00000000-0000-4000-8000-000000000712")
                    if pool_id == "oldlab"
                    else UUID("00000000-0000-4000-8000-000000000711")
                ),
                "controller_authority_sha256": "d" * 64 if pool_id == "oldlab" else "c" * 64,
                "local_authority_sha256": "b" * 64 if pool_id == "oldlab" else "a" * 64,
                "signing_key_id": f"{pool_id}-key",
                "signing_key_sha256": public_key_fingerprint(private_key.public_key()),
                "manager_origin": "https://manager.example.test",
                "bearer_token_file": str(bearer),
                "tls_ca_file": str(tls_ca),
                "tls_certificate_file": str(tls_cert),
                "tls_private_key_file": str(tls_key),
                "state_directory": str(state),
                "journal_file": str(state / "executor.journal"),
                "ownership_key_file": str(key),
                "controller_host": f"{pool_id}-controller.example.test",
                "slurm_cluster": pool_id,
                "partition": f"{pool_id}-workers",
                "association": f"{pool_id}-executor",
                "submitter": f"loom-{pool_id}",
                "qos": "loom",
                "profile_id": f"{pool_id}-profile",
                "profile_generation": 1,
                "profile_digest": "9" * 64 if pool_id == "oldlab" else "8" * 64,
                "local_uid": os.geteuid(),
                "slurm_executables": {
                    name: f"/usr/bin/{name}"
                    for name in ("scontrol", "sacctmgr", "squeue", "sbatch", "scancel", "sacct")
                },
            }
        ),
    )
    return ExecutorFiles(config=config, bearer=bearer)


def test_group_readable_bearer_is_rejected(tmp_path: Path) -> None:
    files = executor_files(tmp_path)
    files.bearer.chmod(0o640)
    with pytest.raises(ExecutorConfigError, match="0600"):
        PoolExecutorConfig.from_files(files.config)


def test_oldlab_and_gb10_configurations_have_distinct_exact_bindings(tmp_path: Path) -> None:
    oldlab = PoolExecutorConfig.from_files(executor_files(tmp_path, pool_id="oldlab").config)
    gb10 = PoolExecutorConfig.from_files(executor_files(tmp_path, pool_id="gb10").config)

    assert oldlab.pool_id == "oldlab"
    assert gb10.pool_id == "gb10"
    assert oldlab.controller_authority_sha256 != gb10.controller_authority_sha256
    assert oldlab.executor_id != gb10.executor_id
    assert oldlab.manifest.partition != gb10.manifest.partition
    assert oldlab.manifest.association != gb10.manifest.association
    assert oldlab.ownership_key.public_key_sha256 != gb10.ownership_key.public_key_sha256


def test_cross_loaded_pool_configuration_fails_before_registration(tmp_path: Path) -> None:
    config = PoolExecutorConfig.from_files(executor_files(tmp_path, pool_id="oldlab").config)
    with pytest.raises(ExecutorConfigError, match="pool binding"):
        config.assert_pool("gb10")


def test_config_accepts_its_exact_inventory_policy_binding(tmp_path: Path) -> None:
    config = PoolExecutorConfig.from_files(executor_files(tmp_path, pool_id="oldlab").config)

    config.assert_inventory_policy_binding(
        pool_id="oldlab",
        pool_generation=1,
        query_uid=os.geteuid(),
        controller_cluster="oldlab",
        relevant_partitions=("oldlab-workers",),
    )


@pytest.mark.parametrize(
    "change",
    (
        {"pool_id": "gb10"},
        {"pool_generation": 2},
        {"query_uid": 0},
        {"query_uid": 1001 if os.geteuid() != 1001 else 1002},
        {"controller_cluster": "gb10"},
        {"relevant_partitions": ("another-partition",)},
    ),
)
def test_config_rejects_inventory_policy_binding_drift(
    tmp_path: Path,
    change: dict[str, object],
) -> None:
    config = PoolExecutorConfig.from_files(executor_files(tmp_path, pool_id="oldlab").config)
    binding: dict[str, object] = {
        "pool_id": "oldlab",
        "pool_generation": 1,
        "query_uid": os.geteuid(),
        "controller_cluster": "oldlab",
        "relevant_partitions": ("oldlab-workers",),
    }
    binding.update(change)

    with pytest.raises(ExecutorConfigError, match="inventory policy"):
        config.assert_inventory_policy_binding(**binding)  # type: ignore[arg-type]


def test_config_requires_an_absolute_journal_path(tmp_path: Path) -> None:
    files = executor_files(tmp_path)
    payload = json.loads(files.config.read_text(encoding="utf-8"))
    payload["journal_file"] = "executor.journal"
    _private(files.config, json.dumps(payload))
    with pytest.raises(ExecutorConfigError, match="absolute"):
        PoolExecutorConfig.from_files(files.config)


def test_config_rejects_symlinked_bearer(tmp_path: Path) -> None:
    files = executor_files(tmp_path)
    target = tmp_path / "target"
    _private(target, "test-bearer\n")
    files.bearer.unlink()
    files.bearer.symlink_to(target)
    with pytest.raises(ExecutorConfigError, match="regular nonsymlink"):
        PoolExecutorConfig.from_files(files.config)


def test_config_requires_exact_controller_partition_association_and_executables(
    tmp_path: Path,
) -> None:
    files = executor_files(tmp_path)
    payload = json.loads(files.config.read_text(encoding="utf-8"))
    payload.pop("association")
    _private(files.config, json.dumps(payload))
    with pytest.raises(ExecutorConfigError, match="association"):
        PoolExecutorConfig.from_files(files.config)


def test_config_rejects_private_key_that_does_not_match_registered_fingerprint(
    tmp_path: Path,
) -> None:
    files = executor_files(tmp_path)
    payload = json.loads(files.config.read_text(encoding="utf-8"))
    payload["signing_key_sha256"] = "f" * 64
    _private(files.config, json.dumps(payload))
    with pytest.raises(ExecutorConfigError, match="fingerprint"):
        PoolExecutorConfig.from_files(files.config)


def test_pinned_manifest_rejects_one_field_mutation_before_registration(tmp_path: Path) -> None:
    files = executor_files(tmp_path)
    loaded = PoolExecutorConfig.from_files(files.config)
    pinned = loaded.manifest.sha256()
    payload = json.loads(files.config.read_text(encoding="utf-8"))
    payload["tls_ca_file"] = payload["tls_certificate_file"]
    _private(files.config, json.dumps(payload))
    with pytest.raises(ExecutorConfigError, match="pinned immutable manifest"):
        PoolExecutorConfig.from_files(files.config, expected_manifest_sha256=pinned)


@pytest.mark.parametrize(
    "field",
    (
        "controller_host",
        "ownership_key_file",
        "bearer_token_file",
        "state_directory",
        "profile_digest",
    ),
)
def test_pinned_manifest_rejects_each_cross_pool_binding(tmp_path: Path, field: str) -> None:
    oldlab_files = executor_files(tmp_path, pool_id="oldlab")
    gb10_files = executor_files(tmp_path, pool_id="gb10")
    pinned = PoolExecutorConfig.from_files(oldlab_files.config).manifest.sha256()
    oldlab_payload = json.loads(oldlab_files.config.read_text(encoding="utf-8"))
    gb10_payload = json.loads(gb10_files.config.read_text(encoding="utf-8"))
    oldlab_payload[field] = gb10_payload[field]
    if field == "state_directory":
        oldlab_payload["journal_file"] = gb10_payload["journal_file"]
    _private(oldlab_files.config, json.dumps(oldlab_payload))
    with pytest.raises(ExecutorConfigError):
        PoolExecutorConfig.from_files(oldlab_files.config, expected_manifest_sha256=pinned)
