from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from uuid import UUID

import pytest

from loom_capacity_executor.config import ExecutorConfigError, PoolExecutorConfig


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
    key = tmp_path / f"{pool_id}.key"
    key.write_bytes(b"x" * 32)
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
                "signing_key_sha256": "1" * 64,
                "manager_origin": "https://manager.example.test",
                "bearer_token_file": str(bearer),
                "state_directory": str(state),
                "journal_file": str(state / "executor.journal"),
                "ownership_key_file": str(key),
                "controller_host": f"{pool_id}-controller.example.test",
                "partition": f"{pool_id}-workers",
                "association": f"{pool_id}-executor",
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


def test_cross_loaded_pool_configuration_fails_before_registration(tmp_path: Path) -> None:
    config = PoolExecutorConfig.from_files(executor_files(tmp_path, pool_id="oldlab").config)
    with pytest.raises(ExecutorConfigError, match="pool binding"):
        config.assert_pool("gb10")


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
