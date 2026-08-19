from __future__ import annotations

import os
from pathlib import Path
from uuid import UUID

import pytest

from loom_worker.pipeline_runtime_secret import (
    AttemptRuntimeSecretLifecycle,
    PipelineStepJwtRotator,
    RuntimeSecretError,
    RuntimeSecretMount,
    require_runtime_secret_tmpfs,
)


def test_attempt_secret_rotation_and_teardown_leave_no_token(tmp_path: Path) -> None:
    secret_dir = tmp_path / "attempt-secret"
    secret_file = RuntimeSecretMount(
        secret_dir,
        container_uid=os.getuid(),
        container_gid=os.getgid(),
    )
    secret_file.initialize()
    secret_file.rotate("loom_step_first")
    first_inode = (secret_dir / "step-jwt").stat().st_ino
    secret_file.rotate("loom_step_second")
    current = secret_dir / "step-jwt"
    assert secret_file.read_verified() == b"loom_step_second"
    assert current.stat().st_ino != first_inode
    assert current.stat().st_mode & 0o777 == 0o400
    secret_file.teardown()
    assert not secret_dir.exists()


def test_runtime_secret_root_requires_most_specific_tmpfs_mount(tmp_path: Path) -> None:
    root = tmp_path / "secrets"
    root.mkdir()
    mountinfo = tmp_path / "mountinfo"
    mountinfo.write_text(
        f"1 0 0:1 / / rw - ext4 disk rw\n2 1 0:2 / {tmp_path} rw - tmpfs tmpfs rw\n",
        encoding="utf-8",
    )

    assert require_runtime_secret_tmpfs(root, mountinfo_path=mountinfo) == root.resolve()

    mountinfo.write_text(
        f"1 0 0:1 / {tmp_path} rw - tmpfs tmpfs rw\n2 1 0:2 / {root} rw - ext4 disk rw\n",
        encoding="utf-8",
    )
    with pytest.raises(RuntimeSecretError, match="not backed by tmpfs"):
        require_runtime_secret_tmpfs(root, mountinfo_path=mountinfo)


async def test_terminalgen_attempt_secret_lifecycle_mints_and_removes_token(
    tmp_path: Path,
) -> None:
    uid = os.getuid() or 65_534
    gid = os.getgid() or 65_534
    secret_mount = RuntimeSecretMount(
        tmp_path / "attempt-secret",
        container_uid=uid,
        container_gid=gid,
    )
    calls: list[tuple[UUID, str, int]] = []

    async def _mint(attempt_id: UUID, step_id: str, ttl_seconds: int) -> str:
        calls.append((attempt_id, step_id, ttl_seconds))
        return "loom_step_terminalgen"

    attempt_id = UUID(int=1432)
    lifecycle = AttemptRuntimeSecretLifecycle(
        secret_mount=secret_mount,
        rotator=PipelineStepJwtRotator(
            attempt_id=attempt_id,
            step_id="generate_card_00",
            ttl_seconds=600,
            secret_mount=secret_mount,
            mint=_mint,
        ),
    )

    await lifecycle.start()
    assert calls == [(attempt_id, "generate_card_00", 600)]
    assert secret_mount.read_verified() == b"loom_step_terminalgen"
    await lifecycle.teardown()
    assert lifecycle.absent


def test_terminalgen_rotator_rejects_unregistered_node(tmp_path: Path) -> None:
    uid = os.getuid() or 65_534
    gid = os.getgid() or 65_534
    secret_mount = RuntimeSecretMount(
        tmp_path / "attempt-secret",
        container_uid=uid,
        container_gid=gid,
    )

    async def _mint(_attempt_id: UUID, _step_id: str, _ttl_seconds: int) -> str:
        return "unreachable"

    with pytest.raises(RuntimeSecretError, match="registered Provider nodes"):
        PipelineStepJwtRotator(
            attempt_id=UUID(int=1432),
            step_id="plan_batch",
            ttl_seconds=600,
            secret_mount=secret_mount,
            mint=_mint,
        )
