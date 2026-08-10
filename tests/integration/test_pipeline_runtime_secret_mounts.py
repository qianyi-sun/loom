from __future__ import annotations

import os
from pathlib import Path

from loom_worker.pipeline_runtime_secret import RuntimeSecretMount


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
