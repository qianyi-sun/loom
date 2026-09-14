"""Read-only Linux kernel evidence; not live Slurm or installed acceptance."""

from contextlib import ExitStack
import os
from pathlib import Path
from uuid import UUID

import pytest

from loom_capacity_executor.native_recovery_observation import (
    NativeRecoveryHostIdentityV1,
    _mount_id,
    _open_directory,
    _read_kernel_text,
    _require_cgroup_mount,
    _require_host_cgroup_namespace,
)


def test_actual_cgroup2_mount_identity_cannot_be_replaced_with_scratch(tmp_path):
    root = Path("/sys/fs/cgroup")
    if not (root / "cgroup.controllers").exists():
        pytest.skip("requires Linux cgroup v2; a skip is not mechanism evidence")
    with ExitStack() as stack:
        descriptor = _open_directory(root, stack)
        assert _require_cgroup_mount(descriptor) == _mount_id(descriptor) > 0
        unrelated = _open_directory(tmp_path, stack)
        with pytest.raises(ValueError, match="mount"):
            _require_cgroup_mount(unrelated)


def test_actual_boot_observation_is_canonical_and_bounded():
    path = Path("/proc/sys/kernel/random/boot_id")
    if not path.exists():
        pytest.skip("requires Linux boot identity; a skip is not mechanism evidence")
    wire = _read_kernel_text(path, 128)
    assert wire == f"{UUID(wire.strip())}\n"
    with pytest.raises(ValueError, match="bound"):
        _read_kernel_text(path, 8)


def test_unprivileged_worker_can_compare_installer_namespace_without_ptrace():
    if os.getuid() == 0:
        pytest.skip("requires an actual unprivileged observer; root is not this regression")
    namespace = os.stat("/proc/self/ns/cgroup")
    # Fixture models a protected installer receipt only; it does not establish
    # that this test process occupies the installed host namespace.
    identity = NativeRecoveryHostIdentityV1(node_id="fixture-node", boot_id=UUID(
        _read_kernel_text(Path("/proc/sys/kernel/random/boot_id"), 128).strip()),
        original_uid=os.getuid(), original_gid=os.getgid(),
        cgroup_namespace_device=namespace.st_dev, cgroup_namespace_inode=namespace.st_ino)
    _require_host_cgroup_namespace(identity)
    with pytest.raises(ValueError, match="namespace"):
        _require_host_cgroup_namespace(identity.model_copy(update={"cgroup_namespace_inode": namespace.st_ino + 1}))
