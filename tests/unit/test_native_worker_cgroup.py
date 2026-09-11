"""Native launch reads aggregate controls from the exact held job directory."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from tests.unit.test_native_worker_container import _allocation


def _job(root: Path) -> Path:
    directory = root / _allocation().cgroup_parent.removeprefix("/")
    directory.mkdir(parents=True)
    for path in (directory, *directory.parents):
        if path == root:
            break
        path.chmod(0o755)
    for name, value in {
        "memory.max": str(_allocation().memory_bytes), "memory.swap.max": "0",
        "pids.max": str(_allocation().pids_max), "cpuset.cpus.effective": "7",
    }.items():
        (directory / name).write_text(value + "\n")
        (directory / name).chmod(0o644)
    return directory


def test_native_limits_are_read_from_the_job_not_container_settings(tmp_path):
    from loom_capacity_executor.native_worker_cgroup import open_native_cgroup

    _job(tmp_path)
    with open_native_cgroup(_allocation(), cgroup_root=tmp_path, trusted_uid=os.geteuid()) as opened:
        opened.assert_current()


@pytest.mark.parametrize(("name", "value"), [
    ("memory.max", "max"), ("memory.max", "0"), ("memory.max", str(2 * 1024**3)),
    ("memory.max", "001"), ("memory.max", "x" * 4097),
    ("memory.swap.max", "max"), ("memory.swap.max", "1"),
    ("pids.max", "max"), ("pids.max", "129"), ("pids.max", "0"),
    ("cpuset.cpus.effective", "0-1"), ("cpuset.cpus.effective", ""),
    ("cpuset.cpus.effective", "0-99999999999999999999"),
    ("cpuset.cpus.effective", "7,7"), ("cpuset.cpus.effective", "8-7"),
    ("cpuset.cpus.effective", "-1"), ("cpuset.cpus.effective", "7,"),
])
def test_native_refuses_unbounded_overbudget_or_malformed_ancestor(tmp_path, name, value):
    from loom_capacity_executor.native_worker_cgroup import open_native_cgroup
    from loom_capacity_executor.native_worker_container import NativeContainerError

    directory = _job(tmp_path)
    (directory / name).write_text(value + "\n")
    with pytest.raises(NativeContainerError, match="cgroup"):
        with open_native_cgroup(_allocation(), cgroup_root=tmp_path, trusted_uid=os.geteuid()):
            pytest.fail("invalid aggregate limits must refuse admission")


@pytest.mark.parametrize("tamper", ["missing", "symlink", "writable", "foreign-owner"])
def test_native_refuses_untrusted_controls(tmp_path, tamper):
    from loom_capacity_executor.native_worker_cgroup import open_native_cgroup
    from loom_capacity_executor.native_worker_container import NativeContainerError

    directory = _job(tmp_path)
    trusted_uid = os.geteuid()
    if tamper in {"missing", "symlink"}:
        (directory / "memory.max").unlink()
    if tamper == "symlink":
        (directory / "memory.max").symlink_to(directory / "pids.max")
    if tamper == "writable":
        (directory / "memory.max").chmod(0o666)
    if tamper == "foreign-owner":
        trusted_uid += 1
    with pytest.raises(NativeContainerError, match="cgroup"):
        with open_native_cgroup(_allocation(), cgroup_root=tmp_path, trusted_uid=trusted_uid):
            pytest.fail("untrusted controls must refuse admission")


@pytest.mark.parametrize("tamper", ["limits", "directory", "symlink-parent"])
def test_opened_native_cgroup_cannot_be_replaced_during_handoff(tmp_path, tamper):
    from loom_capacity_executor.native_worker_cgroup import open_native_cgroup
    from loom_capacity_executor.native_worker_container import NativeContainerError

    directory = _job(tmp_path)
    with open_native_cgroup(_allocation(), cgroup_root=tmp_path, trusted_uid=os.geteuid()) as opened:
        if tamper == "limits":
            (directory / "memory.max").write_text("max\n")
        else:
            directory.rename(directory.with_name("retired"))
            if tamper == "directory":
                _job(tmp_path)
            else:
                directory.symlink_to(directory.with_name("retired"), target_is_directory=True)
        with pytest.raises(NativeContainerError, match="cgroup"):
            opened.assert_current()
