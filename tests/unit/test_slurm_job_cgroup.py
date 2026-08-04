from __future__ import annotations

from pathlib import Path

import pytest

from loom_control_plane import slurm_job_cgroup as cgroup_module
from loom_control_plane.slurm_job_cgroup import (
    SlurmJobCgroupError,
    discover_slurm_job_cgroup,
)


def _cgroup_fixture(
    tmp_path: Path,
    *,
    process_path: str = ("/system.slice/slurmstepd.scope/job_123/step_batch/user/task_0"),
    job_path: str = "/system.slice/slurmstepd.scope/job_123",
    controllers: str = "cpu memory pids",
    subtree_control: str = "cpu memory pids",
    cgroup_type: str = "domain",
    resident_processes: str = "",
    pids_max: str | None = "3072",
) -> tuple[Path, Path]:
    proc_cgroup = tmp_path / "proc-self-cgroup"
    proc_cgroup.write_text(f"0::{process_path}\n", encoding="utf-8")
    root = tmp_path / "cgroup"
    process_dir = root / process_path.removeprefix("/")
    process_dir.mkdir(parents=True)
    job_dir = root / job_path.removeprefix("/")
    (job_dir / "cgroup.controllers").write_text(controllers, encoding="utf-8")
    (job_dir / "cgroup.subtree_control").write_text(
        subtree_control,
        encoding="utf-8",
    )
    (job_dir / "cgroup.type").write_text(cgroup_type, encoding="utf-8")
    (job_dir / "cgroup.procs").write_text(resident_processes, encoding="utf-8")
    if pids_max is not None:
        (job_dir / "pids.max").write_text(pids_max, encoding="utf-8")
    return proc_cgroup, root


def test_discovers_named_slurm_job_scope(tmp_path: Path) -> None:
    proc_cgroup, root = _cgroup_fixture(tmp_path)

    assert (
        discover_slurm_job_cgroup(
            job_id="123",
            pids_max=3072,
            proc_cgroup=proc_cgroup,
            cgroup_root=root,
        )
        == "/system.slice/slurmstepd.scope/job_123"
    )


def test_rejects_opaque_scope_without_job_id_binding(tmp_path: Path) -> None:
    proc_cgroup, root = _cgroup_fixture(
        tmp_path,
        process_path="/system.slice/slurmstepd.scope/s5K1ABC/step_batch/user/task_0",
        job_path="/system.slice/slurmstepd.scope/s5K1ABC",
    )

    with pytest.raises(SlurmJobCgroupError, match="does not bind"):
        discover_slurm_job_cgroup(
            job_id="123",
            pids_max=3072,
            proc_cgroup=proc_cgroup,
            cgroup_root=root,
        )


@pytest.mark.parametrize(
    ("controllers", "subtree_control", "match"),
    [
        ("cpu memory", "cpu memory pids", "does not expose"),
        ("cpu memory pids", "cpu memory", "not delegated"),
    ],
)
def test_rejects_incomplete_controller_delegation(
    tmp_path: Path,
    controllers: str,
    subtree_control: str,
    match: str,
) -> None:
    proc_cgroup, root = _cgroup_fixture(
        tmp_path,
        controllers=controllers,
        subtree_control=subtree_control,
    )

    with pytest.raises(SlurmJobCgroupError, match=match):
        discover_slurm_job_cgroup(
            job_id="123",
            pids_max=3072,
            proc_cgroup=proc_cgroup,
            cgroup_root=root,
        )


def test_rejects_mismatched_named_job_scope(tmp_path: Path) -> None:
    proc_cgroup, root = _cgroup_fixture(tmp_path)

    with pytest.raises(SlurmJobCgroupError, match="different job"):
        discover_slurm_job_cgroup(
            job_id="456",
            pids_max=3072,
            proc_cgroup=proc_cgroup,
            cgroup_root=root,
        )


def test_rejects_non_slurm_ambient_cgroup(tmp_path: Path) -> None:
    proc_cgroup = tmp_path / "proc-self-cgroup"
    proc_cgroup.write_text("0::/user.slice/session.scope\n", encoding="utf-8")
    root = tmp_path / "cgroup"
    (root / "user.slice/session.scope").mkdir(parents=True)

    with pytest.raises(SlurmJobCgroupError, match="identifiable Slurm"):
        discover_slurm_job_cgroup(
            job_id="123",
            pids_max=3072,
            proc_cgroup=proc_cgroup,
            cgroup_root=root,
        )


def test_rejects_matching_job_name_outside_slurm_scope(tmp_path: Path) -> None:
    proc_cgroup = tmp_path / "proc-self-cgroup"
    proc_cgroup.write_text("0::/user.slice/job_123/step_batch\n", encoding="utf-8")
    root = tmp_path / "cgroup"
    (root / "user.slice/job_123/step_batch").mkdir(parents=True)

    with pytest.raises(SlurmJobCgroupError, match="identifiable Slurm"):
        discover_slurm_job_cgroup(
            job_id="123",
            pids_max=3072,
            proc_cgroup=proc_cgroup,
            cgroup_root=root,
        )


def test_rejects_job_scope_with_internal_processes(tmp_path: Path) -> None:
    proc_cgroup, root = _cgroup_fixture(tmp_path, resident_processes="987\n")

    with pytest.raises(SlurmJobCgroupError, match="internal processes"):
        discover_slurm_job_cgroup(
            job_id="123",
            pids_max=3072,
            proc_cgroup=proc_cgroup,
            cgroup_root=root,
        )


@pytest.mark.parametrize("actual", ["max", "3071", "03072"])
def test_rejects_unbounded_or_mismatched_job_pids_max_without_wait(
    tmp_path: Path,
    actual: str,
) -> None:
    proc_cgroup, root = _cgroup_fixture(tmp_path, pids_max=actual)

    with pytest.raises(SlurmJobCgroupError, match=r"pids\.max did not converge"):
        discover_slurm_job_cgroup(
            job_id="123",
            pids_max=3072,
            proc_cgroup=proc_cgroup,
            cgroup_root=root,
        )


@pytest.mark.parametrize("initial", ["max", None])
def test_waits_for_root_guard_to_apply_job_pids_max(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    initial: str | None,
) -> None:
    proc_cgroup, root = _cgroup_fixture(tmp_path, pids_max=initial)
    pids_max_path = root / "system.slice/slurmstepd.scope/job_123/pids.max"
    now = 100.0
    sleeps: list[float] = []

    monkeypatch.setattr(cgroup_module.time, "monotonic", lambda: now)

    def apply_guard_value(delay: float) -> None:
        nonlocal now
        sleeps.append(delay)
        now += delay
        pids_max_path.write_text("3072\n", encoding="utf-8")

    monkeypatch.setattr(cgroup_module.time, "sleep", apply_guard_value)

    assert (
        discover_slurm_job_cgroup(
            job_id="123",
            pids_max=3072,
            wait_seconds=1.0,
            proc_cgroup=proc_cgroup,
            cgroup_root=root,
        )
        == "/system.slice/slurmstepd.scope/job_123"
    )
    assert sleeps == pytest.approx([0.1])


def test_waits_for_root_guard_to_delegate_controllers(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The guard enables subtree delegation asynchronously after the job starts;
    # discovery must poll for it within the bounded wait rather than failing
    # closed the instant it observes an as-yet-undelegated subtree.
    proc_cgroup, root = _cgroup_fixture(tmp_path, subtree_control="")
    subtree_path = root / "system.slice/slurmstepd.scope/job_123/cgroup.subtree_control"
    now = 100.0
    sleeps: list[float] = []

    monkeypatch.setattr(cgroup_module.time, "monotonic", lambda: now)

    def apply_guard_delegation(delay: float) -> None:
        nonlocal now
        sleeps.append(delay)
        now += delay
        subtree_path.write_text("cpu memory pids", encoding="utf-8")

    monkeypatch.setattr(cgroup_module.time, "sleep", apply_guard_delegation)

    assert (
        discover_slurm_job_cgroup(
            job_id="123",
            pids_max=3072,
            wait_seconds=1.0,
            proc_cgroup=proc_cgroup,
            cgroup_root=root,
        )
        == "/system.slice/slurmstepd.scope/job_123"
    )
    assert sleeps == pytest.approx([0.1])


def test_job_pids_max_wait_times_out_deterministically(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    proc_cgroup, root = _cgroup_fixture(tmp_path, pids_max="max")
    now = 200.0
    sleeps: list[float] = []

    monkeypatch.setattr(cgroup_module.time, "monotonic", lambda: now)

    def advance(delay: float) -> None:
        nonlocal now
        sleeps.append(delay)
        now += delay

    monkeypatch.setattr(cgroup_module.time, "sleep", advance)

    with pytest.raises(SlurmJobCgroupError, match="bounded wait expired"):
        discover_slurm_job_cgroup(
            job_id="123",
            pids_max=3072,
            wait_seconds=0.25,
            proc_cgroup=proc_cgroup,
            cgroup_root=root,
        )
    assert sum(sleeps) == pytest.approx(0.25)
    assert all(0 < delay <= 0.1 for delay in sleeps)


def test_structural_wrong_job_fails_without_waiting(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    proc_cgroup, root = _cgroup_fixture(tmp_path)

    def unexpected_sleep(_: float) -> None:
        raise AssertionError("structural failures must not poll")

    monkeypatch.setattr(cgroup_module.time, "sleep", unexpected_sleep)

    with pytest.raises(SlurmJobCgroupError, match="different job"):
        discover_slurm_job_cgroup(
            job_id="456",
            pids_max=3072,
            wait_seconds=30,
            proc_cgroup=proc_cgroup,
            cgroup_root=root,
        )


def test_structural_symlink_fails_without_waiting(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    proc_cgroup, root = _cgroup_fixture(tmp_path)
    job_dir = root / "system.slice/slurmstepd.scope/job_123"
    real_job_dir = root / "system.slice/slurmstepd.scope/real-job-123"
    job_dir.rename(real_job_dir)
    job_dir.symlink_to(real_job_dir, target_is_directory=True)

    def unexpected_sleep(_: float) -> None:
        raise AssertionError("unsafe paths must not poll")

    monkeypatch.setattr(cgroup_module.time, "sleep", unexpected_sleep)

    with pytest.raises(SlurmJobCgroupError, match="symlinks are forbidden"):
        discover_slurm_job_cgroup(
            job_id="123",
            pids_max=3072,
            wait_seconds=30,
            proc_cgroup=proc_cgroup,
            cgroup_root=root,
        )


@pytest.mark.parametrize("wait_seconds", [-1, 61, float("inf"), float("nan"), True])
def test_rejects_unbounded_wait_values(
    tmp_path: Path,
    wait_seconds: object,
) -> None:
    proc_cgroup, root = _cgroup_fixture(tmp_path)

    with pytest.raises(SlurmJobCgroupError, match="wait_seconds must be between"):
        discover_slurm_job_cgroup(
            job_id="123",
            pids_max=3072,
            wait_seconds=wait_seconds,  # type: ignore[arg-type]
            proc_cgroup=proc_cgroup,
            cgroup_root=root,
        )
