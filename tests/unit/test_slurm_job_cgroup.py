from __future__ import annotations

import hashlib
import json
import os
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
    (job_dir / "cpu.max").write_text("200000 100000\n", encoding="utf-8")
    (job_dir / "memory.max").write_text("8388608000\n", encoding="utf-8")
    (job_dir / "memory.swap.max").write_text("max\n", encoding="utf-8")
    (job_dir / "cpuset.cpus.effective").write_text("0-1\n", encoding="utf-8")
    (job_dir / "cpuset.mems.effective").write_text("0\n", encoding="utf-8")
    if pids_max is not None:
        (job_dir / "pids.max").write_text(pids_max, encoding="utf-8")
    return proc_cgroup, root


_SYSTEMD_BINDING = {
    "docker_driver": "systemd",
    "job_id": "123",
    "pids_max": 3072,
    "cluster": "trt-oldlab",
    "node": "oldlab-1",
    "job_start_time": "2026-07-30T00:00:00",
    "account": "lda-a1b2c3d4",
    "env_id": "denv-a1b2c3d4",
    "resource_generation": 7,
    "runtime_id": "future-dev",
    "candidate_id": "cand-" + "1" * 40,
    "candidate_sha": "a" * 40,
    "candidate_tree": "b" * 40,
}


def _publish_systemd_authority(
    tmp_path: Path,
    *,
    extra_field: bool = False,
) -> tuple[Path, Path, str]:
    receipt_root = tmp_path / "receipts"
    unit_root = tmp_path / "units"
    receipt_root.mkdir(parents=True)
    unit_root.mkdir(parents=True)
    unit, identity_sha256 = cgroup_module._systemd_slice_identity(
        **{
            key: value
            for key, value in _SYSTEMD_BINDING.items()
            if key not in {"docker_driver", "pids_max"}
        },
    )
    unit_bytes = b"[Unit]\nDescription=test\n[Slice]\nMemoryMax=8388608000\n"
    unit_path = unit_root / unit
    unit_path.write_bytes(unit_bytes)
    unit_path.chmod(0o644)
    unsigned = {
        "schema_version": 1,
        "kind": "loom.slurm-systemd-slice-receipt",
        "systemd_slice": unit,
        "slice_identity_sha256": identity_sha256,
        "unit_sha256": hashlib.sha256(unit_bytes).hexdigest(),
        "job_id": "123",
        "job_start_time": _SYSTEMD_BINDING["job_start_time"],
        "cluster": "trt-oldlab",
        "node_list": "oldlab-1",
        "account": "lda-a1b2c3d4",
        "env_id": "denv-a1b2c3d4",
        "resource_generation": 7,
        "runtime_id": "future-dev",
        "candidate_id": "cand-" + "1" * 40,
        "candidate_sha": "a" * 40,
        "candidate_tree": "b" * 40,
        "cpu_max": "200000 100000",
        "memory_max": "8388608000",
        "memory_swap_max_source": "max",
        "memory_swap_max_effective": "0",
        "pids_max": "3072",
        "cpuset_cpus": "0-1",
        "cpuset_mems": "0",
        "gpu_tres": "not-required",
        "gpu_detail": "not-required",
    }
    if extra_field:
        unsigned["unexpected"] = True
    receipt = {
        **unsigned,
        "payload_sha256": hashlib.sha256(
            json.dumps(unsigned, sort_keys=True, separators=(",", ":")).encode("ascii"),
        ).hexdigest(),
    }
    receipt_path = receipt_root / f"{unit}.json"
    receipt_path.write_text(
        json.dumps(receipt, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="ascii",
    )
    receipt_path.chmod(0o444)
    return receipt_root, unit_root, unit


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


def test_systemd_driver_returns_exact_receipt_bound_slice(tmp_path: Path) -> None:
    proc_cgroup, root = _cgroup_fixture(tmp_path)
    receipt_root, unit_root, unit = _publish_systemd_authority(tmp_path)

    assert (
        cgroup_module.discover_docker_cgroup_parent(
            **_SYSTEMD_BINDING,
            proc_cgroup=proc_cgroup,
            cgroup_root=root,
            receipt_root=receipt_root,
            unit_root=unit_root,
            expected_authority_uid=os.getuid(),
            expected_authority_gid=os.getgid(),
        )
        == unit
    )


def test_systemd_receipt_waits_within_the_shared_deadline(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    proc_cgroup, root = _cgroup_fixture(tmp_path)
    pending = tmp_path / "pending"
    published = tmp_path / "published"
    receipt_root, unit_root, unit = _publish_systemd_authority(published)
    delayed_receipt_root = pending / "receipts"
    delayed_unit_root = pending / "units"
    delayed_receipt_root.mkdir(parents=True)
    delayed_unit_root.mkdir(parents=True)
    now = 100.0

    monkeypatch.setattr(cgroup_module.time, "monotonic", lambda: now)

    def publish(delay: float) -> None:
        nonlocal now
        now += delay
        (delayed_unit_root / unit).write_bytes((unit_root / unit).read_bytes())
        (delayed_unit_root / unit).chmod(0o644)
        (delayed_receipt_root / f"{unit}.json").write_bytes(
            (receipt_root / f"{unit}.json").read_bytes(),
        )
        (delayed_receipt_root / f"{unit}.json").chmod(0o444)

    monkeypatch.setattr(cgroup_module.time, "sleep", publish)

    assert (
        cgroup_module.discover_docker_cgroup_parent(
            **_SYSTEMD_BINDING,
            wait_seconds=1.0,
            proc_cgroup=proc_cgroup,
            cgroup_root=root,
            receipt_root=delayed_receipt_root,
            unit_root=delayed_unit_root,
            expected_authority_uid=os.getuid(),
            expected_authority_gid=os.getgid(),
        )
        == unit
    )


@pytest.mark.parametrize(
    "attack",
    ("extra_field", "unit_digest", "limit_drift", "identity_drift"),
)
def test_systemd_slice_authority_tampering_fails_closed(
    tmp_path: Path,
    attack: str,
) -> None:
    proc_cgroup, root = _cgroup_fixture(tmp_path)
    receipt_root, unit_root, unit = _publish_systemd_authority(
        tmp_path,
        extra_field=attack == "extra_field",
    )
    if attack == "unit_digest":
        (unit_root / unit).write_text("tampered\n", encoding="ascii")
    elif attack == "limit_drift":
        (root / "system.slice/slurmstepd.scope/job_123/memory.max").write_text(
            "8388607999\n", encoding="ascii"
        )
    elif attack == "identity_drift":
        receipt_path = receipt_root / f"{unit}.json"
        receipt = json.loads(receipt_path.read_text())
        receipt["resource_generation"] = 8
        unsigned = {key: value for key, value in receipt.items() if key != "payload_sha256"}
        receipt["payload_sha256"] = hashlib.sha256(
            json.dumps(unsigned, sort_keys=True, separators=(",", ":")).encode("ascii"),
        ).hexdigest()
        receipt_path.chmod(0o644)
        receipt_path.write_text(
            json.dumps(receipt, sort_keys=True, separators=(",", ":")) + "\n",
            encoding="ascii",
        )
        receipt_path.chmod(0o444)

    with pytest.raises(SlurmJobCgroupError):
        cgroup_module.discover_docker_cgroup_parent(
            **_SYSTEMD_BINDING,
            proc_cgroup=proc_cgroup,
            cgroup_root=root,
            receipt_root=receipt_root,
            unit_root=unit_root,
            expected_authority_uid=os.getuid(),
            expected_authority_gid=os.getgid(),
        )


def test_reused_job_id_with_new_start_time_has_a_distinct_slice(
    tmp_path: Path,
) -> None:
    _receipt_root, _unit_root, old_unit = _publish_systemd_authority(tmp_path)
    new_unit, _digest = cgroup_module._systemd_slice_identity(
        **{
            **{
                key: value
                for key, value in _SYSTEMD_BINDING.items()
                if key not in {"docker_driver", "pids_max"}
            },
            "job_start_time": "2026-07-30T01:00:00",
        },
    )

    assert new_unit != old_unit
