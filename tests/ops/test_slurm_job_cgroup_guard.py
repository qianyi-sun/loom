from __future__ import annotations

from pathlib import Path

import pytest
from scripts.ops import slurm_job_cgroup_guard as guard


def _config(tmp_path: Path, *, node: str = "TRT-EAI-OLDLAB-1") -> guard.GuardConfig:
    return guard.GuardConfig(
        node=node,
        cgroup_root=tmp_path / "cgroup",
        poll_interval_seconds=2.0,
        command_timeout_seconds=15.0,
        squeue_path="squeue",
        scontrol_path="scontrol",
        systemctl_path="systemctl",
    )


def test_comment_grammar_is_closed() -> None:
    assert guard._COMMENT_RE.fullmatch("loom-cgroup-v1:pids=3072") is not None
    for bad in (
        "loom-cgroup-v1:pids=0",
        "loom-cgroup-v1:pids=3072 ",
        "loom-cgroup-v2:pids=1",
        "pids=3072",
        "loom-cgroup-v1:pids=3072:r=deadbeef",
    ):
        assert guard._COMMENT_RE.fullmatch(bad) is None


@pytest.mark.parametrize(
    ("alloc", "expected"),
    [
        ("cpu=4,mem=16000M,node=1", 16000 * 1024**2),
        ("mem=8G", 8 * 1024**3),
        ("cpu=2,node=1", 0),
        ("mem=524288K", 524288 * 1024),
    ],
)
def test_parse_alloc_memory_bytes(alloc: str, expected: int) -> None:
    assert guard._parse_alloc_memory_bytes(alloc) == expected


def _route(responses: dict[str, str]):
    def fake_run(_config: guard.GuardConfig, args) -> str:
        key = args[0].rsplit("/", 1)[-1]
        if key == "squeue":
            return responses.get("squeue", "")
        if key == "scontrol":
            return responses.get(f"scontrol:{args[3]}", "")
        if key == "systemctl":
            sub = args[1]
            return responses.get(f"systemctl:{sub}", "")
        return ""

    return fake_run


def test_discover_job_intents_filters_to_reviewed_comment(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    responses = {
        "squeue": "123|loom-cgroup-v1:pids=3072\n456|some-other-job\n789|loom-cgroup-v1:pids=1024",
        "scontrol:123": "JobId=123 AllocTRES=cpu=4,mem=16000M,node=1 JobState=RUNNING",
        "scontrol:789": "JobId=789 AllocTRES=cpu=2,mem=8000M,node=1 JobState=RUNNING",
    }
    monkeypatch.setattr(guard, "_run", _route(responses))

    intents = guard.discover_job_intents(_config(tmp_path))

    assert set(intents) == {"123", "789"}
    assert intents["123"].pids_max == 3072
    assert intents["123"].memory_max_bytes == 16000 * 1024**2
    assert intents["789"].pids_max == 1024


def test_discover_job_intents_skips_jobs_without_memory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    responses = {
        "squeue": "123|loom-cgroup-v1:pids=3072",
        "scontrol:123": "JobId=123 AllocTRES=cpu=4,node=1 JobState=RUNNING",
    }
    monkeypatch.setattr(guard, "_run", _route(responses))

    assert guard.discover_job_intents(_config(tmp_path)) == {}


def _make_job_cgroup(root: Path, job_id: str, *, cpuset: str = "0-1") -> Path:
    job = root / "system.slice" / "slurmstepd.scope" / f"job_{job_id}"
    job.mkdir(parents=True)
    (job.parent / "cgroup.subtree_control").write_text("cpu memory", encoding="utf-8")
    (job / "cgroup.subtree_control").write_text("cpu memory", encoding="utf-8")
    (job / "pids.max").write_text("max", encoding="utf-8")
    (job / "cpuset.cpus.effective").write_text(cpuset, encoding="utf-8")
    return job


def test_find_job_cgroup_locates_scoped_job(tmp_path: Path) -> None:
    root = tmp_path / "cgroup"
    job = _make_job_cgroup(root, "123")

    assert guard.find_job_cgroup(root, "123") == job
    assert guard.find_job_cgroup(root, "999") is None


def test_delegate_pids_enables_controllers_and_caps(tmp_path: Path) -> None:
    root = tmp_path / "cgroup"
    job = _make_job_cgroup(root, "123")

    guard.delegate_pids(job, 3072)

    assert (job.parent / "cgroup.subtree_control").read_text(encoding="utf-8") == "+pids"
    assert (job / "cgroup.subtree_control").read_text(encoding="utf-8") == "+pids"
    assert (job / "pids.max").read_text(encoding="utf-8") == "3072"


def test_ensure_slice_sizes_from_allocation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "cgroup"
    job = _make_job_cgroup(root, "123", cpuset="2-5")
    calls: list[tuple[str, ...]] = []

    def record(_config: guard.GuardConfig, args) -> str:
        calls.append(tuple(args))
        return ""

    monkeypatch.setattr(guard, "_run", record)
    intent = guard.JobIntent(
        job_id="123",
        pids_max=3072,
        cpu_max_percent=0,
        memory_max_bytes=16000 * 1024**2,
    )

    guard.ensure_slice(_config(tmp_path), intent, job)

    set_property = next(c for c in calls if c[1] == "set-property")
    assert "loom-job-123.slice" in set_property
    assert "AllowedCPUs=2-5" in set_property
    assert "TasksMax=3072" in set_property
    assert f"MemoryMax={16000 * 1024**2}" in set_property
    assert any(c[1] == "start" and "loom-job-123.slice" in c for c in calls)


def test_run_once_reconciles_and_tears_down_stale(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = tmp_path / "cgroup"
    _make_job_cgroup(root, "123")
    stops: list[str] = []

    def route(_config: guard.GuardConfig, args):
        key = args[0].rsplit("/", 1)[-1]
        if key == "squeue":
            return "123|loom-cgroup-v1:pids=3072"
        if key == "scontrol":
            return "JobId=123 AllocTRES=cpu=4,mem=16000M,node=1 JobState=RUNNING"
        if key == "systemctl":
            if args[1] == "list-units":
                return "loom-job-123.slice loaded active active\nloom-job-999.slice loaded active active"
            if args[1] == "stop":
                stops.append(args[2])
        return ""

    monkeypatch.setattr(guard, "_run", route)

    applied = guard.run_once(_config(tmp_path))

    assert applied == 1
    assert stops == ["loom-job-999.slice"]  # the job that left the queue


@pytest.mark.parametrize("observation", ["lookup-error", "missing-memory", "protected", "suspended", "completing"])
def test_unadmitted_present_job_is_not_positive_cleanup_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, observation: str,
) -> None:
    """An admission failure must not tear down a still-existing allocation."""
    _make_job_cgroup(tmp_path / "cgroup", "123")
    calls: list[tuple[str, ...]] = []

    def route(_config: guard.GuardConfig, args) -> str:
        calls.append(tuple(args))
        if args[0] == "squeue":
            comment = "a" * 43 if observation == "protected" else "loom-cgroup-v1:pids=3072"
            return f"123|{comment}"
        if args[0] == "scontrol":
            if observation == "lookup-error":
                raise guard.GuardError("temporary controller readback failure")
            memory = "" if observation == "missing-memory" else ",mem=16000M"
            state = {"suspended": "SUSPENDED", "completing": "COMPLETING"}.get(observation, "RUNNING")
            return f"JobId=123 AllocTRES=cpu=4{memory},node=1 JobState={state}"
        if args[1] == "list-units":
            return "loom-job-123.slice loaded active active\nloom-job-999.slice loaded active active"
        return ""

    monkeypatch.setattr(guard, "_run", route)

    assert guard.run_once(_config(tmp_path)) == 0
    assert [args[2] for args in calls if args[:2] == ("systemctl", "stop")] == ["loom-job-999.slice"]
    assert not any(args[:2] == ("systemctl", "set-property") for args in calls)
    # Cleanup's presence inventory includes more than only RUNNING jobs.
    assert any(args[0] == "squeue" and "--states=all" in args for args in calls)


def test_queue_failure_cannot_teardown_any_slice(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, ...]] = []

    def route(_config: guard.GuardConfig, args) -> str:
        calls.append(tuple(args))
        if args[0] == "squeue":
            raise guard.GuardError("queue unavailable")
        raise AssertionError("failed discovery must not proceed to mutation")

    monkeypatch.setattr(guard, "_run", route)
    with pytest.raises(guard.GuardError, match="queue unavailable"):
        guard.run_once(_config(tmp_path))
    assert len(calls) == 1


@pytest.mark.parametrize("state", ["COMPLETED", "CANCELLED", "FAILED", "TIMEOUT", "NODE_FAIL", "OUT_OF_MEMORY", "PREEMPTED", "BOOT_FAIL", "DEADLINE", "REVOKED"])
def test_terminal_legacy_job_retained_in_queue_does_not_preserve_orphan_slice(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, state: str,
) -> None:
    """Slurm MinJobAge retains terminal rows after their allocation is released."""
    calls: list[tuple[str, ...]] = []

    def route(_config: guard.GuardConfig, args) -> str:
        calls.append(tuple(args))
        if args[0] == "squeue":
            return "123|loom-cgroup-v1:pids=3072"
        if args[0] == "scontrol":
            return f"JobId=123 AllocTRES=cpu=4,mem=16000M,node=1 JobState={state}"
        if args[1] == "list-units":
            return "loom-job-123.slice loaded active active"
        return ""

    monkeypatch.setattr(guard, "_run", route)
    assert guard.run_once(_config(tmp_path)) == 0
    assert ("systemctl", "stop", "loom-job-123.slice") in calls
    assert not any(args[:2] == ("systemctl", "set-property") for args in calls)
