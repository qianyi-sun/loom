from pathlib import Path
from uuid import UUID, uuid4

from loom_worker.orphan_cleanup import cleanup_orphan_trajectories


def test_deletes_terminal_trials_regardless_of_owner(tmp_path: Path) -> None:
    """Terminal trials (succeeded/failed/cancelled) get cleaned up;
    owner identity doesn't matter for the terminal branch."""
    trial_a = uuid4()
    trial_b = uuid4()
    trial_c = uuid4()
    (tmp_path / f"{trial_a}.jsonl").write_text("a")
    (tmp_path / f"{trial_b}.jsonl").write_text("b")
    (tmp_path / f"{trial_c}.jsonl").write_text("c")

    my_worker = uuid4()
    other_worker = uuid4()

    def lookup(tid: UUID) -> tuple[str, UUID | None]:
        if tid == trial_a:
            return "succeeded", my_worker
        if tid == trial_b:
            return "failed", other_worker
        return "cancelled", None

    deleted = cleanup_orphan_trajectories(
        cache_dir=tmp_path,
        owned_worker_id=my_worker,
        state_and_owner_lookup=lookup,
    )
    assert set(deleted) == {trial_a, trial_b, trial_c}
    assert not any(tmp_path.iterdir())


def test_preserves_running_trials_we_own(tmp_path: Path) -> None:
    trial_a = uuid4()
    my_worker = uuid4()
    (tmp_path / f"{trial_a}.jsonl").write_text("x")

    deleted = cleanup_orphan_trajectories(
        cache_dir=tmp_path,
        owned_worker_id=my_worker,
        state_and_owner_lookup=lambda _tid: ("running", my_worker),
    )
    assert deleted == []
    assert (tmp_path / f"{trial_a}.jsonl").is_file()


def test_preserves_running_trial_after_reclaim_orphaned_us(
    tmp_path: Path,
) -> None:
    """#416 root cause: the worker died mid-trial, the CP reclaim
    sweep nulled the owner and re-queued the trial, then this worker
    boots and runs orphan cleanup. The JSONL is the only forensic
    record of the prior attempt — keep it for the reclaim path /
    operator triage. Predicate must not branch on `owner != ours`."""
    trial_a = uuid4()
    (tmp_path / f"{trial_a}.jsonl").write_text("partial trajectory")

    deleted = cleanup_orphan_trajectories(
        cache_dir=tmp_path,
        owned_worker_id=uuid4(),
        # Post-reclaim shape: state=running (reclaim flipped to queued
        # then a new worker picked it up) with owner=None at the moment
        # the sweep observes the file. Either way: non-terminal, keep.
        state_and_owner_lookup=lambda _tid: ("running", None),
    )
    assert deleted == []
    assert (tmp_path / f"{trial_a}.jsonl").is_file()


def test_preserves_running_trial_owned_by_other_worker(
    tmp_path: Path,
) -> None:
    """Same predicate applies when CP reports a different owner — the
    file might be a leftover from a previous incarnation of this
    worker, but the active trial belongs to someone else. Keeping the
    file is harmless and matches the conservative #416 contract."""
    trial_a = uuid4()
    other = uuid4()
    (tmp_path / f"{trial_a}.jsonl").write_text("x")

    deleted = cleanup_orphan_trajectories(
        cache_dir=tmp_path,
        owned_worker_id=uuid4(),
        state_and_owner_lookup=lambda _tid: ("running", other),
    )
    assert deleted == []
    assert (tmp_path / f"{trial_a}.jsonl").is_file()


def test_preserves_claimed_and_queued_trials(tmp_path: Path) -> None:
    """Non-terminal covers `claimed` and `queued` too, not just
    `running`. The reclaim sweep can leave a trial in `queued` and
    the existing local JSONL is from the previous worker incarnation."""
    a, b = uuid4(), uuid4()
    (tmp_path / f"{a}.jsonl").write_text("a")
    (tmp_path / f"{b}.jsonl").write_text("b")
    states = {a: "claimed", b: "queued"}

    deleted = cleanup_orphan_trajectories(
        cache_dir=tmp_path,
        owned_worker_id=uuid4(),
        state_and_owner_lookup=lambda tid: (states[tid], None),
    )
    assert deleted == []
    assert (tmp_path / f"{a}.jsonl").is_file()
    assert (tmp_path / f"{b}.jsonl").is_file()


def test_deletes_non_terminal_when_older_than_fallback_window(
    tmp_path: Path,
) -> None:
    """Safety valve: if a JSONL has been tagged non-terminal for
    longer than `non_terminal_fallback_sec` (default 24h), the reclaim
    sweep is broken or the trial is wedged at the CP — delete the
    file to bound disk growth and log loudly. Drives the
    `orphan_trajectory_deleted_stale` log path."""
    trial_a = uuid4()
    f = tmp_path / f"{trial_a}.jsonl"
    f.write_text("ancient")
    # Backdate the file by 48h.
    import os
    mtime = 1_000_000.0
    os.utime(f, (mtime, mtime))

    deleted = cleanup_orphan_trajectories(
        cache_dir=tmp_path,
        owned_worker_id=uuid4(),
        state_and_owner_lookup=lambda _tid: ("running", None),
        now_sec=mtime + 48 * 60 * 60,
        non_terminal_fallback_sec=24 * 60 * 60,
    )
    assert deleted == [trial_a]
    assert not f.exists()


def test_deletes_unknown_trials(tmp_path: Path) -> None:
    trial_a = uuid4()
    (tmp_path / f"{trial_a}.jsonl").write_text("x")

    def lookup(_tid: UUID) -> tuple[str, UUID | None]:
        raise LookupError(str(_tid))

    deleted = cleanup_orphan_trajectories(
        cache_dir=tmp_path,
        owned_worker_id=uuid4(),
        state_and_owner_lookup=lookup,
    )
    assert deleted == [trial_a]
    assert not (tmp_path / f"{trial_a}.jsonl").exists()


def test_skips_non_jsonl_files(tmp_path: Path) -> None:
    (tmp_path / "not_uuid.jsonl").write_text("x")
    (tmp_path / "README.md").write_text("x")
    sub = tmp_path / "subdir"
    sub.mkdir()

    called: list[UUID] = []
    def lookup(tid: UUID) -> tuple[str, UUID | None]:
        called.append(tid)
        return "succeeded", None

    deleted = cleanup_orphan_trajectories(
        cache_dir=tmp_path,
        owned_worker_id=uuid4(),
        state_and_owner_lookup=lookup,
    )
    assert deleted == []
    assert called == []
    # untouched
    assert (tmp_path / "not_uuid.jsonl").exists()
    assert (tmp_path / "README.md").exists()


def test_returns_empty_when_dir_missing(tmp_path: Path) -> None:
    missing = tmp_path / "does-not-exist"
    deleted = cleanup_orphan_trajectories(
        cache_dir=missing,
        owned_worker_id=uuid4(),
        state_and_owner_lookup=lambda _t: ("succeeded", None),
    )
    assert deleted == []


def test_deletes_every_attempt_file_for_terminal_trial(tmp_path: Path) -> None:
    trial_id = uuid4()
    first = tmp_path / f"{trial_id}.attempt-1.events.jsonl"
    second = tmp_path / f"{trial_id}.attempt-2.events.jsonl"
    first.write_text("first")
    second.write_text("second")

    cleanup_orphan_trajectories(
        cache_dir=tmp_path,
        owned_worker_id=uuid4(),
        state_and_owner_lookup=lambda _tid: ("succeeded", None),
    )

    assert not first.exists()
    assert not second.exists()


def test_preserves_fresh_non_terminal_attempt_file_and_checks_owner(
    tmp_path: Path,
) -> None:
    trial_id = uuid4()
    attempt = tmp_path / f"{trial_id}.attempt-2.events.jsonl"
    attempt.write_text("current")
    looked_up: list[UUID] = []

    cleanup_orphan_trajectories(
        cache_dir=tmp_path,
        owned_worker_id=uuid4(),
        state_and_owner_lookup=lambda tid: (
            looked_up.append(tid) or "running",
            None,
        ),
    )

    assert looked_up == [trial_id]
    assert attempt.exists()


def test_deletes_stale_non_terminal_attempt_file(tmp_path: Path) -> None:
    import os

    trial_id = uuid4()
    attempt = tmp_path / f"{trial_id}.attempt-4.events.jsonl"
    attempt.write_text("stale")
    mtime = 1_000_000.0
    os.utime(attempt, (mtime, mtime))

    cleanup_orphan_trajectories(
        cache_dir=tmp_path,
        owned_worker_id=uuid4(),
        state_and_owner_lookup=lambda _tid: ("queued", None),
        now_sec=mtime + 100,
        non_terminal_fallback_sec=10,
    )

    assert not attempt.exists()
