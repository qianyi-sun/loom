from pathlib import Path
from uuid import UUID, uuid4

from loom_worker.orphan_cleanup import cleanup_orphan_trajectories


def test_deletes_terminal_and_unowned_trials(tmp_path: Path) -> None:
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
            return "running", other_worker
        return "failed", my_worker

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
