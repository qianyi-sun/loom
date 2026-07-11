"""Terminal-Bench 2.1 rev-6 source-lock contract."""

from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from pathlib import Path

import pytest
from loom_benchmark_terminal_bench_2.upstream import (
    TB21Lock,
    TB21LockError,
    TB21TaskLock,
    load_tb21_lock,
    verify_tb21_materialization,
)


def test_lock_is_exact_rev6_89_task_hub_authority() -> None:
    lock = load_tb21_lock()

    assert (lock.dataset, lock.revision) == ("terminal-bench/terminal-bench-2-1", "6")
    assert lock.hub_metadata_version == (
        "sha256:7d7bdc1cbedad549fc1140404bd4dc45e5fd0ea7c4186773687d177ad3a0699a"
    )
    assert lock.source_revision == "dde3cd95b80ff25af5abd99a80b6513a018ad3b4"
    assert lock.manifest_sha256 == "d90b4389992d07ed6f4ab8de963a70241eaa4b60072eeaec4c3b261b6c4a6dd8"
    assert len(lock.tasks) == 89
    assert lock.digest_for("terminal-bench/sanitize-git-repo") == (
        "sha256:6e86297715fae62cd499fbdd27013e11a38d05d7e05b7f661cb50b4ecead128f"
    )
    assert lock.source_manifest_divergences == [{
        "task": "terminal-bench/sanitize-git-repo",
        "source_digest": "sha256:73c94a21ebe370bae843adbeeaaa9e991374867b18483aaf56c7cd470dcddea7",
        "hub_digest": "sha256:6e86297715fae62cd499fbdd27013e11a38d05d7e05b7f661cb50b4ecead128f",
    }]
    assert [task.name for task in lock.tasks] == sorted(task.name for task in lock.tasks)
    assert all(task.digest.startswith("sha256:") for task in lock.tasks)


def test_lock_rejects_hub_revision_metadata_version_and_digest_drift(
    tmp_path: Path,
) -> None:
    lock = _test_lock()

    matching = _write_materialization(tmp_path / "matching", lock)
    verify_tb21_materialization(matching, lock=lock)

    wrong_revision = _write_materialization(tmp_path / "wrong-revision", lock, revision="7")
    with pytest.raises(TB21LockError, match="revision"):
        verify_tb21_materialization(wrong_revision, lock=lock)

    wrong_metadata_version = _write_materialization(
        tmp_path / "wrong-metadata-version",
        lock,
        metadata_version="sha256:" + "f" * 64,
    )
    with pytest.raises(TB21LockError, match="metadata version"):
        verify_tb21_materialization(wrong_metadata_version, lock=lock)

    wrong_digest = _write_materialization(
        tmp_path / "wrong-digest",
        lock,
        package_digests={"terminal-bench/a": "sha256:" + "f" * 64, "terminal-bench/b": "sha256:" + "b" * 64},
    )
    with pytest.raises(TB21LockError, match="digest"):
        verify_tb21_materialization(wrong_digest, lock=lock)


def test_lock_rejects_88_and_90_hub_packages(tmp_path: Path) -> None:
    lock = load_tb21_lock()
    package_digests = lock.package_digests

    truncated = _write_materialization(
        tmp_path / "88-packages",
        lock,
        package_digests=dict(list(package_digests.items())[:-1]),
    )
    with pytest.raises(TB21LockError, match="task-count"):
        verify_tb21_materialization(truncated, lock=lock)

    expanded_digests = dict(package_digests)
    expanded_digests["terminal-bench/unlocked"] = "sha256:" + "f" * 64
    expanded = _write_materialization(
        tmp_path / "90-packages",
        lock,
        package_digests=expanded_digests,
    )
    with pytest.raises(TB21LockError, match="task-count"):
        verify_tb21_materialization(expanded, lock=lock)


def test_lock_rejects_source_name_and_unrecorded_or_changed_divergence(
    tmp_path: Path,
) -> None:
    lock = _test_lock()

    source_name_drift = _write_materialization(
        tmp_path / "source-name-drift",
        lock,
        manifest_tasks=[
            ("terminal-bench/a", "sha256:" + "a" * 64),
            ("terminal-bench/unlocked", "sha256:" + "b" * 64),
        ],
    )
    source_name_drift_lock = replace(
        lock,
        manifest_sha256=_manifest_sha256(source_name_drift, lock),
    )
    with pytest.raises(TB21LockError, match="name"):
        verify_tb21_materialization(source_name_drift, lock=source_name_drift_lock)

    unrecorded_divergence = _write_materialization(
        tmp_path / "unrecorded-divergence",
        lock,
        manifest_tasks=[
            ("terminal-bench/a", "sha256:" + "f" * 64),
            ("terminal-bench/b", "sha256:" + "c" * 64),
        ],
    )
    unrecorded_divergence_lock = replace(
        lock,
        manifest_sha256=_manifest_sha256(unrecorded_divergence, lock),
    )
    with pytest.raises(TB21LockError, match="unrecorded"):
        verify_tb21_materialization(unrecorded_divergence, lock=unrecorded_divergence_lock)

    changed_divergence = _write_materialization(
        tmp_path / "changed-divergence",
        lock,
        manifest_tasks=[
            ("terminal-bench/a", "sha256:" + "a" * 64),
            ("terminal-bench/b", "sha256:" + "f" * 64),
        ],
    )
    changed_divergence_lock = replace(
        lock,
        manifest_sha256=_manifest_sha256(changed_divergence, lock),
    )
    with pytest.raises(TB21LockError, match="divergence"):
        verify_tb21_materialization(changed_divergence, lock=changed_divergence_lock)


def test_lock_rejects_duplicate_source_tasks_and_manifest_hash_drift(tmp_path: Path) -> None:
    lock = _test_lock()
    duplicate_tasks = [
        ("terminal-bench/a", "sha256:" + "a" * 64),
        ("terminal-bench/a", "sha256:" + "a" * 64),
    ]
    duplicate = _write_materialization(tmp_path / "duplicate", lock, manifest_tasks=duplicate_tasks)
    duplicate_lock = replace(lock, manifest_sha256=_manifest_sha256(duplicate, lock))
    with pytest.raises(TB21LockError, match="duplicate"):
        verify_tb21_materialization(duplicate, lock=duplicate_lock)

    drifted_manifest = _write_materialization(tmp_path / "manifest-drift", lock)
    manifest = drifted_manifest / "audit" / lock.manifest_source
    manifest.write_text(manifest.read_text() + "# source drift\n")
    with pytest.raises(TB21LockError, match="manifest SHA-256"):
        verify_tb21_materialization(drifted_manifest, lock=lock)


def _test_lock() -> TB21Lock:
    tasks = (
        TB21TaskLock("terminal-bench/a", "sha256:" + "a" * 64),
        TB21TaskLock("terminal-bench/b", "sha256:" + "b" * 64),
    )
    source_tasks = [
        ("terminal-bench/a", "sha256:" + "a" * 64),
        ("terminal-bench/b", "sha256:" + "c" * 64),
    ]
    manifest = _render_manifest("terminal-bench/terminal-bench-2-1", source_tasks)
    return TB21Lock(
        dataset="terminal-bench/terminal-bench-2-1",
        revision="6",
        hub_metadata_version="sha256:" + "d" * 64,
        source_revision="dde3cd95b80ff25af5abd99a80b6513a018ad3b4",
        manifest_source="tasks/dataset.toml",
        manifest_sha256=hashlib.sha256(manifest.encode()).hexdigest(),
        tasks=tasks,
        source_manifest_divergences=[{
            "task": "terminal-bench/b",
            "source_digest": "sha256:" + "c" * 64,
            "hub_digest": "sha256:" + "b" * 64,
        }],
    )


def _write_materialization(
    root: Path,
    lock: TB21Lock,
    *,
    revision: str | None = None,
    metadata_version: str | None = None,
    package_digests: dict[str, str] | None = None,
    manifest_tasks: list[tuple[str, str]] | None = None,
) -> Path:
    root.mkdir(parents=True)
    (root / "harbor-materialization.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "dataset": lock.dataset,
                "revision": revision or lock.revision,
                "metadata_version": metadata_version or lock.hub_metadata_version,
                "package_digests": package_digests
                or {task.name: task.digest for task in lock.tasks},
            },
        ),
    )
    manifest_path = root / "audit" / lock.manifest_source
    manifest_path.parent.mkdir(parents=True)
    tasks = manifest_tasks or [
        ("terminal-bench/a", "sha256:" + "a" * 64),
        ("terminal-bench/b", "sha256:" + "c" * 64),
    ]
    manifest_path.write_text(_render_manifest(lock.dataset, tasks))
    return root


def _manifest_sha256(root: Path, lock: TB21Lock) -> str:
    return hashlib.sha256((root / "audit" / lock.manifest_source).read_bytes()).hexdigest()


def _render_manifest(
    dataset: str, tasks: tuple[TB21TaskLock, ...] | list[tuple[str, str]],
) -> str:
    rendered = ["[dataset]", f'name = "{dataset}"', ""]
    for task in tasks:
        name, digest = (task.name, task.digest) if isinstance(task, TB21TaskLock) else task
        rendered.extend(["[[tasks]]", f'name = "{name}"', f'digest = "{digest}"', ""])
    return "\n".join(rendered)
