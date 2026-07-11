"""Pinned upstream constants for terminal-bench-core v0.1.1.

The SHA was probed against
https://github.com/laude-institute/terminal-bench/blob/main/registry.json
on 2026-06-08.

Upgrading to a newer TB-2 dataset version requires updating this
constant; the pin-guard test in `tests/test_upstream_pin.py` enforces
lockstep with the SHA below.
"""

from __future__ import annotations

import hashlib
import json
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from loom_benchmarks.base import UpstreamSource
from loom_benchmarks.harbor_dataset import MATERIALIZATION_METADATA_FILENAME

UPSTREAM_REVISION = "91e10457b5410f16c44364da1a34cb6de8c488a5"
"""terminal-bench-core v0.1.1 commit on the
`dataset/terminal-bench-core/v0.1.x` branch."""

UPSTREAM_SOURCE = UpstreamSource(
    kind="git",
    locator="https://github.com/laude-institute/terminal-bench.git",
    revision=UPSTREAM_REVISION,
)
"""Passed to `loom_benchmarks.fetch.fetch_upstream`. The SHA pin flows
through `_looks_like_sha` and uses the `git init && git fetch <sha>` path
instead of `--branch` (raw SHAs are not valid branch refs)."""

DATASET_VERSION = "0.1.1"
"""Surfaced into TB-2 report JSON as a top-level field if Harbor's
reference shape adds one in a future schema bump."""

TASK_SUBDIR = "tasks"
"""Path relative to the repo root that holds per-task directories."""


TB21_DATASET = "terminal-bench/terminal-bench-2-1"
TB21_REVISION = "6"
TB21_HUB_METADATA_VERSION = (
    "sha256:7d7bdc1cbedad549fc1140404bd4dc45e5fd0ea7c4186773687d177ad3a0699a"
)
TB21_SOURCE_REVISION = "dde3cd95b80ff25af5abd99a80b6513a018ad3b4"
TB21_MANIFEST_SOURCE = "tasks/dataset.toml"
TB21_MANIFEST_SHA256 = "d90b4389992d07ed6f4ab8de963a70241eaa4b60072eeaec4c3b261b6c4a6dd8"
TB21_TASK_COUNT = 89
TB21_SOURCE_MANIFEST_DIVERGENCES = [{
    "task": "terminal-bench/sanitize-git-repo",
    "source_digest": "sha256:73c94a21ebe370bae843adbeeaaa9e991374867b18483aaf56c7cd470dcddea7",
    "hub_digest": "sha256:6e86297715fae62cd499fbdd27013e11a38d05d7e05b7f661cb50b4ecead128f",
}]
"""The only reviewed source-reference digest divergence from Hub rev 6."""

TB21_HARBOR_SOURCE = UpstreamSource(
    kind="harbor-package",
    locator=TB21_DATASET,
    revision=TB21_REVISION,
)
"""Canonical TB2.1 Harbor Hub package dataset, pinned at revision 6."""

TB21_AUDIT_SOURCE = UpstreamSource(
    kind="git",
    locator="https://github.com/harbor-framework/terminal-bench-2-1.git",
    revision=TB21_SOURCE_REVISION,
)
"""Pinned source snapshot containing the audited ``tasks/dataset.toml``."""


class TB21LockError(ValueError):
    """The TB2.1 source lock or a materialization does not match."""


@dataclass(frozen=True)
class TB21TaskLock:
    name: str
    digest: str


@dataclass(frozen=True)
class TB21Lock:
    dataset: str
    revision: str
    hub_metadata_version: str
    source_revision: str
    manifest_source: str
    manifest_sha256: str
    tasks: tuple[TB21TaskLock, ...]
    source_manifest_divergences: list[dict[str, str]]

    @property
    def package_digests(self) -> dict[str, str]:
        return {task.name: task.digest for task in self.tasks}

    def digest_for(self, name: str) -> str:
        """Return the immutable Hub package digest for one canonical task."""
        try:
            return self.package_digests[name]
        except KeyError as exc:
            raise TB21LockError(f"TB2.1 lock has no task {name!r}") from exc


def load_tb21_lock() -> TB21Lock:
    """Load the reviewed, immutable TB2.1 rev-6 package/source lock."""
    lock_path = Path(__file__).with_name("tb21_lock.json")
    try:
        raw = json.loads(lock_path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise TB21LockError(f"unable to read TB2.1 lock {lock_path}") from exc
    lock = _parse_tb21_lock(raw)
    _verify_canonical_lock(lock)
    return lock


def verify_tb21_materialization(root: Path, *, lock: TB21Lock | None = None) -> None:
    """Fail closed unless Harbor metadata and the audit manifest match ``lock``.

    Callers stage the independently fetched source audit checkout beneath
    ``root / 'audit'``. No task conversion may begin until the Harbor package
    metadata and ``audit/tasks/dataset.toml`` both equal the checked-in lock.
    """
    active_lock = lock or load_tb21_lock()
    metadata = _load_materialization_metadata(root)
    if metadata.get("dataset") != active_lock.dataset:
        raise TB21LockError(
            "Harbor materialization dataset mismatch: "
            f"expected {active_lock.dataset!r}, got {metadata.get('dataset')!r}",
        )
    if metadata.get("revision") != active_lock.revision:
        raise TB21LockError(
            "Harbor materialization revision mismatch: "
            f"expected {active_lock.revision!r}, got {metadata.get('revision')!r}",
        )
    if metadata.get("metadata_version") != active_lock.hub_metadata_version:
        raise TB21LockError(
            "Harbor materialization metadata version mismatch: "
            f"expected {active_lock.hub_metadata_version!r}, "
            f"got {metadata.get('metadata_version')!r}",
        )
    _verify_task_pairs(
        _mapping_of_digests(metadata.get("package_digests"), source="Harbor materialization"),
        active_lock.package_digests,
        source="Harbor materialization",
    )

    manifest_path = root / "audit" / active_lock.manifest_source
    try:
        manifest_bytes = manifest_path.read_bytes()
    except OSError as exc:
        raise TB21LockError(
            f"TB2.1 audit manifest is unavailable: {manifest_path}",
        ) from exc
    manifest_sha256 = hashlib.sha256(manifest_bytes).hexdigest()
    if manifest_sha256 != active_lock.manifest_sha256:
        raise TB21LockError(
            "TB2.1 audit manifest SHA-256 drift: "
            f"expected {active_lock.manifest_sha256}, got {manifest_sha256}",
        )
    try:
        manifest = tomllib.loads(manifest_bytes.decode("utf-8"))
    except (UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
        raise TB21LockError("TB2.1 audit manifest is not valid TOML") from exc
    dataset = manifest.get("dataset")
    if not isinstance(dataset, dict) or dataset.get("name") != active_lock.dataset:
        raise TB21LockError("TB2.1 audit manifest dataset source drift")
    _verify_source_manifest(
        _manifest_task_digests(manifest.get("tasks")),
        lock=active_lock,
    )


def _parse_tb21_lock(raw: Any) -> TB21Lock:
    if not isinstance(raw, dict):
        raise TB21LockError("TB2.1 lock must be a JSON object")
    if raw.get("schema_version") != 1:
        raise TB21LockError("TB2.1 lock schema_version must be 1")
    values: dict[str, str] = {}
    for key in (
        "dataset",
        "revision",
        "hub_metadata_version",
        "source_revision",
        "manifest_source",
        "manifest_sha256",
    ):
        value = raw.get(key)
        if not isinstance(value, str) or not value:
            raise TB21LockError(f"TB2.1 lock {key!r} must be a non-empty string")
        values[key] = value
    raw_tasks = raw.get("tasks")
    if not isinstance(raw_tasks, list):
        raise TB21LockError("TB2.1 lock tasks must be a list")

    tasks: list[TB21TaskLock] = []
    names: set[str] = set()
    for item in raw_tasks:
        if not isinstance(item, dict):
            raise TB21LockError("TB2.1 lock task entries must be objects")
        name, digest = item.get("name"), item.get("digest")
        if not isinstance(name, str) or not isinstance(digest, str):
            raise TB21LockError("TB2.1 lock task entries require string name and digest")
        if name in names:
            raise TB21LockError(f"TB2.1 lock contains duplicate task {name!r}")
        if not _is_sha256_digest(digest):
            raise TB21LockError(f"TB2.1 lock task {name!r} has a non-immutable digest")
        names.add(name)
        tasks.append(TB21TaskLock(name=name, digest=digest))
    if [task.name for task in tasks] != sorted(task.name for task in tasks):
        raise TB21LockError("TB2.1 lock tasks must be sorted by name")
    divergences = _parse_source_manifest_divergences(raw.get("source_manifest_divergences"))
    return TB21Lock(
        tasks=tuple(tasks),
        source_manifest_divergences=divergences,
        **values,
    )


def _verify_canonical_lock(lock: TB21Lock) -> None:
    if (lock.dataset, lock.revision) != (TB21_DATASET, TB21_REVISION):
        raise TB21LockError("TB2.1 lock dataset or Harbor revision drift")
    if lock.hub_metadata_version != TB21_HUB_METADATA_VERSION:
        raise TB21LockError("TB2.1 lock Harbor metadata version drift")
    if lock.source_revision != TB21_SOURCE_REVISION:
        raise TB21LockError("TB2.1 lock source revision drift")
    if lock.manifest_source != TB21_MANIFEST_SOURCE:
        raise TB21LockError("TB2.1 lock manifest source drift")
    if lock.manifest_sha256 != TB21_MANIFEST_SHA256:
        raise TB21LockError("TB2.1 lock manifest SHA-256 drift")
    if len(lock.tasks) != TB21_TASK_COUNT:
        raise TB21LockError(
            f"TB2.1 lock task-count mismatch: expected {TB21_TASK_COUNT}, got {len(lock.tasks)}",
        )
    if lock.source_manifest_divergences != TB21_SOURCE_MANIFEST_DIVERGENCES:
        raise TB21LockError("TB2.1 lock source-manifest divergence record drift")
    if lock.digest_for("terminal-bench/sanitize-git-repo") != (
        TB21_SOURCE_MANIFEST_DIVERGENCES[0]["hub_digest"]
    ):
        raise TB21LockError("TB2.1 lock reviewed Hub divergence digest drift")


def _load_materialization_metadata(root: Path) -> dict[str, Any]:
    path = root / MATERIALIZATION_METADATA_FILENAME
    try:
        raw = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise TB21LockError(f"Harbor materialization metadata is unavailable: {path}") from exc
    if not isinstance(raw, dict) or raw.get("schema_version") != 1:
        raise TB21LockError("Harbor materialization metadata has an unsupported schema")
    return raw


def _mapping_of_digests(value: Any, *, source: str) -> dict[str, str]:
    if not isinstance(value, dict):
        raise TB21LockError(f"{source} package_digests must be an object")
    result: dict[str, str] = {}
    for name, digest in value.items():
        if not isinstance(name, str) or not isinstance(digest, str) or not _is_sha256_digest(digest):
            raise TB21LockError(f"{source} contains a non-immutable task digest")
        result[name] = digest
    return result


def _manifest_task_digests(value: Any) -> dict[str, str]:
    if not isinstance(value, list):
        raise TB21LockError("TB2.1 audit manifest tasks must be a list")
    result: dict[str, str] = {}
    for item in value:
        if not isinstance(item, dict):
            raise TB21LockError("TB2.1 audit manifest task entries must be objects")
        name, digest = item.get("name"), item.get("digest")
        if not isinstance(name, str) or not isinstance(digest, str) or not _is_sha256_digest(digest):
            raise TB21LockError("TB2.1 audit manifest contains a non-immutable task digest")
        if name in result:
            raise TB21LockError(f"TB2.1 audit manifest contains duplicate task {name!r}")
        result[name] = digest
    return result


def _parse_source_manifest_divergences(value: Any) -> list[dict[str, str]]:
    if not isinstance(value, list):
        raise TB21LockError("TB2.1 lock source_manifest_divergences must be a list")
    required = {"task", "source_digest", "hub_digest"}
    divergences: list[dict[str, str]] = []
    tasks: set[str] = set()
    for item in value:
        if not isinstance(item, dict) or set(item) != required:
            raise TB21LockError(
                "TB2.1 lock source-manifest divergence entries must be exact records",
            )
        task = item["task"]
        source_digest = item["source_digest"]
        hub_digest = item["hub_digest"]
        if (
            not isinstance(task, str)
            or not _is_sha256_digest(source_digest)
            or not _is_sha256_digest(hub_digest)
        ):
            raise TB21LockError(
                "TB2.1 lock source-manifest divergence has an invalid identity",
            )
        if task in tasks:
            raise TB21LockError(
                f"TB2.1 lock contains duplicate source-manifest divergence {task!r}",
            )
        tasks.add(task)
        divergences.append({
            "task": task,
            "source_digest": source_digest,
            "hub_digest": hub_digest,
        })
    if [entry["task"] for entry in divergences] != sorted(tasks):
        raise TB21LockError("TB2.1 lock source-manifest divergences must be sorted")
    return divergences


def _verify_source_manifest(source_digests: dict[str, str], *, lock: TB21Lock) -> None:
    hub_digests = lock.package_digests
    source_names = set(source_digests)
    hub_names = set(hub_digests)
    if source_names != hub_names:
        raise TB21LockError(
            "TB2.1 audit manifest name-set drift: "
            f"missing={sorted(hub_names - source_names)}; "
            f"extra={sorted(source_names - hub_names)}",
        )
    actual_divergences = [{
        "task": task,
        "source_digest": source_digests[task],
        "hub_digest": hub_digests[task],
    } for task in sorted(source_names) if source_digests[task] != hub_digests[task]]
    expected_divergences = lock.source_manifest_divergences
    if actual_divergences != expected_divergences:
        expected_by_task = {entry["task"]: entry for entry in expected_divergences}
        actual_by_task = {entry["task"]: entry for entry in actual_divergences}
        unrecorded = sorted(set(actual_by_task) - set(expected_by_task))
        missing = sorted(set(expected_by_task) - set(actual_by_task))
        changed = sorted(
            task
            for task in set(actual_by_task) & set(expected_by_task)
            if actual_by_task[task] != expected_by_task[task]
        )
        raise TB21LockError(
            "TB2.1 audit manifest divergence drift: "
            f"unrecorded={unrecorded}; missing={missing}; changed={changed}",
        )


def _verify_task_pairs(
    actual: dict[str, str], expected: dict[str, str], *, source: str,
) -> None:
    if len(actual) != len(expected):
        missing = sorted(set(expected) - set(actual))
        extra = sorted(set(actual) - set(expected))
        raise TB21LockError(
            f"{source} task-count mismatch: expected {len(expected)}, got {len(actual)}; "
            f"missing={missing}; extra={extra}",
        )
    missing = sorted(set(expected) - set(actual))
    if missing:
        raise TB21LockError(f"{source} is missing locked tasks: {missing}")
    extra = sorted(set(actual) - set(expected))
    if extra:
        raise TB21LockError(f"{source} has extra unlocked tasks: {extra}")
    changed = sorted(name for name, digest in expected.items() if actual[name] != digest)
    if changed:
        raise TB21LockError(f"{source} package digest mismatch for tasks: {changed}")


def _is_sha256_digest(value: str) -> bool:
    prefix, _, hex_digest = value.partition(":")
    return prefix == "sha256" and len(hex_digest) == 64 and all(
        char in "0123456789abcdef" for char in hex_digest
    )
