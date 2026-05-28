from __future__ import annotations

import hashlib
import json
import re
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any


_LOCK_FILE = "adp-upstream-source-lock.json"


@dataclass(frozen=True)
class UpstreamSourceSpec:
    suite_name: str
    source_type: str
    source_uri: str
    source_version: str

    def __post_init__(self) -> None:
        _require_non_empty("suite_name", self.suite_name)
        _require_non_empty("source_type", self.source_type)
        _require_non_empty("source_uri", self.source_uri)
        _require_non_empty("source_version", self.source_version)


@dataclass(frozen=True)
class MaterializedUpstreamSource:
    suite_name: str
    source_type: str
    source_uri: str
    source_version: str
    root: Path
    lock_path: Path
    reused: bool


def materialize_upstream_source(
    spec: UpstreamSourceSpec,
    *,
    cache_root: Path,
    force_refresh: bool = False,
) -> MaterializedUpstreamSource:
    """Materialize an upstream benchmark source into the platform cache.

    The cache is keyed by suite, source type, source URI, and requested version.
    Reusing a matching lock keeps benchmark runs pinned to the same source tree
    even if a mutable local mirror or branch changes after the first run.
    """

    cache_dir = _cache_dir(cache_root=cache_root, spec=spec)
    root = cache_dir / "tree"
    lock_path = cache_dir / _LOCK_FILE
    if not force_refresh and _lock_matches(lock_path=lock_path, spec=spec, root=root):
        return _materialized(spec=spec, root=root, lock_path=lock_path, reused=True)

    if spec.source_type == "local-tree":
        _replace_with_local_tree(source_uri=spec.source_uri, cache_dir=cache_dir, root=root)
    elif spec.source_type == "git":
        _replace_with_git_checkout(source_uri=spec.source_uri, source_version=spec.source_version, cache_dir=cache_dir, root=root)
    else:
        raise ValueError(f"Unsupported upstream source type: {spec.source_type}")

    _write_lock(spec=spec, root=root, lock_path=lock_path)
    return _materialized(spec=spec, root=root, lock_path=lock_path, reused=False)


def _replace_with_local_tree(*, source_uri: str, cache_dir: Path, root: Path) -> None:
    source_root = Path(source_uri).expanduser().resolve()
    if not source_root.exists() or not source_root.is_dir():
        raise ValueError(f"local-tree source_uri must be an existing directory: {source_uri}")

    _reset_cache_dir(cache_dir)
    shutil.copytree(
        source_root,
        root,
        ignore=shutil.ignore_patterns(".git", "__pycache__", ".DS_Store"),
    )


def _replace_with_git_checkout(*, source_uri: str, source_version: str, cache_dir: Path, root: Path) -> None:
    _reset_cache_dir(cache_dir)
    _run_git(["clone", "--no-checkout", source_uri, str(root)], cwd=None)
    _run_git(["checkout", "--detach", source_version], cwd=root)


def _reset_cache_dir(cache_dir: Path) -> None:
    if cache_dir.exists():
        shutil.rmtree(cache_dir)
    cache_dir.mkdir(parents=True)


def _lock_matches(*, lock_path: Path, spec: UpstreamSourceSpec, root: Path) -> bool:
    if not lock_path.exists() or not root.exists() or not root.is_dir():
        return False
    try:
        lock = json.loads(lock_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return False

    return all(
        [
            lock.get("suite_name") == spec.suite_name,
            lock.get("source_type") == spec.source_type,
            lock.get("source_uri") == spec.source_uri,
            lock.get("source_version") == spec.source_version,
            lock.get("root") == str(root),
        ]
    )


def _write_lock(*, spec: UpstreamSourceSpec, root: Path, lock_path: Path) -> None:
    lock_path.write_text(
        json.dumps(_lock_payload(spec=spec, root=root), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _lock_payload(*, spec: UpstreamSourceSpec, root: Path) -> dict[str, Any]:
    return {
        "suite_name": spec.suite_name,
        "source_type": spec.source_type,
        "source_uri": spec.source_uri,
        "source_version": spec.source_version,
        "root": str(root),
        "cache_key": _cache_key(spec),
    }


def _materialized(
    *,
    spec: UpstreamSourceSpec,
    root: Path,
    lock_path: Path,
    reused: bool,
) -> MaterializedUpstreamSource:
    return MaterializedUpstreamSource(
        suite_name=spec.suite_name,
        source_type=spec.source_type,
        source_uri=spec.source_uri,
        source_version=spec.source_version,
        root=root,
        lock_path=lock_path,
        reused=reused,
    )


def _cache_dir(*, cache_root: Path, spec: UpstreamSourceSpec) -> Path:
    return cache_root / _slug(spec.suite_name) / _slug(spec.source_type) / _cache_key(spec)


def _cache_key(spec: UpstreamSourceSpec) -> str:
    digest = hashlib.sha256(f"{spec.source_uri}\n{spec.source_version}".encode("utf-8")).hexdigest()[:12]
    return f"{_slug(spec.source_version, max_length=48)}-{digest}"


def _slug(value: str, *, max_length: int = 80) -> str:
    slug = re.sub(r"[^A-Za-z0-9._-]+", "-", value.strip()).strip("-._")
    if not slug:
        return "source"
    return slug[:max_length]


def _run_git(args: list[str], *, cwd: Path | None) -> None:
    subprocess.run(
        ["git", *args],
        cwd=cwd,
        capture_output=True,
        text=True,
        check=True,
    )


def _require_non_empty(name: str, value: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")
