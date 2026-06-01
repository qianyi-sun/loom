from __future__ import annotations

import hashlib
import importlib.resources as resources
import json
import re
import shutil
import subprocess
from dataclasses import dataclass
from fnmatch import fnmatch
from pathlib import Path
from typing import Any, Callable


_LOCK_FILE = "adp-upstream-source-lock.json"
_SKILLFLOW_TASK_ASSETS_LOCK_FILE = "adp-skillflow-task-assets-lock.json"
_SKILLFLOW_HARBOR_PATCH_ID = "skillflow-harbor-api-compat-20260601"


@dataclass(frozen=True)
class UpstreamSourcePatch:
    patch_id: str
    resource_name: str


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
    applied_patches: list[str]


@dataclass(frozen=True)
class SkillFlowTaskAssetsSpec:
    repo_id: str
    revision: str
    allow_patterns: list[str]

    def __post_init__(self) -> None:
        _require_non_empty("repo_id", self.repo_id)
        _require_non_empty("revision", self.revision)
        _require_strings("allow_patterns", self.allow_patterns)


@dataclass(frozen=True)
class MaterializedSkillFlowTaskAssets:
    repo_id: str
    revision: str
    allow_patterns: list[str]
    local_dir: Path
    lock_path: Path
    file_count: int
    reused: bool

    @property
    def source_type(self) -> str:
        return "huggingface-dataset"


HuggingFaceSnapshotDownloader = Callable[..., None]


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
    patches = _patches_for_spec(spec)
    patch_metadata = _patch_metadata(patches)
    if not force_refresh and _lock_matches(lock_path=lock_path, spec=spec, root=root):
        return _materialized(
            spec=spec,
            root=root,
            lock_path=lock_path,
            reused=True,
            applied_patches=[patch.patch_id for patch in patches],
        )

    if spec.source_type == "local-tree":
        _replace_with_local_tree(source_uri=spec.source_uri, cache_dir=cache_dir, root=root)
    elif spec.source_type == "git":
        _replace_with_git_checkout(source_uri=spec.source_uri, source_version=spec.source_version, cache_dir=cache_dir, root=root)
    else:
        raise ValueError(f"Unsupported upstream source type: {spec.source_type}")

    _apply_patches(root=root, patches=patches)
    _write_lock(
        spec=spec,
        root=root,
        lock_path=lock_path,
        applied_patches=patch_metadata,
    )
    return _materialized(
        spec=spec,
        root=root,
        lock_path=lock_path,
        reused=False,
        applied_patches=[patch.patch_id for patch in patches],
    )


def materialize_skillflow_task_assets(
    spec: SkillFlowTaskAssetsSpec,
    *,
    local_dir: Path,
    downloader: HuggingFaceSnapshotDownloader | None = None,
    force_refresh: bool = False,
) -> MaterializedSkillFlowTaskAssets:
    """Materialize pinned SkillFlow task assets into an upstream runner tree."""

    lock_path = local_dir / _SKILLFLOW_TASK_ASSETS_LOCK_FILE
    if not force_refresh and _skillflow_task_assets_lock_matches(lock_path=lock_path, spec=spec, local_dir=local_dir):
        lock = json.loads(lock_path.read_text(encoding="utf-8"))
        return MaterializedSkillFlowTaskAssets(
            repo_id=spec.repo_id,
            revision=spec.revision,
            allow_patterns=list(spec.allow_patterns),
            local_dir=local_dir,
            lock_path=lock_path,
            file_count=int(lock["file_count"]),
            reused=True,
        )

    local_dir.mkdir(parents=True, exist_ok=True)
    snapshot_downloader = downloader or download_huggingface_snapshot
    snapshot_downloader(
        repo_id=spec.repo_id,
        repo_type="dataset",
        revision=spec.revision,
        local_dir=local_dir,
        allow_patterns=list(spec.allow_patterns),
    )
    file_count = _count_files_matching_patterns(root=local_dir, patterns=spec.allow_patterns)
    _write_skillflow_task_assets_lock(
        spec=spec,
        local_dir=local_dir,
        lock_path=lock_path,
        file_count=file_count,
    )
    return MaterializedSkillFlowTaskAssets(
        repo_id=spec.repo_id,
        revision=spec.revision,
        allow_patterns=list(spec.allow_patterns),
        local_dir=local_dir,
        lock_path=lock_path,
        file_count=file_count,
        reused=False,
    )


def download_huggingface_snapshot(
    *,
    repo_id: str,
    repo_type: str,
    revision: str,
    local_dir: Path,
    allow_patterns: list[str],
) -> None:
    try:
        from huggingface_hub import snapshot_download
    except ImportError as exc:
        raise RuntimeError("huggingface_hub is required for SkillFlow dataset materialization") from exc

    snapshot_download(
        repo_id=repo_id,
        repo_type=repo_type,
        revision=revision,
        local_dir=local_dir,
        allow_patterns=allow_patterns,
    )


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


def _skillflow_task_assets_lock_matches(
    *,
    lock_path: Path,
    spec: SkillFlowTaskAssetsSpec,
    local_dir: Path,
) -> bool:
    if not lock_path.exists() or not local_dir.exists() or not local_dir.is_dir():
        return False
    try:
        lock = json.loads(lock_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return False

    return all(
        [
            lock.get("source_type") == "huggingface-dataset",
            lock.get("repo_id") == spec.repo_id,
            lock.get("revision") == spec.revision,
            lock.get("allow_patterns") == list(spec.allow_patterns),
            lock.get("local_dir") == str(local_dir),
            isinstance(lock.get("file_count"), int),
        ]
    )


def _write_skillflow_task_assets_lock(
    *,
    spec: SkillFlowTaskAssetsSpec,
    local_dir: Path,
    lock_path: Path,
    file_count: int,
) -> None:
    lock_path.write_text(
        json.dumps(
            {
                "source_type": "huggingface-dataset",
                "repo_id": spec.repo_id,
                "revision": spec.revision,
                "allow_patterns": list(spec.allow_patterns),
                "local_dir": str(local_dir),
                "file_count": file_count,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )


def _count_files_matching_patterns(*, root: Path, patterns: list[str]) -> int:
    return sum(
        1
        for path in root.rglob("*")
        if path.is_file() and _matches_any_pattern(path.relative_to(root).as_posix(), patterns)
    )


def _matches_any_pattern(relative_path: str, patterns: list[str]) -> bool:
    return any(fnmatch(relative_path, pattern) or _prefix_matches_globstar(relative_path, pattern) for pattern in patterns)


def _prefix_matches_globstar(relative_path: str, pattern: str) -> bool:
    if not pattern.endswith("/**"):
        return False
    return relative_path.startswith(pattern[:-3].rstrip("/") + "/")


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
            lock.get("applied_patches") == _patch_metadata(_patches_for_spec(spec)),
        ]
    )


def _write_lock(
    *,
    spec: UpstreamSourceSpec,
    root: Path,
    lock_path: Path,
    applied_patches: list[dict[str, str]],
) -> None:
    lock_path.write_text(
        json.dumps(
            _lock_payload(
                spec=spec,
                root=root,
                applied_patches=applied_patches,
            ),
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )


def _lock_payload(
    *,
    spec: UpstreamSourceSpec,
    root: Path,
    applied_patches: list[dict[str, str]],
) -> dict[str, Any]:
    return {
        "suite_name": spec.suite_name,
        "source_type": spec.source_type,
        "source_uri": spec.source_uri,
        "source_version": spec.source_version,
        "root": str(root),
        "cache_key": _cache_key(spec),
        "applied_patches": applied_patches,
    }


def _materialized(
    *,
    spec: UpstreamSourceSpec,
    root: Path,
    lock_path: Path,
    reused: bool,
    applied_patches: list[str],
) -> MaterializedUpstreamSource:
    return MaterializedUpstreamSource(
        suite_name=spec.suite_name,
        source_type=spec.source_type,
        source_uri=spec.source_uri,
        source_version=spec.source_version,
        root=root,
        lock_path=lock_path,
        reused=reused,
        applied_patches=applied_patches,
    )


def _patches_for_spec(spec: UpstreamSourceSpec) -> list[UpstreamSourcePatch]:
    if spec.suite_name == "SkillFlow":
        return [
            UpstreamSourcePatch(
                patch_id=_SKILLFLOW_HARBOR_PATCH_ID,
                resource_name=f"{_SKILLFLOW_HARBOR_PATCH_ID}.patch",
            )
        ]
    return []


def _patch_metadata(patches: list[UpstreamSourcePatch]) -> list[dict[str, str]]:
    return [
        {
            "id": patch.patch_id,
            "sha256": _patch_sha256(patch),
        }
        for patch in patches
    ]


def _apply_patches(*, root: Path, patches: list[UpstreamSourcePatch]) -> None:
    for patch in patches:
        patch_path = _patch_resource_path(patch)
        subprocess.run(
            ["git", "apply", "--unidiff-zero", "--whitespace=nowarn", str(patch_path)],
            cwd=root,
            capture_output=True,
            text=True,
            check=True,
        )


def _patch_sha256(patch: UpstreamSourcePatch) -> str:
    return hashlib.sha256(_patch_resource_path(patch).read_bytes()).hexdigest()


def _patch_resource_path(patch: UpstreamSourcePatch) -> Path:
    return Path(
        resources.files("agentic_data_platform.benchmarks")
        .joinpath("patches")
        .joinpath(patch.resource_name)
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


def _require_strings(name: str, values: list[str]) -> None:
    if isinstance(values, str) or not values:
        raise ValueError(f"{name} must be a non-empty list of strings")
    for value in values:
        _require_non_empty(name, value)
