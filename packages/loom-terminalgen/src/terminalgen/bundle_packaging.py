from __future__ import annotations

import hashlib
import json
import stat
import zipfile
from pathlib import Path
from typing import Any

from terminalgen.bundle_validation import (
    VALIDATION_RESULTS_NAME,
    task_tree_sha256,
)


PACKAGE_MANIFEST_NAME = "package_manifest.json"
PACKAGE_CHECKSUMS_NAME = "SHA256SUMS"


def package_validated_bundles(
    tasks_root: Path,
    output_dir: Path,
    *,
    shard_size: int = 100,
    include_solutions: bool = True,
    require_docker_validation: bool = True,
) -> dict[str, Any]:
    if shard_size <= 0:
        raise ValueError("shard_size must be > 0")
    if not tasks_root.is_dir():
        raise ValueError(f"tasks root does not exist: {tasks_root}")
    if output_dir.exists() and any(output_dir.iterdir()):
        raise ValueError(f"output directory must be empty: {output_dir}")

    records = _load_validation_records(tasks_root)
    if not records:
        raise ValueError("validation report contains no task bundles")
    failed = [record for record in records if not record.get("passed")]
    if failed:
        raise ValueError(f"cannot package {len(failed)} bundle(s) that failed validation")
    if require_docker_validation:
        static_only = [record for record in records if not record.get("docker_executed")]
        if static_only:
            raise ValueError(
                f"cannot package {len(static_only)} bundle(s) without Docker validation"
            )

    task_roots = _resolve_validated_task_roots(tasks_root, records)
    output_dir.mkdir(parents=True, exist_ok=True)
    shards: list[dict[str, Any]] = []
    for shard_index, offset in enumerate(range(0, len(task_roots), shard_size)):
        selected = task_roots[offset : offset + shard_size]
        archive_name = f"task-bundles-{shard_index:04d}.zip"
        archive_path = output_dir / archive_name
        _write_deterministic_zip(
            archive_path,
            tasks_root=tasks_root,
            task_roots=selected,
            include_solutions=include_solutions,
        )
        shards.append(
            {
                "archive": archive_name,
                "sha256": _file_sha256(archive_path),
                "task_count": len(selected),
                "task_paths": [path.relative_to(tasks_root).as_posix() for path in selected],
            }
        )

    manifest: dict[str, Any] = {
        "format_version": "1.0",
        "tasks_root": tasks_root.name,
        "task_count": len(task_roots),
        "shard_count": len(shards),
        "shard_size": shard_size,
        "includes_solutions": include_solutions,
        "docker_validation_required": require_docker_validation,
        "shards": shards,
    }
    manifest_path = output_dir / PACKAGE_MANIFEST_NAME
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    checksum_rows = [
        f"{shard['sha256']}  {shard['archive']}" for shard in shards
    ]
    checksum_rows.append(f"{_file_sha256(manifest_path)}  {PACKAGE_MANIFEST_NAME}")
    (output_dir / PACKAGE_CHECKSUMS_NAME).write_text(
        "\n".join(checksum_rows) + "\n",
        encoding="utf-8",
    )
    return manifest


def _load_validation_records(tasks_root: Path) -> list[dict[str, Any]]:
    path = tasks_root / VALIDATION_RESULTS_NAME
    if not path.is_file():
        raise ValueError(
            f"missing {VALIDATION_RESULTS_NAME}; run validate-bundles before packaging"
        )
    records: list[dict[str, Any]] = []
    seen_paths: set[str] = set()
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if not line.strip():
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ValueError(f"invalid JSON on {path} line {line_number}: {exc}") from exc
        task_path = record.get("task_path")
        if not isinstance(task_path, str) or not task_path:
            raise ValueError(f"invalid task_path on {path} line {line_number}")
        if task_path in seen_paths:
            raise ValueError(f"duplicate task_path in validation report: {task_path}")
        seen_paths.add(task_path)
        records.append(record)
    return sorted(records, key=lambda record: record["task_path"])


def _resolve_validated_task_roots(
    tasks_root: Path,
    records: list[dict[str, Any]],
) -> list[Path]:
    resolved_root = tasks_root.resolve()
    task_roots: list[Path] = []
    for record in records:
        task_path = Path(record["task_path"])
        if task_path.is_absolute() or ".." in task_path.parts:
            raise ValueError(f"unsafe task_path in validation report: {task_path}")
        task_root = (tasks_root / task_path).resolve()
        if task_root.parent != resolved_root and resolved_root not in task_root.parents:
            raise ValueError(f"task_path escapes tasks root: {task_path}")
        if not (task_root / "task.toml").is_file():
            raise ValueError(f"validated task bundle is missing: {task_path}")
        actual_sha256 = task_tree_sha256(task_root)
        if actual_sha256 != record.get("sha256"):
            raise ValueError(
                f"bundle changed after validation: {task_path} "
                f"expected {record.get('sha256')}, got {actual_sha256}"
            )
        task_roots.append(task_root)

    discovered = {
        path.parent.resolve() for path in tasks_root.rglob("task.toml")
    }
    if discovered != set(task_roots):
        raise ValueError(
            "validation report does not cover the current task tree; rerun validate-bundles"
        )
    return task_roots


def _write_deterministic_zip(
    archive_path: Path,
    *,
    tasks_root: Path,
    task_roots: list[Path],
    include_solutions: bool,
) -> None:
    with zipfile.ZipFile(
        archive_path,
        mode="w",
        compression=zipfile.ZIP_DEFLATED,
        compresslevel=9,
    ) as archive:
        for task_root in task_roots:
            candidates = [task_root, *sorted(task_root.rglob("*"))]
            for path in candidates:
                relative_to_task = path.relative_to(task_root)
                if not include_solutions and relative_to_task.parts[:1] == ("solution",):
                    continue
                arcname = path.relative_to(tasks_root).as_posix()
                if path.is_dir():
                    _write_zip_directory(archive, f"{arcname}/")
                elif path.is_file():
                    _write_zip_file(archive, path, arcname)
                else:
                    raise ValueError(f"unsupported bundle entry: {path}")


def _write_zip_directory(archive: zipfile.ZipFile, arcname: str) -> None:
    info = _zip_info(arcname, mode=stat.S_IFDIR | 0o755)
    archive.writestr(info, b"")


def _write_zip_file(archive: zipfile.ZipFile, path: Path, arcname: str) -> None:
    executable = bool(path.stat().st_mode & 0o111)
    mode = stat.S_IFREG | (0o755 if executable else 0o644)
    info = _zip_info(arcname, mode=mode)
    archive.writestr(info, path.read_bytes())


def _zip_info(arcname: str, *, mode: int) -> zipfile.ZipInfo:
    info = zipfile.ZipInfo(arcname, date_time=(1980, 1, 1, 0, 0, 0))
    info.create_system = 3
    info.external_attr = mode << 16
    info.compress_type = zipfile.ZIP_DEFLATED
    return info


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()
