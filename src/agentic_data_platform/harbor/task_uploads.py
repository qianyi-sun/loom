from __future__ import annotations

import shutil
import stat
import tomllib
import zipfile
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path, PurePosixPath
from typing import Any


DEFAULT_MAX_ARCHIVE_FILES = 256
DEFAULT_MAX_UNCOMPRESSED_BYTES = 50 * 1024 * 1024


@dataclass(frozen=True)
class HarborTaskArchiveValidationResult:
    filename: str
    normalized_root: str
    task_name: str
    files: list[str]
    declared_artifacts: list[str]
    environment: dict[str, Any]
    resource_requirements: dict[str, Any]
    validation_errors: list[str]
    validation_warnings: list[str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "filename": self.filename,
            "normalized_root": self.normalized_root,
            "task_name": self.task_name,
            "files": list(self.files),
            "declared_artifacts": list(self.declared_artifacts),
            "environment": dict(self.environment),
            "resource_requirements": dict(self.resource_requirements),
            "errors": list(self.validation_errors),
            "warnings": list(self.validation_warnings),
        }


class HarborTaskArchiveError(ValueError):
    def __init__(self, errors: list[str]) -> None:
        self.errors = list(errors)
        super().__init__("; ".join(self.errors))


@dataclass(frozen=True)
class _ArchiveFile:
    source_name: str
    normalized_path: str
    mode: int | None


def validate_harbor_task_archive(
    payload: bytes,
    *,
    filename: str,
    max_files: int = DEFAULT_MAX_ARCHIVE_FILES,
    max_uncompressed_bytes: int = DEFAULT_MAX_UNCOMPRESSED_BYTES,
) -> HarborTaskArchiveValidationResult:
    result, _ = _inspect_archive(
        payload,
        filename=filename,
        max_files=max_files,
        max_uncompressed_bytes=max_uncompressed_bytes,
    )
    return result


def materialize_harbor_task_archive(
    payload: bytes,
    *,
    filename: str,
    destination: Path,
    max_files: int = DEFAULT_MAX_ARCHIVE_FILES,
    max_uncompressed_bytes: int = DEFAULT_MAX_UNCOMPRESSED_BYTES,
) -> HarborTaskArchiveValidationResult:
    result, files = _inspect_archive(
        payload,
        filename=filename,
        max_files=max_files,
        max_uncompressed_bytes=max_uncompressed_bytes,
    )
    if destination.exists():
        shutil.rmtree(destination)
    destination.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(BytesIO(payload)) as archive:
        for archive_file in files:
            target = destination / archive_file.normalized_path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(archive.read(archive_file.source_name))
            if archive_file.mode is not None:
                target.chmod(archive_file.mode)
    return result


def _inspect_archive(
    payload: bytes,
    *,
    filename: str,
    max_files: int,
    max_uncompressed_bytes: int,
) -> tuple[HarborTaskArchiveValidationResult, list[_ArchiveFile]]:
    errors: list[str] = []
    warnings: list[str] = []
    if not filename.lower().endswith(".zip"):
        errors.append("only .zip Harbor task archives are supported")
    if not payload:
        errors.append("archive is empty")

    try:
        archive = zipfile.ZipFile(BytesIO(payload))
    except zipfile.BadZipFile as exc:
        raise HarborTaskArchiveError(errors + ["archive is not a valid zip file"]) from exc

    with archive:
        archive_files = _normalized_archive_files(
            archive,
            errors=errors,
            max_files=max_files,
            max_uncompressed_bytes=max_uncompressed_bytes,
        )
        normalized_names = [item.normalized_path for item in archive_files]
        file_set = set(normalized_names)

        for required_path in ("instruction.md", "task.toml", "environment/Dockerfile"):
            if required_path not in file_set:
                errors.append(f"missing required Harbor task file: {required_path}")
        test_files = [name for name in normalized_names if name.startswith("tests/")]
        if not test_files:
            errors.append("missing required Harbor verifier tests/ directory")

        task_config = _read_task_toml(archive, archive_files, errors=errors)
        task_name = _task_name(task_config, errors=errors)
        declared_artifacts = _declared_artifacts(task_config, errors=errors)
        environment = _table(task_config, "environment", errors=errors)
        verifier = _table(task_config, "verifier", errors=errors)
        agent = _table(task_config, "agent", errors=errors)
        resource_requirements = _resource_requirements(
            agent=agent,
            verifier=verifier,
            environment=environment,
        )

        if test_files and not _tests_reference_reward_file(archive, archive_files):
            warnings.append("tests/ does not reference /logs/verifier/reward.txt; Harbor verifier reward may be missing")

    if errors:
        raise HarborTaskArchiveError(errors)

    return (
        HarborTaskArchiveValidationResult(
            filename=filename,
            normalized_root=_normalized_root(archive_files),
            task_name=task_name,
            files=normalized_names,
            declared_artifacts=declared_artifacts,
            environment=environment,
            resource_requirements=resource_requirements,
            validation_errors=[],
            validation_warnings=warnings,
        ),
        archive_files,
    )


def _normalized_archive_files(
    archive: zipfile.ZipFile,
    *,
    errors: list[str],
    max_files: int,
    max_uncompressed_bytes: int,
) -> list[_ArchiveFile]:
    entries: list[tuple[str, str, int | None]] = []
    raw_paths: list[str] = []
    uncompressed_size = 0
    for info in archive.infolist():
        if info.is_dir():
            continue
        path = _posix_path(info.filename)
        if path is None or path.startswith("__MACOSX/") or path.endswith("/.DS_Store"):
            continue
        if _is_unsafe_zip_path(path):
            errors.append(f"unsafe archive path: {info.filename}")
            continue
        if _is_symlink(info):
            errors.append(f"symlink entries are not allowed in Harbor task archives: {info.filename}")
            continue
        raw_paths.append(path)
        uncompressed_size += info.file_size
        entries.append((info.filename, path, _file_mode(info)))

    if not entries:
        errors.append("archive does not contain any regular files")
        return []
    if len(entries) > max_files:
        errors.append(f"archive contains too many files: {len(entries)} > {max_files}")
    if uncompressed_size > max_uncompressed_bytes:
        errors.append(
            "archive uncompressed content is too large: "
            f"{uncompressed_size} bytes > {max_uncompressed_bytes} bytes"
        )

    root = _common_single_root(raw_paths)
    normalized: list[_ArchiveFile] = []
    seen: set[str] = set()
    for source_name, path, mode in entries:
        normalized_path = path
        if root is not None:
            normalized_path = path.removeprefix(f"{root}/")
        if normalized_path in seen:
            errors.append(f"duplicate archive path after root normalization: {normalized_path}")
            continue
        seen.add(normalized_path)
        normalized.append(_ArchiveFile(source_name=source_name, normalized_path=normalized_path, mode=mode))
    normalized.sort(key=lambda item: item.normalized_path)
    return normalized


def _read_task_toml(
    archive: zipfile.ZipFile,
    files: list[_ArchiveFile],
    *,
    errors: list[str],
) -> dict[str, Any]:
    task_file = next((item for item in files if item.normalized_path == "task.toml"), None)
    if task_file is None:
        return {}
    try:
        return tomllib.loads(archive.read(task_file.source_name).decode("utf-8"))
    except UnicodeDecodeError:
        errors.append("task.toml must be UTF-8 encoded")
    except tomllib.TOMLDecodeError as exc:
        errors.append(f"task.toml is invalid TOML: {exc}")
    return {}


def _task_name(task_config: dict[str, Any], *, errors: list[str]) -> str:
    task = _table(task_config, "task", errors=errors)
    name = task.get("name")
    if not isinstance(name, str) or not name.strip():
        errors.append("task.toml must include [task].name")
        return ""
    return name.strip()


def _declared_artifacts(task_config: dict[str, Any], *, errors: list[str]) -> list[str]:
    artifacts = task_config.get("artifacts", [])
    if not isinstance(artifacts, list) or any(not isinstance(item, str) or not item.strip() for item in artifacts):
        errors.append("task.toml artifacts must be a list of non-empty strings")
        return []
    return [item.strip() for item in artifacts]


def _table(task_config: dict[str, Any], name: str, *, errors: list[str]) -> dict[str, Any]:
    value = task_config.get(name)
    if not isinstance(value, dict):
        errors.append(f"task.toml must include [{name}]")
        return {}
    return dict(value)


def _resource_requirements(
    *,
    agent: dict[str, Any],
    verifier: dict[str, Any],
    environment: dict[str, Any],
) -> dict[str, Any]:
    requirements: dict[str, Any] = {}
    _copy_numeric(agent, "timeout_sec", requirements, "agent_timeout_sec")
    _copy_numeric(verifier, "timeout_sec", requirements, "verifier_timeout_sec")
    _copy_numeric(environment, "build_timeout_sec", requirements, "environment_build_timeout_sec")
    return requirements


def _copy_numeric(source: dict[str, Any], source_key: str, target: dict[str, Any], target_key: str) -> None:
    value = source.get(source_key)
    if isinstance(value, int | float) and not isinstance(value, bool):
        target[target_key] = value


def _tests_reference_reward_file(archive: zipfile.ZipFile, files: list[_ArchiveFile]) -> bool:
    for archive_file in files:
        if not archive_file.normalized_path.startswith("tests/"):
            continue
        try:
            content = archive.read(archive_file.source_name).decode("utf-8", errors="ignore")
        except RuntimeError:
            continue
        if "reward.txt" in content:
            return True
    return False


def _normalized_root(files: list[_ArchiveFile]) -> str:
    roots = {item.source_name.split("/", maxsplit=1)[0] for item in files if "/" in item.source_name}
    if len(roots) == 1 and all(item.source_name.startswith(f"{next(iter(roots))}/") for item in files):
        return next(iter(roots))
    return "."


def _common_single_root(paths: list[str]) -> str | None:
    first_parts = {path.split("/", maxsplit=1)[0] for path in paths if "/" in path}
    if len(first_parts) != 1:
        return None
    root = next(iter(first_parts))
    if all(path.startswith(f"{root}/") for path in paths):
        return root
    return None


def _posix_path(raw_name: str) -> str | None:
    path = raw_name.replace("\\", "/").strip()
    return path or None


def _is_unsafe_zip_path(path: str) -> bool:
    pure = PurePosixPath(path)
    return pure.is_absolute() or any(part in {"", ".", ".."} for part in pure.parts)


def _is_symlink(info: zipfile.ZipInfo) -> bool:
    mode = info.external_attr >> 16
    return stat.S_IFMT(mode) == stat.S_IFLNK


def _file_mode(info: zipfile.ZipInfo) -> int | None:
    mode = info.external_attr >> 16
    permissions = stat.S_IMODE(mode)
    return permissions or None
