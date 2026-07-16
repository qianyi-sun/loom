#!/usr/bin/env python3
"""Scan staging rollout runtime artifacts for exact configured secret values."""

from __future__ import annotations

import argparse
import io
import json
import os
import re
import stat
import subprocess
import sys
import tomllib
from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass
from pathlib import Path

from dotenv import dotenv_values as parse_dotenv

CONFIG_PATH = Path("/etc/loom/staging-rollout.toml")
CATALOG_PATH = Path("/shared_work/qianyi/loom-worker-capacity/staging-catalog-provisioning.env")
PRIVATE_KEY_PATH = Path("/var/lib/loom-staging-rollout/gb10-deploy-ed25519")
TASKSET_TOKEN_PATH = Path(
    "/shared_work/qianyi/loom-worker-capacity/staging-taskset-fence-canary-token"
)
REQUEST_ROOT = Path("/var/lib/loom-staging-rollout/requests")
ROLLOUT_ROOT = Path("/data/loom-staging/rollouts")
_MAX_ARTIFACT_BYTES = 64 << 20
_MAX_SECRET_BYTES = 1 << 20
_SECRET_ENV_KEY = re.compile(
    r"(?:TOKEN|SECRET|PASSWORD|CREDENTIAL|ACCESS_KEY|DB_URL)", re.IGNORECASE
)


class BoundaryScanError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class Artifact:
    path: str
    payload: bytes


@dataclass(frozen=True, slots=True)
class ScanResult:
    path: str
    bytes_scanned: int
    match_count: int

    def to_dict(self) -> dict[str, object]:
        return {
            "path": self.path,
            "bytes_scanned": self.bytes_scanned,
            "match_count": self.match_count,
        }


def _read_regular_no_follow(path: Path, *, limit: int) -> bytes:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(path, flags)
    except OSError as exc:
        raise BoundaryScanError(f"required scan input is unavailable: {path}") from exc
    try:
        metadata = os.fstat(fd)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_size > limit:
            raise BoundaryScanError(f"required scan input is invalid: {path}")
        payload = os.read(fd, limit + 1)
        if len(payload) > limit:
            raise BoundaryScanError(f"required scan input is too large: {path}")
        return payload
    finally:
        os.close(fd)


def _file_source(value: object, name: str) -> Path:
    if not isinstance(value, str) or not value.startswith("file:"):
        raise BoundaryScanError(f"{name} is not a protected file source")
    path = Path(value.removeprefix("file:"))
    if not path.is_absolute() or ".." in path.parts:
        raise BoundaryScanError(f"{name} path is unsafe")
    return path


def _dotenv_values(payload: bytes) -> Iterator[bytes]:
    try:
        decoded = payload.decode("utf-8")
        parsed = parse_dotenv(stream=io.StringIO(decoded), interpolate=False)
    except (UnicodeDecodeError, ValueError) as exc:
        raise BoundaryScanError("catalog environment input is invalid") from exc
    for key, value in parsed.items():
        if _SECRET_ENV_KEY.search(key) is not None and value:
            yield value.encode("utf-8")


def load_configured_secrets(
    *,
    config_path: Path = CONFIG_PATH,
    catalog_path: Path = CATALOG_PATH,
    private_key_path: Path = PRIVATE_KEY_PATH,
    taskset_token_path: Path = TASKSET_TOKEN_PATH,
) -> tuple[bytes, ...]:
    config_payload = _read_regular_no_follow(config_path, limit=1 << 20)
    try:
        config = tomllib.loads(config_payload.decode("utf-8"))
    except (UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
        raise BoundaryScanError("staging rollout config is invalid") from exc
    values: list[bytes] = []
    for name in ("admin_token_source", "worker_token_source", "service_token_source"):
        source = _file_source(config.get(name), name)
        value = _read_regular_no_follow(source, limit=_MAX_SECRET_BYTES).strip()
        if value:
            values.append(value)
    values.extend(_dotenv_values(_read_regular_no_follow(catalog_path, limit=_MAX_SECRET_BYTES)))
    taskset_token = _read_regular_no_follow(taskset_token_path, limit=_MAX_SECRET_BYTES).strip()
    if taskset_token:
        values.append(taskset_token)
    private_key = _read_regular_no_follow(private_key_path, limit=_MAX_SECRET_BYTES).strip()
    if private_key:
        values.append(private_key)
        values.extend(line.strip() for line in private_key.splitlines() if line.strip())
    return tuple(dict.fromkeys(values))


def _tree_artifacts(root: Path) -> Iterator[Artifact]:
    if not os.path.lexists(root):
        return
    if root.is_symlink() or not root.is_dir():
        raise BoundaryScanError(f"artifact tree root is unsafe: {root}")

    def fail_walk(error: OSError) -> None:
        raise BoundaryScanError(f"artifact tree is unreadable: {root}") from error

    for directory, directories, filenames in os.walk(root, followlinks=False, onerror=fail_walk):
        for name in directories:
            if (Path(directory) / name).is_symlink():
                raise BoundaryScanError("artifact tree contains a symlink")
        for filename in sorted(filenames):
            path = Path(directory) / filename
            if path.is_symlink():
                raise BoundaryScanError("artifact tree contains a symlink")
            payload = _read_regular_no_follow(path, limit=_MAX_ARTIFACT_BYTES)
            yield Artifact(str(path), payload)


def live_artifacts() -> Iterator[Artifact]:
    proc_root = Path("/proc")
    if proc_root.is_dir():
        for entry in sorted(proc_root.iterdir(), key=lambda item: item.name):
            if not entry.name.isdigit():
                continue
            path = entry / "cmdline"
            try:
                payload = _read_regular_no_follow(path, limit=1 << 20)
            except BoundaryScanError:
                continue
            yield Artifact(f"/proc/{entry.name}/cmdline", payload)

    completed = subprocess.run(
        [
            "journalctl",
            "--no-pager",
            "--output=export",
            "--user-unit=loom-staging-rollout-*",
        ],
        capture_output=True,
        check=False,
    )
    if completed.returncode != 0:
        raise BoundaryScanError("journald export is unavailable")
    if len(completed.stdout) > _MAX_ARTIFACT_BYTES:
        raise BoundaryScanError("journald export is too large")
    yield Artifact("journald:loom-staging-rollout-*", completed.stdout)
    yield from _tree_artifacts(REQUEST_ROOT)
    yield from _tree_artifacts(ROLLOUT_ROOT)


def scan_artifacts(
    secrets: Iterable[bytes],
    artifacts: Iterable[Artifact],
) -> list[ScanResult]:
    needles = tuple(dict.fromkeys(value for value in secrets if value))
    if not needles:
        raise BoundaryScanError("no configured secrets were loaded")
    results: list[ScanResult] = []
    for artifact in artifacts:
        matches = sum(artifact.payload.count(value) for value in needles)
        results.append(ScanResult(artifact.path, len(artifact.payload), matches))
    return results


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="verify_staging_rollout_secret_boundary.py", allow_abbrev=False
    )
    parser.add_argument("--format", choices=("json", "text"), default="text")
    return parser


def main(
    argv: Sequence[str] | None = None,
    *,
    secrets: Iterable[bytes] | None = None,
    artifacts: Iterable[Artifact] | None = None,
) -> int:
    args = _parser().parse_args(list(argv) if argv is not None else None)
    try:
        results = scan_artifacts(
            load_configured_secrets() if secrets is None else secrets,
            live_artifacts() if artifacts is None else artifacts,
        )
    except BoundaryScanError as exc:
        sys.stderr.write(json.dumps({"error": str(exc)}, sort_keys=True) + "\n")
        return 2
    payload = [result.to_dict() for result in results]
    if args.format == "json":
        sys.stdout.write(json.dumps(payload, sort_keys=True) + "\n")
    else:
        for result in results:
            sys.stdout.write(
                f"{result.path}\tbytes={result.bytes_scanned}\tmatches={result.match_count}\n"
            )
    return 1 if any(result.match_count for result in results) else 0


if __name__ == "__main__":
    raise SystemExit(main())
