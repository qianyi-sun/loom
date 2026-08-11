#!/usr/bin/env python3
"""Prove a worker node trusts and can pull from the staging task registry."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import stat
import subprocess
import sys
from pathlib import Path

_SHA256_RE = re.compile(r"[0-9a-f]{64}")
_DIGEST_RE = re.compile(r"sha256:[0-9a-f]{64}")
_REGISTRY_REPO_RE = re.compile(
    r"[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?"
    r"(?::[1-9][0-9]{0,4})?"
    r"/[a-z0-9]+(?:[._-][a-z0-9]+)*"
    r"(?:/[a-z0-9]+(?:[._-][a-z0-9]+)*)*"
)


class ProbeError(RuntimeError):
    """A secret-safe node probe failure."""


def _read_regular_file(path: Path, *, maximum_bytes: int) -> bytes:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise ProbeError("required probe file is unavailable") from exc
    try:
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_nlink != 1
            or metadata.st_size <= 0
            or metadata.st_size > maximum_bytes
        ):
            raise ProbeError("required probe file metadata is unsafe")
        payload = bytearray()
        while len(payload) <= maximum_bytes:
            chunk = os.read(descriptor, min(65536, maximum_bytes + 1 - len(payload)))
            if not chunk:
                break
            payload.extend(chunk)
        if len(payload) != metadata.st_size:
            raise ProbeError("required probe file changed during validation")
        try:
            current = os.lstat(path)
        except OSError as exc:
            raise ProbeError("required probe file changed during validation") from exc
        if (current.st_dev, current.st_ino, current.st_size, current.st_mtime_ns) != (
            metadata.st_dev,
            metadata.st_ino,
            metadata.st_size,
            metadata.st_mtime_ns,
        ):
            raise ProbeError("required probe file changed during validation")
        return bytes(payload)
    finally:
        os.close(descriptor)


def _registry_repo_from_env(path: Path) -> str:
    payload = _read_regular_file(path, maximum_bytes=1024 * 1024)
    try:
        lines = payload.decode("utf-8").splitlines()
    except UnicodeDecodeError as exc:
        raise ProbeError("worker environment is not valid UTF-8") from exc
    values: list[str] = []
    for line in lines:
        key, separator, value = line.partition("=")
        if separator and key == "LOOM_WORKER_TRIAL_CACHE_REGISTRY_REPO":
            values.append(value.strip())
    if len(values) != 1 or _REGISTRY_REPO_RE.fullmatch(values[0]) is None:
        raise ProbeError("worker registry repository is missing or invalid")
    return values[0]


def _run(argv: list[str], *, timeout: float) -> subprocess.CompletedProcess[str]:
    try:
        result = subprocess.run(
            argv,
            capture_output=True,
            text=True,
            check=False,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired as exc:
        raise ProbeError(f"registry probe timed out safely: {Path(argv[0]).name}") from exc
    if result.returncode != 0:
        raise ProbeError(f"registry probe failed safely: {Path(argv[0]).name}")
    return result


def _probe(args: argparse.Namespace) -> dict[str, str]:
    expected_repo = args.expected_registry_repo
    if _REGISTRY_REPO_RE.fullmatch(expected_repo) is None:
        raise ProbeError("expected registry repository is invalid")
    if _SHA256_RE.fullmatch(args.expected_ca_sha256) is None:
        raise ProbeError("expected registry CA digest is invalid")
    if _DIGEST_RE.fullmatch(args.canary_digest) is None:
        raise ProbeError("expected registry canary digest is invalid")

    candidate_ca = _read_regular_file(args.ca_file, maximum_bytes=64 * 1024)
    candidate_digest = hashlib.sha256(candidate_ca).hexdigest()
    if candidate_digest != args.expected_ca_sha256:
        raise ProbeError("candidate registry CA does not match the accepted digest")
    if _registry_repo_from_env(args.env_file) != expected_repo:
        raise ProbeError("worker registry repository drifted from the accepted contract")

    authority = expected_repo.split("/", 1)[0]
    installed_ca_path = args.docker_certs_root / authority / "ca.crt"
    installed_ca = _read_regular_file(installed_ca_path, maximum_bytes=64 * 1024)
    if installed_ca != candidate_ca:
        raise ProbeError("installed registry CA does not match the candidate")

    try:
        docker_metadata = args.docker_bin.lstat()
    except OSError as exc:
        raise ProbeError("Docker CLI is unavailable") from exc
    if (
        args.docker_bin.is_symlink()
        or not stat.S_ISREG(docker_metadata.st_mode)
        or not os.access(args.docker_bin, os.X_OK)
    ):
        raise ProbeError("Docker CLI metadata is unsafe")

    registry_image = f"{expected_repo}:{args.canary_tag}"
    _run(
        [str(args.docker_bin), "pull", "--quiet", registry_image],
        timeout=args.timeout_seconds,
    )
    inspected = _run(
        [
            str(args.docker_bin),
            "image",
            "inspect",
            "--format",
            "{{json .RepoDigests}}",
            registry_image,
        ],
        timeout=15,
    )
    try:
        repo_digests = json.loads(inspected.stdout.strip())
    except json.JSONDecodeError as exc:
        raise ProbeError("Docker returned invalid registry digest evidence") from exc
    expected_repo_digest = f"{expected_repo}@{args.canary_digest}"
    if (
        not isinstance(repo_digests, list)
        or expected_repo_digest not in repo_digests
        or any(not isinstance(item, str) for item in repo_digests)
    ):
        raise ProbeError("pulled registry canary digest does not match")
    return {
        "ca_sha256": candidate_digest,
        "registry_image": registry_image,
        "repo_digest": expected_repo_digest,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", type=Path, required=True)
    parser.add_argument("--ca-file", type=Path, required=True)
    parser.add_argument(
        "--docker-certs-root",
        type=Path,
        default=Path("/etc/docker/certs.d"),
    )
    parser.add_argument("--docker-bin", type=Path, default=Path("/usr/bin/docker"))
    parser.add_argument("--expected-registry-repo", required=True)
    parser.add_argument("--expected-ca-sha256", required=True)
    parser.add_argument("--canary-tag", default="transport-canary")
    parser.add_argument("--canary-digest", required=True)
    parser.add_argument("--timeout-seconds", type=float, default=45.0)
    return parser


def main() -> int:
    args = _parser().parse_args()
    if (
        not args.env_file.is_absolute()
        or not args.ca_file.is_absolute()
        or not args.docker_certs_root.is_absolute()
        or not args.docker_bin.is_absolute()
        or not 0 < args.timeout_seconds <= 120
        or args.canary_tag != "transport-canary"
    ):
        raise ProbeError("registry probe arguments are unsafe")
    print(json.dumps(_probe(args), sort_keys=True))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ProbeError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise SystemExit(1) from None
