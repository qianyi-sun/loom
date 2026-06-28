#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import posixpath
import shlex
import subprocess
import sys
from collections.abc import Callable, Sequence

RunFn = Callable[..., subprocess.CompletedProcess[str | bytes]]


def _as_bytes(value: str | bytes | None) -> bytes:
    if value is None:
        return b""
    if isinstance(value, bytes):
        return value
    return value.encode()


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Write a locally produced secret to a remote file over ssh without "
            "printing the secret or mixing it with a heredoc."
        ),
    )
    parser.add_argument("--host", required=True, help="SSH target host.")
    parser.add_argument(
        "--remote-path",
        required=True,
        help="Absolute path to write on the remote host.",
    )
    parser.add_argument(
        "secret_command",
        nargs=argparse.REMAINDER,
        help="Local command that prints the secret, usually after --.",
    )
    return parser


def _normalize_secret_command(command: Sequence[str]) -> list[str]:
    normalized = list(command)
    if normalized and normalized[0] == "--":
        normalized = normalized[1:]
    return normalized


def _validate_remote_path(remote_path: str) -> None:
    if not remote_path.startswith("/") or "\x00" in remote_path:
        raise ValueError("--remote-path must be an absolute POSIX path")
    if posixpath.basename(remote_path) in {"", ".", ".."}:
        raise ValueError("--remote-path must include a file name")


def _read_secret(command: Sequence[str], run: RunFn) -> bytes:
    normalized = _normalize_secret_command(command)
    if not normalized:
        raise ValueError("secret command is required after --")

    result = run(
        normalized,
        capture_output=True,
        text=False,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(f"secret command failed with exit {result.returncode}")

    secret = _as_bytes(result.stdout).rstrip(b"\r\n")
    if not secret:
        raise ValueError("secret command produced no output")
    return secret


def _remote_write_script(remote_path: str) -> str:
    _validate_remote_path(remote_path)
    remote_dir = posixpath.dirname(remote_path)
    remote_base = posixpath.basename(remote_path)
    return "\n".join(
        [
            "set -euo pipefail",
            f"target={shlex.quote(remote_path)}",
            f"target_dir={shlex.quote(remote_dir)}",
            f"base={shlex.quote(remote_base)}",
            "umask 077",
            'mkdir -p -- "$target_dir"',
            'tmp=$(mktemp "$target_dir/.${base}.XXXXXX")',
            'cleanup() { rm -f -- "$tmp"; }',
            "trap cleanup EXIT",
            'cat > "$tmp"',
            'chmod 600 "$tmp"',
            'mv -f -- "$tmp" "$target"',
            "trap - EXIT",
            "stat -c 'remote_secret_file_written mode=%a size=%s path=%n' \"$target\"",
            "",
        ],
    )


def _write_remote_secret(
    *,
    host: str,
    remote_path: str,
    secret: bytes,
    run: RunFn,
) -> subprocess.CompletedProcess[str]:
    script = _remote_write_script(remote_path)
    return run(
        ["ssh", "-o", "BatchMode=yes", host, f"bash -lc {shlex.quote(script)}"],
        input=secret,
        capture_output=True,
        text=False,
        check=False,
    )


def main(argv: Sequence[str] | None = None, *, run: RunFn = subprocess.run) -> int:
    args = _parser().parse_args(argv)
    try:
        secret = _read_secret(args.secret_command, run)
        result = _write_remote_secret(
            host=args.host,
            remote_path=args.remote_path,
            secret=secret,
            run=run,
        )
    except (RuntimeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    stdout = _as_bytes(result.stdout)
    if stdout:
        sys.stdout.buffer.write(stdout)
        if not stdout.endswith(b"\n"):
            sys.stdout.write(os.linesep)
    if result.returncode != 0:
        print(f"error: remote write failed with exit {result.returncode}", file=sys.stderr)
        return result.returncode
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
