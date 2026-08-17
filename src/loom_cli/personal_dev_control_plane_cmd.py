"""Render the inert personal-development management-plane shadow package."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import selectors
import stat
import subprocess
import sys
import time
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from loom.personal_dev_control_plane_config import (
    load_personal_dev_control_plane_profile,
    load_personal_dev_trusted_release,
)
from loom.personal_dev_control_plane_render import (
    render_shadow_personal_dev_control_plane,
)
from loom.personal_dev_control_plane_status import (
    MAX_PERSONAL_DEV_STATUS_RESPONSE_BYTES,
    observe_personal_dev_shadow_status,
)

_RENDER_ERROR = "error: personal-dev control-plane render inputs are invalid\n"
_STATUS_ERROR = "error: personal-dev control-plane status inputs are invalid\n"
_KUBECTL_READ_BYTES = 64 * 1024


def _render(args: argparse.Namespace) -> int:
    try:
        profile = load_personal_dev_control_plane_profile(args.file)
        release = load_personal_dev_trusted_release(
            args.trusted_release_file,
            args.trusted_release_sha256,
        )
        rendered = render_shadow_personal_dev_control_plane(profile, release)
        evidence = json.dumps(
            {
                "input_sha256": rendered.input_sha256,
                "mode": "shadow",
                "release_sha256": rendered.release_sha256,
                "resource_count": rendered.resource_count,
                "schema": "loom-personal-dev-control-plane-render-v1",
                "source_sha": release.source_sha,
                "source_tree": release.source_tree,
                "yaml_sha256": hashlib.sha256(rendered.yaml_text.encode("utf-8")).hexdigest(),
            },
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        )
    except (OSError, TypeError, ValueError):
        sys.stderr.write(_RENDER_ERROR)
        return 2

    try:
        sys.stdout.write(rendered.yaml_text)
    except BrokenPipeError:
        return 0
    sys.stderr.write(evidence + "\n")
    return 0


class _SubprocessKubectlRunner:
    def __init__(self, kubeconfig: Path) -> None:
        self._prefix = ("kubectl", "--kubeconfig", str(kubeconfig))

    def run(
        self,
        argv: Sequence[str],
        *,
        timeout_seconds: int,
    ) -> subprocess.CompletedProcess[str]:
        if type(timeout_seconds) is not int or not 1 <= timeout_seconds <= 60:
            raise ValueError("kubectl timeout is invalid")
        command = [*self._prefix, *argv]
        process = subprocess.Popen(
            command,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            close_fds=True,
        )
        if process.stdout is None or process.stderr is None:  # pragma: no cover
            process.kill()
            process.wait()
            raise OSError("kubectl output pipes are unavailable")
        streams = {
            "stdout": (process.stdout, bytearray()),
            "stderr": (process.stderr, bytearray()),
        }
        selector = selectors.DefaultSelector()
        deadline = time.monotonic() + timeout_seconds
        total = 0
        try:
            for name, (stream, _buffer) in streams.items():
                os.set_blocking(stream.fileno(), False)
                selector.register(stream, selectors.EVENT_READ, name)
            while selector.get_map():
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise subprocess.TimeoutExpired(command, timeout_seconds)
                events = selector.select(remaining)
                if not events:
                    raise subprocess.TimeoutExpired(command, timeout_seconds)
                for key, _mask in events:
                    try:
                        chunk = os.read(
                            key.fd,
                            min(
                                _KUBECTL_READ_BYTES,
                                MAX_PERSONAL_DEV_STATUS_RESPONSE_BYTES + 1 - total,
                            ),
                        )
                    except BlockingIOError:
                        continue
                    if not chunk:
                        selector.unregister(key.fileobj)
                        continue
                    total += len(chunk)
                    if total > MAX_PERSONAL_DEV_STATUS_RESPONSE_BYTES:
                        raise OSError("kubectl output exceeds its size bound")
                    streams[key.data][1].extend(chunk)
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise subprocess.TimeoutExpired(command, timeout_seconds)
            returncode = process.wait(timeout=remaining)
        except BaseException:
            if process.poll() is None:
                process.kill()
            process.wait()
            raise
        finally:
            selector.close()
            process.stdout.close()
            process.stderr.close()
        return subprocess.CompletedProcess(
            [*self._prefix, *argv],
            returncode,
            streams["stdout"][1].decode("utf-8"),
            streams["stderr"][1].decode("utf-8"),
        )


def _safe_kubeconfig(path: Path) -> bool:
    if not path.is_absolute():
        return False
    try:
        if path.resolve(strict=True) != path:
            return False
        opened = path.lstat()
    except (OSError, RuntimeError):
        return False
    return stat.S_ISREG(opened.st_mode) and not stat.S_ISLNK(opened.st_mode)


def _status(args: argparse.Namespace) -> int:
    try:
        if not _safe_kubeconfig(args.kubeconfig):
            raise ValueError("kubeconfig is invalid")
        profile = load_personal_dev_control_plane_profile(args.file)
        release = load_personal_dev_trusted_release(
            args.trusted_release_file,
            args.trusted_release_sha256,
        )
        expected = render_shadow_personal_dev_control_plane(profile, release)
    except (OSError, TypeError, ValueError):
        sys.stderr.write(_STATUS_ERROR)
        return 2

    status_value = observe_personal_dev_shadow_status(
        _SubprocessKubectlRunner(args.kubeconfig),
        expected=expected,
        namespace=args.namespace,
    )
    output = (
        json.dumps(
            status_value.to_dict(),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        )
        + "\n"
    )
    try:
        sys.stdout.write(output)
    except BrokenPipeError:
        return 0 if status_value.ready else 1
    return 0 if status_value.ready else 1


def add_personal_dev_control_plane_subparser(subparsers: Any) -> None:
    """Register the deliberately render-only personal-dev shadow surface."""

    parent = subparsers.add_parser(
        "personal-dev-control-plane",
        allow_abbrev=False,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        help="Render the inert personal-development management-plane shadow.",
        description=(
            "render-only personal-development management-plane shadow\n"
            "personal mutations disabled\n"
            "physical capacity unchanged"
        ),
    )
    operations = parent.add_subparsers(
        dest="personal_dev_control_plane_op",
        required=True,
    )
    render = operations.add_parser(
        "render",
        allow_abbrev=False,
        help="Render exact Kubernetes YAML and canonical evidence.",
    )
    render.add_argument(
        "--file",
        type=Path,
        required=True,
        help="Strict non-secret personal-dev management-plane TOML profile.",
    )
    render.add_argument(
        "--trusted-release-file",
        type=Path,
        required=True,
        help="Owner-only canonical trusted-release JSON document.",
    )
    render.add_argument(
        "--trusted-release-sha256",
        required=True,
        help="Exact SHA-256 of the canonical trusted-release document.",
    )
    render.set_defaults(handler=_render)

    status_parser = operations.add_parser(
        "status",
        allow_abbrev=False,
        help="Compare bounded live state with one exact trusted shadow render.",
    )
    status_parser.add_argument(
        "--namespace",
        choices=["loom-dev"],
        default="loom-dev",
        help="Fixed shared infrastructure namespace (default: loom-dev).",
    )
    status_parser.add_argument(
        "--kubeconfig",
        type=Path,
        required=True,
        help="Absolute non-symlink path to the reviewed kubeconfig.",
    )
    status_parser.add_argument(
        "--file",
        type=Path,
        required=True,
        help="Strict non-secret personal-dev management-plane TOML profile.",
    )
    status_parser.add_argument(
        "--trusted-release-file",
        type=Path,
        required=True,
        help="Owner-only canonical trusted-release JSON document.",
    )
    status_parser.add_argument(
        "--trusted-release-sha256",
        required=True,
        help="Exact SHA-256 of the canonical trusted-release document.",
    )
    status_parser.set_defaults(handler=_status)


__all__ = ["add_personal_dev_control_plane_subparser"]
