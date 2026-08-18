"""Render and observe the zero-capacity personal-development management plane."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import selectors
import stat
import subprocess
import sys
import tempfile
import time
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import yaml  # type: ignore[import-untyped]

from loom.personal_dev_control_plane_config import (
    load_personal_dev_acceptance_plan,
    load_personal_dev_control_plane_profile,
    load_personal_dev_trusted_release,
)
from loom.personal_dev_control_plane_render import (
    render_acceptance_personal_dev_control_plane,
    render_shadow_personal_dev_control_plane,
)
from loom.personal_dev_control_plane_status import (
    MAX_PERSONAL_DEV_STATUS_RESPONSE_BYTES,
    observe_personal_dev_acceptance_status,
    observe_personal_dev_shadow_status,
)

_RENDER_ERROR = "error: personal-dev control-plane render inputs are invalid\n"
_STATUS_ERROR = "error: personal-dev control-plane status inputs are invalid\n"
_KUBECTL_READ_BYTES = 64 * 1024
_MAX_KUBECONFIG_BYTES = 1024 * 1024
_KubeconfigIdentity = tuple[int, int, int, int, int, int, int, int, int]


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


def _render_acceptance(args: argparse.Namespace) -> int:
    try:
        profile = load_personal_dev_control_plane_profile(args.file)
        release = load_personal_dev_trusted_release(
            args.trusted_release_file,
            args.trusted_release_sha256,
        )
        plan = load_personal_dev_acceptance_plan(
            args.acceptance_plan_file,
            args.acceptance_plan_sha256,
        )
        rendered = render_acceptance_personal_dev_control_plane(
            profile,
            release,
            plan,
            now=datetime.now(UTC),
        )
        evidence = json.dumps(
            {
                "acceptance_plan_sha256": plan.sha256,
                "input_sha256": rendered.input_sha256,
                "mode": "acceptance",
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
        self._kubeconfig = kubeconfig
        loaded = _load_safe_kubeconfig(kubeconfig)
        if loaded is None:
            raise ValueError("kubeconfig is invalid")
        self._kubeconfig_identity, self._kubeconfig_payload = loaded

    def _validate_kubeconfig(self) -> None:
        if _load_safe_kubeconfig(self._kubeconfig) != (
            self._kubeconfig_identity,
            self._kubeconfig_payload,
        ):
            raise OSError("kubeconfig changed during observation")

    def _open_kubeconfig(self) -> int:
        self._validate_kubeconfig()
        return _anonymous_kubeconfig_snapshot(self._kubeconfig_payload)

    def run(
        self,
        argv: Sequence[str],
        *,
        timeout_seconds: int,
    ) -> subprocess.CompletedProcess[str]:
        if type(timeout_seconds) is not int or not 1 <= timeout_seconds <= 60:
            raise ValueError("kubectl timeout is invalid")
        self._validate_kubeconfig()
        kubeconfig_descriptor = self._open_kubeconfig()
        display_command = ["kubectl", "--kubeconfig", str(self._kubeconfig), *argv]
        command = [
            "kubectl",
            "--kubeconfig",
            f"/proc/self/fd/{kubeconfig_descriptor}",
            *argv,
        ]
        try:
            process = subprocess.Popen(
                command,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                close_fds=True,
                pass_fds=(kubeconfig_descriptor,),
            )
        finally:
            os.close(kubeconfig_descriptor)
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
            self._validate_kubeconfig()
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
            display_command,
            returncode,
            streams["stdout"][1].decode("utf-8"),
            streams["stderr"][1].decode("utf-8"),
        )


def _kubeconfig_identity(opened: os.stat_result) -> _KubeconfigIdentity:
    return (
        opened.st_dev,
        opened.st_ino,
        opened.st_mode,
        opened.st_uid,
        opened.st_gid,
        opened.st_nlink,
        opened.st_size,
        opened.st_mtime_ns,
        opened.st_ctime_ns,
    )


def _self_contained_kubeconfig(payload: bytes) -> bool:
    try:
        document = yaml.safe_load(payload)
    except (RecursionError, UnicodeDecodeError, yaml.YAMLError):
        return False
    if (
        not isinstance(document, Mapping)
        or document.get("apiVersion") != "v1"
        or document.get("kind") != "Config"
        or not isinstance(document.get("current-context"), str)
        or not document["current-context"]
    ):
        return False

    def entries(field: str, body_field: str) -> dict[str, Mapping[str, object]] | None:
        values = document.get(field)
        if not isinstance(values, list) or not 1 <= len(values) <= 128:
            return None
        result: dict[str, Mapping[str, object]] = {}
        for value in values:
            if not isinstance(value, Mapping):
                return None
            name = value.get("name")
            body = value.get(body_field)
            if (
                not isinstance(name, str)
                or not name
                or name in result
                or not isinstance(body, Mapping)
            ):
                return None
            result[name] = body
        return result

    clusters = entries("clusters", "cluster")
    contexts = entries("contexts", "context")
    users = entries("users", "user")
    if clusters is None or contexts is None or users is None:
        return False
    current = contexts.get(document["current-context"])
    if current is None:
        return False
    cluster_name = current.get("cluster")
    user_name = current.get("user")
    if (
        not isinstance(cluster_name, str)
        or not isinstance(user_name, str)
        or cluster_name not in clusters
        or user_name not in users
    ):
        return False
    if any("certificate-authority" in cluster for cluster in clusters.values()):
        return False
    external_user_fields = {
        "auth-provider",
        "client-certificate",
        "client-key",
        "exec",
        "tokenFile",
    }
    return all(not external_user_fields.intersection(user) for user in users.values())


def _load_safe_kubeconfig(path: Path) -> tuple[_KubeconfigIdentity, bytes] | None:
    if not path.is_absolute():
        return None
    try:
        if path.resolve(strict=True) != path:
            return None
        path_metadata = path.lstat()
    except (OSError, RuntimeError):
        return None
    if (
        not stat.S_ISREG(path_metadata.st_mode)
        or stat.S_ISLNK(path_metadata.st_mode)
        or path_metadata.st_uid != os.geteuid()
        or stat.S_IMODE(path_metadata.st_mode) != 0o600
        or path_metadata.st_nlink != 1
        or not 0 < path_metadata.st_size <= _MAX_KUBECONFIG_BYTES
    ):
        return None
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError:
        return None
    try:
        opened = os.fstat(descriptor)
        identity = _kubeconfig_identity(opened)
        if identity != _kubeconfig_identity(path_metadata):
            return None
        payload = bytearray()
        while len(payload) <= _MAX_KUBECONFIG_BYTES:
            chunk = os.read(
                descriptor, min(_KUBECTL_READ_BYTES, _MAX_KUBECONFIG_BYTES + 1 - len(payload))
            )
            if not chunk:
                break
            payload.extend(chunk)
        if (
            len(payload) != opened.st_size
            or _kubeconfig_identity(os.fstat(descriptor)) != identity
            or _safe_path_identity(path) != identity
            or not _self_contained_kubeconfig(bytes(payload))
        ):
            return None
        return identity, bytes(payload)
    except OSError:
        return None
    finally:
        os.close(descriptor)


def _safe_path_identity(path: Path) -> _KubeconfigIdentity | None:
    try:
        return _kubeconfig_identity(path.lstat())
    except OSError:
        return None


def _anonymous_kubeconfig_snapshot(payload: bytes) -> int:
    descriptor: int | None = None
    try:
        temporary_directory = tempfile.gettempdir()
        temporary_flag = getattr(os, "O_TMPFILE", 0)
        if temporary_flag:
            try:
                descriptor = os.open(
                    temporary_directory,
                    temporary_flag | os.O_RDWR | getattr(os, "O_CLOEXEC", 0),
                    0o600,
                )
            except OSError:
                descriptor = None
        if descriptor is None:
            descriptor, temporary_path = tempfile.mkstemp(
                prefix="loom-personal-dev-kubeconfig-",
                dir=temporary_directory,
            )
            os.unlink(temporary_path)
        view = memoryview(payload)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise OSError("kubeconfig snapshot write failed")
            view = view[written:]
        os.fchmod(descriptor, 0o400)
        os.lseek(descriptor, 0, os.SEEK_SET)
        read_descriptor = os.open(
            f"/proc/self/fd/{descriptor}",
            os.O_RDONLY | getattr(os, "O_CLOEXEC", 0),
        )
        os.close(descriptor)
        return read_descriptor
    except BaseException:
        if descriptor is not None:
            os.close(descriptor)
        raise


def _safe_kubeconfig(path: Path) -> _KubeconfigIdentity | None:
    loaded = _load_safe_kubeconfig(path)
    return None if loaded is None else loaded[0]


def _status(args: argparse.Namespace) -> int:
    try:
        if _safe_kubeconfig(args.kubeconfig) is None:
            raise ValueError("kubeconfig is invalid")
        profile = load_personal_dev_control_plane_profile(args.file)
        release = load_personal_dev_trusted_release(
            args.trusted_release_file,
            args.trusted_release_sha256,
        )
        expected = render_shadow_personal_dev_control_plane(profile, release)
        runner = _SubprocessKubectlRunner(args.kubeconfig)
    except (OSError, TypeError, ValueError):
        sys.stderr.write(_STATUS_ERROR)
        return 2

    status_value = observe_personal_dev_shadow_status(
        runner,
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


def _status_acceptance(args: argparse.Namespace) -> int:
    try:
        if _safe_kubeconfig(args.kubeconfig) is None:
            raise ValueError("kubeconfig is invalid")
        profile = load_personal_dev_control_plane_profile(args.file)
        release = load_personal_dev_trusted_release(
            args.trusted_release_file,
            args.trusted_release_sha256,
        )
        plan = load_personal_dev_acceptance_plan(
            args.acceptance_plan_file,
            args.acceptance_plan_sha256,
        )
        # Reconstruct the immutable target even after expiry; the live observer
        # reports actual window state as a blocker instead of hiding cluster drift.
        expected = render_acceptance_personal_dev_control_plane(
            profile,
            release,
            plan,
            now=plan.window.started_at,
        )
        runner = _SubprocessKubectlRunner(args.kubeconfig)
    except (OSError, TypeError, ValueError):
        sys.stderr.write(_STATUS_ERROR)
        return 2

    status_value = observe_personal_dev_acceptance_status(
        runner,
        expected=expected,
        plan=plan,
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
    """Register the render-only and read-only personal-dev operator surface."""

    parent = subparsers.add_parser(
        "personal-dev-control-plane",
        allow_abbrev=False,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        help="Render or observe the zero-capacity personal-development management plane.",
        description=(
            "render-only and read-only personal-development management-plane shadow "
            "or acceptance\n"
            "these commands never mutate resources\n"
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

    render_acceptance = operations.add_parser(
        "render-acceptance",
        allow_abbrev=False,
        help="Render exact zero-capacity acceptance YAML and canonical evidence.",
    )
    render_acceptance.add_argument(
        "--file",
        type=Path,
        required=True,
        help="Strict non-secret personal-dev management-plane TOML profile.",
    )
    render_acceptance.add_argument(
        "--trusted-release-file",
        type=Path,
        required=True,
        help="Owner-only canonical trusted-release JSON document.",
    )
    render_acceptance.add_argument(
        "--trusted-release-sha256",
        required=True,
        help="Exact SHA-256 of the canonical trusted-release document.",
    )
    render_acceptance.add_argument(
        "--acceptance-plan-file",
        type=Path,
        required=True,
        help="Owner-only canonical zero-capacity acceptance plan.",
    )
    render_acceptance.add_argument(
        "--acceptance-plan-sha256",
        required=True,
        help="Exact SHA-256 of the canonical acceptance plan.",
    )
    render_acceptance.set_defaults(handler=_render_acceptance)

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

    status_acceptance = operations.add_parser(
        "status-acceptance",
        allow_abbrev=False,
        help="Observe one exact zero-capacity acceptance without mutation.",
    )
    status_acceptance.add_argument(
        "--namespace",
        choices=["loom-dev"],
        default="loom-dev",
        help="Fixed shared infrastructure namespace (default: loom-dev).",
    )
    status_acceptance.add_argument(
        "--kubeconfig",
        type=Path,
        required=True,
        help="Absolute non-symlink path to the reviewed kubeconfig.",
    )
    status_acceptance.add_argument(
        "--file",
        type=Path,
        required=True,
        help="Strict non-secret personal-dev management-plane TOML profile.",
    )
    status_acceptance.add_argument(
        "--trusted-release-file",
        type=Path,
        required=True,
        help="Owner-only canonical trusted-release JSON document.",
    )
    status_acceptance.add_argument(
        "--trusted-release-sha256",
        required=True,
        help="Exact SHA-256 of the canonical trusted-release document.",
    )
    status_acceptance.add_argument(
        "--acceptance-plan-file",
        type=Path,
        required=True,
        help="Owner-only canonical zero-capacity acceptance plan.",
    )
    status_acceptance.add_argument(
        "--acceptance-plan-sha256",
        required=True,
        help="Exact SHA-256 of the canonical acceptance plan.",
    )
    status_acceptance.set_defaults(handler=_status_acceptance)


__all__ = ["add_personal_dev_control_plane_subparser"]
