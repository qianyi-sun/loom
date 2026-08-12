"""Render or observe the inert ``loom-dev`` capacity control plane."""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path
from typing import Any
from uuid import UUID

from pydantic import ValidationError

from loom_cli.capacity_control_plane import (
    load_capacity_control_plane_profile,
    render_capacity_control_plane_manifests,
)

_NAMESPACE = "loom-dev"
_CREDENTIALS = "/var/run/loom-capacity-manager/runtime/credentials"


def _non_nil_uuid_argument(value: str) -> UUID:
    try:
        authority = UUID(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "capacity authority incarnation must be a non-nil UUID"
        ) from exc
    if authority.int == 0:
        raise argparse.ArgumentTypeError(
            "capacity authority incarnation must be a non-nil UUID"
        )
    return authority


def _render(args: argparse.Namespace) -> int:
    try:
        profile = load_capacity_control_plane_profile(Path(args.file).resolve())
        rendered = render_capacity_control_plane_manifests(
            profile,
            manager_image=args.manager_image,
            authority_incarnation=args.authority_incarnation,
        )
    except ValidationError:
        sys.stderr.write("error: capacity control-plane render inputs are invalid\n")
        return 2
    except (OSError, ValueError) as exc:
        sys.stderr.write(f"error: capacity control-plane render failed: {exc}\n")
        return 2
    sys.stdout.write(rendered)
    return 0


def _status(args: argparse.Namespace) -> int:
    command = ["kubectl"]
    if args.kubeconfig is not None:
        kubeconfig = Path(args.kubeconfig).resolve()
        if not kubeconfig.is_file():
            sys.stderr.write("error: capacity status kubeconfig is not a file\n")
            return 2
        command.extend(["--kubeconfig", str(kubeconfig)])
    command.extend(
        [
            "--request-timeout=10s",
            "--namespace",
            _NAMESPACE,
            "exec",
            "deployment/loom-capacity-manager",
            "-c",
            "manager",
            "--",
            "python",
            "-m",
            "loom_capacity_manager.health_probe",
            "--url",
            "https://127.0.0.1:8443/healthz",
            "--ca-file",
            f"{_CREDENTIALS}/server-ca.pem",
            "--certificate-file",
            f"{_CREDENTIALS}/health-certificate.pem",
            "--private-key-file",
            f"{_CREDENTIALS}/health-private-key.pem",
            "--server-certificate-file",
            f"{_CREDENTIALS}/server-certificate.pem",
        ]
    )
    try:
        result = subprocess.run(command, check=False, timeout=15.0)
    except subprocess.TimeoutExpired:
        sys.stderr.write("error: capacity status timed out\n")
        return 2
    except OSError as exc:
        sys.stderr.write(f"error: capacity status could not run kubectl: {exc}\n")
        return 2
    return result.returncode


def add_capacity_control_plane_subparser(subparsers: Any) -> None:
    """Register the deliberately non-mutating capacity operator surface."""

    parent = subparsers.add_parser(
        "capacity-control-plane",
        help="Render or observe the one global capacity manager in loom-dev.",
    )
    operations = parent.add_subparsers(
        dest="capacity_control_plane_op",
        required=True,
    )
    render = operations.add_parser(
        "render",
        help="Render exact Kubernetes YAML to stdout without applying it.",
    )
    render.add_argument(
        "--file",
        type=Path,
        required=True,
        help="Strict non-secret capacity infrastructure TOML profile.",
    )
    render.add_argument(
        "--manager-image",
        required=True,
        help="Complete immutable loom-capacity-manager OCI digest reference.",
    )
    render.add_argument(
        "--authority-incarnation",
        type=_non_nil_uuid_argument,
        required=True,
        help="Reviewed non-nil UUID of the independent capacity authority.",
    )
    render.set_defaults(handler=_render)

    status = operations.add_parser(
        "status",
        help="Run the fixed mTLS zero-ceiling probe inside the manager Pod.",
    )
    status.add_argument(
        "--namespace",
        choices=[_NAMESPACE],
        default=_NAMESPACE,
        help="Fixed shared infrastructure namespace (default: loom-dev).",
    )
    status.add_argument(
        "--kubeconfig",
        type=Path,
        default=None,
        help="Optional kubeconfig used only by the read-only kubectl exec.",
    )
    status.set_defaults(handler=_status)


__all__ = ["add_capacity_control_plane_subparser"]
