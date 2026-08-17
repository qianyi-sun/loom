"""Render the inert personal-development management-plane shadow package."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Any

from loom.personal_dev_control_plane_config import (
    load_personal_dev_control_plane_profile,
    load_personal_dev_trusted_release,
)
from loom.personal_dev_control_plane_render import (
    render_shadow_personal_dev_control_plane,
)

_RENDER_ERROR = "error: personal-dev control-plane render inputs are invalid\n"


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


__all__ = ["add_personal_dev_control_plane_subparser"]
