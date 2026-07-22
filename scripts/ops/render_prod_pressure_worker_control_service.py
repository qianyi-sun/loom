#!/usr/bin/env python3
"""Render the production-pressure worker-control unit for one exact candidate."""

from __future__ import annotations

import argparse
import re
import sys
from collections.abc import Sequence
from pathlib import Path

_GIT_SHA_TOKEN = "${GIT_SHA}"
_FULL_GIT_SHA_RE = re.compile(r"[0-9a-f]{40}")
_EXPECTED_TOKEN_COUNT = 4
_MAX_TEMPLATE_BYTES = 64 * 1024
_LEGACY_RUNTIME_PATHS = (
    "/opt/loom-staging-runner/repo",
    "/opt/loom-staging-runner/venv",
)


def render_service_unit(template: str, *, git_sha: str) -> str:
    """Bind a checked-in service template to one immutable candidate SHA."""
    if _FULL_GIT_SHA_RE.fullmatch(git_sha) is None:
        raise ValueError("git_sha must be a 40-character lowercase hexadecimal SHA")
    if len(template.encode("utf-8")) > _MAX_TEMPLATE_BYTES:
        raise ValueError("service template exceeds the bounded size")
    if any(path in template for path in _LEGACY_RUNTIME_PATHS):
        raise ValueError("service template references a mutable legacy runtime path")
    if template.count(_GIT_SHA_TOKEN) != _EXPECTED_TOKEN_COUNT:
        raise ValueError("service template has an unexpected GIT_SHA placeholder count")

    rendered = template.replace(_GIT_SHA_TOKEN, git_sha)
    if _GIT_SHA_TOKEN in rendered or "${GIT_SHA" in rendered:
        raise ValueError("service template retains an unresolved GIT_SHA placeholder")
    candidate_root = f"/opt/loom-staging-runner/candidates/{git_sha}"
    required_fragments = (
        f"WorkingDirectory={candidate_root}/repo\n",
        f"Environment=PATH={candidate_root}/venv/bin:/usr/local/bin:/usr/bin:/bin\n",
        f"ExecStart={candidate_root}/venv/bin/python -I -B ",
        f" {candidate_root}/repo/scripts/ops/prod_pressure_worker_control.py ",
    )
    if any(fragment not in rendered for fragment in required_fragments):
        raise ValueError("rendered service is not fully bound to the exact candidate")
    return rendered


def _template_path() -> Path:
    return (
        Path(__file__).resolve().parents[2]
        / "deploy/worker-capacity/loom-prod-pressure-worker-control.service"
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--git-sha", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        template = _template_path().read_text(encoding="utf-8")
        rendered = render_service_unit(template, git_sha=args.git_sha)
    except (OSError, UnicodeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    sys.stdout.write(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
