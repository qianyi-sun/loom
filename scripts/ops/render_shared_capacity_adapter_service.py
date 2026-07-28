#!/usr/bin/env python3
"""Render the shared-capacity adapter unit for one immutable candidate."""

from __future__ import annotations

import argparse
import re
import sys
from collections.abc import Sequence
from pathlib import Path

_TOKEN = "${GIT_SHA}"
_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
_EXPECTED_TOKEN_COUNT = 4
_MAX_TEMPLATE_BYTES = 64 * 1024
_FORBIDDEN_RUNTIME_PATHS = (
    "/opt/loom-shared-capacity/current",
    "/opt/loom-shared-capacity/repo",
    "/opt/loom-shared-capacity/venv",
)


def render_service_unit(template: str, *, git_sha: str) -> str:
    if _SHA_RE.fullmatch(git_sha) is None:
        raise ValueError("git_sha must be a 40-character lowercase hexadecimal SHA")
    if len(template.encode("utf-8")) > _MAX_TEMPLATE_BYTES:
        raise ValueError("service template exceeds the bounded size")
    if any(path in template for path in _FORBIDDEN_RUNTIME_PATHS):
        raise ValueError("service template references a mutable runtime path")
    if template.count(_TOKEN) != _EXPECTED_TOKEN_COUNT:
        raise ValueError("service template has an unexpected GIT_SHA placeholder count")
    rendered = template.replace(_TOKEN, git_sha)
    if _TOKEN in rendered or "${GIT_SHA" in rendered:
        raise ValueError("service template retains an unresolved GIT_SHA placeholder")
    root = f"/opt/loom-shared-capacity/candidates/{git_sha}"
    required = (
        f"WorkingDirectory={root}/repo\n",
        f"Environment=PATH={root}/venv/bin:/usr/local/bin:/usr/bin:/bin\n",
        f"ExecStart={root}/venv/bin/python -I -B ",
        f" {root}/repo/scripts/ops/shared_capacity_adapter.py ",
    )
    if any(fragment not in rendered for fragment in required):
        raise ValueError("rendered service is not fully bound to the exact candidate")
    return rendered


def _template_path() -> Path:
    return (
        Path(__file__).resolve().parents[2]
        / "deploy/developer-sandboxes/loom-shared-capacity-adapter@.service"
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--git-sha", required=True)
    args = parser.parse_args(list(argv) if argv is not None else None)
    try:
        rendered = render_service_unit(
            _template_path().read_text(encoding="utf-8"),
            git_sha=args.git_sha,
        )
    except (OSError, UnicodeError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    sys.stdout.write(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
