#!/usr/bin/env python3
"""Render a published integration candidate into an independent Nebius platform."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "src"))

from loom.nebius_platform_render import (  # noqa: E402
    NebiusPlatformError,
    build_platform,
    write_platform,
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("environment-config", "candidate", "runtime-profile", "trusted-keyring", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    args = parser.parse_args()
    phase = "read-inputs"
    try:
        config = json.loads(args.environment_config.read_text())
        candidate = json.loads(args.candidate.read_text())
        profile = json.loads(args.runtime_profile.read_text())
        keyring_json = args.trusted_keyring.read_text()
        phase = "render-environment"
        files = build_platform(config, candidate, profile, json.loads(keyring_json), repo_root=ROOT)
        phase = "write-output"
        result = write_platform(files, config, candidate, args.output)
    except (KeyError, TypeError, ValueError, OSError) as exc:
        diagnostic = {"phase": phase, "error_type": type(exc).__name__}
        if type(exc) is ValueError or isinstance(exc, NebiusPlatformError):
            diagnostic["reason"] = str(exc)
        print(
            json.dumps(diagnostic, sort_keys=True),
            file=sys.stderr,
        )
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
