#!/usr/bin/env python3
"""Refresh ephemeral registry auth from an independently scoped authorized key."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
if __package__ in {None, ""}:
    sys.path.insert(0, str(ROOT / "src"))

from loom.nebius_registry_auth import mint_registry_auth  # noqa: E402


def refresh_registry_auth(
    credentials: Path,
    registry_prefix: str,
    auth_file: Path,
    *,
    sdk_factory: Any = None,
) -> dict[str, str]:
    """Write Docker/containers auth atomically; never return or print the token."""
    if (
        credentials.is_symlink()
        or not credentials.is_file()
        or credentials.stat().st_mode & 0o077
        or credentials.stat().st_size > 1024 * 1024
        or credentials.resolve().is_relative_to(ROOT)
        or auth_file.resolve().is_relative_to(ROOT)
        or auth_file.is_symlink()
    ):
        raise ValueError("registry credentials must be private and outside the build context")
    return mint_registry_auth(credentials, registry_prefix, auth_file, sdk_factory=sdk_factory)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--credentials", type=Path, required=True)
    parser.add_argument("--registry-prefix", required=True)
    parser.add_argument("--auth-file", type=Path, required=True)
    args = parser.parse_args()
    try:
        result = refresh_registry_auth(args.credentials, args.registry_prefix, args.auth_file)
    except Exception as exc:
        print(f"Nebius registry authentication failed ({type(exc).__name__})", file=sys.stderr)
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
