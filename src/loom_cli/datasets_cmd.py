"""`loom datasets list` — Plan 23 ships a placeholder that lists the
built-in `loom_benchmarks.REGISTRY` entries. Plan 24 replaces this with
full discovery (entry-points + remote registry + remote CP query).
"""

from __future__ import annotations

import argparse
import sys

from loom_benchmarks.registry import REGISTRY


def dispatch(args: argparse.Namespace) -> int:
    if args.datasets_cmd == "list":
        return _list()
    print(f"unknown datasets subcommand: {args.datasets_cmd}", file=sys.stderr)
    return 2


def _list() -> int:
    print(f"{'SLUG':<28} {'LICENSE':<14}")
    for slug, adapter in sorted(REGISTRY.items()):
        print(f"{slug:<28} {adapter.license_spdx:<14}")
    return 0
