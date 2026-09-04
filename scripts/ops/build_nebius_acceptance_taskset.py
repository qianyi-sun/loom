#!/usr/bin/env python3
"""Build a deterministic, candidate-bound TaskSet for Nebius live acceptance."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from loom.nebius_acceptance_taskset import (
    NebiusAcceptanceTaskSetError,
    build_nebius_acceptance_taskset,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runtime-profile", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        evidence = build_nebius_acceptance_taskset(
            runtime_profile_path=args.runtime_profile.resolve(),
            output_dir=args.output.resolve(),
        )
    except NebiusAcceptanceTaskSetError as exc:
        sys.stderr.write(f"error: {exc}\n")
        return 1
    print(f"Built Nebius acceptance TaskSet: {args.output.resolve()}")
    print(f"candidate: {evidence['candidate_sha']}")
    print(f"bundle_sha256: {evidence['bundle_sha256']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
