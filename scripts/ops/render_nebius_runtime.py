#!/usr/bin/env python3
"""Render one environment-local Nebius runtime deployment set."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from loom.nebius_runtime_render import NebiusRuntimeRenderError, render_nebius_runtime


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--environment", choices=("development", "staging", "production"), required=True
    )
    parser.add_argument("--image", required=True, help="Digest-pinned execution actuator image.")
    parser.add_argument("--capacity-policy", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--topology",
        type=Path,
        default=None,
    )
    parser.add_argument(
        "--physical-binding",
        type=Path,
        default=None,
    )
    args = parser.parse_args(argv)
    repo_root = Path(__file__).resolve().parents[2]
    topology_path = (
        args.topology.resolve()
        if args.topology is not None
        else repo_root / "config" / "service-execution-topology.json"
    )
    physical_binding_path = (
        args.physical_binding.resolve()
        if args.physical_binding is not None
        else repo_root / "config" / "nebius-runtime-physical-binding.json"
    )
    try:
        manifest = render_nebius_runtime(
            repo_root=repo_root,
            environment=args.environment,
            image=args.image,
            topology_path=topology_path,
            physical_binding_path=physical_binding_path,
            capacity_policy_path=args.capacity_policy.resolve(),
            output_dir=args.output.resolve(),
        )
    except NebiusRuntimeRenderError as exc:
        sys.stderr.write(f"error: {exc}\n")
        return 1
    print(f"Rendered {manifest['environment']} Nebius runtime: {args.output.resolve()}")
    print(f"target: {manifest['target_id']}")
    print(f"namespace: {manifest['namespace']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
