#!/usr/bin/env python3
# Copyright (c) 2023 Stanford Vision and Learning Group
# SPDX-License-Identifier: MIT
#
# Derived and modified from OmniGibson agentic_sweep validation. Loom keeps the
# authoritative implementation importable instead of maintaining a shell parser.
"""Validate offline-judge output bytes with Loom's canonical validator."""

from __future__ import annotations

import argparse
from pathlib import Path

from loom.integrations.behavior.agentic_sweep import validate_sweep_outputs


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", required=True, type=Path)
    parser.add_argument("--seed", required=True, type=Path)
    parser.add_argument("--task-name", required=True)
    parser.add_argument("--engine-task-instance-id", required=True, type=int)
    parser.add_argument("--task-id", required=True, type=int)
    parser.add_argument("--demo-id", required=True, type=int)
    parser.add_argument("--n-steps", required=True, type=int)
    parser.add_argument("--rollout-artifact-id", required=True)
    args = parser.parse_args(argv)
    validate_sweep_outputs(
        args.report.read_bytes(),
        args.seed.read_bytes(),
        task_name=args.task_name,
        engine_task_instance_id=args.engine_task_instance_id,
        task_id=args.task_id,
        demo_id=args.demo_id,
        n_steps=args.n_steps,
        rollout_artifact_id=args.rollout_artifact_id,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
