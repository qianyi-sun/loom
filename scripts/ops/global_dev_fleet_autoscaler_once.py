#!/usr/bin/env python3
"""Run one global development-fleet capacity reconciliation."""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from pathlib import Path

from loom_control_plane.global_dev_fleet_autoscaler import (
    DevCapacityDemand,
    GlobalDevAutoscalerError,
    GlobalDevFleetAutoscaler,
)
from loom_control_plane.shared_capacity_broker import (
    BrokerBudgets,
    BrokerError,
    LeaseObservation,
    SharedCapacityBroker,
)


def _budget_entries(values: list[str], field: str) -> dict[str, int]:
    result: dict[str, int] = {}
    for value in values:
        pool, separator, raw_slots = value.partition("=")
        if separator != "=" or not pool or pool in result:
            raise GlobalDevAutoscalerError(f"{field} must contain unique POOL=SLOTS entries")
        try:
            slots = int(raw_slots)
        except ValueError as exc:
            raise GlobalDevAutoscalerError(f"{field} slots must be integers") from exc
        if slots < 0:
            raise GlobalDevAutoscalerError(f"{field} slots must be non-negative")
        result[pool] = slots
    return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Allocate one shared worker budget across all development environments.",
        allow_abbrev=False,
    )
    parser.add_argument("--state-db", type=Path, required=True)
    parser.add_argument("--input-json", type=Path, required=True)
    parser.add_argument("--output-json", type=Path, required=True)
    parser.add_argument("--global-budget", type=int, required=True)
    parser.add_argument("--pool-budget", action="append", default=[], required=True)
    parser.add_argument("--global-pending-budget", type=int)
    parser.add_argument("--pool-pending-budget", action="append", default=[])
    parser.add_argument("--snapshot-freshness-seconds", type=int, default=120)
    parser.add_argument("--lease-ttl-seconds", type=int, default=300)
    return parser


def _load_input(path: Path) -> tuple[tuple[DevCapacityDemand, ...], tuple[LeaseObservation, ...]]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise GlobalDevAutoscalerError("input snapshot is unavailable or invalid JSON") from exc
    if not isinstance(raw, dict) or set(raw) != {"schema_version", "demands", "observations"}:
        raise GlobalDevAutoscalerError("input snapshot fields do not match the versioned contract")
    if (
        raw["schema_version"] != 1
        or not isinstance(raw["demands"], list)
        or not isinstance(raw["observations"], list)
    ):
        raise GlobalDevAutoscalerError("input snapshot contract is invalid")
    demands = tuple(
        DevCapacityDemand.from_mapping(item) for item in raw["demands"] if isinstance(item, dict)
    )
    observations = tuple(
        LeaseObservation.from_mapping(item)
        for item in raw["observations"]
        if isinstance(item, dict)
    )
    if len(demands) != len(raw["demands"]) or len(observations) != len(raw["observations"]):
        raise GlobalDevAutoscalerError("input snapshot entries must be objects")
    return demands, observations


def _atomic_write(path: Path, document: dict[str, object]) -> None:
    if not path.is_absolute() or not path.parent.is_dir():
        raise GlobalDevAutoscalerError("output path must be absolute with an existing parent")
    payload = (json.dumps(document, sort_keys=True, separators=(",", ":")) + "\n").encode()
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    except BaseException:
        try:
            os.close(descriptor)
        except OSError:
            pass
        try:
            os.unlink(temporary)
        except OSError:
            pass
        raise


def main() -> int:
    args = _parser().parse_args()
    try:
        demands, observations = _load_input(args.input_json)
        pool_slots = _budget_entries(args.pool_budget, "--pool-budget")
        pending_slots = _budget_entries(args.pool_pending_budget, "--pool-pending-budget")
        if not pending_slots:
            pending_slots = dict(pool_slots)
        report = GlobalDevFleetAutoscaler(
            SharedCapacityBroker(args.state_db),
            snapshot_freshness_seconds=args.snapshot_freshness_seconds,
            lease_ttl_seconds=args.lease_ttl_seconds,
        ).reconcile(
            demands,
            BrokerBudgets(
                global_slots=args.global_budget,
                pool_slots=pool_slots,
                global_pending_slots=(
                    args.global_budget
                    if args.global_pending_budget is None
                    else args.global_pending_budget
                ),
                pool_pending_slots=pending_slots,
            ),
            observations=observations,
        )
        _atomic_write(args.output_json, report)
        print(json.dumps({"authority": report["authority"], "aggregate": report["aggregate"]}))
        return 0
    except (BrokerError, GlobalDevAutoscalerError) as exc:
        sys.stderr.write(f"error: {exc}\n")
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
