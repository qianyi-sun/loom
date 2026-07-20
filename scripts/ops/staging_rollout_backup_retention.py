#!/usr/bin/env python3
"""Inventory or converge exact superseded staging backup payloads."""

from __future__ import annotations

import argparse
import json
import os
import stat
from pathlib import Path

from loom_cli.rollout.operator.config import OperatorConfig
from loom_cli.rollout.operator.legacy_backup_retention import (
    LegacyBackupRetention,
    LegacyBackupRetentionPlan,
)
from loom_cli.rollout.operator.store import RequestStore

_CONFIG_PATH = Path("/etc/loom/staging-rollout.toml")
_STATE_ROOT = Path("/var/lib/loom-staging-rollout")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("action", choices=("inventory", "apply"))
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--plan", type=Path)
    parser.add_argument("--approved-inventory-digest")
    parser.add_argument("--protect-bundle", action="append", default=[])
    return parser


def _publish(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    descriptor = os.open(
        path,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
        0o600,
    )
    with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, sort_keys=True, separators=(",", ":"))
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())


def _read_plan(path: Path) -> LegacyBackupRetentionPlan:
    metadata = path.lstat()
    if (
        not path.is_absolute()
        or not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != os.geteuid()
        or stat.S_IMODE(metadata.st_mode) != 0o600
    ):
        raise ValueError("backup retention plan path is unsafe")
    descriptor = os.open(
        path,
        os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
    )
    with os.fdopen(descriptor, "r", encoding="utf-8") as handle:
        opened = os.fstat(handle.fileno())
        if (opened.st_dev, opened.st_ino) != (metadata.st_dev, metadata.st_ino):
            raise ValueError("backup retention plan changed during open")
        payload = json.load(handle)
    if not isinstance(payload, dict) or set(payload) != {"evidence_digest", "plan"}:
        raise ValueError("backup retention plan document is invalid")
    if not isinstance(payload["plan"], dict) or not isinstance(payload["evidence_digest"], str):
        raise ValueError("backup retention plan document is invalid")
    plan = LegacyBackupRetentionPlan.from_dict(payload["plan"])
    if plan.evidence_digest != payload["evidence_digest"]:
        raise ValueError("backup retention plan digest drifted")
    return plan


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    config = OperatorConfig.load(_CONFIG_PATH)
    service_uid = os.geteuid()
    retention = LegacyBackupRetention(
        config=config,
        service_uid=service_uid,
        store=RequestStore(_STATE_ROOT),
    )
    if args.action == "inventory":
        if args.plan is not None or args.approved_inventory_digest is not None:
            raise ValueError("inventory does not accept apply authority")
        plan = retention.inventory(additionally_protected=frozenset(args.protect_bundle))
        document: dict[str, object] = {
            "evidence_digest": plan.evidence_digest,
            "plan": plan.to_dict(),
        }
    else:
        if args.plan is None or args.approved_inventory_digest is None or args.protect_bundle:
            raise ValueError("apply requires only an exact plan and approved digest")
        plan = _read_plan(args.plan)
        document = retention.apply(
            plan,
            approved_inventory_digest=args.approved_inventory_digest,
        )
    _publish(args.output, document)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
