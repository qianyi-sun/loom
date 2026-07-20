#!/usr/bin/env python3
"""Inventory or initialize the one exact staging lifecycle epoch authority."""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path

from sqlalchemy import create_engine

from loom.data_lifecycle_bootstrap import (
    SqlAlchemyLifecycleBootstrap,
    lifecycle_bootstrap_plan_document,
)
from loom.data_lifecycle_gc import GcScope
from loom_control_plane.config import ControlPlaneSettings

_REQUEST_ID_RE = re.compile(r"^[a-z0-9][a-z0-9-]{7,79}$")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("action", choices=("inventory", "apply"))
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--requested-by")
    parser.add_argument("--request-id")
    parser.add_argument("--approved-inventory-digest")
    parser.add_argument("--output", type=Path)
    return parser


def _write(document: dict[str, object], output: Path | None) -> None:
    rendered = json.dumps(document, sort_keys=True, separators=(",", ":")) + "\n"
    if output is None:
        sys.stdout.write(rendered)
        return
    output.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    descriptor = os.open(output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
        handle.write(rendered)
        handle.flush()
        os.fsync(handle.fileno())


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    mutation_values = (args.requested_by, args.request_id, args.approved_inventory_digest)
    if args.action == "inventory" and any(mutation_values):
        raise SystemExit("inventory does not accept mutation authority")
    if args.action == "apply" and not all(mutation_values):
        raise SystemExit(
            "apply requires --requested-by, --request-id, and --approved-inventory-digest"
        )
    if args.action == "apply" and (
        not isinstance(args.requested_by, str)
        or not args.requested_by.strip()
        or args.requested_by != args.requested_by.strip()
        or not isinstance(args.request_id, str)
        or _REQUEST_ID_RE.fullmatch(args.request_id) is None
    ):
        raise SystemExit("apply mutation identity is invalid")

    settings = ControlPlaneSettings()
    engine = create_engine(settings.db_engine_url, connect_args=settings.db_engine_connect_args)
    try:
        scope = GcScope(environment="staging", namespace=args.namespace)
        bootstrap = SqlAlchemyLifecycleBootstrap(engine)
        plan = bootstrap.inventory(scope=scope)
        before_digest = plan.inventory_digest
        applied = False
        if args.action == "apply":
            assert args.approved_inventory_digest is not None
            plan = bootstrap.apply(
                plan=plan,
                approved_inventory_digest=args.approved_inventory_digest,
            )
            applied = before_digest != plan.inventory_digest
        document = lifecycle_bootstrap_plan_document(plan)
        document.update(
            {
                "action": args.action,
                "applied": applied,
                "approved_inventory_digest": (
                    args.approved_inventory_digest if args.action == "apply" else None
                ),
                "request_id": args.request_id if args.action == "apply" else None,
                "requested_by": args.requested_by if args.action == "apply" else None,
            }
        )
        _write(document, args.output)
    finally:
        engine.dispose()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
