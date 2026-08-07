#!/usr/bin/env python3
"""Reconcile the known dirty pre-bootstrap staging mutation epoch."""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path

from loom.data_lifecycle_dirty_epoch_reconcile import (
    SqlAlchemyDirtyEpochReconciler,
    dirty_epoch_reconcile_plan_document,
)
from loom.data_lifecycle_gc import GcScope
from loom.data_lifecycle_runtime import (
    build_lifecycle_engine,
    load_lifecycle_database_runtime,
)

_REQUEST_ID_RE = re.compile(r"^[a-z0-9][a-z0-9-]{7,79}$")
_REQUESTED_BY_RE = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,63}$")
_DIGEST_RE = re.compile(r"^[0-9a-f]{64}$")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("action", choices=("inventory", "apply"))
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--requested-by")
    parser.add_argument("--request-id")
    parser.add_argument("--approved-inventory-digest")
    parser.add_argument("--output", type=Path)
    return parser


def _reserve_output(output: Path | None) -> int | None:
    if output is None:
        return None
    output.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    return os.open(output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)


def _write(document: dict[str, object], descriptor: int | None) -> None:
    rendered = json.dumps(document, sort_keys=True, separators=(",", ":")) + "\n"
    if descriptor is None:
        sys.stdout.write(rendered)
        return
    payload = rendered.encode()
    offset = 0
    while offset < len(payload):
        written = os.write(descriptor, payload[offset:])
        if written < 1:
            raise OSError("operator evidence write made no progress")
        offset += written
    os.fsync(descriptor)


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
        or _REQUESTED_BY_RE.fullmatch(args.requested_by) is None
        or not isinstance(args.request_id, str)
        or _REQUEST_ID_RE.fullmatch(args.request_id) is None
        or not isinstance(args.approved_inventory_digest, str)
        or _DIGEST_RE.fullmatch(args.approved_inventory_digest) is None
    ):
        raise SystemExit("apply mutation identity is invalid")

    output_descriptor = _reserve_output(args.output)
    engine = None
    mutation_committed = False
    try:
        engine = build_lifecycle_engine(load_lifecycle_database_runtime())
        scope = GcScope(environment="staging", namespace=args.namespace)
        reconciler = SqlAlchemyDirtyEpochReconciler(engine)
        plan = reconciler.inventory(scope=scope)
        document = dirty_epoch_reconcile_plan_document(plan)
        document.update(
            {
                "action": args.action,
                "applied": False,
                "approved_inventory_digest": (
                    args.approved_inventory_digest if args.action == "apply" else None
                ),
                "request_id": args.request_id if args.action == "apply" else None,
                "requested_by": args.requested_by if args.action == "apply" else None,
            }
        )
        if args.action == "apply":
            assert isinstance(args.approved_inventory_digest, str)
            assert isinstance(args.request_id, str)
            state = reconciler.apply(
                plan=plan,
                approved_inventory_digest=args.approved_inventory_digest,
                request_id=args.request_id,
            )
            mutation_committed = True
            document.update(
                {
                    "applied": True,
                    "epoch": state.epoch,
                    "epoch_evidence_sha256": state.evidence_sha256,
                    "epoch_mutation_class": state.mutation_class.value,
                    "epoch_updated_at": state.updated_at.isoformat(),
                }
            )
        try:
            _write(document, output_descriptor)
        except OSError:
            if mutation_committed:
                sys.stderr.write(
                    "error: database mutation succeeded but operator evidence output failed\n"
                )
                return 3
            raise
    finally:
        if engine is not None:
            engine.dispose()
        if output_descriptor is not None:
            os.close(output_descriptor)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
