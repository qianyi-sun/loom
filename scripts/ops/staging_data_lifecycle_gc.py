#!/usr/bin/env python3
"""Inventory or execute exact two-phase staging execution-data GC."""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID, uuid4

from loom.data_lifecycle_gc import GcScope
from loom.data_lifecycle_gc_s3 import S3ExactObjectDeleter
from loom.data_lifecycle_gc_sql import SqlAlchemyGcJournal
from loom.data_lifecycle_inventory_s3 import (
    ReconcilingLifecycleInventory,
    S3ObservedObjectInventory,
)
from loom.data_lifecycle_inventory_sql import SqlAlchemyLifecycleInventory
from loom.data_lifecycle_operator import (
    LifecycleOperatorRequest,
    OperatorAction,
    run_lifecycle_operator,
)
from loom.data_lifecycle_runtime import (
    build_lifecycle_engine,
    build_lifecycle_object_store_client,
    load_lifecycle_runtime,
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument(
        "action",
        choices=tuple(action.value for action in OperatorAction),
    )
    parser.add_argument("--namespace", required=True)
    parser.add_argument(
        "--bucket",
        action="append",
        required=True,
        help="Exact staging execution bucket to reconcile; repeat for each allowlisted bucket.",
    )
    parser.add_argument("--requested-by", required=True)
    parser.add_argument("--approved-inventory-digest")
    parser.add_argument("--request-id")
    parser.add_argument("--resume-run-id", type=UUID)
    parser.add_argument("--output", type=Path)
    return parser


def _write_document(document: dict[str, object], output: Path | None) -> None:
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
    request_id = args.request_id
    if args.action == OperatorAction.AUTO and request_id is None:
        request_id = f"req-gc-auto-{uuid4().hex[:16]}"
    runtime = load_lifecycle_runtime()
    engine = build_lifecycle_engine(runtime.database)
    client = build_lifecycle_object_store_client(runtime.object_store)
    try:
        document = run_lifecycle_operator(
            request=LifecycleOperatorRequest(
                action=OperatorAction(args.action),
                requested_by=args.requested_by,
                now=datetime.now(UTC),
                approved_inventory_digest=args.approved_inventory_digest,
                request_id=request_id,
                resume_run_id=args.resume_run_id,
            ),
            scope=GcScope(environment="staging", namespace=args.namespace),
            inventory=ReconcilingLifecycleInventory(
                SqlAlchemyLifecycleInventory(engine),
                S3ObservedObjectInventory(client),
                buckets=args.bucket,
            ),
            journal=SqlAlchemyGcJournal(engine),
            object_deleter=S3ExactObjectDeleter(client),
            batch_size=1000,
        )
        _write_document(document, args.output)
    finally:
        client.close()
        engine.dispose()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
