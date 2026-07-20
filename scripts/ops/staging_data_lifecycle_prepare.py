#!/usr/bin/env python3
"""Inventory or prepare the exact staging lifecycle schema and epoch authority."""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path

from loom.data_lifecycle_gc import GcScope
from loom.data_lifecycle_prepare import (
    LifecycleSourceIdentity,
    SqlAlchemyLifecyclePreparer,
    lifecycle_prepare_plan_document,
    verify_lifecycle_source,
)
from loom.data_lifecycle_runtime import (
    build_lifecycle_engine,
    load_lifecycle_database_runtime,
)
from loom_cli.rollout.migration_readiness import inspect_migration_plan

_REQUEST_ID_RE = re.compile(r"^[a-z0-9][a-z0-9-]{7,79}$")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("action", choices=("inventory", "apply"))
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--source-sha", required=True)
    parser.add_argument("--source-tree", required=True)
    parser.add_argument("--approved-base-sha", required=True)
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
    mutation = (args.requested_by, args.request_id, args.approved_inventory_digest)
    if args.action == "inventory" and any(mutation):
        raise SystemExit("inventory does not accept mutation authority")
    if args.action == "apply" and (
        not all(mutation)
        or not isinstance(args.requested_by, str)
        or args.requested_by != args.requested_by.strip()
        or not args.requested_by
        or not isinstance(args.request_id, str)
        or _REQUEST_ID_RE.fullmatch(args.request_id) is None
    ):
        raise SystemExit(
            "apply requires valid --requested-by, --request-id, and --approved-inventory-digest"
        )
    root = Path(__file__).resolve().parents[2]
    migration_policy = root / "config" / "staging-migration-policy.json"
    engine = build_lifecycle_engine(load_lifecycle_database_runtime())
    try:
        source = LifecycleSourceIdentity(
            candidate_sha=args.source_sha,
            candidate_tree=args.source_tree,
            approved_base_sha=args.approved_base_sha,
        )
        verify_lifecycle_source(root, source)
        migration = inspect_migration_plan(
            root / "migrations" / "alembic.ini",
            policy_path=migration_policy,
        )
        preparer = SqlAlchemyLifecyclePreparer(
            engine,
            alembic_config_path=root / "migrations" / "alembic.ini",
            source=source,
            migration_policy_sha256=migration.policy_digest,
            migration_plan_sha256=migration.plan_digest,
            migration_target_revision=migration.head,
        )
        scope = GcScope(environment="staging", namespace=args.namespace)
        plan = preparer.inventory(scope=scope)
        before_digest = plan.inventory_digest
        if args.action == "apply":
            assert args.approved_inventory_digest is not None
            # The inventory and mutation must consume one unchanged source and
            # migration graph.  The checkout is root-owned in production, but
            # re-read both authorities here rather than relying on that policy
            # as a time-of-check/time-of-use shortcut.
            verify_lifecycle_source(root, source)
            current_migration = inspect_migration_plan(
                root / "migrations" / "alembic.ini",
                policy_path=migration_policy,
            )
            if current_migration != migration:
                raise SystemExit("lifecycle preparation migration authority drifted")
            plan = preparer.apply(
                plan=plan,
                approved_inventory_digest=args.approved_inventory_digest,
            )
        document = lifecycle_prepare_plan_document(plan)
        document.update(
            {
                "action": args.action,
                "approved_inventory_digest": (
                    args.approved_inventory_digest if args.action == "apply" else None
                ),
                "before_inventory_digest": before_digest,
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
