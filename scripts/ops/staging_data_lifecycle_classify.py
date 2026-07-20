#!/usr/bin/env python3
"""Inventory or apply exact classification of legacy staging execution history."""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path

from sqlalchemy import create_engine

from loom.data_lifecycle_gc import GcScope
from loom.data_lifecycle_legacy import classification_plan_summary
from loom.data_lifecycle_legacy_s3 import S3LegacyObjectInspector
from loom.data_lifecycle_legacy_sql import SqlAlchemyLegacyClassifier
from loom.storage_credentials import build_s3_client
from loom_control_plane.config import ControlPlaneSettings


def _aware_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise argparse.ArgumentTypeError("legacy expiry cutoff must include a timezone")
    return parsed


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("action", choices=("inventory", "apply"))
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--requested-by")
    parser.add_argument("--approved-inventory-digest")
    parser.add_argument("--request-id")
    parser.add_argument("--artifacts-bucket", required=True)
    parser.add_argument("--trajectories-bucket", required=True)
    parser.add_argument("--inspection-workers", type=int, default=32)
    parser.add_argument("--expire-created-before", type=_aware_datetime)
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
    if args.action == "apply" and not all(
        (args.requested_by, args.approved_inventory_digest, args.request_id)
    ):
        raise SystemExit(
            "apply requires --requested-by, --approved-inventory-digest, and --request-id"
        )
    if args.action == "inventory" and any(
        (args.requested_by, args.approved_inventory_digest, args.request_id)
    ):
        raise SystemExit("inventory does not accept mutation authority")

    settings = ControlPlaneSettings()
    engine = create_engine(
        settings.db_engine_url,
        connect_args=settings.db_engine_connect_args,
    )
    client = build_s3_client(
        endpoint_url=settings.minio_endpoint,
        auth_kind=settings.storage_auth_kind,
        access_key=settings.minio_access_key.get_secret_value(),
        secret_key=settings.minio_secret_key.get_secret_value(),
        region=settings.minio_region,
    )
    try:
        now = datetime.now(UTC)
        classifier = SqlAlchemyLegacyClassifier(
            engine,
            S3LegacyObjectInspector(client),
            bucket_aliases={
                "artifacts": args.artifacts_bucket,
                "trajectories": args.trajectories_bucket,
            },
            inspection_workers=args.inspection_workers,
        )
        plan = classifier.inventory(
            scope=GcScope(environment="staging", namespace=args.namespace),
            planned_at=now,
            expire_created_before=args.expire_created_before,
        )
        document = dict(classification_plan_summary(plan))
        document["action"] = args.action
        document["applicable"] = not plan.blockers and bool(plan.rows)
        if args.action == "apply":
            assert args.requested_by is not None
            assert args.approved_inventory_digest is not None
            assert args.request_id is not None
            state = classifier.apply(
                plan=plan,
                approved_inventory_digest=args.approved_inventory_digest,
                request_id=args.request_id,
                applied_at=now,
            )
            document["mutation_epoch_after"] = state.epoch
        _write_document(document, args.output)
    finally:
        client.close()
        engine.dispose()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
