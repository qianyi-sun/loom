#!/usr/bin/env python3
"""Publish capacity and run one exact automatic staging lifecycle GC cycle."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from sqlalchemy import create_engine

from loom.data_lifecycle_capacity import collect_staging_capacity
from loom.data_lifecycle_capacity_sql import SqlAlchemyStagingCapacityStore
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
from loom.storage_credentials import build_s3_client
from loom_control_plane.config import ControlPlaneSettings


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--bucket", action="append", required=True)
    parser.add_argument("--filesystem-path", type=Path, required=True)
    parser.add_argument("--requested-by", default="staging-lifecycle-timer")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
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
        object_inventory = S3ObservedObjectInventory(client)
        observed = object_inventory.load(buckets=args.bucket)
        capacity = collect_staging_capacity(
            namespace=args.namespace,
            objects=observed,
            filesystem_path=args.filesystem_path,
            observed_at=now,
        )
        SqlAlchemyStagingCapacityStore(engine).publish(capacity)
        document = run_lifecycle_operator(
            request=LifecycleOperatorRequest(
                action=OperatorAction.AUTO,
                requested_by=args.requested_by,
                now=now,
                request_id=f"req-gc-auto-{uuid4().hex[:16]}",
            ),
            scope=GcScope(environment="staging", namespace=args.namespace),
            inventory=ReconcilingLifecycleInventory(
                SqlAlchemyLifecycleInventory(engine),
                object_inventory,
                buckets=args.bucket,
            ),
            journal=SqlAlchemyGcJournal(engine),
            object_deleter=S3ExactObjectDeleter(client),
        )
        print(
            json.dumps(
                {
                    "schema_version": 1,
                    "capacity": {
                        "admission_allowed": capacity.capacity.admission_allowed,
                        "bytes_used": capacity.capacity.bytes_used,
                        "disk_free_percent": capacity.capacity.disk_free_percent,
                        "evidence_sha256": capacity.evidence_sha256,
                        "gc_required": capacity.capacity.gc_required,
                        "inode_free_percent": capacity.capacity.inode_free_percent,
                        "object_count": capacity.capacity.object_count,
                        "observed_at": capacity.observed_at.isoformat(),
                        "policy_sha256": capacity.policy_sha256,
                    },
                    "gc": document,
                },
                sort_keys=True,
                separators=(",", ":"),
            ),
            flush=True,
        )
    finally:
        client.close()
        engine.dispose()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
