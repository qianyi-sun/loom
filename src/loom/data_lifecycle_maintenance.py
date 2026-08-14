"""Exact staging lifecycle maintenance entry point.

This module lives in the installed ``loom-control-plane`` image.  It uses a
dedicated, least-authority environment contract instead of importing the full
control-plane settings (which would unnecessarily require the JWT signing
secret and unrelated service configuration).
"""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID, uuid4

from loom.data_lifecycle_capacity import (
    collect_staging_capacity,
    collect_staging_capacity_from_drives,
)
from loom.data_lifecycle_capacity_minio import probe_minio_admin_drives
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
from loom.data_lifecycle_runtime import (
    build_lifecycle_engine,
    build_lifecycle_object_store_client,
    load_lifecycle_runtime,
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--action", choices=("auto", "capacity", "resume"), default="auto")
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--bucket", action="append", required=True)
    parser.add_argument(
        "--capacity-source",
        choices=("filesystem", "minio-admin"),
        default="filesystem",
        help=(
            "How to measure per-drive disk/inode headroom. 'filesystem' stats "
            "locally-mounted drive paths (single-node/hostPath MinIO); "
            "'minio-admin' queries the MinIO admin API over the network "
            "(distributed MinIO, whose RWO drive PVCs cannot be co-mounted)."
        ),
    )
    parser.add_argument("--expected-drive-count", type=int, default=None)
    parser.add_argument("--filesystem-path", action="append", type=Path, default=None)
    parser.add_argument("--requested-by", default="staging-lifecycle-cronjob")
    parser.add_argument("--resume-run-id", type=UUID)
    parser.add_argument("--request-id")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.action == "resume":
        if args.resume_run_id is None or args.request_id is None:
            raise RuntimeError("resume requires --resume-run-id and --request-id")
    else:
        if args.resume_run_id is not None or args.request_id is not None:
            raise RuntimeError("auto and capacity do not accept resume authority")
        if args.capacity_source == "minio-admin" and (
            args.expected_drive_count is None or args.expected_drive_count < 1
        ):
            raise RuntimeError(
                "--capacity-source minio-admin requires a positive --expected-drive-count"
            )
        if args.capacity_source == "filesystem" and args.expected_drive_count is not None:
            raise RuntimeError("--capacity-source filesystem cannot use --expected-drive-count")
    runtime = load_lifecycle_runtime()
    engine = build_lifecycle_engine(runtime.database)
    client = build_lifecycle_object_store_client(runtime.object_store)
    try:
        now = datetime.now(UTC)
        object_inventory = S3ObservedObjectInventory(client)
        document: dict[str, object] | None
        if args.action == "resume":
            document = run_lifecycle_operator(
                request=LifecycleOperatorRequest(
                    action=OperatorAction.RESUME,
                    requested_by=args.requested_by,
                    now=now,
                    request_id=args.request_id,
                    resume_run_id=args.resume_run_id,
                ),
                scope=GcScope(environment="staging", namespace=args.namespace),
                inventory=ReconcilingLifecycleInventory(
                    SqlAlchemyLifecycleInventory(engine),
                    object_inventory,
                    buckets=args.bucket,
                ),
                journal=SqlAlchemyGcJournal(engine),
                object_deleter=S3ExactObjectDeleter(client),
                batch_size=1000,
            )
            print(
                json.dumps(
                    {
                        "schema_version": 1,
                        "action": args.action,
                        "capacity": None,
                        "gc": document,
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                ),
                flush=True,
            )
            return 0

        observed = object_inventory.load(buckets=args.bucket)
        if args.capacity_source == "minio-admin":
            store = runtime.object_store
            if store.access_key is None or store.secret_key is None:
                raise RuntimeError(
                    "--capacity-source minio-admin requires static "
                    "MINIO_ACCESS_KEY / MINIO_SECRET_KEY credentials"
                )
            drives = probe_minio_admin_drives(
                endpoint_url=store.endpoint_url,
                access_key=store.access_key,
                secret_key=store.secret_key,
                expected_drive_count=args.expected_drive_count,
            )
            capacity = collect_staging_capacity_from_drives(
                namespace=args.namespace,
                objects=observed,
                drives=drives,
                observed_at=now,
            )
        else:
            if not args.filesystem_path:
                raise RuntimeError(
                    "--capacity-source filesystem requires at least one --filesystem-path"
                )
            capacity = collect_staging_capacity(
                namespace=args.namespace,
                objects=observed,
                filesystem_paths=args.filesystem_path,
                observed_at=now,
            )
        SqlAlchemyStagingCapacityStore(engine).publish(capacity)
        document = None
        if args.action == "auto":
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
                batch_size=1000,
            )
        print(
            json.dumps(
                {
                    "schema_version": 1,
                    "action": args.action,
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


if __name__ == "__main__":  # pragma: no cover - exercised through console execution
    raise SystemExit(main())
