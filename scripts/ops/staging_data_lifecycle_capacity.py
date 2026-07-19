#!/usr/bin/env python3
"""Publish exact staging object/disk capacity for runtime admission."""

from __future__ import annotations

import argparse
import json
import os
from datetime import UTC, datetime
from pathlib import Path

from sqlalchemy import create_engine

from loom.data_lifecycle_capacity import collect_staging_capacity
from loom.data_lifecycle_capacity_sql import SqlAlchemyStagingCapacityStore
from loom.data_lifecycle_inventory_s3 import S3ObservedObjectInventory
from loom.storage_credentials import build_s3_client
from loom_control_plane.config import ControlPlaneSettings


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--bucket", action="append", required=True)
    parser.add_argument("--filesystem-path", action="append", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    return parser


def _publish_output(path: Path, document: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
        json.dump(document, handle, sort_keys=True, separators=(",", ":"))
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())


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
        observed_at = datetime.now(UTC)
        objects = S3ObservedObjectInventory(client).load(buckets=args.bucket)
        evidence = collect_staging_capacity(
            namespace=args.namespace,
            objects=objects,
            filesystem_paths=args.filesystem_path,
            observed_at=observed_at,
        )
        SqlAlchemyStagingCapacityStore(engine).publish(evidence)
        _publish_output(
            args.output,
            {
                "schema_version": 1,
                "environment": "staging",
                "namespace": evidence.namespace,
                "observed_at": evidence.observed_at.isoformat(),
                "source": evidence.source,
                "object_count": evidence.capacity.object_count,
                "bytes_used": evidence.capacity.bytes_used,
                "disk_free_percent": evidence.capacity.disk_free_percent,
                "inode_free_percent": evidence.capacity.inode_free_percent,
                "gc_required": evidence.capacity.gc_required,
                "admission_allowed": evidence.capacity.admission_allowed,
                "policy_sha256": evidence.policy_sha256,
                "evidence_sha256": evidence.evidence_sha256,
            },
        )
    finally:
        client.close()
        engine.dispose()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
