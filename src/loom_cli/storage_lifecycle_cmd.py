"""Validate, preview and apply S3-compatible storage lifecycle policies."""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Any


def bootstrap_storage_lifecycle(args: argparse.Namespace) -> int:
    """Apply storage retention rules from storage-lifecycle.toml.

    Reads the operator-managed config, renders provider-neutral rules
    into the S3 LifecycleConfiguration dict shape, and applies via
    boto3. Re-applying the same config is a no-op at the storage layer.

    Print-only mode (``--dry-run``) emits the rendered rules to stdout
    as JSON so operators can inspect before mutating the live store.
    """
    import json

    from loom.storage_credentials import (
        UnsupportedAuthKindError,
        build_s3_client,
    )
    from loom.storage_retention import (
        S3_COMPATIBLE_BACKENDS as _S3_COMPATIBLE_BACKENDS,
    )
    from loom.storage_retention import (
        apply_lifecycle_to_s3,
    )
    from loom.storage_retention_loader import load_retention_config

    config_path = Path(args.config)
    try:
        cfg = load_retention_config(config_path)
    except (FileNotFoundError, ValueError) as exc:
        sys.stderr.write(f"error: {exc}\n")
        return 2

    if args.dry_run:
        rendered: dict[str, Any] = {}
        for bucket in sorted({r.bucket for r in cfg.rules}):
            from loom.storage_retention import render_bucket_lifecycle

            rb = render_bucket_lifecycle(cfg, bucket=bucket)
            if rb["Rules"]:
                rendered[bucket] = rb
        json.dump({"backend": cfg.backend, "lifecycle": rendered}, sys.stdout, indent=2)
        sys.stdout.write("\n")
        return 0

    # Live apply path. Reuse the service settings for credentials so
    # operators don't have to plumb separate auth for this subcommand.
    backend = os.environ.get("LOOM_SVC_STORAGE_BACKEND", "minio")
    if backend not in _S3_COMPATIBLE_BACKENDS:
        sys.stderr.write(
            f"error: storage_backend={backend!r} is not S3-compatible. "
            "S3-compatible: minio | s3 | r2 | b2 | wasabi. "
            "Live lifecycle apply through this command supports only "
            "S3-compatible backends.\n",
        )
        return 2
    # The requested policy must match the configured storage backend.
    if cfg.backend != backend:
        sys.stderr.write(
            f"error: storage_backend={backend!r} (from env) does not "
            f"match storage-lifecycle.toml backend={cfg.backend!r}. "
            "Set LOOM_SVC_STORAGE_BACKEND to the cluster's actual "
            "backend before running this command.\n",
        )
        return 2

    auth_kind = os.environ.get("LOOM_SVC_STORAGE_AUTH_KIND", "static_keys")
    endpoint = args.endpoint or os.environ.get(
        "LOOM_SVC_MINIO_ENDPOINT",
        "http://loom-minio:9000",
    )
    region = os.environ.get("LOOM_SVC_MINIO_REGION", "us-east-1")
    access_key = os.environ.get(
        "LOOM_SVC_MINIO_ACCESS_KEY",
    ) or os.environ.get("MINIO_ROOT_USER", "")
    secret_key = os.environ.get(
        "LOOM_SVC_MINIO_SECRET_KEY",
    ) or os.environ.get("MINIO_ROOT_PASSWORD", "")

    try:
        s3 = build_s3_client(
            endpoint_url=endpoint,
            auth_kind=auth_kind,
            access_key=access_key,
            secret_key=secret_key,
            region=region,
        )
    except UnsupportedAuthKindError as exc:
        sys.stderr.write(f"error: {exc}\n")
        return 2
    except ValueError as exc:
        # static_keys path with missing creds → surface the helpful
        # env-var hint.
        sys.stderr.write(
            f"error: {exc} (or set MINIO_ROOT_USER + MINIO_ROOT_PASSWORD).\n",
        )
        return 2

    try:
        applied = apply_lifecycle_to_s3(s3, cfg)
    finally:
        s3.close()

    sys.stdout.write(
        f"Applied storage lifecycle rules to {len(applied)} bucket(s):\n",
    )
    for bucket, rendered in sorted(applied.items()):
        n = len(rendered["Rules"])
        sys.stdout.write(f"  {bucket}: {n} rule(s)\n")
    if not applied:
        sys.stdout.write(
            "  (no rules applied — every bucket resolved to keep_forever "
            "or had no matching rules).\n",
        )
    return 0
