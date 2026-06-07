"""`python -m loom_benchmark_tool <subcommand>`."""

from __future__ import annotations

import argparse
import asyncio
import os
from pathlib import Path

from loom.trajectory.storage import MinioObjectStore
from loom_benchmark_tool.import_cmd import run_import
from loom_benchmark_tool.list_cmd import run_list
from loom_benchmark_tool.verify_cmd import run_verify


def main() -> None:
    p = argparse.ArgumentParser(prog="loom_benchmark_tool")
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("list")

    p_import = sub.add_parser("import")
    p_import.add_argument("benchmark")
    # Each secret falls back to an env var so operators don't have to
    # leak them through `ps`/shell history. CLI flag wins when both set.
    p_import.add_argument(
        "--db-url",
        default=os.environ.get("LOOM_DB_URL"),
    )
    p_import.add_argument(
        "--minio-endpoint",
        default=os.environ.get("LOOM_MINIO_ENDPOINT"),
    )
    p_import.add_argument(
        "--minio-access-key",
        default=os.environ.get("LOOM_MINIO_ACCESS_KEY"),
    )
    p_import.add_argument(
        "--minio-secret-key",
        default=os.environ.get("LOOM_MINIO_SECRET_KEY"),
    )
    p_import.add_argument("--bucket", default="loom-benchmarks")
    p_import.add_argument(
        "--cache-dir", type=Path, default=Path("/tmp/loom-benchmark-cache"),
    )
    p_import.add_argument("--limit", type=int, default=None)
    p_import.add_argument("--imported-by", default=None)
    p_import.add_argument("--refresh", action="store_true")

    p_verify = sub.add_parser("verify")
    p_verify.add_argument("benchmark")
    p_verify.add_argument("--limit", type=int, default=10)

    args = p.parse_args()
    if args.cmd == "list":
        print(run_list())
        return
    if args.cmd == "import":
        missing = [
            f for f, v in (
                ("--db-url / LOOM_DB_URL", args.db_url),
                ("--minio-endpoint / LOOM_MINIO_ENDPOINT", args.minio_endpoint),
                ("--minio-access-key / LOOM_MINIO_ACCESS_KEY", args.minio_access_key),
                ("--minio-secret-key / LOOM_MINIO_SECRET_KEY", args.minio_secret_key),
            ) if not v
        ]
        if missing:
            p.error(f"import requires: {', '.join(missing)}")
        store = MinioObjectStore(
            endpoint_url=args.minio_endpoint,
            access_key=args.minio_access_key,
            secret_key=args.minio_secret_key,
        )
        stats = asyncio.run(run_import(
            benchmark=args.benchmark,
            db_url=args.db_url,
            object_store=store,
            bucket=args.bucket,
            cache_dir=args.cache_dir,
            limit=args.limit,
            imported_by=args.imported_by,
            refresh=args.refresh,
        ))
        print(f"converted={stats['converted']} warnings={stats['warnings']}")
        return
    if args.cmd == "verify":
        asyncio.run(run_verify(benchmark=args.benchmark, limit=args.limit))
        return


if __name__ == "__main__":
    main()
