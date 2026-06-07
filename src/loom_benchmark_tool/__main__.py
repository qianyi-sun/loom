"""`python -m loom_benchmark_tool <subcommand>`."""

from __future__ import annotations

import argparse
import asyncio
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
    p_import.add_argument("--db-url", required=True)
    p_import.add_argument("--minio-endpoint", required=True)
    p_import.add_argument("--minio-access-key", required=True)
    p_import.add_argument("--minio-secret-key", required=True)
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
