"""`python -m loom_benchmark_tool <subcommand>`."""

from __future__ import annotations

import argparse
import asyncio
import os
from pathlib import Path

from loom.trajectory.storage import MinioObjectStore
from loom_benchmark_tool.import_cmd import run_import
from loom_benchmark_tool.list_cmd import run_list
from loom_benchmark_tool.publish_cmd import run_publish
from loom_benchmark_tool.register_cmd import run_register
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

    p_publish = sub.add_parser(
        "publish",
        help=(
            "Convert + push a benchmark's task bundles to a HuggingFace "
            "dataset repo. Loom-team-side operation; run once per "
            "benchmark per release."
        ),
    )
    p_publish.add_argument("benchmark")
    p_publish.add_argument(
        "--hf-org", default=os.environ.get("LOOM_HF_ORG", "PRHW"),
        help=(
            "HF namespace to publish under (default: env LOOM_HF_ORG, "
            "falling back to 'PRHW')."
        ),
    )
    p_publish.add_argument(
        "--hf-token", default=os.environ.get("HF_TOKEN"),
        help="HF write token (env: HF_TOKEN). Required.",
    )
    p_publish.add_argument(
        "--cache-dir", type=Path,
        default=Path(
            os.environ.get(
                "LOOM_BENCHMARK_CACHE", "/tmp/loom-benchmark-cache",
            ),
        ),
    )
    p_publish.add_argument("--limit", type=int, default=None)
    p_publish.add_argument(
        "--private", action="store_true",
        help="Make the HF dataset private (default: public).",
    )
    p_publish.add_argument("--refresh", action="store_true")

    p_register = sub.add_parser(
        "register",
        help=(
            "Read a benchmark's manifest from HF Hub and upsert task "
            "rows pointing at hf:// URLs. Per-deploy operation — runs "
            "in ~1s per benchmark, no upstream fetch, no MinIO upload."
        ),
    )
    p_register.add_argument("benchmark")
    p_register.add_argument(
        "--hf-org", default=os.environ.get("LOOM_HF_ORG", "PRHW"),
    )
    p_register.add_argument(
        "--hf-token", default=os.environ.get("HF_TOKEN"),
        help="HF read token (optional for public datasets).",
    )
    p_register.add_argument(
        "--db-url", default=os.environ.get("LOOM_DB_URL"),
    )
    p_register.add_argument(
        "--revision", default="main",
        help="HF dataset revision (default: main).",
    )
    p_register.add_argument(
        "--registered-by", default=None,
        help="Label written to benchmarks.imported_by for audit.",
    )

    p_verify = sub.add_parser("verify")
    p_verify.add_argument("benchmark")
    p_verify.add_argument("--limit", type=int, default=10)
    p_verify.add_argument(
        "--minio-endpoint",
        default=os.environ.get("LOOM_MINIO_ENDPOINT"),
    )
    p_verify.add_argument(
        "--minio-access-key",
        default=os.environ.get("LOOM_MINIO_ACCESS_KEY"),
    )
    p_verify.add_argument(
        "--minio-secret-key",
        default=os.environ.get("LOOM_MINIO_SECRET_KEY"),
    )
    p_verify.add_argument("--bucket", default="loom-benchmarks")
    p_verify.add_argument("--seed", type=int, default=0)

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
    if args.cmd == "publish":
        if not args.hf_token:
            p.error("publish requires --hf-token / env HF_TOKEN")
        result = run_publish(
            benchmark=args.benchmark,
            hf_org=args.hf_org,
            hf_token=args.hf_token,
            cache_dir=args.cache_dir,
            limit=args.limit,
            private=args.private,
            refresh=args.refresh,
        )
        print(
            f"publish {args.benchmark}: "
            f"published={result['published']} "
            f"warnings={result['warnings']} "
            f"repo={result['repo_id']} "
            f"rev={result['revision']}",
        )
        return
    if args.cmd == "register":
        if not args.db_url:
            p.error("register requires --db-url / env LOOM_DB_URL")
        result_reg = asyncio.run(run_register(
            benchmark=args.benchmark,
            hf_org=args.hf_org,
            hf_token=args.hf_token,
            db_url=args.db_url,
            revision=args.revision,
            registered_by=args.registered_by,
        ))
        print(
            f"register {args.benchmark}: "
            f"registered={result_reg['registered']} "
            f"skipped={result_reg['skipped']} "
            f"repo={result_reg['repo_id']} "
            f"rev={result_reg['revision']}",
        )
        return
    if args.cmd == "verify":
        missing = [
            f for f, v in (
                ("--minio-endpoint / LOOM_MINIO_ENDPOINT", args.minio_endpoint),
                ("--minio-access-key / LOOM_MINIO_ACCESS_KEY", args.minio_access_key),
                ("--minio-secret-key / LOOM_MINIO_SECRET_KEY", args.minio_secret_key),
            ) if not v
        ]
        if missing:
            p.error(f"verify requires: {', '.join(missing)}")
        store = MinioObjectStore(
            endpoint_url=args.minio_endpoint,
            access_key=args.minio_access_key,
            secret_key=args.minio_secret_key,
        )
        report = asyncio.run(run_verify(
            benchmark=args.benchmark,
            object_store=store,
            bucket=args.bucket,
            limit=args.limit,
            seed=args.seed,
        ))
        print(
            f"verify {args.benchmark}: "
            f"total={report['total']} "
            f"passed={report['passed']} "
            f"failed={report['failed']}",
        )
        for r in report["results"]:
            if not r["passed"]:
                print(f"  FAIL {r['task_id']}: {r['stderr_tail']}")
        if report["total"] == 0:
            # An operator running `verify` against the wrong benchmark
            # name (or before any import) should not see a silent
            # green. Exit non-zero so CI / runbooks catch the typo.
            print(
                f"  WARNING: no tasks found for benchmark "
                f"{args.benchmark!r} under bucket {args.bucket!r}",
            )
            raise SystemExit(2)
        if report["failed"] > 0:
            raise SystemExit(1)
        return


if __name__ == "__main__":
    main()
