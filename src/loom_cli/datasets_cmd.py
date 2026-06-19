"""`loom datasets <subcommand>` — discovery + lifecycle commands.

Subcommands:
- list [--installed | --available | --remote] [--json]
- show <slug>
- install <slug>
- refresh-catalog
- import <slug> [--db-url --minio-* --bucket --cache-dir --limit ...]
- publish <slug> [--hf-org --hf-token --cache-dir --limit --private]
- register <slug> [--hf-org --hf-token --db-url --revision]
- verify <slug> [--limit --minio-* --bucket --seed]

The {import, publish, register, verify} subcommands were previously
shipped as `python -m loom_benchmark_tool <cmd>`. Folded into
`loom datasets` here so operators have one CLI to learn instead of
guessing which tool owns which verb. The old `loom_benchmark_tool`
entry-point stays as a deprecation shim — see its module docstring.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from collections.abc import Callable
from pathlib import Path

from loom_cli import builtin as builtin_mod
from loom_cli import catalog as catalog_mod
from loom_cli import install as install_mod
from loom_cli import remote as remote_mod
from loom_cli.discovery import DatasetEntry, union_entries
from loom_cli.output import render_datasets_json, render_datasets_table


def _add_import_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("benchmark")
    # Each secret falls back to an env var so operators don't have to
    # leak them through `ps`/shell history. CLI flag wins when both set.
    p.add_argument("--db-url", default=os.environ.get("LOOM_DB_URL"))
    p.add_argument(
        "--minio-endpoint",
        default=os.environ.get("LOOM_MINIO_ENDPOINT"),
    )
    p.add_argument(
        "--minio-access-key",
        default=os.environ.get("LOOM_MINIO_ACCESS_KEY"),
    )
    p.add_argument(
        "--minio-secret-key",
        default=os.environ.get("LOOM_MINIO_SECRET_KEY"),
    )
    p.add_argument("--bucket", default="loom-benchmarks")
    p.add_argument(
        "--cache-dir", type=Path, default=Path("/tmp/loom-benchmark-cache"),
    )
    p.add_argument("--limit", type=int, default=None)
    p.add_argument("--imported-by", default=None)
    p.add_argument("--refresh", action="store_true")


def _add_publish_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("benchmark")
    p.add_argument(
        "--hf-org", default=os.environ.get("LOOM_HF_ORG", "PRHW"),
        help="HF namespace to publish under (default: env LOOM_HF_ORG, falling back to 'PRHW').",
    )
    p.add_argument(
        "--hf-token", default=os.environ.get("HF_TOKEN"),
        help="HF write token (env: HF_TOKEN). Required.",
    )
    p.add_argument(
        "--cache-dir", type=Path,
        default=Path(
            os.environ.get(
                "LOOM_BENCHMARK_CACHE", "/tmp/loom-benchmark-cache",
            ),
        ),
    )
    p.add_argument("--limit", type=int, default=None)
    p.add_argument("--private", action="store_true")
    p.add_argument("--refresh", action="store_true")


def _add_register_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("benchmark")
    p.add_argument(
        "--hf-org", default=os.environ.get("LOOM_HF_ORG", "PRHW"),
    )
    p.add_argument(
        "--hf-token", default=os.environ.get("HF_TOKEN"),
        help="HF read token (optional for public datasets).",
    )
    p.add_argument("--db-url", default=os.environ.get("LOOM_DB_URL"))
    p.add_argument("--revision", default="main")
    p.add_argument("--registered-by", default=None)


def _add_verify_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("benchmark")
    p.add_argument("--limit", type=int, default=10)
    p.add_argument(
        "--minio-endpoint",
        default=os.environ.get("LOOM_MINIO_ENDPOINT"),
    )
    p.add_argument(
        "--minio-access-key",
        default=os.environ.get("LOOM_MINIO_ACCESS_KEY"),
    )
    p.add_argument(
        "--minio-secret-key",
        default=os.environ.get("LOOM_MINIO_SECRET_KEY"),
    )
    p.add_argument("--bucket", default="loom-benchmarks")
    p.add_argument("--seed", type=int, default=0)


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="loom datasets")
    sub = p.add_subparsers(dest="subcmd", required=True)

    p_list = sub.add_parser("list")
    filt = p_list.add_mutually_exclusive_group()
    filt.add_argument("--installed", action="store_true")
    filt.add_argument("--available", action="store_true")
    filt.add_argument("--remote", action="store_true")
    p_list.add_argument("--json", action="store_true", dest="as_json")
    p_list.add_argument("--catalog-url", default=None)
    p_list.add_argument("--server-url", default=None)
    p_list.add_argument("--token", default=None)

    p_show = sub.add_parser("show")
    p_show.add_argument("slug")
    p_show.add_argument("--catalog-url", default=None)
    p_show.add_argument("--server-url", default=None)
    p_show.add_argument("--token", default=None)

    p_install = sub.add_parser("install")
    p_install.add_argument("slug")
    p_install.add_argument("--catalog-url", default=None)

    sub.add_parser("refresh-catalog")

    # Folded-in benchmark-tool subcommands.
    _add_import_args(sub.add_parser(
        "import",
        help="Convert a benchmark's tasks + upload to MinIO + insert task rows.",
    ))
    _add_publish_args(sub.add_parser(
        "publish",
        help="Convert + push a benchmark to a HuggingFace dataset repo (Loom-team operation).",
    ))
    _add_register_args(sub.add_parser(
        "register",
        help="Read a benchmark's HF manifest + upsert task rows pointing at hf:// URLs.",
    ))
    _add_verify_args(sub.add_parser(
        "verify",
        help="Sample tasks from a benchmark + run the oracle agent end-to-end.",
    ))

    p_sync = sub.add_parser(
        "sync-config",
        help="Sync config/benchmarks.toml into the benchmarks + tasks tables (issue #234).",
    )
    p_sync.add_argument(
        "--config", type=Path, default=None,
        help="Path to benchmarks.toml. Defaults to "
        "$LOOM_BENCHMARKS_CONFIG_PATH, then ./config/benchmarks.toml, "
        "then /etc/loom/benchmarks.toml.",
    )
    p_sync.add_argument(
        "--fixtures-root", type=Path, default=None,
        help="Override the worker fixtures_root used to resolve "
        "[[local]] entries. Defaults to $LOOM_WORKER_FIXTURES_ROOT.",
    )
    p_sync.add_argument(
        "--db-url", default=os.environ.get("LOOM_DB_URL"),
        help="Postgres URL (defaults to env LOOM_DB_URL).",
    )
    p_sync.add_argument(
        "--dry-run", action="store_true",
        help="Compute the plan + print it without writing to the DB.",
    )

    return p


def _gather(
    *,
    only: str | None,
    catalog_url: str | None,
    server_url: str | None,
    token: str | None,
) -> list[DatasetEntry]:
    builtin = builtin_mod.load_builtin_entries() if only in (None, "installed") else []
    if only in (None, "available"):
        try:
            catalog = catalog_mod.load_catalog_entries(url=catalog_url)
        except catalog_mod.CatalogFetchError as exc:
            print(f"warning: catalog fetch failed: {exc}", file=sys.stderr)
            catalog = []
    else:
        catalog = []
    remote = (
        remote_mod.load_remote_entries(server_url=server_url, token=token)
        if only in (None, "remote") else []
    )
    return union_entries(builtin=builtin, catalog=catalog, remote=remote)


def _cmd_list(args: argparse.Namespace) -> int:
    only: str | None = None
    if args.installed:
        only = "installed"
    elif args.available:
        only = "available"
    elif args.remote:
        only = "remote"
    entries = _gather(
        only=only, catalog_url=args.catalog_url,
        server_url=args.server_url, token=args.token,
    )
    if args.as_json:
        print(render_datasets_json(entries))
    else:
        print(render_datasets_table(entries))
    return 0


def _cmd_show(args: argparse.Namespace) -> int:
    entries = _gather(
        only=None, catalog_url=args.catalog_url,
        server_url=args.server_url, token=args.token,
    )
    match = next((e for e in entries if e.slug == args.slug), None)
    if match is None:
        print(f"error: dataset {args.slug!r} not found", file=sys.stderr)
        return 2
    print(f"slug:           {match.slug}")
    print(f"display_name:   {match.display_name}")
    print(f"source:         {match.source}")
    print(f"upstream_kind:  {match.upstream_kind or '-'}")
    print(f"status:         {match.status}")
    print(f"license:        {match.license_spdx}")
    print(f"license_url:    {match.license_url}")
    print(f"task_count:     {match.task_count if match.task_count is not None else '-'}")
    if match.entry_point:
        print(f"entry_point:    {match.entry_point}")
    if match.available_pip_spec:
        print(f"pip_spec:       {match.available_pip_spec}")
    return 0


def _cmd_install(args: argparse.Namespace) -> int:
    entries = _gather(
        only=None, catalog_url=args.catalog_url,
        server_url=None, token=None,
    )
    match = next((e for e in entries if e.slug == args.slug), None)
    if match is None or not match.available_pip_spec:
        print(
            f"error: dataset {args.slug!r} not found in catalog "
            "(no pip spec available)",
            file=sys.stderr,
        )
        return 2
    try:
        output = install_mod.install_dataset(pip_spec=match.available_pip_spec)
    except install_mod.InstallError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(output)
    return 0


def _cmd_refresh(_args: argparse.Namespace) -> int:
    catalog_mod.purge_catalog_cache()
    print("catalog cache purged")
    return 0


def _cmd_import(args: argparse.Namespace) -> int:
    from loom.trajectory.storage import MinioObjectStore
    from loom_benchmark_tool.import_cmd import run_import

    missing = [
        f for f, v in (
            ("--db-url / LOOM_DB_URL", args.db_url),
            ("--minio-endpoint / LOOM_MINIO_ENDPOINT", args.minio_endpoint),
            ("--minio-access-key / LOOM_MINIO_ACCESS_KEY", args.minio_access_key),
            ("--minio-secret-key / LOOM_MINIO_SECRET_KEY", args.minio_secret_key),
        ) if not v
    ]
    if missing:
        print(f"error: import requires: {', '.join(missing)}", file=sys.stderr)
        return 2
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
    return 0


def _cmd_publish(args: argparse.Namespace) -> int:
    from loom_benchmark_tool.publish_cmd import run_publish

    if not args.hf_token:
        print(
            "error: publish requires --hf-token / env HF_TOKEN",
            file=sys.stderr,
        )
        return 2
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
    return 0


def _cmd_register(args: argparse.Namespace) -> int:
    from loom_benchmark_tool.register_cmd import run_register

    if not args.db_url:
        print(
            "error: register requires --db-url / env LOOM_DB_URL",
            file=sys.stderr,
        )
        return 2
    result = asyncio.run(run_register(
        benchmark=args.benchmark,
        hf_org=args.hf_org,
        hf_token=args.hf_token,
        db_url=args.db_url,
        revision=args.revision,
        registered_by=args.registered_by,
    ))
    print(
        f"register {args.benchmark}: "
        f"registered={result['registered']} "
        f"skipped={result['skipped']} "
        f"repo={result['repo_id']} "
        f"rev={result['revision']}",
    )
    return 0


def _cmd_verify(args: argparse.Namespace) -> int:
    from loom.trajectory.storage import MinioObjectStore
    from loom_benchmark_tool.verify_cmd import run_verify

    missing = [
        f for f, v in (
            ("--minio-endpoint / LOOM_MINIO_ENDPOINT", args.minio_endpoint),
            ("--minio-access-key / LOOM_MINIO_ACCESS_KEY", args.minio_access_key),
            ("--minio-secret-key / LOOM_MINIO_SECRET_KEY", args.minio_secret_key),
        ) if not v
    ]
    if missing:
        print(f"error: verify requires: {', '.join(missing)}", file=sys.stderr)
        return 2
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
        print(
            f"  WARNING: no tasks found for benchmark "
            f"{args.benchmark!r} under bucket {args.bucket!r}",
        )
        return 2
    if report["failed"] > 0:
        return 1
    return 0


def _cmd_sync_config(args: argparse.Namespace) -> int:
    from loom_benchmarks.registry import REGISTRY
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from loom.config.benchmarks import load_benchmarks_config, resolve_config_path
    from loom_cli.benchmarks_sync import (
        SyncError,
        render_plan_table,
        sync,
    )

    config_path = resolve_config_path(args.config)
    if config_path is None:
        if args.config is not None:
            print(
                f"benchmarks.toml not found at {args.config}; nothing to sync",
                file=sys.stderr,
            )
        else:
            print("no benchmarks.toml found; nothing to sync")
        return 0

    try:
        cfg = load_benchmarks_config(config_path)
    except Exception as exc:
        print(f"error: invalid {config_path}: {exc}", file=sys.stderr)
        return 1
    if cfg is None:
        print("no benchmarks.toml found; nothing to sync")
        return 0

    fixtures_root = args.fixtures_root or (
        Path(env) if (env := os.environ.get("LOOM_WORKER_FIXTURES_ROOT")) else None
    )
    if cfg.local and fixtures_root is None:
        print(
            "error: [[local]] entries require --fixtures-root or "
            "$LOOM_WORKER_FIXTURES_ROOT (the directory holding "
            "<benchmark-id>/<task>/ bundles)",
            file=sys.stderr,
        )
        return 2

    if not args.db_url:
        print(
            "error: sync-config requires --db-url / env LOOM_DB_URL",
            file=sys.stderr,
        )
        return 2

    db_url = args.db_url
    if not db_url.startswith("postgresql+"):
        db_url = db_url.replace("postgresql://", "postgresql+psycopg://", 1)

    async def _run() -> int:
        engine = create_async_engine(db_url)
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        try:
            async with session_factory() as session:
                plan = await sync(
                    cfg,
                    fixtures_root=fixtures_root or Path("/"),
                    session=session,
                    registry_names=set(REGISTRY),
                    dry_run=args.dry_run,
                )
        finally:
            await engine.dispose()

        banner = (
            "DRY RUN — no DB writes" if args.dry_run
            else f"synced {config_path}"
        )
        print(banner)
        print(render_plan_table(plan))
        if plan.tasks:
            print()
            for bid, counts in sorted(plan.tasks.items()):
                print(
                    f"  {bid}: {counts.total} tasks "
                    f"(inserted={counts.inserted} "
                    f"updated={counts.updated} "
                    f"unchanged={counts.unchanged})",
                )
        return 0

    try:
        return asyncio.run(_run())
    except SyncError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


_DISPATCH: dict[str, Callable[[argparse.Namespace], int]] = {
    "list": _cmd_list,
    "show": _cmd_show,
    "install": _cmd_install,
    "refresh-catalog": _cmd_refresh,
    "import": _cmd_import,
    "publish": _cmd_publish,
    "register": _cmd_register,
    "verify": _cmd_verify,
    "sync-config": _cmd_sync_config,
}


def dispatch(argv: list[str]) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    handler = _DISPATCH.get(args.subcmd)
    if handler is None:
        parser.error(f"unknown subcommand: {args.subcmd}")  # raises SystemExit
    return handler(args)
