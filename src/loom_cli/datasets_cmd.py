"""`loom datasets <subcommand>` — discovery + lifecycle commands.

Subcommands:
- list [--installed | --available | --remote] [--json]
- show <slug>
- install <slug>
- refresh-catalog
- import <slug> [--db-url --minio-* --bucket --cache-dir --limit ...]
- provision-catalog [--source-db-url --target-db-url --source-minio-* --target-minio-*]
- publish-local <path> [--db-url env:VAR --minio-* --bucket ...]
- publish <slug> [--hf-org --hf-token --cache-dir --limit --private]
- register <slug> [--hf-org --hf-token --db-url --revision --mirror-to-object-store --minio-*]
- verify <slug> [--limit --minio-* --bucket --seed]
- audit [--all | <slug>] [--db-url] [--json]
- hf-boundary-evidence <slug> --environment staging --output PATH

The {import, publish, register, verify} subcommands were previously
shipped as `python -m loom_benchmark_tool <cmd>`. Folded into
`loom datasets` here so operators have one CLI to learn instead of
guessing which tool owns which verb. The old `loom_benchmark_tool`
entry-point stays as a deprecation shim — see its module docstring.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from collections.abc import Callable
from pathlib import Path

from loom_cli import builtin as builtin_mod
from loom_cli import catalog as catalog_mod
from loom_cli import install as install_mod
from loom_cli import remote as remote_mod
from loom_cli.benchmark_readiness import (
    render_readiness_json,
    render_readiness_table,
    run_bundle_presence_audit,
    run_readiness_audit,
)
from loom_cli.discovery import DatasetEntry, union_entries
from loom_cli.output import render_datasets_json, render_datasets_table
from loom_cli.secret_source import SecretSourceError, resolve_secret_source


def _env_first(*names: str) -> str | None:
    for name in names:
        value = os.environ.get(name)
        if value:
            return value
    return None


def _target_db_url() -> str | None:
    return _env_first("LOOM_DB_URL", "LOOM_SVC_DB_URL")


def _target_minio_env(name: str) -> str | None:
    return _env_first(f"LOOM_MINIO_{name}", f"LOOM_SVC_MINIO_{name}")


def _add_import_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("benchmark")
    # Each secret falls back to an env var so operators don't have to
    # leak them through `ps`/shell history. CLI flag wins when both set.
    p.add_argument("--db-url", default=_target_db_url())
    p.add_argument(
        "--minio-endpoint",
        default=_target_minio_env("ENDPOINT"),
    )
    p.add_argument(
        "--minio-access-key",
        default=_target_minio_env("ACCESS_KEY"),
    )
    p.add_argument(
        "--minio-secret-key",
        default=_target_minio_env("SECRET_KEY"),
    )
    p.add_argument("--bucket", default="loom-benchmarks")
    p.add_argument(
        "--cache-dir", type=Path, default=Path("/tmp/loom-benchmark-cache"),
    )
    p.add_argument("--limit", type=int, default=None)
    p.add_argument(
        "--instance-id",
        dest="instance_ids",
        action="append",
        default=None,
        help="Import only the requested adapter instance id. Repeat for multiple ids.",
    )
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
    p.add_argument(
        "--instance-id",
        dest="instance_ids",
        action="append",
        default=None,
        help="Publish only the requested adapter instance id. Repeat for multiple ids.",
    )
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
    p.add_argument(
        "--db-url",
        default=_target_db_url(),
        help=(
            "Postgres URL (defaults to env LOOM_DB_URL, then "
            "LOOM_SVC_DB_URL inside deployed service pods)."
        ),
    )
    p.add_argument("--revision", default="main")
    p.add_argument("--registered-by", default=None)
    p.add_argument(
        "--mirror-to-object-store",
        action="store_true",
        help=(
            "Download HF task bundles with the operator HF token, mirror them "
            "into internal object storage, and register s3:// runtime sources "
            "instead of hf:// worker sources."
        ),
    )
    p.add_argument(
        "--minio-endpoint",
        default=_target_minio_env("ENDPOINT"),
        help=(
            "Target object-store endpoint for --mirror-to-object-store "
            "(env LOOM_MINIO_ENDPOINT, then LOOM_SVC_MINIO_ENDPOINT)."
        ),
    )
    p.add_argument(
        "--minio-access-key",
        default=_target_minio_env("ACCESS_KEY"),
        help=(
            "Target object-store access key for --mirror-to-object-store "
            "(env LOOM_MINIO_ACCESS_KEY, then LOOM_SVC_MINIO_ACCESS_KEY)."
        ),
    )
    p.add_argument(
        "--minio-secret-key",
        default=_target_minio_env("SECRET_KEY"),
        help=(
            "Target object-store secret key for --mirror-to-object-store "
            "(env LOOM_MINIO_SECRET_KEY, then LOOM_SVC_MINIO_SECRET_KEY)."
        ),
    )
    p.add_argument(
        "--bucket",
        default=os.environ.get("LOOM_BENCHMARK_BUCKET", "loom-benchmarks"),
        help="Target object-store bucket for mirrored task bundles.",
    )


def _add_verify_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("benchmark")
    p.add_argument("--limit", type=int, default=10)
    p.add_argument(
        "--minio-endpoint",
        default=_target_minio_env("ENDPOINT"),
    )
    p.add_argument(
        "--minio-access-key",
        default=_target_minio_env("ACCESS_KEY"),
    )
    p.add_argument(
        "--minio-secret-key",
        default=_target_minio_env("SECRET_KEY"),
    )
    p.add_argument("--bucket", default="loom-benchmarks")
    p.add_argument("--seed", type=int, default=0)


def _add_audit_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("benchmark", nargs="?")
    p.add_argument("--all", dest="all_benchmarks", action="store_true")
    p.add_argument("--json", dest="as_json", action="store_true")
    p.add_argument(
        "--db-url",
        default=_target_db_url(),
        help="Postgres URL (defaults to env LOOM_DB_URL, then LOOM_SVC_DB_URL).",
    )
    p.add_argument(
        "--verify-bundles",
        action="store_true",
        help=(
            "Also verify internal s3:// task bundle prefixes in object storage "
            "by reading each mirrored task.toml."
        ),
    )
    p.add_argument(
        "--minio-endpoint",
        default=_target_minio_env("ENDPOINT"),
    )
    p.add_argument(
        "--minio-access-key",
        default=_target_minio_env("ACCESS_KEY"),
    )
    p.add_argument(
        "--minio-secret-key",
        default=_target_minio_env("SECRET_KEY"),
    )


def _add_hf_boundary_evidence_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("benchmark")
    p.add_argument("--environment", required=True, choices=("staging", "production"))
    p.add_argument("--output", required=True, type=Path)
    p.add_argument(
        "--namespace",
        default=None,
        help=(
            "Kubernetes namespace whose loom-service pod owns DB/MinIO env. "
            "When supplied, catalog audit, source summary, and canary summary "
            "are collected through kubectl exec without printing secrets."
        ),
    )
    p.add_argument("--kube-service-deployment", default="loom-service")
    p.add_argument("--kube-service-container", default="loom-service")
    p.add_argument(
        "--db-url",
        default=_target_db_url(),
        help=(
            "Postgres URL for non-kubernetes evidence generation. Prefer env "
            "LOOM_DB_URL or LOOM_SVC_DB_URL."
        ),
    )
    p.add_argument("--audit-json", type=Path, default=None)
    p.add_argument("--source-summary-json", type=Path, default=None)
    p.add_argument("--canary-summary-json", type=Path, default=None)
    p.add_argument("--worker-boundary-json", type=Path, default=None)
    p.add_argument("--canary-batch-id", default=None)
    p.add_argument("--worker-pool", default="gb10-arm64")
    p.add_argument("--cluster-config", type=Path, default=None)
    p.add_argument(
        "--gb10-workers-status",
        type=Path,
        default=None,
        help=(
            "Release-gate GB10 status artifact path, kept with generated "
            "evidence for traceability."
        ),
    )
    p.add_argument("--ssh-timeout-sec", type=float, default=60.0)
    p.add_argument("--minio-endpoint", default=_target_minio_env("ENDPOINT"))
    p.add_argument("--minio-access-key", default=_target_minio_env("ACCESS_KEY"))
    p.add_argument("--minio-secret-key", default=_target_minio_env("SECRET_KEY"))


def _source_env(name: str) -> str | None:
    return (
        os.environ.get(f"LOOM_CATALOG_SOURCE_{name}")
        or os.environ.get(f"LOOM_SOURCE_{name}")
    )


def _add_provision_catalog_provision_args(p: argparse.ArgumentParser) -> None:
    p.add_argument(
        "--source-db-url",
        default=_source_env("DB_URL"),
        help="Source Postgres URL (env: LOOM_CATALOG_SOURCE_DB_URL or LOOM_SOURCE_DB_URL).",
    )
    p.add_argument(
        "--target-db-url",
        default=_target_db_url(),
        help=(
            "Target staging Postgres URL (defaults to env LOOM_DB_URL, "
            "then LOOM_SVC_DB_URL inside deployed service pods)."
        ),
    )
    p.add_argument(
        "--source-minio-endpoint",
        default=_source_env("MINIO_ENDPOINT"),
    )
    p.add_argument(
        "--source-minio-access-key",
        default=_source_env("MINIO_ACCESS_KEY"),
    )
    p.add_argument(
        "--source-minio-secret-key",
        default=_source_env("MINIO_SECRET_KEY"),
    )
    p.add_argument(
        "--target-minio-endpoint",
        default=_target_minio_env("ENDPOINT"),
    )
    p.add_argument(
        "--target-minio-access-key",
        default=_target_minio_env("ACCESS_KEY"),
    )
    p.add_argument(
        "--target-minio-secret-key",
        default=_target_minio_env("SECRET_KEY"),
    )
    p.add_argument(
        "--target-bucket",
        default=os.environ.get("LOOM_BENCHMARK_BUCKET", "loom-benchmarks"),
    )
    p.add_argument("--imported-by", default="staging-provision")


def _add_validate_local_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("path", type=Path)
    p.add_argument(
        "--id",
        dest="benchmark_id",
        default=None,
        help="Benchmark id when PATH has no benchmark.toml.",
    )
    p.add_argument(
        "--display-name",
        default=None,
        help="Benchmark display name when PATH has no benchmark.toml.",
    )
    p.add_argument(
        "--series",
        default=None,
        help="Benchmark series when PATH has no benchmark.toml.",
    )
    p.add_argument(
        "--license-spdx",
        default=None,
        help="Benchmark license SPDX id when PATH has no benchmark.toml.",
    )
    p.add_argument(
        "--source-subdir",
        default=None,
        help="Optional relative task-bundle subdir for direct-layout PATHs.",
    )
    p.add_argument("--json", dest="as_json", action="store_true")


def _add_publish_local_args(p: argparse.ArgumentParser) -> None:
    p.add_argument("path", type=Path)
    p.add_argument(
        "--db-url",
        default=None,
        help=(
            "Postgres URL source for publish-local. Prefer LOOM_DB_URL "
            "(then LOOM_SVC_DB_URL) in the environment; explicit values must "
            "use env:LOOM_DB_URL, file:PATH, or -. Literal values are rejected "
            "because argv is visible through process listings."
        ),
    )
    p.add_argument(
        "--minio-endpoint",
        default=_target_minio_env("ENDPOINT"),
        help=(
            "Target object-store endpoint (env LOOM_MINIO_ENDPOINT, then "
            "LOOM_SVC_MINIO_ENDPOINT)."
        ),
    )
    p.add_argument(
        "--minio-access-key",
        default=None,
        help=(
            "Target object-store access-key source. Prefer "
            "LOOM_MINIO_ACCESS_KEY (then LOOM_SVC_MINIO_ACCESS_KEY) in the "
            "environment; explicit values must use env:LOOM_MINIO_ACCESS_KEY, "
            "file:PATH, or -. Literal values are rejected because argv is "
            "visible through process listings."
        ),
    )
    p.add_argument(
        "--minio-secret-key",
        default=None,
        help=(
            "Target object-store secret-key source. Prefer "
            "LOOM_MINIO_SECRET_KEY (then LOOM_SVC_MINIO_SECRET_KEY) in the "
            "environment; explicit values must use env:LOOM_MINIO_SECRET_KEY, "
            "file:PATH, or -. Literal values are rejected because argv is "
            "visible through process listings."
        ),
    )
    p.add_argument("--bucket", default="loom-benchmarks")
    p.add_argument("--imported-by", default=None)
    p.add_argument(
        "--id",
        dest="benchmark_id",
        default=None,
        help="Benchmark id when PATH has no benchmark.toml.",
    )
    p.add_argument(
        "--display-name",
        default=None,
        help="Benchmark display name when PATH has no benchmark.toml.",
    )
    p.add_argument(
        "--series",
        default=None,
        help="Benchmark series when PATH has no benchmark.toml.",
    )
    p.add_argument(
        "--license-spdx",
        default=None,
        help="Benchmark license SPDX id when PATH has no benchmark.toml.",
    )
    p.add_argument(
        "--source-subdir",
        default=None,
        help="Optional relative task-bundle subdir for direct-layout PATHs.",
    )
    p.add_argument(
        "--compat-flatten-environment",
        action="store_true",
        help=(
            "Explicit operator compatibility override: stage-copy files from "
            "environment/ to the bundle root before compatibility preflight. "
            "Use only with retained evidence for legacy Source Useful-style "
            "bundles; the default is diagnostic fail-fast."
        ),
    )


def _resolve_secret_source_or_env(
    value: str | None,
    *,
    flag_name: str,
    env_names: tuple[str, ...],
    errors: list[str],
) -> str | None:
    """Resolve a secret-bearing publish-local argument without encouraging argv."""

    if value is None:
        return _env_first(*env_names)
    try:
        return resolve_secret_source(value, flag_name=flag_name)
    except SecretSourceError as exc:
        errors.append(str(exc))
        return None


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
        help=(
            "Read a benchmark's HF manifest and upsert task rows; optionally "
            "mirror bundles into internal object storage."
        ),
    ))
    _add_verify_args(sub.add_parser(
        "verify",
        help="Sample tasks from a benchmark + run the oracle agent end-to-end.",
    ))
    _add_audit_args(sub.add_parser(
        "audit",
        help="Inspect benchmark readiness from registered catalog/task rows.",
    ))
    _add_hf_boundary_evidence_args(sub.add_parser(
        "hf-boundary-evidence",
        help="Generate secret-safe HF mirror/token-boundary release evidence.",
    ))
    _add_provision_catalog_provision_args(sub.add_parser(
        "provision-catalog",
        help=(
            "Copy runnable benchmark/task rows, supported agent rows, and "
            "their S3 bundles from a source environment into staging."
        ),
    ))

    _add_validate_local_args(sub.add_parser(
        "validate-local",
        aliases=["validate"],
        help="Validate a local user-owned benchmark folder and print a registry snippet.",
    ))

    _add_publish_local_args(sub.add_parser(
        "publish-local",
        help="Upload a validated local benchmark folder to object storage and register it.",
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
        "--db-url",
        default=_target_db_url(),
        help="Postgres URL (defaults to env LOOM_DB_URL, then LOOM_SVC_DB_URL).",
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
            ("--db-url / LOOM_DB_URL / LOOM_SVC_DB_URL", args.db_url),
            (
                "--minio-endpoint / LOOM_MINIO_ENDPOINT / "
                "LOOM_SVC_MINIO_ENDPOINT",
                args.minio_endpoint,
            ),
            (
                "--minio-access-key / LOOM_MINIO_ACCESS_KEY / "
                "LOOM_SVC_MINIO_ACCESS_KEY",
                args.minio_access_key,
            ),
            (
                "--minio-secret-key / LOOM_MINIO_SECRET_KEY / "
                "LOOM_SVC_MINIO_SECRET_KEY",
                args.minio_secret_key,
            ),
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
    try:
        stats = asyncio.run(run_import(
            benchmark=args.benchmark,
            db_url=args.db_url,
            object_store=store,
            bucket=args.bucket,
            cache_dir=args.cache_dir,
            limit=args.limit,
            instance_ids=set(args.instance_ids) if args.instance_ids else None,
            imported_by=args.imported_by,
            refresh=args.refresh,
        ))
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    print(f"converted={stats['converted']} warnings={stats['warnings']}")
    return 0


def _cmd_publish(args: argparse.Namespace) -> int:
    from loom.security.redaction import redact_text
    from loom_benchmark_tool.publish_cmd import run_publish

    if not args.hf_token:
        print(
            "error: publish requires --hf-token / env HF_TOKEN",
            file=sys.stderr,
        )
        return 2
    try:
        result = run_publish(
            benchmark=args.benchmark,
            hf_org=args.hf_org,
            hf_token=args.hf_token,
            cache_dir=args.cache_dir,
            limit=args.limit,
            instance_ids=set(args.instance_ids) if args.instance_ids else None,
            private=args.private,
            refresh=args.refresh,
        )
    except ValueError as exc:
        print(f"error: {redact_text(str(exc))}", file=sys.stderr)
        return 2
    except Exception as exc:
        message = redact_text(str(exc))
        if args.hf_token:
            message = message.replace(args.hf_token, "[REDACTED:hf-token]")
        print(
            f"error: publish failed for {args.benchmark}: {message}",
            file=sys.stderr,
        )
        return 1
    print(
        f"publish {args.benchmark}: "
        f"published={result['published']} "
        f"warnings={result['warnings']} "
        f"repo={result['repo_id']} "
        f"rev={result['revision']}",
    )
    return 0


def _cmd_register(args: argparse.Namespace) -> int:
    from loom.trajectory.storage import MinioObjectStore
    from loom_benchmark_tool.register_cmd import run_register

    if not args.db_url:
        print(
            "error: register requires --db-url / env LOOM_DB_URL "
            "or LOOM_SVC_DB_URL",
            file=sys.stderr,
        )
        return 2
    object_store = None
    if args.mirror_to_object_store:
        missing = [
            f for f, v in (
                (
                    "--minio-endpoint / LOOM_MINIO_ENDPOINT / "
                    "LOOM_SVC_MINIO_ENDPOINT",
                    args.minio_endpoint,
                ),
                (
                    "--minio-access-key / LOOM_MINIO_ACCESS_KEY / "
                    "LOOM_SVC_MINIO_ACCESS_KEY",
                    args.minio_access_key,
                ),
                (
                    "--minio-secret-key / LOOM_MINIO_SECRET_KEY / "
                    "LOOM_SVC_MINIO_SECRET_KEY",
                    args.minio_secret_key,
                ),
            ) if not v
        ]
        if missing:
            print(f"error: register mirror requires: {', '.join(missing)}", file=sys.stderr)
            return 2
        object_store = MinioObjectStore(
            endpoint_url=args.minio_endpoint,
            access_key=args.minio_access_key,
            secret_key=args.minio_secret_key,
        )
    result = asyncio.run(run_register(
        benchmark=args.benchmark,
        hf_org=args.hf_org,
        hf_token=args.hf_token,
        db_url=args.db_url,
        revision=args.revision,
        registered_by=args.registered_by,
        mirror_to_object_store=args.mirror_to_object_store,
        object_store=object_store,
        bucket=args.bucket,
    ))
    parts = [
        f"register {args.benchmark}:",
        f"registered={result['registered']}",
        f"legacy_placeholders={result['legacy_placeholders']}",
        f"skipped={result['skipped']}",
    ]
    if args.mirror_to_object_store:
        parts.extend([
            f"mirrored={result['mirrored']}",
            f"mirror_uploaded={result['mirror_uploaded']}",
            f"mirror_skipped={result['mirror_skipped']}",
        ])
    parts.extend([f"repo={result['repo_id']}", f"rev={result['revision']}"])
    print(" ".join(parts))
    return 0


def _cmd_verify(args: argparse.Namespace) -> int:
    from loom.trajectory.storage import MinioObjectStore
    from loom_benchmark_tool.verify_cmd import run_verify

    missing = [
        f for f, v in (
            (
                "--minio-endpoint / LOOM_MINIO_ENDPOINT / "
                "LOOM_SVC_MINIO_ENDPOINT",
                args.minio_endpoint,
            ),
            (
                "--minio-access-key / LOOM_MINIO_ACCESS_KEY / "
                "LOOM_SVC_MINIO_ACCESS_KEY",
                args.minio_access_key,
            ),
            (
                "--minio-secret-key / LOOM_MINIO_SECRET_KEY / "
                "LOOM_SVC_MINIO_SECRET_KEY",
                args.minio_secret_key,
            ),
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


def _cmd_audit(args: argparse.Namespace) -> int:
    from loom.trajectory.storage import MinioObjectStore

    if not args.db_url:
        print(
            "error: audit requires --db-url / env LOOM_DB_URL "
            "or LOOM_SVC_DB_URL",
            file=sys.stderr,
        )
        return 2
    if args.all_benchmarks and args.benchmark:
        print(
            "error: pass either --all or a benchmark, not both",
            file=sys.stderr,
        )
        return 2
    if not args.all_benchmarks and not args.benchmark:
        print(
            "error: audit requires --all or a benchmark id",
            file=sys.stderr,
        )
        return 2

    object_store = None
    if args.verify_bundles:
        missing = [
            f for f, v in (
                (
                    "--minio-endpoint / LOOM_MINIO_ENDPOINT / "
                    "LOOM_SVC_MINIO_ENDPOINT",
                    args.minio_endpoint,
                ),
                (
                    "--minio-access-key / LOOM_MINIO_ACCESS_KEY / "
                    "LOOM_SVC_MINIO_ACCESS_KEY",
                    args.minio_access_key,
                ),
                (
                    "--minio-secret-key / LOOM_MINIO_SECRET_KEY / "
                    "LOOM_SVC_MINIO_SECRET_KEY",
                    args.minio_secret_key,
                ),
            ) if not v
        ]
        if missing:
            print(f"error: audit --verify-bundles requires: {', '.join(missing)}", file=sys.stderr)
            return 2
        object_store = MinioObjectStore(
            endpoint_url=args.minio_endpoint,
            access_key=args.minio_access_key,
            secret_key=args.minio_secret_key,
        )

    items = asyncio.run(run_readiness_audit(
        db_url=args.db_url,
        benchmark=None if args.all_benchmarks else args.benchmark,
    ))
    bundle_report = None
    if args.verify_bundles:
        assert object_store is not None
        bundle_report = asyncio.run(run_bundle_presence_audit(
            db_url=args.db_url,
            object_store=object_store,
            benchmark=None if args.all_benchmarks else args.benchmark,
        ))
    if args.as_json:
        payload = json.loads(render_readiness_json(items))
        if bundle_report is not None:
            payload["bundle_presence"] = {
                "s3_tasks": bundle_report.s3_tasks,
                "verified": bundle_report.verified,
                "missing": bundle_report.missing,
                "missing_sources": bundle_report.missing_sources,
            }
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(render_readiness_table(items))
        if bundle_report is not None:
            print(
                "bundle_presence "
                f"s3_tasks={bundle_report.s3_tasks} "
                f"verified={bundle_report.verified} "
                f"missing={bundle_report.missing}",
            )
            for source in bundle_report.missing_sources[:10]:
                print(f"  missing {source}")
    if bundle_report is not None and bundle_report.missing > 0:
        return 1
    return 0


def _cmd_provision_catalog_provision(args: argparse.Namespace) -> int:
    from loom_cli.catalog_provision import (
        Boto3CatalogObjectStore,
        PostgresCatalogStore,
        provision_ready_benchmark_catalog,
    )

    missing: list[str] = []

    def require_present(label: str, *, present: bool) -> None:
        if not present:
            missing.append(label)

    require_present(
        "--source-db-url / LOOM_CATALOG_SOURCE_DB_URL / LOOM_SOURCE_DB_URL",
        present=bool(args.source_db_url),
    )
    require_present(
        "--target-db-url / LOOM_DB_URL / LOOM_SVC_DB_URL",
        present=bool(args.target_db_url),
    )
    require_present(
        "--source-minio-endpoint / LOOM_CATALOG_SOURCE_MINIO_ENDPOINT / "
        "LOOM_SOURCE_MINIO_ENDPOINT",
        present=bool(args.source_minio_endpoint),
    )
    require_present(
        "--source-minio-access-key / LOOM_CATALOG_SOURCE_MINIO_ACCESS_KEY / "
        "LOOM_SOURCE_MINIO_ACCESS_KEY",
        present=bool(args.source_minio_access_key),
    )
    require_present(
        "--source-minio-secret-key / LOOM_CATALOG_SOURCE_MINIO_SECRET_KEY / "
        "LOOM_SOURCE_MINIO_SECRET_KEY",
        present=bool(args.source_minio_secret_key),
    )
    require_present(
        "--target-minio-endpoint / LOOM_MINIO_ENDPOINT / LOOM_SVC_MINIO_ENDPOINT",
        present=bool(args.target_minio_endpoint),
    )
    require_present(
        "--target-minio-access-key / LOOM_MINIO_ACCESS_KEY / "
        "LOOM_SVC_MINIO_ACCESS_KEY",
        present=bool(args.target_minio_access_key),
    )
    require_present(
        "--target-minio-secret-key / LOOM_MINIO_SECRET_KEY / "
        "LOOM_SVC_MINIO_SECRET_KEY",
        present=bool(args.target_minio_secret_key),
    )
    if missing:
        print(
            f"error: provision-catalog requires: {', '.join(missing)}",
            file=sys.stderr,
        )
        return 2

    source_catalog = PostgresCatalogStore(args.source_db_url)
    target_catalog = PostgresCatalogStore(args.target_db_url)
    source_objects = Boto3CatalogObjectStore(
        endpoint_url=args.source_minio_endpoint,
        access_key=args.source_minio_access_key,
        secret_key=args.source_minio_secret_key,
    )
    target_objects = Boto3CatalogObjectStore(
        endpoint_url=args.target_minio_endpoint,
        access_key=args.target_minio_access_key,
        secret_key=args.target_minio_secret_key,
    )
    stats = asyncio.run(provision_ready_benchmark_catalog(
        source_catalog=source_catalog,
        target_catalog=target_catalog,
        source_objects=source_objects,
        target_objects=target_objects,
        target_bucket=args.target_bucket,
        imported_by=args.imported_by,
    ))
    print(
        "staging-catalog: "
        f"ready_agents={stats.ready_agents} "
        f"ready_benchmarks={stats.ready_benchmarks} "
        f"ready_tasks={stats.ready_tasks} "
        f"source_objects={stats.source_objects} "
        f"uploaded={stats.target_objects_uploaded} "
        f"skipped={stats.target_objects_skipped} "
        f"missing={stats.target_objects_missing} "
        f"bytes_uploaded={stats.bytes_uploaded} "
        f"bytes_skipped={stats.bytes_skipped}",
    )
    if stats.target_objects_missing:
        print(
            "error: source catalog referenced task bundle prefixes with no objects; "
            "target DB rows were not updated",
            file=sys.stderr,
        )
        return 1
    return 0


def _cmd_validate_local(args: argparse.Namespace) -> int:
    from loom_cli.local_benchmark_validate import (
        LocalBenchmarkValidationError,
        render_config_snippet,
        render_validation_json,
        validate_local_benchmark,
    )

    try:
        result = validate_local_benchmark(
            args.path,
            benchmark_id=args.benchmark_id,
            display_name=args.display_name,
            series=args.series,
            license_spdx=args.license_spdx,
            source_subdir=args.source_subdir,
        )
    except LocalBenchmarkValidationError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return exc.exit_code

    if args.as_json:
        print(render_validation_json(result))
        return 0

    print("valid local benchmark folder")
    print(f"benchmark_id: {result.entry.id}")
    print(f"display_name: {result.entry.display_name}")
    print(f"series:       {result.entry.series}")
    print(f"license:      {result.entry.license_spdx}")
    if result.entry.source_subdir:
        print(f"source_subdir: {result.entry.source_subdir}")
    print(f"tasks:        {result.task_count} valid")
    print("config snippet:")
    print(render_config_snippet(result.entry))
    return 0


def _cmd_publish_local(args: argparse.Namespace) -> int:
    from loom.trajectory.storage import MinioObjectStore
    from loom_cli.local_benchmark_publish import publish_local_benchmark
    from loom_cli.local_benchmark_validate import LocalBenchmarkValidationError

    secret_source_errors: list[str] = []
    db_url = _resolve_secret_source_or_env(
        args.db_url,
        flag_name="--db-url",
        env_names=("LOOM_DB_URL", "LOOM_SVC_DB_URL"),
        errors=secret_source_errors,
    )
    minio_access_key = _resolve_secret_source_or_env(
        args.minio_access_key,
        flag_name="--minio-access-key",
        env_names=("LOOM_MINIO_ACCESS_KEY", "LOOM_SVC_MINIO_ACCESS_KEY"),
        errors=secret_source_errors,
    )
    minio_secret_key = _resolve_secret_source_or_env(
        args.minio_secret_key,
        flag_name="--minio-secret-key",
        env_names=("LOOM_MINIO_SECRET_KEY", "LOOM_SVC_MINIO_SECRET_KEY"),
        errors=secret_source_errors,
    )
    if secret_source_errors:
        print(
            "error: publish-local refuses secret values in command-line argv; "
            "use LOOM_* environment variables or env:VAR/file:PATH/- references.",
            file=sys.stderr,
        )
        for error in secret_source_errors:
            print(f"  {error}", file=sys.stderr)
        return 2

    missing = [
        f for f, v in (
            ("--db-url / LOOM_DB_URL / LOOM_SVC_DB_URL", db_url),
            (
                "--minio-endpoint / LOOM_MINIO_ENDPOINT / "
                "LOOM_SVC_MINIO_ENDPOINT",
                args.minio_endpoint,
            ),
            (
                "--minio-access-key / LOOM_MINIO_ACCESS_KEY / "
                "LOOM_SVC_MINIO_ACCESS_KEY",
                minio_access_key,
            ),
            (
                "--minio-secret-key / LOOM_MINIO_SECRET_KEY / "
                "LOOM_SVC_MINIO_SECRET_KEY",
                minio_secret_key,
            ),
        ) if not v
    ]
    if missing:
        print(
            f"error: publish-local requires: {', '.join(missing)}",
            file=sys.stderr,
        )
        return 2

    assert db_url is not None
    assert args.minio_endpoint is not None
    assert minio_access_key is not None
    assert minio_secret_key is not None

    store = MinioObjectStore(
        endpoint_url=args.minio_endpoint,
        access_key=minio_access_key,
        secret_key=minio_secret_key,
    )
    try:
        stats = asyncio.run(publish_local_benchmark(
            args.path,
            db_url=db_url,
            object_store=store,
            bucket=args.bucket,
            benchmark_id=args.benchmark_id,
            display_name=args.display_name,
            series=args.series,
            license_spdx=args.license_spdx,
            source_subdir=args.source_subdir,
            imported_by=args.imported_by,
            compat_flatten_environment=args.compat_flatten_environment,
        ))
    except LocalBenchmarkValidationError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return exc.exit_code

    print(
        f"publish-local {stats.benchmark_id}: "
        f"tasks={stats.task_count} "
        f"inserted={stats.inserted} "
        f"updated={stats.updated} "
        f"unchanged={stats.unchanged} "
        f"uploaded_objects={stats.uploaded_objects} "
        f"compat_flattened_files={stats.compat_flattened_files} "
        f"source={stats.source_prefix}",
    )
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
            "error: sync-config requires --db-url / env LOOM_DB_URL "
            "or LOOM_SVC_DB_URL",
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


def _cmd_hf_boundary_evidence(args: argparse.Namespace) -> int:
    from loom_cli.hf_boundary_evidence import run_hf_boundary_evidence_command

    return run_hf_boundary_evidence_command(args)


_DISPATCH: dict[str, Callable[[argparse.Namespace], int]] = {
    "list": _cmd_list,
    "show": _cmd_show,
    "install": _cmd_install,
    "refresh-catalog": _cmd_refresh,
    "import": _cmd_import,
    "publish": _cmd_publish,
    "register": _cmd_register,
    "verify": _cmd_verify,
    "audit": _cmd_audit,
    "hf-boundary-evidence": _cmd_hf_boundary_evidence,
    "provision-catalog": _cmd_provision_catalog_provision,
    "validate-local": _cmd_validate_local,
    "validate": _cmd_validate_local,
    "publish-local": _cmd_publish_local,
    "sync-config": _cmd_sync_config,
}


def dispatch(argv: list[str]) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    handler = _DISPATCH.get(args.subcmd)
    if handler is None:
        parser.error(f"unknown subcommand: {args.subcmd}")  # raises SystemExit
    return handler(args)
