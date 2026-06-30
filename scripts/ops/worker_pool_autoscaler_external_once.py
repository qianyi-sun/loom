#!/usr/bin/env python3
"""Run one scoped external worker-pool autoscaler reconciliation.

This entrypoint is intended for Slurm submit hosts where the external runner can
call sbatch/scancel and reach the control-plane database through a local
port-forward. Keep it pool-scoped so release gates can supervise OLDLAB without
changing GB10 policy.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import json
import os
import subprocess
from collections.abc import Sequence

from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom_control_plane.worker_pool_autoscaler import (
    reconcile_worker_pool_autoscaler_once,
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run one scoped external worker-pool autoscaler reconcile.",
    )
    parser.add_argument(
        "--pool-name",
        action="append",
        required=True,
        help="Pool to reconcile. Repeat for multiple pools.",
    )
    parser.add_argument("--namespace", default="loom-public-beta")
    parser.add_argument(
        "--kubeconfig",
        default=os.environ.get("KUBECONFIG", "/home/qianyi/.kube/config"),
    )
    parser.add_argument("--kubectl", default="/usr/local/bin/kubectl")
    parser.add_argument("--db-secret-name", default="loom-secrets")
    parser.add_argument("--db-secret-key", default="cp-db-url")
    parser.add_argument("--db-local-host", default="127.0.0.1")
    parser.add_argument("--db-local-port", type=int, default=15447)
    parser.add_argument("--freshness-sec", type=int, default=120)
    return parser


def _load_cp_db_url(args: argparse.Namespace) -> str:
    encoded = subprocess.check_output(
        [
            args.kubectl,
            "--kubeconfig",
            args.kubeconfig,
            "-n",
            args.namespace,
            "get",
            "secret",
            args.db_secret_name,
            "-o",
            f"jsonpath={{.data.{args.db_secret_key}}}",
        ],
        text=True,
    ).strip()
    return base64.b64decode(encoded).decode("utf-8")


def _scoped_pool_names(values: Sequence[str]) -> tuple[str, ...]:
    pool_names = tuple(dict.fromkeys(value.strip() for value in values if value.strip()))
    if not pool_names:
        raise SystemExit("--pool-name must include at least one non-empty pool")
    return pool_names


async def _main_async(args: argparse.Namespace) -> None:
    pool_names = _scoped_pool_names(args.pool_name)
    db_url = _load_cp_db_url(args)
    url = make_url(db_url).set(host=args.db_local_host, port=args.db_local_port)
    engine = create_async_engine(url, pool_pre_ping=True)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with session_factory() as session:
            decisions = await reconcile_worker_pool_autoscaler_once(
                session,
                freshness_sec=args.freshness_sec,
                include_external_policies=True,
                external_only=True,
                pool_names=pool_names,
            )
            await session.commit()
        print(json.dumps([decision.__dict__ for decision in decisions], default=str))
    finally:
        await engine.dispose()


def main() -> None:
    asyncio.run(_main_async(_parser().parse_args()))


if __name__ == "__main__":
    main()
