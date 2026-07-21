#!/usr/bin/env python3
"""Bridge production queue pressure into staging worker claim control.

This process reads a secret-free pressure snapshot from the production Control
Plane and submits it to staging's actuator-neutral worker-pool prod-pressure
endpoint.  The staging CP applies the signal per the pool's actuator: GB10
pools drain the worker registry, Slurm pools record drain intent that the
external autoscaler actor + scheduler claim path consume.  Either way the
existing claim query stops assigning new work.

One invocation can fan out to several ``ENVIRONMENT:POOL`` targets; each target
is fetched from the prod CP and applied to staging independently, and the
per-target results are aggregated into a single evidence report.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
import urllib.error
import urllib.request
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from loom_cli.secret_source import (
    SecretSourceError,
    resolve_secret_source,
    secret_source_argparse_type,
)

_SECRET_RE = re.compile(
    r"(?i)(bearer\s+|token=|secret=|password=|api[_-]?key=)\S+|"
    r"\b(?:loom_(?:admin|w)_|sk-)[A-Za-z0-9._~+/=-]+",
)


def redact_text(value: str) -> str:
    return _SECRET_RE.sub("<redacted>", value)


def _http_json(
    *,
    method: str,
    base_url: str,
    token: str,
    path: str,
    body: Mapping[str, Any] | None = None,
    timeout: float = 30.0,
) -> dict[str, Any]:
    data = None
    headers = {"Authorization": f"Bearer {token}"}
    if body is not None:
        data = json.dumps(dict(body)).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(
        base_url.rstrip("/") + path,
        data=data,
        headers=headers,
        method=method,
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            payload = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(
            f"{method} {path} failed HTTP {exc.code}: {redact_text(detail)}",
        ) from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(
            f"{method} {path} failed: {redact_text(str(exc.reason))}",
        ) from exc
    parsed = json.loads(payload or "{}")
    if not isinstance(parsed, dict):
        raise RuntimeError(f"{method} {path} returned non-object JSON")
    return parsed


_REQUIRED_PRESSURE_FIELDS = (
    "prod_pending_count",
    "prod_active_count",
    "prod_capacity_shortfall",
)


def _run_once_for_target(
    *,
    prod_cp_url: str,
    prod_admin_token: str,
    staging_cp_url: str,
    staging_admin_token: str,
    environment: str,
    pool_name: str,
    preemptible: bool,
    grace_period_seconds: int,
    freshness_seconds: int,
    timeout: float,
) -> dict[str, Any]:
    """Bridge one ``(environment, pool_name)`` target.

    GETs the secret-free pressure signal for ``pool_name`` from the prod CP,
    then POSTs it to that target's actuator-neutral staging route. Returns a
    per-target entry; on a POST/validation failure the entry carries an
    ``error`` field (redacted) instead of ``worker_control`` and the caller
    fails the aggregate report.
    """
    entry: dict[str, Any] = {"environment": environment, "pool_name": pool_name}
    pressure_path = (
        f"/admin/worker-pools/{pool_name}/prod-pressure"
        f"?freshness_sec={freshness_seconds}"
    )
    pressure_fetch_error: str | None = None
    try:
        pressure = _http_json(
            method="GET",
            base_url=prod_cp_url,
            token=prod_admin_token,
            path=pressure_path,
            timeout=timeout,
        )
    except RuntimeError as exc:
        pressure_fetch_error = redact_text(str(exc))
        pressure = {
            "has_pressure": True,
            "cause": "prod_pressure_signal_unavailable",
            "prod_pending_count": 0,
            "prod_active_count": 0,
            "prod_capacity_shortfall": 1,
            "source": "production pressure signal unavailable; fail-closed drain",
        }
    entry["pressure"] = pressure
    if pressure_fetch_error is not None:
        entry["pressure_fetch_error"] = pressure_fetch_error
        entry["fail_closed"] = True
    missing = [field for field in _REQUIRED_PRESSURE_FIELDS if field not in pressure]
    if missing:
        entry["error"] = (
            "production pressure response is missing: " + ", ".join(missing)
        )
        return entry
    body = {field: pressure[field] for field in _REQUIRED_PRESSURE_FIELDS}
    body.update(
        {
            "source": str(pressure.get("source") or "control-plane prod queue summary"),
            "preemptible": preemptible,
            "grace_period_seconds": grace_period_seconds,
        },
    )
    try:
        result = _http_json(
            method="POST",
            base_url=staging_cp_url,
            token=staging_admin_token,
            path=f"/admin/worker-pools/{environment}/{pool_name}/prod-pressure",
            body=body,
            timeout=timeout,
        )
    except RuntimeError as exc:
        entry["error"] = redact_text(str(exc))
        return entry
    entry["worker_control"] = result
    return entry


def run_once(
    *,
    prod_cp_url: str,
    prod_admin_token: str,
    staging_cp_url: str,
    staging_admin_token: str,
    targets: list[tuple[str, str]],
    preemptible: bool,
    grace_period_seconds: int,
    freshness_seconds: int,
    timeout: float,
) -> dict[str, Any]:
    """Fan out to every ``(environment, pool_name)`` target and aggregate.

    Each target is bridged independently: a failing target records its error
    and flips the top-level ``status`` to ``"fail"`` without aborting the
    remaining targets.
    """
    entries: list[dict[str, Any]] = []
    status = "pass"
    for environment, pool_name in targets:
        entry = _run_once_for_target(
            prod_cp_url=prod_cp_url,
            prod_admin_token=prod_admin_token,
            staging_cp_url=staging_cp_url,
            staging_admin_token=staging_admin_token,
            environment=environment,
            pool_name=pool_name,
            preemptible=preemptible,
            grace_period_seconds=grace_period_seconds,
            freshness_seconds=freshness_seconds,
            timeout=timeout,
        )
        if entry.get("error") is not None:
            status = "fail"
        entries.append(entry)
    return {
        "artifact_type": "prod-pressure-worker-control",
        "status": status,
        "targets": entries,
    }


def _write_evidence(path: Path, report: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(dict(report), indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _parse_target(value: str) -> tuple[str, str]:
    """Parse a ``--target ENVIRONMENT:POOL`` value into a target tuple."""
    environment, sep, pool_name = value.partition(":")
    environment = environment.strip()
    pool_name = pool_name.strip()
    if not sep or not environment or not pool_name:
        raise argparse.ArgumentTypeError(
            f"--target must be ENVIRONMENT:POOL with both non-empty, got {value!r}",
        )
    return (environment, pool_name)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prod-cp-url", required=True)
    parser.add_argument(
        "--prod-admin-token",
        required=True,
        type=secret_source_argparse_type("--prod-admin-token"),
    )
    parser.add_argument("--staging-cp-url", required=True)
    parser.add_argument(
        "--staging-admin-token",
        required=True,
        type=secret_source_argparse_type("--staging-admin-token"),
    )
    parser.add_argument("--staging-environment", default="staging")
    parser.add_argument("--pool-name", default="gb10-arm64")
    parser.add_argument(
        "--target",
        dest="targets",
        action="append",
        type=_parse_target,
        default=None,
        metavar="ENVIRONMENT:POOL",
        help=(
            "Repeatable staging target as ENVIRONMENT:POOL. When omitted, the "
            "single --staging-environment/--pool-name pair is used."
        ),
    )
    parser.set_defaults(preemptible=True)
    preemptible = parser.add_mutually_exclusive_group()
    preemptible.add_argument("--preemptible", dest="preemptible", action="store_true")
    preemptible.add_argument(
        "--non-preemptible",
        dest="preemptible",
        action="store_false",
    )
    parser.add_argument("--grace-period-seconds", type=int, default=600)
    parser.add_argument("--freshness-seconds", type=int, default=120)
    parser.add_argument(
        "--watch-interval-seconds",
        type=float,
        default=0,
        help="0 runs once; a positive value continuously reconciles pressure.",
    )
    parser.add_argument("--http-timeout", type=float, default=30.0)
    parser.add_argument("--evidence-out", type=Path)
    args = parser.parse_args(argv)
    if args.grace_period_seconds < 0:
        parser.error("--grace-period-seconds must be non-negative")
    if args.freshness_seconds <= 0:
        parser.error("--freshness-seconds must be positive")
    if args.watch_interval_seconds < 0:
        parser.error("--watch-interval-seconds must be non-negative")

    try:
        prod_admin_token = resolve_secret_source(
            args.prod_admin_token,
            flag_name="--prod-admin-token",
        )
        staging_admin_token = resolve_secret_source(
            args.staging_admin_token,
            flag_name="--staging-admin-token",
        )
    except SecretSourceError as exc:
        parser.error(str(exc))

    targets = args.targets or [(args.staging_environment, args.pool_name)]

    while True:
        try:
            report = run_once(
                prod_cp_url=args.prod_cp_url,
                prod_admin_token=prod_admin_token,
                staging_cp_url=args.staging_cp_url,
                staging_admin_token=staging_admin_token,
                targets=targets,
                preemptible=args.preemptible,
                grace_period_seconds=args.grace_period_seconds,
                freshness_seconds=args.freshness_seconds,
                timeout=args.http_timeout,
            )
        except (RuntimeError, ValueError) as exc:
            sys.stderr.write(f"error: {redact_text(str(exc))}\n")
            return 1
        if args.evidence_out is not None:
            _write_evidence(args.evidence_out, report)
        json.dump(report, sys.stdout, sort_keys=True)
        sys.stdout.write("\n")
        sys.stdout.flush()
        if args.watch_interval_seconds <= 0:
            return 0 if report["status"] == "pass" else 1
        time.sleep(args.watch_interval_seconds)


if __name__ == "__main__":
    raise SystemExit(main())
