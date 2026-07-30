#!/usr/bin/env python3
"""Run one scoped external worker-pool autoscaler reconciliation.

This entrypoint is the fixed staging GB10 control-plane driver. It reaches the
database through its bounded local port-forward, while every allocation query,
submission, and cancellation runs through non-interactive sudo to the
root-owned ``loom-staging-external-slurm-authority`` and from there to
``trt-gb10-1``. It has no direct ``squeue``/``sbatch``/``scancel``, local Slurm,
OLDLAB, or alternate-controller fallback.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import contextlib
import ipaddress
import json
import math
import os
import re
import socket
import subprocess
import sys
import tempfile
import time
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from typing import Any

from sqlalchemy import select
from sqlalchemy.engine import URL, make_url
from sqlalchemy.exc import ArgumentError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import WorkerPoolAutoscalerPolicy
from loom_control_plane.worker_pool_autoscaler import (
    reconcile_worker_pool_autoscaler_once,
)

_MAX_PORT_FORWARD_READY_TIMEOUT_SEC = 60.0
_MAX_PORT_FORWARD_STOP_TIMEOUT_SEC = 30.0
_MAX_PORT_FORWARD_STARTUP_OUTPUT_BYTES = 16 * 1024
_KUBERNETES_NAME = re.compile(r"[a-z0-9](?:[-a-z0-9]*[a-z0-9])?")


class ExternalAutoscalerError(RuntimeError):
    """Expected, non-secret failure that is safe to show to an operator."""


class ExternalAutoscalerConfigurationError(ExternalAutoscalerError, ValueError):
    """Raised when command-line transport configuration is unsafe."""


class DatabasePortForwardError(ExternalAutoscalerError):
    """Raised when the owned database tunnel cannot be proven healthy."""


class ExternalPolicyValidationError(ExternalAutoscalerError):
    """Raised when requested external policies are missing or ambiguous."""


@dataclass(frozen=True)
class DatabasePortForwardConfig:
    kubectl: str
    kubeconfig: str
    namespace: str
    service: str
    local_host: str
    local_port: int
    remote_port: int
    ready_timeout_sec: float
    stop_timeout_sec: float


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
    parser.add_argument(
        "--environment",
        required=True,
        help="Exact environment whose external-runner policies may be reconciled.",
    )
    parser.add_argument("--namespace", default="loom-staging")
    parser.add_argument(
        "--kubeconfig",
        default=os.environ.get("KUBECONFIG", "/home/qianyi/.kube/config"),
    )
    parser.add_argument("--kubectl", default="/usr/local/bin/kubectl")
    parser.add_argument("--db-secret-name", default="loom-secrets")
    parser.add_argument("--db-secret-key", default="cp-db-url")
    parser.add_argument("--db-local-host", default="127.0.0.1")
    parser.add_argument("--db-local-port", type=int, default=15447)
    parser.add_argument("--db-service", default="service/loom-postgres")
    parser.add_argument("--db-remote-port", type=int, default=5432)
    parser.add_argument("--db-port-forward-ready-timeout-sec", type=float, default=10.0)
    parser.add_argument("--db-port-forward-stop-timeout-sec", type=float, default=5.0)
    parser.add_argument("--db-connect-timeout-sec", type=float, default=10.0)
    parser.add_argument("--freshness-sec", type=int, default=120)
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help=(
            "Validate the exact DB secret, tunnel, and external-policy presence "
            "without reconciling or invoking an actuator."
        ),
    )
    return parser


def _load_cp_db_url(args: argparse.Namespace, *, timeout_sec: float) -> str:
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
        timeout=timeout_sec,
    ).strip()
    return base64.b64decode(encoded).decode("utf-8")


def _scoped_pool_names(values: Sequence[str]) -> tuple[str, ...]:
    pool_names = tuple(dict.fromkeys(value.strip() for value in values if value.strip()))
    if not pool_names:
        raise SystemExit("--pool-name must include at least one non-empty pool")
    return pool_names


def _scoped_environment(value: object) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or any(ord(character) < 32 or ord(character) == 127 for character in value)
    ):
        raise ExternalAutoscalerConfigurationError(
            "--environment must be an exact non-empty value without surrounding whitespace"
        )
    return value


def _validated_port(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= 65535:
        raise ExternalAutoscalerConfigurationError(f"{field} must be an integer in 1..65535")
    return value


def _validated_timeout(value: object, field: str, *, maximum: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ExternalAutoscalerConfigurationError(
            f"{field} must be a finite positive number no greater than {maximum:g}"
        )
    normalized = float(value)
    if not math.isfinite(normalized) or not 0 < normalized <= maximum:
        raise ExternalAutoscalerConfigurationError(
            f"{field} must be a finite positive number no greater than {maximum:g}"
        )
    return normalized


def _validated_kubernetes_name(value: object, field: str) -> str:
    if not isinstance(value, str):
        raise ExternalAutoscalerConfigurationError(f"{field} must be a Kubernetes DNS label")
    normalized = value.strip()
    if len(normalized) > 63 or _KUBERNETES_NAME.fullmatch(normalized) is None:
        raise ExternalAutoscalerConfigurationError(f"{field} must be a Kubernetes DNS label")
    return normalized


def _validated_service(value: object) -> str:
    if not isinstance(value, str):
        raise ExternalAutoscalerConfigurationError(
            "--db-service must be service/<Kubernetes DNS label>"
        )
    prefix, separator, name = value.strip().partition("/")
    if separator != "/" or prefix not in {"service", "svc"}:
        raise ExternalAutoscalerConfigurationError(
            "--db-service must be service/<Kubernetes DNS label>"
        )
    return f"service/{_validated_kubernetes_name(name, '--db-service')}"


def _database_port_forward_config(args: argparse.Namespace) -> DatabasePortForwardConfig:
    try:
        local_address = ipaddress.ip_address(str(args.db_local_host).strip())
    except ValueError as exc:
        raise ExternalAutoscalerConfigurationError(
            "--db-local-host must be a loopback IP address"
        ) from exc
    if not local_address.is_loopback:
        raise ExternalAutoscalerConfigurationError("--db-local-host must be a loopback IP address")
    kubectl = str(args.kubectl).strip()
    kubeconfig = str(args.kubeconfig).strip()
    if not kubectl:
        raise ExternalAutoscalerConfigurationError("--kubectl must be non-empty")
    if not kubeconfig:
        raise ExternalAutoscalerConfigurationError("--kubeconfig must be non-empty")
    return DatabasePortForwardConfig(
        kubectl=kubectl,
        kubeconfig=kubeconfig,
        namespace=_validated_kubernetes_name(args.namespace, "--namespace"),
        service=_validated_service(args.db_service),
        local_host=str(local_address),
        local_port=_validated_port(args.db_local_port, "--db-local-port"),
        remote_port=_validated_port(args.db_remote_port, "--db-remote-port"),
        ready_timeout_sec=_validated_timeout(
            args.db_port_forward_ready_timeout_sec,
            "--db-port-forward-ready-timeout-sec",
            maximum=_MAX_PORT_FORWARD_READY_TIMEOUT_SEC,
        ),
        stop_timeout_sec=_validated_timeout(
            args.db_port_forward_stop_timeout_sec,
            "--db-port-forward-stop-timeout-sec",
            maximum=_MAX_PORT_FORWARD_STOP_TIMEOUT_SEC,
        ),
    )


def _preflight_database_url(
    database_url: str,
    *,
    port_forward: DatabasePortForwardConfig,
) -> URL:
    """Bind a secret URL to the owned tunnel without libpq routing escapes."""
    try:
        url = make_url(database_url)
    except (ArgumentError, TypeError, ValueError) as exc:
        raise ExternalAutoscalerConfigurationError("database URL is invalid") from exc
    # Driver query options can override not only host routing but also the
    # user, password, and database carried by the URL authority.  This
    # privileged one-shot path has no need for query options, so an empty
    # query is the only fail-closed contract.  SQLAlchemy percent-decodes keys
    # before exposing this mapping, covering encoded representations as well.
    if url.query:
        raise ExternalAutoscalerConfigurationError("database URL query options are not permitted")
    return url.set(host=port_forward.local_host, port=port_forward.local_port)


def _socket_family(host: str) -> socket.AddressFamily:
    return socket.AF_INET6 if ipaddress.ip_address(host).version == 6 else socket.AF_INET


def _assert_local_port_available(config: DatabasePortForwardConfig) -> None:
    try:
        with socket.socket(_socket_family(config.local_host), socket.SOCK_STREAM) as probe:
            probe.bind((config.local_host, config.local_port))
    except OSError as exc:
        raise DatabasePortForwardError(
            "database port-forward local endpoint is unavailable"
        ) from exc


def _wait_for_database_port_forward(
    process: subprocess.Popen[Any],
    config: DatabasePortForwardConfig,
    output: Any,
) -> None:
    rendered_host = (
        f"[{config.local_host}]"
        if ipaddress.ip_address(config.local_host).version == 6
        else config.local_host
    )
    expected_line = (
        f"Forwarding from {rendered_host}:{config.local_port} -> {config.remote_port}"
    ).encode()
    deadline = time.monotonic() + config.ready_timeout_sec
    observed = b""
    offset = 0
    while True:
        if process.poll() is not None:
            raise DatabasePortForwardError("database port-forward exited before readiness")
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise DatabasePortForwardError("database port-forward readiness timed out")
        try:
            chunk = os.pread(
                output.fileno(),
                _MAX_PORT_FORWARD_STARTUP_OUTPUT_BYTES + 1 - len(observed),
                offset,
            )
        except (AttributeError, OSError, ValueError) as exc:
            raise DatabasePortForwardError(
                "database port-forward readiness output is unavailable"
            ) from exc
        if chunk:
            observed += chunk
            offset += len(chunk)
            if len(observed) > _MAX_PORT_FORWARD_STARTUP_OUTPUT_BYTES:
                raise DatabasePortForwardError(
                    "database port-forward readiness output exceeded its bound"
                )
        complete_lines = observed.splitlines()
        if expected_line not in complete_lines:
            time.sleep(min(0.05, remaining))
            continue
        try:
            with socket.create_connection(
                (config.local_host, config.local_port),
                timeout=min(0.2, remaining),
            ):
                if process.poll() is not None:
                    raise DatabasePortForwardError("database port-forward exited during readiness")
                return
        except OSError:
            time.sleep(min(0.05, remaining))


def _stop_database_port_forward(
    process: subprocess.Popen[Any],
    *,
    timeout_sec: float,
) -> None:
    if process.poll() is not None:
        process.wait(timeout=0)
        return
    try:
        process.terminate()
    except ProcessLookupError:
        pass
    except OSError:
        # A failed SIGTERM delivery must still take the hard-stop path.
        pass
    else:
        try:
            process.wait(timeout=timeout_sec)
            return
        except subprocess.TimeoutExpired:
            pass
    try:
        process.kill()
    except ProcessLookupError:
        pass
    except OSError as exc:
        raise DatabasePortForwardError("database port-forward could not be killed") from exc
    try:
        process.wait(timeout=timeout_sec)
    except subprocess.TimeoutExpired as exc:
        raise DatabasePortForwardError("database port-forward could not be stopped") from exc


def _start_database_port_forward(
    config: DatabasePortForwardConfig,
) -> subprocess.Popen[Any]:
    _assert_local_port_available(config)
    command = [
        config.kubectl,
        "--kubeconfig",
        config.kubeconfig,
        "-n",
        config.namespace,
        "port-forward",
        "--address",
        config.local_host,
        config.service,
        f"{config.local_port}:{config.remote_port}",
    ]
    output = tempfile.TemporaryFile(mode="w+b")
    try:
        process = subprocess.Popen(
            command,
            stdin=subprocess.DEVNULL,
            stdout=output,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    except OSError as exc:
        output.close()
        raise DatabasePortForwardError("database port-forward could not start") from exc
    try:
        _wait_for_database_port_forward(process, config, output)
    except BaseException:
        _stop_database_port_forward(process, timeout_sec=config.stop_timeout_sec)
        output.close()
        raise
    # The child retains its own anonymous descriptor and can continue emitting
    # bounded kubectl connection notices without ever blocking on a pipe.  The
    # parent no longer needs the startup proof after the exact bind line and
    # live socket have both been observed.
    output.close()
    return process


@contextlib.contextmanager
def _database_port_forward(config: DatabasePortForwardConfig) -> Iterator[None]:
    process = _start_database_port_forward(config)
    try:
        yield
    finally:
        _stop_database_port_forward(process, timeout_sec=config.stop_timeout_sec)


async def _validate_requested_external_policies(
    session: Any,
    *,
    environment: str,
    pool_names: tuple[str, ...],
) -> list[dict[str, object]]:
    result = await session.execute(
        select(WorkerPoolAutoscalerPolicy).where(
            WorkerPoolAutoscalerPolicy.enabled.is_(True),
            WorkerPoolAutoscalerPolicy.actuator == "slurm",
            WorkerPoolAutoscalerPolicy.environment == environment,
            WorkerPoolAutoscalerPolicy.pool_name.in_(pool_names),
        )
    )
    counts = dict.fromkeys(pool_names, 0)
    for row in result.scalars().all():
        actuator_config = row.actuator_config if isinstance(row.actuator_config, dict) else {}
        if actuator_config.get("external_runner") is True:
            if (
                environment != "staging"
                or row.pool_name != "gb10"
                or actuator_config.get("external_broker") != "staging-gb10-v1"
                or actuator_config.get("cluster") != "trt-gb10"
                or not isinstance(actuator_config.get("candidate_sha"), str)
                or re.fullmatch(
                    r"[0-9a-f]{40}",
                    str(actuator_config.get("candidate_sha")),
                )
                is None
                or (
                    actuator_config.get("candidate_tree") not in {None, ""}
                    and (
                        not isinstance(actuator_config.get("candidate_tree"), str)
                        or re.fullmatch(
                            r"[0-9a-f]{40}",
                            str(actuator_config.get("candidate_tree")),
                        )
                        is None
                    )
                )
            ):
                raise ExternalPolicyValidationError(
                    "external autoscaler policy must bind staging/gb10 to "
                    "staging-gb10-v1 on trt-gb10 with an exact candidate identity"
                )
            counts[row.pool_name] += 1
    invalid = sorted(pool_name for pool_name, count in counts.items() if count != 1)
    if invalid:
        raise ExternalPolicyValidationError(
            "requested external autoscaler policy must exist exactly once and be enabled: "
            + ", ".join(invalid)
        )
    return [
        {
            "environment": environment,
            "pool_name": pool_name,
            "enabled_external_policy_count": counts[pool_name],
        }
        for pool_name in pool_names
    ]


async def _main_async(args: argparse.Namespace) -> None:
    environment = _scoped_environment(args.environment)
    pool_names = _scoped_pool_names(args.pool_name)
    port_forward = _database_port_forward_config(args)
    db_connect_timeout_sec = _validated_timeout(
        args.db_connect_timeout_sec,
        "--db-connect-timeout-sec",
        maximum=_MAX_PORT_FORWARD_READY_TIMEOUT_SEC,
    )
    db_url = _load_cp_db_url(args, timeout_sec=db_connect_timeout_sec)
    url = _preflight_database_url(db_url, port_forward=port_forward)
    with _database_port_forward(port_forward):
        engine = create_async_engine(url, pool_pre_ping=True)
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        try:
            async with session_factory() as session:
                async with asyncio.timeout(db_connect_timeout_sec):
                    try:
                        validation = await _validate_requested_external_policies(
                            session,
                            environment=environment,
                            pool_names=pool_names,
                        )
                    finally:
                        await session.rollback()
                if args.validate_only:
                    print(
                        json.dumps(
                            {
                                "mode": "validate-only",
                                "database_reachable": True,
                                "pools": validation,
                            },
                            sort_keys=True,
                        )
                    )
                    return
                decisions = await reconcile_worker_pool_autoscaler_once(
                    session,
                    environment=environment,
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
    try:
        asyncio.run(_main_async(_parser().parse_args()))
    except ExternalAutoscalerError as exc:
        sys.stderr.write(f"error: {exc}\n")
        raise SystemExit(1) from None
    except (KeyboardInterrupt, SystemExit):
        raise
    except Exception:
        # SQLAlchemy/driver exceptions may embed connection details. Keep the
        # systemd journal non-secret and leave exact diagnostics to protected
        # database/Kubernetes logs.
        sys.stderr.write("error: external autoscaler reconcile failed safely\n")
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
