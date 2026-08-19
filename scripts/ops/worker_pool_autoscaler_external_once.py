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
import contextlib
import ipaddress
import json
import math
import os
import re
import selectors
import socket
import subprocess
import sys
import tempfile
import time
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from sqlalchemy import select
from sqlalchemy.engine import URL, make_url
from sqlalchemy.exc import ArgumentError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import WorkerPoolAutoscalerPolicy
from loom.dev_instance import dev_pool_instance_name
from loom_control_plane.global_dev_fleet_autoscaler import (
    GlobalDevAutoscalerError,
    capacity_grants_from_report,
)
from loom_control_plane.global_execution_fence import (
    GlobalExecutionFenceError,
    GlobalExecutionWitness,
    assert_legacy_scale_up_allowed,
    load_global_execution_witness,
    parse_global_execution_witness_export,
)
from loom_control_plane.slurm_worker_jobs import slurm_cluster_for_pool
from loom_control_plane.worker_pool_autoscaler import (
    reconcile_worker_pool_autoscaler_once,
)

_MAX_PORT_FORWARD_READY_TIMEOUT_SEC = 60.0
_MAX_PORT_FORWARD_STOP_TIMEOUT_SEC = 30.0
_MAX_PORT_FORWARD_STARTUP_OUTPUT_BYTES = 16 * 1024
_SLURM_AUTHORITY_TIMEOUT_SEC = 10.0
_GLOBAL_EXECUTION_EXPORT_TIMEOUT_SEC = 15.0
_MAX_GLOBAL_EXECUTION_EXPORT_BYTES = 64 * 1024
_MAX_GLOBAL_EXECUTION_EXPORT_ERROR_BYTES = 16 * 1024
_KUBERNETES_NAME = re.compile(r"[a-z0-9](?:[-a-z0-9]*[a-z0-9])?")
_SLURM_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}")
_SHA256 = re.compile(r"[0-9a-f]{64}")


class ExternalAutoscalerError(RuntimeError):
    """Expected, non-secret failure that is safe to show to an operator."""


class ExternalAutoscalerConfigurationError(ExternalAutoscalerError, ValueError):
    """Raised when command-line transport configuration is unsafe."""


class DatabasePortForwardError(ExternalAutoscalerError):
    """Raised when the owned database tunnel cannot be proven healthy."""


class ExternalPolicyValidationError(ExternalAutoscalerError):
    """Raised when requested external policies are missing or ambiguous."""


class SlurmAuthorityValidationError(ExternalAutoscalerError):
    """Raised when the local Slurm submit authority is not the expected one."""


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


@dataclass(frozen=True)
class SlurmPolicyAuthority:
    cluster_name: str
    controller_host: str


@dataclass(frozen=True)
class SlurmAuthority(SlurmPolicyAuthority):
    local_hostname: str


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
    parser.add_argument("--expected-slurm-cluster-name", required=True)
    parser.add_argument("--expected-slurm-controller-host", required=True)
    parser.add_argument("--scontrol", default="/usr/bin/scontrol")
    parser.add_argument("--namespace", default="loom-staging")
    parser.add_argument(
        "--kubeconfig",
        default=os.environ.get("KUBECONFIG", str(Path.home() / ".kube" / "config")),
    )
    parser.add_argument("--kubectl", default="/usr/local/bin/kubectl")
    parser.add_argument("--db-secret-name", default="loom-secrets")
    parser.add_argument("--db-secret-key", default="cp-db-url")
    parser.add_argument("--db-local-host", default="127.0.0.1")
    parser.add_argument("--db-local-port", type=int, default=15447)
    parser.add_argument("--db-service", default="service/loom-postgres-rw")
    parser.add_argument("--db-remote-port", type=int, default=5432)
    parser.add_argument("--db-port-forward-ready-timeout-sec", type=float, default=10.0)
    parser.add_argument("--db-port-forward-stop-timeout-sec", type=float, default=5.0)
    parser.add_argument("--db-connect-timeout-sec", type=float, default=10.0)
    parser.add_argument("--freshness-sec", type=int, default=120)
    parser.add_argument(
        "--capacity-grants-json",
        type=Path,
        help="Global development-fleet autoscaler report for hard grant ceilings.",
    )
    parser.add_argument(
        "--deployment-generation",
        type=int,
        help="Exact deployment generation bound to the capacity grant.",
    )
    witness_source = parser.add_mutually_exclusive_group(required=True)
    witness_source.add_argument(
        "--global-execution-witness-json",
        type=Path,
        help="Pinned-key manager witness required before local scale-up.",
    )
    witness_source.add_argument(
        "--global-execution-manager-export",
        metavar="DEPLOYMENT",
        help="Fetch a fresh witness from the protected capacity-manager deployment.",
    )
    parser.add_argument("--global-execution-manager-namespace")
    parser.add_argument("--global-execution-manager-kubeconfig")
    parser.add_argument("--manager-public-key", type=Path)
    manager_pin = parser.add_mutually_exclusive_group(required=True)
    manager_pin.add_argument("--expected-manager-public-key-sha256")
    manager_pin.add_argument("--expected-manager-public-key-sha256-file", type=Path)
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help=(
            "Validate the exact DB secret, tunnel, and external-policy presence "
            "without reconciling or invoking an actuator."
        ),
    )
    return parser


def _stop_global_execution_export(process: subprocess.Popen[bytes]) -> None:
    if process.poll() is not None:
        process.wait(timeout=0)
        return
    try:
        process.terminate()
    except OSError:
        pass
    else:
        try:
            process.wait(timeout=1.0)
            return
        except subprocess.TimeoutExpired:
            pass
    try:
        process.kill()
    except OSError:
        pass
    try:
        process.wait(timeout=1.0)
    except subprocess.TimeoutExpired:
        pass


def _run_bounded_global_execution_export(
    command: list[str],
) -> subprocess.CompletedProcess[bytes]:
    process: subprocess.Popen[bytes] | None = None
    completed = False
    try:
        process = subprocess.Popen(
            command,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        if process.stdout is None or process.stderr is None:  # pragma: no cover
            raise OSError("global execution witness export pipes are unavailable")
        stdout = bytearray()
        stderr = bytearray()
        streams = {
            process.stdout: (stdout, _MAX_GLOBAL_EXECUTION_EXPORT_BYTES),
            process.stderr: (stderr, _MAX_GLOBAL_EXECUTION_EXPORT_ERROR_BYTES),
        }
        deadline = time.monotonic() + _GLOBAL_EXECUTION_EXPORT_TIMEOUT_SEC
        with selectors.DefaultSelector() as selector:
            for stream, stream_state in streams.items():
                os.set_blocking(stream.fileno(), False)
                selector.register(stream, selectors.EVENT_READ, stream_state)
            while selector.get_map():
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise subprocess.TimeoutExpired(
                        command,
                        _GLOBAL_EXECUTION_EXPORT_TIMEOUT_SEC,
                    )
                events = selector.select(remaining)
                if not events:
                    raise subprocess.TimeoutExpired(
                        command,
                        _GLOBAL_EXECUTION_EXPORT_TIMEOUT_SEC,
                    )
                for key, _mask in events:
                    buffer, maximum_bytes = key.data
                    try:
                        chunk = os.read(
                            key.fd,
                            min(64 * 1024, maximum_bytes + 1 - len(buffer)),
                        )
                    except BlockingIOError:
                        continue
                    if not chunk:
                        selector.unregister(key.fileobj)
                        continue
                    buffer.extend(chunk)
                    if len(buffer) > maximum_bytes:
                        raise OverflowError(
                            "global execution witness export exceeded its output bound"
                        )
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise subprocess.TimeoutExpired(
                command,
                _GLOBAL_EXECUTION_EXPORT_TIMEOUT_SEC,
            )
        returncode = process.wait(timeout=remaining)
        completed = True
        return subprocess.CompletedProcess(
            command,
            returncode,
            bytes(stdout),
            bytes(stderr),
        )
    except (OSError, OverflowError, subprocess.SubprocessError) as exc:
        raise GlobalExecutionFenceError(
            "global execution witness export is unavailable"
        ) from exc
    finally:
        if process is not None:
            if not completed:
                _stop_global_execution_export(process)
            if process.stdout is not None:
                process.stdout.close()
            if process.stderr is not None:
                process.stderr.close()


def _load_current_global_execution_witness(
    args: argparse.Namespace,
    *,
    pool_id: str,
) -> GlobalExecutionWitness | None:
    if pool_id not in {"gb10", "oldlab"}:
        raise ExternalAutoscalerConfigurationError(
            "global execution witness pool must be gb10 or oldlab"
        )
    if args.global_execution_manager_export is not None:
        fingerprint = args.expected_manager_public_key_sha256
        if (
            args.global_execution_manager_export != "deployment/loom-capacity-manager"
            or args.global_execution_witness_json is not None
            or args.manager_public_key is not None
            or args.expected_manager_public_key_sha256_file is not None
            or args.global_execution_manager_namespace is None
            or args.global_execution_manager_kubeconfig is None
            or not isinstance(fingerprint, str)
            or _SHA256.fullmatch(fingerprint) is None
        ):
            raise ExternalAutoscalerConfigurationError(
                "manager export requires exactly one reviewed public key fingerprint"
            )
        kubectl = str(args.kubectl).strip()
        kubeconfig = str(args.global_execution_manager_kubeconfig).strip()
        if not kubectl or not kubeconfig:
            raise ExternalAutoscalerConfigurationError(
                "manager export Kubernetes transport is invalid"
            )
        namespace = _validated_kubernetes_name(
            args.global_execution_manager_namespace,
            "--global-execution-manager-namespace",
        )
        command = [
            kubectl,
            "--kubeconfig",
            kubeconfig,
            "--request-timeout=10s",
            "-n",
            namespace,
            "exec",
            args.global_execution_manager_export,
            "-c",
            "manager",
            "--",
            "python",
            "-I",
            "-B",
            "-m",
            "loom_capacity_manager.global_execution_witness",
            "--pool-id",
            pool_id,
        ]
        result = _run_bounded_global_execution_export(command)
        if (
            result.returncode != 0
            or not isinstance(result.stdout, bytes)
            or not isinstance(result.stderr, bytes)
            or not 0 < len(result.stdout) <= _MAX_GLOBAL_EXECUTION_EXPORT_BYTES
            or len(result.stderr) > _MAX_GLOBAL_EXECUTION_EXPORT_ERROR_BYTES
        ):
            raise GlobalExecutionFenceError(
                "global execution witness export is unavailable"
            )
        return parse_global_execution_witness_export(
            result.stdout,
            expected_manager_public_key_sha256=fingerprint,
        )
    if (
        args.global_execution_witness_json is None
        or args.manager_public_key is None
        or args.global_execution_manager_namespace is not None
        or args.global_execution_manager_kubeconfig is not None
    ):
        raise ExternalAutoscalerConfigurationError(
            "global execution witness file source is incomplete"
        )
    return load_global_execution_witness(
        args.global_execution_witness_json,
        manager_public_key_path=args.manager_public_key,
        expected_manager_public_key_sha256=args.expected_manager_public_key_sha256,
        expected_manager_public_key_sha256_file=(
            args.expected_manager_public_key_sha256_file
        ),
    )


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


def _validated_slurm_identifier(value: object, field: str) -> str:
    if not isinstance(value, str) or _SLURM_IDENTIFIER.fullmatch(value) is None:
        raise ExternalAutoscalerConfigurationError(f"{field} must be an exact Slurm identifier")
    return value


def _validated_scontrol_path(value: object) -> str:
    if not isinstance(value, str):
        raise ExternalAutoscalerConfigurationError("--scontrol must be an absolute path")
    parsed = Path(value)
    if not parsed.is_absolute() or str(parsed) != value or ".." in parsed.parts:
        raise ExternalAutoscalerConfigurationError("--scontrol must be an absolute path")
    return value


def _validate_local_slurm_authority(args: argparse.Namespace) -> SlurmAuthority:
    expected_cluster = _validated_slurm_identifier(
        args.expected_slurm_cluster_name,
        "--expected-slurm-cluster-name",
    )
    expected_controller = _validated_slurm_identifier(
        args.expected_slurm_controller_host,
        "--expected-slurm-controller-host",
    )
    scontrol = _validated_scontrol_path(args.scontrol)
    try:
        result = subprocess.run(
            [scontrol, "show", "config"],
            capture_output=True,
            check=False,
            text=True,
            timeout=_SLURM_AUTHORITY_TIMEOUT_SEC,
        )
        local_hostname = socket.gethostname()
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise SlurmAuthorityValidationError("local Slurm authority is unavailable") from exc
    if result.returncode != 0:
        raise SlurmAuthorityValidationError("local Slurm authority is unavailable")

    clusters: list[str] = []
    controllers: list[str] = []
    for line in result.stdout.splitlines():
        key, separator, raw_value = line.partition("=")
        if not separator:
            continue
        normalized_key = key.strip()
        value = raw_value.strip()
        if normalized_key == "ClusterName":
            clusters.append(value)
        elif normalized_key == "SlurmctldHost" or normalized_key.startswith("SlurmctldHost["):
            controllers.append(value.partition("(")[0].strip())

    if (
        clusters != [expected_cluster]
        or controllers != [expected_controller]
        or local_hostname != expected_controller
    ):
        raise SlurmAuthorityValidationError("local Slurm authority does not match expectation")
    return SlurmAuthority(
        cluster_name=expected_cluster,
        controller_host=expected_controller,
        local_hostname=local_hostname,
    )


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


def _load_capacity_grants(args: argparse.Namespace) -> dict[Any, Any] | None:
    path = args.capacity_grants_json
    generation = args.deployment_generation
    if (path is None) != (generation is None):
        raise ExternalAutoscalerConfigurationError(
            "--capacity-grants-json and --deployment-generation must be provided together"
        )
    if path is None:
        return None
    if isinstance(generation, bool) or not isinstance(generation, int) or generation <= 0:
        raise ExternalAutoscalerConfigurationError(
            "--deployment-generation must be a positive integer"
        )
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            raise GlobalDevAutoscalerError("capacity grant report must be an object")
        return capacity_grants_from_report(raw)
    except (OSError, json.JSONDecodeError, GlobalDevAutoscalerError) as exc:
        raise ExternalAutoscalerConfigurationError(
            "capacity grant report is unavailable or invalid"
        ) from exc


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
    authority: SlurmPolicyAuthority,
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
    authority_mismatches: set[str] = set()
    for row in result.scalars().all():
        actuator_config = row.actuator_config if isinstance(row.actuator_config, dict) else {}
        if actuator_config.get("external_runner") is True:
            if (
                actuator_config.get("slurm_cluster_name") != authority.cluster_name
                or actuator_config.get("slurm_controller_host") != authority.controller_host
            ):
                authority_mismatches.add(row.pool_name)
                continue
            counts[row.pool_name] += 1
    if authority_mismatches:
        raise ExternalPolicyValidationError(
            "requested external autoscaler policy has a foreign Slurm authority: "
            + ", ".join(sorted(authority_mismatches))
        )
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


async def _validate_external_policies_once(
    args: argparse.Namespace,
    *,
    authority: SlurmPolicyAuthority,
) -> list[dict[str, object]]:
    """Query exact external policies through the owned DB tunnel without mutation."""
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
                        return await _validate_requested_external_policies(
                            session,
                            environment=environment,
                            pool_names=pool_names,
                            authority=authority,
                        )
                    finally:
                        await session.rollback()
        finally:
            await engine.dispose()


async def _main_async(args: argparse.Namespace) -> None:
    environment = _scoped_environment(args.environment)
    pool_names = _scoped_pool_names(args.pool_name)
    capacity_grants = _load_capacity_grants(args)
    if capacity_grants is None and any(
        dev_pool_instance_name(pool_name) is not None for pool_name in pool_names
    ):
        raise ExternalAutoscalerConfigurationError(
            "dev pools require --capacity-grants-json and --deployment-generation"
        )
    port_forward = _database_port_forward_config(args)
    db_connect_timeout_sec = _validated_timeout(
        args.db_connect_timeout_sec,
        "--db-connect-timeout-sec",
        maximum=_MAX_PORT_FORWARD_READY_TIMEOUT_SEC,
    )
    slurm_authority = _validate_local_slurm_authority(args)
    physical_pool_ids = {slurm_cluster_for_pool(pool_name) for pool_name in pool_names}
    if len(physical_pool_ids) != 1:
        raise ExternalAutoscalerConfigurationError(
            "one supervisor cannot reconcile multiple physical Slurm pools"
        )
    physical_pool_id = next(iter(physical_pool_ids))
    if args.validate_only:
        validation = await _validate_external_policies_once(
            args,
            authority=slurm_authority,
        )
        try:
            global_execution_witness = _load_current_global_execution_witness(
                args,
                pool_id=physical_pool_id,
            )
            assert_legacy_scale_up_allowed(
                global_execution_witness,
                expected_authority="global-capacity-manager",
                expected_pool_id=physical_pool_id,
                now=datetime.now(UTC),
            )
        except GlobalExecutionFenceError as exc:
            raise ExternalAutoscalerError(
                "global execution witness is unavailable"
            ) from exc
        print(
            json.dumps(
                {
                    "mode": "validate-only",
                    "database_reachable": True,
                    "slurm_authority": {
                        "cluster_name": slurm_authority.cluster_name,
                        "controller_host": slurm_authority.controller_host,
                        "local_hostname": slurm_authority.local_hostname,
                    },
                    "pools": validation,
                },
                sort_keys=True,
            )
        )
        return
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
                            authority=slurm_authority,
                        )
                    finally:
                        await session.rollback()
                try:
                    global_execution_witness = _load_current_global_execution_witness(
                        args,
                        pool_id=physical_pool_id,
                    )
                except Exception:
                    # Evidence failure is not allowed to skip the reciprocal
                    # zero-capacity drain/release reconciliation below.
                    global_execution_witness = None
                    witness_failed = True
                else:
                    witness_failed = global_execution_witness is None
                    if global_execution_witness is not None:
                        try:
                            for pool_name in pool_names:
                                assert_legacy_scale_up_allowed(
                                    global_execution_witness,
                                    expected_authority="global-capacity-manager",
                                    expected_pool_id=slurm_cluster_for_pool(pool_name),
                                    now=datetime.now(UTC),
                                )
                        except GlobalExecutionFenceError:
                            witness_failed = True
                reconcile_kwargs: dict[str, Any] = {
                    "environment": environment,
                    "freshness_sec": args.freshness_sec,
                    "include_external_policies": True,
                    "external_only": True,
                    "pool_names": pool_names,
                    "global_execution_witness": global_execution_witness,
                }
                if capacity_grants is not None:
                    reconcile_kwargs["capacity_grants"] = capacity_grants
                    reconcile_kwargs["deployment_generation"] = args.deployment_generation
                decisions = await reconcile_worker_pool_autoscaler_once(
                    session,
                    **reconcile_kwargs,
                )
                await session.commit()
            print(json.dumps([decision.__dict__ for decision in decisions], default=str))
        finally:
            await engine.dispose()
    if witness_failed:
        raise ExternalAutoscalerError("global execution witness is unavailable")


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
