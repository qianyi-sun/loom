"""DaytonaDriver — Driver Protocol over the Daytona AsyncSandbox API.

Spec mapping:
- start() → AsyncDaytona.create(CreateSandboxFromImageParams(image=...))
- stop()  → AsyncDaytona.delete(sandbox, timeout=cfg.delete_timeout_sec)
- exec()  → sandbox.process.exec(command). Daytona merges stdout+stderr in
            `result`; we put it in ExecResult.stdout, stderr=b"".
- exec_streaming() → see exec_stream.open_session_stream
- upload/download → sandbox.fs.upload_file / download_file (single-file)
- set_network_policy() → sandbox.update_network_settings; domain-only
                         policies use Daytona's network-layer domain firewall.
                         Mixed domain/CIDR policies resolve on the trusted
                         worker because Daytona modes are mutually exclusive.
- run_healthcheck() → same loop pattern as DockerDriver.run_healthcheck

Lifecycle invariants follow the Driver Protocol spec:
- start() once per instance; second call raises DriverAlreadyStartedError.
- stop() idempotent; safe before start() (no-op).
- exec/upload/download require state == 'running'.
"""

from __future__ import annotations

import asyncio
import ipaddress
import logging
import socket
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path, PurePosixPath
from typing import Any, Literal
from uuid import UUID

from loom.driver.base import (
    MAX_EXEC_STREAM_BYTES,
    DriverResourceSnapshot,
    ExecHandle,
    StartOptions,
)
from loom.errors import (
    DriverAlreadyStartedError,
    DriverError,
    DriverNotStartedError,
)
from loom.models.capabilities import Capabilities
from loom.models.exec import ExecResult
from loom.models.healthcheck import HealthcheckSpec
from loom.models.networking import Allowlist, NetworkPolicy, Public
from loom.models.types import OS
from loom.security.redaction import redact_environment_mapping, redact_text
from loom_drivers.daytona.client import DaytonaClient
from loom_drivers.daytona.config import DaytonaConfig
from loom_drivers.daytona.exec_stream import open_session_stream
from loom_drivers.daytona.network import DaytonaNetworkArgs, to_daytona_network_args
from loom_drivers.daytona.registry import get_process_registry
from loom_drivers.daytona.service_controller import DaytonaApiGate
from loom_drivers.daytona.usage import (
    DEFAULT_PER_SECOND_USD,
    compute_record,
    persist_record,
)

logger = logging.getLogger(__name__)

ReserveCallback = Callable[[], Awaitable[Mapping[str, Any]]]
StartedCallback = Callable[[UUID, str, datetime], Awaitable[None]]
DeletedCallback = Callable[[UUID, bool, datetime, str | None], Awaitable[None]]


async def _resolve_public_ipv4(domain: str) -> tuple[str, ...]:
    def resolve() -> tuple[str, ...]:
        addresses = socket.getaddrinfo(
            domain,
            443,
            family=socket.AF_INET,
            type=socket.SOCK_STREAM,
        )
        return tuple(
            sorted(
                {
                    str(item[4][0])
                    for item in addresses
                    if ipaddress.ip_address(str(item[4][0])).is_global
                }
            )
        )

    return await asyncio.to_thread(resolve)


def _default_caps() -> Capabilities:
    return Capabilities(
        backend="daytona",
        os="linux",
        gpu_vendor="none",
        network_policies=frozenset(["public", "no-network", "allowlist"]),
        dynamic_network_policy=True,
        mounted_fs=True,
        resource_modes=frozenset(["auto"]),
    )


@dataclass
class DaytonaDriver:
    image: str
    config: DaytonaConfig
    workspace: PurePosixPath = field(
        default_factory=lambda: PurePosixPath("/workspace"),
    )
    capabilities: Capabilities = field(default_factory=_default_caps)
    os: OS = "linux"
    network_policy_baseline: NetworkPolicy = field(default_factory=Public)
    trial_id: UUID | None = None
    team_id: UUID | None = None
    session_factory: Callable[[], Any] | None = None
    sandbox_name: str | None = None
    candidate_sha: str | None = None
    provider_scope: str | None = None
    attempt_count: int | None = None
    api_gate: DaytonaApiGate | None = None
    reserve_callback: ReserveCallback | None = None
    started_callback: StartedCallback | None = None
    deleted_callback: DeletedCallback | None = None
    per_second_usd: Decimal = field(default=DEFAULT_PER_SECOND_USD)
    allow_public_network: bool = True
    allowed_network_domains: frozenset[str] | None = None
    allow_network_cidrs: bool = True
    require_scoped_gateway_credentials: bool = False
    _client: DaytonaClient | None = field(default=None, init=False, repr=False)
    _sandbox: Any | None = field(default=None, init=False, repr=False)
    _started_at: datetime | None = field(default=None, init=False, repr=False)
    _sandbox_id: str | None = field(default=None, init=False, repr=False)
    _ledger_id: UUID | None = field(default=None, init=False, repr=False)
    _state: Literal["constructed", "running", "stopped"] = field(
        default="constructed",
        init=False,
    )

    @property
    def state(self) -> str:
        return self._state

    @asynccontextmanager
    async def _provider_slot(self) -> AsyncIterator[None]:
        if self.api_gate is None:
            yield
            return
        async with self.api_gate.slot():
            yield

    async def start(self, *, options: StartOptions | None = None) -> None:
        if self._state != "constructed":
            raise DriverAlreadyStartedError(
                f"DaytonaDriver.start() rejected in state {self._state!r}",
            )
        opts = options or StartOptions()
        if self.require_scoped_gateway_credentials:
            sensitive_names = sorted(
                redact_text(entry.name)
                for entry in redact_environment_mapping(dict(opts.environment))
                if entry.sensitive
            )
            if sensitive_names:
                raise DriverError(
                    "DAYTONA_RAW_SECRET_DENIED: sandbox startup environment "
                    "contains secret-bearing variables: " + ", ".join(sensitive_names)
                )
        network_args = await self._network_args(self.network_policy_baseline)
        if (
            any(value is not None for value in (opts.cpus, opts.memory_mb, opts.storage_mb))
            or opts.gpus
        ):
            raise DriverError(
                "DaytonaDriver cannot enforce task resource limits; use a "
                "compatible backend instead",
            )
        if (
            opts.network is not None
            or opts.volumes
            or opts.extra_hosts
            or opts.dns
            or opts.tmpfs
            or opts.container_cpus > 0
            or opts.container_memory_mib > 0
            or opts.container_pids > 0
            or opts.cgroup_parent is not None
            or opts.slurm_allocated_gpus >= 0
            or opts.slurm_gpu_device_ids
        ):
            raise DriverError(
                "DaytonaDriver cannot honor Docker/Slurm-specific StartOptions; "
                "route this workload to a compatible backend",
            )
        existing_sandbox_id: str | None = None
        if self.reserve_callback is not None:
            reservation = await self.reserve_callback()
            self._ledger_id = UUID(str(reservation["id"]))
            existing_sandbox_id = (
                str(reservation["sandbox_id"])
                if reservation.get("sandbox_id") is not None
                else None
            )
        self._client = DaytonaClient(self.config)
        await self._client.open()
        try:
            from daytona import CreateSandboxFromImageParams, DaytonaError

            assert self._client is not None
            client = self._client
            lookup_ref = existing_sandbox_id or self.sandbox_name
            if lookup_ref is not None:
                try:
                    async with self._provider_slot():
                        self._sandbox = await client.sdk.get(lookup_ref)
                except DaytonaError as exc:
                    if exc.status_code != 404:
                        raise
            if self._sandbox is None:
                labels = {
                    key: value
                    for key, value in {
                        "loom.trial_id": str(self.trial_id) if self.trial_id else None,
                        "loom.team_id": str(self.team_id) if self.team_id else None,
                        "loom.candidate_sha": self.candidate_sha,
                        "loom.image": self.image,
                    }.items()
                    if value is not None
                }
                if self.require_scoped_gateway_credentials:
                    labels["loom.security_profile"] = "gateway-only-v1"
                labels.update(dict(opts.labels))
                params = CreateSandboxFromImageParams(
                    image=self.image,
                    name=self.sandbox_name,
                    labels=labels or None,
                    env_vars=dict(opts.environment) or None,
                    network_block_all=network_args.network_block_all,
                    network_allow_list=network_args.network_allow_list,
                    domain_allow_list=network_args.domain_allow_list,
                    # Loom owns deadline, TTL and cleanup reconciliation. A
                    # provider idle heuristic must never kill a background job.
                    auto_stop_interval=0 if self.reserve_callback is not None else None,
                    auto_pause_interval=0 if self.reserve_callback is not None else None,
                    auto_delete_interval=-1 if self.reserve_callback is not None else None,
                )
                try:
                    async with self._provider_slot():
                        self._sandbox = await client.with_retry(
                            lambda: client.sdk.create(params),
                        )
                except DaytonaError as create_error:
                    # A timed-out create response may still have committed on
                    # the provider. The deterministic name is the idempotency
                    # key: re-read it before allowing the trial to fail.
                    if self.sandbox_name is None:
                        raise
                    for recovery_attempt in range(3):
                        try:
                            async with self._provider_slot():
                                self._sandbox = await client.sdk.get(self.sandbox_name)
                            break
                        except DaytonaError as lookup_error:
                            if lookup_error.status_code != 404:
                                raise
                            if recovery_attempt < 2:
                                await asyncio.sleep(0.5 * (recovery_attempt + 1))
                    if self._sandbox is None:
                        raise create_error
            sandbox = self._sandbox
            assert sandbox is not None  # narrows for mypy
            if self.require_scoped_gateway_credentials and (
                getattr(sandbox, "labels", {}).get("loom.security_profile")
                != "gateway-only-v1"
            ):
                raise DriverError(
                    "DAYTONA_SECURITY_PROFILE_MISSING: refusing to reuse an "
                    "unlabelled sandbox"
                )
            get_process_registry().register(self._client.sdk, sandbox)
            self._state = "running"
            self._started_at = datetime.now(tz=UTC)
            self._sandbox_id = sandbox.id
            if self.started_callback is not None and self._ledger_id is not None:
                await self.started_callback(
                    self._ledger_id,
                    self._sandbox_id,
                    self._started_at,
                )
        except BaseException:
            await self._teardown(delete=True)
            self._state = "stopped"
            raise

    async def stop(self, *, delete: bool = True) -> None:
        await self._teardown(delete=delete)
        if self._state == "running":
            self._state = "stopped"

    async def resource_snapshot(self) -> DriverResourceSnapshot | None:
        # Daytona's current API exposes billed lifetime but not per-sandbox
        # CPU/RSS/PID/I/O counters. The accounting wrapper persists a typed
        # unavailable record instead of manufacturing zeros.
        return None

    async def _teardown(self, *, delete: bool) -> None:
        if (
            self._sandbox is not None
            and self._client is not None
            and self._started_at is not None
            and self._sandbox_id is not None
            and self.trial_id is not None
            and self.team_id is not None
            and self.session_factory is not None
            and self.deleted_callback is None
        ):
            try:
                stopped_at = datetime.now(tz=UTC)
                rec = compute_record(
                    team_id=self.team_id,
                    trial_id=self.trial_id,
                    sandbox_id=self._sandbox_id,
                    image=self.image,
                    started_at=self._started_at,
                    stopped_at=stopped_at,
                    per_second_usd=self.per_second_usd,
                )
                async with self.session_factory() as s:
                    await persist_record(s, rec)
                    await s.commit()
            except Exception:
                logger.warning(
                    "DaytonaDriver: usage persistence failed",
                    exc_info=True,
                )
        if self._sandbox is not None and self._client is not None and delete:
            sandbox = self._sandbox
            delete_succeeded = False
            try:
                async with self._provider_slot():
                    await asyncio.wait_for(
                        self._client.sdk.delete(
                            sandbox,
                            timeout=self.config.delete_timeout_sec,
                        ),
                        timeout=self.config.delete_timeout_sec + 5.0,
                    )
                delete_succeeded = True
            except Exception as exc:
                delete_error = f"{type(exc).__name__}: {exc}"
                logger.warning(
                    "DaytonaDriver: delete failed for sandbox %s; "
                    "leaving in registry for atexit/SIGINT retry",
                    sandbox.id,
                    exc_info=True,
                )
            # Only unregister on success — otherwise atexit/SIGINT
            # cleanup paths get a second chance to delete the sandbox
            # so we don't pay for it until Daytona's auto-stop fires
            # (default 30 min).
            if delete_succeeded:
                get_process_registry().unregister(sandbox)
            if self.deleted_callback is not None and self._ledger_id is not None:
                try:
                    await self.deleted_callback(
                        self._ledger_id,
                        delete_succeeded,
                        datetime.now(tz=UTC),
                        None if delete_succeeded else delete_error,
                    )
                except Exception:
                    logger.warning(
                        "DaytonaDriver: durable delete report failed",
                        exc_info=True,
                    )
        self._sandbox = None
        if self._client is not None:
            await self._client.close()
            self._client = None

    def _require_running(self) -> Any:
        if self._state != "running" or self._sandbox is None:
            raise DriverNotStartedError(
                f"DaytonaDriver in state {self._state!r}",
            )
        return self._sandbox

    async def exec(
        self,
        cmd: str,
        *,
        user: str | int | None = None,
        cwd: PurePosixPath | None = None,
        env: Mapping[str, str] | None = None,
        timeout_sec: float | None = None,
    ) -> ExecResult:
        sb = self._require_running()
        self._validate_exec_credentials(command=cmd, env=dict(env or {}))
        loop = asyncio.get_running_loop()
        started = loop.time()
        resp = await sb.process.exec(
            cmd,
            cwd=str(cwd) if cwd is not None else None,
            env=dict(env) if env is not None else None,
            timeout=int(timeout_sec) if timeout_sec is not None else None,
        )
        duration = loop.time() - started

        stdout = (resp.result or "").encode("utf-8", errors="replace")
        truncated = False
        if len(stdout) > MAX_EXEC_STREAM_BYTES:
            stdout = stdout[:MAX_EXEC_STREAM_BYTES]
            truncated = True
        return ExecResult(
            return_code=int(resp.exit_code),
            stdout=stdout,
            stderr=b"",
            truncated=truncated,
            duration_sec=duration,
        )

    async def exec_streaming(
        self,
        argv: list[str],
        *,
        env_vars: dict[str, str],
        cwd: PurePosixPath,
        user: str | int | None = None,
    ) -> ExecHandle:
        sb = self._require_running()
        self._validate_exec_credentials(command=" ".join(argv), env=env_vars)
        return await open_session_stream(
            sandbox=sb,
            argv=argv,
            env_vars=env_vars,
            cwd=cwd,
            user=user,
        )

    async def upload(self, src: Path, dst: PurePosixPath) -> None:
        sb = self._require_running()
        if not src.is_file():
            raise FileNotFoundError(
                f"upload source {src} is not a regular file",
            )
        data = src.read_bytes()
        await sb.fs.upload_file(data, str(dst))

    async def download(self, src: PurePosixPath, dst: Path) -> None:
        sb = self._require_running()
        data = await sb.fs.download_file(str(src))
        dst.parent.mkdir(parents=True, exist_ok=True)
        dst.write_bytes(data)

    async def set_network_policy(self, policy: NetworkPolicy) -> None:
        sb = self._require_running()
        args = await self._network_args(policy)
        await sb.update_network_settings(
            network_block_all=args.network_block_all,
            network_allow_list=args.network_allow_list,
            domain_allow_list=args.domain_allow_list,
        )
        self.network_policy_baseline = policy

    async def _network_args(self, policy: NetworkPolicy) -> DaytonaNetworkArgs:
        if isinstance(policy, Public) and not self.allow_public_network:
            raise DriverError(
                "DAYTONA_NETWORK_POLICY_DENIED: public internet access is disabled"
            )
        if isinstance(policy, Allowlist):
            normalized = frozenset(domain.rstrip(".").lower() for domain in policy.domains)
            if (
                self.allowed_network_domains is not None
                and not normalized.issubset(self.allowed_network_domains)
            ):
                raise DriverError(
                    "DAYTONA_NETWORK_POLICY_DENIED: allowlist contains an "
                    "unreviewed domain"
                )
            if policy.cidrs and not self.allow_network_cidrs:
                raise DriverError(
                    "DAYTONA_NETWORK_POLICY_DENIED: arbitrary CIDRs are disabled"
                )
        resolved: dict[str, tuple[str, ...]] = {}
        if isinstance(policy, Allowlist) and policy.domains and policy.cidrs:
            for domain in policy.domains:
                ips = await _resolve_public_ipv4(domain)
                if not ips:
                    raise DriverError(
                        "DAYTONA_NETWORK_POLICY_DENIED: allowlist domain did not "
                        "resolve to a public IPv4 address"
                    )
                resolved[domain] = ips
        try:
            return to_daytona_network_args(
                policy,
                resolved_domain_ips=resolved,
            )
        except ValueError as exc:
            raise DriverError(str(exc)) from exc

    def _validate_exec_credentials(self, *, command: str, env: Mapping[str, str]) -> None:
        if not self.require_scoped_gateway_credentials:
            return
        allowed_tokens: set[str] = set()
        sensitive_names: list[str] = []
        for entry in redact_environment_mapping(env):
            if not entry.sensitive:
                continue
            value = str(env[entry.name])
            if value.startswith("loom_step_"):
                allowed_tokens.add(value)
            else:
                sensitive_names.append(redact_text(entry.name))
        if sensitive_names:
            raise DriverError(
                "DAYTONA_RAW_SECRET_DENIED: command environment contains "
                "non-scoped credentials: " + ", ".join(sorted(sensitive_names))
            )
        scrubbed = command
        for token in allowed_tokens:
            scrubbed = scrubbed.replace(token, "[LOOM_SCOPED_STEP_TOKEN]")
        if redact_text(scrubbed) != scrubbed:
            raise DriverError(
                "DAYTONA_RAW_SECRET_DENIED: command arguments contain a raw credential"
            )

    async def run_healthcheck(
        self,
        hc: HealthcheckSpec | None = None,
    ) -> None:
        self._require_running()
        if hc is None:
            return
        loop = asyncio.get_running_loop()
        deadline_start_period = loop.time() + hc.start_period_sec
        consecutive_failures = 0
        while True:
            in_grace = loop.time() < deadline_start_period
            try:
                r = await self.exec(hc.command, timeout_sec=hc.timeout_sec)
                if r.return_code == 0:
                    return
            except TimeoutError:
                pass
            if not in_grace:
                consecutive_failures += 1
                if consecutive_failures > hc.retries:
                    raise DriverError(
                        f"Healthcheck failed after {hc.retries} consecutive "
                        f"retries: {hc.command!r}",
                    )
            await asyncio.sleep(hc.interval_sec)
