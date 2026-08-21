"""Worker-side sampling and pre-delete accounting finalization (#1503)."""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import logging
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, fields
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any

from loom.driver.base import Driver, DriverResourceSnapshot, ExecHandle, StartOptions
from loom.driver.docker import read_container_cgroup_v2_files, snapshot_from_docker_stats
from loom.models.exec import ExecResult
from loom.models.healthcheck import HealthcheckSpec
from loom.models.networking import NetworkPolicy
from loom.models.resource_usage import (
    ResourceCounters,
    ResourceLimits,
    TrialResourceUsageReport,
)

logger = logging.getLogger(__name__)

UsageSink = Callable[[TrialResourceUsageReport, bool], Awaitable[None]]
_COUNTER_NAMES = tuple(field.name for field in fields(DriverResourceSnapshot))
_COUNTER_NAMES = tuple(
    name
    for name in _COUNTER_NAMES
    if name not in {"observed_at", "source", "runtime_id", "image_digest", "container_started_at"}
)
_GAUGE_NAMES = frozenset({"memory_current_bytes", "pids_current"})


@dataclass
class ResourceUsageAccumulator:
    first_observed_at: datetime
    last_observed_at: datetime
    source: str = "unsupported"
    runtime_id: str | None = None
    image_digest: str | None = None
    container_started_at: datetime | None = None
    observation_seq: int = 0
    diagnostic_code: str | None = None
    _values: dict[str, int | None] = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        if self._values is None:
            self._values = {name: None for name in _COUNTER_NAMES}

    def observe(self, snapshot: DriverResourceSnapshot) -> None:
        self.observation_seq += 1
        self.last_observed_at = max(self.last_observed_at, snapshot.observed_at)
        self.source = snapshot.source
        self.runtime_id = snapshot.runtime_id or self.runtime_id
        self.image_digest = snapshot.image_digest or self.image_digest
        self.container_started_at = snapshot.container_started_at or self.container_started_at
        for name in _COUNTER_NAMES:
            incoming = getattr(snapshot, name)
            if incoming is None:
                continue
            current = self._values[name]
            # Current gauges are the latest observation. Everything else is a
            # cumulative counter or sampled peak and must never decrease.
            if name in _GAUGE_NAMES:
                self._values[name] = incoming
            else:
                self._values[name] = incoming if current is None else max(current, incoming)
        memory_current = self._values["memory_current_bytes"]
        memory_peak = self._values["memory_peak_bytes"]
        if memory_current is not None:
            self._values["memory_peak_bytes"] = (
                memory_current if memory_peak is None else max(memory_peak, memory_current)
            )
        pids_current = self._values["pids_current"]
        pids_peak = self._values["pids_peak"]
        if pids_current is not None:
            self._values["pids_peak"] = (
                pids_current if pids_peak is None else max(pids_peak, pids_current)
            )

    def counters(self) -> ResourceCounters:
        return ResourceCounters.model_validate(self._values)

    def has_measurement(self) -> bool:
        return any(value is not None for value in self._values.values())


class ResourceAccountingDriver:
    """Driver decorator that journals observations and finalizes before stop."""

    def __init__(
        self,
        inner: Driver,
        *,
        trial_id: object,
        attempt_count: int,
        worker_id: object,
        execution_key: str,
        container_role: str,
        role_name: str,
        architecture: str | None,
        candidate_sha: str | None,
        sink: UsageSink,
        sample_interval_sec: float = 5.0,
    ) -> None:
        self.inner = inner
        self.capabilities = inner.capabilities
        self.os = inner.os
        self.trial_id = trial_id
        self.attempt_count = attempt_count
        self.worker_id = worker_id
        self.execution_key = execution_key
        self.container_role = container_role
        self.role_name = role_name
        self.architecture = architecture
        self.candidate_sha = candidate_sha or None
        self.sink = sink
        self.sample_interval_sec = sample_interval_sec
        now = datetime.now(UTC)
        self.accumulator = ResourceUsageAccumulator(now, now)
        self._limits = ResourceLimits()
        self._sampler: asyncio.Task[None] | None = None
        self._finalized = False

    def __getattr__(self, name: str) -> object:
        # Preserve backend-specific read-only introspection used by diagnostics
        # and contract tests while lifecycle/IO methods remain explicit below.
        return getattr(self.inner, name)

    async def start(self, *, options: StartOptions | None = None) -> None:
        opts = options or StartOptions()
        self._limits = _limits_from_options(opts)
        try:
            await self.inner.start(options=options)
        except BaseException:
            self.accumulator.diagnostic_code = "container_start_failed"
            await self._emit(final=True, terminal_reason="start_failed")
            raise
        await self._sample()
        self._sampler = asyncio.create_task(
            self._sample_loop(),
            name=f"resource-usage-{self.execution_key[:12]}",
        )

    async def stop(self, *, delete: bool = True) -> None:
        if not self._finalized:
            sampler = self._sampler
            self._sampler = None
            if sampler is not None:
                sampler.cancel()
                try:
                    await sampler
                except asyncio.CancelledError:
                    pass
            await self._sample()
            await self._emit(final=True, terminal_reason="container_stopped")
            self._finalized = True
        await self.inner.stop(delete=delete)

    async def resource_snapshot(self) -> DriverResourceSnapshot | None:
        return await self.inner.resource_snapshot()

    async def _sample_loop(self) -> None:
        while True:
            await asyncio.sleep(self.sample_interval_sec)
            await self._sample()

    async def _sample(self) -> None:
        try:
            snapshot = await self.inner.resource_snapshot()
        except Exception:
            self.accumulator.diagnostic_code = "snapshot_failed"
            logger.warning("resource_snapshot_failed", exc_info=True)
            await self._emit(final=False, terminal_reason=None)
            return
        if snapshot is not None:
            self.accumulator.observe(snapshot)
        await self._emit(final=False, terminal_reason=None)

    async def _emit(self, *, final: bool, terminal_reason: str | None) -> None:
        report = self.report(final=final, terminal_reason=terminal_reason)
        try:
            await self.sink(report, final)
        except Exception:
            # Resource telemetry must not change the user's trial result. The
            # sink stages locally before remote delivery and reports its own
            # bounded failure metric.
            logger.warning("resource_usage_sink_failed", exc_info=True)

    def report(
        self,
        *,
        final: bool,
        terminal_reason: str | None,
    ) -> TrialResourceUsageReport:
        has_snapshot = self.accumulator.has_measurement()
        diagnostic = self.accumulator.diagnostic_code
        completeness = "complete" if final and has_snapshot and diagnostic is None else "partial"
        if final and not has_snapshot:
            completeness = "unavailable"
            diagnostic = diagnostic or "backend_telemetry_unavailable"
        return TrialResourceUsageReport(
            trial_id=self.trial_id,
            attempt_count=self.attempt_count,
            worker_id=self.worker_id,
            execution_key=self.execution_key,
            runtime_id_hash=(
                hashlib.sha256(self.accumulator.runtime_id.encode()).hexdigest()
                if self.accumulator.runtime_id
                else None
            ),
            container_role=self.container_role,
            role_name=self.role_name,
            backend=self.capabilities.backend,
            architecture=self.architecture,
            candidate_sha=self.candidate_sha,
            image_digest=_normalized_image_digest(self.accumulator.image_digest),
            source=(self.accumulator.source if has_snapshot else "unsupported"),
            observation_seq=self.accumulator.observation_seq,
            container_started_at=self.accumulator.container_started_at,
            first_observed_at=self.accumulator.first_observed_at,
            last_observed_at=self.accumulator.last_observed_at,
            finalized_at=(
                max(datetime.now(UTC), self.accumulator.last_observed_at) if final else None
            ),
            terminal_reason=terminal_reason,
            completeness=completeness,
            diagnostic_code=diagnostic,
            limits=self._limits,
            counters=self.accumulator.counters(),
        )

    async def exec(
        self,
        cmd: str,
        *,
        user: str | int | None = None,
        cwd: PurePosixPath | None = None,
        env: Mapping[str, str] | None = None,
        timeout_sec: float | None = None,
    ) -> ExecResult:
        return await self.inner.exec(cmd, user=user, cwd=cwd, env=env, timeout_sec=timeout_sec)

    async def exec_streaming(
        self,
        argv: list[str],
        *,
        env_vars: dict[str, str],
        cwd: PurePosixPath,
        user: str | int | None = None,
    ) -> ExecHandle:
        return await self.inner.exec_streaming(argv, env_vars=env_vars, cwd=cwd, user=user)

    async def upload(self, src: Path, dst: PurePosixPath) -> None:
        await self.inner.upload(src, dst)

    async def download(self, src: PurePosixPath, dst: Path) -> None:
        await self.inner.download(src, dst)

    async def set_network_policy(self, policy: NetworkPolicy) -> None:
        await self.inner.set_network_policy(policy)

    async def run_healthcheck(self, hc: HealthcheckSpec | None = None) -> None:
        await self.inner.run_healthcheck(hc)


class DockerContainerResourceMonitor:
    """Accounting monitor for task-sidecar containers owned outside Driver."""

    def __init__(
        self,
        container: Any,
        *,
        trial_id: object,
        attempt_count: int,
        worker_id: object,
        execution_key: str,
        role_name: str,
        architecture: str | None,
        candidate_sha: str | None,
        limits: ResourceLimits,
        sink: UsageSink,
        sample_interval_sec: float = 5.0,
    ) -> None:
        self.container = container
        self.trial_id = trial_id
        self.attempt_count = attempt_count
        self.worker_id = worker_id
        self.execution_key = execution_key
        self.role_name = role_name
        self.architecture = architecture
        self.candidate_sha = candidate_sha or None
        self.limits = limits
        self.sink = sink
        self.sample_interval_sec = sample_interval_sec
        now = datetime.now(UTC)
        self.accumulator = ResourceUsageAccumulator(now, now)
        self._sampler: asyncio.Task[None] | None = None

    async def start(self) -> None:
        await self._sample()
        self._sampler = asyncio.create_task(
            self._sample_loop(),
            name=f"resource-sidecar-{self.execution_key[:12]}",
        )

    async def stop(self) -> None:
        sampler = self._sampler
        self._sampler = None
        if sampler is not None:
            sampler.cancel()
            try:
                await sampler
            except asyncio.CancelledError:
                pass
        await self._sample()
        await self._emit(final=True)

    async def _sample_loop(self) -> None:
        while True:
            await asyncio.sleep(self.sample_interval_sec)
            await self._sample()

    async def _sample(self) -> None:
        try:
            stats = await asyncio.to_thread(
                self.container.stats,
                stream=False,
                one_shot=True,
            )
            cgroup_files = await asyncio.to_thread(
                read_container_cgroup_v2_files,
                self.container,
            )
            with contextlib.suppress(Exception):
                await asyncio.to_thread(self.container.reload)
            attrs = self.container.attrs if isinstance(self.container.attrs, dict) else {}
            self.accumulator.observe(
                snapshot_from_docker_stats(
                    stats if isinstance(stats, dict) else {},
                    attrs=attrs,
                    observed_at=datetime.now(UTC),
                    cgroup_files=cgroup_files,
                )
            )
        except Exception:
            self.accumulator.diagnostic_code = "snapshot_failed"
        await self._emit(final=False)

    async def _emit(self, *, final: bool) -> None:
        has_snapshot = self.accumulator.has_measurement()
        diagnostic = self.accumulator.diagnostic_code
        completeness = "complete" if final and has_snapshot and diagnostic is None else "partial"
        if final and not has_snapshot:
            completeness = "unavailable"
            diagnostic = diagnostic or "backend_telemetry_unavailable"
        report = TrialResourceUsageReport(
            trial_id=self.trial_id,
            attempt_count=self.attempt_count,
            worker_id=self.worker_id,
            execution_key=self.execution_key,
            runtime_id_hash=(
                hashlib.sha256(self.accumulator.runtime_id.encode()).hexdigest()
                if self.accumulator.runtime_id
                else None
            ),
            container_role="sidecar",
            role_name=self.role_name,
            backend="docker",
            architecture=self.architecture,
            candidate_sha=self.candidate_sha,
            image_digest=_normalized_image_digest(self.accumulator.image_digest),
            source=(self.accumulator.source if has_snapshot else "unsupported"),
            observation_seq=self.accumulator.observation_seq,
            container_started_at=self.accumulator.container_started_at,
            first_observed_at=self.accumulator.first_observed_at,
            last_observed_at=self.accumulator.last_observed_at,
            finalized_at=(
                max(datetime.now(UTC), self.accumulator.last_observed_at) if final else None
            ),
            terminal_reason="container_stopped" if final else None,
            completeness=completeness,
            diagnostic_code=diagnostic,
            limits=self.limits,
            counters=self.accumulator.counters(),
        )
        try:
            await self.sink(report, final)
        except Exception:
            logger.warning("sidecar_resource_usage_sink_failed", exc_info=True)


def _limits_from_options(options: StartOptions) -> ResourceLimits:
    cpu_values = [value for value in (options.cpus, options.container_cpus) if value and value > 0]
    memory_values = [
        value
        for value in (options.memory_mb, options.container_memory_mib)
        if value is not None and value > 0
    ]
    return ResourceLimits(
        cpu_cores=min(cpu_values) if cpu_values else None,
        memory_bytes=min(memory_values) * 1024 * 1024 if memory_values else None,
        pids=options.container_pids if options.container_pids > 0 else None,
    )


def _normalized_image_digest(value: str | None) -> str | None:
    if not value:
        return None
    digest = value.lower()
    if digest.startswith("sha256:"):
        digest = digest[7:]
    if len(digest) == 64 and all(ch in "0123456789abcdef" for ch in digest):
        return f"sha256:{digest}"
    return None


def execution_key(*parts: object) -> str:
    material = "\x00".join(str(part) for part in parts)
    return hashlib.sha256(material.encode("utf-8")).hexdigest()
