"""Restart-safe runtime for one independently installed personal-dev demand agent."""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Protocol

from pydantic import ValidationError
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from loom_capacity_agent.client import (
    DemandReporterClient,
    DemandReporterConnection,
    DemandReporterTLSFiles,
    read_owner_only_bytes,
)
from loom_capacity_agent.contracts import (
    AgentRegistrationV1,
    GuardLifecycleDemandObservationV2,
    ReporterConfigurationV1,
)
from loom_capacity_agent.reporter import build_lifecycle_demand_snapshot
from loom_capacity_agent.store import (
    CapacityAgentStoreError,
    capture_lifecycle_demand_observation,
    read_agent_lifecycle_demand_observation,
    read_agent_reporter_high_water,
)
from loom_capacity_manager.contracts import DemandSnapshotV1

logger = logging.getLogger(__name__)


class DemandPublisher(Protocol):
    async def publish(self, snapshot: DemandSnapshotV1) -> object: ...


Capture = Callable[..., Awaitable[GuardLifecycleDemandObservationV2]]
Recover = Callable[..., Awaitable[GuardLifecycleDemandObservationV2]]
ReadHighWater = Callable[..., Awaitable[int]]


def load_reporter_configuration(path: Path) -> ReporterConfigurationV1:
    """Load one exact owner-only trusted configuration file."""

    try:
        return ReporterConfigurationV1.model_validate_json(
            read_owner_only_bytes(path, max_bytes=1024 * 1024)
        )
    except (ValidationError, ValueError) as exc:
        raise ValueError("trusted reporter configuration is invalid") from exc


def load_database_url(path: Path) -> str:
    """Load a single owner-only PostgreSQL URL without normalizing its credential."""

    try:
        value = read_owner_only_bytes(path).decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError("capacity agent database URL is not UTF-8") from exc
    if value.endswith("\n"):
        value = value[:-1]
    if not value or value != value.strip() or any(item in value for item in ("\r", "\n", "\x00")):
        raise ValueError("capacity agent database URL must contain one exact line")
    parsed = make_url(value)
    if not parsed.drivername.startswith("postgresql") or not parsed.username or not parsed.database:
        raise ValueError("capacity agent database URL must be a role-scoped PostgreSQL URL")
    return value


class CapacityAgentRuntime:
    """Capture, durably replay, and publish one complete protected view at a time."""

    def __init__(
        self,
        *,
        configuration: ReporterConfigurationV1,
        session_factory: async_sessionmaker[AsyncSession],
        publisher: DemandPublisher,
        max_attempts: int,
        capture: Capture = capture_lifecycle_demand_observation,
        recover: Recover = read_agent_lifecycle_demand_observation,
        read_high_water: ReadHighWater = read_agent_reporter_high_water,
    ) -> None:
        if not 1 <= max_attempts <= 10_000:
            raise ValueError("capacity capture bound must be between 1 and 10000")
        self._configuration = configuration
        self._registration = AgentRegistrationV1.model_validate(
            {
                field: getattr(configuration, field)
                for field in AgentRegistrationV1.model_fields
            }
        )
        self._session_factory = session_factory
        self._publisher = publisher
        self._max_attempts = max_attempts
        self._capture = capture
        self._recover = recover
        self._read_high_water = read_high_water
        self._high_water = 0
        self._pending: DemandSnapshotV1 | None = None
        self._initialized = False
        self.ready = False

    async def initialize(self) -> None:
        """Recover the durable publication edge before advertising readiness."""

        self._initialized = False
        self.ready = False
        async with self._session_factory() as session, session.begin():
            high_water = await self._read_high_water(
                session,
                registration=self._registration,
            )
            observation = (
                await self._recover(
                    session,
                    registration=self._registration,
                    sequence=high_water,
                )
                if high_water > 0
                else None
            )
        self._high_water = high_water
        self._pending = (
            build_lifecycle_demand_snapshot(observation, self._configuration)
            if observation is not None
            and observation.configuration_generation
            == self._configuration.configuration_generation
            and observation.deployment_generation == self._configuration.deployment_generation
            and observation.reporter_incarnation == self._configuration.reporter_incarnation
            and observation.candidate_digest == self._configuration.candidate_digest
            else None
        )
        self._initialized = True

    async def run_once(self) -> None:
        """Publish a recovered view or capture and publish exactly one new view."""

        if not self._initialized:
            raise CapacityAgentStoreError("capacity agent is not initialized")
        try:
            if self._pending is None:
                async with self._session_factory() as session, session.begin():
                    observation = await self._capture(
                        session,
                        registration=self._registration,
                        expected_high_water=self._high_water,
                        max_attempts=self._max_attempts,
                    )
                self._high_water = observation.sequence
                self._pending = build_lifecycle_demand_snapshot(
                    observation,
                    self._configuration,
                )
            await self._publisher.publish(self._pending)
        except BaseException:
            self.ready = False
            raise
        self._pending = None
        self.ready = True

    async def run_forever(self, *, poll_interval_seconds: float) -> None:
        if not 0 < poll_interval_seconds <= 300:
            raise ValueError("capacity agent poll interval must be between 0 and 300 seconds")
        await self.initialize()
        while True:
            try:
                await self.run_once()
            except asyncio.CancelledError:
                raise
            except CapacityAgentStoreError as exc:
                self._initialized = False
                self.ready = False
                logger.error(
                    "capacity_agent_protected_state_error",
                    extra={"error_type": type(exc).__name__},
                )
                await asyncio.sleep(poll_interval_seconds)
                await self.initialize()
                continue
            except Exception as exc:
                self.ready = False
                logger.error(
                    "capacity_agent_publish_iteration_failed",
                    extra={"error_type": type(exc).__name__},
                )
            await asyncio.sleep(poll_interval_seconds)


async def _health_response(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    *,
    runtime: CapacityAgentRuntime,
) -> None:
    try:
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(reader.read(4096), timeout=2)
        status = b"200 OK" if runtime.ready else b"503 Service Unavailable"
        body = b"ready\n" if runtime.ready else b"not-ready\n"
        writer.write(
            b"HTTP/1.1 "
            + status
            + b"\r\nContent-Type: text/plain\r\nContent-Length: "
            + str(len(body)).encode("ascii")
            + b"\r\nConnection: close\r\n\r\n"
            + body
        )
        await writer.drain()
    finally:
        writer.close()
        await writer.wait_closed()


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a trusted Loom capacity demand agent")
    parser.add_argument("--configuration-file", type=Path, required=True)
    parser.add_argument("--database-url-file", type=Path, required=True)
    parser.add_argument("--manager-origin", required=True)
    parser.add_argument("--bearer-token-file", type=Path, required=True)
    parser.add_argument("--ca-file", type=Path, required=True)
    parser.add_argument("--certificate-file", type=Path, required=True)
    parser.add_argument("--private-key-file", type=Path, required=True)
    parser.add_argument("--poll-interval-seconds", type=float, default=5.0)
    parser.add_argument("--max-attempts", type=int, default=10_000)
    parser.add_argument("--health-port", type=int, default=8081)
    return parser


async def _main_async(arguments: argparse.Namespace) -> None:
    configuration = load_reporter_configuration(arguments.configuration_file)
    engine = create_async_engine(load_database_url(arguments.database_url_file))
    publisher = DemandReporterClient.from_files(
        configuration,
        DemandReporterConnection(
            manager_origin=arguments.manager_origin,
            bearer_token_file=arguments.bearer_token_file,
            tls_files=DemandReporterTLSFiles(
                ca_file=arguments.ca_file,
                certificate_file=arguments.certificate_file,
                private_key_file=arguments.private_key_file,
            ),
        ),
    )
    runtime = CapacityAgentRuntime(
        configuration=configuration,
        session_factory=async_sessionmaker(engine, expire_on_commit=False),
        publisher=publisher,
        max_attempts=arguments.max_attempts,
    )
    server = await asyncio.start_server(
        lambda reader, writer: _health_response(reader, writer, runtime=runtime),
        host="0.0.0.0",
        port=arguments.health_port,
    )
    try:
        async with server:
            await runtime.run_forever(poll_interval_seconds=arguments.poll_interval_seconds)
    finally:
        await publisher.aclose()
        await engine.dispose()


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    asyncio.run(_main_async(_parser().parse_args()))


if __name__ == "__main__":  # pragma: no cover - module entry point
    main()


__all__ = [
    "CapacityAgentRuntime",
    "DemandPublisher",
    "load_database_url",
    "load_reporter_configuration",
    "main",
]
