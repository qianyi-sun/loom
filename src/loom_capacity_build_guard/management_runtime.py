"""Installation-scoped native management loops; health is not build readiness."""

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Literal

from sqlalchemy.exc import DBAPIError
from sqlalchemy.exc import TimeoutError as DatabasePoolTimeout
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from loom_capacity_agent.client import DemandPublishError, DemandReporterClient
from loom_capacity_build_guard.bootstrap_store import BuildBootstrapCoordinator
from loom_capacity_build_guard.coordinator import BuildPlanCoordinator
from loom_capacity_build_guard.demand_store import BuildDemandCoordinator
from loom_capacity_build_guard.installation_store import (
    BuildGuardInstallationV1,
    RetainedBuildInstallation,
)
from loom_capacity_build_guard.recovery import BuildRecoveryCoordinator
from loom_capacity_build_guard.release_outbox import BuildReleaseCoordinator
from loom_capacity_build_guard.terminal_recovery import BuildTerminalRecoveryCoordinator
from loom_capacity_manager.contracts import canonical_bytes
from loom_capacity_manager.executable_contracts import ExecutableAdmissionPlanClosureV2

logger = logging.getLogger(__name__)
Stage = Literal["terminal", "release", "retirement", "demand", "bootstrap", "admission"]


@dataclass(frozen=True, slots=True)
class BuildManagementPass:
    """Diagnostics only; never membership activation or execution attestation."""

    failed_stages: tuple[Stage, ...]


class BuildManagementRuntime:
    """Compose durable protocols while keeping cleanup independent of intake.

    The service owns the session factory, exact installed scope and authenticated
    manager connection. Run this only from trusted management, not build workers.
    No stage grants source access or publishes candidate success.
    """

    def __init__(self, *, session_factory: async_sessionmaker[AsyncSession], installation: RetainedBuildInstallation,
        manager: DemandReporterClient, configuration_generation: int,
    ) -> None:
        document = BuildGuardInstallationV1.model_validate_json(installation.wire_payload)
        if document != installation.document or canonical_bytes(document) != installation.wire_payload:
            raise ValueError("build management installation changed")
        if type(configuration_generation) is not int or not 0 < configuration_generation < 2**63:
            raise ValueError("build management configuration generation is invalid")
        self._manager = manager
        self._generation = configuration_generation
        self._terminal = BuildTerminalRecoveryCoordinator(session_factory=session_factory, installation=installation, manager=manager)
        self._release = BuildReleaseCoordinator(session_factory=session_factory, installation=installation, publisher=manager)
        self._retirement = BuildRecoveryCoordinator(session_factory=session_factory, installation=installation, manager=manager)
        self._demand = BuildDemandCoordinator(session_factory, installation=installation)
        self._bootstrap = BuildBootstrapCoordinator(session_factory, installation=installation, publisher=manager)
        self._plans = BuildPlanCoordinator(session_factory, installation=installation, publisher=manager)
        self._lock = asyncio.Lock()

    async def _recover_terminal(self) -> bool:
        return all(result.state != "failed" for result in await self._terminal.reconcile())

    async def _publish_release(self) -> bool:
        await self._release.publish_next()
        return True

    async def _retire(self) -> bool:
        return all(result.state != "failed" for result in await self._retirement.reconcile())

    async def _publish_demand(self) -> bool:
        await self._demand.capture(configuration_generation=self._generation)
        await self._demand.publish_latest(self._manager)
        return True

    async def _protect_bootstrap(self) -> bool:
        proposal = await self._manager.next_executable_bootstrap()
        if proposal is not None:
            await self._bootstrap.register(proposal)
            await self._bootstrap.publish(proposal.binding.intent_id)
        return True

    async def _converge_plan(self, *, allow_new: bool) -> bool:
        work = await self._manager.next_executable_admission_plan()
        if isinstance(work, ExecutableAdmissionPlanClosureV2):
            await self._plans.reconcile_closure(work)
        if allow_new:
            # Retry already prepared work even when the manager has accepted its
            # acknowledgement and removed it from the queue. Do this before new
            # preparation so a lost reply remains a visible failure for this pass.
            recovered = await self._plans.publish_pending()
            if work is not None and not isinstance(work, ExecutableAdmissionPlanClosureV2):
                await self._plans.converge(work)
            return recovered
        return True

    async def run_once(self, *, admission_enabled: bool) -> BuildManagementPass:
        if type(admission_enabled) is not bool:
            raise ValueError("build management intake mode must be explicit")
        async with self._lock:
            failures: list[Stage] = []

            async def stage(name: Stage, action: Callable[[], Awaitable[bool]]) -> None:
                try:
                    async with asyncio.timeout(60):
                        success = await action()
                    if not success:
                        failures.append(name)
                except (DemandPublishError, DBAPIError, DatabasePoolTimeout, ValueError, TimeoutError):
                    failures.append(name)
                # Unexpected programming errors and cancellation propagate.

            await stage("terminal", self._recover_terminal)
            await stage("release", self._publish_release)
            await stage("retirement", self._retire)
            await stage("demand", self._publish_demand)
            allow_new = admission_enabled and not failures
            if allow_new:
                await stage("bootstrap", self._protect_bootstrap)
            # Closure must still run after intake closes or another stage fails.
            await stage("admission", lambda: self._converge_plan(allow_new=allow_new and "bootstrap" not in failures))
            return BuildManagementPass(tuple(failures))

    async def run_forever(self, *, admission_enabled: Callable[[], bool], poll_interval_seconds: float = 5) -> None:
        if (isinstance(poll_interval_seconds, bool) or not isinstance(poll_interval_seconds, (int, float))
            or not 0.05 <= poll_interval_seconds <= 60):
            raise ValueError("build management poll interval must be between 0.05 and 60 seconds")
        while True:
            result = await self.run_once(admission_enabled=admission_enabled())
            if result.failed_stages:
                logger.warning("native build management stages unavailable: %s", ",".join(result.failed_stages))
            await asyncio.sleep(poll_interval_seconds)
