"""Service-owned, installation-scoped native recovery; never an intake override."""

from __future__ import annotations

import asyncio
import hmac
import logging
import os
import tempfile
from contextlib import AsyncExitStack, suppress
from dataclasses import dataclass, field
from hashlib import sha256
from pathlib import Path
from typing import Annotated, Literal, Self

from pydantic import Field, field_validator, model_validator
from sqlalchemy import text

from loom_capacity_agent.client import (
    DemandReporterClient,
    DemandReporterConnection,
    DemandReporterTLSFiles,
    canonical_manager_origin,
    read_owner_only_bytes,
)
from loom_capacity_agent.contracts import ReporterConfigurationV1
from loom_capacity_build_guard.installation_store import (
    BuildGuardInstallationV1,
    RetainedBuildInstallation,
)
from loom_capacity_build_guard.management_runtime import BuildManagementRuntime
from loom_capacity_manager.contracts import Digest, StrictV1Model, canonical_bytes
from loom_service.config import LoomServiceSettings
from loom_service.personal_dev_build_admission import (
    PersonalBuildAdmissionRuntime,
    _assert_private_agent,
)

logger = logging.getLogger(__name__)
_MANAGEMENT_SIGNATURES = (
    "assert_management_installation(uuid,bytea)",
    "read_pending_native_workers(uuid,bigint,bigint,integer)",
    "import_terminal_inventory(uuid,jsonb,bytea,text)",
    "settle_interrupted_claim(uuid,jsonb,bytea,text)",
    "read_outcome(uuid,jsonb,bytea,text)",
    "release_terminal_worker(uuid,jsonb,bytea,text,text)",
    "read_next_protected_release(uuid)",
    "acknowledge_protected_release(uuid,jsonb,bytea,text,text)",
    "read_pending_retirements(uuid,bigint,bigint,integer)",
    "retire_request_hold(uuid,jsonb,bytea,text)",
    "capture_demand(uuid,bigint,jsonb)", "read_demand(uuid)", "read_pending_sources(uuid)",
    "close_plan(uuid,jsonb,bytea,text)", "authorize_closure_publication(uuid,uuid)",
)


class BuildManagementFileV1(StrictV1Model):
    path: str
    sha256: Digest

    @field_validator("path")
    @classmethod
    def _path(cls, value: str) -> str:
        path = Path(value)
        if not path.is_absolute() or value != str(path) or value == "/" or ".." in path.parts or "\0" in value:
            raise ValueError("build management file path must be canonical and absolute")
        return value

    def read(self) -> bytes:
        path = Path(self.path)
        wire = read_owner_only_bytes(path, max_bytes=1024*1024)
        if not hmac.compare_digest(sha256(wire).hexdigest(), self.sha256):
            raise ValueError("build management credential file changed")
        return wire


class BuildManagementScopeV1(StrictV1Model):
    installation: BuildGuardInstallationV1
    reporter: ReporterConfigurationV1
    manager_origin: str
    bearer_token: BuildManagementFileV1
    ca: BuildManagementFileV1
    certificate: BuildManagementFileV1
    private_key: BuildManagementFileV1

    @model_validator(mode="after")
    def _scope(self) -> Self:
        document, reporter = self.installation, self.reporter
        candidate = document.runtime.candidate
        if (reporter.subject_id != document.subject_id or reporter.subject_incarnation != document.subject_incarnation
            or reporter.reporter_incarnation != document.reporter_incarnation
            or reporter.deployment_generation != document.deployment_generation
            or reporter.protected_admission_sha256 != document.protected_admission_sha256
            or reporter.candidate_identity_algorithm != candidate.algorithm or reporter.candidate_identity != candidate.identity
            or reporter.candidate_publication_sha256 != candidate.publication_sha256):
            raise ValueError("build management reporter installation binding changed")
        canonical_manager_origin(self.manager_origin)
        return self


class BuildManagementServiceConfigV1(StrictV1Model):
    # Widening this requires the actual native source/execution/readiness wiring,
    # not a deployment toggle on an otherwise incomplete graph of consumers.
    mode: Literal["recovery-only"]
    scopes: Annotated[tuple[BuildManagementScopeV1, ...], Field(min_length=1, max_length=64)]

    @model_validator(mode="after")
    def _unique(self) -> Self:
        if (len({item.installation.id for item in self.scopes}) != len(self.scopes)
            or len({item.reporter.reporter_incarnation for item in self.scopes}) != len(self.scopes)):
            raise ValueError("build management installation or reporter is duplicated")
        return self


@dataclass(slots=True)
class PersonalBuildManagementServiceRuntime:
    managers: tuple[BuildManagementRuntime, ...]
    clients: tuple[DemandReporterClient, ...]
    _task: asyncio.Task[None] | None = field(default=None, init=False)
    _closed: bool = field(default=False, init=False)
    _close_lock: asyncio.Lock = field(default_factory=asyncio.Lock, init=False)

    async def _run(self) -> None:
        async with asyncio.TaskGroup() as group:
            for manager in self.managers:
                group.create_task(manager.run_forever(admission_enabled=lambda: False, poll_interval_seconds=5),
                    name="loom-native-build-scope-recovery")

    @staticmethod
    def _observe(task: asyncio.Task[None]) -> None:
        if not task.cancelled() and task.exception() is not None:
            logger.error("native build management recovery stopped unexpectedly")

    def start(self) -> None:
        if self._task is not None or self._closed:
            raise RuntimeError("build management already started or closed")
        self._task = asyncio.create_task(self._run(), name="loom-native-build-management")
        self._task.add_done_callback(self._observe)

    async def wait(self) -> None:
        if self._task is None:
            raise RuntimeError("build management has not started")
        await self._task

    async def aclose(self) -> None:
        async with self._close_lock:
            if self._closed:
                return
            if self._task is not None:
                self._task.cancel()
                with suppress(asyncio.CancelledError, Exception):
                    await self._task
            self._closed = True
            async with AsyncExitStack() as cleanup:
                for client in self.clients:
                    cleanup.push_async_callback(client.aclose)


async def build_personal_build_management_runtime(settings: LoomServiceSettings, *, admission: PersonalBuildAdmissionRuntime | None,
) -> PersonalBuildManagementServiceRuntime | None:
    path, expected = settings.personal_dev_build_management_config_file, settings.personal_dev_build_management_config_sha256
    if path is None and not expected:
        return None
    if path is None or not expected:
        raise ValueError("build management configuration requires path and digest")
    wire = read_owner_only_bytes(path, max_bytes=1024*1024)
    if not hmac.compare_digest(sha256(wire).hexdigest(), expected):
        raise ValueError("build management configuration digest changed")
    config = BuildManagementServiceConfigV1.model_validate_json(wire)
    if canonical_bytes(config) != wire:
        raise ValueError("build management configuration must be canonical")
    if admission is None or admission.mode not in {"native-claims", "native-source", "native-artifacts"}:
        raise ValueError("build management recovery requires private native claim admission")
    credentials = [(scope.bearer_token.read(), scope.ca.read(), scope.certificate.read(), scope.private_key.read())
        for scope in config.scopes]
    async with asyncio.timeout(30), admission.sessions.begin() as session:
        connection = await session.connection()
        await _assert_private_agent(connection, registration_enabled=True, claims_enabled=True,
            additional_signatures=_MANAGEMENT_SIGNATURES)
        for scope in config.scopes:
            installed = await session.scalar(text("SELECT loom_capacity_build_guard.assert_management_installation(:id,:wire)"),
                {"id": scope.installation.id, "wire": canonical_bytes(scope.installation)})
            if installed is not True:
                raise ValueError("build management installation is unavailable or changed")
    clients, managers = [], []
    async with AsyncExitStack() as cleanup:
        for scope, credential_bytes in zip(config.scopes, credentials, strict=True):
            # Construct TLS and bearer state from the bytes actually verified,
            # not a later reread of mutable source paths. The client loads all
            # state synchronously; temporary 0700/0600 files are then removed.
            with tempfile.TemporaryDirectory(prefix="loom-native-build-client-") as directory:
                paths = tuple(Path(directory) / name for name in ("token", "ca", "certificate", "key"))
                for credential_path, payload in zip(paths, credential_bytes, strict=True):
                    descriptor = os.open(credential_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
                    with os.fdopen(descriptor, "wb") as stream:
                        stream.write(payload)
                client = DemandReporterClient.from_files(scope.reporter, DemandReporterConnection(
                    manager_origin=scope.manager_origin, bearer_token_file=paths[0],
                    tls_files=DemandReporterTLSFiles(ca_file=paths[1], certificate_file=paths[2], private_key_file=paths[3])))
                cleanup.push_async_callback(client.aclose)
            clients.append(client)
            managers.append(BuildManagementRuntime(session_factory=admission.sessions,
                installation=RetainedBuildInstallation(scope.installation, canonical_bytes(scope.installation)),
                manager=client, configuration_generation=scope.reporter.configuration_generation))
        runtime = PersonalBuildManagementServiceRuntime(tuple(managers), tuple(clients))
        cleanup.pop_all()
        return runtime
