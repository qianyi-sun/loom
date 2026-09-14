"""SQL and exact Kubernetes effects for the retained application migration.

Construction does not establish rollout authority. The enclosing installed
component supplies independently admitted target/credential/CA/artifact inputs,
checks them around every phase, and persists the returned backend before writes.
Context exit closes all privileged peers, including after transport failure.
"""

from __future__ import annotations

import base64
import secrets
import time
from collections.abc import Callable, Mapping, Sequence
from contextlib import AbstractContextManager, ExitStack
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, timedelta
from types import TracebackType
from typing import Protocol

from loom.application_completed_authority import ApplicationOwnerSuccessor
from loom.application_database_admission import (
    ApplicationDatabaseAdmissionTarget,
    ApplicationDatabaseCoordinationGuard,
    ApplicationDatabaseHandoffBackend,
)
from loom.application_database_connection import ApplicationDatabaseConnection
from loom.application_migrator_admission import (
    close_application_migrator_admission,
    reopen_application_migrator_admission,
)
from loom.application_migrator_provision import (
    arm_application_migrator,
    create_application_migrator,
    seal_application_migrator,
)
from loom.application_migrator_recovery import (
    observe_application_migration_backend,
    observe_application_migrator_role,
    require_application_migration_peer_retired,
)
from loom.application_migrator_retirement import retire_application_migrator

from .final_gate_plan import FinalGatePlan
from .protected_application_admission_recovery import _backend, _string
from .protected_application_migration_documents import (
    application_migration_documents,
    application_migration_role,
)
from .protected_application_migration_journal import ApplicationMigrationEvent
from .protected_application_migration_resources import (
    ApplicationMigrationResourceRunner,
    ProtectedApplicationMigrationResources,
)
from .staging_mutation_guard import MutationGuardEvidence


class ApplicationMigrationRunner(ApplicationMigrationResourceRunner, Protocol):
    def open_staging_peer_database(self) -> AbstractContextManager[ApplicationDatabaseConnection]: ...
    def open_staging_peer_maintenance_database(self) -> AbstractContextManager[ApplicationDatabaseConnection]: ...


@dataclass
class ProtectedApplicationMigrationRuntime:
    plan: FinalGatePlan
    guard: MutationGuardEvidence
    target: ApplicationDatabaseAdmissionTarget
    coordination_guard: ApplicationDatabaseCoordinationGuard
    runner: ApplicationMigrationRunner
    template: bytes = field(repr=False)
    ca_certificate: bytes = field(repr=False)
    runtime_password: str = field(repr=False)
    container_registry: str
    assert_guard: Callable[[], MutationGuardEvidence]
    assert_inputs: Callable[[], None]
    intent_digest: str
    provisioner_role: str = "postgres"
    _creator_stack: ExitStack = field(default_factory=ExitStack, init=False, repr=False)
    _maintenance_stack: ExitStack = field(default_factory=ExitStack, init=False, repr=False)
    _creator: ApplicationDatabaseConnection | None = field(default=None, init=False, repr=False)
    _maintenance: ApplicationDatabaseConnection | None = field(default=None, init=False, repr=False)

    def checkpoint(self) -> None:
        if (self.assert_guard() != self.guard or self.guard.state != "ready"
                or self.coordination_guard.backend.pid != self.guard.database_backend_pid
                or self.guard.request_id != self.plan.request_id
                or self.guard.candidate_sha != self.plan.candidate_sha or self.guard.candidate_tree != self.plan.candidate_tree):
            raise RuntimeError("application migration original supervised guard changed")
        self.assert_inputs()
        if self.assert_guard() != self.guard:
            raise RuntimeError("application migration guard changed during input admission")

    def prepare_generation(self, ordinal: int) -> Mapping[str, object]:
        self.checkpoint()
        self.release_creator()
        self._maintenance_stack.close()
        self._maintenance = None
        self._creator = self._creator_stack.enter_context(self.runner.open_staging_peer_database())
        backend = observe_application_migration_backend(self._creator, target=self.target,
            coordination_guard=self.coordination_guard, provisioner_role=self.provisioner_role, maintenance=False)
        self.checkpoint()
        return {"ordinal": ordinal, "nonce": secrets.token_hex(16), "password": secrets.token_urlsafe(48),
            "expires_at": (datetime.now(UTC) + timedelta(minutes=45)).isoformat(),
            "creation_backend": asdict(backend), "ca_certificate": base64.b64encode(self.ca_certificate).decode()}

    def _generation(self, generation: ApplicationMigrationEvent) -> None:
        self.checkpoint()
        if (generation.intent_digest != self.intent_digest or generation.guard_digest != self.guard.evidence_digest
                or generation.payload.get("ca_certificate") != base64.b64encode(self.ca_certificate).decode()):
            raise RuntimeError("application migration recorded generation inputs changed")
        application_migration_role(generation)

    def _creation_peer(self, generation: ApplicationMigrationEvent) -> ApplicationDatabaseConnection:
        self._generation(generation)
        if self._creator is None:
            raise RuntimeError("application migration original creation peer is absent")
        backend = observe_application_migration_backend(self._creator, target=self.target,
            coordination_guard=self.coordination_guard, provisioner_role=self.provisioner_role, maintenance=False)
        if backend != _backend(generation.payload["creation_backend"]):
            raise RuntimeError("application migration creation peer changed")
        return self._creator

    def create(self, generation: ApplicationMigrationEvent, persist: Callable[[int], None]) -> None:
        def publish(identity: ApplicationOwnerSuccessor) -> None:
            persist(identity.role_oid)
            self.checkpoint()
        create_application_migrator(self._creation_peer(generation), target=self.target,
            coordination_guard=self.coordination_guard, provisioner_role=self.provisioner_role,
            migrator_role=application_migration_role(generation), persist_identity=publish)
        self.checkpoint()

    def arm(self, generation: ApplicationMigrationEvent, oid: int) -> None:
        arm_application_migrator(self._creation_peer(generation), target=self.target,
            coordination_guard=self.coordination_guard, provisioner_role=self.provisioner_role,
            identity=ApplicationOwnerSuccessor(application_migration_role(generation), oid),
            password=_string(generation.payload, "password"), expires_at=datetime.fromisoformat(_string(generation.payload, "expires_at")))
        self.checkpoint()

    def resources(self, generation: ApplicationMigrationEvent) -> ProtectedApplicationMigrationResources:
        self._generation(generation)
        job, secret = application_migration_documents(self.plan, template=self.template, generation=generation,
            guard=self.guard, container_registry=self.container_registry)
        return ProtectedApplicationMigrationResources(runner=self.runner, job=job, secret=secret, guard=self.guard,
            assert_guard=self._resource_guard)

    def _resource_guard(self) -> MutationGuardEvidence:
        self.checkpoint()
        return self.guard

    def release_creator(self) -> None:
        self._creator_stack.close()
        self._creator = None

    def _maintenance_peer(self) -> ApplicationDatabaseConnection:
        if self._maintenance is None:
            self._maintenance = self._maintenance_stack.enter_context(self.runner.open_staging_peer_maintenance_database())
        return self._maintenance

    def begin_retirement(self, generation: ApplicationMigrationEvent,
                         peers: Sequence[ApplicationDatabaseHandoffBackend]) -> ApplicationDatabaseHandoffBackend:
        self._generation(generation)
        self.release_creator()
        connection = self._maintenance_peer()
        backend = observe_application_migration_backend(connection, target=self.target,
            coordination_guard=self.coordination_guard, provisioner_role=self.provisioner_role, maintenance=True)
        for previous in (_backend(generation.payload["creation_backend"]), *peers):
            if previous == backend:
                continue
            deadline = time.monotonic() + 35
            while True:
                self.checkpoint()
                try:
                    require_application_migration_peer_retired(connection, target=self.target,
                        coordination_guard=self.coordination_guard, provisioner_role=self.provisioner_role, backend=previous)
                    break
                except RuntimeError as exc:
                    if str(exc) != "application migration previous peer still exists" or time.monotonic() >= deadline:
                        raise
                    time.sleep(0.1)
        self.checkpoint()
        return backend

    def role_exists(self, generation: ApplicationMigrationEvent, oid: int | None) -> bool:
        self._generation(generation)
        observed = observe_application_migrator_role(self._maintenance_peer(), target=self.target,
            coordination_guard=self.coordination_guard, provisioner_role=self.provisioner_role,
            migrator_role=application_migration_role(generation), migrator_oid=oid)
        self.checkpoint()
        return observed

    def seal(self, generation: ApplicationMigrationEvent, oid: int) -> None:
        self._generation(generation)
        seal_application_migrator(self._maintenance_peer(), target=self.target,
            coordination_guard=self.coordination_guard, provisioner_role=self.provisioner_role,
            identity=ApplicationOwnerSuccessor(application_migration_role(generation), oid))
        self.checkpoint()

    def close(self, generation: ApplicationMigrationEvent, oid: int) -> None:
        self._generation(generation)
        close_application_migrator_admission(self._maintenance_peer(), target=self.target,
            coordination_guard=self.coordination_guard, provisioner_role=self.provisioner_role,
            identity=ApplicationOwnerSuccessor(application_migration_role(generation), oid))
        self.checkpoint()

    def retire(self, generation: ApplicationMigrationEvent, oid: int) -> None:
        self._generation(generation)
        retire_application_migrator(self._maintenance_peer(), target=self.target,
            coordination_guard=self.coordination_guard, provisioner_role=self.provisioner_role,
            migrator_role=application_migration_role(generation), migrator_oid=oid)
        self.checkpoint()

    def reopen(self, generation: ApplicationMigrationEvent, oid: int) -> None:
        self._generation(generation)
        reopen_application_migrator_admission(self._maintenance_peer(), target=self.target,
            coordination_guard=self.coordination_guard, provisioner_role=self.provisioner_role,
            identity=ApplicationOwnerSuccessor(application_migration_role(generation), oid), runtime_password=self.runtime_password)
        self.checkpoint()

    def read_revision(self) -> str:
        self.checkpoint()
        with self.runner.open_staging_peer_database() as peer:
            observe_application_migration_backend(peer, target=self.target, coordination_guard=self.coordination_guard,
                provisioner_role=self.provisioner_role, maintenance=False)
            with peer.transaction():
                peer.execute("SET TRANSACTION READ ONLY")
                peer.execute("SET LOCAL search_path=pg_catalog,pg_temp")
                peer.execute("SET LOCAL statement_timeout='30s'")
                rows = peer.execute("SELECT version_num FROM public.alembic_version").fetchall()
                if len(rows) != 1 or len(rows[0]) != 1 or rows[0][0] not in {self.plan.schema_revision, self.plan.migration_target_revision}:
                    raise RuntimeError("application migration schema revision is invalid")
                revision = str(rows[0][0])
        self.checkpoint()
        return revision

    def __enter__(self) -> ProtectedApplicationMigrationRuntime:
        self.checkpoint()
        return self

    def __exit__(self, exc_type: type[BaseException] | None, exc: BaseException | None, traceback: TracebackType | None) -> None:
        try:
            self.release_creator()
        finally:
            self._maintenance_stack.close()
            self._maintenance = None
