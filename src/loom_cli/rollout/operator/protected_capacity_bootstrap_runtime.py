"""Capacity bootstrap effects beneath the original retained migration journal.

The installed component admits original role OIDs, seed and ownership terminals.
This adapter preserves permanent roles and only promotes ordinary credentials
once the independently observed desired database configuration is exact.
"""

from __future__ import annotations

import hashlib
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import datetime

from psycopg import sql

from loom.application_capacity_runtime_credentials import (
    _credentials,
    _roles,
    arm_application_capacity_runtime_credentials,
    finalize_application_capacity_runtime_credentials,
)
from loom.application_completed_authority import ApplicationOwnerSuccessor
from loom.application_guard_migrator_provision import (
    arm_application_guard_migrator,
    seal_application_guard_migrator,
)
from loom.application_guard_migrator_retirement import (
    _memberships,
    _require_roles,
    close_application_guard_migrator_admission,
    reopen_application_guard_migrator_admission,
    require_application_guard_migrator_retired,
    retire_application_guard_migrator,
)
from loom.application_migrator_provision import _transaction

from .protected_application_admission_recovery import _string
from .protected_application_migration_journal import ApplicationMigrationEvent
from .protected_application_migration_resources import ProtectedApplicationMigrationResources
from .protected_application_migration_runtime import ProtectedApplicationMigrationRuntime
from .protected_capacity_bootstrap_resources import capacity_bootstrap_resources
from .protected_staging_capacity_database_component import (
    KubernetesProtectedStagingCapacityDatabaseComponent,
    _DatabaseState,
    _seed_credential,
)


@dataclass(kw_only=True)
class ProtectedCapacityBootstrapRuntime(ProtectedApplicationMigrationRuntime):
    base: KubernetesProtectedStagingCapacityDatabaseComponent
    seed: Mapping[str, object] = field(repr=False)
    identity: ApplicationOwnerSuccessor
    runtime_role_oids: Mapping[str, int]
    initial_database_state: _DatabaseState = _DatabaseState.NEEDS_CONVERGENCE
    rebind_sha256: str | None = None

    def _identity(self, generation: ApplicationMigrationEvent, oid: int | None) -> None:
        self._generation(generation)
        if (oid != self.identity.role_oid or self.identity.guard_owner is None
                or self.base.application_owner_role != self.target.successor_role
                or id(self.base.runner) != id(self.runner)):
            raise RuntimeError("application capacity original runtime binding changed")

    def _passwords(self) -> Mapping[str, str]:
        return {f"loom_cap_staging_{role}": _seed_credential(self.seed, f"{role}_database_password")
            for role in ("agent", "observer", "runtime")}

    def create(self, generation: ApplicationMigrationEvent, persist: Callable[[int], None]) -> None:
        self._identity(generation, self.identity.role_oid)
        peer = self._creation_peer(generation)
        require_application_guard_migrator_retired(peer, target=self.target,
            coordination_guard=self.coordination_guard, provisioner_role=self.provisioner_role, identity=self.identity)
        with _transaction(peer, self.target, self.coordination_guard, self.provisioner_role):
            _roles(peer, self.runtime_role_oids)
        persist(self.identity.role_oid)
        self.checkpoint()

    def arm(self, generation: ApplicationMigrationEvent, oid: int) -> None:
        self._identity(generation, oid)
        peer = self._creation_peer(generation)
        self._rebind(generation)
        expiry = datetime.fromisoformat(_string(generation.payload, "expires_at"))
        arm_application_capacity_runtime_credentials(peer, target=self.target,
            coordination_guard=self.coordination_guard, provisioner_role=self.provisioner_role,
            role_oids=self.runtime_role_oids, passwords=self._passwords(), expires_at=expiry)
        self.checkpoint()
        arm_application_guard_migrator(peer, target=self.target, coordination_guard=self.coordination_guard,
            provisioner_role=self.provisioner_role, identity=self.identity,
            password=_string(generation.payload, "password"), expires_at=expiry)
        self.checkpoint()

    def _rebind(self, generation: ApplicationMigrationEvent) -> None:
        states = {_DatabaseState.AUTHORITY_REBIND_REQUIRED, _DatabaseState.AUTHORITY_REBIND_RECOVERY_REQUIRED}
        state = self.base._database_state(self.plan, dict(self.seed))
        if state in states and self.initial_database_state not in states:
            raise RuntimeError("application capacity rebind was not originally admitted")
        if state == _DatabaseState.AUTHORITY_REBIND_REQUIRED:
            if self.initial_database_state != state or self.rebind_sha256 is None:
                raise RuntimeError("application capacity rebind original source changed")
            payload = self.base._legacy_authority_rebind_payload(self.plan, self.seed)
            if hashlib.sha256(payload).hexdigest() != self.rebind_sha256:
                raise RuntimeError("application capacity original rebind payload changed")
            # The creator identity and credential generation are durable already.
            # The unchanged certified transaction validates and locks all source
            # rows; a lost reply recovers its committed target before any rearm.
            self._creation_peer(generation).execute(sql.SQL(payload.decode("ascii")))
            self.checkpoint()
            state = self.base._database_state(self.plan, dict(self.seed))
            if state not in {_DatabaseState.EXACT, _DatabaseState.AUTHORITY_REBIND_RECOVERY_REQUIRED}:
                raise RuntimeError("application capacity rebind target was not certified")
        elif state not in {_DatabaseState.EXACT, _DatabaseState.NEEDS_CONVERGENCE, _DatabaseState.AUTHORITY_REBIND_RECOVERY_REQUIRED}:
            raise RuntimeError("application capacity rebind configuration drifted")
        self.checkpoint()

    def resources(self, generation: ApplicationMigrationEvent) -> ProtectedApplicationMigrationResources:
        self._identity(generation, self.identity.role_oid)
        return capacity_bootstrap_resources(base=self.base, plan=self.plan, seed=self.seed,
            generation=generation, guard=self.guard, assert_guard=self._resource_guard)

    def role_exists(self, generation: ApplicationMigrationEvent, oid: int | None) -> bool:
        self._identity(generation, oid)
        peer = self._maintenance_peer()
        with _transaction(peer, self.target, self.coordination_guard, self.provisioner_role, allow_closed=True):
            _require_roles(peer, self.target, self.identity, allow_migrator_login=True)
            _memberships(peer, self.target, self.identity)
        self.checkpoint()
        return True

    def require_role_retired(self, generation: ApplicationMigrationEvent, oid: int | None) -> None:
        self._identity(generation, oid)
        require_application_guard_migrator_retired(self._maintenance_peer(), target=self.target,
            coordination_guard=self.coordination_guard, provisioner_role=self.provisioner_role, identity=self.identity)
        self.checkpoint()

    def seal(self, generation: ApplicationMigrationEvent, oid: int) -> None:
        self._identity(generation, oid)
        seal_application_guard_migrator(self._maintenance_peer(), target=self.target,
            coordination_guard=self.coordination_guard, provisioner_role=self.provisioner_role, identity=self.identity)
        self.checkpoint()

    def close(self, generation: ApplicationMigrationEvent, oid: int) -> None:
        self._identity(generation, oid)
        close_application_guard_migrator_admission(self._maintenance_peer(), target=self.target,
            coordination_guard=self.coordination_guard, provisioner_role=self.provisioner_role, identity=self.identity)
        self.checkpoint()

    def retire(self, generation: ApplicationMigrationEvent, oid: int) -> None:
        self._identity(generation, oid)
        retire_application_guard_migrator(self._maintenance_peer(), target=self.target,
            coordination_guard=self.coordination_guard, provisioner_role=self.provisioner_role, identity=self.identity)
        self.checkpoint()

    def reopen(self, generation: ApplicationMigrationEvent, oid: int) -> None:
        self._identity(generation, oid)
        self.resources(generation).require_retired()
        reopen_application_guard_migrator_admission(self._maintenance_peer(), target=self.target,
            coordination_guard=self.coordination_guard, provisioner_role=self.provisioner_role,
            identity=self.identity, runtime_password=self.runtime_password)
        self.checkpoint()
        if self.read_revision() == "exact":
            return
        if self.base._database_state(self.plan, dict(self.seed), durable_runtime_credentials=False) == _DatabaseState.EXACT:
            self.checkpoint()
            finalize_application_capacity_runtime_credentials(self._maintenance_peer(), target=self.target,
                coordination_guard=self.coordination_guard, provisioner_role=self.provisioner_role,
                role_oids=self.runtime_role_oids, passwords=self._passwords())
            if self.read_revision() != "exact":
                raise RuntimeError("application capacity durable configuration changed")
        self.checkpoint()

    def read_revision(self) -> str:
        self.checkpoint()
        with self.runner.open_staging_peer_database() as peer:
            with _transaction(peer, self.target, self.coordination_guard, self.provisioner_role):
                roles = _roles(peer, self.runtime_role_oids)
                _credentials(roles, self._passwords(), require_login=False)
        state = self.base._database_state(self.plan, dict(self.seed))
        self.checkpoint()
        if state == _DatabaseState.EXACT:
            _credentials(roles, self._passwords(), require_login=True)
            return "exact"
        if state == _DatabaseState.NEEDS_CONVERGENCE:
            return "pending"
        if (state in {_DatabaseState.AUTHORITY_REBIND_REQUIRED, _DatabaseState.AUTHORITY_REBIND_RECOVERY_REQUIRED}
                and self.initial_database_state in {_DatabaseState.AUTHORITY_REBIND_REQUIRED, _DatabaseState.AUTHORITY_REBIND_RECOVERY_REQUIRED}):
            return "pending"
        raise RuntimeError("application capacity configuration drifted outside admitted bootstrap")
