"""Retained capacity bootstrap following the completed ownership and migration.

Pending classification consults immutable history and external input bindings;
it never opens the potentially closed application database. Original role OIDs
are admitted before a credential generation and remain fixed through retirement.
"""

from __future__ import annotations

import hashlib
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass, field

from loom.application_capacity_runtime_credentials import _credentials, _roles
from loom.application_completed_authority import (
    ApplicationGuardOwner,
    ApplicationOwnerSuccessor,
    observe_completed_application_authority,
)
from loom.application_guard_migrator_retirement import require_application_guard_migrator_retired

from .protected_application_admission_recovery import (
    ApplicationAdmissionRecoveryRecord,
    _integer,
    admission_record_digest,
)
from .protected_application_handoff_component import _admit_sql_profiles
from .protected_application_migration_component import (
    ApplicationMigrationInputs,
    ProtectedApplicationMigrationComponent,
    _finished,
)
from .protected_application_migration_journal import ApplicationMigrationEvent
from .protected_application_migration_lifecycle import ApplicationMigrationLifecycle
from .protected_application_owner_preparation import APPLICATION_OWNER_ROLE, _observe
from .protected_apply_journal import ComponentTerminal, ProtectedApplyComponent
from .protected_capacity_bootstrap_resources import capacity_bootstrap_resources
from .protected_capacity_bootstrap_runtime import ProtectedCapacityBootstrapRuntime
from .protected_cnpg_sql_admission import require_cnpg_effective_sql_profile
from .protected_cnpg_writer_configuration import _mapping
from .protected_executor_admission_journal import ExecutorAdmissionRecord
from .protected_staging_capacity_database_component import (
    KubernetesProtectedStagingCapacityDatabaseComponent,
    _DatabaseState,
    _ResourceState,
    _seed_credential,
)
from .staging_mutation_guard import MutationGuardEvidence


@dataclass(frozen=True, slots=True)
class ProtectedCapacityBootstrapComponent(ProtectedApplicationMigrationComponent):
    base: KubernetesProtectedStagingCapacityDatabaseComponent
    seed_source: Callable[[], Mapping[str, object]] = field(repr=False)
    migration_source: Callable[[], tuple[Sequence[ApplicationMigrationEvent], ComponentTerminal]]

    executor_admission_source: Callable[[], tuple[ExecutorAdmissionRecord, bool] | None] | None = field(default=None, kw_only=True, repr=False)

    def component(self) -> ProtectedApplyComponent:
        return ProtectedApplyComponent("staging-capacity-database",
            hashlib.sha256(b"loom-protected-owner-capacity-bootstrap-v1").hexdigest(),
            admission_record_digest({"plan_digest": self.plan.plan_digest, "ordinal": self.ordinal,
                "container_registry": self.container_registry}), self.classify, self.apply)

    def _seed_digest(self) -> str:
        return admission_record_digest(self.seed_source())

    def _migration(self) -> tuple[Sequence[ApplicationMigrationEvent], ComponentTerminal]:
        events, terminal = self.migration_source()
        if (not _finished(events) or terminal.component_id != "database-migration"
                or terminal.intent_digest != events[0].intent_digest
                or terminal.observed_epoch != self.plan.starting_mutation_epoch + 1
                or terminal.evidence_digest != admission_record_digest({"migration_event_digest": events[-1].event_digest,
                    "target_revision": self.plan.migration_target_revision, "plan_digest": self.plan.plan_digest})):
            raise RuntimeError("application capacity original migration has not completed")
        return events, terminal

    def _context(self, events: Sequence[ApplicationMigrationEvent]) -> tuple[MutationGuardEvidence, ApplicationMigrationInputs]:
        context = super(ProtectedCapacityBootstrapComponent, self)._context(events)
        _, terminal = self._migration()
        seed_digest = self._seed_digest()
        if events and (events[0].payload["seed_digest"] != seed_digest
                or events[0].payload["migration_digest"] != terminal.terminal_digest):
            raise RuntimeError("application capacity original seed or migration changed")
        return context

    def _template(self) -> bytes:
        return self.base._manifest(self.plan, dict(self.seed_source()))

    def apply(self, plan: object) -> None:
        if plan != self.plan:
            raise RuntimeError("application capacity original plan changed")
        journal = self._journal()
        events = journal.read()
        guard, inputs = self._context(events)
        seed = dict(self.seed_source())
        template = self._template()
        if (self.base.application_owner_role != APPLICATION_OWNER_ROLE
                or id(self.base.runner) != id(self.runner) or self.base.container_registry != self.container_registry):
            raise RuntimeError("application capacity separated owner runtime changed")
        self._retain(guard)
        view, handoff = self._handoff()
        assert view.admission is not None
        if not events:
            # This is initial admission only. Recovery never runs these queries.
            resources, _ = self.base._resource_state(self.plan, template)
            if resources != _ResourceState.ABSENT:
                raise RuntimeError("application capacity legacy bootstrap resources have not retired")
            state = self.base._database_state(self.plan, seed)
            if state not in {_DatabaseState.EXACT, _DatabaseState.NEEDS_CONVERGENCE,
                    _DatabaseState.AUTHORITY_REBIND_REQUIRED, _DatabaseState.AUTHORITY_REBIND_RECOVERY_REQUIRED}:
                raise RuntimeError("application capacity initial configuration is not admitted")
            rebind_sha256 = (hashlib.sha256(self.base._legacy_authority_rebind_payload(self.plan, seed)).hexdigest()
                if state == _DatabaseState.AUTHORITY_REBIND_REQUIRED else None)
            with self.runner.open_staging_peer_database() as peer:
                _admit_sql_profiles(self.runner, peer, guard, separated_owner=True)
                observe_completed_application_authority(peer, target=view.admission.target,
                    runtime_password=inputs.credential.credential.password)
                backend, coordination = _observe(peer, guard, owner_role=APPLICATION_OWNER_ROLE)
                admission = ApplicationAdmissionRecoveryRecord(journal.intent.intent_digest, view.admission.target, backend, coordination)
                with peer.transaction():
                    peer.execute("SET TRANSACTION READ ONLY")
                    peer.execute("SET LOCAL search_path=pg_catalog,pg_temp")
                    rows = peer.execute("SELECT rolname,oid::bigint FROM pg_catalog.pg_roles WHERE rolname IN "
                        "('loom_cap_staging_owner','loom_cap_staging_migrator','loom_cap_staging_agent',"
                        "'loom_cap_staging_executor','loom_cap_staging_observer','loom_cap_staging_runtime')").fetchall()
                roles = {str(name): oid for name, oid in rows}
                if len(roles) != 6:
                    raise RuntimeError("application capacity permanent roles are absent")
                owner = ApplicationGuardOwner("loom_cap_staging_owner", _integer(roles, "loom_cap_staging_owner"))
                identity = ApplicationOwnerSuccessor("loom_cap_staging_migrator", _integer(roles, "loom_cap_staging_migrator"), owner)
                roles.pop("loom_cap_staging_owner")
                roles.pop("loom_cap_staging_migrator")
                require_application_guard_migrator_retired(peer, target=admission.target,
                    coordination_guard=coordination, provisioner_role="postgres", identity=identity)
                _, migration_terminal = self._migration()
                if self._context(()) != (guard, inputs) or dict(self.seed_source()) != seed:
                    raise RuntimeError("application capacity initial authority changed")
                journal.append("authority", {"admission": admission.to_dict(), "guard": guard.to_dict(),
                    "handoff_digest": handoff.terminal_digest, "credential_digest": inputs.credential_digest,
                    "inputs_digest": inputs.digest(), "guard_owner": asdict(owner),
                    "guard_migrator": {"role_name": identity.role_name, "role_oid": identity.role_oid},
                    "runtime_role_oids": roles, "seed_digest": admission_record_digest(seed),
                    "migration_digest": migration_terminal.terminal_digest, "initial_database_state": state.value,
                    "rebind_sha256": rebind_sha256}, guard=guard)
            events = journal.read()
        authority = events[0].payload
        original = ApplicationAdmissionRecoveryRecord.from_dict(_mapping(authority["admission"]))
        assert original.coordination_guard is not None
        identity, runtime_oids = _identities(authority)
        def checkpoint() -> None:
            if (self._context(journal.read()) != (guard, inputs) or self._template() != template
                    or dict(self.seed_source()) != seed):
                raise RuntimeError("application capacity original active authority changed")
            for database, opener in (("postgres", self.runner.open_staging_peer_maintenance_database),
                                     ("template1", self.runner.open_staging_peer_template_database)):
                with opener() as peer:
                    require_cnpg_effective_sql_profile(peer, database=database, original=original.handoff_backend)
        with ProtectedCapacityBootstrapRuntime(plan=self.plan, guard=guard, target=original.target,
                coordination_guard=original.coordination_guard, runner=self.runner, template=template,
                ca_certificate=inputs.ca.certificate, runtime_password=inputs.credential.credential.password,
                container_registry=self.container_registry, assert_guard=self._guard, assert_inputs=checkpoint,
                intent_digest=journal.intent.intent_digest, base=self.base, seed=seed, identity=identity,
                runtime_role_oids=runtime_oids, initial_database_state=_DatabaseState(str(authority["initial_database_state"])),
                rebind_sha256=None if authority["rebind_sha256"] is None else str(authority["rebind_sha256"])) as runtime:
            ApplicationMigrationLifecycle(journal, guard, runtime).run()

    def _observe_completed(self, events: Sequence[ApplicationMigrationEvent], guard: MutationGuardEvidence,
                           inputs: ApplicationMigrationInputs) -> None:
        original = ApplicationAdmissionRecoveryRecord.from_dict(_mapping(events[0].payload["admission"]))
        historical_guard = MutationGuardEvidence.from_dict(_mapping(events[0].payload["guard"]))
        identity, runtime_oids = _identities(events[0].payload)
        seed = dict(self.seed_source())
        successor = None if self.executor_admission_source is None else self.executor_admission_source()
        issued = False
        with self.runner.open_staging_peer_database() as peer:
            _admit_sql_profiles(self.runner, peer, guard, separated_owner=True)
            _, coordination = _observe(peer, guard, owner_role=APPLICATION_OWNER_ROLE)
            observe_completed_application_authority(peer, target=original.target,
                runtime_password=inputs.credential.credential.password)
            require_application_guard_migrator_retired(peer, target=original.target, coordination_guard=coordination,
                provisioner_role="postgres", identity=identity)
            if successor is not None:
                from loom.application_executor_admission import (
                    admit_sealed_executor,
                    require_issued_executor,
                )

                from .protected_executor_admission_component import require_executor_schema
                record, marked_issued = successor
                terminal = self._terminal(events)
                if (terminal is None or record.bootstrap_terminal_digest != terminal.terminal_digest
                        or record.bootstrap_event_digest != events[-1].event_digest
                        or record.seed_digest != self._seed_digest() or record.admission.target != original.target
                        or record.identity.role_oid != runtime_oids["loom_cap_staging_executor"]):
                    raise RuntimeError("application capacity executor successor changed")
                with peer.transaction():
                    peer.execute("SET TRANSACTION READ ONLY")
                    role = peer.execute("SELECT rolcanlogin FROM pg_catalog.pg_roles WHERE rolname='loom_cap_staging_executor'").fetchone()
                if role not in ((True,), (False,)):
                    raise RuntimeError("application capacity executor successor role is absent")
                issued = role == (True,)
                if issued:
                    require_issued_executor(peer, target=original.target, coordination_guard=coordination,
                        provisioner_role="postgres", identity=record.identity, password=record.password)
                elif marked_issued or admit_sealed_executor(peer, target=original.target,
                        coordination_guard=coordination, provisioner_role="postgres") != record.identity:
                    raise RuntimeError("application capacity executor successor lost its issuance")
                require_executor_schema(peer, issued=issued)
            with peer.transaction():
                peer.execute("SET TRANSACTION READ ONLY")
                rows = _roles(peer, runtime_oids)
                _credentials([row for row in rows if not issued or row[1] != "loom_cap_staging_executor"],
                    {f"loom_cap_staging_{role}": _seed_credential(seed, f"{role}_database_password")
                        for role in ("agent", "observer", "runtime")}, require_login=True)
        for generation in (event for event in events if event.phase == "generation"):
            capacity_bootstrap_resources(base=self.base, plan=self.plan, seed=seed, generation=generation,
                guard=historical_guard, assert_guard=lambda: historical_guard if self._guard() == guard else self._guard()).require_retired()
        state = (self.base._database_state(self.plan, seed, executor_admission=successor[0].identity)
            if issued and successor is not None else self.base._database_state(self.plan, seed))
        if state != _DatabaseState.EXACT:
            raise RuntimeError("application capacity completed configuration changed")
        if (self._context(events) != (guard, inputs) or self._journal().read() != tuple(events)
                or (None if self.executor_admission_source is None else self.executor_admission_source()) != successor):
            raise RuntimeError("application capacity completed inputs changed during observation")


def _identities(authority: Mapping[str, object]) -> tuple[ApplicationOwnerSuccessor, Mapping[str, int]]:
    owner = _mapping(authority["guard_owner"])
    migrator = _mapping(authority["guard_migrator"])
    runtime = _mapping(authority["runtime_role_oids"])
    return (ApplicationOwnerSuccessor(str(migrator["role_name"]), _integer(migrator, "role_oid"),
        ApplicationGuardOwner(str(owner["role_name"]), _integer(owner, "role_oid"))),
        {name: _integer(runtime, name) for name in runtime})
