"""Migration authority and retirement must remain an ordered durable prefix."""

import base64
import json
import os
from dataclasses import asdict, replace

import pytest

from loom.application_database_admission import ApplicationDatabaseCoordinationGuard
from loom.staging_mutation_coordination import rollout_guard_application_name
from loom_cli.rollout.operator.protected_application_admission_recovery import (
    ApplicationAdmissionRecoveryRecord,
)
from loom_cli.rollout.operator.protected_application_guard_retention import (
    application_guard_is_retained,
)
from loom_cli.rollout.operator.protected_apply_journal import ComponentIntent
from tests.loom_cli.rollout.operator.test_application_admission_recovery import (
    _component,
    _handoff,
    _target,
)
from tests.loom_cli.rollout.operator.test_application_guard_retention import _guard, _setup


def _authority(plan, component, guard):
    coordination = ApplicationDatabaseCoordinationGuard(replace(_handoff(), pid=guard.database_backend_pid), 55,
        rollout_guard_application_name(request_id=guard.request_id, candidate_sha=guard.candidate_sha,
            candidate_tree=guard.candidate_tree, generation=guard.generation))
    admission = ApplicationAdmissionRecoveryRecord(ComponentIntent.build(plan, component, 0).intent_digest,
        replace(_target(), successor_role="loom_app_staging_owner"), _handoff(), coordination)
    authority = {"admission": admission.to_dict(), "guard": guard.to_dict(), "handoff_digest": "a" * 64,
        "credential_digest": "b" * 64, "inputs_digest": "c" * 64}
    if component.component_id == "staging-capacity-database":
        authority.update(guard_owner={"role_name": "loom_cap_staging_owner", "role_oid": 90},
            guard_migrator={"role_name": "loom_cap_staging_migrator", "role_oid": 91},
            runtime_role_oids={"loom_cap_staging_agent": 92, "loom_cap_staging_executor": 93,
                "loom_cap_staging_observer": 94, "loom_cap_staging_runtime": 95},
            seed_digest="d" * 64, migration_digest="e" * 64)
    return authority


def _generation(ordinal=1):
    return {"ordinal": ordinal, "nonce": str(ordinal) * 32, "password": "s" * 64,
        "expires_at": "2026-09-14T23:45:00+00:00", "creation_backend": asdict(_handoff()),
        "ca_certificate": base64.b64encode(b"public-ca-fixture" * 8).decode()}


@pytest.mark.parametrize("stop", ["role", "job", "retirement", "role-retired", "complete"])
@pytest.mark.parametrize("component_id", ["database-migration", "staging-capacity-database"])
def test_migration_journal_recovers_only_the_original_ordered_prefix(tmp_path, stop, component_id):
    from loom_cli.rollout.operator.protected_application_migration_journal import (
        ApplicationMigrationJournal,
    )

    plan, journal = _setup(tmp_path)
    guard = _guard(plan)
    events = []
    def apply(_):
        journal.retain_application_guard(plan, guard=guard)
        assert application_guard_is_retained(tmp_path / "state", request_id=plan.request_id,
            service_uid=os.getuid(), guard=guard, acknowledge=True)
        migration = ApplicationMigrationJournal(journal=journal, plan=plan, component=component, ordinal=0)
        for phase, payload in [
            ("authority", _authority(plan, component, guard)), ("generation", _generation()),
            ("role", {"oid": 91}), ("secret-dispatch", {"manifest_sha256": "1" * 64}),
            ("secret", {"uid": "11111111-1111-4111-8111-111111111111"}),
            ("job-dispatch", {"manifest_sha256": "2" * 64}),
            ("job", {"uid": "22222222-2222-4222-8222-222222222222"}),
            ("retirement", {"successful": True, "maintenance_backend": asdict(replace(_handoff(), pid=777, database_oid=5))}),
            ("job-stopped", {}), ("closed", {}), ("role-retire", {}), ("role-retired", {}),
            ("secret-deleted", {}), ("reopen", {}), ("reopened", {}),
            ("complete", {"successful": True, "revision": "exact" if component_id == "staging-capacity-database" else plan.migration_target_revision}),
        ]:
            event = migration.append(phase, payload, guard=guard)
            assert migration.append(phase, payload, guard=guard) == event
            events.append(event)
            if phase == stop:
                raise RuntimeError("interrupted migration prefix")
    component = replace(_component(apply), component_id=component_id)
    migration = ApplicationMigrationJournal(journal=journal, plan=plan, component=component, ordinal=0)
    with pytest.raises(RuntimeError, match="active component"):
        migration.append("generation", _generation(), guard=guard)
    with pytest.raises(RuntimeError, match="interrupted migration prefix"):
        journal.execute(plan, [component])
    assert migration.read() == tuple(events)
    assert all("s" * 64 not in repr(event) for event in events)
    path = journal.root / f"00-{component_id}" / "migration-event-0001.json"
    value = json.loads(path.read_text())
    value["payload"]["nonce"] = "e" * 32
    path.write_text(json.dumps(value))
    with pytest.raises(ValueError, match="migration"):
        migration.read()


@pytest.mark.parametrize("phase", ["secret-dispatch", "reopen", "generation"])
def test_migration_journal_cannot_skip_retirement_or_rearm_pending_generation(tmp_path, phase):
    from loom_cli.rollout.operator.protected_application_migration_journal import (
        ApplicationMigrationJournal,
    )

    plan, journal = _setup(tmp_path)
    guard = _guard(plan)
    def apply(_):
        journal.retain_application_guard(plan, guard=guard)
        application_guard_is_retained(tmp_path / "state", request_id=plan.request_id,
            service_uid=os.getuid(), guard=guard, acknowledge=True)
        migration = ApplicationMigrationJournal(journal=journal, plan=plan, component=component, ordinal=0)
        migration.append("authority", _authority(plan, component, guard), guard=guard)
        migration.append("generation", _generation(), guard=guard)
        payload = {"manifest_sha256": "1" * 64} if phase == "secret-dispatch" else _generation(2) if phase == "generation" else {}
        with pytest.raises(ValueError, match="migration"):
            migration.append(phase, payload, guard=guard)
        assert len(migration.read()) == 2
        raise RuntimeError("end test")
    component = replace(_component(apply), component_id="database-migration")
    with pytest.raises(RuntimeError, match="end test"):
        journal.execute(plan, [component])


@pytest.mark.parametrize("field,value", [("phase", []), ("phase", {}), ("sequence", True), ("sequence", "0")])
def test_migration_event_rejects_malformed_fields_without_type_errors(field, value):
    from loom_cli.rollout.operator.protected_application_admission_recovery import (
        admission_record_digest,
    )
    from loom_cli.rollout.operator.protected_application_migration_journal import (
        ApplicationMigrationEvent,
    )

    record = {"schema_version": 1, "sequence": 0, "phase": "authority", "payload": {},
        "intent_digest": "a" * 64, "guard_digest": "b" * 64, "previous_digest": "c" * 64}
    record[field] = value
    record["event_digest"] = admission_record_digest(record)
    with pytest.raises(ValueError, match="migration event"):
        ApplicationMigrationEvent.from_dict(record)


def test_migration_retirement_journals_replacement_peer_before_continuing(tmp_path):
    from loom_cli.rollout.operator.protected_application_migration_journal import (
        ApplicationMigrationJournal,
    )

    plan, journal = _setup(tmp_path)
    guard = _guard(plan)
    def apply(_):
        journal.retain_application_guard(plan, guard=guard)
        application_guard_is_retained(tmp_path / "state", request_id=plan.request_id,
            service_uid=os.getuid(), guard=guard, acknowledge=True)
        migration = ApplicationMigrationJournal(journal=journal, plan=plan, component=component, ordinal=0)
        migration.append("authority", _authority(plan, component, guard), guard=guard)
        migration.append("generation", _generation(), guard=guard)
        migration.append("role", {"oid": 91}, guard=guard)
        migration.append("retirement", {"successful": False,
            "maintenance_backend": asdict(replace(_handoff(), pid=777, database_oid=5))}, guard=guard)
        migration.append("job-stopped", {}, guard=guard)
        migration.append("maintenance-peer", {"backend": asdict(replace(_handoff(), pid=778, database_oid=5))}, guard=guard)
        migration.append("closed", {}, guard=guard)
        with pytest.raises(ValueError, match="migration"):
            migration.append("maintenance-peer", {"backend": asdict(replace(_handoff(), pid=777, database_oid=5))}, guard=guard)
        raise RuntimeError("end test")
    component = replace(_component(apply), component_id="database-migration")
    with pytest.raises(RuntimeError, match="end test"):
        journal.execute(plan, [component])
