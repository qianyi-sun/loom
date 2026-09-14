"""Pending migration classification cannot open the closed application database."""

import os
from dataclasses import asdict, replace
from types import SimpleNamespace

import pytest

from loom_cli.rollout.operator.protected_application_guard_retention import (
    application_guard_is_retained,
)
from loom_cli.rollout.operator.protected_application_migration_journal import (
    ApplicationMigrationJournal,
)
from loom_cli.rollout.operator.protected_apply_journal import ComponentState
from tests.loom_cli.rollout.operator.test_application_admission_recovery import _handoff
from tests.loom_cli.rollout.operator.test_application_guard_retention import _guard, _setup
from tests.loom_cli.rollout.operator.test_application_migration_journal import (
    _authority,
    _generation,
)


@pytest.mark.parametrize("drift", [None, "inputs", "guard", "handoff", "seed", "migration"])
def test_pending_capacity_classifies_without_application_database_reads(tmp_path, monkeypatch, drift):
    from loom_cli.rollout.operator import protected_capacity_bootstrap_component as module

    plan, journal = _setup(tmp_path)
    guard = _guard(plan)
    current = {"inputs": "c" * 64, "guard": guard, "handoff": "a" * 64, "seed": "d" * 64, "migration": "e" * 64}
    class Runner:
        def open_staging_peer_database(self):
            pytest.fail("normal application database read before pending migration cleanup")
    owner = module.ProtectedCapacityBootstrapComponent(plan=plan, journal=journal, runner=Runner(), ordinal=1,
        guard_source=lambda: current["guard"], epoch_source=lambda: plan.starting_mutation_epoch + 1,
        inputs_source=lambda: None, handoff_source=lambda: None, successor_source=lambda: None,
        container_registry="registry.example", base=None, seed_source=lambda: {}, migration_source=lambda: None)
    monkeypatch.setattr(module.ProtectedCapacityBootstrapComponent, "_handoff", lambda _: (
        SimpleNamespace(admission=None), SimpleNamespace(terminal_digest=current["handoff"])))
    inputs = SimpleNamespace(credential_digest="b" * 64, digest=lambda: current["inputs"])
    monkeypatch.setattr(module.ProtectedCapacityBootstrapComponent, "_inputs", lambda _: inputs)
    monkeypatch.setattr(module.ProtectedCapacityBootstrapComponent, "_seed_digest", lambda _: current["seed"])
    monkeypatch.setattr(module.ProtectedCapacityBootstrapComponent, "_migration", lambda _: (
        (), SimpleNamespace(terminal_digest=current["migration"])))
    component = owner.component()
    assert component.component_id == "staging-capacity-database"
    assert component.terminal_recovery_authority is None
    def apply(_):
        journal.retain_application_guard(plan, guard=guard)
        application_guard_is_retained(tmp_path / "state", request_id=plan.request_id,
            service_uid=os.getuid(), guard=guard, acknowledge=True)
        migration = ApplicationMigrationJournal(journal=journal, plan=plan, component=component, ordinal=1)
        authority = _authority(plan, component, guard)
        authority["admission"]["intent_digest"] = migration.intent.intent_digest
        migration.append("authority", authority, guard=guard)
        migration.append("generation", _generation(), guard=guard)
        migration.append("role", {"oid": 91}, guard=guard)
        migration.append("retirement", {"successful": False,
            "maintenance_backend": asdict(replace(_handoff(), pid=777, database_oid=5))}, guard=guard)
        migration.append("job-stopped", {}, guard=guard)
        migration.append("closed", {}, guard=guard)
        raise RuntimeError("interrupted with admission closed")
    # Use the production component binding; only inject the lost-effect prefix.
    from tests.loom_cli.rollout.operator.test_application_admission_recovery import _component
    prior = replace(_component(lambda _: None), component_id="epoch-placeholder",
        classify=lambda _: SimpleNamespace(state=ComponentState.EXACT, evidence_digest="f" * 64,
            observed_epoch=plan.starting_mutation_epoch + 1))
    with pytest.raises(RuntimeError, match="admission closed"):
        journal.execute(plan, [prior, replace(component, apply=apply)])
    if drift == "guard":
        values = {k: v for k, v in guard.to_dict().items() if k not in {"schema_version", "evidence_digest"}}
        current["guard"] = type(guard).build(**{**values, "generation": "d" * 32})
    elif drift:
        current[drift] = "f" * 64
    if drift:
        with pytest.raises(RuntimeError, match=r"migration|capacity"):
            component.classify(plan)
    else:
        assert component.classify(plan).state is ComponentState.READY


def test_capacity_initial_admission_rejects_legacy_bootstrap_resources_before_sql(tmp_path, monkeypatch):
    from loom_cli.rollout.operator.protected_capacity_bootstrap_component import ProtectedCapacityBootstrapComponent
    from loom_cli.rollout.operator.protected_staging_capacity_database_component import _ResourceState

    plan, journal = _setup(tmp_path)
    guard = _guard(plan)
    runner = object()
    base = SimpleNamespace(application_owner_role="loom_app_staging_owner", runner=runner, container_registry="registry.example",
        _manifest=lambda *args: b"template", _resource_state=lambda *args: (_ResourceState.EXACT, "f" * 64),
        _database_state=lambda *args: pytest.fail("database read while old bootstrap resources survive"))
    owner = ProtectedCapacityBootstrapComponent(plan=plan, journal=journal, runner=runner, ordinal=1,
        guard_source=lambda: guard, epoch_source=lambda: plan.starting_mutation_epoch + 1,
        inputs_source=lambda: None, handoff_source=lambda: None, successor_source=lambda: None,
        container_registry="registry.example", base=base, seed_source=lambda: {}, migration_source=lambda: None)
    monkeypatch.setattr(type(owner), "_context", lambda *args: (guard, None))
    monkeypatch.setattr(type(owner), "_retain", lambda *args: None)
    monkeypatch.setattr(type(owner), "_handoff", lambda *args: (SimpleNamespace(admission=object()), None))
    with pytest.raises(RuntimeError, match="legacy bootstrap resources"):
        owner.apply(plan)
