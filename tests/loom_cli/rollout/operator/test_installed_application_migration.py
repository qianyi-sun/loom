"""Installed migration uses the original retained peer while admission is closed."""

import os
from types import SimpleNamespace

import pytest

from tests.loom_cli.rollout.operator.test_application_guard_retention import _guard
from tests.loom_cli.rollout.operator.test_installed_application_handoff import _plan
from tests.loom_cli.rollout.operator.test_staging_mutation_guard import _config


@pytest.mark.parametrize("acknowledged", [False, True])
def test_installed_migration_selects_retained_epoch_without_ordinary_sql(tmp_path, monkeypatch, acknowledged):
    from loom_cli.rollout.operator import installed_application_migration as module
    from loom_cli.rollout.operator.installed_application_handoff import (
        InstalledApplicationHandoffFactory,
    )
    from loom_cli.rollout.operator.protected_apply_journal import (
        ComponentObservation,
        ComponentState,
        ProtectedApplyJournal,
    )

    plan, config = _plan(tmp_path), _config(tmp_path)
    original = _guard(plan)
    fields = {key: value for key, value in original.to_dict().items() if key not in {"schema_version", "evidence_digest"}}
    guard = type(original).build(**{**fields, "generation": "e" * 32, "mutation_epoch": original.mutation_epoch + 1})
    calls = []
    def probe(evidence, **kwargs):
        assert acknowledged and evidence == guard
        calls.append("retained")
        return plan.starting_mutation_epoch + 1
    def classify(candidate):
        assert not acknowledged and candidate == plan
        calls.append("ordinary")
        return ComponentObservation(ComponentState.EXACT, "f" * 64, plan.starting_mutation_epoch + 1)
    handoff = InstalledApplicationHandoffFactory(config=config, service_uid=os.getuid(),
        runner=SimpleNamespace(environment={}), successor_source=lambda *args: None)
    object.__setattr__(handoff, "manager", SimpleNamespace(assert_ready=lambda *args, **kwargs: guard, observe_retained_epoch=probe))
    monkeypatch.setattr(module, "_read_pending_retention", lambda *args, **kwargs: SimpleNamespace(acknowledged=acknowledged))
    monkeypatch.setattr(module, "KubernetesProtectedEpochComponent", lambda **kwargs: SimpleNamespace(classify=classify))
    factory = module.InstalledApplicationMigrationFactory(handoff=handoff, container_registry="registry.example")
    journal = ProtectedApplyJournal(config.state_root, request_id=plan.request_id, attempt_number=plan.attempt_number)
    assert factory(plan, journal=journal, ordinal=3, handoff_ordinal=2).component_id == "database-migration"
    assert calls == [] and not journal.root.exists()
    assert factory.epoch(plan) == plan.starting_mutation_epoch + 1
    assert calls == ["retained" if acknowledged else "ordinary"]


def test_installed_capacity_binds_original_chain_without_opening_database(tmp_path):
    from loom_cli.rollout.operator.installed_application_handoff import InstalledApplicationHandoffFactory
    from loom_cli.rollout.operator.installed_application_migration import InstalledApplicationMigrationFactory
    from loom_cli.rollout.operator.protected_apply_journal import ProtectedApplyJournal
    from loom_cli.rollout.operator.protected_staging_capacity_database_component import (
        KubernetesProtectedStagingCapacityDatabaseComponent,
    )

    plan, config = _plan(tmp_path), _config(tmp_path)
    runner = SimpleNamespace(environment={})
    handoff = InstalledApplicationHandoffFactory(config=config, service_uid=os.getuid(),
        runner=runner, successor_source=lambda *args: None)
    factory = InstalledApplicationMigrationFactory(handoff=handoff, container_registry="registry.example")
    journal = ProtectedApplyJournal(config.state_root, request_id=plan.request_id, attempt_number=plan.attempt_number)
    base = KubernetesProtectedStagingCapacityDatabaseComponent(runner, "registry.example", lambda: {})
    capacity = factory.capacity(plan, journal=journal, ordinal=5, handoff_ordinal=2,
        base=base, seed_source=lambda: {})
    assert capacity.component_id == "staging-capacity-database"
    assert capacity.terminal_recovery_authority is None
    owner = capacity.apply.__self__
    assert owner.journal is journal and owner.ordinal == 5
    assert owner.base.application_owner_role == "loom_app_staging_owner"
    assert not journal.root.exists()
    with pytest.raises(ValueError, match="ordinal"):
        factory.capacity(plan, journal=journal, ordinal=4, handoff_ordinal=2, base=base, seed_source=lambda: {})
