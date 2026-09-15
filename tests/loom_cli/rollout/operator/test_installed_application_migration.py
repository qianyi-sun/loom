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
    from loom_cli.rollout.operator.installed_application_handoff import (
        InstalledApplicationHandoffFactory,
    )
    from loom_cli.rollout.operator.installed_application_migration import (
        InstalledApplicationMigrationFactory,
    )
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


def test_installed_future_rollout_selects_historical_observer_and_same_origin_for_capacity(tmp_path, monkeypatch):
    from loom_cli.rollout.operator import installed_application_migration as module
    from loom_cli.rollout.operator.installed_application_handoff import (
        InstalledApplicationHandoffFactory,
    )
    from loom_cli.rollout.operator.protected_apply_journal import ProtectedApplyJournal
    from loom_cli.rollout.operator.protected_staging_capacity_database_component import (
        KubernetesProtectedStagingCapacityDatabaseComponent,
    )
    from tests.loom_cli.rollout.operator.test_application_handoff_history import _later

    original, config = _plan(tmp_path), _config(tmp_path)
    plan = _later(original)
    runner = SimpleNamespace(environment={})
    handoff = InstalledApplicationHandoffFactory(config=config, service_uid=os.getuid(), runner=runner, successor_source=lambda *args: None)
    factory = module.InstalledApplicationMigrationFactory(handoff, "registry.example")
    journal = ProtectedApplyJournal(config.state_root, request_id=plan.request_id, attempt_number=plan.attempt_number)
    admitted = []
    marker = (object(), object())
    def history(candidate, *, journal, component):
        admitted.append((candidate, journal, component))
        return marker
    origin = SimpleNamespace(plan=original, terminal=SimpleNamespace(terminal_digest="f" * 64), admitted_for=history)
    monkeypatch.setattr(module, "select_completed_handoff", lambda *args, **kwargs: origin)
    components = factory.components(plan, journal=journal, ordinal=2)
    migration = components[1].apply.__self__
    assert migration.handoff_plan_source() == original
    assert migration.handoff_source() == marker
    assert admitted[-1][0] == plan and admitted[-1][1] is journal
    assert admitted[-1][2].input_fingerprint == components[0].input_fingerprint
    capacity = factory.capacity(plan, journal=journal, ordinal=5, handoff_ordinal=2,
        base=KubernetesProtectedStagingCapacityDatabaseComponent(runner, "registry.example", lambda: {}), seed_source=lambda: {})
    assert capacity.apply.__self__.handoff_plan_source() == original
    assert not journal.root.exists()
    with pytest.raises(ValueError, match="ordinal"):
        factory.components(plan, journal=journal, ordinal=1)


def test_installed_executor_issuer_and_bootstrap_share_exact_successor_journal(tmp_path):
    import hashlib
    import json

    from loom_cli.rollout.operator.installed_application_handoff import (
        InstalledApplicationHandoffFactory,
    )
    from loom_cli.rollout.operator.installed_application_migration import (
        InstalledApplicationMigrationFactory,
    )
    from loom_cli.rollout.operator.protected_apply_journal import (
        ComponentIntent,
        ProtectedApplyJournal,
    )
    from loom_cli.rollout.operator.protected_staging_capacity_database_component import (
        KubernetesProtectedStagingCapacityDatabaseComponent,
    )
    from tests.loom_cli.rollout.operator.test_protected_staging_capacity_runtime import (
        _execution_plan,
    )

    # Preserve the installed handoff's database-authority checkpoint while using
    # the complete execution preparation fields from the execution-plan fixture.
    execution = _execution_plan(tmp_path)
    base_root = tmp_path / "handoff-checkpoint"
    base_root.mkdir()
    base_plan = _plan(base_root)
    payload = base_plan.to_dict()
    payload.update({key: value for key, value in execution.to_dict().items()
        if key.startswith("execution_") or key == "executor_profile_seed_sha256"})
    payload["schema_version"] = 7
    payload["plan_digest"] = hashlib.sha256(json.dumps(
        {key: value for key, value in payload.items() if key != "plan_digest"},
        sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    plan = type(base_plan).from_dict(payload)
    config = _config(tmp_path)
    runner = SimpleNamespace(environment={})
    handoff = InstalledApplicationHandoffFactory(config=config, service_uid=os.getuid(),
        runner=runner, successor_source=lambda *args: None)
    factory = InstalledApplicationMigrationFactory(handoff=handoff, container_registry="registry.example")
    journal = ProtectedApplyJournal(config.state_root, request_id=plan.request_id, attempt_number=plan.attempt_number)
    base = KubernetesProtectedStagingCapacityDatabaseComponent(runner, "registry.example", lambda: {})
    bootstrap = factory.capacity(plan, journal=journal, ordinal=5, handoff_ordinal=2, base=base, seed_source=lambda: {})
    issuer = factory.executor_admission(plan, journal=journal, base=base, seed_source=lambda: {})
    assert issuer.component_id == "executor-database-admission" and issuer.terminal_recovery_authority is None
    assert bootstrap.apply.__self__.executor_admission_source.__self__.intent == ComponentIntent.build(plan, issuer, 14)
    assert issuer.apply.__self__.completed_bootstrap().component().input_fingerprint == bootstrap.input_fingerprint
    assert not journal.root.exists()
