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


@pytest.mark.parametrize("drift", [None, "inputs", "guard", "handoff"])
def test_pending_migration_classifies_without_application_database_reads(tmp_path, monkeypatch, drift):
    from loom_cli.rollout.operator import protected_application_migration_component as module

    plan, journal = _setup(tmp_path)
    guard = _guard(plan)
    current = {"inputs": "c" * 64, "guard": guard, "handoff": "a" * 64}
    class Runner:
        def open_staging_peer_database(self):
            pytest.fail("normal application database read before pending migration cleanup")
    owner = module.ProtectedApplicationMigrationComponent(plan=plan, journal=journal, runner=Runner(), ordinal=1,
        guard_source=lambda: current["guard"], epoch_source=lambda: plan.starting_mutation_epoch + 1,
        inputs_source=lambda: None, handoff_source=lambda: None, successor_source=lambda: None,
        container_registry="registry.example")
    monkeypatch.setattr(module.ProtectedApplicationMigrationComponent, "_handoff", lambda _: (
        SimpleNamespace(admission=None), SimpleNamespace(terminal_digest=current["handoff"])))
    inputs = SimpleNamespace(credential_digest="b" * 64, digest=lambda: current["inputs"])
    monkeypatch.setattr(module.ProtectedApplicationMigrationComponent, "_inputs", lambda _: inputs)
    component = owner.component()
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
        current[drift] = "e" * 64
    if drift:
        with pytest.raises(RuntimeError, match="migration"):
            component.classify(plan)
    else:
        assert component.classify(plan).state is ComponentState.READY


def test_migration_uses_explicit_prior_handoff_plan_for_original_identity(tmp_path):
    from loom_cli.rollout.operator.protected_application_migration_component import (
        ProtectedApplicationMigrationComponent,
    )
    from loom_cli.rollout.operator.protected_application_restoration import _bound_evidence
    from loom_cli.rollout.operator.protected_apply_journal import (
        ComponentObservation,
        ComponentTerminal,
        ProtectedApplyJournal,
    )
    from tests.loom_cli.rollout.operator.test_application_handoff_history import _later
    from tests.loom_cli.rollout.operator.test_application_restoration import _inputs

    original, view, *_ = _inputs(tmp_path)
    current = _later(original)
    view = replace(view, restoration=_bound_evidence(view), fences_retiring=True)
    terminal = ComponentTerminal.build(view.intent, ComponentObservation(ComponentState.EXACT, "a" * 64,
        original.starting_mutation_epoch + 1), applied=True)
    journal = ProtectedApplyJournal(tmp_path / "state", request_id=current.request_id, attempt_number=current.attempt_number)
    owner = ProtectedApplicationMigrationComponent(plan=current, journal=journal, runner=None, ordinal=5,
        guard_source=lambda: None, epoch_source=lambda: None, inputs_source=lambda: None,
        handoff_source=lambda: (view, terminal), successor_source=lambda: None, container_registry="registry.example",
        handoff_plan_source=lambda: original)
    assert owner._handoff() == (view, terminal)
    with pytest.raises(RuntimeError, match="handoff"):
        replace(owner, handoff_plan_source=None)._handoff()


@pytest.mark.parametrize("refuse", [False, True])
def test_runtime_prerequisite_runs_before_any_credential_generation(tmp_path, monkeypatch, refuse):
    from contextlib import contextmanager

    from loom.application_database_admission import ApplicationDatabaseCoordinationGuard
    from loom_cli.rollout.operator import protected_application_migration_runtime as module
    from tests.loom_cli.rollout.operator.test_application_admission_recovery import _target

    plan, _journal = _setup(tmp_path)
    guard = _guard(plan)
    coordination = ApplicationDatabaseCoordinationGuard(replace(_handoff(), pid=guard.database_backend_pid), 55, "loom-rollout-guard-" + "a" * 40)
    events = []
    peer = object()
    @contextmanager
    def open_peer():
        events.append("open")
        try:
            yield peer
        finally:
            events.append("close")
    def backend(connection, **kwargs):
        assert connection is peer
        events.append("admitted")
        return _handoff()
    monkeypatch.setattr(module, "observe_application_migration_backend", backend)
    def prerequisite(connection):
        assert connection is peer and "admitted" in events and "credential" not in events
        events.append("prerequisite")
        if refuse:
            raise RuntimeError("prerequisite refused")
    def credential(_):
        assert "prerequisite" in events
        events.append("credential")
        return "s" * 64
    monkeypatch.setattr(module.secrets, "token_urlsafe", credential)
    with module.ProtectedApplicationMigrationRuntime(plan=plan, guard=guard, target=_target(),
            coordination_guard=coordination, runner=SimpleNamespace(open_staging_peer_database=open_peer),
            template=b"template", ca_certificate=b"ca", runtime_password="retained", container_registry="registry.example",
            assert_guard=lambda: guard, assert_inputs=lambda: events.append("checkpoint"),
            intent_digest="a" * 64, before_generation=prerequisite) as runtime:
        if refuse:
            with pytest.raises(RuntimeError, match="prerequisite refused"):
                runtime.prepare_generation(1)
            assert "credential" not in events
        else:
            assert runtime.prepare_generation(1)["password"] == "s" * 64
            assert events.index("admitted") < events.index("prerequisite") < events.index("credential")
    assert events[-1] == "close"


@pytest.mark.parametrize("target_revision", ["0142", "0147", "0148", "0149"])
def test_installed_component_retains_guard_prerequisite_in_original_migration_journal(tmp_path, monkeypatch, target_revision):
    from contextlib import nullcontext

    from loom_cli.rollout.operator import protected_application_migration_component as module
    from loom_cli.rollout.operator import protected_application_migration_runtime as runtime_module
    from loom_cli.rollout.operator.protected_apply_journal import ComponentObservation
    from tests.loom_cli.rollout.operator.test_application_admission_recovery import (
        _component,
        _target,
    )

    plan, journal = _setup(tmp_path)
    from loom_cli.rollout.operator.final_gate_plan import _hash_json, _plan_payload
    plan = replace(plan, migration_target_revision=target_revision)
    plan = replace(plan, plan_digest=_hash_json(_plan_payload(plan, include_digest=False)))
    guard = _guard(plan)
    inputs = SimpleNamespace(credential=SimpleNamespace(credential=SimpleNamespace(password="retained")),
        ca=SimpleNamespace(certificate=b"ca"))
    peer = object()
    runner = SimpleNamespace(open_staging_peer_database=lambda: nullcontext(peer),
        open_staging_peer_maintenance_database=lambda: nullcontext(peer),
        open_staging_peer_template_database=lambda: nullcontext(peer))
    owner = module.ProtectedApplicationMigrationComponent(plan=plan, journal=journal, runner=runner, ordinal=1,
        guard_source=lambda: guard, epoch_source=lambda: plan.starting_mutation_epoch + 1,
        inputs_source=lambda: inputs, handoff_source=lambda: None, successor_source=lambda: None,
        container_registry="registry.example")
    monkeypatch.setattr(module.ProtectedApplicationMigrationComponent, "_context", lambda *args: (guard, inputs))
    monkeypatch.setattr(module.ProtectedApplicationMigrationComponent, "_template", lambda _: b"template")
    monkeypatch.setattr(module.ProtectedApplicationMigrationComponent, "_handoff", lambda _: (
        SimpleNamespace(admission=SimpleNamespace(target=_target())), None))
    monkeypatch.setattr(module, "require_cnpg_effective_sql_profile", lambda *args, **kwargs: None)
    monkeypatch.setattr(runtime_module, "observe_application_migration_backend", lambda *args, **kwargs: _handoff())
    component = owner.component()
    migration = ApplicationMigrationJournal(journal, plan, component, 1)
    calls = []
    binding = {"function_oid": 123, "before_sha256": "a" * 64, "after_sha256": "b" * 64}
    def prerequisite(connection, **kwargs):
        assert connection is peer and kwargs["guard_owner"] == "loom_cap_staging_owner"
        assert kwargs["coordination_guard"].backend.pid == guard.database_backend_pid
        assert all(event.phase == "authority" for event in migration.read())
        if calls:
            assert kwargs["retained"] == binding
        else:
            assert kwargs["retained"] is None
            kwargs["persist"](binding)
        calls.append("compatibility")
    monkeypatch.setattr(module, "ensure_guard_claim_compatibility", prerequisite)
    def run(lifecycle):
        assert lifecycle.migration.intent == migration.intent
        lifecycle.runtime.prepare_generation(1)
        assert calls == (["compatibility"] if target_revision in {"0147", "0148", "0149"} else [])
        lifecycle.runtime.prepare_generation(1)
        assert calls == (["compatibility", "compatibility"] if target_revision in {"0147", "0148", "0149"} else [])
        raise RuntimeError("installed prerequisite verified")
    monkeypatch.setattr(module.ApplicationMigrationLifecycle, "run", run)
    def apply(_):
        journal.retain_application_guard(plan, guard=guard)
        application_guard_is_retained(tmp_path / "state", request_id=plan.request_id,
            service_uid=os.getuid(), guard=guard, acknowledge=True)
        authority = _authority(plan, component, guard)
        authority["admission"]["intent_digest"] = migration.intent.intent_digest
        migration.append("authority", authority, guard=guard)
        owner.apply(plan)
    prior = replace(_component(lambda _: None), component_id="epoch-placeholder",
        classify=lambda _: ComponentObservation(ComponentState.EXACT, "f" * 64, plan.starting_mutation_epoch + 1))
    with pytest.raises(RuntimeError, match="installed prerequisite verified"):
        journal.execute(plan, [prior, replace(component, apply=apply,
            classify=lambda _: ComponentObservation(ComponentState.READY, "e" * 64, plan.starting_mutation_epoch + 1))])
    assert migration.read_claim_compatibility() == (binding if target_revision in {"0147", "0148", "0149"} else None)
