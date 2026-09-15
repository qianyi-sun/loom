"""Protected issuance resumes a lost SQL commit using the retained credential."""

import os
from contextlib import contextmanager
from dataclasses import dataclass, replace
from types import SimpleNamespace

import pytest

from loom.application_executor_admission import ApplicationExecutorAdmissionIdentity
from loom_cli.rollout.operator.protected_application_guard_retention import (
    application_guard_is_retained,
)
from loom_cli.rollout.operator.protected_apply_journal import ComponentObservation, ComponentState
from tests.loom_cli.rollout.operator.test_application_admission_recovery import _component
from tests.loom_cli.rollout.operator.test_application_guard_retention import _guard, _setup
from tests.loom_cli.rollout.operator.test_executor_admission_journal import _record


@pytest.mark.parametrize("interrupt", ["after-retention", "after-sql-commit", "after-issued-marker", None])
def test_issuance_retains_credential_before_effect_and_recovers_original_commit(tmp_path, monkeypatch, interrupt):
    from loom_cli.rollout.operator import protected_executor_admission_component as module

    plan, journal = _setup(tmp_path)
    guard = _guard(plan)
    fixture_component = replace(_component(lambda _: None), component_id="executor-database-admission")
    authority = _record(plan, fixture_component, guard)
    issued = []
    calls = []
    inputs = SimpleNamespace(digest=lambda: "c" * 64)
    @dataclass
    class Bootstrap:
        executor_admission_source: object = None
        ordinal = 0
        def _journal(self):
            return SimpleNamespace(read=lambda: (SimpleNamespace(event_digest="b" * 64),),
                intent=SimpleNamespace(intent_digest="f" * 64))
        def _terminal(self, events):
            return SimpleNamespace(terminal_digest="a" * 64)
        def _guard(self):
            return guard
        def _context(self, events):
            return guard, inputs
        def _seed_digest(self):
            return "d" * 64
        def _handoff(self):
            return SimpleNamespace(admission=authority.admission), None
        def classify(self, candidate):
            saved = self.executor_admission_source()
            if issued:
                assert saved is not None and saved[0].password == issued[0]
            return ComponentObservation(ComponentState.EXACT, "e" * 64, plan.starting_mutation_epoch + 1)
    @contextmanager
    def peer():
        yield object()
    bootstrap = Bootstrap()
    bootstrap.plan, bootstrap.journal = plan, journal
    bootstrap.runner = SimpleNamespace(open_staging_peer_database=peer)
    # dataclasses.replace preserves actual production fields; these fixture fields
    # are class attributes to keep the replacement's authority stable as well.
    Bootstrap.plan, Bootstrap.journal, Bootstrap.runner = plan, journal, bootstrap.runner
    owner = module.ProtectedExecutorAdmissionComponent(bootstrap, 1)
    saved_journal = owner._journal()
    monkeypatch.setattr(module, "_admit_sql_profiles", lambda *args, **kwargs: None)
    monkeypatch.setattr(module, "_observe", lambda *args, **kwargs: (
        authority.admission.handoff_backend, authority.admission.coordination_guard))
    monkeypatch.setattr(module, "require_executor_schema", lambda *args, **kwargs: None)
    monkeypatch.setattr(module, "admit_sealed_executor", lambda *args, **kwargs: authority.identity)
    def retained(self, evidence):
        journal.retain_application_guard(plan, guard=evidence)
        application_guard_is_retained(tmp_path / "state", request_id=plan.request_id,
            service_uid=os.getuid(), guard=evidence, acknowledge=True)
    monkeypatch.setattr(module.ProtectedExecutorAdmissionComponent, "_retain", retained)
    def issue(peer, **kwargs):
        record, done = saved_journal.read()
        assert not done and kwargs["password"] == record.password
        calls.append(record.password)
        if interrupt == "after-retention" and len(calls) == 1:
            raise RuntimeError("interrupted issuance")
        if issued:
            assert kwargs["password"] == issued[0]
        else:
            issued.append(kwargs["password"])
        if interrupt == "after-sql-commit" and len(calls) == 1:
            raise RuntimeError("interrupted issuance")
    monkeypatch.setattr(module, "issue_executor_admission", issue)
    mark_issued = type(saved_journal).mark_issued
    def mark(self, record, *, guard):
        mark_issued(self, record, guard=guard)
        if interrupt == "after-issued-marker":
            raise RuntimeError("interrupted issuance")
    monkeypatch.setattr(type(saved_journal), "mark_issued", mark)
    monkeypatch.setattr(module, "require_issued_executor", lambda peer, **kwargs: (
        None if issued == [kwargs["password"]] else pytest.fail("issued credential changed")))
    prior = replace(_component(lambda _: None), component_id="completed-bootstrap",
        classify=lambda _: ComponentObservation(ComponentState.EXACT, "f" * 64, plan.starting_mutation_epoch + 1))
    components = [prior, owner.component()]
    if interrupt:
        with pytest.raises(RuntimeError, match="interrupted issuance"):
            journal.execute(plan, components)
        assert saved_journal.read()[1] == (interrupt == "after-issued-marker")
        assert owner.classify(plan).state is (ComponentState.EXACT if interrupt == "after-issued-marker" else ComponentState.READY)
        original_guard = guard
        fields = {k: v for k, v in guard.to_dict().items() if k not in {"schema_version", "evidence_digest"}}
        guard = type(guard).build(**{**fields, "generation": "d" * 32, "mutation_epoch": plan.starting_mutation_epoch + 1})
        with pytest.raises(RuntimeError, match="original bootstrap inputs or guard changed"):
            owner.classify(plan)
        guard = original_guard
    journal.execute(plan, components)
    assert saved_journal.read()[1] is True
    assert owner.classify(plan).state is ComponentState.EXACT
    journal.execute(plan, components)
    fields = {k: v for k, v in guard.to_dict().items() if k not in {"schema_version", "evidence_digest"}}
    guard = type(guard).build(**{**fields, "generation": "e" * 32, "mutation_epoch": plan.starting_mutation_epoch + 1})
    assert owner.classify(plan).state is ComponentState.EXACT
    journal.execute(plan, components)
    assert len(calls) == (2 if interrupt in {"after-retention", "after-sql-commit"} else 1) and len(set(calls)) == 1

    from uuid import NAMESPACE_URL, uuid5

    from sqlalchemy.engine import make_url

    from loom_cli.rollout.operator.protected_staging_capacity_manager_configuration_component import (
        derive_protected_staging_capacity_configuration,
    )
    from tests.loom_cli.rollout.operator.test_application_migration_ca import _ca
    from tests.loom_cli.rollout.operator.test_protected_staging_capacity_manager_configuration_component import (
        _active_document,
        _live_fleet,
        _seed,
    )
    seed = {**_seed(), "subject_id": str(uuid5(NAMESPACE_URL, "loom:staging:capacity-subject")),
        "subject_incarnation": str(uuid5(NAMESPACE_URL, "loom:staging:capacity-subject:v1"))}
    subject = derive_protected_staging_capacity_configuration(
        active_document=_active_document(_live_fleet(), ()), seed_values=seed,
        target_generation=plan.starting_mutation_epoch + 1).staging_subject
    assert str(subject.demand_reporter_incarnation) != seed["reporter_incarnation"]
    inputs.ca = SimpleNamespace(certificate=_ca())
    Bootstrap.inputs_source = staticmethod(lambda: inputs)
    Bootstrap.seed_source = staticmethod(lambda: dict(seed))
    bundle = owner.controller_admission(plan, subject=subject, state_directory="/var/lib/loom-capacity-executor",
        protected_admission_sha256="a" * 64)
    assert make_url(bundle.database_url.decode()).password == issued[0]
    assert bundle.issuance_digest == saved_journal.read()[0].digest
    assert len(calls) == (2 if interrupt in {"after-retention", "after-sql-commit"} else 1)
    for changes in ({"demand_reporter_incarnation": uuid5(NAMESPACE_URL, "wrong-reporter")},
            {"configuration_generation": subject.configuration_generation + 1},
            {"deployment_generation": subject.deployment_generation + 1},
            {"candidate_generation": subject.candidate_generation + 1}):
        with pytest.raises(RuntimeError, match="source binding"):
            owner.controller_admission(plan, subject=subject.model_copy(update=changes),
                state_directory="/var/lib/loom-capacity-executor", protected_admission_sha256="a" * 64)
    (saved_journal.root / "terminal.json").unlink()
    with pytest.raises(RuntimeError):
        owner.controller_admission(plan, subject=subject, state_directory="/var/lib/loom-capacity-executor",
            protected_admission_sha256="a" * 64)


@pytest.mark.parametrize("drift", [None, "grantor", "temp", "elevation", "owner-session"])
def test_executor_successor_configuration_preserves_sealed_bootstrap_contract(tmp_path, monkeypatch, drift):
    from loom_cli.rollout.operator.protected_staging_capacity_database_component import (
        KubernetesProtectedStagingCapacityDatabaseComponent,
        _DatabaseState,
    )
    from tests.loom_cli.rollout.operator.test_protected_staging_capacity_runtime import (
        _database_component,
    )

    plan, runner, _ = _database_component(tmp_path, database_state="exact")
    for role in ("agent", "observer", "runtime", "executor"):
        runner.protected_database_privileges[f"loom_cap_staging_{role}"] = {
            "acl": [{"grantable": False, "grantor": "loom_app_staging_owner", "privilege": "CONNECT"}],
            "connect": True, "create": False, "temporary": False,
        }
    original = runner._details
    def details():
        value = original()
        value["roles"]["loom_cap_staging_executor"].update(can_login=True, has_password=True)
        if drift == "elevation":
            value["roles"]["loom_cap_staging_executor"]["superuser"] = True
        return value
    monkeypatch.setattr(runner, "_details", details)
    runner.active_protected_sessions["loom_cap_staging_executor"] = 1
    if drift == "grantor":
        runner.protected_database_privileges["loom_cap_staging_executor"]["acl"][0]["grantor"] = "loom"
    elif drift == "temp":
        runner.protected_database_privileges["loom_cap_staging_executor"]["temporary"] = True
    elif drift == "owner-session":
        runner.active_protected_sessions["loom_cap_staging_owner"] = 1
    base = KubernetesProtectedStagingCapacityDatabaseComponent(runner, "registry.example.test/loom", lambda: runner.seed,
        application_owner_role="loom_app_staging_owner")
    assert base._database_state(plan, runner.seed) is not _DatabaseState.EXACT
    actual = base._database_state(plan, runner.seed, executor_admission=ApplicationExecutorAdmissionIdentity(93, "e" * 64))
    assert (actual is _DatabaseState.EXACT) == (drift is None)
