"""Installed activation composes existing preparation and retained issuance sources."""

from contextlib import contextmanager
from types import SimpleNamespace

import pytest

from loom_cli.rollout.operator import installed_execution_activation as module
from loom_cli.rollout.operator.protected_apply_journal import ComponentState
from loom_cli.rollout.operator.protected_execution_preparation_journal import (
    ExecutionPreparationRecoveryState,
)
from tests.loom_cli.rollout.operator.test_protected_execution_activation import fixture


def installed(tmp_path, monkeypatch):
    owner, manager, controllers, calls = fixture(tmp_path)
    observed = []
    manager.get_configuration = lambda: {}
    @contextmanager
    def client_context(**kwargs):
        yield manager
    exact = SimpleNamespace(classify=lambda _: (ComponentState.EXACT, "a" * 64),
        classify_execution=lambda _, **kwargs: (ComponentState.EXACT, "a" * 64))
    preparation = SimpleNamespace(classify=exact.classify,
        _operation_journal=lambda _: SimpleNamespace(recovery_state=lambda *args, **kwargs: ExecutionPreparationRecoveryState.FORWARD_COMPLETE),
        _profile_store=lambda: SimpleNamespace(observe=lambda _: object()),
        _controller_requests=lambda *args: {pool: request.prepared for pool, request in owner.requests.items()})
    runtime = SimpleNamespace(state_root=owner.journal.state_root, service_uid=owner.journal.service_uid,
        service_gid=owner.journal.service_uid, credentials_root=tmp_path / "credentials", runner=object(),
        _read_execution_prerequisite=lambda _: owner.artifact, prepared_controller_transports=controllers,
        manager_configuration_client_context=client_context, _execution_credential_component=lambda: exact,
        _manager_configuration_component=lambda _: exact, _manager_runtime_component=lambda _: exact,
        execution_preparation_dependency_guard=lambda *args: "a" * 64,
        _execution_preparation_component=lambda: preparation, read_credential_seed=lambda: {},
        _database_component=lambda _: object(), _credential_seed_for_plan=lambda _: {})
    def admission(plan, **kwargs):
        assert plan == owner.plan and kwargs["subject"] == owner.subject
        pool = kwargs["state_directory"].rsplit("/", 1)[1]
        observed.append(pool)
        return owner.requests[pool].admission
    application = SimpleNamespace(new_journal=lambda _: object(), controller_admission=admission)
    monkeypatch.setattr(module, "derive_protected_staging_capacity_configuration",
        lambda **kwargs: SimpleNamespace(exact=True, staging_subject=owner.subject))
    source = module.InstalledExecutionActivation(runtime, application, controllers, checkpoint_guard=lambda: None)
    documents = {pool: request.document for pool, request in owner.requests.items()}
    return source, owner, manager, controllers, calls, observed, documents, preparation


def test_installed_activation_delivers_existing_issuance_before_controller_effects(tmp_path, monkeypatch):
    source, owner, manager, _, calls, observed, documents, _ = installed(tmp_path, monkeypatch)
    assert source.execute(owner.plan, documents=documents) == manager.expected
    assert observed == ["gb10", "oldlab"]
    assert calls[-2:] == [("gb10", "enable"), ("oldlab", "enable")]


def test_installed_resume_does_not_reopen_failed_forward_sources(tmp_path, monkeypatch):
    source, owner, _, controllers, calls, observed, documents, _ = installed(tmp_path, monkeypatch)
    controllers["oldlab"].fail = True
    with pytest.raises(RuntimeError, match="controller enable failed"):
        source.execute(owner.plan, documents=documents)
    def forbidden(*args, **kwargs):
        raise AssertionError("forward source must remain closed")
    source.runtime._execution_preparation_component = forbidden
    source.runtime.read_credential_seed = forbidden
    source.application.controller_admission = forbidden
    source.runtime.execution_preparation_dependency_guard = forbidden
    assert source.execute(owner.plan).execution_state == "drain-only"
    assert calls.count(("manager", "activate")) == 1
    assert observed == ["gb10", "oldlab"]


def test_installed_activation_refuses_unfinished_preparation_before_delivery(tmp_path, monkeypatch):
    source, owner, _, _, calls, observed, documents, preparation = installed(tmp_path, monkeypatch)
    preparation._operation_journal = lambda _: SimpleNamespace(recovery_state=lambda *args, **kwargs: ExecutionPreparationRecoveryState.UNRESOLVED)
    with pytest.raises(RuntimeError, match="completed preparation journal"):
        source.execute(owner.plan, documents=documents)
    assert calls == observed == []


def test_installed_activation_checkpoint_expiry_prevents_forward_but_not_drain(tmp_path, monkeypatch):
    from dataclasses import replace
    source, owner, manager, controllers, calls, _, documents, _ = installed(tmp_path, monkeypatch)
    def expired():
        raise RuntimeError("checkpoint expired")
    expired_source = replace(source, checkpoint_guard=expired)
    with pytest.raises(RuntimeError, match="checkpoint expired"):
        expired_source.execute(owner.plan, documents=documents)
    assert calls == []
    controllers["oldlab"].fail = True
    with pytest.raises(RuntimeError, match="controller enable failed"):
        source.execute(owner.plan, documents=documents)
    assert expired_source.execute(owner.plan).execution_state == "drain-only"
    assert calls.count(("manager", "activate")) == 1
    assert manager.execution.execution_state == "drain-only"
