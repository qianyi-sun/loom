"""Retained rebind dispatch consumes the original certified SQL on its creator."""

import hashlib
from types import SimpleNamespace

import pytest

from loom_cli.rollout.operator.protected_staging_capacity_database_component import _DatabaseState
from tests.loom_cli.rollout.operator.test_application_guard_retention import _guard
from tests.loom_cli.rollout.operator.test_application_migration_journal import _generation
from tests.loom_cli.rollout.operator.test_protected_staging_capacity_runtime import (
    _database_component,
)


@pytest.mark.parametrize("drift", [None, "payload", "state", "unadmitted"])
def test_capacity_rebind_dispatches_only_original_payload_on_journaled_creation_peer(tmp_path, monkeypatch, drift):
    from loom_cli.rollout.operator import protected_capacity_bootstrap_runtime as module
    from loom_cli.rollout.operator.protected_application_migration_journal import (
        ApplicationMigrationEvent,
    )

    plan, source, _ = _database_component(tmp_path, database_state="exact")
    guard = _guard(plan)
    calls = []
    payload = b"BEGIN; SELECT 1; COMMIT;"
    state = [_DatabaseState.AUTHORITY_REBIND_REQUIRED]
    class Peer:
        def execute(self, statement):
            calls.append(statement.as_string())
            state[0] = _DatabaseState.AUTHORITY_REBIND_RECOVERY_REQUIRED
    peer = Peer()
    class Base:
        def _database_state(self, candidate, seed):
            assert candidate == plan and seed == source.seed
            return state[0]
        def _legacy_authority_rebind_payload(self, candidate, seed):
            return payload if drift != "payload" else payload + b"changed"
    runtime = module.ProtectedCapacityBootstrapRuntime(plan=plan, guard=guard, target=None, coordination_guard=None,
        runner=None, template=b"", ca_certificate=b"", runtime_password="", container_registry="registry.example",
        assert_guard=lambda: guard, assert_inputs=lambda: None, intent_digest="1" * 64, base=Base(), seed=source.seed,
        identity=SimpleNamespace(role_oid=91), runtime_role_oids={},
        initial_database_state=_DatabaseState.NEEDS_CONVERGENCE if drift == "unadmitted" else _DatabaseState.AUTHORITY_REBIND_REQUIRED,
        rebind_sha256=hashlib.sha256(payload).hexdigest())
    monkeypatch.setattr(type(runtime), "_identity", lambda *args: None)
    monkeypatch.setattr(type(runtime), "checkpoint", lambda *args: None)
    monkeypatch.setattr(type(runtime), "_creation_peer", lambda *args: peer)
    monkeypatch.setattr(module, "arm_application_capacity_runtime_credentials", lambda *args, **kwargs: calls.append("ordinary"))
    monkeypatch.setattr(module, "arm_application_guard_migrator", lambda *args, **kwargs: calls.append("elevated"))
    if drift == "state":
        state[0] = _DatabaseState.DRIFTED
    generation = ApplicationMigrationEvent.build(sequence=1, phase="generation", payload=_generation(),
        intent_digest="1" * 64, guard_digest=guard.evidence_digest, previous_digest="2" * 64)
    if drift:
        with pytest.raises(RuntimeError, match="rebind"):
            runtime.arm(generation, 91)
        assert not calls
    else:
        runtime.arm(generation, 91)
        assert calls == [payload.decode(), "ordinary", "elevated"]
        # A committed rebind is independently certified by the state observer.
        calls.clear()
        runtime.arm(generation, 91)
        assert calls == ["ordinary", "elevated"]
