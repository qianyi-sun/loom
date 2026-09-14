"""Actual retained authority is narrowed to exact node policy before local cleanup."""

from importlib import import_module
from uuid import uuid4

import pytest

from loom_capacity_build_guard.native_recovery_sender import NativeNodeRecoveryRequestV1
from loom_capacity_build_guard.native_terminal_recovery import NativeTerminalRecoveryStore
from loom_capacity_build_guard.terminal_recovery import BuildTerminalRecoveryCoordinator
from loom_capacity_manager.contracts import canonical_digest
from tests.integration.test_native_terminal_recovery_readback import (
    build_guard_database as build_guard_database,
)
from tests.integration.test_native_terminal_recovery_readback import (
    owner_sessions as owner_sessions,
)
from tests.integration.test_native_terminal_recovery_readback import (
    prepared_input as prepared_input,
)
from tests.integration.test_native_terminal_recovery_readback import retained_attempt
from tests.integration.test_native_terminal_recovery_readback import (
    sessions as sessions,
)
from tests.unit.test_native_node_recovery_policy import scope_values


@pytest.mark.parametrize("boundary", ["exact", "preparation-only", "profile", "host", "root", "device", "maps", "installation"])
async def test_node_policy_binds_retained_history_without_accepting_caller_paths(
    prepared_input, owner_sessions, monkeypatch, boundary,
):
    module = import_module("loom_capacity_executor.native_node_recovery_policy")
    claim, profile, prepared, final, terminal = await retained_attempt(prepared_input, owner_sessions, monkeypatch,
        finalized=boundary != "preparation-only")
    factory, _engine, installation, *_ = prepared_input

    class Manager:
        async def get_build_terminal_inventory_evidence(self, intent_id):
            assert intent_id == claim.binding.intent_id
            return terminal

    assert (await BuildTerminalRecoveryCoordinator(session_factory=factory, installation=installation,
        manager=Manager()).reconcile())[0].state == "released"
    async with factory.begin() as session:
        history = await NativeTerminalRecoveryStore(session, installation=installation).read(claim.operation_id)
    values = scope_values() | dict(installation_id=installation.id, pool_id=profile.pool_id,
        profile_sha256=canonical_digest(profile), host=history.host, scratch_root="/scratch")
    if boundary == "profile":
        values["profile_sha256"] = "f" * 64
    elif boundary == "host":
        values["host"] = history.host.model_copy(update={"boot_id": uuid4()})
    elif boundary == "root":
        values["scratch_root"] = "/foreign"
    elif boundary == "device":
        values["scratch_device"] += 1
    elif boundary == "maps":
        values["uid_map"] = (values["uid_map"][0], values["uid_map"][1].model_copy(update={"outside": 300000}))
    elif boundary == "installation":
        values["installation_id"] = uuid4()
    scope = module.NativeNodeRecoveryScopeV1(**values)
    policy = module.NativeNodeRecoveryPolicyV1(management_uid=25000, scopes=(scope,))
    request = NativeNodeRecoveryRequestV1(invocation_id=uuid4(), history=history)
    if boundary not in {"exact", "preparation-only"}:
        with pytest.raises(ValueError):
            module.bind_native_node_recovery(policy, request)
        return
    bound = module.bind_native_node_recovery(policy, request)
    assert str(bound.source) == prepared.request.record.locator.directory
    assert bound.identity.inode == prepared.request.record.locator.inode
    assert bound.scope == scope
    assert bound.key == module.bind_native_node_recovery(policy, request.model_copy(update={"invocation_id": uuid4()})).key
    if final is None:
        assert bound.identity.uid_ranges == ((history.host.original_uid, 1),)
        assert bound.identity.gid_ranges == ((history.host.original_gid, 1),)
    else:
        assert bound.identity.uid_ranges == tuple((item.outside, item.count) for item in final.request.record.uid_map)
