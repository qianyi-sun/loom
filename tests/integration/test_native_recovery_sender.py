"""Management owns the exact terminal read; only SSH is doubled in this lane."""

from importlib import import_module
from uuid import uuid4

import pytest

from loom_capacity_build_guard.terminal_recovery import BuildTerminalRecoveryCoordinator
from loom_capacity_manager.contracts import canonical_bytes, canonical_digest
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


@pytest.mark.parametrize("boundary", ["exact", "unreleased", "node", "profile", "host", "installation",
    "response-digest", "response-status", "response-canonical", "workload-uid", "mapped-workload-uid"])
async def test_sender_reads_committed_scope_and_pins_transport(prepared_input, owner_sessions, monkeypatch, boundary):
    module = import_module("loom_capacity_build_guard.native_recovery_sender")
    claim, profile, prepared, _final, terminal = await retained_attempt(prepared_input, owner_sessions, monkeypatch)
    factory, _engine, installation, *_ = prepared_input

    class Manager:
        async def get_build_terminal_inventory_evidence(self, intent_id):
            assert intent_id == claim.binding.intent_id
            return terminal

    if boundary != "unreleased":
        assert (await BuildTerminalRecoveryCoordinator(session_factory=factory, installation=installation,
            manager=Manager()).reconcile())[0].state == "released"
    configured = module.NativeRecoveryTargetV1(installation_id=uuid4() if boundary == "installation" else installation.id,
        pool_id=claim.binding.pool_id, node_id="foreign" if boundary == "node" else prepared.request.record.node_id,
        address="192.0.2.10", port=22, profile_sha256="f" * 64 if boundary == "profile" else canonical_digest(profile),
        host_sha256="f" * 64 if boundary == "host" else prepared.request.record.node_configuration_sha256,
        identity="/var/lib/loom-native-recovery/key", identity_sha256="a" * 64,
        known_hosts="/var/lib/loom-native-recovery/known_hosts", known_hosts_sha256="b" * 64)
    calls = []

    async def exchange(target, wire):
        request = module.NativeNodeRecoveryRequestV1.model_validate_json(wire)
        calls.append(request)
        assert target == configured and canonical_bytes(request) == wire
        assert request.history.preparation == prepared
        assert request.history.release.binding == claim.binding
        # Read retained authority through its protected API, never direct tables.
        async with factory.begin() as session:
            readback = await module.NativeTerminalRecoveryStore(session, installation=installation).read(claim.operation_id)
            assert readback == request.history
        result = module.NativeNodeRecoveryResultV1(request_sha256="f" * 64 if boundary == "response-digest" else canonical_digest(request),
            state="completed" if boundary == "response-status" else "retained", reason="unsupported")
        return canonical_bytes(result) + (b" \n" if boundary == "response-canonical" else b"\n")

    monkeypatch.setattr(module, "_exchange", exchange)
    if boundary == "workload-uid":
        monkeypatch.setattr(module.os, "getuid", lambda: prepared.request.record.original_uid)
    elif boundary == "mapped-workload-uid":
        monkeypatch.setattr(module.os, "getuid", lambda: 100001)
    sender = module.NativeRecoverySender(session_factory=factory, installation=installation, targets=(configured,))
    if boundary in {"exact", "unreleased"}:
        result = await sender.reconcile(claim.operation_id)
        assert (result is None) == (boundary == "unreleased")
        assert len(calls) == int(boundary == "exact")
    else:
        with pytest.raises(ValueError, match=r"target|binding|principal"):
            await sender.reconcile(claim.operation_id)
        assert len(calls) == int(boundary.startswith("response-"))
