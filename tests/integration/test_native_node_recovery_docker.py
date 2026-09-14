"""Real kernel/filesystem recovery composition; SSH/sudo is separately verified."""

from uuid import uuid4

import pytest

from loom_capacity_build_guard.native_recovery_sender import NativeNodeRecoveryRequestV1
from loom_capacity_build_guard.native_terminal_recovery import NativeTerminalRecoveryStore
from loom_capacity_build_guard.terminal_recovery import BuildTerminalRecoveryCoordinator
from loom_capacity_manager.contracts import canonical_bytes
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


@pytest.mark.docker
@pytest.mark.parametrize("boundary", ["exact", "locator", "delegated"])
async def test_fixed_node_recovery_uses_real_private_cgroups_and_journal(
    prepared_input, owner_sessions, monkeypatch, boundary,
):
    import asyncio
    import subprocess

    from tests.integration.test_native_oci_kvm import EXECUTOR, ROOT

    claim, _profile, _prepared, _final, terminal = await retained_attempt(prepared_input, owner_sessions, monkeypatch)
    factory, _engine, installation, *_ = prepared_input

    class Manager:
        async def get_build_terminal_inventory_evidence(self, intent_id):
            assert intent_id == claim.binding.intent_id
            return terminal

    await BuildTerminalRecoveryCoordinator(session_factory=factory, installation=installation, manager=Manager()).reconcile()
    async with factory.begin() as session:
        history = await NativeTerminalRecoveryStore(session, installation=installation).read(claim.operation_id)
    request = NativeNodeRecoveryRequestV1(invocation_id=uuid4(), history=history)
    name = "loom-native-node-recovery-" + uuid4().hex
    argv = ["docker", "run", "--rm", "--init", "--interactive", "--name", name,
        "--network=none", "--cgroupns=private", "--cpus=1", "--memory=256m", "--pids-limit=64",
        "--user=0:0", "--cap-drop=ALL", "--cap-add=SYS_ADMIN", "--cap-add=CHOWN", "--cap-add=DAC_OVERRIDE",
        "--cap-add=FOWNER", "--security-opt=apparmor=unconfined", "--security-opt=seccomp=unconfined",
        "--tmpfs=/run:rw,nodev,size=16m,mode=0755", "--env=PYTHONPATH=/trusted-src", "--env=FIXTURE_MODE=" + boundary,
        "--mount", f"type=bind,src={ROOT / 'tests/support/native_kvm'},dst=/test-support,readonly",
        "--mount", f"type=bind,src={ROOT / 'src'},dst=/trusted-src,readonly",
        EXECUTOR, "python3", "/test-support/node_recovery.py"]
    try:
        result = await asyncio.to_thread(subprocess.run, argv, input=canonical_bytes(request),
            capture_output=True, timeout=60, check=False)
        assert result.returncode == 0, result.stderr.decode()
        assert ("native-node-recovery-" + boundary + "-verified").encode() in result.stdout
    finally:
        await asyncio.to_thread(subprocess.run, ["docker", "rm", "-f", name],
            capture_output=True, timeout=20, check=False)
