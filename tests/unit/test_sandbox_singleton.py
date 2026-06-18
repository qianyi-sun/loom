"""SandboxSingletonManager — start/attach/stop with fake docker
runner (#78 PR-B2)."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any
from uuid import UUID

import pytest

from loom_worker.sandbox_network import (
    SandboxBridge,
    SandboxNetworkError,
)
from loom_worker.sandbox_singleton import (
    SandboxSingletonManager,
    SingletonStartupError,
)

_WORKER = UUID("00000000-0000-0000-0000-000000000abc")
_IMAGE = "loom-llm-gateway-sandbox:dev"
_SECRETS_DIR = "/var/run/loom/singleton-secrets"


class FakeDockerRunner:
    """argv-recording fake docker. Tuple-keyed dispatch for predefined
    responses; falls through to a default."""

    def __init__(self, responses: list[Any]) -> None:
        self._responses = list(responses)
        self.calls: list[list[str]] = []

    async def __call__(self, argv: Sequence[str]) -> str:
        self.calls.append(list(argv))
        if not self._responses:
            raise AssertionError(f"unexpected docker call: {argv}")
        nxt = self._responses.pop(0)
        if isinstance(nxt, Exception):
            raise nxt
        return nxt


def _bridge(name: str = "loom-sandbox-xyz") -> SandboxBridge:
    return SandboxBridge(
        name=name, subnet="10.42.1.0/24",
        network_id="net-id", subnet_index=1,
    )


# ─── start (fresh container) ────────────────────────────────────────


async def test_start_spawns_new_container() -> None:
    runner = FakeDockerRunner([
        "",                       # ps query — no existing container
        "container-abc-id\n",     # run -d response = new container id
    ])
    mgr = SandboxSingletonManager(
        worker_id=_WORKER, image=_IMAGE,
        secrets_host_dir=_SECRETS_DIR, docker_runner=runner,
    )
    await mgr.start()
    assert mgr.container_id == "container-abc-id"
    # First call is the existence probe.
    assert runner.calls[0][:2] == ["ps", "-a"]
    # Second is the spawn — verify the critical flags + mount.
    spawn = runner.calls[1]
    assert spawn[0] == "run"
    assert "--name" in spawn
    assert (
        spawn[spawn.index("--name") + 1]
        == "loom-llm-gateway-sandbox-00000000-0000-0000-0000-000000000abc"
    )
    assert "--network" in spawn
    assert spawn[spawn.index("--network") + 1] == "loom-uplink"
    assert "-v" in spawn
    assert (
        spawn[spawn.index("-v") + 1]
        == "/var/run/loom/singleton-secrets:/run/loom:ro"
    )
    # Image name appears as a positional arg, followed by Go-binary
    # flags (--listen-addr, --upstream-url, --tls-cert-file, …).
    assert _IMAGE in spawn
    assert "--tls-cert-file" in spawn
    # Image MUST precede the binary flags.
    assert spawn.index(_IMAGE) < spawn.index("--tls-cert-file")


# ─── start (idempotent: existing container) ─────────────────────────


async def test_start_reattaches_to_existing_running_container() -> None:
    # `docker ps -a -q --filter name=...` returns existing ID; the
    # subsequent inspect says Running=true, so no `docker start`
    # needed. Total = 2 calls.
    runner = FakeDockerRunner([
        "existing-id\n",
        "true\n",
    ])
    mgr = SandboxSingletonManager(
        worker_id=_WORKER, image=_IMAGE,
        secrets_host_dir=_SECRETS_DIR, docker_runner=runner,
    )
    await mgr.start()
    assert mgr.container_id == "existing-id"
    assert len(runner.calls) == 2
    assert runner.calls[1][:2] == ["inspect", "-f"]


async def test_start_restarts_existing_stopped_container() -> None:
    # Existing container but stopped → manager calls `docker start`.
    runner = FakeDockerRunner([
        "existing-id\n",
        "false\n",         # not running
        "existing-id\n",   # docker start response
    ])
    mgr = SandboxSingletonManager(
        worker_id=_WORKER, image=_IMAGE,
        secrets_host_dir=_SECRETS_DIR, docker_runner=runner,
    )
    await mgr.start()
    assert mgr.container_id == "existing-id"
    assert runner.calls[2] == ["start", "existing-id"]


async def test_start_propagates_docker_error_as_startup_error() -> None:
    runner = FakeDockerRunner([
        "",                                                  # no existing
        SandboxNetworkError("docker daemon unreachable"),    # run fails
    ])
    mgr = SandboxSingletonManager(
        worker_id=_WORKER, image=_IMAGE,
        secrets_host_dir=_SECRETS_DIR, docker_runner=runner,
    )
    with pytest.raises(SingletonStartupError, match="failed to start"):
        await mgr.start()


# ─── attach_to_bridge ───────────────────────────────────────────────


async def test_attach_invokes_network_connect() -> None:
    runner = FakeDockerRunner([
        "",                                  # ps — no existing
        "container-attach-id\n",             # run
        "",                                  # network connect — success
    ])
    mgr = SandboxSingletonManager(
        worker_id=_WORKER, image=_IMAGE,
        secrets_host_dir=_SECRETS_DIR, docker_runner=runner,
    )
    await mgr.start()
    await mgr.attach_to_bridge(_bridge())
    # connect_sandbox_to_singleton issues: docker network connect <bridge> <id>
    assert runner.calls[-1] == [
        "network", "connect", "loom-sandbox-xyz", "container-attach-id",
    ]


async def test_attach_before_start_raises_clear_error() -> None:
    mgr = SandboxSingletonManager(
        worker_id=_WORKER, image=_IMAGE,
        secrets_host_dir=_SECRETS_DIR,
        docker_runner=FakeDockerRunner([]),
    )
    with pytest.raises(SingletonStartupError, match="before start"):
        await mgr.attach_to_bridge(_bridge())


# ─── stop ────────────────────────────────────────────────────────────


async def test_stop_runs_stop_and_rm() -> None:
    runner = FakeDockerRunner([
        "",                          # ps
        "container-stop-id\n",       # run
        "",                          # stop
        "",                          # rm -f
    ])
    mgr = SandboxSingletonManager(
        worker_id=_WORKER, image=_IMAGE,
        secrets_host_dir=_SECRETS_DIR, docker_runner=runner,
    )
    await mgr.start()
    await mgr.stop()
    assert runner.calls[-2] == ["stop", "container-stop-id"]
    assert runner.calls[-1] == ["rm", "-f", "container-stop-id"]
    assert mgr.container_id is None


async def test_stop_delete_false_keeps_container() -> None:
    runner = FakeDockerRunner([
        "", "container-x\n",
        "",   # stop only — no rm
    ])
    mgr = SandboxSingletonManager(
        worker_id=_WORKER, image=_IMAGE,
        secrets_host_dir=_SECRETS_DIR, docker_runner=runner,
    )
    await mgr.start()
    await mgr.stop(delete=False)
    assert runner.calls[-1] == ["stop", "container-x"]


async def test_stop_before_start_is_noop() -> None:
    runner = FakeDockerRunner([])  # no calls expected
    mgr = SandboxSingletonManager(
        worker_id=_WORKER, image=_IMAGE,
        secrets_host_dir=_SECRETS_DIR, docker_runner=runner,
    )
    await mgr.stop()  # no error, no calls
    assert runner.calls == []


async def test_stop_swallows_docker_errors() -> None:
    # Docker rm fails after stop succeeds — manager should log + move
    # on (defensive cleanup; the operator can clean up by hand).
    runner = FakeDockerRunner([
        "", "container-y\n",
        SandboxNetworkError("stop failed"),
        SandboxNetworkError("rm failed"),
    ])
    mgr = SandboxSingletonManager(
        worker_id=_WORKER, image=_IMAGE,
        secrets_host_dir=_SECRETS_DIR, docker_runner=runner,
    )
    await mgr.start()
    # Both errors are swallowed; the manager still clears its state.
    await mgr.stop()
    assert mgr.container_id is None
