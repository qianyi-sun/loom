"""Per-node singleton manager for `loom-llm-gateway-sandbox` (#78 PR-B2).

PR-B1 (#220) shipped the standalone Go binary. This module is the
worker-side lifecycle that:

1. At worker startup, spawns ONE container per node from the Go
   binary image (`loom-llm-gateway-sandbox:<tag>`) on the
   `loom-uplink` docker network. Idempotent — if a container with
   the expected name already exists (worker restart, manual rerun),
   we attach to it rather than recreate.
2. For every new per-trial sandbox bridge (created by #189's
   `create_sandbox_bridge`), calls `attach_to_bridge` which runs
   `docker network connect <bridge.name> <singleton-container>`.
   That gives the sandbox a hop to the singleton's TLS port.
3. At worker SIGTERM, `stop()` cleans up the singleton.

Gated by `LOOM_WORKER_SANDBOX_ISOLATION=1`. Default off — operators
opt in when ready to use the egress chain. Default off preserves
the pre-Phase-B behavior exactly so this PR can land without
breaking any existing trial.

Like `sandbox_network.py` (#189), shells out to the docker CLI for
the same reasons: exact-argv test assertions, future `docker events`
support, no docker-py kwarg roundtripping.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from uuid import UUID

from loom_worker.sandbox_network import (
    SandboxBridge,
    SandboxNetworkError,
    connect_sandbox_to_singleton,
)

logger = logging.getLogger(__name__)

# Container + bridge naming. The singleton name is per-WORKER not
# per-node because a node may run multiple worker processes (#189
# added the worker_index partition); each worker gets its own
# singleton so attaches don't collide on Docker's network-endpoint
# limits.
_SINGLETON_NAME_TEMPLATE = "loom-llm-gateway-sandbox-{worker_id}"

# The fixed bridge each singleton uses to reach gateway-router. The
# operator/k8s side ensures this bridge exists OR docker auto-
# creates it on first reference. Per-node, not per-trial.
_UPLINK_NETWORK = "loom-uplink"

# Default container start args. These are flags on the Go binary
# (deploy/Dockerfile.gateway-sandbox's ENTRYPOINT); the secrets dir
# is bind-mounted from the worker, which prepares cert + JWT key
# there as part of singleton.start().
_BINARY_FLAGS = (
    "--listen-addr", ":8443",
    "--upstream-url", "http://gateway-router:30443",
    "--jwt-signing-key-file", "/run/loom/jwt-signing-key",
    "--tls-cert-file", "/run/loom/loom-sandbox-gateway.crt",
    "--tls-key-file", "/run/loom/loom-sandbox-gateway.key",
)


class SingletonStartupError(Exception):
    """Raised when docker fails to start (or attach to) the singleton."""


# (argv,) -> stdout. Default impl forks `docker`; tests inject a fake.
DockerRunner = Callable[[Sequence[str]], Awaitable[str]]


@dataclass
class SandboxSingletonManager:
    """Lifecycle of the per-worker `loom-llm-gateway-sandbox` container.

    `start()` must be called before any `attach_to_bridge`.
    `stop()` is safe to call multiple times. The manager is NOT
    thread-safe; the worker calls into it from one asyncio loop.
    """

    worker_id: UUID
    image: str
    secrets_host_dir: str
    docker_runner: DockerRunner | None = None
    container_id: str | None = field(default=None, init=False)
    container_name: str = field(default="", init=False)

    def __post_init__(self) -> None:
        self.container_name = _SINGLETON_NAME_TEMPLATE.format(
            worker_id=self.worker_id,
        )

    async def start(self) -> None:
        """Spawn or attach to the singleton container. Idempotent: a
        re-run of `worker up` reattaches rather than recreate, so an
        in-flight trial's bridge endpoint doesn't get dropped."""
        runner = self.docker_runner or _default_docker_runner
        # Check for an existing container by name. `docker ps -a -q
        # --filter` returns the container ID, or empty if not found.
        existing = (await runner([
            "ps", "-a", "-q", "--filter", f"name=^{self.container_name}$",
        ])).strip()
        if existing:
            # Existing container. Make sure it's running (was the
            # worker killed mid-run?), then attach to its ID.
            logger.info(
                "sandbox_singleton_reattach name=%s id=%s",
                self.container_name, existing,
            )
            await self._ensure_running(runner, existing)
            self.container_id = existing
            return

        # No existing container. Spawn fresh.
        argv = [
            "run", "-d",
            "--name", self.container_name,
            "--network", _UPLINK_NETWORK,
            "-v", f"{self.secrets_host_dir}:/run/loom:ro",
            "--restart", "unless-stopped",
            self.image,
            *list(_BINARY_FLAGS),
        ]
        try:
            stdout = await runner(argv)
        except SandboxNetworkError as exc:
            raise SingletonStartupError(
                f"failed to start sandbox singleton container "
                f"{self.container_name!r}: {exc}",
            ) from exc
        self.container_id = stdout.strip()
        logger.info(
            "sandbox_singleton_started name=%s id=%s image=%s",
            self.container_name, self.container_id, self.image,
        )

    async def _ensure_running(
        self, runner: DockerRunner, container_id: str,
    ) -> None:
        """If the existing container is stopped (worker crash leaving
        it in 'exited'), `docker start` it. Live containers no-op."""
        state = (await runner([
            "inspect", "-f", "{{.State.Running}}", container_id,
        ])).strip()
        if state == "true":
            return
        logger.warning(
            "sandbox_singleton_restart name=%s id=%s prior_state=%s",
            self.container_name, container_id, state,
        )
        await runner(["start", container_id])

    async def attach_to_bridge(self, bridge: SandboxBridge) -> None:
        """Connect the singleton container to a per-trial bridge.

        Idempotent at the caller layer — docker rejects re-connect to
        the same network with `endpoint with name ... already exists`,
        which `connect_sandbox_to_singleton` surfaces as a
        SandboxNetworkError. The trial-runner caller is responsible
        for tracking which bridges it's already attached.
        """
        if self.container_id is None:
            raise SingletonStartupError(
                "attach_to_bridge called before start() — programmer error",
            )
        await connect_sandbox_to_singleton(
            bridge=bridge,
            singleton_container=self.container_id,
            docker_runner=self.docker_runner,
        )

    async def stop(self, *, delete: bool = True) -> None:
        """Best-effort teardown. Safe to call multiple times + safe
        to call before `start` (no-op).

        `delete=False` keeps the stopped container around for
        operator inspection (`docker logs loom-llm-gateway-sandbox-...`
        post-mortem). Default `True` matches the worker SIGTERM path.
        """
        if self.container_id is None:
            return
        runner = self.docker_runner or _default_docker_runner
        try:
            await runner(["stop", self.container_id])
        except SandboxNetworkError as exc:
            logger.warning(
                "sandbox_singleton_stop_failed name=%s id=%s err=%s",
                self.container_name, self.container_id, exc,
            )
        if delete:
            try:
                await runner(["rm", "-f", self.container_id])
            except SandboxNetworkError as exc:
                logger.warning(
                    "sandbox_singleton_rm_failed name=%s id=%s err=%s",
                    self.container_name, self.container_id, exc,
                )
        self.container_id = None


async def _default_docker_runner(argv: Sequence[str]) -> str:
    """Imported lazily from sandbox_network to share the impl. This
    module exposes the alias so test code can stub at the right
    level."""
    from loom_worker.sandbox_network import _default_docker_runner as _impl
    return await _impl(argv)
