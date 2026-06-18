"""Per-trial sandbox bridge management (#78 Phase A).

Each trial gets its own `--internal` docker network so the sandbox
container has no host route. The per-node `loom-llm-gateway-sandbox`
singleton (Phase B) joins each bridge to mediate model traffic; until
that lands, this module is callable but unwired from Trial.run().

Subnet allocation: pull a free `/24` from `10.42.{1..254}.0/24` per
worker. Allocator state is worker-local; multiple workers on the same
host SHOULD partition the pool (env-configurable second octet planned
in Phase B) to avoid CIDR collisions across worker processes.

Shells out to the `docker` CLI rather than the docker-py SDK because:
1. `docker network create --internal` is one positional flag we don't
   want to misroute through the SDK's network-driver kwargs;
2. tests can inject a fake `docker_runner` and verify exact argv;
3. the singleton (Phase B) will use `docker events`, also CLI-only.
"""

from __future__ import annotations

import asyncio
import logging
import threading
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from uuid import UUID

logger = logging.getLogger(__name__)

# Allocator pool. Skip x.0.0/24 (overlaps loopback-like patterns in
# some test environments) and x.255.0/24 (avoid edge confusion).
_MIN_INDEX = 1
_MAX_INDEX = 254
_SUBNET_TEMPLATE = "10.42.{}.0/24"
_BRIDGE_NAME_TEMPLATE = "loom-sandbox-{trial_id}"

# Bound the retry loop so a persistently-broken docker daemon fails
# fast rather than chewing through all 254 subnets each request.
_MAX_CREATE_RETRIES = 5

# (argv,) -> stdout. Default impl forks `docker`; tests inject a fake.
DockerRunner = Callable[[Sequence[str]], Awaitable[str]]


class SandboxNetworkError(Exception):
    """Raised when bridge create / connect / teardown fails."""


@dataclass(frozen=True)
class SandboxBridge:
    """Handle to a created `--internal` docker network."""

    name: str
    subnet: str
    network_id: str
    # The allocator index this bridge holds. Stored so teardown can
    # release without re-parsing the subnet CIDR.
    subnet_index: int


class SandboxNetworkAllocator:
    """Worker-local subnet allocator. Thread-safe so the RunnerPool's
    asyncio tasks (which may resolve on different threads when handing
    off to `asyncio.to_thread`) can acquire concurrently.

    The pool is finite (254 /24s); callers MUST `release` when they
    teardown the bridge or the worker eventually exhausts the pool.
    """

    def __init__(self) -> None:
        self._in_use: set[int] = set()
        self._lock = threading.Lock()

    def acquire(self) -> tuple[int, str]:
        """Reserve the smallest free index. Returns (index, CIDR)."""
        with self._lock:
            for i in range(_MIN_INDEX, _MAX_INDEX + 1):
                if i not in self._in_use:
                    self._in_use.add(i)
                    return i, _SUBNET_TEMPLATE.format(i)
        raise SandboxNetworkError(
            f"sandbox subnet pool exhausted ({_MAX_INDEX} in use)",
        )

    def release(self, index: int) -> None:
        """Return an index to the pool. Idempotent; no-op if absent."""
        with self._lock:
            self._in_use.discard(index)

    @property
    def in_use_count(self) -> int:
        with self._lock:
            return len(self._in_use)


async def _default_docker_runner(argv: Sequence[str]) -> str:
    """Fork `docker ...` and return stdout. Raises SandboxNetworkError
    on non-zero exit, propagating stderr so callers can see what
    docker complained about."""
    proc = await asyncio.create_subprocess_exec(
        "docker", *argv,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout_b, stderr_b = await proc.communicate()
    if proc.returncode != 0:
        raise SandboxNetworkError(
            f"docker {' '.join(argv)!r} exited rc={proc.returncode}: "
            f"{stderr_b.decode(errors='replace').strip()}",
        )
    return stdout_b.decode(errors="replace")


def _bridge_name(trial_id: UUID) -> str:
    return _BRIDGE_NAME_TEMPLATE.format(trial_id=trial_id)


def _is_recoverable_create_error(msg: str) -> bool:
    """Docker reports CIDR-conflict + name-collision through different
    error strings depending on engine version. Both are retryable
    (allocator picks a different index; name collision means a prior
    teardown leaked, retry after a brief wait won't help — we surface
    instead)."""
    return "Pool overlaps" in msg or "address space conflicts" in msg


async def create_sandbox_bridge(
    *,
    trial_id: UUID,
    allocator: SandboxNetworkAllocator,
    docker_runner: DockerRunner | None = None,
) -> SandboxBridge:
    """`docker network create --internal --subnet <CIDR> loom-sandbox-<trial>`.

    Retries on CIDR-overlap (another worker / dangling network claimed
    the same /24 between allocator.acquire() and docker create); each
    retry releases the colliding index and acquires a fresh one. Caps
    at _MAX_CREATE_RETRIES so a wedged docker daemon doesn't churn
    through the whole pool.
    """
    runner = docker_runner or _default_docker_runner
    name = _bridge_name(trial_id)
    last_error: str | None = None
    for _ in range(_MAX_CREATE_RETRIES):
        index, subnet = allocator.acquire()
        try:
            network_id = await runner([
                "network", "create",
                "--internal",
                "--subnet", subnet,
                name,
            ])
            return SandboxBridge(
                name=name,
                subnet=subnet,
                network_id=network_id.strip(),
                subnet_index=index,
            )
        except SandboxNetworkError as exc:
            allocator.release(index)
            msg = str(exc)
            if not _is_recoverable_create_error(msg):
                raise
            last_error = msg
            logger.warning(
                "sandbox_bridge_create_retry trial=%s subnet=%s msg=%s",
                trial_id, subnet, msg,
            )
    raise SandboxNetworkError(
        f"failed to create sandbox bridge after {_MAX_CREATE_RETRIES} "
        f"CIDR-conflict retries: {last_error}",
    )


async def connect_sandbox_to_singleton(
    *,
    bridge: SandboxBridge,
    singleton_container: str,
    docker_runner: DockerRunner | None = None,
) -> None:
    """`docker network connect <bridge> <singleton_container>`.

    Lets the per-node `loom-llm-gateway-sandbox` singleton (Phase B)
    reach the trial's sandbox. Idempotent at the caller layer — if
    the singleton is already attached, docker prints `Error response
    from daemon: endpoint with name ... already exists` and we surface
    it; Phase B's lifecycle manager is responsible for not re-
    connecting.
    """
    runner = docker_runner or _default_docker_runner
    await runner([
        "network", "connect", bridge.name, singleton_container,
    ])


async def teardown_sandbox_bridge(
    *,
    bridge: SandboxBridge,
    allocator: SandboxNetworkAllocator,
    docker_runner: DockerRunner | None = None,
) -> None:
    """`docker network rm <bridge>` and release the subnet.

    The subnet release runs in a `finally` block so even if rm fails
    (network in use, docker daemon flaky), we don't leak the /24
    forever — at worst the docker-side network gets re-claimed when
    the next worker restart prunes orphans.
    """
    runner = docker_runner or _default_docker_runner
    try:
        await runner(["network", "rm", bridge.name])
    except SandboxNetworkError as exc:
        logger.warning(
            "sandbox_bridge_teardown_rm_failed name=%s err=%s",
            bridge.name, exc,
        )
        raise
    finally:
        allocator.release(bridge.subnet_index)
