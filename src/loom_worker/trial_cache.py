"""Per-trial agent-install image cache (#317 Phase 1).

When a worker about to spawn a trial finds the task image doesn't have
the chosen agent's CLI, it builds a content-addressed layered image
that adds the agent install on top of the task image, then runs the
trial against the layered image. The build is shared:

- LOCAL CACHE: the worker's own Docker daemon caches the layered
  image; subsequent trials with the same (task_image, agent) on this
  worker hit instantly.

- REGISTRY CACHE (optional): if `trial_cache_registry_repo` is set,
  workers pull the cached image from a shared registry before
  building. After local build, they push for the next worker.

- BUILD COORDINATION: cluster-wide. Workers claim a builder slot via
  the `active_trial_cache_builds` table (4 CP HTTP routes); only the
  claimer builds, others wait. Crash-safe via TTL + heartbeat refresh.

See `.claude/plans/2026-06-22-issue-317-agent-runtime-install.md` for
the full design including the v3 self-review log.
"""
from __future__ import annotations

import asyncio
import contextlib
import logging
import random
import textwrap
from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING, Any, Protocol
from uuid import UUID

import docker
from docker.errors import APIError, BuildError, ImageNotFound, NotFound

if TYPE_CHECKING:
    from loom_launcher.adapter import AgentAdapter

    from loom_worker.config import WorkerSettings
    from loom_worker.control_plane_client import HttpControlPlaneClient

logger = logging.getLogger(__name__)

LOCAL_TAG_PREFIX = "loom-trial-cache"
HEARTBEAT_INTERVAL_SEC = 60.0


class TrialCacheError(RuntimeError):
    """Raised when the worker cannot resolve a layered trial image."""


def _normalize_install_script(raw: str | None) -> str | None:
    """Empty / whitespace-only scripts are treated as None (no install
    needed — let the trial run on the bare task image)."""
    if raw is None:
        return None
    if not raw.strip():
        return None
    return raw


def _cache_key(*, task_image_digest: str, install_script: str) -> str:
    """Content-addressed cache key.

    Inputs that affect the resulting image bytes: the base image
    content (its digest) + the install script text. Output: 32 hex
    chars = 128 bits, comfortably collision-safe at any realistic
    Loom scale (Docker tags accept the full 64-hex; 32 chosen for
    readability)."""
    # NUL separator prevents (digest+script) collision between
    # (digestA, scriptB) and (digestAscriptB-prefix, suffix).
    material = (task_image_digest + "\x00" + install_script).encode("utf-8")
    return sha256(material).hexdigest()[:32]


async def _pull_or_get_digest(
    client: Any, image_ref: str, *, timeout_sec: float,
) -> str:
    """Return the local content digest for an image reference.

    - `sha256:...` → returned unchanged
    - tag already in local Docker daemon → inspect
    - tag not local → pull (with timeout) then inspect

    Timeout sized for the largest expected base image; SWE-Bench
    instance images are 1-2GB and on slow networks the pull can take
    10-20 min, hence the operator-configurable
    `trial_cache_base_image_pull_timeout_sec` (default 1800s).
    """
    if image_ref.startswith("sha256:"):
        return image_ref
    try:
        image = await asyncio.to_thread(client.images.get, image_ref)
        return str(image.id)
    except (ImageNotFound, NotFound):
        pass
    try:
        await asyncio.wait_for(
            asyncio.to_thread(client.images.pull, image_ref),
            timeout=timeout_sec,
        )
    except TimeoutError as exc:
        raise TrialCacheError(
            f"timed out pulling task image {image_ref!r} after {timeout_sec:g}s",
        ) from exc
    except APIError as exc:
        raise TrialCacheError(
            f"failed to pull task image {image_ref!r}: {exc}",
        ) from exc
    image = await asyncio.to_thread(client.images.get, image_ref)
    return str(image.id)


def _image_exists_locally(client: Any, tag: str) -> bool:
    try:
        client.images.get(tag)
        return True
    except (ImageNotFound, NotFound):
        return False


def _tag_alias(client: Any, source: str, target: str) -> None:
    """Tag an already-pulled image with a second name. Used after
    pulling `<registry>/<repo>:<key>` to also expose it as
    `loom-trial-cache:<key>` locally."""
    repo, _, tag = target.partition(":")
    client.images.get(source).tag(repository=repo, tag=tag or "latest")


def _build_layered_image_sync(
    *,
    client: Any,
    tag: str,
    base_digest: str,
    install_script: str,
) -> None:
    """Synthesize a Dockerfile that layers `install_script` on top of
    `base_digest`, then run `docker build`. Tempdir cleanup is automatic.

    Layer pattern: `COPY install.sh /tmp/install.sh; RUN bash /tmp/install.sh`
    — beats inline `RUN bash -c '<script>'` because shell quoting
    doesn't bite when scripts contain $, backticks, or single quotes."""
    with TemporaryDirectory(prefix="loom-trial-build-") as ctx:
        ctx_path = Path(ctx)
        (ctx_path / "install.sh").write_text(install_script)
        (ctx_path / "Dockerfile").write_text(textwrap.dedent(f"""\
            FROM {base_digest}
            COPY install.sh /tmp/install.sh
            RUN bash /tmp/install.sh && rm /tmp/install.sh
        """))
        try:
            client.images.build(
                path=str(ctx_path),
                tag=tag,
                rm=True,
                forcerm=True,
                pull=False,
                labels={
                    "loom.trial-cache": "true",
                    "loom.cache-key": tag.split(":", 1)[1],
                    "loom.created-at": datetime.now(UTC).isoformat(),
                    # No loom.last-used-at: Docker labels can't be
                    # updated post-build; eviction uses creation-age
                    # via `docker image prune --filter until=`.
                },
            )
        except (APIError, BuildError) as exc:
            raise TrialCacheError(
                f"failed to build layered image {tag!r}: {exc}",
            ) from exc


@contextlib.asynccontextmanager
async def _builder_with_heartbeat(
    cp_client: HttpControlPlaneClient,
    cache_key: str,
    worker_id: UUID,
    *,
    ttl_sec: float,
    interval_sec: float = HEARTBEAT_INTERVAL_SEC,
) -> AsyncIterator[None]:
    """Spawn a background task that refreshes the slot TTL every
    `interval_sec` seconds until the context exits.

    On crash (cancellation, OOM, segfault), the heartbeat task dies →
    next refresh doesn't happen → slot expires within ttl_sec → another
    worker can steal it. This decouples maximum allowed build duration
    (TTL) from crash-recovery latency (~interval_sec)."""
    stop = asyncio.Event()

    async def _beat() -> None:
        while not stop.is_set():
            try:
                await asyncio.wait_for(stop.wait(), timeout=interval_sec)
            except TimeoutError:
                try:
                    refreshed = await cp_client.refresh_trial_cache_slot(
                        cache_key, worker_id, ttl_sec=ttl_sec,
                    )
                    if not refreshed:
                        logger.warning(
                            "trial_cache slot stolen mid-build "
                            "cache_key=%s; another worker is now the builder",
                            cache_key,
                        )
                except Exception:
                    logger.exception(
                        "trial_cache heartbeat failed cache_key=%s", cache_key,
                    )

    task = asyncio.create_task(_beat())
    try:
        yield
    finally:
        stop.set()
        with contextlib.suppress(asyncio.CancelledError):
            await task


async def resolve_trial_image(
    *,
    task_image: str,
    adapter: AgentAdapter,
    settings: WorkerSettings,
    cp_client: HttpControlPlaneClient,
    worker_id: UUID,
    docker_client: Any | None = None,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> str:
    """Return the Docker image tag the worker should `docker run` for
    this trial.

    Flow (see plan §Architecture):

    1. If the adapter declares no install_script (oracle, in-box,
       legacy), just return the task image as-is.
    2. Resolve task image to a digest (pull if needed).
    3. Compute cache_key = sha256(digest + install_script)[:32].
    4. Local cache hit → return.
    5. Claim builder slot via CP HTTP route.
       - If not the builder: poll cheaply (SELECT) for the slot to
         disappear OR the image to appear locally/in the registry.
         Re-claim only when the slot row is gone.
       - If the builder: try registry pull first; on miss, build
         locally; push to registry (awaited under the slot to prevent
         the next worker from rebuilding).
    6. Return the layered image tag.
    """
    # Phase 1 adapters (aider, claude-code, openhands) declare
    # install_script; the 9 legacy adapters don't yet — treat the
    # missing attribute as "no install needed" until Phase 2 lands.
    install_script = _normalize_install_script(
        getattr(adapter, "install_script", None),
    )
    if install_script is None:
        return task_image

    client = docker_client if docker_client is not None else docker.from_env()

    # Step 2: resolve task image to a stable digest.
    task_image_digest = await _pull_or_get_digest(
        client,
        task_image,
        timeout_sec=settings.trial_cache_base_image_pull_timeout_sec,
    )

    # Step 3: cache key + tag names.
    cache_key = _cache_key(
        task_image_digest=task_image_digest, install_script=install_script,
    )
    local_tag = f"{LOCAL_TAG_PREFIX}:{cache_key}"
    registry_repo = settings.trial_cache_registry_repo
    registry_tag: str | None = (
        f"{registry_repo}:{cache_key}" if registry_repo else None
    )

    # Step 4: local hit?
    if await asyncio.to_thread(_image_exists_locally, client, local_tag):
        return local_tag

    # Step 5: claim or wait for a builder slot.
    i_am_builder = await cp_client.claim_trial_cache_slot(
        cache_key, worker_id,
        ttl_sec=settings.trial_cache_build_lock_timeout_sec,
    )
    while not i_am_builder:
        await sleep(random.uniform(2.0, 5.0))

        # Most-common exit paths first.
        if await asyncio.to_thread(_image_exists_locally, client, local_tag):
            return local_tag
        if registry_tag and await _try_registry_pull(
            client, registry_tag, local_tag, settings,
        ):
            return local_tag

        # Cheap probe: is some other worker still holding the slot?
        if await cp_client.trial_cache_slot_exists(cache_key):
            continue
        # Slot is gone (builder finished or crashed). Race to claim.
        i_am_builder = await cp_client.claim_trial_cache_slot(
            cache_key, worker_id,
            ttl_sec=settings.trial_cache_build_lock_timeout_sec,
        )

    # We're the builder. Hold the slot through build + push via the
    # heartbeat-refreshed context manager. Long pulls/builds extend
    # the TTL every 60s; on crash, slot expires within ~60s.
    async with _builder_with_heartbeat(
        cp_client, cache_key, worker_id,
        ttl_sec=settings.trial_cache_build_lock_timeout_sec,
    ):
        try:
            # Even as the builder, try the registry first — another
            # worker may have pushed between our claim and now (race
            # window: slot disappeared, we got it, someone else's push
            # had completed by the time we acquired). Returning here
            # still runs the `finally` clause that releases the slot.
            if registry_tag and await _try_registry_pull(
                client, registry_tag, local_tag, settings,
            ):
                return local_tag

            await asyncio.to_thread(
                _build_layered_image_sync,
                client=client,
                tag=local_tag,
                base_digest=task_image_digest,
                install_script=install_script,
            )

            # AWAITED push under the slot — prevents the v3 race
            # where async push lets the next worker pull-miss and
            # build redundantly.
            if registry_tag:
                try:
                    await asyncio.to_thread(
                        _push_image, client, local_tag, registry_tag,
                    )
                except Exception as exc:
                    logger.warning(
                        "trial_cache push failed cache_key=%s "
                        "(next worker will rebuild): %s",
                        cache_key, exc,
                    )
        finally:
            with contextlib.suppress(Exception):
                await cp_client.release_trial_cache_slot(cache_key, worker_id)

    return local_tag


async def _try_registry_pull(
    client: Any,
    registry_tag: str,
    local_tag: str,
    settings: WorkerSettings,
) -> bool:
    """Pull `registry_tag` with a bounded timeout. On success, tag it
    locally as `local_tag` and return True. On any error (missing tag,
    timeout, auth failure), return False."""
    try:
        await asyncio.wait_for(
            asyncio.to_thread(client.images.pull, registry_tag),
            timeout=settings.trial_cache_registry_pull_timeout_sec,
        )
    except (TimeoutError, ImageNotFound, NotFound, APIError) as exc:
        logger.debug(
            "trial_cache registry pull miss %s: %s", registry_tag, exc,
        )
        return False
    try:
        await asyncio.to_thread(_tag_alias, client, registry_tag, local_tag)
    except APIError as exc:
        logger.warning(
            "trial_cache pulled %s but failed to alias as %s: %s",
            registry_tag, local_tag, exc,
        )
        return False
    return True


def _push_image(client: Any, source_tag: str, registry_tag: str) -> None:
    """Tag `source_tag` as `registry_tag` then push. `push` raises on
    auth / network failure; caller logs + continues."""
    repo, _, tag = registry_tag.partition(":")
    client.images.get(source_tag).tag(repository=repo, tag=tag or "latest")
    # Docker SDK's push is generator-based; consume it so we wait for
    # completion and surface errors.
    for line in client.images.push(repository=repo, tag=tag, stream=True, decode=True):
        if isinstance(line, dict) and line.get("errorDetail"):
            raise APIError(line["errorDetail"].get("message", "push failed"))


# ─── Eviction ──────────────────────────────────────────────────────


def evict_stale_cache(client: Any, settings: WorkerSettings) -> None:
    """Worker-side cache cleanup. Called once per hour from the worker
    daemon. Two passes:

    1. TTL prune by creation age (Docker's native filter). Removes
       cached images older than `trial_cache_ttl_hours`.
    2. Capacity backstop: if free disk drops below
       `trial_cache_min_free_gb`, delete oldest-by-creation
       trial-cache images regardless of TTL.

    Docker doesn't track last-access on images, so this is creation-
    age TTL + creation-order LRU approximation. True LRU would require
    a sidecar SQLite file (deferred — see plan risk #17)."""
    import shutil

    # Pass 1: TTL prune
    try:
        client.images.prune(filters={
            "label": "loom.trial-cache=true",
            "until": f"{settings.trial_cache_ttl_hours}h",
            "dangling": False,
        })
    except APIError as exc:
        logger.warning("trial_cache TTL prune failed: %s", exc)

    # Pass 2: capacity backstop
    try:
        free_gb = shutil.disk_usage("/").free / 1024**3
    except OSError as exc:
        logger.warning("trial_cache disk-usage probe failed: %s", exc)
        return
    if free_gb >= settings.trial_cache_min_free_gb:
        return

    try:
        cached = sorted(
            client.images.list(filters={"label": "loom.trial-cache=true"}),
            key=lambda img: img.attrs.get("Created", ""),
        )
    except APIError as exc:
        logger.warning("trial_cache list failed: %s", exc)
        return

    for img in cached:
        if free_gb >= settings.trial_cache_min_free_gb:
            break
        try:
            client.images.remove(img.id, force=True)
        except APIError as exc:
            logger.debug("trial_cache remove failed for %s: %s", img.id, exc)
            continue
        try:
            free_gb = shutil.disk_usage("/").free / 1024**3
        except OSError:
            return


class _SupportsCacheSlots(Protocol):
    """Subset of HttpControlPlaneClient the cache layer uses. Kept
    here so unit tests can supply a fake without instantiating the
    full HTTP client."""

    async def claim_trial_cache_slot(
        self, cache_key: str, worker_id: UUID, *, ttl_sec: float,
    ) -> bool: ...

    async def trial_cache_slot_exists(self, cache_key: str) -> bool: ...

    async def release_trial_cache_slot(
        self, cache_key: str, worker_id: UUID,
    ) -> None: ...

    async def refresh_trial_cache_slot(
        self, cache_key: str, worker_id: UUID, *, ttl_sec: float,
    ) -> bool: ...
