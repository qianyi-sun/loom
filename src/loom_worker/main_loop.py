"""Worker main loop — wires settings → register → heartbeat thread →
claim loop → runner pool → drain.

The claim payload carries the trial's id + team + task_id + trial config
+ requires_caps; the full TaskConfig body lives behind a second
round-trip to `GET /tasks/{task_id}/bundle` (Plan 7 Task 1).

Remaining v1 limitation: the worker uses a tempfile mkdtemp() for the
task directory. The solution/ + tests/ + environment/ subtrees that live
under a real fixture directory must be fetched out-of-band — production
deploys mount a shared volume or run a git clone against
`bundle["source"]`. v1 documents this as an ops requirement and leaves
the dir empty; agents that depend on disk content (OracleAgent,
PytestVerifier with local tests) will error out until the ops
integration ships.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import platform
import shutil
import socket
import tempfile
import time
from collections.abc import Awaitable, Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
from uuid import UUID

import httpx

from loom.agent.base import AgentRuntime
from loom.agent.gateway_client import LLMGatewayClient
from loom.agent.http_gateway_client import HttpLLMGatewayClient
from loom.agent.litellm import LiteLLMAgent
from loom.agent.oracle import OracleAgent
from loom.agent.terminus2.runtime import LoomTerminus2Runtime
from loom.driver.docker import DockerDriver
from loom.errors import AgentError, classify_failure_message
from loom.models.result import FailureReason
from loom.models.task import TaskConfig
from loom.models.trial import RetryPolicy, RetryReason, TrialConfig
from loom.models.types import ModelSpec
from loom.retry import next_attempt_at
from loom.security.redaction import redact_text
from loom.startup_retry import (
    DEFAULT_STARTUP_RETRY_CONFIG,
    StartupRetryConfig,
    retry_startup_dependency,
    retry_startup_dependency_sync,
)
from loom.task_bundle_compat import validate_task_dir_compatibility
from loom.trajectory.cp_event_sink import CpEventSink
from loom.trajectory.storage import MinioObjectStore, ObjectStore
from loom.trial.workspace import TB21_AGENT_WORKSPACE_POLICY, WorkspaceStagingPolicy
from loom.verifier.base import Verifier
from loom.verifier.pytest_verifier import PytestVerifier
from loom.verifier.script_verifier import ScriptVerifier
from loom_worker.config import WorkerSettings
from loom_worker.control_plane_client import HttpControlPlaneClient, StepTokenClient
from loom_worker.heartbeat import HeartbeatThread
from loom_worker.materializers import (
    build_default_materializers,
    dispatch_materialize,
)
from loom_worker.orphan_cleanup import cleanup_orphan_trajectories
from loom_worker.orphan_containers import cleanup_orphan_sandbox_containers
from loom_worker.runner_pool import RunnerPool
from loom_worker.sandbox_network import SandboxNetworkAllocator
from loom_worker.sandbox_singleton import (
    SandboxSingletonManager,
    SingletonStartupError,
)
from loom_worker.signal_handler import ShutdownState, install_signal_handlers
from loom_worker.task_image import resolve_task_image
from loom_worker.task_sidecars import DockerTaskSidecarRuntime
from loom_worker.trial_cache import (
    _daemon_build_slot,
    evict_stale_cache,
    resolve_trial_image,
)
from loom_worker.trial_cancellation_watchdog import run_with_watchdog
from loom_worker.trial_runner import AgentFactory, LocalTrialRunner
from loom_worker.vllm_registry import WorkerVLLMRegistry

_DOCKER_HOST_GATEWAY_EXTRA_HOSTS: tuple[tuple[str, str], ...] = (
    ("host.docker.internal", "host-gateway"),
)
_SETUP_FAILURE_MESSAGE_LIMIT = 1000
_SETUP_FAILURE_HEAD_CHARS = 360
_SETUP_FAILURE_TRUNCATION_MARKER = (
    "\n...[truncated setup diagnostic; preserved trailing output]...\n"
)

logger = logging.getLogger(__name__)

_VERIFIER_CTORS: dict[str, Callable[..., Verifier]] = {
    "pytest": PytestVerifier,
    "script": ScriptVerifier,
}


def _tb21_workspace_staging_policy_from_provenance(
    raw_policy: object,
) -> WorkspaceStagingPolicy:
    """Parse only the reviewed TB2.1 rev-6 workspace boundary.

    WorkspaceStagingPolicy intentionally supports future benchmark-specific
    layouts.  The physical TB2.1 profile is stricter: its four private paths
    are part of the reviewed immutable contract, so a merely valid alternate
    policy must fail before materialization or driver startup.
    """
    if not isinstance(raw_policy, dict):
        raise ValueError("TB2.1 task is missing canonical private workspace isolation policy")
    try:
        policy = WorkspaceStagingPolicy.from_provenance(raw_policy)
    except ValueError as exc:
        raise ValueError("TB2.1 task has invalid private workspace isolation policy") from exc
    canonical = WorkspaceStagingPolicy.from_provenance(TB21_AGENT_WORKSPACE_POLICY)
    if policy != canonical:
        raise ValueError("TB2.1 task requires the canonical private workspace isolation policy")
    return policy


def _host_cpu_arch() -> str:
    machine = platform.machine().lower()
    if machine in {"aarch64", "arm64"}:
        return "arm64"
    return "x86_64"


def _worker_hostname(configured_hostname: str | None) -> str:
    if configured_hostname:
        return configured_hostname
    return socket.gethostname()


_DEFAULT_CAPS = [
    {
        "os": "linux",
        "cpu_arch": _host_cpu_arch(),
        "gpu_vendor": "none",
        "network_policies": ["public", "no-network", "allowlist"],
        "dynamic_network_policy": True,
        "mounted_fs": True,
        "resource_modes": ["auto", "limit", "guarantee"],
    }
]


@dataclass
class _IdleExitTracker:
    after_seconds: float | None
    now: Callable[[], float] = time.monotonic
    _idle_started_at: float | None = None
    _idle_for_seconds: float = 0.0

    def __post_init__(self) -> None:
        if self.after_seconds is not None and self.after_seconds < 0:
            raise ValueError("idle_exit_after_seconds must be >= 0")

    @property
    def idle_for_seconds(self) -> float:
        return self._idle_for_seconds

    def observe(self, *, claimed: int, in_flight: int) -> bool:
        if self.after_seconds is None:
            self._idle_started_at = None
            self._idle_for_seconds = 0.0
            return False
        if claimed > 0 or in_flight > 0:
            self._idle_started_at = None
            self._idle_for_seconds = 0.0
            return False

        current = self.now()
        if self._idle_started_at is None:
            self._idle_started_at = current
        self._idle_for_seconds = current - self._idle_started_at
        return self._idle_for_seconds >= self.after_seconds


def _resolve_blocking_io_max_workers(settings: WorkerSettings) -> int:
    configured = settings.blocking_io_max_workers
    if configured is not None:
        if configured < 1:
            raise ValueError("blocking_io_max_workers must be >= 1")
        return configured
    concurrency = max(1, settings.max_concurrent)
    return max(32, min(concurrency * 4, 256))


def _configure_blocking_io_executor(settings: WorkerSettings) -> None:
    max_workers = _resolve_blocking_io_max_workers(settings)
    executor = ThreadPoolExecutor(
        max_workers=max_workers,
        thread_name_prefix="loom-worker-io",
    )
    asyncio.get_running_loop().set_default_executor(executor)
    logger.info("worker_blocking_io_executor_configured max_workers=%d", max_workers)


def _build_worker_object_store(settings: WorkerSettings) -> MinioObjectStore:
    return MinioObjectStore(
        endpoint_url=settings.minio_endpoint,
        access_key=settings.minio_access_key.get_secret_value(),
        secret_key=settings.minio_secret_key.get_secret_value(),
        region=settings.minio_region,
        max_pool_connections=settings.minio_max_pool_connections,
        connect_timeout=settings.minio_connect_timeout_sec,
        read_timeout=settings.minio_read_timeout_sec,
        operation_timeout=settings.minio_operation_timeout_sec,
        operation_attempts=settings.minio_operation_attempts,
    )


def _docker_registry_auth_summary(
    config_path: Path | None = None,
) -> dict[str, object]:
    if config_path is None:
        docker_config = os.environ.get("DOCKER_CONFIG")
        if docker_config:
            config_path = Path(docker_config) / "config.json"
        else:
            config_path = Path.home() / ".docker" / "config.json"

    summary: dict[str, object] = {
        "config_path": str(config_path),
        "present": False,
        "auth_registries": [],
        "uses_credential_store": False,
    }
    try:
        present = config_path.is_file()
    except OSError:
        return summary
    if not present:
        return summary

    summary["present"] = True
    try:
        data = json.loads(config_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return summary

    auths = data.get("auths")
    if isinstance(auths, dict):
        summary["auth_registries"] = sorted(str(key) for key in auths)
    cred_helpers = data.get("credHelpers")
    summary["uses_credential_store"] = bool(
        data.get("credsStore") or (isinstance(cred_helpers, dict) and cred_helpers)
    )
    return summary


def _log_docker_registry_auth_summary() -> None:
    summary = _docker_registry_auth_summary()
    logger.info(
        "docker_registry_auth_config config_path=%s present=%s "
        "auth_registries=%s uses_credential_store=%s",
        summary["config_path"],
        summary["present"],
        summary["auth_registries"],
        summary["uses_credential_store"],
    )


async def run_worker(settings: WorkerSettings) -> None:
    state = ShutdownState()
    install_signal_handlers(state)

    settings.trajectory_cache_dir.mkdir(parents=True, exist_ok=True)
    _configure_blocking_io_executor(settings)
    _log_docker_registry_auth_summary()

    async with (
        httpx.AsyncClient(
            base_url=str(settings.control_plane_url),
            timeout=30.0,
        ) as cp_http,
        httpx.AsyncClient(
            base_url=str(settings.gateway_url),
            timeout=120.0,
        ) as gw_http,
    ):
        cp_client = HttpControlPlaneClient(
            base_url=str(settings.control_plane_url),
            token=settings.token.get_secret_value(),
            _client=cp_http,
        )
        gateway_client = HttpLLMGatewayClient(
            base_url=str(settings.gateway_url),
            token=settings.token.get_secret_value(),
            _client=gw_http,
        )

        info = await _register_worker_with_retry(
            cp_client=cp_client,
            settings=settings,
        )
        worker_id = UUID(info["worker_id"])
        logger.info("worker_registered worker_id=%s", worker_id)

        _run_orphan_cleanup(settings, worker_id)
        _run_trial_cache_eviction(settings)

        sync_http = httpx.Client(
            base_url=str(settings.control_plane_url),
            timeout=5.0,
        )
        token_value = settings.token.get_secret_value()

        def _hb_tick() -> None:
            sync_http.post(
                f"/workers/{worker_id}/heartbeat",
                headers={"Authorization": f"Bearer {token_value}"},
            )

        hb = HeartbeatThread(
            worker_id=worker_id,
            interval_sec=settings.heartbeat_interval_sec,
            tick_fn=_hb_tick,
        )
        hb.start()

        try:
            pool = RunnerPool(max_concurrent=settings.max_concurrent)
            object_store = _build_worker_object_store(settings)
            await _ensure_runtime_buckets(object_store)
            idle_exit = _IdleExitTracker(
                after_seconds=settings.idle_exit_after_seconds,
            )
            # PR-E: per-worker vLLM registry. Opt-in via settings; the
            # `enabled=False` path still constructs the object so the
            # trial runner gets a deterministic AgentError instead of
            # a None-dereference when a trial requests local-vllm.
            vllm_registry = WorkerVLLMRegistry(
                enabled=settings.enable_worker_vllm,
                default_gpu_memory_utilization=(settings.vllm_gpu_memory_utilization),
                default_tensor_parallel_size=(settings.vllm_tensor_parallel_size),
            )

            # #188 / Phase B: per-worker sandbox-isolation resources.
            # Allocator is always built (cheap); singleton is started
            # only when isolation is on so default-off workers don't
            # need the singleton image pulled.
            sandbox_allocator = SandboxNetworkAllocator(
                worker_index=settings.sandbox_worker_index,
            )
            sandbox_singleton: SandboxSingletonManager | None = None
            if settings.sandbox_isolation:
                sandbox_singleton = SandboxSingletonManager(
                    worker_id=worker_id,
                    image=settings.sandbox_singleton_image,
                    secrets_host_dir=settings.sandbox_singleton_secrets_dir,
                )
                try:
                    await sandbox_singleton.start()
                except SingletonStartupError as exc:
                    logger.error(
                        "sandbox_singleton_start_failed err=%s — "
                        "isolation disabled for this worker",
                        exc,
                    )
                    sandbox_singleton = None

            while not state.shutting_down:
                claimed = await _claim_available_trials(
                    pool=pool,
                    settings=settings,
                    cp_client=cp_client,
                    gateway_client=gateway_client,
                    object_store=object_store,
                    worker_id=worker_id,
                    vllm_registry=vllm_registry,
                    sandbox_allocator=sandbox_allocator,
                    sandbox_singleton=sandbox_singleton,
                )
                should_idle_exit = idle_exit.observe(
                    claimed=claimed,
                    in_flight=pool.in_flight,
                )
                if should_idle_exit:
                    idle_exit_after = settings.idle_exit_after_seconds
                    if idle_exit_after is None:
                        raise RuntimeError("idle exit triggered while disabled")
                    logger.info(
                        "worker_idle_exit worker_id=%s idle_for_seconds=%.3f "
                        "idle_exit_after_seconds=%.3f",
                        worker_id,
                        idle_exit.idle_for_seconds,
                        idle_exit_after,
                    )
                    await _report_worker_idle_exit(cp_client, worker_id)
                    break
                if claimed == 0 or pool.in_flight >= settings.max_concurrent:
                    await asyncio.sleep(settings.claim_poll_interval_sec)

            logger.info(
                "drain_started timeout=%ss in_flight=%d",
                settings.drain_timeout_sec,
                pool.in_flight,
            )
            await pool.wait_all(timeout=float(settings.drain_timeout_sec))
            if pool.in_flight > 0:
                logger.warning(
                    "drain_timeout in_flight=%d — cancelling",
                    pool.in_flight,
                )
                pool.cancel_all()
                await pool.wait_all(timeout=60.0)
        finally:
            hb.stop()
            hb.join(timeout=10.0)
            sync_http.close()
            # PR-E: tear down any worker-spawned vLLMs on graceful
            # drain. Hard-kill via signal handlers in vllm_runner
            # covers crash paths.
            await vllm_registry.shutdown()


async def _register_worker_with_retry(
    *,
    cp_client: HttpControlPlaneClient,
    settings: WorkerSettings,
    retry_config: StartupRetryConfig = DEFAULT_STARTUP_RETRY_CONFIG,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> dict[str, Any]:
    return await retry_startup_dependency(
        lambda: cp_client.register(
            hostname=_worker_hostname(settings.hostname),
            version="0.0.1",
            capabilities=_DEFAULT_CAPS,
            max_concurrent=max(1, settings.max_concurrent),
            pool_name=settings.pool_name,
        ),
        operation_name="worker control-plane registration",
        config=retry_config,
        sleep=sleep,
    )


async def _report_worker_idle_exit(
    cp_client: HttpControlPlaneClient,
    worker_id: UUID,
) -> None:
    try:
        await cp_client.heartbeat(worker_id, status="idle-exit")
    except Exception:
        logger.warning(
            "worker_idle_exit_heartbeat_failed worker_id=%s",
            worker_id,
            exc_info=True,
        )


def _run_orphan_cleanup(
    settings: WorkerSettings,
    worker_id: UUID,
    *,
    retry_config: StartupRetryConfig = DEFAULT_STARTUP_RETRY_CONFIG,
    sleep: Callable[[float], None] = time.sleep,
) -> None:
    """Sync HTTP lookup against /trials/{id} — invoked once at startup."""
    token_value = settings.token.get_secret_value()

    def _lookup(trial_id: UUID) -> tuple[str, UUID | None]:
        with httpx.Client(
            base_url=str(settings.control_plane_url),
            timeout=10.0,
        ) as sync_http:
            r = sync_http.get(
                f"/trials/{trial_id}",
                headers={"Authorization": f"Bearer {token_value}"},
            )
            if r.status_code == 404:
                raise LookupError(str(trial_id))
            r.raise_for_status()
            body = r.json()
            # The Control Plane's GET /trials/{id} returns `state` but
            # currently doesn't expose `worker_id`. Owner is returned
            # as None — only `state` drives the predicate now (#416).
            return body["state"], None

    retry_startup_dependency_sync(
        lambda: cleanup_orphan_trajectories(
            cache_dir=settings.trajectory_cache_dir,
            owned_worker_id=worker_id,
            state_and_owner_lookup=_lookup,
        ),
        operation_name="worker orphan trajectory startup cleanup",
        config=retry_config,
        sleep=sleep,
    )
    _run_orphan_sandbox_cleanup(_lookup)


def _run_orphan_sandbox_cleanup(
    lookup: Callable[[UUID], tuple[str, UUID | None]],
) -> None:
    """Best-effort sweep of Docker containers left by dead workers (#605).

    Errors are logged and swallowed — sandbox cleanup must not fail
    worker boot. The reclaim sweep will eventually re-queue any trial
    whose worker went away, and the next startup will retry the sweep.
    """
    import docker as _docker

    try:
        client = _docker.from_env()
    except Exception:
        logger.exception("orphan_sandbox_docker_client_failed")
        return
    try:
        cleanup_orphan_sandbox_containers(
            docker_client=client,
            state_lookup=lambda tid: lookup(tid)[0],
        )
    except Exception:
        logger.exception("orphan_sandbox_cleanup_failed")
    finally:
        with contextlib.suppress(Exception):
            client.close()


def _run_trial_cache_eviction(settings: WorkerSettings) -> None:
    """Best-effort prune of stale layered images at worker startup.

    TTL (trial_cache_ttl_hours) + free-space backstop
    (trial_cache_min_free_gb). Docker errors are logged and swallowed —
    eviction is opportunistic and must not fail worker boot."""
    import docker as _docker

    try:
        client = _docker.from_env()
        evict_stale_cache(client, settings)
    except Exception:
        logger.exception("trial_cache eviction failed at startup")


async def _ensure_runtime_buckets(object_store: ObjectStore) -> None:
    for bucket in ("trajectories", "artifacts"):
        try:
            await object_store.ensure_bucket(bucket)
        except Exception:
            logger.exception("runtime_bucket_ensure_failed bucket=%s", bucket)
            raise
        logger.info("runtime_bucket_ensured bucket=%s", bucket)


async def _claim_available_trials(
    *,
    pool: RunnerPool,
    settings: WorkerSettings,
    cp_client: HttpControlPlaneClient,
    gateway_client: HttpLLMGatewayClient,
    object_store: ObjectStore,
    worker_id: UUID,
    vllm_registry: WorkerVLLMRegistry,
    sandbox_allocator: SandboxNetworkAllocator | None = None,
    sandbox_singleton: SandboxSingletonManager | None = None,
) -> int:
    claimed = 0
    while pool.in_flight < settings.max_concurrent:
        try:
            trial_payload = await cp_client.claim(
                worker_id=worker_id,
                caps=_DEFAULT_CAPS,
            )
        except httpx.HTTPError as exc:
            logger.warning(
                "trial_claim_transient_error worker_id=%s err=%s",
                worker_id,
                exc,
            )
            break
        if trial_payload is None:
            break
        await _spawn_trial(
            pool=pool,
            settings=settings,
            cp_client=cp_client,
            gateway_client=gateway_client,
            object_store=object_store,
            worker_id=worker_id,
            payload=trial_payload,
            vllm_registry=vllm_registry,
            sandbox_allocator=sandbox_allocator,
            sandbox_singleton=sandbox_singleton,
        )
        claimed += 1
    return claimed


async def _prepare_family_state_mount_if_any(
    *,
    payload: dict[str, Any],
    settings: WorkerSettings,
    trial_id: UUID,
    object_store: ObjectStore,
) -> Any:
    """Download the family-run state tarball when the claim payload
    carries a ``family_state_uri`` (#672 PR-3).

    Returns None for classic trials so the runner picks up an empty
    ``family_state_volumes`` tuple. Raises are propagated so the trial
    is marked ``failed`` via the outer ``_mark_setup_failed`` path —
    a family-run trial that can't hydrate its shared state has no way
    to run correctly.
    """
    from loom.family_run.prestart import (
        FamilyStateMount,
        prepare_family_state_mount,
    )
    from loom.family_run.state_backends import S3ArtifactsStateBackend

    state_uri = payload.get("family_state_uri")
    if not state_uri:
        return None
    family_run_spec = payload.get("family_run_spec") or {}
    state_backend_ref = family_run_spec.get("state_backend") or {}
    backend_name = state_backend_ref.get("name") or "s3_artifacts"
    if backend_name != "s3_artifacts":
        # Only the built-in backend is wired in v1; when a caller ships
        # a custom entry-point the plugin will need to be resolvable
        # from the worker's python env. For now we fail loud instead
        # of silently mounting an empty dir.
        raise RuntimeError(
            f"unsupported family_run state_backend {backend_name!r}; "
            "only 's3_artifacts' is wired in the worker path",
        )
    # `artifacts` is the canonical bucket ensured at worker startup
    # (see _ensure_runtime_buckets) — matches loom_service's default.
    backend = S3ArtifactsStateBackend(
        store=object_store,
        bucket="artifacts",
    )
    mount_path = family_run_spec.get("mount_path") or "/root/.skills"
    timeout_sec = getattr(
        settings,
        "family_state_download_timeout_sec",
        120.0,
    )
    mount: FamilyStateMount = await prepare_family_state_mount(
        trial_id=str(trial_id),
        state_uri=state_uri,
        mount_path=mount_path,
        state_backend=backend,
        backend_params=state_backend_ref.get("params") or {},
        download_timeout_sec=timeout_sec,
    )
    return mount


async def _resolve_layered_trial_image(
    *,
    task_image: str,
    agent_name: str,
    settings: WorkerSettings,
    cp_client: HttpControlPlaneClient,
    worker_id: UUID,
) -> str:
    """Look up the agent adapter and, if it declares an install_script,
    return the cached layered image (or build it). Returns `task_image`
    unchanged for agents without an install_script (oracle, litellm,
    or adapters that haven't declared an install_script yet)."""
    # Built-in agents (oracle, litellm) aren't in the launcher registry
    # and don't need an install step. Skip.
    if agent_name in {"oracle", "litellm", "terminus-2"}:
        return task_image
    adapter_name = agent_name
    try:
        from loom_launcher import get_adapter
    except ImportError:
        return task_image
    adapter = get_adapter(adapter_name)
    if adapter is None:
        # Unknown name — let `_default_agent_factory` raise AgentError
        # at agent-spawn time with the existing "unknown agent.name"
        # message. Don't duplicate that here.
        return task_image
    return await resolve_trial_image(
        task_image=task_image,
        adapter=adapter,
        settings=settings,
        cp_client=cp_client,
        worker_id=worker_id,
    )


async def _spawn_trial(
    *,
    pool: RunnerPool,
    settings: WorkerSettings,
    cp_client: HttpControlPlaneClient,
    gateway_client: HttpLLMGatewayClient,
    object_store: ObjectStore,
    worker_id: UUID,
    payload: dict[str, Any],
    vllm_registry: WorkerVLLMRegistry,
    sandbox_allocator: SandboxNetworkAllocator | None = None,
    sandbox_singleton: SandboxSingletonManager | None = None,
) -> None:
    trial_id = UUID(str(payload["trial_id"]))
    team_id = UUID(str(payload["team_id"]))
    attempt_count = int(payload.get("attempt_count") or 1)

    async def _setup_run_and_cleanup() -> None:
        task_dir: Path | None = None
        trial_config: TrialConfig | None = None
        pre_start_heartbeat_task = asyncio.create_task(
            _run_pre_start_heartbeat(
                cp_client=cp_client,
                trial_id=trial_id,
                worker_id=worker_id,
                interval_sec=settings.pre_start_heartbeat_interval_sec,
            )
        )
        try:
            trial_config = TrialConfig.model_validate(payload.get("config") or {})
            bundle = await cp_client.get_task_bundle(str(payload["task_id"]))
            task_config = TaskConfig.model_validate(bundle["config"])
            task_checksum = str(bundle["checksum"])
            raw_provenance = bundle.get("source_provenance")
            provenance = raw_provenance if isinstance(raw_provenance, dict) else {}
            raw_policy = provenance.get("workspace_staging_policy")
            if task_config.task.id.startswith("terminal-bench-2@tb2.1-r6/"):
                workspace_staging_policy = _tb21_workspace_staging_policy_from_provenance(
                    raw_policy,
                )
            else:
                workspace_staging_policy = (
                    WorkspaceStagingPolicy.from_provenance(raw_policy)
                    if isinstance(raw_policy, dict)
                    else None
                )

            # Plan 13 Task 3: materialize the fixture content from
            # bundle["source"] when it's an s3:// URL (benchmark-imported
            # tasks). Hand-authored tasks with source=None or git+... still
            # get an empty tempdir; the operator runbook documents the
            # volume-mount / git-clone alternatives.
            task_dir = await _materialize_task_dir(
                bundle=bundle,
                object_store=object_store,
                trial_id=trial_id,
                fixtures_root=settings.fixtures_root,
                benchmark_cache=settings.benchmark_cache,
                timeout_sec=settings.task_materialize_timeout_sec,
            )
            validate_task_dir_compatibility(task_dir)
            # #275: serialize concurrent task-image builds so a burst of
            # trials cannot fan out unbounded apt-get / dpkg / build
            # containers on a shared host Docker daemon (e.g. OLDLAB).
            # `_daemon_build_slot` reuses the same slot pool that
            # `_resolve_layered_trial_image` uses; when the tag is
            # already cached, resolve_task_image never enters the slot.
            task_image = await resolve_task_image(
                task_config=task_config,
                task_dir=task_dir,
                task_checksum=task_checksum,
                docker_api_timeout_sec=settings.docker_api_timeout_sec,
                build_slot_provider=lambda: _daemon_build_slot(
                    cp_client,
                    settings,
                    worker_id,
                ),
            )
            # #317 Phase 1: if the chosen agent declares an
            # install_script, layer the agent install onto the task
            # image and run against the cached layered tag instead.
            # Build is content-addressed + cluster-shared via the
            # active_trial_cache_builds slot table.
            task_image = await _resolve_layered_trial_image(
                task_image=task_image,
                agent_name=trial_config.agent_name,
                settings=settings,
                cp_client=cp_client,
                worker_id=worker_id,
            )
        except Exception as exc:
            if task_dir is not None:
                shutil.rmtree(task_dir, ignore_errors=True)
            await _mark_setup_failed(
                cp_client=cp_client,
                trial_id=trial_id,
                worker_id=worker_id,
                detail=str(exc),
                diagnostic_detail=getattr(exc, "diagnostic_detail", None),
                setup_diagnostics_root=(settings.trajectory_cache_dir / "setup-diagnostics"),
                retry_policy=trial_config.retry if trial_config is not None else None,
                attempt_count=attempt_count,
            )
            return
        finally:
            pre_start_heartbeat_task.cancel()
            try:
                await pre_start_heartbeat_task
            except asyncio.CancelledError:
                pass

        async def _state_patch(
            state: str,
            fr: str | None,
            fm: str | None = None,
        ) -> bool:
            return await cp_client.patch_state(
                trial_id=trial_id,
                worker_id=worker_id,
                state=state,
                failure_reason=fr,
                failure_message=fm,
            )

        async def _output_projection(
            result_payload: dict[str, object],
            trajectory_index: dict[str, object],
        ) -> bool:
            return await cp_client.patch_output_projection(
                trial_id=trial_id,
                worker_id=worker_id,
                result=result_payload,
                trajectory_index=trajectory_index,
            )

        # #5 Slice 3b: per-trial CP event sink. The send callback is
        # a thin shim around `cp_client.append_events` that captures
        # trial_id + worker_id; the sink owns batching + flush
        # scheduling internally. MinIO is still authoritative in
        # this slice — sink errors are swallowed.
        async def _send_event_batch(
            events: list[dict[str, object]],
        ) -> bool:
            return await cp_client.append_events(
                trial_id=trial_id,
                worker_id=worker_id,
                events=events,
            )

        trial_cp_event_sink = CpEventSink(
            trial_id=trial_id,
            worker_id=worker_id,
            send_batch=_send_event_batch,
        )

        subprocess_gateway_url = getattr(settings, "subprocess_gateway_url", None)
        subprocess_gateway_url_str = (
            str(subprocess_gateway_url) if subprocess_gateway_url is not None else None
        )

        # #672 PR-3: when the CP claim payload carries a family_state_uri,
        # download the shared skills tarball into a per-trial staging
        # dir and hand the (host, container, mode) volume tuple to the
        # runner. Cleanup lives in the outer finally block below.
        family_state_mount = await _prepare_family_state_mount_if_any(
            payload=payload,
            settings=settings,
            trial_id=trial_id,
            object_store=object_store,
        )
        family_state_volumes: tuple[tuple[str, str, str], ...] = ()
        if family_state_mount is not None:
            family_state_volumes = (family_state_mount.as_volume_tuple(),)

        runner = LocalTrialRunner(
            trial_id=trial_id,
            team_id=team_id,
            task_config=task_config,
            task_checksum=task_checksum,
            task_dir=task_dir,
            trial_config=trial_config,
            driver_factory=lambda: DockerDriver(
                image=task_image,
                workspace=task_config.environment.workdir,
                docker_api_timeout_sec=settings.docker_api_timeout_sec,
            ),
            agent_factory=_default_agent_factory(
                team_id,
                trial_id,
                cp_client=cp_client,
                worker_gateway_url=str(settings.gateway_url),
                sandbox_gateway_url=subprocess_gateway_url_str,
                provider_connection_id=payload.get("provider_connection_id"),
            ),
            verifier_factory=_verifier_factory(task_config),
            object_store=object_store,
            gateway_client=gateway_client,
            local_trajectory_root=settings.trajectory_cache_dir,
            state_patch_callback=_state_patch,
            output_projection_callback=_output_projection,
            # A11.1: query CP for the trial's llm_calls rows at finalize,
            # project each into an LLMCallEvent. No-op for trials that
            # don't route through the Gateway (oracle, in-box runtimes).
            llm_calls_fetcher=cp_client.get_trial_llm_calls,
            # #5 Slice 3b: mirror typed trajectory events to the CP
            # `trial_events` table alongside the existing MinIO writer.
            # Slice 3c will flip the SSE reader from MinIO-poll to
            # Postgres-LISTEN, at which point sink failures start
            # mattering more; for now MinIO remains authoritative
            # and the sink swallows its own errors.
            cp_event_sink=trial_cp_event_sink,
            # PR-E: worker-spawned vLLM registry. Shared across trials
            # claimed by this worker process so the 1-3 min vLLM startup
            # is amortised across many same-model trials.
            vllm_registry=vllm_registry,
            # #188 / Phase B: per-trial sandbox isolation. Both None →
            # legacy direct-network behavior. Both populated → per-trial
            # bridge + singleton attach.
            sandbox_allocator=(sandbox_allocator if sandbox_singleton is not None else None),
            sandbox_singleton=sandbox_singleton,
            # Phase D: step-JWT minting + rotation. Only wired when
            # singleton started (isolation on); the mint_callback closes
            # over `cp_client` so the rotator hits POST /admin/step-tokens
            # with a stable step_id (per-step attribution is a future
            # improvement — for now sandbox isolation prioritizes session
            # continuity over per-step cost attribution).
            sandbox_mint_token=(
                _build_mint_callback(cp_client, team_id) if sandbox_singleton is not None else None
            ),
            sandbox_secrets_root=(
                Path(settings.sandbox_singleton_secrets_dir) / "trials"
                if sandbox_singleton is not None
                else None
            ),
            sandbox_step_jwt_ttl_sec=settings.sandbox_step_jwt_ttl_sec,
            sandbox_extra_hosts=_sandbox_extra_hosts_for_url(subprocess_gateway_url_str),
            family_state_volumes=family_state_volumes,
            workspace_staging_policy=workspace_staging_policy,
            sidecar_runtime_factory=lambda: DockerTaskSidecarRuntime(
                task_config=task_config,
                task_dir=task_dir,
                task_checksum=task_checksum,
                trial_id=trial_id,
                docker_api_timeout_sec=settings.docker_api_timeout_sec,
                setup_slot_provider=lambda: _daemon_build_slot(
                    cp_client,
                    settings,
                    worker_id,
                ),
            ),
        )

        # #360 + #378: wrap the runner with the cancellation watchdog so
        # (a) operator-initiated CP cancellations propagate to the
        # in-container subprocess-agent within one poll interval and
        # (b) trials that hang past a generous multiple of
        # agent.timeout_sec get force-cancelled instead of running
        # indefinitely.
        hard_deadline_sec: float | None = None
        multiplier = settings.trial_hard_deadline_multiplier
        if multiplier > 0:
            hard_deadline_sec = (
                task_config.agent.timeout_sec * multiplier + settings.trial_hard_deadline_grace_sec
            )
        try:
            await run_with_watchdog(
                runner.run(),
                trial_id=trial_id,
                cp_client=cp_client,
                poll_interval_sec=settings.trial_cancel_poll_interval_sec,
                hard_deadline_sec=hard_deadline_sec,
            )
        except asyncio.CancelledError:
            # Watchdog fired (cp_cancelled or hard_deadline). Trial.run's
            # own CancelledError handler already recorded the terminal
            # state; propagate so the outer task cleanup fires.
            raise
        except Exception as exc:
            logger.exception(
                "trial_runner_failed trial_id=%s worker_id=%s",
                trial_id,
                worker_id,
            )
            await _mark_setup_failed(
                cp_client=cp_client,
                trial_id=trial_id,
                worker_id=worker_id,
                detail=str(exc),
                diagnostic_detail=getattr(exc, "diagnostic_detail", None),
                setup_diagnostics_root=(settings.trajectory_cache_dir / "setup-diagnostics"),
                retry_policy=trial_config.retry,
                attempt_count=attempt_count,
            )
        finally:
            shutil.rmtree(task_dir, ignore_errors=True)
            # #672 PR-3: release the family-state staging dir. The
            # runner uses the bind-mount for the trial's lifetime;
            # after the sandbox stops, the tarball has been re-uploaded
            # by any subsequent adapter.evolve call, so the local copy
            # can go.
            if family_state_mount is not None:
                family_state_mount.cleanup()

    await pool.spawn(_setup_run_and_cleanup())


async def _run_pre_start_heartbeat(
    *,
    cp_client: HttpControlPlaneClient,
    trial_id: UUID,
    worker_id: UUID,
    interval_sec: float,
) -> None:
    interval = max(0.001, float(interval_sec))
    while True:
        try:
            accepted = await cp_client.pre_start_heartbeat(
                trial_id=trial_id,
                worker_id=worker_id,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning(
                "pre_start_heartbeat_error trial_id=%s worker_id=%s err=%s",
                trial_id,
                worker_id,
                exc,
            )
        else:
            if not accepted:
                logger.warning(
                    "pre_start_heartbeat_fenced trial_id=%s worker_id=%s",
                    trial_id,
                    worker_id,
                )
                return
        await asyncio.sleep(interval)


def _verifier_factory(task_config: TaskConfig) -> Callable[[], Verifier]:
    name = task_config.verifier.name
    ctor = _VERIFIER_CTORS.get(name)
    if ctor is None:
        raise ValueError(f"unknown verifier: {name!r}")
    args = dict(task_config.verifier.args)
    return lambda: ctor(**args)


def _build_mint_callback(
    cp_client: StepTokenClient,
    team_id: UUID,
) -> Callable[[UUID], Awaitable[str]]:
    """Closure the rotator calls on each tick. Stable step_id
    `"sandbox-rotated"` because Phase D's primary goal is sandbox
    isolation; per-step cost-attribution refinements are a follow-up."""

    async def _mint(trial_id: UUID) -> str:
        return await cp_client.mint_step_token(
            team_id=team_id,
            trial_id=trial_id,
            step_id="sandbox-rotated",
            ttl_sec=600,
        )

    return _mint


async def _mark_setup_failed(
    *,
    cp_client: HttpControlPlaneClient,
    trial_id: UUID,
    worker_id: UUID,
    detail: str,
    diagnostic_detail: str | None = None,
    setup_diagnostics_root: Path | None = None,
    retry_policy: RetryPolicy | None = None,
    attempt_count: int | None = None,
) -> None:
    safe_diagnostic_detail = redact_text(diagnostic_detail or detail).strip()
    diagnostic_path = _write_setup_failure_diagnostic(
        safe_diagnostic_detail=safe_diagnostic_detail,
        trial_id=trial_id,
        setup_diagnostics_root=setup_diagnostics_root,
    )
    safe_detail = _format_setup_failure_detail(
        detail,
        diagnostic_path=diagnostic_path,
    )
    classified_text = classify_failure_message(safe_detail)
    if classified_text is not None:
        failure_reason, classified_message = classified_text
        if classified_message:
            safe_detail = classified_message
    else:
        failure_reason = _classify_setup_failure(safe_detail)
    logger.warning(
        "trial_setup_failed trial_id=%s worker_id=%s detail=%s",
        trial_id,
        worker_id,
        safe_detail,
    )
    retry_after_sec = _setup_retry_after_seconds(
        failure_reason=failure_reason,
        retry_policy=retry_policy,
        attempt_count=attempt_count,
    )
    if retry_after_sec is not None:
        try:
            ok = await cp_client.requeue_trial_retry(
                trial_id=trial_id,
                worker_id=worker_id,
                failure_reason=failure_reason.value,
                failure_message=safe_detail,
                retry_after_sec=retry_after_sec,
            )
        except Exception:
            logger.exception(
                "trial_setup_retry_requeue_error trial_id=%s",
                trial_id,
            )
            ok = False
        if ok:
            logger.info(
                "trial_setup_retry_requeued trial_id=%s worker_id=%s "
                "failure_reason=%s attempt_count=%s retry_after_sec=%.3f",
                trial_id,
                worker_id,
                failure_reason.value,
                attempt_count,
                retry_after_sec,
            )
            return
        logger.warning(
            "trial_setup_retry_requeue_unaccepted trial_id=%s worker_id=%s",
            trial_id,
            worker_id,
        )
    try:
        ok = await cp_client.patch_state(
            trial_id=trial_id,
            worker_id=worker_id,
            state="failed",
            failure_reason=failure_reason.value,
            failure_message=safe_detail,
        )
    except Exception:
        logger.exception(
            "trial_setup_failed_state_patch_error trial_id=%s",
            trial_id,
        )
        return
    if not ok:
        logger.warning(
            "trial_setup_failed_state_patch_fenced trial_id=%s worker_id=%s",
            trial_id,
            worker_id,
        )


def _write_setup_failure_diagnostic(
    *,
    safe_diagnostic_detail: str,
    trial_id: UUID,
    setup_diagnostics_root: Path | None,
) -> Path | None:
    if setup_diagnostics_root is None:
        return None
    if len(safe_diagnostic_detail) <= _SETUP_FAILURE_MESSAGE_LIMIT:
        return None

    diagnostic_path = setup_diagnostics_root / f"{trial_id}.log"
    try:
        diagnostic_path.parent.mkdir(parents=True, exist_ok=True)
        diagnostic_path.write_text(safe_diagnostic_detail + "\n", encoding="utf-8")
    except OSError:
        logger.warning(
            "trial_setup_failed_diagnostic_write_failed trial_id=%s path=%s",
            trial_id,
            diagnostic_path,
            exc_info=True,
        )
        return None
    return diagnostic_path


def _format_setup_failure_detail(
    detail: str,
    *,
    diagnostic_path: Path | None = None,
) -> str:
    safe_detail = redact_text(detail).strip()
    diagnostic_path_line = (
        f"\nfull_setup_diagnostic_path: {diagnostic_path}" if diagnostic_path is not None else ""
    )
    if len(safe_detail) + len(diagnostic_path_line) <= _SETUP_FAILURE_MESSAGE_LIMIT:
        if diagnostic_path_line:
            return f"{safe_detail}{diagnostic_path_line}"
        return safe_detail

    max_head_budget = min(
        _SETUP_FAILURE_HEAD_CHARS,
        _SETUP_FAILURE_MESSAGE_LIMIT - len(_SETUP_FAILURE_TRUNCATION_MARKER) - 1,
    )
    head = _setup_failure_diagnostic_head(safe_detail, max_head_budget)
    if diagnostic_path_line:
        head = f"{head}{diagnostic_path_line}"
        if len(head) + len(_SETUP_FAILURE_TRUNCATION_MARKER) >= (_SETUP_FAILURE_MESSAGE_LIMIT):
            head_budget = _SETUP_FAILURE_MESSAGE_LIMIT - len(_SETUP_FAILURE_TRUNCATION_MARKER) - 1
            head = head[:head_budget].rstrip()
    tail_budget = _SETUP_FAILURE_MESSAGE_LIMIT - len(head) - len(_SETUP_FAILURE_TRUNCATION_MARKER)
    tail = safe_detail[-tail_budget:].lstrip()
    first_newline = tail.find("\n")
    if 0 < first_newline < len(tail) - 1:
        tail = tail[first_newline + 1 :]
    compact = f"{head}{_SETUP_FAILURE_TRUNCATION_MARKER}{tail.lstrip()}"
    return compact[:_SETUP_FAILURE_MESSAGE_LIMIT].strip()


def _setup_failure_diagnostic_head(detail: str, max_chars: int) -> str:
    build_log_index = detail.lower().find("\nbuild log")
    if build_log_index >= 0:
        build_log_line_end = detail.find("\n", build_log_index + 1)
        if 0 < build_log_line_end <= max_chars:
            return detail[:build_log_line_end].rstrip()
    return detail[:max_chars].rstrip()


def _classify_setup_failure(detail: str) -> FailureReason:
    if "SETUP_ADMISSION_BLOCKED" in detail:
        return FailureReason.NODE_SETUP_HEALTH
    if "TASK_COMPAT_" in detail:
        return FailureReason.TASK_COMPATIBILITY
    if "building Docker image" in detail and " from " in detail and " exceeded " in detail:
        return FailureReason.TASK_IMAGE_BUILD_TIMEOUT
    lowered = detail.lower()
    if "failed to build layered image" in lowered and (
        "temporary failure resolving" in lowered
        or "could not resolve host" in lowered
        or "name or service not known" in lowered
    ):
        return FailureReason.TASK_COMPATIBILITY
    return FailureReason.INTERNAL_ERROR


def _setup_retry_reason_for_failure(reason: FailureReason) -> RetryReason | None:
    if reason == FailureReason.PROVIDER_TRANSPORT_DISCONNECT:
        return RetryReason.PROVIDER_TRANSPORT_DISCONNECT
    if reason == FailureReason.GATEWAY_ERROR:
        return RetryReason.GATEWAY_ERROR
    return None


def _setup_retry_after_seconds(
    *,
    failure_reason: FailureReason,
    retry_policy: RetryPolicy | None,
    attempt_count: int | None,
) -> float | None:
    if retry_policy is None or attempt_count is None:
        return None
    retry_reason = _setup_retry_reason_for_failure(failure_reason)
    if retry_reason is None:
        return None
    if retry_reason not in retry_policy.retry_on:
        return None
    if attempt_count >= retry_policy.max_attempts:
        return None
    now = datetime.now(UTC)
    retry_at = next_attempt_at(
        attempt_count=attempt_count,
        backoff=retry_policy.backoff,
        now=now,
    )
    return max(0.0, (retry_at - now).total_seconds())


async def _materialize_task_dir(
    *,
    bundle: dict[str, Any],
    object_store: ObjectStore,
    trial_id: UUID,
    fixtures_root: Path | None = None,
    benchmark_cache: Path | None = None,
    timeout_sec: float | None = None,
) -> Path:
    """Create a fresh tempdir and populate it from `bundle["source"]`.

    Dispatches to the registered `Materializer` whose `matches(source)`
    returns True. Adding a new URL scheme is a new Materializer impl in
    `loom_worker.materializers`; the dispatcher stays as-is.
    """
    task_dir = Path(tempfile.mkdtemp(prefix=f"loom-trial-{trial_id}-"))
    materializers = build_default_materializers(
        object_store=object_store,
        fixtures_root=fixtures_root,
        benchmark_cache=benchmark_cache,
    )
    source = bundle.get("source")
    if timeout_sec is not None and timeout_sec <= 0:
        raise ValueError("task_materialize_timeout_sec must be > 0")
    try:
        if timeout_sec is None:
            return await dispatch_materialize(
                source=source,
                task_dir=task_dir,
                trial_id=trial_id,
                materializers=materializers,
            )
        async with asyncio.timeout(timeout_sec):
            return await dispatch_materialize(
                source=source,
                task_dir=task_dir,
                trial_id=trial_id,
                materializers=materializers,
            )
    except TimeoutError as exc:
        raise TimeoutError(
            f"task materialization timed out after {timeout_sec:g}s "
            f"(source_scheme={_source_scheme_for_diagnostic(source)})",
        ) from exc


def _source_scheme_for_diagnostic(source: object) -> str:
    if not isinstance(source, str) or not source:
        return "none"
    scheme, sep, _rest = source.partition(":")
    if not sep or not scheme:
        return "unknown"
    return scheme


def _default_agent_factory(
    team_id: UUID,
    trial_id: UUID,
    *,
    cp_client: StepTokenClient,
    worker_gateway_url: str,
    sandbox_gateway_url: str | None = None,
    provider_connection_id: str | None = None,
) -> AgentFactory:
    """Build the agent factory used by LocalTrialRunner. Routes by
    `agent_name` (read from `task_config.agent.name`):

    - "oracle"      → OracleAgent (solution/solve.sh baseline)
    - "litellm"     → LiteLLMAgent (v0.7 tool-loop runtime)
    - "terminus-2"  → LoomTerminus2Runtime (Harbor-embedded in-box runtime;
                      uses ``worker_gateway_url`` only)
    - anything else → SubprocessAgent wrapping the loom-launcher adapter
      of that name (uses ``sandbox_gateway_url`` when set). Raises
      AgentError if the name is unknown (i.e. no v0.7 runtime and no
      registered adapter).
    """

    def make(
        task_dir: Path,
        gateway: LLMGatewayClient,
        model: ModelSpec | None,
        agent_name: str,
    ) -> AgentRuntime:
        agent: AgentRuntime
        if agent_name == "oracle":
            agent = OracleAgent(task_dir=task_dir, trial_id=trial_id)
        elif agent_name == "litellm":
            if model is None:
                raise AgentError(
                    "litellm agent requires task.agent.model to be set",
                )
            # mypy: LiteLLMAgent.model is ModelSpec while the AgentRuntime
            # protocol declares ModelSpec | None; covariant on a mutable
            # attribute trips invariance. Both are structurally compatible.
            agent = LiteLLMAgent(  # type: ignore[assignment]
                model=model,
                gateway=gateway,
                team_id=str(team_id),
                trial_id=trial_id,
                provider_connection_id=provider_connection_id,
            )
        elif agent_name == "terminus-2":
            if model is None:
                raise AgentError(
                    "terminus-2 agent requires task.agent.model to be set",
                )
            agent = LoomTerminus2Runtime(  # type: ignore[assignment]
                model=model,
                team_id=str(team_id),
                trial_id=trial_id,
                cp_client=cp_client,
                gateway_url=worker_gateway_url,
                provider_connection_id=provider_connection_id,
            )
        else:
            # Try the loom-launcher registry. Imports are lazy so the
            # launcher dep stays optional for sites that only run
            # oracle/litellm.
            from loom_launcher import get_adapter

            from loom.agent.subprocess import SubprocessAgent

            adapter = get_adapter(agent_name)
            if adapter is None:
                # Surface as AgentError so Trial.run() classifies it as
                # AGENT_ERROR and the trial fails cleanly instead of
                # crashing the worker.
                raise AgentError(
                    f"unknown agent.name {agent_name!r} — not a v0.7 "
                    f"runtime and not registered in loom-launcher",
                )
            if model is None:
                raise AgentError(
                    f"{agent_name} requires task.agent.model to be set",
                )
            # Same Protocol-variance situation as LiteLLMAgent above:
            # SubprocessAgent.model is ModelSpec while AgentRuntime.model
            # is ModelSpec | None. Structurally compatible.
            agent = SubprocessAgent(  # type: ignore[assignment]
                adapter=adapter,
                model=model,
                cp_client=cp_client,
                gateway_url=worker_gateway_url,
                agent_gateway_url=sandbox_gateway_url,
                team_id=team_id,
                trial_id=trial_id,
            )
        return agent

    return make


def _sandbox_extra_hosts_for_url(url: str | None) -> tuple[tuple[str, str], ...]:
    if not url:
        return ()
    if urlparse(url).hostname == "host.docker.internal":
        return _DOCKER_HOST_GATEWAY_EXTRA_HOSTS
    return ()
