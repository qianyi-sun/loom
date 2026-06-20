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
import logging
import shutil
import tempfile
from collections.abc import Awaitable, Callable
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any
from uuid import UUID

import httpx
from pydantic import ValidationError

from loom.agent.base import AgentRuntime
from loom.agent.gateway_client import LLMGatewayClient
from loom.agent.http_gateway_client import HttpLLMGatewayClient
from loom.agent.litellm import LiteLLMAgent
from loom.agent.oracle import OracleAgent
from loom.driver.docker import DockerDriver
from loom.errors import AgentError
from loom.models.result import FailureReason
from loom.models.task import TaskConfig
from loom.models.trial import TrialConfig
from loom.models.types import ModelSpec
from loom.trajectory.storage import MinioObjectStore, ObjectStore
from loom.verifier.base import Verifier
from loom.verifier.pytest_verifier import PytestVerifier
from loom.verifier.script_verifier import ScriptVerifier
from loom_worker.config import WorkerSettings
from loom_worker.control_plane_client import HttpControlPlaneClient
from loom_worker.heartbeat import HeartbeatThread
from loom_worker.materializers import (
    build_default_materializers,
    dispatch_materialize,
)
from loom_worker.orphan_cleanup import cleanup_orphan_trajectories
from loom_worker.runner_pool import RunnerPool
from loom_worker.sandbox_network import SandboxNetworkAllocator
from loom_worker.sandbox_singleton import (
    SandboxSingletonManager,
    SingletonStartupError,
)
from loom_worker.signal_handler import ShutdownState, install_signal_handlers
from loom_worker.task_image import TaskImageBuildError, resolve_task_image
from loom_worker.trial_runner import AgentFactory, LocalTrialRunner
from loom_worker.vllm_registry import WorkerVLLMRegistry

logger = logging.getLogger(__name__)

_VERIFIER_CTORS: dict[str, Callable[..., Verifier]] = {
    "pytest": PytestVerifier,
    "script": ScriptVerifier,
}


_DEFAULT_CAPS = [
    {
        "os": "linux",
        "gpu_vendor": "none",
        "network_policies": ["public", "no-network", "allowlist"],
        "dynamic_network_policy": True,
        "mounted_fs": True,
        "resource_modes": ["auto", "limit", "guarantee"],
    }
]


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


async def run_worker(settings: WorkerSettings) -> None:
    state = ShutdownState()
    install_signal_handlers(state)

    settings.trajectory_cache_dir.mkdir(parents=True, exist_ok=True)
    _configure_blocking_io_executor(settings)

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

        info = await cp_client.register(
            hostname="worker",
            version="0.0.1",
            capabilities=_DEFAULT_CAPS,
        )
        worker_id = UUID(info["worker_id"])
        logger.info("worker_registered worker_id=%s", worker_id)

        _run_orphan_cleanup(settings, worker_id)

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
            object_store = MinioObjectStore(
                endpoint_url=settings.minio_endpoint,
                access_key=settings.minio_access_key.get_secret_value(),
                secret_key=settings.minio_secret_key.get_secret_value(),
                region=settings.minio_region,
            )
            await _ensure_runtime_buckets(object_store)
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


def _run_orphan_cleanup(settings: WorkerSettings, worker_id: UUID) -> None:
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
            # currently doesn't expose `worker_id`. Treat ownership as
            # unknown → cleanup deletes any non-terminal record we hold.
            return body["state"], None

    cleanup_orphan_trajectories(
        cache_dir=settings.trajectory_cache_dir,
        owned_worker_id=worker_id,
        state_and_owner_lookup=_lookup,
    )


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
        trial_payload = await cp_client.claim(
            worker_id=worker_id,
            caps=_DEFAULT_CAPS,
        )
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

    async def _setup_run_and_cleanup() -> None:
        task_dir: Path | None = None
        try:
            bundle = await cp_client.get_task_bundle(str(payload["task_id"]))
            task_config = TaskConfig.model_validate(bundle["config"])
            task_checksum = str(bundle["checksum"])
            trial_config = TrialConfig.model_validate(payload.get("config") or {})

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
            )
            task_image = await resolve_task_image(
                task_config=task_config,
                task_dir=task_dir,
                task_checksum=task_checksum,
            )
        except (
            httpx.HTTPError,
            ValidationError,
            OSError,
            ValueError,
            TaskImageBuildError,
        ) as exc:
            if task_dir is not None:
                shutil.rmtree(task_dir, ignore_errors=True)
            await _mark_setup_failed(
                cp_client=cp_client,
                trial_id=trial_id,
                worker_id=worker_id,
                detail=str(exc),
            )
            return

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
            ),
            agent_factory=_default_agent_factory(
                team_id,
                trial_id,
                cp_client=cp_client,
                gateway_url=str(settings.gateway_url),
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
        )

        try:
            await runner.run()
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
            )
        finally:
            shutil.rmtree(task_dir, ignore_errors=True)

    await pool.spawn(_setup_run_and_cleanup())


def _verifier_factory(task_config: TaskConfig) -> Callable[[], Verifier]:
    name = task_config.verifier.name
    ctor = _VERIFIER_CTORS.get(name)
    if ctor is None:
        raise ValueError(f"unknown verifier: {name!r}")
    args = dict(task_config.verifier.args)
    return lambda: ctor(**args)


def _build_mint_callback(
    cp_client: HttpControlPlaneClient,
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
) -> None:
    logger.warning(
        "trial_setup_failed trial_id=%s worker_id=%s detail=%s",
        trial_id,
        worker_id,
        detail,
    )
    try:
        ok = await cp_client.patch_state(
            trial_id=trial_id,
            worker_id=worker_id,
            state="failed",
            failure_reason=FailureReason.INTERNAL_ERROR.value,
            failure_message=detail,
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


async def _materialize_task_dir(
    *,
    bundle: dict[str, Any],
    object_store: ObjectStore,
    trial_id: UUID,
    fixtures_root: Path | None = None,
    benchmark_cache: Path | None = None,
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
    return await dispatch_materialize(
        source=bundle.get("source"),
        task_dir=task_dir,
        trial_id=trial_id,
        materializers=materializers,
    )


def _default_agent_factory(
    team_id: UUID,
    trial_id: UUID,
    *,
    cp_client: HttpControlPlaneClient,
    gateway_url: str,
    provider_connection_id: str | None = None,
) -> AgentFactory:
    """Build the agent factory used by LocalTrialRunner. Routes by
    `agent_name` (read from `task_config.agent.name`):

    - "oracle"            → OracleAgent (solution/solve.sh baseline)
    - "litellm"           → LiteLLMAgent (v0.7 tool-loop runtime)
    - "claude-code-inbox" → v0.7 ClaudeCodeAgent (in-box runtime, kept
      for backwards compat under the renamed name; the new subprocess
      "claude-code" adapter lives in loom-launcher)
    - anything else       → SubprocessAgent wrapping the loom-launcher
      adapter of that name. Raises ValueError if the name is unknown
      (i.e. no v0.7 runtime and no registered adapter).
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
                gateway_url=gateway_url,
                team_id=team_id,
                trial_id=trial_id,
            )
        return agent

    return make
