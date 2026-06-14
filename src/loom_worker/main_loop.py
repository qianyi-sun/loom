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
from pathlib import Path
from typing import Any
from uuid import UUID

import httpx

from loom.agent.base import AgentRuntime
from loom.agent.gateway_client import LLMGatewayClient
from loom.agent.http_gateway_client import HttpLLMGatewayClient
from loom.agent.litellm import LiteLLMAgent
from loom.agent.oracle import OracleAgent
from loom.driver.docker import DockerDriver
from loom.errors import AgentError
from loom.models.task import TaskConfig
from loom.models.trial import TrialConfig
from loom.models.types import ModelSpec
from loom.trajectory.storage import MinioObjectStore, ObjectStore
from loom.verifier.pytest_verifier import PytestVerifier
from loom_worker.config import WorkerSettings
from loom_worker.control_plane_client import HttpControlPlaneClient
from loom_worker.heartbeat import HeartbeatThread
from loom_worker.orphan_cleanup import cleanup_orphan_trajectories
from loom_worker.runner_pool import RunnerPool
from loom_worker.signal_handler import ShutdownState, install_signal_handlers
from loom_worker.trial_runner import AgentFactory, LocalTrialRunner
from loom_worker.vllm_registry import WorkerVLLMRegistry

logger = logging.getLogger(__name__)


_DEFAULT_CAPS = [{
    "os": "linux",
    "gpu_vendor": "none",
    "network_policies": ["public", "no-network", "allowlist"],
    "dynamic_network_policy": True,
    "mounted_fs": True,
    "resource_modes": ["auto", "limit", "guarantee"],
}]


async def run_worker(settings: WorkerSettings) -> None:
    state = ShutdownState()
    install_signal_handlers(state)

    settings.trajectory_cache_dir.mkdir(parents=True, exist_ok=True)

    async with httpx.AsyncClient(
        base_url=str(settings.control_plane_url), timeout=30.0,
    ) as cp_http, httpx.AsyncClient(
        base_url=str(settings.gateway_url), timeout=120.0,
    ) as gw_http:
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
            hostname="worker", version="0.0.1", capabilities=_DEFAULT_CAPS,
        )
        worker_id = UUID(info["worker_id"])
        logger.info("worker_registered worker_id=%s", worker_id)

        _run_orphan_cleanup(settings, worker_id)

        sync_http = httpx.Client(
            base_url=str(settings.control_plane_url), timeout=5.0,
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
            # PR-E: per-worker vLLM registry. Opt-in via settings; the
            # `enabled=False` path still constructs the object so the
            # trial runner gets a deterministic AgentError instead of
            # a None-dereference when a trial requests local-vllm.
            vllm_registry = WorkerVLLMRegistry(
                enabled=settings.enable_worker_vllm,
                default_gpu_memory_utilization=(
                    settings.vllm_gpu_memory_utilization
                ),
                default_tensor_parallel_size=(
                    settings.vllm_tensor_parallel_size
                ),
            )

            while not state.shutting_down:
                if pool.in_flight < settings.max_concurrent:
                    trial_payload = await cp_client.claim(
                        worker_id=worker_id, caps=_DEFAULT_CAPS,
                    )
                    if trial_payload is not None:
                        await _spawn_trial(
                            pool=pool, settings=settings,
                            cp_client=cp_client,
                            gateway_client=gateway_client,
                            object_store=object_store,
                            worker_id=worker_id,
                            payload=trial_payload,
                            vllm_registry=vllm_registry,
                        )
                await asyncio.sleep(settings.claim_poll_interval_sec)

            logger.info(
                "drain_started timeout=%ss in_flight=%d",
                settings.drain_timeout_sec, pool.in_flight,
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
            base_url=str(settings.control_plane_url), timeout=10.0,
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
) -> None:
    trial_id = UUID(str(payload["trial_id"]))
    team_id = UUID(str(payload["team_id"]))
    bundle = await cp_client.get_task_bundle(str(payload["task_id"]))
    task_config = TaskConfig.model_validate(bundle["config"])
    task_checksum = str(bundle["checksum"])
    trial_config = TrialConfig.model_validate(payload.get("config") or {})

    # Plan 13 Task 3: materialize the fixture content from bundle["source"]
    # when it's an s3:// URL (benchmark-imported tasks). Hand-authored
    # tasks with source=None or git+... still get an empty tempdir; the
    # operator runbook documents the volume-mount / git-clone alternatives.
    task_dir = await _materialize_task_dir(
        bundle=bundle, object_store=object_store, trial_id=trial_id,
        fixtures_root=settings.fixtures_root,
        benchmark_cache=settings.benchmark_cache,
    )

    async def _state_patch(state: str, fr: str | None) -> bool:
        return await cp_client.patch_state(
            trial_id=trial_id, worker_id=worker_id,
            state=state, failure_reason=fr,
        )

    runner = LocalTrialRunner(
        trial_id=trial_id, team_id=team_id,
        task_config=task_config, task_checksum=task_checksum,
        task_dir=task_dir,
        trial_config=trial_config,
        driver_factory=lambda: DockerDriver(
            image=task_config.environment.docker_image or "alpine",
        ),
        agent_factory=_default_agent_factory(
            team_id, trial_id,
            cp_client=cp_client,
            gateway_url=str(settings.gateway_url),
        ),
        verifier_factory=lambda: PytestVerifier(),
        object_store=object_store,
        gateway_client=gateway_client,
        local_trajectory_root=settings.trajectory_cache_dir,
        state_patch_callback=_state_patch,
        # A11.1: query CP for the trial's llm_calls rows at finalize,
        # project each into an LLMCallEvent. No-op for trials that
        # don't route through the Gateway (oracle, in-box runtimes).
        llm_calls_fetcher=cp_client.get_trial_llm_calls,
        # PR-E: worker-spawned vLLM registry. Shared across trials
        # claimed by this worker process so the 1-3 min vLLM startup
        # is amortised across many same-model trials.
        vllm_registry=vllm_registry,
    )

    async def _run_and_cleanup() -> None:
        # Bug 4 fix: drop the per-trial mkdtemp once the trial body is
        # done. Without this, every claim leaks a directory under /tmp
        # until the host or PV runs out of inodes. Cleanup runs in a
        # try/finally so it fires on cancellation + agent error too.
        try:
            await runner.run()
        finally:
            shutil.rmtree(task_dir, ignore_errors=True)

    await pool.spawn(_run_and_cleanup())


def _parse_hf_source(source: str) -> tuple[str, str, str]:
    """Parse `hf://{org}/{repo}@{rev}/{path}` into
    (repo_id, revision, path). Revision defaults to "main" if absent.

    Raises ValueError on malformed input — the caller logs + skips."""
    if not source.startswith("hf://"):
        raise ValueError(f"not an hf:// URL: {source!r}")
    without_scheme = source[len("hf://"):]
    # `org/repo@rev/path` — split on first / to separate org, then
    # split on @ to peel the revision off the repo segment, then the
    # remainder of the string is the path. HF org + repo names don't
    # contain `/` or `@`, so this is unambiguous.
    if "/" not in without_scheme:
        raise ValueError(f"hf:// URL missing repo: {source!r}")
    org, rest = without_scheme.split("/", 1)
    # rest is e.g. "loom-benchmark-aime@main/HumanEval_0/"
    if "@" in rest:
        repo, after_at = rest.split("@", 1)
        revision, _, path = after_at.partition("/")
    else:
        repo, _, path = rest.partition("/")
        revision = "main"
    if not org or not repo:
        raise ValueError(f"hf:// URL missing org or repo: {source!r}")
    return f"{org}/{repo}", revision, path


async def _materialize_hf_dir(
    *, source: str, task_dir: Path, cache_dir: Path | None,
    trial_id: UUID,
) -> Path:
    """Pull a `hf://{repo}@{rev}/{path}` bundle into `task_dir`.

    Uses `huggingface_hub.snapshot_download` with `allow_patterns` so
    only the one bundle's files transfer, not the entire repo. The
    HF library handles its own cache + atomic writes; if `cache_dir`
    is set, snapshots live there (persisted across worker restarts)
    instead of the default `~/.cache/huggingface/`.

    snapshot_download writes the bundle at `{snapshot_root}/{path}/...`
    so the worker shifts files up one level into `task_dir` to match
    the layout `Trial.run` expects (task.toml at the root).
    """
    from huggingface_hub import snapshot_download

    repo_id, revision, path = _parse_hf_source(source)
    if not path:
        raise ValueError(
            f"hf:// URL missing bundle path: {source!r}",
        )
    # snapshot_download patterns are gitignore-style; `path*` matches
    # the bundle dir + every file under it.
    pattern_root = path.rstrip("/")
    snapshot = await asyncio.to_thread(
        snapshot_download,
        repo_id=repo_id,
        revision=revision,
        repo_type="dataset",
        allow_patterns=[f"{pattern_root}/*"],
        cache_dir=str(cache_dir) if cache_dir is not None else None,
    )
    bundle_dir = Path(snapshot) / pattern_root
    if not bundle_dir.is_dir():
        raise FileNotFoundError(
            f"hf:// bundle dir not found after snapshot: {bundle_dir}",
        )
    # Mirror the s3:// dispatcher's layout: bundle contents at the
    # root of task_dir, not nested under the instance-id segment.
    for src_path in bundle_dir.iterdir():
        dst = task_dir / src_path.name
        if src_path.is_dir():
            shutil.copytree(src_path, dst, dirs_exist_ok=True)
        else:
            shutil.copy2(src_path, dst)
    logger.info(
        "materialized_task_dir trial=%s hf=%s repo=%s path=%s",
        trial_id, source, repo_id, path,
    )
    return task_dir


async def _materialize_task_dir(
    *,
    bundle: dict[str, Any],
    object_store: ObjectStore,
    trial_id: UUID,
    fixtures_root: Path | None = None,
    benchmark_cache: Path | None = None,
) -> Path:
    """Plan 13 Task 3: create a fresh tempdir and populate it from
    `bundle["source"]`.

    - `s3://bucket/prefix/` — pull every object via download_prefix.
      Benchmark-imported tasks follow this shape.
    - `fixture://<task_id>` — when `fixtures_root` is set, copy from
      `<fixtures_root>/<task_id>/` (dev compose mounts the repo's
      tests/fixtures/tasks into the worker so the canary hello-world
      trial runs end-to-end). When `fixtures_root` is None, leave the
      dir empty — production rejects fixture:// in favor of s3://.
    - None / `git+...` — leave the dir empty. The operator runbook
      documents the volume-mount alternative.
    - `s3://bucket` or `s3://bucket/` with no prefix is REJECTED with
      an empty dir + warning — without a prefix, download_prefix would
      drain the entire bucket into one trial's workspace.

    If download_prefix raises, the tempdir is removed before the
    exception propagates so failed claims don't leak `/tmp` inodes.
    """
    task_dir = Path(tempfile.mkdtemp(prefix=f"loom-trial-{trial_id}-"))
    source = bundle.get("source")
    if isinstance(source, str) and source.startswith("hf://"):
        try:
            return await _materialize_hf_dir(
                source=source, task_dir=task_dir,
                cache_dir=benchmark_cache, trial_id=trial_id,
            )
        except BaseException:
            shutil.rmtree(task_dir, ignore_errors=True)
            raise
    if isinstance(source, str) and source.startswith("fixture://"):
        task_id = source[len("fixture://"):]
        if fixtures_root is None:
            logger.warning(
                "materialize_task_dir trial=%s fixture:// source %r but "
                "fixtures_root unset; leaving dir empty",
                trial_id, source,
            )
            return task_dir
        src = fixtures_root / task_id
        if not src.is_dir():
            logger.warning(
                "materialize_task_dir trial=%s fixture %r not found at %s; "
                "leaving dir empty",
                trial_id, task_id, src,
            )
            return task_dir
        try:
            # copytree into the already-created tempdir. Using
            # dirs_exist_ok=True because mkdtemp() pre-created task_dir.
            shutil.copytree(src, task_dir, dirs_exist_ok=True)
        except BaseException:
            shutil.rmtree(task_dir, ignore_errors=True)
            raise
        logger.info(
            "materialized_task_dir trial=%s fixture=%s from=%s",
            trial_id, task_id, src,
        )
        return task_dir
    if isinstance(source, str) and source.startswith("s3://"):
        without_scheme = source[len("s3://"):]
        if "/" not in without_scheme:
            logger.warning(
                "bundle source %s has no key prefix; skipping materialize",
                source,
            )
            return task_dir
        bucket, prefix = without_scheme.split("/", 1)
        if not prefix:
            logger.warning(
                "bundle source %s has empty key prefix; refusing to "
                "drain entire bucket — leaving task_dir empty",
                source,
            )
            return task_dir
        try:
            count = await object_store.download_prefix(
                bucket=bucket, prefix=prefix, out_dir=task_dir,
            )
        except BaseException:
            shutil.rmtree(task_dir, ignore_errors=True)
            raise
        logger.info(
            "materialized_task_dir trial=%s objects=%d source=%s",
            trial_id, count, source,
        )
    else:
        logger.info(
            "materialize_task_dir trial=%s left dir empty (source=%r)",
            trial_id, source,
        )
    return task_dir


def _default_agent_factory(
    team_id: UUID,
    trial_id: UUID,
    *,
    cp_client: HttpControlPlaneClient,
    gateway_url: str,
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
                model=model, gateway=gateway,
                team_id=str(team_id), trial_id=trial_id,
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
                adapter=adapter, model=model,
                cp_client=cp_client, gateway_url=gateway_url,
                team_id=team_id, trial_id=trial_id,
            )
        return agent
    return make


