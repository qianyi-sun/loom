"""Per-worker registry of vLLM subprocesses keyed by HF model id.

Used when a trial requests `ModelSpec.source=hf, hf_execution=local-vllm`.
The worker spawns vLLM lazily on first use, caches the server info, and
reuses it for subsequent trials of the same model. Concurrent claims of
the same model are serialised by a per-model `asyncio.Lock` so we never
double-spawn.

Lifecycle:
- Lazy spawn on `get_or_launch(model_id)`. Wraps the existing sync
  `loom_cli.vllm_runner.launch_vllm` in `asyncio.to_thread` so the
  trial loop isn't blocked.
- Reused for the worker's entire lifetime (vLLM startup is 1-3 min;
  amortising across N trials of the same model is the whole point).
- `shutdown()` kills every subprocess on worker drain / SIGTERM. The
  existing `_LIVE_PROCESSES` registry in vllm_runner.py handles the
  signal-driven cleanup; we still call our own teardown for graceful
  drain so finalize-style teardown logs which models died.

The registry is opt-in: workers that don't ship the `vllm` extra (or
explicitly disable it) construct `WorkerVLLMRegistry(enabled=False)`,
which converts any `get_or_launch` into a clear AgentError instead of
crashing on `MissingVLLMDependencyError` mid-claim.
"""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from dataclasses import dataclass, field

from loom.errors import AgentError

logger = logging.getLogger(__name__)


@dataclass
class _ServerHandle:
    """What we track per-model. `pid` is recorded so an operator can
    correlate a hung vLLM with the worker that spawned it."""

    base_url: str
    served_model_name: str
    pid: int


@dataclass
class WorkerVLLMRegistry:
    """Per-worker map: HF model id → live vLLM subprocess.

    Threading: every method that touches `_servers` either holds
    `_global_lock` (for shutdown / membership reads) or one of the
    per-model locks in `_per_model_lock`. The two are never held
    together to avoid deadlocks.
    """

    enabled: bool = True
    # Optional: override vLLM launch knobs at worker startup. Most
    # operators leave defaults; explicit overrides land here when we
    # want per-fleet tuning (e.g. tensor_parallel_size=8 on H100 boxes).
    default_gpu_memory_utilization: float = 0.90
    default_tensor_parallel_size: int = 1

    _servers: dict[str, _ServerHandle] = field(default_factory=dict)
    _per_model_lock: dict[str, asyncio.Lock] = field(
        default_factory=lambda: defaultdict(asyncio.Lock),
    )
    _global_lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    async def get_or_launch(self, model_id: str) -> _ServerHandle:
        """Returns the live vLLM serving `model_id`, spawning if needed.

        Raises `AgentError` when:
        - the registry is disabled (`enabled=False`)
        - vLLM isn't installed in the worker env
        - vLLM fails to bind a port or never reaches healthy
        """
        if not self.enabled:
            raise AgentError(
                "this worker is not configured to run worker-spawned vLLM; "
                "set up a separate worker with the vllm extra installed, "
                "or use ModelSpec.source='local-server' to target an "
                "operator-configured server instead.",
            )

        # Fast path: already running, no need to lock.
        existing = self._servers.get(model_id)
        if existing is not None:
            return existing

        lock = self._per_model_lock[model_id]
        async with lock:
            # Recheck inside the lock — a concurrent task may have
            # spawned while we were waiting.
            existing = self._servers.get(model_id)
            if existing is not None:
                return existing

            handle = await self._spawn(model_id)
            self._servers[model_id] = handle
            return handle

    async def _spawn(self, model_id: str) -> _ServerHandle:
        """Wrap the sync launch_vllm in to_thread so the trial loop
        isn't blocked. vLLM startup is several minutes for large
        models; running on the main loop would stall every other
        trial."""
        try:
            # Lazy import — keeps `loom_cli` (CLI deps like rich) out
            # of the worker's hot path when vLLM is disabled.
            from loom_cli.vllm_runner import (
                MissingVLLMDependencyError,
                VLLMLaunchSpec,
                launch_vllm,
            )
        except ImportError as exc:
            raise AgentError(
                f"loom_cli.vllm_runner unavailable: {exc}",
            ) from exc

        spec = VLLMLaunchSpec(
            model=model_id,
            gpu_memory_utilization=self.default_gpu_memory_utilization,
            tensor_parallel_size=self.default_tensor_parallel_size,
        )

        logger.info(
            "spawning worker-vllm for model %s (gpu_mem=%.2f, tp=%d)",
            model_id,
            self.default_gpu_memory_utilization,
            self.default_tensor_parallel_size,
        )

        try:
            info = await asyncio.to_thread(launch_vllm, spec)
        except MissingVLLMDependencyError as exc:
            raise AgentError(
                "vLLM is not installed in this worker. Install with "
                "`pip install loom[vllm]` on the worker host.",
            ) from exc
        except Exception as exc:
            raise AgentError(
                f"vLLM failed to start for model {model_id!r}: {exc}",
            ) from exc

        logger.info(
            "worker-vllm ready: model=%s url=%s served=%s pid=%d",
            model_id, info.base_url, info.served_model_name, info.pid,
        )
        return _ServerHandle(
            base_url=info.base_url,
            served_model_name=info.served_model_name,
            pid=info.pid,
        )

    async def shutdown(self) -> None:
        """Kill every cached vLLM. Called on worker drain / SIGTERM.

        The vllm_runner module also installs SIGINT/SIGTERM handlers
        that hit `_LIVE_PROCESSES` (its own registry), so a hard kill
        of the worker still cleans up; this method is for graceful
        drain that wants to log which models came down."""
        async with self._global_lock:
            for model_id, handle in list(self._servers.items()):
                logger.info(
                    "shutting down worker-vllm: model=%s pid=%d",
                    model_id, handle.pid,
                )
                # Delegate to vllm_runner's tracked process kill —
                # avoids duplicating its SIGTERM + grace + SIGKILL
                # dance.
                try:
                    from loom_cli.vllm_runner import (
                        _LIVE_PROCESSES,
                        _stop_process,
                    )
                except ImportError:
                    continue
                for proc in list(_LIVE_PROCESSES):
                    if proc.pid == handle.pid:
                        await asyncio.to_thread(_stop_process, proc)
                        if proc in _LIVE_PROCESSES:
                            _LIVE_PROCESSES.remove(proc)
            self._servers.clear()
