"""WorkerSettings (spec §7.5)."""

from __future__ import annotations

from pathlib import Path

from pydantic import HttpUrl, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

from loom.models.types import LogLevel


class WorkerSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="LOOM_WORKER_", env_file=".env", extra="forbid",
    )

    control_plane_url: HttpUrl
    gateway_url: HttpUrl
    token: SecretStr

    minio_endpoint: str
    minio_access_key: SecretStr
    minio_secret_key: SecretStr
    minio_region: str = "us-east-1"

    max_concurrent: int = 5
    drain_timeout_sec: int = 600
    claim_poll_interval_sec: float = 1.0
    heartbeat_interval_sec: float = 5.0

    trajectory_cache_dir: Path = Path("/var/lib/loom/trajectories")
    docker_socket: Path = Path("/var/run/docker.sock")

    # Root the worker scans for `fixture://<task_id>` sources. The
    # dev compose mounts the repo's tests/fixtures/tasks here so the
    # canary hello-world trial can run end-to-end without a bundle
    # upload. Production leaves it unset and rejects fixture:// in
    # favor of s3://bundles/<sha>/.
    fixtures_root: Path | None = None

    # Directory the worker caches HF-fetched benchmark bundles in. When
    # set, `snapshot_download` writes to this dir so re-claims of the
    # same task hit local disk instead of re-fetching from HF. When
    # unset, HF Hub's default cache (~/.cache/huggingface/) is used —
    # fine for single-host dev, less so for ephemeral worker pods.
    # Default unset; dev compose mounts /var/lib/loom/benchmarks for
    # persistence across container restarts.
    benchmark_cache: Path | None = None

    # PR-E: worker-spawned vLLM. When True, the worker can serve
    # trials that select `ModelSpec.source=hf, hf_execution=local-vllm`
    # by spawning vLLM subprocesses against HF model ids. Requires the
    # `vllm` extra (`pip install loom[vllm]`) and a GPU. Defaults False
    # because spawning vLLM without GPUs / extras would 500 every
    # trial; opt-in protects deployments that don't run local weights.
    enable_worker_vllm: bool = False
    # Knobs that the registry passes to every spawned vLLM. Match
    # vLLM's own defaults except where Loom's expected workloads
    # benefit from a fleet-wide override.
    vllm_gpu_memory_utilization: float = 0.90
    vllm_tensor_parallel_size: int = 1

    log_level: LogLevel = "info"
    metrics_port: int = 9090
