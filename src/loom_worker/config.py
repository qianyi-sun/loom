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

    log_level: LogLevel = "info"
    metrics_port: int = 9090
