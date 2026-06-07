"""LoomServiceSettings — env-driven config (spec §2, §9).

env_prefix `LOOM_SVC_`. `extra="forbid"` so unknown vars fail at
startup rather than silently no-op. The campaign-runner knobs are
plumbed early (Plan 19 wires them) so the settings shape stays stable
across the 6-plan service-layer arc.
"""

from __future__ import annotations

from pydantic import HttpUrl, PostgresDsn, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

from loom.models.types import LogLevel


class LoomServiceSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="LOOM_SVC_", env_file=".env", extra="forbid",
    )

    db_url: PostgresDsn
    minio_endpoint: str
    minio_access_key: SecretStr
    minio_secret_key: SecretStr
    minio_region: str = "us-east-1"
    control_plane_url: HttpUrl
    gateway_url: HttpUrl

    bind_host: str = "0.0.0.0"
    bind_port: int = 8090
    log_level: LogLevel = "info"

    # Trajectory / ATIF presigned URLs (Plan 18).
    signed_url_expiry_sec: int = 3600

    # Bucket names — match what the worker + finalize.py actually use.
    # Worker's TrajectoryWriter writes to bucket `trajectories` at key
    # `{team_id}/{trial_id}/events.jsonl`. `finalize.py` writes ATIF to
    # the same bucket at key `{team_id}/{trial_id}/atif.json`.
    # `loom_control_plane/routes/artifacts.py` presigns the `artifacts`
    # bucket. Promoting to settings so deploys can override without
    # patching Python.
    trajectories_bucket: str = "trajectories"
    artifacts_bucket: str = "artifacts"

    # Campaign runner (Plan 19); plumbed early to keep settings stable.
    campaign_runner_batch_size: int = 50
    campaign_runner_submit_rate_per_sec: int = 100
    campaign_runner_poll_interval_sec: int = 5
