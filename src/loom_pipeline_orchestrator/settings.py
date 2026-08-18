"""Closed environment settings for the standalone Pipeline orchestrator."""

from __future__ import annotations

from pathlib import Path

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class PipelineOrchestratorSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="LOOM_PIPELINE_ORCHESTRATOR_",
        extra="ignore",
    )

    db_url: str
    controller_id: str = Field(min_length=1, max_length=128)
    poll_seconds: float = Field(default=2.0, gt=0)
    picker_batch: int = Field(default=50, ge=1, le=50)
    lease_seconds: int = Field(default=60, ge=60, le=60)
    renew_seconds: int = Field(default=10, ge=10, le=10)
    health_host: str = "0.0.0.0"
    health_port: int = Field(default=8092, ge=1, le=65535)
    repo_root: Path = Path(".")
    resource_profiles_path: Path = Path("config/resource-profiles.toml")
    image_runtime_contracts_path: Path = Path("config/image-runtime-contracts.toml")
    artifacts_bucket: str = "artifacts"
    minio_endpoint: str = "http://loom-minio:9000"
    minio_access_key: SecretStr
    minio_secret_key: SecretStr
    minio_region: str = "us-east-1"
