"""Environment configuration for one read-only capacity collection pass."""

from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class ExecutionCapacityCollectorSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="LOOM_EXECUTION_CAPACITY_COLLECTOR_",
        extra="ignore",
    )

    target_id: str = Field(min_length=1, max_length=120)
    pool_id: str = Field(min_length=1, max_length=120)
    namespace: str = Field(pattern=r"^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$")
    node_label_selector: str = Field(min_length=1, max_length=500)
    nebius_project_id: str = Field(min_length=1, max_length=160)
    nebius_node_group_id: str = Field(min_length=1, max_length=160)
    nebius_region: str = Field(min_length=1, max_length=80)
    nebius_credentials_file: Path
    control_plane_url: str = Field(pattern=r"^https?://")
    control_plane_bearer_token_file: Path
    quota_nodes_name: str = Field(min_length=1, max_length=255)
    quota_vcpu_name: str = Field(min_length=1, max_length=255)
    quota_memory_name: str = Field(min_length=1, max_length=255)
    quota_storage_name: str = Field(min_length=1, max_length=255)
    quota_nodes_unit: str = Field(min_length=1, max_length=40)
    quota_vcpu_unit: str = Field(min_length=1, max_length=40)
    quota_memory_unit: str = Field(min_length=1, max_length=40)
    quota_storage_unit: str = Field(min_length=1, max_length=40)
    quota_service: str = Field(default="compute", min_length=1, max_length=120)
    source: str = Field(
        default="nebius-kubernetes-capacity-collector", min_length=1, max_length=120
    )
    request_timeout_seconds: float = Field(default=15.0, ge=1.0, le=60.0)
    request_attempts: int = Field(default=3, ge=1, le=5)


__all__ = ["ExecutionCapacityCollectorSettings"]
