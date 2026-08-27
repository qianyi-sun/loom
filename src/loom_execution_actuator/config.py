from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class ExecutionActuatorSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="LOOM_EXECUTION_ACTUATOR_",
        extra="ignore",
    )

    db_url: str
    controller_id: str = Field(min_length=1, max_length=120)
    target_id: str = Field(min_length=1, max_length=80)
    namespace: str = Field(pattern=r"^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$")
    runtime_class_name: str = Field(min_length=1, max_length=63)
    service_account_name: str = "loom-execution-attempt"
    credential_broker_url: str = (
        "http://loom-llm-gateway.loom.svc.cluster.local:9100/internal/service-execution"
    )
    poll_seconds: float = Field(default=2.0, ge=0.25, le=60)
    full_reconcile_seconds: float = Field(default=30.0, ge=5, le=300)
    watch_timeout_seconds: int = Field(default=15, ge=5, le=60)
    command_limit: int = Field(default=20, ge=1, le=100)
    command_lease_seconds: int = Field(default=60, ge=5, le=300)
    delete_grace_seconds: int = Field(default=30, ge=0, le=300)
    health_host: str = "0.0.0.0"
    health_port: int = Field(default=8093, ge=1024, le=65535)
