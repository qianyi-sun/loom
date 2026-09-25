"""LoomServiceSettings — wraps codegen with computed_fields.

Schema: `config/loom-schema.toml`. Edit the schema, not the fields.
Only computed_fields (behavior on top of schema-driven config) belong
here.
"""
from __future__ import annotations

import math
import os
from functools import cached_property
from typing import Any, Self
from urllib.parse import urlsplit

from pydantic import computed_field, model_validator

from loom.workload_trust import WorkloadTrustContract
from loom_service.config._generated import LoomServiceSettings as _BaseSettings
from loom_service.public_links import configured_public_base_url


class LoomServiceSettings(_BaseSettings):
    """LoomServiceSettings adds behavior on top of the codegen'd class."""

    @model_validator(mode="after")
    def _validate_service_mode(self) -> Self:
        if (self.management_http_max_body_bytes <= 0 or self.management_http_max_inflight <= 0
                or not math.isfinite(self.management_http_body_timeout_sec)
                or self.management_http_body_timeout_sec <= 0):
            raise ValueError("management HTTP limits must be positive and finite")
        if self.service_mode not in {"application", "api_only", "management"}:
            raise ValueError("service_mode must be application, api_only or management")
        if self.service_mode in {"application", "api_only"}:
            self.storage_credentials()
        if self.managed_environment_config_file is not None and self.service_mode != "application":
            raise ValueError("child environment configuration requires application mode")
        if self.environment_management_config_file is not None and (
            self.service_mode != "management" or self.environment_management_github_token is None
            or not self.environment_management_github_token.get_secret_value().strip()
        ):
            raise ValueError("environment management configuration requires management mode and a GitHub credential")
        return self

    def storage_credentials(self) -> tuple[str, str]:
        """Reject workload storage use without explicitly supplied credentials."""
        if self.minio_access_key is None or self.minio_secret_key is None:
            raise ValueError("application mode requires storage credentials")
        return self.minio_access_key.get_secret_value(), self.minio_secret_key.get_secret_value()

    @property
    def hosted_session_cookie(self) -> bool:
        """Secure by default; local HTTP must be explicitly selected."""
        public_url = configured_public_base_url(self.public_base_url)
        return (
            not self.auth_local_http
            or self.auth_session_cookie_name.startswith("__Host-")
            or os.environ.get("LOOM_ENV", "").lower() == "production"
            or (public_url is not None and urlsplit(public_url).scheme == "https")
        )

    @property
    def session_cookie_name(self) -> str:
        name = self.auth_session_cookie_name
        if self.hosted_session_cookie and not name.startswith("__Host-"):
            return "__Host-" + name
        return name

    @cached_property
    def workload_contract(self) -> WorkloadTrustContract:
        """The v1 workload-trust contract represented by this settings object."""
        return WorkloadTrustContract(
            workload_trust_mode=self.workload_trust_mode,
            taskset_transforms_enabled=self.taskset_materializer_transforms_enabled,
            taskset_transform_network_isolated=(
                self.taskset_materializer_transform_network_isolated
            ),
            untrusted_workload_isolation=self.untrusted_workload_isolation,
        )

    @computed_field  # type: ignore[prop-decorator]
    @property
    def db_engine_url(self) -> str:
        """DSN for SQLAlchemy engine construction (#609).

        Returns db_url_pool when set (pgbouncer path), else db_url
        (direct). Callers constructing SQLAlchemy engines MUST use
        this, never db_url directly. db_url is reserved for LISTEN
        watchers and Alembic which need direct-to-Postgres semantics.
        """
        if self.db_url_pool:
            return str(self.db_url_pool)
        return str(self.db_url)

    @computed_field  # type: ignore[prop-decorator]
    @property
    def db_engine_connect_args(self) -> dict[str, Any]:
        """psycopg3 connect_args paired with db_engine_url (#609).

        prepare_threshold=None when routed through pgbouncer
        (transaction mode is incompatible with server-side prepared
        statements). Empty dict on the direct path (psycopg3 default:
        prepare after 5 executions).
        """
        if self.db_url_pool:
            return {"prepare_threshold": None}
        return {}


__all__ = ["LoomServiceSettings"]
