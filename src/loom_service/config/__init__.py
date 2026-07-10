"""LoomServiceSettings — wraps codegen with computed_fields.

Schema: `config/loom-schema.toml`. Edit the schema, not the fields.
Only computed_fields (behavior on top of schema-driven config) belong
here.
"""
from __future__ import annotations

from functools import cached_property
from typing import Any

from pydantic import computed_field

from loom.workload_trust import WorkloadTrustContract
from loom_service.config._generated import LoomServiceSettings as _BaseSettings


class LoomServiceSettings(_BaseSettings):
    """LoomServiceSettings adds behavior on top of the codegen'd class."""

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
