"""Cluster-deploy config (#76 Phase 1B + #146).

Public surface unchanged: `ClusterConfig` dataclass with operator
fields, `load_cluster_config(path)` returns one. Fields and defaults
now come from `config/loom-schema.toml` (`render_config` section).
The dataclass shape is materialized at import time so call sites
keep dot-access (`cfg.image_tag`, `cfg.replicas.service`).
"""
from __future__ import annotations

import tomllib
from dataclasses import dataclass, field, fields, make_dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from loom_config.loader import RenderConfigEntry, load_schema

_REPO_ROOT = Path(__file__).resolve().parents[2]
_SCHEMA = load_schema(_REPO_ROOT / "config" / "loom-schema.toml")


def _make_table_dataclass(entry: RenderConfigEntry) -> type:
    """For `python_type = "table"` entries (e.g. replicas), build a
    frozen dataclass whose fields are `entry.fields.keys()`."""
    assert entry.fields is not None
    cls = make_dataclass(
        f"_{entry.name.capitalize()}Config",
        [(k, type(v), field(default=v)) for k, v in entry.fields.items()],
        frozen=True,
    )
    return cls


def _build_cluster_config_cls() -> type:
    """Materialize `ClusterConfig` from `render_config`."""
    spec: list[tuple[str, type, Any]] = []
    for name in sorted(_SCHEMA.render_config):
        entry = _SCHEMA.render_config[name]
        if entry.python_type == "table":
            sub_cls = _make_table_dataclass(entry)
            spec.append((name, sub_cls, field(default_factory=sub_cls)))
        elif entry.python_type == "str_list":
            default = tuple(entry.default or ())
            spec.append((name, tuple[str, ...], field(default=default)))
        else:
            py_type = {"str": str, "int": int, "bool": bool, "float": float}[entry.python_type]
            spec.append((name, py_type, field(default=entry.default)))

    def _to_render_context(self: Any) -> dict[str, Any]:
        out: dict[str, Any] = {}
        for f in fields(self):
            val = getattr(self, f.name)
            if hasattr(val, "__dataclass_fields__"):
                out[f.name] = {sub.name: getattr(val, sub.name) for sub in fields(val)}
            else:
                out[f.name] = val
        return out

    return make_dataclass(
        "ClusterConfig",
        spec,
        frozen=True,
        namespace={"to_render_context": _to_render_context},
    )


if TYPE_CHECKING:
    # Static shape for mypy. Mirrors the runtime dataclass produced by
    # _build_cluster_config_cls() so call sites get attribute checking
    # and `-> ClusterConfig` annotations type-check. When the schema
    # (config/loom-schema.toml) changes, refresh this stub to match.

    @dataclass(frozen=True)
    class _ReplicasConfig:
        service: int = 2
        control_plane: int = 2
        gateway: int = 2
        web: int = 0
        worker: int = 3

    @dataclass(frozen=True)
    class _WorkerCapacityConfig:
        max_concurrent: int = 16
        cpu_request: str = "500m"
        cpu_limit: str = "8"
        memory_request: str = "1Gi"
        memory_limit: str = "16Gi"

    @dataclass(frozen=True)
    class ClusterConfig:
        image_tag: str = "0.7"
        ingress_cert_manager_cluster_issuer: str = ""
        ingress_class_name: str = "nginx"
        ingress_host: str = "loom.example.com"
        ingress_tls_secret_name: str = "loom-tls"
        artifacts_bucket: str = "artifacts"
        minio_image: str = "minio/minio"
        minio_storage_gi: int = 500
        namespace: str = "loom"
        postgres_image: str = "postgres:16"
        postgres_storage_gi: int = 50
        provider_egress_allowlist: tuple[str, ...] = ()
        replicas: _ReplicasConfig = field(default_factory=_ReplicasConfig)
        trajectories_bucket: str = "trajectories"
        worker_capacity: _WorkerCapacityConfig = field(default_factory=_WorkerCapacityConfig)
        worker_trajectory_storage_gi: int = 100

        def to_render_context(self) -> dict[str, Any]: ...
else:
    ClusterConfig = _build_cluster_config_cls()


_DEPRECATED_TOP_LEVEL_KEYS: dict[str, str] = {
    "gateway_public_host": (
        "gateway_public_host is no longer supported; public beta keeps "
        "loom-llm-gateway internal-only. Remove it and route all public "
        "clients through ingress_host + /api/v1."
    ),
}


def load_cluster_config(path: Path | None) -> ClusterConfig:
    """Same semantics as before #146: empty/missing path → defaults;
    unknown top-level or nested keys raise loudly."""
    if path is None:
        return ClusterConfig()
    if not path.exists():
        raise FileNotFoundError(f"cluster config not found: {path}")
    raw = tomllib.loads(path.read_text(encoding="utf-8"))
    deprecated = set(raw) & set(_DEPRECATED_TOP_LEVEL_KEYS)
    if deprecated:
        messages = [_DEPRECATED_TOP_LEVEL_KEYS[key] for key in sorted(deprecated)]
        raise ValueError("; ".join(messages))
    field_names = {f.name for f in fields(ClusterConfig)}
    unknown = set(raw.keys()) - field_names
    if unknown:
        raise ValueError(
            f"unknown keys in cluster config: {sorted(unknown)} "
            f"(known: {sorted(field_names)})"
        )
    kwargs: dict[str, Any] = {}
    for name, val in raw.items():
        entry_field = next(f for f in fields(ClusterConfig) if f.name == name)
        field_type = entry_field.type
        if isinstance(field_type, type) and hasattr(field_type, "__dataclass_fields__"):
            sub_cls: type = field_type
            sub_known = {f.name for f in fields(sub_cls)}
            if not isinstance(val, dict):
                raise ValueError(f"[{name}] must be a TOML table")
            sub_unknown = set(val.keys()) - sub_known
            if sub_unknown:
                raise ValueError(
                    f"unknown keys under [{name}]: {sorted(sub_unknown)} "
                    f"(known: {sorted(sub_known)})"
                )
            # Coerce values to the right type via the sub-dataclass default's type
            default_instance = sub_cls()
            coerced = {k: type(getattr(default_instance, k))(v) for k, v in val.items()}
            kwargs[name] = sub_cls(**coerced)
        else:
            if entry_field.type == tuple[str, ...]:
                if not isinstance(val, list) or not all(isinstance(v, str) for v in val):
                    raise ValueError(f"{name} must be a TOML array of strings")
                kwargs[name] = tuple(val)
            else:
                kwargs[name] = val
    return ClusterConfig(**kwargs)
