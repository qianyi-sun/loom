"""TOML parser for `config/loom-schema.toml`.

Produces a typed `Schema` object whose helpers are consumed by
codegen (`codegen.py`), template rendering (`render.py`), and the
operator commands (`doctor.py`, `bootstrap.py`). Strict validation
on load — typos and missing required fields surface immediately,
not at first deploy.
"""
from __future__ import annotations

import tomllib
from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

_PYTHON_TYPES = frozenset({
    "str", "int", "bool", "float", "Path",
    "SecretStr", "PostgresDsn", "HttpUrl", "LogLevel",
    "table",
})


@dataclass(frozen=True)
class SecretRef:
    key: str | None = None
    key_per_service: Mapping[str, str] | None = None
    generate: str | None = None


@dataclass(frozen=True)
class ServiceConfigEntry:
    name: str
    used_by: tuple[str, ...]
    python_type: str
    required: bool = False
    default: Any = None
    default_per_service: Mapping[str, Any] | None = None
    secret: SecretRef | None = None
    description: str = ""
    env_override: Mapping[str, str] | None = None
    _prefix: Mapping[str, str] = field(default_factory=dict, repr=False)

    def env_var_for(self, service: str) -> str:
        if self.env_override and service in self.env_override:
            return self.env_override[service]
        return f"LOOM_{self._prefix[service]}_{self.name.upper()}"

    def secret_key_for(self, service: str) -> str:
        if self.secret is None:
            raise ValueError(f"{self.name} is not secret-backed")
        if self.secret.key is not None:
            return self.secret.key
        assert self.secret.key_per_service is not None
        return self.secret.key_per_service[service]

    def value_for(self, service: str) -> Any:
        if self.secret is not None:
            raise ValueError(f"{self.name} is secret-backed; use secret_key_for")
        if self.default_per_service is not None:
            return self.default_per_service[service]
        if self.default is not None:
            return self.default
        raise ValueError(f"{self.name} has no default for {service}")


@dataclass(frozen=True)
class InfraSecretEntry:
    """A Secret key used by a 3rd-party container (e.g. postgres).

    These live in ``loom-secrets`` and are referenced by k8s templates but
    do NOT correspond to a Pydantic Settings field in any loom microservice.
    """

    name: str
    description: str = ""
    generate: str | None = None


@dataclass(frozen=True)
class RenderConfigEntry:
    name: str
    python_type: str
    default: Any = None
    fields: Mapping[str, Any] | None = None
    description: str = ""


@dataclass(frozen=True)
class Schema:
    version: int
    service_prefix: Mapping[str, str]
    service_config: Mapping[str, ServiceConfigEntry]
    render_config: Mapping[str, RenderConfigEntry]
    infra_secrets: Mapping[str, InfraSecretEntry] = field(default_factory=dict)

    def service_config_for(
        self, service: str,
    ) -> Iterator[ServiceConfigEntry]:
        for name in sorted(self.service_config):
            entry = self.service_config[name]
            if service in entry.used_by:
                yield entry


def _parse_secret(raw: Any, entry_name: str) -> SecretRef | None:
    if raw is None:
        return None
    if not isinstance(raw, dict):
        raise ValueError(f"{entry_name}.secret must be a table")
    key = raw.get("key")
    per_svc = raw.get("key_per_service")
    if (key is None) == (per_svc is None):
        raise ValueError(
            f"{entry_name}.secret: exactly one of 'key' or "
            f"'key_per_service' must be set"
        )
    return SecretRef(
        key=key,
        key_per_service=per_svc,
        generate=raw.get("generate"),
    )


def _parse_service_entry(
    name: str,
    raw: Mapping[str, Any],
    prefix: Mapping[str, str],
) -> ServiceConfigEntry:
    used_by = tuple(raw.get("used_by", ()))
    if not used_by:
        raise ValueError(f"service_config.{name}.used_by must be non-empty")
    unknown_services = set(used_by) - set(prefix)
    if unknown_services:
        raise ValueError(
            f"service_config.{name}.used_by references unknown services: "
            f"{sorted(unknown_services)} (known: {sorted(prefix)})"
        )
    py = raw.get("python_type")
    if py not in _PYTHON_TYPES:
        raise ValueError(
            f"service_config.{name}: unknown python_type {py!r} "
            f"(allowed: {sorted(_PYTHON_TYPES)})"
        )
    return ServiceConfigEntry(
        name=name,
        used_by=used_by,
        python_type=py,
        required=bool(raw.get("required", False)),
        default=raw.get("default"),
        default_per_service=raw.get("default_per_service"),
        secret=_parse_secret(raw.get("secret"), f"service_config.{name}"),
        description=raw.get("description", ""),
        env_override=raw.get("env_override"),
        _prefix=prefix,
    )


def _parse_render_entry(name: str, raw: Mapping[str, Any]) -> RenderConfigEntry:
    py = raw.get("python_type")
    if py not in _PYTHON_TYPES:
        raise ValueError(
            f"render_config.{name}: unknown python_type {py!r}"
        )
    return RenderConfigEntry(
        name=name,
        python_type=py,
        default=raw.get("default"),
        fields=raw.get("fields"),
        description=raw.get("description", ""),
    )


def load_schema(path: Path) -> Schema:
    raw = tomllib.loads(path.read_text(encoding="utf-8"))
    meta = raw.get("meta") or {}
    version = int(meta.get("version", 0))
    if version != 1:
        raise ValueError(f"unsupported schema version: {version}")
    prefix = raw.get("service_prefix")
    if not isinstance(prefix, dict) or not prefix:
        raise ValueError("[service_prefix] table is required and non-empty")
    service_raw = raw.get("service_config", {}) or {}
    render_raw = raw.get("render_config", {}) or {}
    infra_raw = raw.get("infra_secrets", {}) or {}
    service = {
        name: _parse_service_entry(name, val, prefix)
        for name, val in service_raw.items()
    }
    render = {
        name: _parse_render_entry(name, val)
        for name, val in render_raw.items()
    }
    infra = {
        name: InfraSecretEntry(
            name=name,
            description=val.get("description", ""),
            generate=val.get("generate"),
        )
        for name, val in infra_raw.items()
    }
    return Schema(
        version=version,
        service_prefix=prefix,
        service_config=service,
        render_config=render,
        infra_secrets=infra,
    )
