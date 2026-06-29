"""`loom cluster doctor`: schema-vs-cluster reconciliation."""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from loom_config.loader import Schema, ServiceConfigEntry

_K8S_TEMPLATE_ENV_ENTRIES: Mapping[str, frozenset[str]] = {
    # These schema entries are optional from the service Settings point of
    # view, but the default Kubernetes templates inject concrete values for
    # them. Doctor should still flag drift if they disappear from live pods.
    "control-plane": frozenset({"admin_secret_file"}),
    "llm-gateway": frozenset({"admin_secret_file"}),
    "loom-service": frozenset({"admin_secret_file"}),
    "worker": frozenset({"subprocess_gateway_url"}),
}


@dataclass(frozen=True)
class DoctorViolation:
    entry: str
    kind: str  # "missing_secret" | "missing_env" | "orphan_secret"
    detail: str


@dataclass
class DoctorReport:
    violations: list[DoctorViolation] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.violations


def reconcile(schema: Schema, core_v1_api: Any, namespace: str) -> DoctorReport:
    """Walk the schema against a live cluster.

    `core_v1_api` is a `kubernetes.client.CoreV1Api` instance.
    """
    violations: list[DoctorViolation] = []
    secret = core_v1_api.read_namespaced_secret(
        name="loom-secrets", namespace=namespace,
    )
    secret_keys = set((secret.data or {}).keys())

    # 1) every required secret key exists
    expected_secret_keys: dict[str, str] = {}  # key → entry name
    for name in schema.service_config:
        entry = schema.service_config[name]
        if entry.secret is None or not entry.required:
            continue
        for svc in entry.used_by:
            k = entry.secret_key_for(svc)
            expected_secret_keys[k] = name
    for k, entry_name in expected_secret_keys.items():
        if k not in secret_keys:
            violations.append(DoctorViolation(
                entry=entry_name,
                kind="missing_secret",
                detail=f"Secret key {k!r} not in loom-secrets/{namespace}",
            ))

    # 2) every declared env var present in each pod
    pods = core_v1_api.list_namespaced_pod(namespace=namespace).items
    for pod in pods:
        pod_svc = _service_from_pod_name(pod.metadata.name, schema.service_prefix)
        if pod_svc is None:
            continue
        expected = _expected_env_vars_for_service(schema, pod_svc)
        for c in pod.spec.containers:
            present = {e.name for e in (c.env or [])}
            for missing in expected - present:
                violations.append(DoctorViolation(
                    entry=missing,
                    kind="missing_env",
                    detail=f"Pod {pod.metadata.name} container missing env {missing}",
                ))

    # 3) orphan secrets (key in loom-secrets but no schema entry refs it)
    # Collect all keys declared in schema (required + optional service_config,
    # and infra_secrets for 3rd-party containers like postgres).
    referenced_keys: set[str] = set()
    for name in schema.service_config:
        entry = schema.service_config[name]
        if entry.secret is None:
            continue
        for svc in entry.used_by:
            referenced_keys.add(entry.secret_key_for(svc))
    referenced_keys.update(schema.infra_secrets)
    for k in secret_keys - referenced_keys:
        violations.append(DoctorViolation(
            entry=k, kind="orphan_secret",
            detail=f"Secret key {k!r} present but unreferenced by schema",
        ))

    return DoctorReport(violations=violations)


def _expected_env_vars_for_service(schema: Schema, service: str) -> set[str]:
    """Env vars the default Kubernetes render should put in a service pod."""
    template_entries = _K8S_TEMPLATE_ENV_ENTRIES.get(service, frozenset())
    return {
        entry.env_var_for(service)
        for entry in schema.service_config_for(service)
        if _entry_emits_env_by_default(entry) or entry.name in template_entries
    }


def _entry_emits_env_by_default(entry: ServiceConfigEntry) -> bool:
    return (
        entry.secret is not None
        or entry.required
        or entry.default is not None
        or entry.default_per_service is not None
    )


def _service_from_pod_name(name: str | None, prefix: Mapping[str, str]) -> str | None:
    """Pods are named `loom-<service>-...`. Find which service this is."""
    if name is None:
        return None
    for svc in prefix:
        if name.startswith(f"loom-{svc}-") or name == f"loom-{svc}":
            return svc
    return None
