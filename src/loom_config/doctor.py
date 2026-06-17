"""`loom cluster doctor`: schema-vs-cluster reconciliation."""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from loom_config.loader import Schema


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
        expected = {e.env_var_for(pod_svc) for e in schema.service_config_for(pod_svc)}
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


def _service_from_pod_name(name: str | None, prefix: Mapping[str, str]) -> str | None:
    """Pods are named `loom-<service>-...`. Find which service this is."""
    if name is None:
        return None
    for svc in prefix:
        if name.startswith(f"loom-{svc}-") or name == f"loom-{svc}":
            return svc
    return None
