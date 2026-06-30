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
    secret_keys = _read_secret_keys(core_v1_api, namespace)

    # 1) every required secret key exists
    violations.extend(_secret_violations(schema, secret_keys, namespace))

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
    violations.extend(_orphan_secret_violations(schema, secret_keys))

    return DoctorReport(violations=violations)


def reconcile_rendered(
    schema: Schema,
    core_v1_api: Any,
    namespace: str,
    rendered_manifests: str,
) -> DoctorReport:
    """Reconcile schema against the target rendered manifests.

    `loom cluster preflight --config` runs before the new Deployment is
    applied. Checking live pods for newly-added env vars would block every
    legitimate rollout that introduces a schema-backed env. This variant keeps
    live Secret validation but checks Deployment container env against the
    target manifests that `loom cluster up` is about to apply.
    """
    violations: list[DoctorViolation] = []
    secret_keys = _read_secret_keys(core_v1_api, namespace)
    violations.extend(_secret_violations(schema, secret_keys, namespace))

    for deployment_name, service_envs in _rendered_deployment_envs(
        schema,
        rendered_manifests,
    ).items():
        for svc, container_envs in service_envs.items():
            expected = _expected_env_vars_for_service(schema, svc)
            for container_name, present in container_envs.items():
                for missing in expected - present:
                    violations.append(DoctorViolation(
                        entry=missing,
                        kind="missing_env",
                        detail=(
                            f"Rendered Deployment {deployment_name} container "
                            f"{container_name} missing env {missing}"
                        ),
                    ))

    violations.extend(_orphan_secret_violations(schema, secret_keys))
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


def _read_secret_keys(core_v1_api: Any, namespace: str) -> set[str]:
    secret = core_v1_api.read_namespaced_secret(
        name="loom-secrets", namespace=namespace,
    )
    return set((secret.data or {}).keys())


def _secret_violations(
    schema: Schema,
    secret_keys: set[str],
    namespace: str,
) -> list[DoctorViolation]:
    expected_secret_keys: dict[str, str] = {}  # key -> entry name
    for name in schema.service_config:
        entry = schema.service_config[name]
        if entry.secret is None or not entry.required:
            continue
        for svc in entry.used_by:
            expected_secret_keys[entry.secret_key_for(svc)] = name

    violations: list[DoctorViolation] = []
    for key, entry_name in expected_secret_keys.items():
        if key not in secret_keys:
            violations.append(DoctorViolation(
                entry=entry_name,
                kind="missing_secret",
                detail=f"Secret key {key!r} not in loom-secrets/{namespace}",
            ))
    return violations


def _orphan_secret_violations(
    schema: Schema,
    secret_keys: set[str],
) -> list[DoctorViolation]:
    referenced_keys: set[str] = set()
    for name in schema.service_config:
        entry = schema.service_config[name]
        if entry.secret is None:
            continue
        for svc in entry.used_by:
            referenced_keys.add(entry.secret_key_for(svc))
    referenced_keys.update(schema.infra_secrets)
    return [
        DoctorViolation(
            entry=key,
            kind="orphan_secret",
            detail=f"Secret key {key!r} present but unreferenced by schema",
        )
        for key in secret_keys - referenced_keys
    ]


def _rendered_deployment_envs(
    schema: Schema,
    rendered_manifests: str,
) -> dict[str, dict[str, dict[str, set[str]]]]:
    try:
        import yaml  # type: ignore[import-untyped]
    except ModuleNotFoundError as exc:  # pragma: no cover - cluster extra provides it
        raise RuntimeError(
            "the 'yaml' package is required for rendered schema reconciliation",
        ) from exc

    deployments: dict[str, dict[str, dict[str, set[str]]]] = {}
    for doc in yaml.safe_load_all(rendered_manifests):
        if not isinstance(doc, dict) or doc.get("kind") != "Deployment":
            continue
        metadata = doc.get("metadata")
        if not isinstance(metadata, dict):
            continue
        deployment_name = metadata.get("name")
        if not isinstance(deployment_name, str):
            continue
        svc = _service_from_workload_name(deployment_name, schema.service_prefix)
        if svc is None:
            continue
        containers = (
            doc.get("spec", {})
            .get("template", {})
            .get("spec", {})
            .get("containers", [])
        )
        if not isinstance(containers, list):
            continue
        service_envs: dict[str, set[str]] = {}
        for index, container in enumerate(containers):
            if not isinstance(container, dict):
                continue
            container_name = container.get("name")
            if not isinstance(container_name, str):
                container_name = f"container-{index}"
            env = container.get("env", [])
            names = {
                entry.get("name")
                for entry in env
                if isinstance(entry, dict) and isinstance(entry.get("name"), str)
            }
            service_envs[container_name] = names
        deployments[deployment_name] = {svc: service_envs}
    return deployments


def _service_from_pod_name(name: str | None, prefix: Mapping[str, str]) -> str | None:
    """Pods are named `loom-<service>-...`. Find which service this is."""
    if name is None:
        return None
    return _service_from_workload_name(name, prefix)


def _service_from_workload_name(name: str, prefix: Mapping[str, str]) -> str | None:
    for svc in prefix:
        workload_name = svc if svc.startswith("loom-") else f"loom-{svc}"
        if name.startswith(f"{workload_name}-") or name == workload_name:
            return svc
    return None
