"""`loom cluster doctor`: schema-vs-cluster reconciliation."""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlsplit

from loom_config.loader import Schema, ServiceConfigEntry

# TCP-proxy DaemonSets (`loom cluster` gateway/worker/minio routers) forward a
# node hostPort into an in-cluster Service so off-cluster workers can reach it.
# Their pod names share a prefix with a schema service (`loom-worker-router`
# vs `loom-worker`) but they run only a socat container — never classify them
# as that service. See `_service_from_workload_name`.
_AUXILIARY_PROXY_WORKLOADS: frozenset[str] = frozenset({
    "loom-gateway-router",
    "loom-worker-router",
    "loom-minio-router",
})

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

    # 4) pgbouncer URL ↔ enabled invariants
    pgbouncer_cfg = schema.render_config.get("pgbouncer", None)
    if pgbouncer_cfg is not None and (pgbouncer_cfg.fields or {}).get("enabled"):
        import base64
        secret_values: dict[str, str] = {}
        try:
            secret = core_v1_api.read_namespaced_secret("loom-secrets", namespace)
            for key in (
                "cp-db-url", "gw-db-url", "svc-db-url",
                "cp-db-url-pool", "gw-db-url-pool", "svc-db-url-pool",
            ):
                raw = (secret.data or {}).get(key)
                if raw is not None:
                    secret_values[key] = base64.b64decode(raw).decode("utf-8")
        except Exception:
            # Silently swallow: _secret_violations already surfaces
            # missing-secret conditions.
            pass
        violations.extend(_check_pgbouncer_invariants(schema, secret_values))

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

    # pgbouncer URL ↔ enabled invariants
    pgbouncer_cfg = schema.render_config.get("pgbouncer", None)
    if pgbouncer_cfg is not None and (pgbouncer_cfg.fields or {}).get("enabled"):
        import base64
        secret_values_r: dict[str, str] = {}
        try:
            secret = core_v1_api.read_namespaced_secret("loom-secrets", namespace)
            for key in (
                "cp-db-url", "gw-db-url", "svc-db-url",
                "cp-db-url-pool", "gw-db-url-pool", "svc-db-url-pool",
            ):
                raw = (secret.data or {}).get(key)
                if raw is not None:
                    secret_values_r[key] = base64.b64decode(raw).decode("utf-8")
        except Exception:
            # Silently swallow: _secret_violations already surfaces
            # missing-secret conditions.
            pass
        violations.extend(_check_pgbouncer_invariants(schema, secret_values_r))

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
            names: set[str] = {
                entry["name"]
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
    # Auxiliary socat proxy workloads share a name prefix with a schema
    # service — `loom-worker-router` starts with `loom-worker-`, `loom-minio-
    # router` with `loom-minio-` — but they are NOT that service: their pods
    # carry a bare socat container with none of the service env. Classifying
    # them as the service makes doctor false-flag every declared env var as
    # missing on the proxy pod. Exclude them before the prefix match.
    for aux in _AUXILIARY_PROXY_WORKLOADS:
        if name == aux or name.startswith(f"{aux}-"):
            return None
    for svc in prefix:
        workload_name = svc if svc.startswith("loom-") else f"loom-{svc}"
        if name.startswith(f"{workload_name}-") or name == workload_name:
            return svc
    return None


# ---------------------------------------------------------------------------
# pgbouncer URL invariant check (#609)
# ---------------------------------------------------------------------------

_PGBOUNCER_DIRECT_KEYS = ("cp-db-url", "gw-db-url", "svc-db-url")


def _dsn_host(dsn: str) -> str:
    """Return the hostname component of a DSN, or '' on parse failure."""
    try:
        return urlsplit(dsn).hostname or ""
    except (ValueError, TypeError):
        return ""


def _check_pgbouncer_invariants(
    schema: Schema,
    secret_values: Mapping[str, str],
) -> list[DoctorViolation]:
    """Verify db_url ↔ pgbouncer.enabled host invariants (#609).

    When ``pgbouncer.enabled=true``:

    * ``cp-db-url``, ``gw-db-url``, ``svc-db-url`` MUST point at
      ``loom-postgres`` (direct).  Alembic and LISTEN watchers depend on a
      direct Postgres connection and are broken when routed through PgBouncer.

    * ``cp-db-url-pool``, ``gw-db-url-pool``, ``svc-db-url-pool`` MUST point
      at ``loom-pgbouncer``.  When pgbouncer is enabled but pool URLs still
      resolve to ``loom-postgres``, services silently bypass the pool.

    Missing secret keys are skipped — ``_secret_violations`` handles that.
    """
    pgbouncer_entry = schema.render_config.get("pgbouncer")
    if pgbouncer_entry is None:
        return []
    fields = pgbouncer_entry.fields or {}
    if not fields.get("enabled"):
        return []

    violations: list[DoctorViolation] = []
    for direct_key in _PGBOUNCER_DIRECT_KEYS:
        pool_key = f"{direct_key}-pool"

        direct_url = secret_values.get(direct_key, "")
        if direct_url:
            host = _dsn_host(direct_url)
            if host != "loom-postgres":
                violations.append(DoctorViolation(
                    entry=direct_key,
                    kind="pgbouncer_invariant",
                    detail=(
                        f"{direct_key} host is {host!r}, expected 'loom-postgres'."
                        " LISTEN watchers and Alembic require a direct Postgres"
                        " connection."
                    ),
                ))

        pool_url = secret_values.get(pool_key, "")
        if pool_url:
            host = _dsn_host(pool_url)
            if host != "loom-pgbouncer":
                violations.append(DoctorViolation(
                    entry=pool_key,
                    kind="pgbouncer_invariant",
                    detail=(
                        f"{pool_key} host is {host!r}, expected 'loom-pgbouncer'."
                        " pgbouncer.enabled=true but service would bypass the pool."
                    ),
                ))

    return violations
