"""`loom cluster doctor` core: schema-vs-cluster reconciliation."""
from __future__ import annotations

import base64
from pathlib import Path
from unittest.mock import MagicMock

from loom_config.doctor import (
    reconcile,
)
from loom_config.loader import load_schema

# Correct DSN values that satisfy pgbouncer invariants:
# direct URLs must resolve to loom-postgres; pool URLs to loom-pgbouncer.
_DIRECT_DSN = base64.b64encode(
    b"postgresql+psycopg://loom:pw@loom-postgres:5432/loom"
).decode()
_POOL_DSN = base64.b64encode(
    b"postgresql+psycopg://loom:pw@loom-pgbouncer:6432/loom"
).decode()

_PGBOUNCER_SECRET_VALUES: dict[str, str] = {
    "cp-db-url": _DIRECT_DSN,
    "gw-db-url": _DIRECT_DSN,
    "svc-db-url": _DIRECT_DSN,
    "cp-db-url-pool": _POOL_DSN,
    "gw-db-url-pool": _POOL_DSN,
    "svc-db-url-pool": _POOL_DSN,
}


def _fake_clients(
    secret_keys: set[str],
    pod_envs: dict[str, set[str]],
):
    core = MagicMock()
    secret = MagicMock()
    # Build secret.data from the provided keys.  For db-url keys that are
    # present in secret_keys, use correct pgbouncer-friendly DSN values so
    # _check_pgbouncer_invariants does not fire spurious violations.  Any key
    # that was intentionally omitted from secret_keys stays absent.
    data: dict[str, str] = {}
    for k in secret_keys:
        data[k] = _PGBOUNCER_SECRET_VALUES.get(k, "AAA=")
    # Pool keys (db_url_pool) are required=false so they never appear in
    # secret_keys, but the pgbouncer invariant check skips absent keys, so
    # no extra injection is needed.
    secret.data = data
    core.read_namespaced_secret.return_value = secret
    pods_list = MagicMock()
    pods = []
    for pod_name, envs in pod_envs.items():
        p = MagicMock()
        p.metadata.name = pod_name
        env_objs = []
        for e in envs:
            env = MagicMock()
            env.name = e
            env_objs.append(env)
        c = MagicMock()
        c.env = env_objs
        p.spec.containers = [c]
        pods.append(p)
    pods_list.items = pods
    core.list_namespaced_pod.return_value = pods_list
    return core


def test_clean_cluster_has_no_violations() -> None:
    schema = load_schema(Path("config/loom-schema.toml"))
    secret_keys = set()
    for name in schema.service_config:
        e = schema.service_config[name]
        if e.secret is None:
            continue
        for svc in e.used_by:
            secret_keys.add(e.secret_key_for(svc))
    pod_envs = {}
    for svc in schema.service_prefix:
        pod_envs[f"loom-{svc}-0"] = {e.env_var_for(svc) for e in schema.service_config_for(svc)}
    core = _fake_clients(secret_keys, pod_envs)
    report = reconcile(schema, core, namespace="loom")
    assert report.violations == []


def test_router_proxy_pods_are_not_classified_as_a_service() -> None:
    # `loom-worker-router` / `loom-minio-router` socat pods share a name
    # prefix with the `worker` / `minio` schema services but carry none of
    # their env. Doctor must not flag every declared env var as missing on
    # them (regression: kind staging smoke storage-lifecycle round-trip).
    schema = load_schema(Path("config/loom-schema.toml"))
    secret_keys = set()
    for name in schema.service_config:
        e = schema.service_config[name]
        if e.secret is None:
            continue
        for svc in e.used_by:
            secret_keys.add(e.secret_key_for(svc))
    pod_envs = {svc: set() for svc in ()}
    for svc in schema.service_prefix:
        pod_envs[f"loom-{svc}-0"] = {
            e.env_var_for(svc) for e in schema.service_config_for(svc)
        }
    # Bare socat proxy pods with no env whatsoever.
    pod_envs["loom-worker-router-p2bnb"] = set()
    pod_envs["loom-gateway-router-abcde"] = set()
    pod_envs["loom-minio-router-zzz12"] = set()
    core = _fake_clients(secret_keys, pod_envs)
    report = reconcile(schema, core, namespace="loom")
    missing_env_pods = {
        v.detail for v in report.violations if v.kind == "missing_env"
    }
    assert missing_env_pods == set(), missing_env_pods


def test_family_orchestrator_gateway_secrets_are_schema_owned() -> None:
    schema = load_schema(Path("config/loom-schema.toml"))
    secret_keys = set()
    for name in schema.service_config:
        e = schema.service_config[name]
        if e.secret is None:
            continue
        for svc in e.used_by:
            secret_keys.add(e.secret_key_for(svc))
    secret_keys.add("family-orchestrator-token")
    core = _fake_clients(secret_keys, {})

    report = reconcile(schema, core, namespace="loom")

    orphan_entries = {
        v.entry
        for v in report.violations
        if v.kind == "orphan_secret"
    }
    assert "family-orchestrator-token" not in orphan_entries


def test_missing_secret_key_is_a_violation() -> None:
    schema = load_schema(Path("config/loom-schema.toml"))
    secret_keys = set()
    for name in schema.service_config:
        e = schema.service_config[name]
        if e.secret is None or not e.required:
            continue
        for svc in e.used_by:
            if name == "db_url" and svc == "control-plane":
                continue  # intentionally drop cp-db-url
            secret_keys.add(e.secret_key_for(svc))
    pod_envs = {}
    core = _fake_clients(secret_keys, pod_envs)
    report = reconcile(schema, core, namespace="loom")
    names = [v.entry for v in report.violations]
    assert "db_url" in names
