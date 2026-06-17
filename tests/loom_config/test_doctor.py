"""`loom cluster doctor` core: schema-vs-cluster reconciliation."""
from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from loom_config.doctor import (
    reconcile,
)
from loom_config.loader import load_schema


def _fake_clients(
    secret_keys: set[str],
    pod_envs: dict[str, set[str]],
):
    core = MagicMock()
    secret = MagicMock()
    secret.data = {k: "AAA=" for k in secret_keys}
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
