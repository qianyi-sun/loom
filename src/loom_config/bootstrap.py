"""`loom cluster bootstrap-secrets` — schema-driven Secret materialization.

Emits a single `kubectl create secret generic loom-secrets --from-literal=...`
line covering every secret-backed entry in the schema. Operator runs
the printed command; the CLI does NOT invoke kubectl directly so
secrets never live in the CLI's process memory longer than necessary.
"""
from __future__ import annotations

import shlex
import subprocess
from collections.abc import Mapping
from urllib.parse import urlsplit, urlunsplit

from loom_config.loader import Schema


def _rewrite_dsn_host_port(dsn: str, *, host: str, port: int) -> str:
    """Rewrite the netloc's host+port in a psycopg/SQLAlchemy DSN,
    preserving scheme, credentials, database name, and query parameters.

    Raises ValueError on malformed inputs (missing userinfo, missing
    host, non-URL input).
    """
    try:
        parsed = urlsplit(dsn)
    except ValueError as exc:
        raise ValueError(f"malformed DSN: {dsn!r}") from exc

    if not parsed.scheme or not parsed.scheme.startswith(("postgres", "postgresql")):
        raise ValueError(f"unsupported scheme in DSN: {dsn!r}")
    if parsed.username is None or parsed.password is None:
        raise ValueError(f"DSN missing userinfo (user:password@): {dsn!r}")
    if not parsed.hostname:
        raise ValueError(f"DSN missing host: {dsn!r}")

    netloc = f"{parsed.username}:{parsed.password}@{host}:{port}"
    return urlunsplit((
        parsed.scheme, netloc, parsed.path, parsed.query, parsed.fragment,
    ))


_SMOKE_DEFAULTS: Mapping[str, str] = {
    "step-jwt-signing-key":   "smoke-jwt-key-do-not-use-in-prod",
    "cp-db-url":              "postgresql+psycopg://loom:loom@loom-postgres:5432/loom",
    "gw-db-url":              "postgresql+psycopg://loom:loom@loom-postgres:5432/loom",
    "svc-db-url":             "postgresql+psycopg://loom:loom@loom-postgres:5432/loom",
    "minio-access-key":       "minioadmin",
    "minio-secret-key":       "minioadmin",
    "worker-token":           "smoke-worker-token",
    "anthropic-api-key":      "smoke-anthropic",
    "openai-api-key":         "smoke-openai",
    "google-api-key":         "smoke-google",
    "together-api-key":       "smoke-together",
    "huggingface-api-key":    "smoke-hf",
    "batch-runner-cp-token":  "smoke-batch-cp-token",
    "secret-store-master-key": "YWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWFhYWE",
    # infra_secrets: non-codegen secrets that read from loom-secrets
    "postgres-user":          "loom",
    "postgres-password":      "loom",
}


def _all_secret_keys(schema: Schema) -> set[str]:
    keys: set[str] = set()
    for name in schema.service_config:
        e = schema.service_config[name]
        if e.secret is None:
            continue
        for svc in e.used_by:
            keys.add(e.secret_key_for(svc))
    keys.update(schema.infra_secrets)
    return keys


def _value_for(key: str, *, smoke_defaults: bool, rotate: bool, schema: Schema) -> str:
    if smoke_defaults and key in _SMOKE_DEFAULTS:
        return _SMOKE_DEFAULTS[key]
    if rotate:
        for name in schema.service_config:
            e = schema.service_config[name]
            if e.secret is None or e.secret.generate is None:
                continue
            for svc in e.used_by:
                if e.secret_key_for(svc) == key:
                    return subprocess.check_output(
                        shlex.split(e.secret.generate), text=True,
                    ).strip()
        # infra_secrets entries
        if key in schema.infra_secrets:
            infra = schema.infra_secrets[key]
            if infra.generate:
                return subprocess.check_output(
                    shlex.split(infra.generate), text=True,
                ).strip()
    return "<EDIT_ME>"


def render_bootstrap_command(
    schema: Schema,
    namespace: str = "loom",
    smoke_defaults: bool = False,
    rotate: bool = False,
) -> str:
    parts = ["kubectl create secret generic loom-secrets",
             f"--namespace={namespace}"]
    for key in sorted(_all_secret_keys(schema)):
        val = _value_for(key, smoke_defaults=smoke_defaults, rotate=rotate, schema=schema)
        parts.append(f"--from-literal={key}={shlex.quote(val)}")
    return " \\\n    ".join(parts)
