#!/usr/bin/env python3
"""Persist canonical staging identities before reconciling their backing services.

The caller supplies its authorized canonical Kubernetes transport. This module
does not install a rollout driver, copy admin credentials, or invoke a shell.
CNPG creates the database roles from the separately protected Cluster render.
"""

from __future__ import annotations

import base64
import binascii
import ipaddress
import json
import os
import re
import secrets
import subprocess
import tempfile
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, Protocol, cast
from urllib.parse import quote, urlsplit, urlunsplit

NAMESPACE = "loom-staging"
DB_SECRETS = {
    "loom-nebius-staging-db-gateway": "loom_nebius_staging_gateway",
    "loom-nebius-staging-db-actuator": "loom_nebius_staging_actuator",
}
MINIO_SECRET = "loom-nebius-staging-canonical-inputs"
MINIO_USER = "loom-nebius-staging-inputs"
MINIO_POLICY = "loom-nebius-staging-inputs"
COLLECTOR_SECRET = "loom-nebius-staging-collector"
OWNER_LABEL = "loom.ca/nebius-staging-identity"
OWNER_VALUE = "canonical-v1"
INPUT_POLICY = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": ["s3:GetBucketLocation", "s3:ListBucket"],
            "Resource": ["arn:aws:s3:::loom-staging-artifacts"],
        },
        {
            "Effect": "Allow",
            "Action": ["s3:GetObject"],
            "Resource": ["arn:aws:s3:::loom-staging-artifacts/*"],
        },
    ],
}

# Sent as source to the existing canonical control-plane container. The raw
# token is delivered through stdin only; SQL stores its hash. ON CONFLICT never
# resets an existing token's expiry, scopes or revocation state.
COLLECTOR_REGISTER_SOURCE = r'''
import hashlib
import json
import os
import re
import sys

from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url

try:
    if os.environ.get("LOOM_ENV") != "staging" or os.environ.get("LOOM_NAMESPACE") != "loom-staging":
        raise ValueError("canonical identity mismatch")
    token = sys.stdin.buffer.read(256).decode("ascii")
    if not re.fullmatch(r"loom_ecc_[a-f0-9]{64}", token):
        raise ValueError("invalid token")
    url = make_url(os.environ["LOOM_CP_DB_URL"])
    if url.get_backend_name() != "postgresql" or url.database != "loom":
        raise ValueError("canonical database mismatch")
    engine = create_engine(url.set(drivername="postgresql+psycopg"))
    try:
        with engine.begin() as conn:
            conn.execute(text("SET LOCAL statement_timeout = '15s'"))
            digest = hashlib.sha256(token.encode()).digest()
            created = conn.execute(text("""
                INSERT INTO tokens (token_hash, type, scopes, team_id, issued_at, expires_at)
                VALUES (:hash, 'worker', ARRAY['execution:capacity:observe'], NULL, now(), NULL)
                ON CONFLICT (token_hash) DO NOTHING
                RETURNING token_hash
            """), {"hash": digest}).first() is not None
            row = conn.execute(text("""
                SELECT type, scopes, team_id, expires_at, revoked_at
                FROM tokens WHERE token_hash = :hash FOR UPDATE
            """), {"hash": digest}).one()
            if (row.type != "worker" or row.scopes != ["execution:capacity:observe"]
                    or row.team_id is not None or row.expires_at is not None
                    or row.revoked_at is not None):
                raise ValueError("existing token is not reusable")
    finally:
        engine.dispose()
    print(json.dumps({"collector_created": int(created), "collector_verified": 1}))
except Exception:
    print("collector registration failed", file=sys.stderr)
    raise SystemExit(1) from None
'''


class BootstrapError(RuntimeError):
    """Secret-safe reconciliation failure; source errors must not be displayed."""


class BootstrapAdapter(Protocol):
    def get_secret(self, namespace: str, name: str) -> dict[str, Any] | None: ...

    def create_secret(self, document: dict[str, Any]) -> bool:
        """Create only: return False for an AlreadyExists conflict, never replace."""
        ...

    def exec_control_plane(self, source: str, input_payload: bytes) -> bytes: ...


class CommandRunner(Protocol):
    def __call__(
        self,
        argv: Sequence[str],
        *,
        env: Mapping[str, str],
        input: bytes | None,
        stdout: int,
        stderr: int,
        timeout: int,
        check: bool,
    ) -> subprocess.CompletedProcess[bytes]: ...


def _data(document: dict[str, Any], *, name: str, keys: set[str] | None = None) -> dict[str, str]:
    try:
        metadata = document["metadata"]
        if metadata["name"] != name or metadata["namespace"] != NAMESPACE:
            raise ValueError
        raw = document["data"]
        if not isinstance(raw, dict) or not raw:
            raise ValueError
        if keys is not None:
            raw = {key: raw[key] for key in keys}
        result = {
            key: base64.b64decode(value, validate=True).decode("utf-8")
            for key, value in raw.items()
        }
        if any(not item or any(ord(c) < 32 for c in item) for item in result.values()):
            raise ValueError
        return result
    except (KeyError, TypeError, ValueError, UnicodeError, binascii.Error):
        raise BootstrapError("canonical Secret is incomplete or invalid") from None


def _validate_owned(document: dict[str, Any], name: str) -> dict[str, str]:
    values = _data(document, name=name)
    labels = document.get("metadata", {}).get("labels", {})
    if labels.get(OWNER_LABEL) != OWNER_VALUE:
        raise BootstrapError("canonical identity Secret is not owned by this bootstrap")
    if name in DB_SECRETS:
        if (
            document.get("type") != "kubernetes.io/basic-auth"
            or labels.get("cnpg.io/reload") != "true"
            or set(values) != {"username", "password"}
            or values["username"] != DB_SECRETS[name]
            or re.fullmatch(r"[a-f0-9]{64}", values["password"]) is None
        ):
            raise BootstrapError("canonical database identity Secret is invalid")
    elif name == MINIO_SECRET:
        if (
            document.get("type") != "Opaque"
            or set(values) != {"access-key", "secret-key"}
            or values["access-key"] != MINIO_USER
            or re.fullmatch(r"[a-f0-9]{64}", values["secret-key"]) is None
        ):
            raise BootstrapError("canonical input-store identity Secret is invalid")
    elif (
        document.get("type") != "Opaque"
        or set(values) != {"token"}
        or re.fullmatch(r"loom_ecc_[a-f0-9]{64}", values["token"]) is None
    ):
        raise BootstrapError("canonical collector identity Secret is invalid")
    return values


def _new_secret(name: str) -> dict[str, Any]:
    password = secrets.token_hex(32)
    labels = {OWNER_LABEL: OWNER_VALUE}
    secret_type = "Opaque"
    if name in DB_SECRETS:
        values = {"username": DB_SECRETS[name], "password": password}
        labels["cnpg.io/reload"] = "true"
        secret_type = "kubernetes.io/basic-auth"
    elif name == MINIO_SECRET:
        values = {"access-key": MINIO_USER, "secret-key": password}
    else:
        values = {"token": "loom_ecc_" + password}
    return {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {"name": name, "namespace": NAMESPACE, "labels": labels},
        "type": secret_type,
        "data": {key: base64.b64encode(value.encode()).decode() for key, value in values.items()},
    }


def _ensure_secret(adapter: BootstrapAdapter, name: str) -> tuple[dict[str, str], bool]:
    existing = adapter.get_secret(NAMESPACE, name)
    created = False
    if existing is None:
        created = adapter.create_secret(_new_secret(name))
        existing = adapter.get_secret(NAMESPACE, name)
    if existing is None:
        raise BootstrapError("canonical identity Secret persistence was not confirmed")
    return _validate_owned(existing, name), created


def _minio_host(endpoint: str, root: dict[str, str]) -> str:
    try:
        parsed = urlsplit(endpoint)
        if (
            parsed.scheme not in {"http", "https"}
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or parsed.path not in {"", "/"}
            or parsed.query
            or parsed.fragment
            or parsed.port == 0
            or any(c.isspace() for c in endpoint)
        ):
            raise ValueError
        # Plain HTTP is only useful inside the canonical cluster or a local
        # port-forward. Never send root credentials to a caller's public origin.
        if parsed.scheme == "http" and parsed.hostname not in {
            "127.0.0.1",
            "localhost",
            "::1",
            "loom-minio",
            "loom-minio.loom-staging",
            "loom-minio.loom-staging.svc",
            "loom-minio.loom-staging.svc.cluster.local",
        }:
            raise ValueError
        user = quote(root["minio-access-key"], safe="")
        password = quote(root["minio-secret-key"], safe="")
        return urlunsplit((parsed.scheme, f"{user}:{password}@{parsed.netloc}", "", "", ""))
    except (ValueError, KeyError):
        raise BootstrapError("canonical MinIO endpoint or root identity is invalid") from None


def _mc(
    runner: CommandRunner,
    binary: str,
    args: Sequence[str],
    env: Mapping[str, str],
    *,
    payload: bytes | None = None,
    allow_missing_user: bool = False,
    resolve_rule: str | None = None,
) -> dict[str, Any] | None:
    try:
        result = runner(
            [binary, "--json", *(["--resolve", resolve_rule] if resolve_rule else []), *args],
            env=env,
            input=payload,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=60,
            check=False,
        )
        data = json.loads(result.stdout)
        if not isinstance(data, dict):
            raise ValueError
        if result.returncode != 0 or data.get("status") != "success":
            error = data.get("error", {})
            cause = error.get("cause", {}) if isinstance(error, dict) else {}
            if allow_missing_user and isinstance(cause, dict):
                if cause.get("error", {}).get("Code") == "XMinioAdminNoSuchUser":
                    return None
            raise ValueError
        return data
    except Exception:
        raise BootstrapError("canonical MinIO identity reconciliation failed") from None


def _minio_resolve_rule(endpoint: str, address: str | None) -> str | None:
    """Route only this HTTPS origin through mc's native resolver override."""
    if address is None:
        return None
    try:
        parsed = urlsplit(endpoint)
        hostname = parsed.hostname or ""
        if (
            parsed.scheme != "https"
            or parsed.username is not None
            or parsed.password is not None
            or parsed.path not in {"", "/"}
            or parsed.query
            or parsed.fragment
            or any(c.isspace() for c in endpoint)
            or parsed.port == 0
            or not hostname
            or len(hostname) > 253
            or not all(
                re.fullmatch(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", label)
                for label in hostname.split(".")
            )
        ):
            raise ValueError
        try:
            ipaddress.ip_address(hostname)
        except ValueError:
            pass
        else:
            raise ValueError
        target = ipaddress.IPv4Address(address)
        if not any(
            target in ipaddress.IPv4Network(network)
            for network in ("10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16")
        ):
            raise ValueError
        return f"{hostname}:{parsed.port or 443}={target}"
    except (TypeError, ValueError):
        raise BootstrapError("canonical MinIO private route is invalid") from None


def _reconcile_minio(
    values: dict[str, str],
    root: dict[str, str],
    *,
    endpoint: str,
    binary: str,
    runner: CommandRunner,
    resolve_address: str | None = None,
) -> None:
    resolve_rule = _minio_resolve_rule(endpoint, resolve_address)
    with tempfile.TemporaryDirectory(prefix="loom-nebius-identity-") as directory:
        os.chmod(directory, 0o700)
        policy = Path(directory) / "input-policy.json"
        descriptor = os.open(policy, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "w") as stream:
            json.dump(INPUT_POLICY, stream)
        # Only mc receives root credentials. No shell, args, inherited MC_HOST
        # aliases, or persisted mc config is used. Both output streams captured.
        environment = {
            "PATH": os.environ.get("PATH", os.defpath),
            "MC_HOST_loom": _minio_host(endpoint, root),
            "MC_CONFIG_DIR": directory,
            "MC_NO_COLOR": "1",
        }
        info = _mc(
            runner,
            binary,
            ["admin", "user", "info", "loom", MINIO_USER],
            environment,
            allow_missing_user=True,
            resolve_rule=resolve_rule,
        )
        if info is not None and info.get("userStatus") != "enabled":
            raise BootstrapError("canonical MinIO input identity is disabled")
        if info is not None and (
            info.get("policyName", "") not in {"", MINIO_POLICY} or info.get("memberOf")
        ):
            raise BootstrapError("canonical MinIO input identity has unrelated access")
        _mc(
            runner,
            binary,
            ["admin", "user", "add", "loom"],
            environment,
            payload=f"{MINIO_USER}\n{values['secret-key']}\n".encode(),
            resolve_rule=resolve_rule,
        )
        _mc(
            runner,
            binary,
            ["admin", "policy", "create", "loom", MINIO_POLICY, str(policy)],
            environment,
            resolve_rule=resolve_rule,
        )
        if info is None or info.get("policyName") != MINIO_POLICY:
            _mc(
                runner,
                binary,
                ["admin", "policy", "attach", "loom", MINIO_POLICY, "--user", MINIO_USER],
                environment,
                resolve_rule=resolve_rule,
            )
        info = _mc(
            runner,
            binary,
            ["admin", "user", "info", "loom", MINIO_USER],
            environment,
            resolve_rule=resolve_rule,
        )
        if (
            info is None
            or info.get("userStatus") != "enabled"
            or info.get("policyName") != MINIO_POLICY
            or info.get("memberOf")
        ):
            raise BootstrapError("canonical MinIO input identity access did not converge")


def seed_identities(adapter: BootstrapAdapter) -> dict[str, int]:
    """Prepare only stable canonical Secrets before the protected CNPG rollout."""
    try:
        created = 0
        for name in (*DB_SECRETS, MINIO_SECRET, COLLECTOR_SECRET):
            _, is_new = _ensure_secret(adapter, name)
            created += int(is_new)
        return {
            "secrets_created": created,
            "secrets_verified": 4,
            "database_role_secrets_verified": 2,
        }
    except BootstrapError:
        raise
    except Exception:
        raise BootstrapError("canonical staging identity seed failed") from None


def bootstrap_identities(
    adapter: BootstrapAdapter,
    *,
    minio_endpoint: str,
    minio_resolve: str | None = None,
    mc_binary: str = "mc",
    run: CommandRunner | None = None,
) -> dict[str, int]:
    """Reconcile only fixed Loom-owned staging identities; return safe counts.

    All generated secrets are create-only and read back before external writes.
    A partially populated, foreign, or revoked existing identity fails closed.
    The caller must serialize bootstrap with its supported staging rollout.
    minio_resolve is an optional RFC1918 IPv4 literal for the canonical private
    listener. mc receives HOST:PORT=IP while the HTTPS hostname and certificate
    verification remain unchanged; no machine DNS or hosts files are modified.
    """
    try:
        _minio_resolve_rule(minio_endpoint, minio_resolve)
        seeded = seed_identities(adapter)
        values = {}
        for name in (MINIO_SECRET, COLLECTOR_SECRET):
            document = adapter.get_secret(NAMESPACE, name)
            if document is None:
                raise BootstrapError("canonical identity Secret disappeared")
            values[name] = _validate_owned(document, name)
        root_document = adapter.get_secret(NAMESPACE, "loom-secrets")
        if root_document is None:
            raise BootstrapError("canonical MinIO root identity is unavailable")
        root = _data(
            root_document, name="loom-secrets", keys={"minio-access-key", "minio-secret-key"}
        )
        _reconcile_minio(
            values[MINIO_SECRET],
            root,
            endpoint=minio_endpoint,
            binary=mc_binary,
            runner=run if run is not None else cast(CommandRunner, subprocess.run),
            resolve_address=minio_resolve,
        )
        reply = adapter.exec_control_plane(
            COLLECTOR_REGISTER_SOURCE, values[COLLECTOR_SECRET]["token"].encode()
        )
        result = json.loads(reply)
        if (
            not isinstance(result, dict)
            or set(result) != {"collector_created", "collector_verified"}
            or type(result["collector_created"]) is not int
            or result["collector_created"] not in {0, 1}
            or type(result["collector_verified"]) is not int
            or result["collector_verified"] != 1
        ):
            raise BootstrapError("canonical collector identity registration was not confirmed")
        return {**seeded, "minio_identity_verified": 1, **result}
    except BootstrapError:
        raise
    except Exception:
        raise BootstrapError("canonical staging identity bootstrap failed") from None
