"""Least-authority runtime configuration for staging lifecycle tools.

Lifecycle maintenance needs one database credential and, for object-aware
operations, one narrowly scoped S3 credential.  It must not load the full
control-plane settings because doing so couples read-only inventory to JWT,
provider, and unrelated service secrets.

Secret-bearing values are accepted only through ``LOOM_LIFECYCLE_*``
environment variables.  They are excluded from representations and never
included in validation errors.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlsplit

from sqlalchemy import Engine, create_engine

from loom.storage_credentials import SUPPORTED_AUTH_KINDS, build_s3_client

_ENV_PREFIX = "LOOM_LIFECYCLE_"


def _read_required(environment: Mapping[str, str], name: str) -> str:
    value = environment.get(f"{_ENV_PREFIX}{name}", "")
    if not value or value != value.strip() or "\x00" in value:
        raise RuntimeError(f"required lifecycle environment {name} is unavailable")
    return value


def _read_optional(
    environment: Mapping[str, str],
    name: str,
    *,
    default: str | None = None,
) -> str | None:
    value = environment.get(f"{_ENV_PREFIX}{name}")
    if value is None:
        return default
    if not value or value != value.strip() or "\x00" in value:
        raise RuntimeError(f"optional lifecycle environment {name} is invalid")
    return value


@dataclass(frozen=True)
class LifecycleDatabaseRuntime:
    """One direct SQLAlchemy database URL, hidden from diagnostics."""

    url: str = field(repr=False)


@dataclass(frozen=True)
class LifecycleObjectStoreRuntime:
    """One exact S3 endpoint and its dedicated lifecycle authority."""

    endpoint_url: str
    auth_kind: str
    access_key: str | None = field(default=None, repr=False)
    secret_key: str | None = field(default=None, repr=False)
    region: str = "us-east-1"


@dataclass(frozen=True)
class LifecycleRuntime:
    """Complete DB + object-store runtime for lifecycle inventory and GC."""

    database: LifecycleDatabaseRuntime
    object_store: LifecycleObjectStoreRuntime


def load_lifecycle_database_runtime(
    environment: Mapping[str, str] | None = None,
) -> LifecycleDatabaseRuntime:
    source = os.environ if environment is None else environment
    return LifecycleDatabaseRuntime(url=_read_required(source, "DB_URL"))


def load_lifecycle_object_store_runtime(
    environment: Mapping[str, str] | None = None,
) -> LifecycleObjectStoreRuntime:
    source = os.environ if environment is None else environment
    endpoint_url = _read_required(source, "MINIO_ENDPOINT")
    parsed_endpoint = urlsplit(endpoint_url)
    if (
        parsed_endpoint.scheme not in {"http", "https"}
        or not parsed_endpoint.hostname
        or parsed_endpoint.username is not None
        or parsed_endpoint.password is not None
        or parsed_endpoint.query
        or parsed_endpoint.fragment
    ):
        raise RuntimeError("lifecycle MINIO_ENDPOINT is not a credential-free HTTP URL")

    auth_kind = _read_optional(source, "STORAGE_AUTH_KIND", default="static_keys")
    assert auth_kind is not None
    if auth_kind not in SUPPORTED_AUTH_KINDS:
        raise RuntimeError("lifecycle STORAGE_AUTH_KIND is unsupported")
    access_key = _read_optional(source, "MINIO_ACCESS_KEY")
    secret_key = _read_optional(source, "MINIO_SECRET_KEY")
    if auth_kind == "static_keys" and (access_key is None or secret_key is None):
        raise RuntimeError("lifecycle static object-store credentials are unavailable")
    region = _read_optional(source, "MINIO_REGION", default="us-east-1")
    assert region is not None
    return LifecycleObjectStoreRuntime(
        endpoint_url=endpoint_url,
        auth_kind=auth_kind,
        access_key=access_key,
        secret_key=secret_key,
        region=region,
    )


def load_lifecycle_runtime(
    environment: Mapping[str, str] | None = None,
) -> LifecycleRuntime:
    return LifecycleRuntime(
        database=load_lifecycle_database_runtime(environment),
        object_store=load_lifecycle_object_store_runtime(environment),
    )


def build_lifecycle_engine(database: LifecycleDatabaseRuntime) -> Engine:
    """Create the direct lifecycle engine without importing service config."""

    return create_engine(database.url)


def build_lifecycle_object_store_client(
    object_store: LifecycleObjectStoreRuntime,
) -> Any:
    return build_s3_client(
        endpoint_url=object_store.endpoint_url,
        auth_kind=object_store.auth_kind,
        access_key=object_store.access_key,
        secret_key=object_store.secret_key,
        region=object_store.region,
    )


__all__ = [
    "LifecycleDatabaseRuntime",
    "LifecycleObjectStoreRuntime",
    "LifecycleRuntime",
    "build_lifecycle_engine",
    "build_lifecycle_object_store_client",
    "load_lifecycle_database_runtime",
    "load_lifecycle_object_store_runtime",
    "load_lifecycle_runtime",
]
