"""Canonical per-instance capacity role and runtime database identity."""

from __future__ import annotations

from sqlalchemy.engine import make_url

from loom.dev_instance import DevInstanceIdentity

PROTECTED_WORKER_RUNTIME_SECRET_NAME = "loom-protected-worker-runtime"


class CapacityRuntimeCredentialError(ValueError):
    """A protected runtime database URL is malformed or targets another instance."""


def capacity_role_names(identity: DevInstanceIdentity) -> tuple[str, str, str, str, str, str]:
    slug = identity.name.replace("-", "_")
    return (
        f"loom_cap_{slug}_owner",
        f"loom_cap_{slug}_migrator",
        f"loom_cap_{slug}_agent",
        f"loom_cap_{slug}_executor",
        f"loom_cap_{slug}_observer",
        f"loom_cap_{slug}_runtime",
    )


def capacity_runtime_database_url(
    admin_url: str,
    identity: DevInstanceIdentity,
    password: str,
) -> str:
    runtime_role = capacity_role_names(identity)[-1]
    return (
        make_url(admin_url)
        .set(
            drivername="postgresql+psycopg",
            database=identity.database,
            username=runtime_role,
            password=password,
        )
        .render_as_string(hide_password=False)
    )


def capacity_runtime_database_password(
    value: str,
    identity: DevInstanceIdentity,
) -> str:
    try:
        parsed = make_url(value)
    except Exception:
        raise CapacityRuntimeCredentialError(
            "protected worker runtime database credential is invalid"
        ) from None
    expected_role = capacity_role_names(identity)[-1]
    password = parsed.password
    if (
        parsed.drivername != "postgresql+psycopg"
        or parsed.username != expected_role
        or parsed.database != identity.database
        or not parsed.host
        or not password
    ):
        raise CapacityRuntimeCredentialError(
            "protected worker runtime database credential is invalid"
        )
    try:
        encoded = password.encode("ascii")
    except UnicodeEncodeError:
        raise CapacityRuntimeCredentialError(
            "protected worker runtime database credential is invalid"
        ) from None
    if not 32 <= len(encoded) <= 1024 or any(not 0x21 <= byte <= 0x7E for byte in encoded):
        raise CapacityRuntimeCredentialError(
            "protected worker runtime database credential is invalid"
        )
    return password


__all__ = [
    "PROTECTED_WORKER_RUNTIME_SECRET_NAME",
    "CapacityRuntimeCredentialError",
    "capacity_role_names",
    "capacity_runtime_database_password",
    "capacity_runtime_database_url",
]
