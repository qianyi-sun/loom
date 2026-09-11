"""Loom security primitives.

The :class:`~loom.security.secret_store.SecretStore` Protocol abstracts
encrypted-secret storage so the gateway / control-plane / admin tools
don't need to know whether secrets live in a Postgres-backed
AES-GCM store (the default, used by both ``loom service`` and the data
path of ``loom cluster``) or in a k8s Secret (used for bootstrap
infra credentials in cluster mode).
"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from loom.security.secret_store import (
        LocalEncryptedSecretStore,
        SecretNotFoundError,
        SecretStore,
        SecretStoreError,
        parse_ref,
    )

__all__ = [
    "LocalEncryptedSecretStore",
    "SecretNotFoundError",
    "SecretStore",
    "SecretStoreError",
    "parse_ref",
]


def __getattr__(name: str) -> object:
    if name in __all__:
        from loom.security import secret_store

        value = getattr(secret_store, name)
        globals()[name] = value
        return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
