"""Loom security primitives.

The :class:`~loom.security.secret_store.SecretStore` Protocol abstracts
encrypted-secret storage so the gateway / control-plane / admin tools
don't need to know whether secrets live in a Postgres-backed
AES-GCM store (the default, used by both ``loom service`` and the data
path of ``loom cluster``) or in a k8s Secret (used for bootstrap
infra credentials in cluster mode).
"""

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
