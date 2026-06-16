"""SecretStore Protocol + the local-encrypted Postgres-backed impl.

Spec: docs/architecture/cluster-deploy.md §Secrets, SSRF, gateway hot path.

The Protocol lets gateway code stay backend-agnostic — it does not need
to know whether a given ref points at an AES-GCM-encrypted Postgres
row (``loom://...``) or a k8s Secret (``k8s://...``). The
:func:`dispatch_for_ref` function routes by URL scheme; the k8s-secret
backend lands in a follow-up PR.

Why async: the gateway is FastAPI; sync DB calls would block the event
loop. asyncpg via SQLAlchemy 2.0's async session is the existing
convention (see ``loom_service/dependencies.py``).

Why AES-GCM: authenticated encryption with associated data. Tampering
with the ciphertext, the nonce, or the master-key version produces
``InvalidTag`` at decrypt time rather than silently returning bad bytes.

Master key: 32 bytes (AES-256). Loaded once at construction from the
operator-supplied ``LOOM_SECRET_STORE_MASTER_KEY`` env var, base64-encoded.
Rotation is a Phase 5 concern; the ``master_key_version`` column on the
``secrets`` row records which key generation encrypted each row so the
rotation walker can decrypt with the historic key and re-encrypt with
the current one.
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Protocol, runtime_checkable
from uuid import UUID, uuid4

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from loom.db.schema import Secret


class SecretStoreError(Exception):
    """Base class for SecretStore errors."""


class SecretNotFoundError(SecretStoreError):
    """get(ref) was called on a ref that doesn't exist (or was deleted)."""


class InvalidRefError(SecretStoreError):
    """ref doesn't parse as a valid loom://<namespace>/<uuid> URL."""


class DecryptError(SecretStoreError):
    """Ciphertext failed AEAD verification — wrong key, tampered ciphertext,
    or wrong master_key_version with no prev-key configured."""


@dataclass(frozen=True)
class ParsedRef:
    """A ``loom://<namespace>/<uuid>`` ref decomposed into its parts.

    Refs are intentionally opaque to callers (just strings). This
    dataclass is exposed for the rotation walker + admin tools that
    need to list / filter by namespace; ordinary callers should treat
    refs as cookies and pass them around unmodified.
    """
    scheme: str
    namespace: str
    secret_id: UUID

    def as_string(self) -> str:
        return f"{self.scheme}://{self.namespace}/{self.secret_id}"


def parse_ref(ref: str) -> ParsedRef:
    """Parse a ``loom://<namespace>/<uuid>`` ref. Validates the scheme,
    namespace presence, and UUID format. Raises :class:`InvalidRefError`
    on any malformed input — callers should treat this as a 400 if it
    came from user-supplied data, or as a 500 if it came from a row
    we wrote (DB corruption).
    """
    if "://" not in ref:
        raise InvalidRefError(f"ref missing scheme separator: {ref!r}")
    scheme, rest = ref.split("://", 1)
    if scheme != "loom":
        raise InvalidRefError(
            f"unsupported ref scheme {scheme!r}; expected 'loom' "
            f"(other schemes route to a different SecretStore impl)",
        )
    if "/" not in rest:
        raise InvalidRefError(
            f"ref missing namespace/uuid separator: {ref!r}",
        )
    namespace, uuid_str = rest.rsplit("/", 1)
    if not namespace:
        raise InvalidRefError(f"ref namespace is empty: {ref!r}")
    try:
        secret_id = UUID(uuid_str)
    except ValueError as e:
        raise InvalidRefError(f"ref uuid does not parse: {ref!r}") from e
    return ParsedRef(scheme=scheme, namespace=namespace, secret_id=secret_id)


@runtime_checkable
class SecretStore(Protocol):
    """Async Protocol for encrypted-secret storage.

    Implementations:

    - :class:`LocalEncryptedSecretStore` — AES-GCM ciphertext in the
      ``secrets`` Postgres table. Default in both ``loom service`` and
      ``loom cluster`` for the user-API-key data path.
    - k8s-secret (separate PR) — one k8s Secret per ref. Default in
      ``loom cluster`` for bootstrap-supplied infra creds.
    """

    async def put(self, *, namespace: str, value: str) -> str:
        """Encrypt + persist ``value``; return the opaque ref the caller
        stores. Idempotency: every call generates a fresh ref (new UUID
        + new nonce), so two puts of the same value yield different refs.
        Namespacing is for query/audit grouping (typically the team_id
        as a string); it doesn't affect encryption.
        """
        ...

    async def get(self, ref: str) -> str:
        """Decrypt + return the secret value. Raises
        :class:`SecretNotFoundError` if ref is unknown,
        :class:`DecryptError` if the ciphertext fails AEAD verification.
        """
        ...

    async def delete(self, ref: str) -> None:
        """Remove the secret. Idempotent — deleting an unknown ref is
        a no-op (no exception). Rationale: simplifies caller code in
        rollback paths (``DELETE provider_connection`` → ``DELETE
        secret`` doesn't care if the secret was never created).
        """
        ...

    def list_refs(
        self, *, namespace: str | None = None,
    ) -> AsyncIterator[str]:
        """Yield every ref the store knows about, optionally filtered
        by namespace. For the rotation walker + admin audit tools.
        Implementations stream rather than materialize so an
        unbounded secrets table doesn't blow memory.
        """
        ...

    async def rewrap(self, ref: str, *, new_master_key: bytes) -> str:
        """Decrypt with the current master key, re-encrypt with the
        supplied new master key, persist. Returns the ref — for
        ``local-encrypted`` the ref is stable (only ciphertext +
        master_key_version change); for k8s-secret it's a no-op
        (etcd encryption handles at-rest rotation) and returns the
        same ref unchanged.
        """
        ...


_MASTER_KEY_LEN = 32  # AES-256
_NONCE_LEN = 12       # AES-GCM standard


def _load_master_key_from_env(env_var: str = "LOOM_SECRET_STORE_MASTER_KEY") -> bytes:
    """Load + validate a 32-byte AES-256 master key from a base64-encoded
    env var. Fail-fast if missing / wrong length / not base64.
    """
    import base64

    raw = os.environ.get(env_var)
    if not raw:
        raise SecretStoreError(
            f"{env_var} not set. Generate with: "
            f"python -c 'import os, base64; "
            f"print(base64.b64encode(os.urandom(32)).decode())'",
        )
    try:
        key = base64.b64decode(raw, validate=True)
    except (ValueError, base64.binascii.Error) as e:  # type: ignore[attr-defined]
        raise SecretStoreError(
            f"{env_var} is not valid base64: {e}",
        ) from e
    if len(key) != _MASTER_KEY_LEN:
        raise SecretStoreError(
            f"{env_var} decodes to {len(key)} bytes; expected "
            f"{_MASTER_KEY_LEN} (AES-256)",
        )
    return key


class LocalEncryptedSecretStore:
    """Postgres-backed AES-GCM SecretStore.

    Refs have the format ``loom://<namespace>/<uuid>``. The UUID is
    generated per-put; the namespace is operator-supplied (typically
    ``team:<team_id>`` for user API keys).

    ``master_key_version`` lets the rotation walker (Phase 5) identify
    which key generation encrypted each row. This PR's impl supports
    one current key — the rotation cutover with prev_master_key is a
    Phase 5 concern (covered by the dual-key validation window in the
    spec).

    Thread/coroutine safety: stateless apart from the loaded master key.
    Safe to share one instance across the FastAPI worker process.
    """

    def __init__(
        self,
        session: AsyncSession,
        *,
        master_key: bytes | None = None,
        master_key_version: int = 1,
    ) -> None:
        if master_key is None:
            master_key = _load_master_key_from_env()
        if len(master_key) != _MASTER_KEY_LEN:
            raise SecretStoreError(
                f"master_key length {len(master_key)} != {_MASTER_KEY_LEN}",
            )
        self._session = session
        self._aead = AESGCM(master_key)
        self._master_key_version = master_key_version

    async def put(self, *, namespace: str, value: str) -> str:
        if not namespace:
            raise InvalidRefError("namespace must be non-empty")
        secret_id = uuid4()
        ref = f"loom://{namespace}/{secret_id}"
        nonce = os.urandom(_NONCE_LEN)
        # Bind the ref as Additional Authenticated Data (AAD) so a row
        # whose ciphertext was copy-pasted into a different ref slot
        # fails decryption. Defense against a future bug where ref-vs-row
        # gets shuffled.
        ciphertext = self._aead.encrypt(
            nonce, value.encode("utf-8"), associated_data=ref.encode("utf-8"),
        )
        secret = Secret(
            ref=ref,
            ciphertext=ciphertext,
            nonce=nonce,
            master_key_version=self._master_key_version,
        )
        self._session.add(secret)
        await self._session.flush()
        return ref

    async def get(self, ref: str) -> str:
        parse_ref(ref)  # validates format; raises InvalidRefError
        row = (await self._session.execute(
            select(Secret).where(Secret.ref == ref),
        )).scalar_one_or_none()
        if row is None:
            raise SecretNotFoundError(f"no secret for ref {ref!r}")
        if row.master_key_version != self._master_key_version:
            raise DecryptError(
                f"secret {ref!r} encrypted with master_key_version="
                f"{row.master_key_version}, but store is configured for "
                f"version {self._master_key_version}. Rotation walker "
                f"(Phase 5) re-encrypts on demand.",
            )
        try:
            plaintext = self._aead.decrypt(
                row.nonce, row.ciphertext,
                associated_data=ref.encode("utf-8"),
            )
        except InvalidTag as e:
            raise DecryptError(
                f"AEAD verification failed for {ref!r}; ciphertext, "
                f"nonce, or master key has changed",
            ) from e
        return plaintext.decode("utf-8")

    async def delete(self, ref: str) -> None:
        # Validate format (catches typos at the API boundary) but
        # treat row-not-found as success — delete-on-missing is the
        # standard idempotent contract.
        parse_ref(ref)
        await self._session.execute(
            delete(Secret).where(Secret.ref == ref),
        )
        await self._session.flush()

    async def list_refs(
        self, *, namespace: str | None = None,
    ) -> AsyncIterator[str]:
        stmt = select(Secret.ref)
        if namespace is not None:
            # ref format is `loom://<namespace>/<uuid>`, so the namespace
            # filter is a prefix match on `loom://<namespace>/`.
            prefix = f"loom://{namespace}/"
            stmt = stmt.where(Secret.ref.startswith(prefix))
        result = await self._session.stream(stmt)
        async for row in result:
            yield row[0]

    async def rewrap(self, ref: str, *, new_master_key: bytes) -> str:
        """Re-encrypt the secret with ``new_master_key``. The ref is
        stable (returned unchanged); the row's ciphertext + nonce +
        master_key_version are updated in place inside the caller's
        transaction.

        Version bump: the new row's ``master_key_version`` is
        ``self._master_key_version + 1``. The walker that drives
        rotation MUST be invoked against a store configured with the
        OLD version (so get() decrypts the existing row), and the
        resulting row carries version+1. For multi-step rotation,
        reconstruct the store with the new (key, version) pair before
        the next rewrap call.

        The caller is responsible for sequencing this with the
        gateway-cache invalidation (the rotation walker bumps every
        affected ``provider_connections.updated_at`` afterwards so the
        ``updated_at``-keyed cache pattern observes the change).
        """
        if len(new_master_key) != _MASTER_KEY_LEN:
            raise SecretStoreError(
                f"new_master_key length {len(new_master_key)} != "
                f"{_MASTER_KEY_LEN}",
            )
        plaintext = await self.get(ref)
        new_aead = AESGCM(new_master_key)
        new_nonce = os.urandom(_NONCE_LEN)
        new_ciphertext = new_aead.encrypt(
            new_nonce, plaintext.encode("utf-8"),
            associated_data=ref.encode("utf-8"),
        )
        # In-place UPDATE preserves the ref string the consuming row
        # (e.g., provider_connections.encrypted_api_key_ref) already
        # holds.
        row = (await self._session.execute(
            select(Secret).where(Secret.ref == ref),
        )).scalar_one()
        row.ciphertext = new_ciphertext
        row.nonce = new_nonce
        row.master_key_version = self._master_key_version + 1
        await self._session.flush()
        return ref
