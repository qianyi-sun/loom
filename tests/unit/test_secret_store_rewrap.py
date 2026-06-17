"""Unit tests for SecretStore multi-key rotation (approach C).

Tests:
- rewrap with new key re-encrypts the row
- a value encrypted under a fallback key is still readable
- a value rewrapped to the primary key is then readable by primary only
- fallback key with wrong bytes raises DecryptError (tamper detection)
- unknown master_key_version raises DecryptError
- rewrap of corrupt ciphertext fails gracefully (DecryptError)
- load_master_keys_from_env: singular, plural, both-set error, unset error
- LocalEncryptedSecretStore version assignment: primary=highest, fallbacks=lower
"""

from __future__ import annotations

import base64
import os
from collections.abc import AsyncIterator

import pytest

from loom.security.secret_store import (
    _MASTER_KEY_LEN,
    DecryptError,
    LocalEncryptedSecretStore,
    SecretStoreError,
    _decode_b64_key,
    load_master_keys_from_env,
)


def _make_key() -> bytes:
    """Generate a random 32-byte AES-256 key."""
    return os.urandom(_MASTER_KEY_LEN)


def _b64(key: bytes) -> str:
    return base64.b64encode(key).decode()


# ── load_master_keys_from_env ─────────────────────────────────────────


def test_load_singular_key(monkeypatch: pytest.MonkeyPatch) -> None:
    key = _make_key()
    monkeypatch.setenv("LOOM_SECRET_STORE_MASTER_KEY", _b64(key))
    monkeypatch.delenv("LOOM_SECRET_STORE_MASTER_KEYS", raising=False)
    primary, version, fallbacks = load_master_keys_from_env()
    assert primary == key
    assert version == 1
    assert fallbacks == {}


def test_load_plural_single_entry(monkeypatch: pytest.MonkeyPatch) -> None:
    key = _make_key()
    monkeypatch.setenv("LOOM_SECRET_STORE_MASTER_KEYS", _b64(key))
    monkeypatch.delenv("LOOM_SECRET_STORE_MASTER_KEY", raising=False)
    primary, version, fallbacks = load_master_keys_from_env()
    assert primary == key
    assert version == 1
    assert fallbacks == {}


def test_load_plural_two_entries(monkeypatch: pytest.MonkeyPatch) -> None:
    new_key = _make_key()
    old_key = _make_key()
    monkeypatch.setenv(
        "LOOM_SECRET_STORE_MASTER_KEYS",
        f"{_b64(new_key)},{_b64(old_key)}",
    )
    monkeypatch.delenv("LOOM_SECRET_STORE_MASTER_KEY", raising=False)
    primary, version, fallbacks = load_master_keys_from_env()
    assert primary == new_key
    assert version == 2          # primary gets highest version
    assert fallbacks == {1: old_key}  # old key is version 1


def test_load_plural_three_entries(monkeypatch: pytest.MonkeyPatch) -> None:
    k3, k2, k1 = _make_key(), _make_key(), _make_key()
    monkeypatch.setenv(
        "LOOM_SECRET_STORE_MASTER_KEYS",
        f"{_b64(k3)},{_b64(k2)},{_b64(k1)}",
    )
    monkeypatch.delenv("LOOM_SECRET_STORE_MASTER_KEY", raising=False)
    primary, version, fallbacks = load_master_keys_from_env()
    assert primary == k3
    assert version == 3
    assert fallbacks == {2: k2, 1: k1}


def test_both_env_vars_set_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    key = _make_key()
    monkeypatch.setenv("LOOM_SECRET_STORE_MASTER_KEY", _b64(key))
    monkeypatch.setenv("LOOM_SECRET_STORE_MASTER_KEYS", _b64(key))
    with pytest.raises(SecretStoreError, match=r"Both.*are set"):
        load_master_keys_from_env()


def test_neither_env_var_set_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("LOOM_SECRET_STORE_MASTER_KEY", raising=False)
    monkeypatch.delenv("LOOM_SECRET_STORE_MASTER_KEYS", raising=False)
    with pytest.raises(SecretStoreError, match="Neither"):
        load_master_keys_from_env()


def test_decode_b64_key_wrong_length() -> None:
    short = base64.b64encode(b"tooshort").decode()
    with pytest.raises(SecretStoreError, match="8 bytes"):
        _decode_b64_key(short, "test")


def test_decode_b64_key_invalid_base64() -> None:
    with pytest.raises(SecretStoreError, match="not valid base64"):
        _decode_b64_key("!!!not-base64!!!", "test")


# ── LocalEncryptedSecretStore multi-key integration ────────────────────
# These tests use an in-memory fake session to avoid needing Postgres.

class _FakeRow:
    def __init__(self, ref: str, ciphertext: bytes, nonce: bytes, version: int) -> None:
        self.ref = ref
        self.ciphertext = ciphertext
        self.nonce = nonce
        self.master_key_version = version


class _FakeSession:
    """Minimal async session fake for unit testing SecretStore internals."""

    def __init__(self) -> None:
        self._rows: dict[str, _FakeRow] = {}
        self._added: list[object] = []

    # SQLAlchemy add() for inserts
    def add(self, obj: object) -> None:
        from loom.db.schema import Secret
        assert isinstance(obj, Secret)
        row = _FakeRow(
            ref=obj.ref,
            ciphertext=obj.ciphertext,
            nonce=obj.nonce,
            version=obj.master_key_version,
        )
        self._rows[obj.ref] = row

    async def flush(self) -> None:
        pass

    async def execute(self, stmt: object) -> object:
        # Introspect the statement to figure out what to return.
        # We do this by inspecting the compiled string — fragile but
        # sufficient for unit tests that don't need a real DB.
        compiled = str(stmt.compile(compile_kwargs={"literal_binds": True}))  # type: ignore[union-attr]
        if "DELETE" in compiled:
            # delete by ref
            ref = self._extract_ref_from_compiled(compiled)
            if ref and ref in self._rows:
                del self._rows[ref]
            return _FakeScalar(None)
        # SELECT
        ref = self._extract_ref_from_compiled(compiled)
        if ref:
            row = self._rows.get(ref)
            return _FakeScalar(row)
        # list_refs — handled separately in stream()
        return _FakeScalar(None)

    def _extract_ref_from_compiled(self, compiled: str) -> str | None:
        import re
        m = re.search(r"'(loom://[^']+)'", compiled)
        return m.group(1) if m else None

    async def stream(self, stmt: object) -> AsyncIterator[tuple[str]]:
        for ref in list(self._rows):
            yield (ref,)

    async def commit(self) -> None:
        pass


class _FakeScalar:
    def __init__(self, value: object) -> None:
        self._value = value

    def scalar_one_or_none(self) -> object:
        return self._value

    def scalar_one(self) -> object:
        if self._value is None:
            raise Exception("no row")
        return self._value


@pytest.fixture
def fake_session() -> _FakeSession:
    return _FakeSession()


@pytest.mark.asyncio
async def test_put_and_get_single_key(fake_session: _FakeSession) -> None:
    key = _make_key()
    store = LocalEncryptedSecretStore(
        fake_session,  # type: ignore[arg-type]
        master_key=key,
        master_key_version=1,
    )
    ref = await store.put(namespace="team:abc", value="sk-secret-value")
    assert ref.startswith("loom://team:abc/")
    assert await store.get(ref) == "sk-secret-value"


@pytest.mark.asyncio
async def test_fallback_key_decrypts_old_row(fake_session: _FakeSession) -> None:
    """Row encrypted with old_key (v1) is readable by store with new_key (v2)
    + old_key as fallback."""
    old_key = _make_key()
    new_key = _make_key()

    # Write with old key (v1).
    old_store = LocalEncryptedSecretStore(
        fake_session,  # type: ignore[arg-type]
        master_key=old_key,
        master_key_version=1,
    )
    ref = await old_store.put(namespace="team:abc", value="secret-value")

    # Read with new primary key + old as fallback.
    new_store = LocalEncryptedSecretStore(
        fake_session,  # type: ignore[arg-type]
        master_key=new_key,
        master_key_version=2,
        fallback_keys={1: old_key},
    )
    assert await new_store.get(ref) == "secret-value"


@pytest.mark.asyncio
async def test_rewrap_migrates_to_primary(fake_session: _FakeSession) -> None:
    """rewrap() on a fallback-version row re-encrypts to the primary key."""
    old_key = _make_key()
    new_key = _make_key()

    # Write with old key.
    old_store = LocalEncryptedSecretStore(
        fake_session,  # type: ignore[arg-type]
        master_key=old_key,
        master_key_version=1,
    )
    ref = await old_store.put(namespace="ns", value="plaintext")

    # Rewrap to new key.
    new_store = LocalEncryptedSecretStore(
        fake_session,  # type: ignore[arg-type]
        master_key=new_key,
        master_key_version=2,
        fallback_keys={1: old_key},
    )
    returned_ref = await new_store.rewrap(ref, new_master_key=new_key)
    assert returned_ref == ref

    # Row should now decrypt with new_key only (version 2).
    row = fake_session._rows[ref]
    assert row.master_key_version == 2

    # Readable by new_store (primary only).
    new_only_store = LocalEncryptedSecretStore(
        fake_session,  # type: ignore[arg-type]
        master_key=new_key,
        master_key_version=2,
    )
    assert await new_only_store.get(ref) == "plaintext"


@pytest.mark.asyncio
async def test_rewrap_to_primary_idempotent(fake_session: _FakeSession) -> None:
    """rewrap(ref, new_master_key=PRIMARY) on an already-primary row is a
    no-op in terms of plaintext; version stays at primary version."""
    key = _make_key()
    store = LocalEncryptedSecretStore(
        fake_session,  # type: ignore[arg-type]
        master_key=key,
        master_key_version=1,
    )
    ref = await store.put(namespace="ns", value="val")
    await store.rewrap(ref, new_master_key=key)
    row = fake_session._rows[ref]
    assert row.master_key_version == 1  # still primary version
    assert await store.get(ref) == "val"


@pytest.mark.asyncio
async def test_unknown_version_raises_decrypt_error(fake_session: _FakeSession) -> None:
    """Trying to decrypt a row whose version is unknown → DecryptError."""
    key = _make_key()
    store = LocalEncryptedSecretStore(
        fake_session,  # type: ignore[arg-type]
        master_key=key,
        master_key_version=1,
    )
    ref = await store.put(namespace="ns", value="x")
    # Manually set version to unknown value.
    fake_session._rows[ref].master_key_version = 99
    with pytest.raises(DecryptError, match="only knows versions"):
        await store.get(ref)


@pytest.mark.asyncio
async def test_tampered_ciphertext_raises_decrypt_error(
    fake_session: _FakeSession,
) -> None:
    """A tampered ciphertext → DecryptError (AEAD InvalidTag)."""
    key = _make_key()
    store = LocalEncryptedSecretStore(
        fake_session,  # type: ignore[arg-type]
        master_key=key,
        master_key_version=1,
    )
    ref = await store.put(namespace="ns", value="secret")
    row = fake_session._rows[ref]
    # Flip the first byte of the ciphertext.
    row.ciphertext = bytes([row.ciphertext[0] ^ 0xFF]) + row.ciphertext[1:]
    with pytest.raises(DecryptError, match="AEAD verification failed"):
        await store.get(ref)
