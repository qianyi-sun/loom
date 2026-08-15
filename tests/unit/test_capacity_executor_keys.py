from __future__ import annotations

import os
from pathlib import Path

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from loom_capacity_executor.keys import (
    ExecutorKeyError,
    ExecutorOwnershipKey,
    load_executor_ownership_key,
    load_ownership_private_key,
)
from loom_capacity_manager.ownership import public_key_fingerprint


def _raw_private_key(key: Ed25519PrivateKey) -> bytes:
    return key.private_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PrivateFormat.Raw,
        encryption_algorithm=serialization.NoEncryption(),
    )


def test_private_key_loads_only_with_exact_expected_fingerprint(tmp_path: Path) -> None:
    path = tmp_path / "ownership.key"
    private_key = Ed25519PrivateKey.generate()
    path.write_bytes(_raw_private_key(private_key))
    path.chmod(0o600)

    loaded = load_ownership_private_key(
        path,
        expected_public_key_sha256=public_key_fingerprint(private_key.public_key()),
    )

    assert public_key_fingerprint(loaded.public_key()) == public_key_fingerprint(
        private_key.public_key()
    )
    with pytest.raises(ExecutorKeyError, match="fingerprint"):
        load_ownership_private_key(path, expected_public_key_sha256="f" * 64)


def test_private_key_rejects_bad_permissions_size_and_symlink(tmp_path: Path) -> None:
    path = tmp_path / "ownership.key"
    path.write_bytes(os.urandom(32))
    path.chmod(0o640)
    with pytest.raises(ExecutorKeyError, match="permissions"):
        load_ownership_private_key(path, expected_public_key_sha256="f" * 64)

    path.chmod(0o600)
    path.write_bytes(b"short")
    with pytest.raises(ExecutorKeyError, match="32 raw bytes"):
        load_ownership_private_key(path, expected_public_key_sha256="f" * 64)

    target = tmp_path / "target.key"
    target.write_bytes(os.urandom(32))
    target.chmod(0o600)
    path.unlink()
    path.symlink_to(target)
    with pytest.raises(ExecutorKeyError, match="symlink"):
        load_ownership_private_key(path, expected_public_key_sha256="f" * 64)


def test_controller_key_bundle_binds_registered_id_key_and_fingerprint(tmp_path: Path) -> None:
    path = tmp_path / "ownership.key"
    private_key = Ed25519PrivateKey.generate()
    path.write_bytes(_raw_private_key(private_key))
    path.chmod(0o600)
    fingerprint = public_key_fingerprint(private_key.public_key())

    loaded = load_executor_ownership_key(
        path,
        signing_key_id="oldlab-key-1",
        expected_public_key_sha256=fingerprint,
    )

    assert isinstance(loaded, ExecutorOwnershipKey)
    assert loaded.signing_key_id == "oldlab-key-1"
    assert loaded.public_key_sha256 == fingerprint
    assert public_key_fingerprint(loaded.private_key.public_key()) == fingerprint

    with pytest.raises(ExecutorKeyError, match="signing key id"):
        load_executor_ownership_key(
            path,
            signing_key_id="OLDLAB KEY",
            expected_public_key_sha256=fingerprint,
        )
