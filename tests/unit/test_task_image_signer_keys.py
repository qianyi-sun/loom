"""Explicit owner-only key files; never generate/fallback/export production keys."""

import importlib
import os

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey


def module():
    name = "loom_task_image_signer.keys"
    assert importlib.util.find_spec(name) is not None, "protected signer key loader is missing"
    return importlib.import_module(name)


def material(tmp_path):
    directory = tmp_path / "keys"
    directory.mkdir(mode=0o700)
    private = Ed25519PrivateKey.generate()
    path = directory / "publication.seed"
    path.write_bytes(private.private_bytes_raw())
    path.chmod(0o600)
    return path, private


async def test_protected_existing_seed_matches_public_pin_and_signs(tmp_path):
    m = module()
    path, private = material(tmp_path)
    pin = private.public_key().public_bytes_raw()
    provider = m.load_signing_key(path, expected_public_key=pin)
    assert provider.public_key == pin
    signature = await provider.sign(b"fixed-domain\x00canonical-input")
    private.public_key().verify(signature, b"fixed-domain\x00canonical-input")
    assert private.private_bytes_raw().hex() not in repr(provider)
    assert not hasattr(provider, "private_bytes")
    assert not hasattr(provider, "private_key")


@pytest.mark.parametrize("change", [
    "missing", "relative", "symlink", "parent-symlink", "hardlink", "fifo", "directory",
    "group-readable", "world-readable", "executable", "empty", "short", "long", "bad-pin",
    "writable-parent", "changed-during-read",
])
def test_loader_rejects_unsafe_paths_metadata_content_and_substitution(tmp_path, monkeypatch, change):
    m = module()
    path, private = material(tmp_path)
    pin = private.public_key().public_bytes_raw()
    if change == "missing":
        path = path.parent / "missing"
    elif change == "relative":
        path = path.relative_to(path.anchor)
    elif change in {"symlink", "parent-symlink"}:
        link = tmp_path / "link"
        link.symlink_to(path if change == "symlink" else path.parent)
        path = link if change == "symlink" else link / path.name
    elif change == "hardlink":
        os.link(path, path.parent / "other")
    elif change == "fifo":
        path = path.parent / "fifo"
        os.mkfifo(path, mode=0o600)
    elif change == "directory":
        path = path.parent
    elif change in {"group-readable", "world-readable", "executable"}:
        path.chmod({"group-readable": 0o640, "world-readable": 0o604, "executable": 0o700}[change])
    elif change in {"empty", "short", "long"}:
        path.write_bytes(b"x" * {"empty": 0, "short": 31, "long": 33}[change])
    elif change == "bad-pin":
        pin = Ed25519PrivateKey.generate().public_key().public_bytes_raw()
    elif change == "writable-parent":
        path.parent.chmod(0o777)
    else:
        original = os.read
        def read(fd, count):
            value = original(fd, count)
            path.chmod(0o400)
            return value
        monkeypatch.setattr(os, "read", read)
    with pytest.raises(ValueError, match="signer key"):
        m.load_signing_key(path, expected_public_key=pin)
    if change == "missing":
        assert not path.exists()


def test_wrong_owner_is_refused_without_requiring_privileged_test_user(tmp_path, monkeypatch):
    m = module()
    path, private = material(tmp_path)
    monkeypatch.setattr(os, "geteuid", lambda: 123456789)
    with pytest.raises(ValueError, match="signer key"):
        m.load_signing_key(path, expected_public_key=private.public_key().public_bytes_raw())
