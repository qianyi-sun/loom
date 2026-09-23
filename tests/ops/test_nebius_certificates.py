"""Certificate delivery must validate the real chain and preserve recovery state."""
from __future__ import annotations

import importlib
import json
import os
from datetime import UTC, datetime, timedelta

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

NOW = datetime(2026, 9, 23, 12, tzinfo=UTC)
NAMES = ("*.dev.example.test", "management.example.test")


def module():
    return importlib.import_module("scripts.ops.nebius_certificates")


def material(*, names=NAMES, days=70, start_days=-1, ca_leaf=False, client_only=False):
    ca_key = ec.generate_private_key(ec.SECP256R1())
    ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "Local certificate test CA")])
    ca = (x509.CertificateBuilder().subject_name(ca_name).issuer_name(ca_name)
          .public_key(ca_key.public_key()).serial_number(x509.random_serial_number())
          .not_valid_before(NOW - timedelta(days=3)).not_valid_after(NOW + timedelta(days=400))
          .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
          .add_extension(x509.KeyUsage(False, False, False, False, False, True, True, False, False), critical=True)
          .add_extension(x509.SubjectKeyIdentifier.from_public_key(ca_key.public_key()), critical=False)
          .sign(ca_key, hashes.SHA256()))
    key = ec.generate_private_key(ec.SECP256R1())
    leaf = (x509.CertificateBuilder()
            .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, names[-1])]))
            .issuer_name(ca.subject).public_key(key.public_key()).serial_number(x509.random_serial_number())
            .not_valid_before(NOW + timedelta(days=start_days)).not_valid_after(NOW + timedelta(days=days))
            .add_extension(x509.BasicConstraints(ca=ca_leaf, path_length=None), critical=True)
            .add_extension(x509.KeyUsage(True, False, False, False, False, False, False, False, False), critical=True)
            .add_extension(x509.AuthorityKeyIdentifier.from_issuer_public_key(ca_key.public_key()), critical=False)
            .add_extension(x509.SubjectAlternativeName([x509.DNSName(name) for name in names]), critical=False)
            .add_extension(x509.ExtendedKeyUsage([
                ExtendedKeyUsageOID.CLIENT_AUTH if client_only else ExtendedKeyUsageOID.SERVER_AUTH,
            ]), critical=False).sign(ca_key, hashes.SHA256()))
    return (leaf.public_bytes(serialization.Encoding.PEM),
            key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                              serialization.NoEncryption()), [ca])


def validate(chain, key, roots):
    return module().validate_certificate(chain, key, child_domain="dev.example.test",
                                         management_host="management.example.test", now=NOW, roots=roots)


def test_real_certificate_chain_matches_both_names_and_private_key():
    chain, key, roots = material()
    report = validate(chain, key, roots)
    assert report["sans"] == list(NAMES)
    assert report["expires_at"] == "2026-12-02T12:00:00+00:00"
    assert len(report["fingerprint_sha256"]) == 64
    assert "PRIVATE KEY" not in json.dumps(report)


@pytest.mark.parametrize("change", [
    {"names": ("*.dev.example.test", "foreign.example.test")},
    {"names": (*NAMES, "extra.example.test")},
    {"names": (NAMES[0], NAMES[1], NAMES[1])},
    {"days": -1, "start_days": -3}, {"days": 6}, {"start_days": 1},
    {"ca_leaf": True}, {"client_only": True},
])
def test_invalid_certificate_is_rejected(change):
    with pytest.raises(module().CertificateError):
        validate(*material(**change))


def test_untrusted_chain_and_wrong_private_key_are_rejected():
    chain, key, roots = material()
    _, foreign_key, foreign_roots = material()
    for candidate_key, candidate_roots in [(foreign_key, roots), (key, foreign_roots)]:
        with pytest.raises(module().CertificateError):
            validate(chain, candidate_key, candidate_roots)


@pytest.mark.parametrize("chain,key", [(b"private-payload", b"private-key"), (b"x" * 65537, b"key")])
def test_malformed_or_oversized_inputs_have_fixed_diagnostics(chain, key):
    with pytest.raises(module().CertificateError) as error:
        validate(chain, key, material()[2])
    assert "private-" not in str(error.value)


def test_management_host_cannot_be_inside_personal_zone():
    chain, key, roots = material()
    with pytest.raises(module().CertificateError):
        module().validate_certificate(chain, key, child_domain="example.test",
                                      management_host="management.example.test", now=NOW, roots=roots)


def publish(root, chain, key, roots):
    return module().publish_certificate(root, chain, key, child_domain="dev.example.test",
                                        management_host="management.example.test", now=NOW, roots=roots)


def test_publish_is_private_durable_replayable_and_preserves_previous_generation(tmp_path):
    root = tmp_path / "state"
    first = material()
    result = publish(root, *first)
    selected = json.loads((root / "selected.json").read_text())
    assert selected["generation"] == result["generation"]
    generation = root / "generations" / selected["generation"]
    assert (generation / "fullchain.pem").read_bytes() == first[0]
    assert (generation / "privkey.pem").read_bytes() == first[1]
    original = (root / "selected.json").read_bytes()
    assert publish(root, *first) == result
    assert (root / "selected.json").read_bytes() == original
    second = publish(root, *material())
    assert second["generation"] != result["generation"]
    assert (generation / "privkey.pem").read_bytes() == first[1]
    assert json.loads((root / "selected.json").read_text())["previous_generation"] == result["generation"]
    for path in [root, *root.rglob("*")]:
        assert path.stat().st_mode & 0o077 == 0


def test_invalid_renewal_leaves_selected_generation_unchanged(tmp_path):
    root = tmp_path / "state"
    publish(root, *material())
    original = (root / "selected.json").read_bytes()
    with pytest.raises(module().CertificateError):
        publish(root, *material(names=("*.foreign.test", "management.example.test")))
    assert (root / "selected.json").read_bytes() == original


@pytest.mark.parametrize("unsafe", ["symlink_root", "public_root", "symlink_selected", "symlink_generation"])
def test_unsafe_storage_is_not_followed_or_overwritten(tmp_path, unsafe):
    root = tmp_path / "state"
    external = tmp_path / "external"
    external.mkdir(mode=0o700)
    sentinel = external / "sentinel"
    sentinel.write_text("foreign data")
    root.mkdir(mode=0o700)
    if unsafe == "symlink_root":
        root.rmdir()
        root.symlink_to(external, target_is_directory=True)
    elif unsafe == "public_root":
        root.chmod(0o755)
    elif unsafe == "symlink_selected":
        (root / "selected.json").symlink_to(sentinel)
    else:
        (root / "generations").symlink_to(external, target_is_directory=True)
    with pytest.raises(module().CertificateError):
        publish(root, *material())
    assert sentinel.read_text() == "foreign data"


def test_failed_atomic_selection_retains_previously_deliverable_generation(tmp_path, monkeypatch):
    root = tmp_path / "state"
    publish(root, *material())
    original = (root / "selected.json").read_bytes()
    replace = os.replace

    def interrupt(source, destination):
        if str(destination).endswith("selected.json"):
            raise OSError("private filesystem diagnostic")
        return replace(source, destination)

    monkeypatch.setattr(os, "replace", interrupt)
    with pytest.raises(module().CertificateError):
        publish(root, *material())
    assert (root / "selected.json").read_bytes() == original


def test_generation_files_are_synced_before_selecting_them(tmp_path, monkeypatch):
    real_fsync, real_replace = os.fsync, os.replace
    synced = []

    def fsync(fd):
        synced.append(os.readlink(f"/proc/self/fd/{fd}"))
        real_fsync(fd)

    def replace(source, destination):
        if str(destination).endswith("selected.json"):
            assert any(path.endswith("/fullchain.pem") for path in synced)
            assert any(path.endswith("/privkey.pem") for path in synced)
            assert any(path.endswith("/generations") for path in synced)
            assert str(tmp_path) in synced, "new state directory itself was not durable"
        return real_replace(source, destination)

    monkeypatch.setattr(os, "fsync", fsync)
    monkeypatch.setattr(os, "replace", replace)
    publish(tmp_path / "state", *material())
