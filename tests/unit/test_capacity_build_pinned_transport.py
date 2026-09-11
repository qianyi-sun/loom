"""Pinned admission credentials are consumed from verified immutable snapshots."""

from hashlib import sha256
from importlib import import_module

import pytest

from tests.unit.test_capacity_agent_client import _owner_file


def pinned_inputs(tmp_path):
    from cryptography.hazmat.primitives import serialization

    from tests.integration.test_capacity_manager_mtls import _new_ca, _private_key_bytes, _signed_certificate

    ca_key, ca = _new_ca("pinned-admission-ca")
    key, cert = _signed_certificate("pool-executor",ca_key,ca,server=False)
    values = {"ca":ca.public_bytes(serialization.Encoding.PEM),
        "certificate":cert.public_bytes(serialization.Encoding.PEM),
        "private_key":_private_key_bytes(key),"bearer_token":b"pool-only-secret"}
    pins = {}
    for name, payload in values.items():
        path = _owner_file(tmp_path/name,payload)
        pins[name] = {"path":str(path),"sha256":sha256(payload).hexdigest()}
    return pins


def test_pinned_tls_uses_hashed_bytes_not_reopened_paths(tmp_path,monkeypatch):
    from pathlib import Path

    module = import_module("loom_capacity_executor.pinned_admission_transport")
    pins = pinned_inputs(tmp_path)
    config = module.PinnedBuildAdmissionConnectionV1(origin="https://management.test",**pins)
    original = module.read_owner_only_bytes
    read = []

    def replace_after_read(path,**kwargs):
        data = original(path,**kwargs)
        read.append(path)
        # Each file becomes invalid immediately after its verified read. The TLS
        # constructor must still use the exact pinned snapshot, not reopen it.
        Path(path).write_bytes(b"replaced-with-untrusted-data")
        return data

    monkeypatch.setattr(module,"read_owner_only_bytes",replace_after_read)
    context, token = module.load_pinned_admission_credentials(config)
    assert context.check_hostname is True
    assert token == "pool-only-secret"
    assert len(read) == 4
    assert len(context.get_ca_certs()) == 1


@pytest.mark.parametrize("boundary", ["token", "ca", "certificate", "private_key", "mode", "symlink", "origin"])
def test_pinned_credentials_reject_drift(tmp_path,boundary):
    from pathlib import Path

    module = import_module("loom_capacity_executor.pinned_admission_transport")
    pins = pinned_inputs(tmp_path)
    origin = "http://management.test" if boundary == "origin" else "https://management.test"
    field = "bearer_token" if boundary == "token" else boundary
    if field in pins:
        pins[field]["sha256"] = "f"*64
    elif boundary == "mode":
        Path(pins["private_key"]["path"]).chmod(0o644)
    elif boundary == "symlink":
        link = tmp_path/"linked-key"
        link.symlink_to(pins["private_key"]["path"])
        pins["private_key"]["path"] = str(link)
    with pytest.raises((ValueError,RuntimeError,OSError)) as failure:
        config = module.PinnedBuildAdmissionConnectionV1(origin=origin,**pins)
        module.load_pinned_admission_credentials(config)
    assert "pool-only-secret" not in str(failure.value)
