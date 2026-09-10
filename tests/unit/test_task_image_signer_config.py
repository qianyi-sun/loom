"""Explicit protected service configuration; no environment-key fallback."""

import base64
import importlib
import json
from dataclasses import asdict

import pytest

from tests.unit.test_task_image_publication_keyset import fixture
from tests.unit.test_task_image_publication_signing import setup_signing


def module():
    name = "loom_task_image_signer.config"
    assert importlib.util.find_spec(name) is not None, "dedicated signer configuration missing"
    return importlib.import_module(name)


def document(tmp_path):
    from loom_task_image_signer.policy import PublicationSelection
    _, root, *_ = fixture()
    _, _, _, key, _, _, unsigned, _ = setup_signing()
    return dict(
        schema="loom.task-image-signer/v1", host="127.0.0.1", port=8447,
        database_url_file=str(tmp_path / "database-url"),
        ca_file=str(tmp_path / "client-ca.pem"), certificate_file=str(tmp_path / "server.pem"),
        private_key_file=str(tmp_path / "server.key"),
        execution=dict(key_id=root.key_id, environment=root.environment,
            public_key=base64.urlsafe_b64encode(root.public_key).rstrip(b"=").decode(),
            activated_at=root.activated_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
            expires_at=root.expires_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
            seed_file=str(tmp_path / "execution.seed")),
        publication=dict(key_id=key.key_id, public_key=base64.urlsafe_b64encode(key.public_key).rstrip(b"=").decode(), seed_file=str(tmp_path / "publication.seed")),
        selections=[asdict(PublicationSelection.from_unsigned(unsigned))],
        peer_operations={"a" * 64: ["keyset"], "b" * 64: ["publication"]},
    )


def test_config_is_explicit_closed_and_preserves_fixed_separate_roots(tmp_path):
    m = module()
    data = document(tmp_path)
    parsed = m.decode_signer_settings(json.dumps(data).encode())
    assert parsed.trust_root().key_id == data["execution"]["key_id"]
    assert parsed.publication.key_id != parsed.execution.key_id
    assert parsed.peer_operations["a" * 64] == ("keyset",)


@pytest.mark.parametrize("change", ["unknown", "same-root", "same-id", "relative", "duplicate", "null", "empty-peers", "wrong-operation", "wrong-environment", "bad-root-time", "oversize"])
def test_invalid_ambiguous_or_unscoped_service_config_refused(tmp_path, change):
    m = module()
    data = document(tmp_path)
    if change == "unknown":
        data["auto_generate_keys"] = True
    elif change == "same-root":
        data["publication"]["public_key"] = data["execution"]["public_key"]
    elif change == "same-id":
        data["publication"]["key_id"] = data["execution"]["key_id"]
    elif change == "relative":
        data["database_url_file"] = "relative"
    elif change == "null":
        data["execution"]["seed_file"] = None
    elif change == "empty-peers":
        data["peer_operations"] = {}
    elif change == "wrong-operation":
        data["peer_operations"]["a" * 64] = ["arbitrary"]
    elif change == "wrong-environment":
        data["selections"][0]["environment"] = "other"
    elif change == "bad-root-time":
        data["execution"]["expires_at"] = data["execution"]["activated_at"]
    wire = json.dumps(data).encode()
    if change == "duplicate":
        wire = wire.replace(b'"port": 8447', b'"port": 8447, "port": 8447')
    elif change == "oversize":
        wire = b"x" * (65536 + 1)
    with pytest.raises(ValueError):
        m.decode_signer_settings(wire)
