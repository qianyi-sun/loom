"""Complete expected sets, not individual valid signatures, define image identity."""

import hashlib
import importlib
from datetime import timedelta
from uuid import UUID

import pytest
import rfc8785
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from loom_task_image_authority.registry_token import publication_repository
from tests.unit.test_task_image_materialization import _task_config
from tests.unit.test_task_image_publication_keyset import _b64, _sign, _time
from tests.unit.test_task_image_publication_signing import DOMAIN, NOW, setup_signing


def module():
    name = "loom_task_image_authority.publication_set"
    assert importlib.util.find_spec(name) is not None, "complete publication-set verifier missing"
    return importlib.import_module(name)


def fixture(*, change=None):
    m = module()
    c, s, private, key, state, distribution, original, _ = setup_signing()
    task = _task_config(cpu_arch="arm64", dockerfile="Dockerfile", sidecars=[
        {"name": "db", "dockerfile": "db/Dockerfile"},
        {"name": "cache", "docker_image": "example/cache@sha256:" + "c" * 64},
    ])
    execution = Ed25519PrivateKey.generate()
    root = m.ExecutionGrantTrustRoot(
        key_id="execution-1", environment="production", public_key=execution.public_key().public_bytes_raw(),
        activated_at=NOW - timedelta(days=1), expires_at=NOW + timedelta(days=1),
    )
    keyset = _sign(dict(
        schema="loom.task-image-publication-keyset/v1", environment="production", keyset_version=3,
        revocation_epoch=2, issued_at=_time(NOW - timedelta(seconds=10)),
        expires_at=_time(NOW + timedelta(minutes=5)), keys=[dict(
            key_id=key.key_id, public_key=_b64(key.public_key), status="active", activated_at=_time(key.activated_at),
        )],
    ), execution)
    expected, wires = [], []
    for component in ("sidecar:db", "task"):
        payload = original.model_dump(mode="json", by_alias=True, exclude_none=True)
        payload.update(component=component, task_id=task.task.id)
        if component == "task" and change is not None:
            payload.update(change)
        arch = "arm64" if payload["platform"] == "linux/arm64" else "x86_64"
        payload["repository"] = publication_repository(
            purpose=payload["purpose"], shadow_campaign_id=UUID(payload["shadow_campaign_id"]) if payload.get("shadow_campaign_id") else None,
            cpu_arch=arch, attempt_id=UUID(payload["attempt_id"]), component=component,
        )
        unsigned = c.decode_unsigned_input(rfc8785.dumps(payload))
        statement = s.prepare_publication_statement(unsigned, key=key, state=state, distribution=distribution, signer_now=NOW)
        canonical = c.canonical_publication_bytes(statement)
        wire = rfc8785.dumps(dict(canonical_statement=canonical.decode(), statement_sha256=hashlib.sha256(canonical).hexdigest(),
                                key_id=key.key_id, algorithm="Ed25519", signature=_b64(private.sign(DOMAIN + canonical))))
        expected.append(m.ExpectedPublication(unsigned=unsigned, envelope_sha256=hashlib.sha256(wire).hexdigest()))
        wires.append(wire)
    return dict(publication_wires=tuple(wires), expected=tuple(expected), task=task, keyset_wire=keyset,
                trust_root=root, expected_state=state, expected_snapshot_sha256=hashlib.sha256(keyset).hexdigest(), now=NOW)


def test_exact_complete_set_returns_verified_native_manifest_references_only():
    data = fixture()
    result = module().verify_publication_set(**data)
    assert tuple(item.statement.component for item in result.publications) == ("sidecar:db", "task")
    assert result.registry_images == tuple(
        (item.unsigned.component, f"registry.example/{item.unsigned.repository}@{item.unsigned.manifest.digest}")
        for item in data["expected"]
    )
    assert not hasattr(result, "start_authorization")
    assert not hasattr(result, "ready")


@pytest.mark.parametrize("change", ["missing", "extra", "duplicate", "reorder", "missing-expected", "empty", "many", "list", "oversize", "bad-wire", "keyset-pin", "envelope-pin", "wrong-task", "wrong-arch", "wrong-components", "expired"])
def test_incomplete_ambiguous_or_unbound_sets_are_refused(change):
    data = fixture()
    wires = data["publication_wires"]
    if change == "missing":
        data["publication_wires"] = wires[:1]
    elif change == "extra":
        data["publication_wires"] = wires + wires[:1]
    elif change == "duplicate":
        data["publication_wires"] = wires[:1] * 2
    elif change == "reorder":
        data["publication_wires"] = wires[::-1]
    elif change == "missing-expected":
        data["expected"], data["publication_wires"] = data["expected"][:1], wires[:1]
    elif change == "empty":
        data["expected"], data["publication_wires"] = (), ()
    elif change == "many":
        data["expected"], data["publication_wires"] = data["expected"][:1] * 129, wires[:1] * 129
    elif change == "list":
        data["publication_wires"] = list(wires)
    elif change == "oversize":
        data["publication_wires"] = (b"x" * (128 * 1024 + 1), wires[1])
    elif change == "bad-wire":
        data["publication_wires"] = (wires[0] + b" ", wires[1])
    elif change == "keyset-pin":
        data["expected_snapshot_sha256"] = "f" * 64
    elif change == "envelope-pin":
        # The inner statement digest is not the envelope digest.
        import json
        from dataclasses import replace
        data["expected"] = (replace(data["expected"][0], envelope_sha256=json.loads(wires[0])["statement_sha256"]), data["expected"][1])
    elif change in {"wrong-task", "wrong-arch", "wrong-components"}:
        task = data["task"].model_dump(mode="json")
        if change == "wrong-task":
            task["task"]["id"] = "different-task"
        elif change == "wrong-arch":
            task["environment"]["cpu_arch"] = "x86_64"
        else:
            task["environment"]["sidecars"] = []
        data["task"] = type(data["task"]).model_validate(task)
    elif change == "expired":
        data["now"] = NOW + timedelta(minutes=6)
    with pytest.raises(ValueError):
        module().verify_publication_set(**data)


@pytest.mark.parametrize("change", [
    {"attempt_id": "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"}, {"attempt_number": 2}, {"lease_epoch": 3},
    {"grant_id": "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"},
    {"original_claim_session_id": "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"}, {"original_claim_session_generation": 2},
    {"frozen_plan_sha256": "f" * 64}, {"task_checksum": "f" * 64}, {"materialization_key": "f" * 64},
    {"materialization_id": "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"}, {"pool_id": "another-pool"},
    {"slurm_job_id": "9999"}, {"build_policy_sha256": "f" * 64}, {"builder_release_sha256": "f" * 64},
    {"supervisor_executable_sha256": "f" * 64}, {"containment_attestation_sha256": "f" * 64},
    {"registry_origin": "https://other.example"},
    {"purpose": "shadow", "shadow_campaign_id": "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"},
])
def test_individually_signed_components_cannot_mix_build_authorities(change):
    data = fixture(change=change)
    # All signatures/pins are valid. Only complete-set coherence rejects these.
    with pytest.raises(ValueError):
        module().verify_publication_set(**data)
