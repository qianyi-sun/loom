from __future__ import annotations

import copy
import importlib
import json

import pytest
import rfc8785

MODULE = "loom_task_image_authority.publication_contracts"
ATTEMPT = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"


def unsigned_payload() -> dict:
    descriptor = {
        "media_type": "application/vnd.oci.image.manifest.v1+json",
        "digest": "sha256:" + "1" * 64,
        "size": 512,
    }
    return {
        "schema": "loom.task-image-publication/v1",
        "materialization_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        "materialization_key": "2" * 64,
        "task_id": "task-123",
        "task_checksum": "3" * 64,
        "component": "task",
        "platform": "linux/arm64",
        "purpose": "production",
        "attempt_id": ATTEMPT,
        "attempt_number": 1,
        "lease_epoch": 2,
        "grant_id": "cccccccc-cccc-4ccc-8ccc-cccccccccccc",
        "original_claim_session_id": "dddddddd-dddd-4ddd-8ddd-dddddddddddd",
        "original_claim_session_generation": 1,
        "frozen_plan_sha256": "4" * 64,
        "environment": "production",
        "pool_id": "gb10-builder",
        "slurm_cluster_id": "gb10",
        "slurm_job_id": "1234",
        "build_policy_sha256": "5" * 64,
        "builder_release_sha256": "6" * 64,
        "supervisor_executable_sha256": "7" * 64,
        "containment_attestation_sha256": "8" * 64,
        "registry_origin": "https://registry.example",
        "repository": f"loom-task-image-attempts/arm64/{ATTEMPT}/task",
        "root": descriptor,
        "manifest": copy.deepcopy(descriptor),
        "config": {
            "media_type": "application/vnd.oci.image.config.v1+json",
            "digest": "sha256:" + "9" * 64,
            "size": 128,
        },
        "layers": [
            {
                "media_type": "application/vnd.oci.image.layer.v1.tar+gzip",
                "digest": "sha256:" + "a" * 64,
                "size": 1024,
            }
        ],
        "observed_base_digests": [],
    }


def contracts():
    assert importlib.util.find_spec(MODULE) is not None, "publication contracts are missing"
    return importlib.import_module(MODULE)


def test_canonical_contract_roundtrip_preserves_order_and_explicit_scratch_evidence():
    c = contracts()
    payload = unsigned_payload()
    payload["layers"] *= 2
    encoded = rfc8785.dumps(payload)
    parsed = c.decode_unsigned_input(encoded)
    assert c.canonical_publication_bytes(parsed) == encoded
    assert len(parsed.layers) == 2
    assert parsed.observed_base_digests == ()
    assert b"null" not in encoded


@pytest.mark.parametrize("field", list(unsigned_payload()))
def test_all_unsigned_fields_are_mandatory(field):
    c = contracts()
    payload = unsigned_payload()
    del payload[field]
    with pytest.raises(ValueError):
        c.decode_unsigned_input(rfc8785.dumps(payload))


@pytest.mark.parametrize(
    "field,value",
    [
        ("schema", "other/v1"),
        ("unexpected", "authority"),
        ("materialization_id", "AAAAAAAA-AAAA-4AAA-8AAA-AAAAAAAAAAAA"),
        ("grant_id", "00000000-0000-0000-0000-000000000000"),
        ("attempt_id", "bbbbbbbbbbbb4bbb8bbbbbbbbbbbbbbb"),
        ("task_checksum", "0" * 64),
        ("frozen_plan_sha256", "F" * 64),
        ("attempt_number", True),
        ("lease_epoch", 0),
        ("original_claim_session_generation", 9007199254740992),
        ("attempt_number", 1.0),
        ("task_id", "\ud800"),
        ("platform", "linux/amd64"),
        ("purpose", "shadow"),
        ("shadow_campaign_id", None),
        ("registry_origin", "http://registry.example"),
        ("registry_origin", "https://registry.example/path"),
        ("repository", f"loom-task-image-attempts/x86_64/{ATTEMPT}/task"),
        ("observed_base_digests", ["sha256:" + "a" * 64] * 2),
        ("observed_base_digests", ["sha256:" + "b" * 64, "sha256:" + "a" * 64]),
        ("observed_base_digests", ["ubuntu:latest"]),
        ("observed_base_digests", ["sha256:" + "1" * 64] * 129),
        ("observed_base_digests", None),
        ("issued_at", "2026-09-05T12:00:00Z"),
    ],
)
def test_rejects_invalid_unsigned_authority(field, value):
    c = contracts()
    payload = unsigned_payload()
    payload[field] = value
    with pytest.raises(ValueError):
        c.decode_unsigned_input(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode())


@pytest.mark.parametrize(
    "transform",
    [
        lambda data: b" " + data,
        lambda data: data.replace(b'"attempt_number":1', b'"attempt_number":1,"attempt_number":1'),
        lambda data: data.replace(b'"attempt_number":1', b'"attempt_number":NaN'),
        lambda data: data.replace(b'"task-123"', b'"task-\\u0031\\u0032\\u0033"'),
        lambda data: b"[" * 1000 + data + b"]" * 1000,
        lambda data: b"x" * (128 * 1024),
        lambda data: b"\xff",
    ],
)
def test_untrusted_decoder_rejects_noncanonical_and_unbounded_bytes(transform):
    c = contracts()
    with pytest.raises(ValueError):
        c.decode_unsigned_input(transform(rfc8785.dumps(unsigned_payload())))


def test_shadow_schema_is_distinct_and_does_not_relax_production_repository():
    c = contracts()
    payload = unsigned_payload()
    campaign = "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"
    payload.update(
        purpose="shadow",
        shadow_campaign_id=campaign,
        repository=f"loom-task-image-shadow/{campaign}/arm64/{ATTEMPT}/task",
    )
    assert c.decode_unsigned_input(rfc8785.dumps(payload)).purpose == "shadow"
    payload["purpose"] = "production"
    with pytest.raises(ValueError):
        c.decode_unsigned_input(rfc8785.dumps(payload))


@pytest.mark.parametrize(
    "field,value",
    [
        ("size", True),
        ("size", -1),
        ("size", 9007199254740992),
        ("digest", "sha256:" + "0" * 64),
        ("media_type", "application/unknown"),
        ("urls", ["https://external.example"]),
    ],
)
def test_descriptors_are_closed_bounded_and_typed(field, value):
    c = contracts()
    payload = unsigned_payload()
    payload["layers"][0][field] = value
    with pytest.raises(ValueError):
        c.decode_unsigned_input(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode())
