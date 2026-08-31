from __future__ import annotations

import hashlib
import json
import os
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import MappingProxyType
from uuid import UUID

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from loom.personal_dev_native_builder_protocol import (
    NATIVE_BUILDER_MAX_CONCURRENCY,
    NATIVE_BUILDER_PLATFORM,
    NATIVE_BUILDER_PROTOCOL_VERSION,
    NATIVE_BUILDER_PROVIDER,
    NativeBuilderAgentStatus,
    NativeBuilderCompletion,
    NativeBuilderGrantPayload,
    NativeBuilderHeartbeatRequest,
    NativeBuilderPollRequest,
    NativeBuilderRuntimeEvidence,
    PersonalDevNativeBuilderSigner,
    PersonalDevNativeBuilderVerifier,
    load_personal_dev_native_builder_signer,
    load_personal_dev_native_builder_verifier,
)

_NOW = datetime(2026, 8, 30, 16, 0, tzinfo=UTC)
_INSTANCE_ID = UUID("00000000-0000-0000-0000-000000000001")
_BOOT_ID = UUID("00000000-0000-0000-0000-000000000002")
_GRANT_ID = UUID("00000000-0000-0000-0000-000000000003")
_SECOND_GRANT_ID = UUID("00000000-0000-0000-0000-000000000004")
_ATTEMPT_ID = UUID("00000000-0000-0000-0000-000000000005")
_CANDIDATE_ID = UUID("00000000-0000-0000-0000-000000000006")
_NONCE = UUID("00000000-0000-0000-0000-000000000007")
_AGENT_IMAGE = "ghcr.io/qianyi-sun/loom-personal-dev-native-builder-agent@sha256:" + "a" * 64
_BUILDER_IMAGE = "ghcr.io/qianyi-sun/loom-personal-dev-builder@sha256:" + "b" * 64
_CONTRACT = '{"platform":"linux/arm64","schema_version":1}'
_CONTRACT_SHA256 = "6a70281ff4f91c00db4f7c1d0c0dadaead31b7a425fc1fb40dfb2c8a3b4bb714"


def _status() -> NativeBuilderAgentStatus:
    return NativeBuilderAgentStatus(
        agent_instance_id=_INSTANCE_ID,
        agent_key_id="gb10-native-builder-v1",
        provider=NATIVE_BUILDER_PROVIDER,
        platform=NATIVE_BUILDER_PLATFORM,
        protocol_version=NATIVE_BUILDER_PROTOCOL_VERSION,
        host_name="gx10-01c7",
        host_architecture="aarch64",
        host_boot_id=_BOOT_ID,
        agent_image=_AGENT_IMAGE,
        builder_image=_BUILDER_IMAGE,
        runtime_profile_sha256="c" * 64,
        max_concurrency=NATIVE_BUILDER_MAX_CONCURRENCY,
        managed_grant_ids=(_GRANT_ID, _SECOND_GRANT_ID),
        active_grant_ids=(_GRANT_ID,),
        available=True,
        unavailable_reason=None,
        readiness_evidence_sha256="d" * 64,
    )


def _poll() -> NativeBuilderPollRequest:
    return NativeBuilderPollRequest(
        status=_status(),
        requested_at=_NOW,
        request_nonce=_NONCE,
    )


def _grant() -> NativeBuilderGrantPayload:
    return NativeBuilderGrantPayload(
        grant_id=_GRANT_ID,
        candidate_id=_CANDIDATE_ID,
        candidate_sha="e" * 64,
        attempt_id=_ATTEMPT_ID,
        attempt_lease_epoch=11,
        platform=NATIVE_BUILDER_PLATFORM,
        provider=NATIVE_BUILDER_PROVIDER,
        agent_instance_id=_INSTANCE_ID,
        agent_key_id="gb10-native-builder-v1",
        builder_image=_BUILDER_IMAGE,
        runtime_profile_sha256="c" * 64,
        contract_json=_CONTRACT,
        contract_sha256=_CONTRACT_SHA256,
        source_get_url="https://objects.example/personal-dev/source?token=secret",
        artifact_upload_url="https://objects.example/personal-dev/upload",
        artifact_upload_fields={"key": "bound/object", "policy": "secret"},
        artifact_max_bytes=8 * 1024 * 1024 * 1024,
        capability_expires_at=_NOW,
        active_deadline_seconds=3600,
    )


def _heartbeat() -> NativeBuilderHeartbeatRequest:
    return NativeBuilderHeartbeatRequest(
        agent_instance_id=_INSTANCE_ID,
        agent_key_id="gb10-native-builder-v1",
        grant_id=_GRANT_ID,
        attempt_id=_ATTEMPT_ID,
        attempt_lease_epoch=11,
        requested_at=_NOW,
        request_nonce=_NONCE,
    )


def _evidence() -> NativeBuilderRuntimeEvidence:
    return NativeBuilderRuntimeEvidence(
        agent_instance_id=_INSTANCE_ID,
        grant_id=_GRANT_ID,
        attempt_id=_ATTEMPT_ID,
        attempt_lease_epoch=11,
        provider=NATIVE_BUILDER_PROVIDER,
        platform=NATIVE_BUILDER_PLATFORM,
        host_name="gx10-01c7",
        host_architecture="aarch64",
        host_boot_id=_BOOT_ID,
        agent_image=_AGENT_IMAGE,
        builder_image=_BUILDER_IMAGE,
        runtime_profile_sha256="c" * 64,
        contract_sha256=_CONTRACT_SHA256,
        runtime_name="runsc-personal-dev-native",
        client_container_id="1" * 64,
        buildkit_container_id="2" * 64,
        network_id="3" * 64,
        client_inspect_sha256="4" * 64,
        buildkit_inspect_sha256="5" * 64,
        network_inspect_sha256="6" * 64,
        client_exit_code=0,
        client_oom_killed=False,
        client_restart_count=0,
        buildkit_restart_count=0,
        buildkit_running=True,
        observed_at=_NOW,
    )


def _completion() -> NativeBuilderCompletion:
    return NativeBuilderCompletion(
        agent_instance_id=_INSTANCE_ID,
        agent_key_id="gb10-native-builder-v1",
        grant_id=_GRANT_ID,
        attempt_id=_ATTEMPT_ID,
        attempt_lease_epoch=11,
        outcome="succeeded",
        failure_reason=None,
        evidence=_evidence(),
        requested_at=_NOW,
        request_nonce=_NONCE,
    )


def _keys() -> tuple[bytes, bytes]:
    private = Ed25519PrivateKey.generate()
    return (
        private.private_bytes_raw(),
        private.public_key().public_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PublicFormat.Raw,
        ),
    )


def test_native_builder_poll_has_literal_canonical_ascii_contract() -> None:
    """A field omission, rename, or non-canonical encoder must break this test."""
    expected = (
        '{"request_nonce":"00000000-0000-0000-0000-000000000007",'
        '"requested_at":"2026-08-30T16:00:00Z","schema_version":1,"status":{'
        '"active_grant_ids":["00000000-0000-0000-0000-000000000003"],'
        '"agent_image":"'
        + _AGENT_IMAGE
        + '","agent_instance_id":"00000000-0000-0000-0000-000000000001",'
        '"agent_key_id":"gb10-native-builder-v1","available":true,'
        '"builder_image":"'
        + _BUILDER_IMAGE
        + '","host_architecture":"aarch64",'
        '"host_boot_id":"00000000-0000-0000-0000-000000000002",'
        '"host_name":"gx10-01c7","managed_grant_ids":['
        '"00000000-0000-0000-0000-000000000003",'
        '"00000000-0000-0000-0000-000000000004"],"max_concurrency":2,'
        '"platform":"linux/arm64","protocol_version":1,'
        '"provider":"gb10-gvisor-docker-v1",'
        '"readiness_evidence_sha256":"'
        + "d" * 64
        + '","runtime_profile_sha256":"'
        + "c" * 64
        + '","unavailable_reason":null}}'
    ).encode("ascii")

    assert _poll().canonical_bytes() == expected
    assert json.loads(expected)["schema_version"] == 1


def test_native_builder_signed_messages_have_disjoint_exact_field_sets() -> None:
    """Reusing one message type's fields for another must not preserve its contract."""
    poll = json.loads(_poll().canonical_bytes())
    heartbeat = json.loads(_heartbeat().canonical_bytes())
    completion = json.loads(_completion().canonical_bytes())

    assert set(poll) == {"request_nonce", "requested_at", "schema_version", "status"}
    assert set(heartbeat) == {
        "agent_instance_id",
        "agent_key_id",
        "attempt_id",
        "attempt_lease_epoch",
        "grant_id",
        "request_nonce",
        "requested_at",
        "schema_version",
    }
    assert set(completion) == {
        "agent_instance_id",
        "agent_key_id",
        "attempt_id",
        "attempt_lease_epoch",
        "evidence",
        "failure_reason",
        "grant_id",
        "outcome",
        "request_nonce",
        "requested_at",
        "schema_version",
    }


@pytest.mark.parametrize(
    ("field", "value", "message"),
    (
        ("provider", "other-provider", "provider"),
        ("platform", "linux/amd64", "platform"),
        ("protocol_version", 2, "protocol"),
        ("host_architecture", "x86_64", "architecture"),
        ("agent_image", "repo/agent:latest", "image"),
        ("builder_image", "repo/builder:latest", "image"),
        ("runtime_profile_sha256", "C" * 64, "digest"),
        ("max_concurrency", 1, "concurrency"),
        ("readiness_evidence_sha256", "0" * 64, "digest"),
    ),
)
def test_native_builder_agent_status_rejects_identity_relaxation(
    field: str,
    value: object,
    message: str,
) -> None:
    """Relaxing a release/runtime identity must make an agent unavailable to claim."""
    with pytest.raises(ValueError, match=message):
        replace(_status(), **{field: value})


def test_native_builder_agent_status_requires_sorted_unique_bounded_inventory() -> None:
    """Duplicate, unsorted, or non-subset inventories could bypass two-slot admission."""
    with pytest.raises(ValueError, match="inventory"):
        replace(_status(), managed_grant_ids=(_SECOND_GRANT_ID, _GRANT_ID))
    with pytest.raises(ValueError, match="inventory"):
        replace(_status(), managed_grant_ids=(_GRANT_ID, _GRANT_ID))
    with pytest.raises(ValueError, match="inventory"):
        replace(_status(), managed_grant_ids=(_GRANT_ID,) * 3)
    with pytest.raises(ValueError, match="inventory"):
        replace(_status(), active_grant_ids=(_SECOND_GRANT_ID, _GRANT_ID))
    with pytest.raises(ValueError, match="inventory"):
        replace(_status(), managed_grant_ids=(), active_grant_ids=(_GRANT_ID,))


def test_native_builder_agent_availability_requires_exact_bounded_reason() -> None:
    """Availability and drift evidence must never contradict one another."""
    with pytest.raises(ValueError, match="availability"):
        replace(_status(), unavailable_reason="shape_drift")
    unavailable = replace(
        _status(),
        available=False,
        unavailable_reason="managed_resource_shape_drift",
        active_grant_ids=(),
    )
    assert unavailable.unavailable_reason == "managed_resource_shape_drift"
    with pytest.raises(ValueError, match="availability"):
        replace(unavailable, unavailable_reason=None)
    with pytest.raises(ValueError, match="reason"):
        replace(unavailable, unavailable_reason="secret\nvalue")


@pytest.mark.parametrize(
    ("factory", "field"),
    (
        (_poll, "requested_at"),
        (_heartbeat, "requested_at"),
        (_completion, "requested_at"),
        (_evidence, "observed_at"),
        (_grant, "capability_expires_at"),
    ),
)
def test_native_builder_protocol_rejects_naive_timestamps(factory, field: str) -> None:
    """A local-time timestamp must never enter freshness or lease comparisons."""
    with pytest.raises(ValueError, match="timezone"):
        replace(factory(), **{field: datetime(2026, 8, 30, 16, 0)})


def test_native_builder_grant_freezes_capability_fields_and_binds_identity() -> None:
    """A caller mutation must not alter the capability after grant construction."""
    fields = {"key": "bound/object", "policy": "secret"}
    grant = replace(_grant(), artifact_upload_fields=fields)
    fields["key"] = "other/object"

    assert grant.artifact_upload_fields == MappingProxyType(
        {"key": "bound/object", "policy": "secret"}
    )
    with pytest.raises(TypeError):
        grant.artifact_upload_fields["key"] = "other"  # type: ignore[index]
    with pytest.raises(ValueError, match="platform"):
        replace(grant, platform="linux/amd64")
    with pytest.raises(ValueError, match="lease"):
        replace(grant, attempt_lease_epoch=0)
    with pytest.raises(ValueError, match="URL"):
        replace(grant, source_get_url="http://objects.example/source")
    with pytest.raises(ValueError, match="contract"):
        replace(grant, contract_json='{"schema_version": 1}')


def test_native_builder_runtime_evidence_rejects_non_native_or_ambiguous_runtime() -> None:
    """Success evidence must identify two distinct exact gVisor sandboxes."""
    with pytest.raises(ValueError, match="runtime"):
        replace(_evidence(), runtime_name="runc")
    with pytest.raises(ValueError, match="container"):
        replace(_evidence(), buildkit_container_id="1" * 64)
    with pytest.raises(ValueError, match="exit"):
        replace(_evidence(), client_exit_code=-1)
    with pytest.raises(ValueError, match="restart"):
        replace(_evidence(), client_restart_count=1)


def test_native_builder_completion_enforces_exclusive_terminal_shape() -> None:
    """A completion must never express both success and failure or neither."""
    with pytest.raises(ValueError, match="completion"):
        replace(_completion(), failure_reason="failed")
    with pytest.raises(ValueError, match="completion"):
        replace(_completion(), evidence=None)
    failure = replace(
        _completion(),
        outcome="failed",
        failure_reason="client_exit_nonzero",
        evidence=None,
    )
    assert failure.outcome == "failed"
    with pytest.raises(ValueError, match="completion"):
        replace(failure, evidence=_evidence())
    with pytest.raises(ValueError, match="reason"):
        replace(failure, failure_reason="candidate output\nsecret")


def test_native_builder_dataclass_constructors_reject_unknown_fields() -> None:
    """A silently ignored future field would make the signed schema ambiguous."""
    values = dict(_heartbeat().__dict__) if hasattr(_heartbeat(), "__dict__") else {
        "agent_instance_id": _INSTANCE_ID,
        "agent_key_id": "gb10-native-builder-v1",
        "grant_id": _GRANT_ID,
        "attempt_id": _ATTEMPT_ID,
        "attempt_lease_epoch": 11,
        "requested_at": _NOW,
        "request_nonce": _NONCE,
    }
    values["unknown"] = True
    with pytest.raises(TypeError, match="unknown"):
        NativeBuilderHeartbeatRequest(**values)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("message_factory", "sign_method", "verify_method"),
    (
        (_poll, "sign_poll", "verify_poll"),
        (_heartbeat, "sign_heartbeat", "verify_heartbeat"),
        (_completion, "sign_completion", "verify_completion"),
    ),
)
def test_native_builder_signatures_authenticate_each_canonical_message_type(
    message_factory,
    sign_method: str,
    verify_method: str,
) -> None:
    """Dropping a type-specific signing method must break protocol authentication."""
    private_key, public_key = _keys()
    signer = PersonalDevNativeBuilderSigner(
        keys={"gb10-native-builder-v1": private_key}
    )
    verifier = PersonalDevNativeBuilderVerifier(
        keys={"gb10-native-builder-v1": public_key}
    )
    message = message_factory()
    signature = getattr(signer, sign_method)(message)

    assert len(signature) == 128
    assert signature == signature.lower()
    assert getattr(verifier, verify_method)(
        message,
        signature=signature,
        now=_NOW,
    ) == hashlib.sha256(message.canonical_bytes()).hexdigest()


def test_native_builder_signature_cannot_be_reused_for_another_message_type() -> None:
    """A valid poll signature must not authenticate a heartbeat or completion."""
    private_key, public_key = _keys()
    signer = PersonalDevNativeBuilderSigner(
        keys={"gb10-native-builder-v1": private_key}
    )
    verifier = PersonalDevNativeBuilderVerifier(
        keys={"gb10-native-builder-v1": public_key}
    )
    signature = signer.sign_poll(_poll())

    with pytest.raises(ValueError, match="signature"):
        verifier.verify_heartbeat(_heartbeat(), signature=signature, now=_NOW)
    with pytest.raises(ValueError, match="signature"):
        verifier.verify_completion(_completion(), signature=signature, now=_NOW)


@pytest.mark.parametrize(
    "tampered",
    (
        replace(_poll(), status=replace(_status(), agent_instance_id=_SECOND_GRANT_ID)),
        replace(_poll(), status=replace(_status(), runtime_profile_sha256="7" * 64)),
        replace(_poll(), status=replace(_status(), builder_image=_AGENT_IMAGE)),
    ),
)
def test_native_builder_poll_signature_detects_identity_tampering(
    tampered: NativeBuilderPollRequest,
) -> None:
    """Changing any claimed agent/runtime identity must invalidate the poll."""
    private_key, public_key = _keys()
    signer = PersonalDevNativeBuilderSigner(
        keys={"gb10-native-builder-v1": private_key}
    )
    verifier = PersonalDevNativeBuilderVerifier(
        keys={"gb10-native-builder-v1": public_key}
    )
    signature = signer.sign_poll(_poll())

    with pytest.raises(ValueError, match="signature"):
        verifier.verify_poll(tampered, signature=signature, now=_NOW)


@pytest.mark.parametrize(
    "tampered",
    (
        replace(_heartbeat(), grant_id=_SECOND_GRANT_ID),
        replace(_heartbeat(), attempt_lease_epoch=12),
        replace(_heartbeat(), agent_instance_id=_SECOND_GRANT_ID),
    ),
)
def test_native_builder_heartbeat_signature_detects_grant_tampering(
    tampered: NativeBuilderHeartbeatRequest,
) -> None:
    """Changing a grant or whole-attempt fence must invalidate its heartbeat."""
    private_key, public_key = _keys()
    signer = PersonalDevNativeBuilderSigner(
        keys={"gb10-native-builder-v1": private_key}
    )
    verifier = PersonalDevNativeBuilderVerifier(
        keys={"gb10-native-builder-v1": public_key}
    )
    signature = signer.sign_heartbeat(_heartbeat())

    with pytest.raises(ValueError, match="signature"):
        verifier.verify_heartbeat(tampered, signature=signature, now=_NOW)


def test_native_builder_completion_signature_detects_outcome_and_evidence_tampering() -> None:
    """A signed success cannot be converted into a failure or different runtime."""
    private_key, public_key = _keys()
    signer = PersonalDevNativeBuilderSigner(
        keys={"gb10-native-builder-v1": private_key}
    )
    verifier = PersonalDevNativeBuilderVerifier(
        keys={"gb10-native-builder-v1": public_key}
    )
    signature = signer.sign_completion(_completion())
    changed_evidence = replace(
        _completion(),
        evidence=replace(_evidence(), network_inspect_sha256="8" * 64),
    )
    changed_outcome = replace(
        _completion(),
        outcome="failed",
        failure_reason="client_exit_nonzero",
        evidence=None,
    )

    with pytest.raises(ValueError, match="signature"):
        verifier.verify_completion(changed_evidence, signature=signature, now=_NOW)
    with pytest.raises(ValueError, match="signature"):
        verifier.verify_completion(changed_outcome, signature=signature, now=_NOW)


def test_native_builder_verifier_enforces_exact_freshness_window_and_key() -> None:
    """Captured, future, unknown-key, or wrong-key messages must not authenticate."""
    private_key, public_key = _keys()
    other_private, other_public = _keys()
    signer = PersonalDevNativeBuilderSigner(
        keys={"gb10-native-builder-v1": private_key}
    )
    verifier = PersonalDevNativeBuilderVerifier(
        keys={"gb10-native-builder-v1": public_key}
    )
    signature = signer.sign_poll(_poll())

    with pytest.raises(ValueError, match="freshness"):
        verifier.verify_poll(
            _poll(),
            signature=signature,
            now=_NOW + timedelta(seconds=61),
        )
    with pytest.raises(ValueError, match="freshness"):
        verifier.verify_poll(
            _poll(),
            signature=signature,
            now=_NOW - timedelta(seconds=16),
        )
    with pytest.raises(ValueError, match="key"):
        verifier.verify_poll(
            replace(
                _poll(),
                status=replace(_status(), agent_key_id="unknown-native-builder"),
            ),
            signature=signature,
            now=_NOW,
        )
    wrong_verifier = PersonalDevNativeBuilderVerifier(
        keys={"gb10-native-builder-v1": other_public}
    )
    with pytest.raises(ValueError, match="signature"):
        wrong_verifier.verify_poll(_poll(), signature=signature, now=_NOW)
    wrong_signer = PersonalDevNativeBuilderSigner(
        keys={"gb10-native-builder-v1": other_private}
    )
    with pytest.raises(ValueError, match="signature"):
        verifier.verify_poll(
            _poll(),
            signature=wrong_signer.sign_poll(_poll()),
            now=_NOW,
        )


def test_native_builder_verifier_rejects_malformed_signature_and_naive_now() -> None:
    """Malformed authentication inputs must fail before any payload is accepted."""
    private_key, public_key = _keys()
    signer = PersonalDevNativeBuilderSigner(
        keys={"gb10-native-builder-v1": private_key}
    )
    verifier = PersonalDevNativeBuilderVerifier(
        keys={"gb10-native-builder-v1": public_key}
    )

    with pytest.raises(ValueError, match="signature"):
        verifier.verify_poll(_poll(), signature="A" * 128, now=_NOW)
    with pytest.raises(ValueError, match="timezone"):
        verifier.verify_poll(
            _poll(),
            signature=signer.sign_poll(_poll()),
            now=datetime(2026, 8, 30, 16, 0),
        )


def test_native_builder_key_loaders_separate_read_only_verification_from_signing(
    tmp_path,
) -> None:
    """The management public-key loader must never gain private signing authority."""
    private_key, public_key = _keys()
    private_file = tmp_path / "native-builder.key"
    private_file.write_bytes(private_key)
    private_file.chmod(0o400)
    public_file = tmp_path / "native-builder.pub"
    public_file.write_bytes(public_key)
    public_file.chmod(0o440)

    signer = load_personal_dev_native_builder_signer(
        private_file,
        key_id="gb10-native-builder-v1",
    )
    verifier = load_personal_dev_native_builder_verifier(
        public_file,
        key_id="gb10-native-builder-v1",
        expected_sha256=hashlib.sha256(public_key).hexdigest(),
    )
    signature = signer.sign_completion(_completion())

    assert verifier.verify_completion(_completion(), signature=signature, now=_NOW)
    assert signer.public_key_bytes("gb10-native-builder-v1") == public_key
    private_file.chmod(0o600)
    with pytest.raises(RuntimeError, match="owner-only"):
        load_personal_dev_native_builder_signer(
            private_file,
            key_id="gb10-native-builder-v1",
        )


def test_native_builder_public_key_loader_rejects_digest_link_and_mode_drift(tmp_path) -> None:
    """A substituted, linked, or writable verification key must fail closed."""
    _private_key, public_key = _keys()
    public_file = tmp_path / "native-builder.pub"
    public_file.write_bytes(public_key)
    public_file.chmod(0o400)

    with pytest.raises(RuntimeError, match="digest"):
        load_personal_dev_native_builder_verifier(
            public_file,
            key_id="gb10-native-builder-v1",
            expected_sha256="1" * 64,
        )
    link = tmp_path / "native-builder-link.pub"
    link.symlink_to(public_file)
    with pytest.raises(RuntimeError, match="regular file"):
        load_personal_dev_native_builder_verifier(
            link,
            key_id="gb10-native-builder-v1",
        )
    link.unlink()
    hardlink = tmp_path / "native-builder-hardlink.pub"
    os.link(public_file, hardlink)
    with pytest.raises(RuntimeError, match="read-only"):
        load_personal_dev_native_builder_verifier(
            public_file,
            key_id="gb10-native-builder-v1",
        )
    hardlink.unlink()
    public_file.chmod(0o640)
    with pytest.raises(RuntimeError, match="read-only"):
        load_personal_dev_native_builder_verifier(
            public_file,
            key_id="gb10-native-builder-v1",
        )
