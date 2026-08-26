from __future__ import annotations

import base64
import json
from datetime import UTC, datetime, timedelta

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from loom.execution_image_admission import (
    ExecutionImageAdmissionBundleV1,
    ImageAdmissionError,
    ImageAdmissionKeyring,
    SignedImageAdmissionV1,
    verify_execution_image_admission,
)
from tests.support.execution_image_admission import (
    IMAGE_ADMISSION_KEYRING,
    signed_image_admission_bundle,
)

_TASK = "registry.example/task@sha256:" + "a" * 64
_RUNTIME = "registry.example/runtime@sha256:" + "b" * 64


def test_signed_image_admission_verifies_exact_runtime_images() -> None:
    now = datetime.now(UTC)
    bundle = signed_image_admission_bundle((_TASK, _RUNTIME), now=now)

    verify_execution_image_admission(
        bundle,
        required_image_refs=(_TASK, _RUNTIME),
        keyring=IMAGE_ADMISSION_KEYRING,
        now=now,
    )


def test_image_admission_rejects_tamper_missing_image_and_unknown_key() -> None:
    now = datetime.now(UTC)
    bundle = signed_image_admission_bundle((_TASK, _RUNTIME), now=now)
    first = bundle.admissions[0]
    tampered = SignedImageAdmissionV1(
        statement=first.statement.model_copy(update={"sbom_sha256": "sha256:" + "f" * 64}),
        signing_key_id=first.signing_key_id,
        signature_base64=first.signature_base64,
    )
    tampered_bundle = ExecutionImageAdmissionBundleV1(
        schema_version=bundle.schema_version,
        admissions=(tampered, bundle.admissions[1]),
    )
    with pytest.raises(ImageAdmissionError, match="signature"):
        verify_execution_image_admission(
            tampered_bundle,
            required_image_refs=(_TASK, _RUNTIME),
            keyring=IMAGE_ADMISSION_KEYRING,
            now=now,
        )
    with pytest.raises(ImageAdmissionError, match="coverage"):
        verify_execution_image_admission(
            bundle,
            required_image_refs=(_TASK, _RUNTIME, "registry.example/sidecar@sha256:" + "c" * 64),
            keyring=IMAGE_ADMISSION_KEYRING,
            now=now,
        )
    unknown = bundle.model_copy(
        update={
            "admissions": (
                first.model_copy(update={"signing_key_id": "unknown-builder"}),
                bundle.admissions[1],
            )
        }
    )
    with pytest.raises(ImageAdmissionError, match="signature"):
        verify_execution_image_admission(
            unknown,
            required_image_refs=(_TASK, _RUNTIME),
            keyring=IMAGE_ADMISSION_KEYRING,
            now=now,
        )


def test_image_admission_rejects_high_severity_and_expiry() -> None:
    now = datetime.now(UTC)
    high = signed_image_admission_bundle((_TASK, _RUNTIME), now=now, severity="high")
    with pytest.raises(ImageAdmissionError, match="vulnerability"):
        verify_execution_image_admission(
            high,
            required_image_refs=(_TASK, _RUNTIME),
            keyring=IMAGE_ADMISSION_KEYRING,
            now=now,
        )
    expired = signed_image_admission_bundle((_TASK, _RUNTIME), now=now - timedelta(hours=2))
    with pytest.raises(ImageAdmissionError, match="expired"):
        verify_execution_image_admission(
            expired,
            required_image_refs=(_TASK, _RUNTIME),
            keyring=IMAGE_ADMISSION_KEYRING,
            now=now,
        )


def test_image_admission_keyring_json_is_strict_and_bounded() -> None:
    private_key = Ed25519PrivateKey.generate()
    public_bytes = private_key.public_key().public_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PublicFormat.Raw,
    )
    raw = json.dumps(
        {
            "schema_version": 1,
            "keys": [
                {
                    "signing_key_id": "builder-2026-08",
                    "public_key_base64": base64.b64encode(public_bytes).decode("ascii"),
                }
            ],
        }
    )
    assert isinstance(ImageAdmissionKeyring.from_json(raw), ImageAdmissionKeyring)
    with pytest.raises(ImageAdmissionError, match="duplicate JSON"):
        ImageAdmissionKeyring.from_json('{"schema_version":1,"schema_version":1,"keys":[]}')
