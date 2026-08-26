from __future__ import annotations

import base64
import hashlib
from datetime import UTC, datetime, timedelta

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from loom.execution_image_admission import (
    ExecutionImageAdmissionBundleV1,
    ImageAdmissionKeyring,
    ImageAdmissionStatementV1,
    SignedImageAdmissionV1,
)
from loom.pipeline.keys import canonical_document

_PRIVATE_KEY = Ed25519PrivateKey.from_private_bytes(b"\x15" * 32)
IMAGE_ADMISSION_KEYRING = ImageAdmissionKeyring({"test-builder": _PRIVATE_KEY.public_key()})


def signed_image_admission_bundle(
    image_refs: tuple[str, ...],
    *,
    now: datetime | None = None,
    severity: str = "medium",
) -> ExecutionImageAdmissionBundleV1:
    issued_at = (now or datetime.now(UTC)).astimezone(UTC)
    admissions: list[SignedImageAdmissionV1] = []
    for index, image_ref in enumerate(dict.fromkeys(image_refs), start=1):
        evidence = [
            "sha256:" + hashlib.sha256(f"{index}:{kind}".encode()).hexdigest()
            for kind in ("sbom", "provenance", "vulnerability")
        ]
        statement = ImageAdmissionStatementV1(
            schema_version="loom.image-admission-statement.v1",
            image_ref=image_ref,
            platform="linux/x86_64",
            sbom_sha256=evidence[0],
            provenance_sha256=evidence[1],
            vulnerability_report_sha256=evidence[2],
            policy_sha256="sha256:" + "a" * 64,
            highest_vulnerability_severity=severity,
            issued_at=issued_at,
            expires_at=issued_at + timedelta(hours=1),
        )
        signature = _PRIVATE_KEY.sign(canonical_document(statement.model_dump(mode="json")))
        admissions.append(
            SignedImageAdmissionV1(
                statement=statement,
                signing_key_id="test-builder",
                signature_base64=base64.b64encode(signature).decode("ascii"),
            )
        )
    return ExecutionImageAdmissionBundleV1(
        schema_version="loom.execution-image-admission.v1",
        admissions=tuple(admissions),
    )
