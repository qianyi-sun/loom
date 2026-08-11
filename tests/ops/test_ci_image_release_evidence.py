from __future__ import annotations

from copy import deepcopy
from typing import Any, TypedDict

import pytest
from scripts.ci_image_release_evidence import (
    EvidenceError,
    architecture_predicate,
    manifest_predicate,
    verify_architecture_attestation,
    verify_manifest_attestation,
)

_SHA = "a" * 40
_TREE = "b" * 40
_DIGEST = "c" * 64
_SCAN = "d" * 64
_AMD64_DIGEST = "e" * 64
_ARM64_DIGEST = "f" * 64


class _Common(TypedDict):
    repository: str
    ref_name: str
    head_sha: str
    tree_sha: str
    run_id: int
    run_attempt: int
    image: str
    image_name: str
    dockerfile: str
    build_context: str


def _common() -> _Common:
    return {
        "repository": "qianyi-sun/loom",
        "ref_name": "dev",
        "head_sha": _SHA,
        "tree_sha": _TREE,
        "run_id": 123,
        "run_attempt": 2,
        "image": "capacity-manager",
        "image_name": "loom-capacity-manager",
        "dockerfile": "deploy/Dockerfile.capacity-manager",
        "build_context": ".",
    }


def _verification(
    predicate: dict[str, object],
    *,
    digest: str = _DIGEST,
) -> list[dict[str, object]]:
    return [
        {
            "verificationResult": {
                "statement": {
                    "_type": "https://in-toto.io/Statement/v1",
                    "subject": [
                        {
                            "name": "ghcr.io/qianyi-sun/loom-capacity-manager",
                            "digest": {"sha256": digest},
                        }
                    ],
                    "predicateType": "https://slsa.dev/provenance/v1",
                    "predicate": predicate,
                }
            }
        }
    ]


def test_architecture_predicate_binds_source_build_scan_and_invocation() -> None:
    predicate = architecture_predicate(
        **_common(),
        platform="linux/amd64",
        architecture="amd64",
        scan_report_sha256=_SCAN,
    )

    external: Any = predicate["buildDefinition"]
    external = external["externalParameters"]
    assert external["source"] == {
        "repository": "qianyi-sun/loom",
        "ref": "refs/heads/dev",
        "commit": _SHA,
        "tree": _TREE,
    }
    assert external["image"] == {
        "component": "capacity-manager",
        "repository": "loom-capacity-manager",
        "dockerfile": "deploy/Dockerfile.capacity-manager",
        "context": ".",
        "platform": "linux/amd64",
        "architecture": "amd64",
    }
    assert external["scan"]["report_sha256"] == _SCAN
    run_details: Any = predicate["runDetails"]
    assert run_details["metadata"]["invocationId"].endswith(
        "/actions/runs/123/attempts/2"
    )


def test_architecture_verification_is_exact_and_returns_the_scan_digest() -> None:
    predicate = architecture_predicate(
        **_common(),
        platform="linux/amd64",
        architecture="amd64",
        scan_report_sha256=_SCAN,
    )

    assert verify_architecture_attestation(
        _verification(predicate),
        **_common(),
        platform="linux/amd64",
        architecture="amd64",
        subject_name="ghcr.io/qianyi-sun/loom-capacity-manager",
        subject_digest=f"sha256:{_DIGEST}",
    ) == _SCAN

    tampered = _verification(deepcopy(predicate))
    tampered_payload: Any = tampered
    tampered_payload[0]["verificationResult"]["statement"]["predicate"][
        "buildDefinition"
    ]["externalParameters"]["source"]["tree"] = "0" * 40
    with pytest.raises(EvidenceError):
        verify_architecture_attestation(
            tampered,
            **_common(),
            platform="linux/amd64",
            architecture="amd64",
            subject_name="ghcr.io/qianyi-sun/loom-capacity-manager",
            subject_digest=f"sha256:{_DIGEST}",
        )


def test_manifest_attestation_binds_both_verified_architecture_subjects() -> None:
    predicate = manifest_predicate(
        **_common(),
        architecture_digests={
            "linux/amd64": f"sha256:{_AMD64_DIGEST}",
            "linux/arm64": f"sha256:{_ARM64_DIGEST}",
        },
        scan_report_digests={"linux/amd64": _SCAN, "linux/arm64": "1" * 64},
    )
    verification = _verification(predicate)

    verify_manifest_attestation(
        verification,
        **_common(),
        subject_name="ghcr.io/qianyi-sun/loom-capacity-manager",
        subject_digest=f"sha256:{_DIGEST}",
        architecture_digests={
            "linux/amd64": f"sha256:{_AMD64_DIGEST}",
            "linux/arm64": f"sha256:{_ARM64_DIGEST}",
        },
        scan_report_digests={"linux/amd64": _SCAN, "linux/arm64": "1" * 64},
    )

    duplicated = verification * 2
    with pytest.raises(EvidenceError, match="exactly one"):
        verify_manifest_attestation(
            duplicated,
            **_common(),
            subject_name="ghcr.io/qianyi-sun/loom-capacity-manager",
            subject_digest=f"sha256:{_DIGEST}",
            architecture_digests={
                "linux/amd64": f"sha256:{_AMD64_DIGEST}",
                "linux/arm64": f"sha256:{_ARM64_DIGEST}",
            },
            scan_report_digests={"linux/amd64": _SCAN, "linux/arm64": "1" * 64},
        )
