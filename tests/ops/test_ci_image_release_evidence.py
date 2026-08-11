from __future__ import annotations

import json
import subprocess
import sys
from copy import deepcopy
from pathlib import Path
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
_CANDIDATE_HEAD = "1" * 40
_CANDIDATE_TREE = "2" * 40
_TRIVY_ACTION = (
    "aquasecurity/trivy-action@ed142fd0673e97e23eac54620cfb913e5ce36c25"
)
_SCRIPT = Path(__file__).resolve().parents[2] / "scripts/ci_image_release_evidence.py"


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
        build_mode="trusted-rebuild",
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
    assert external["build"] == {"mode": "trusted-rebuild"}
    assert external["scan"] == {
        "action": _TRIVY_ACTION,
        "scan_type": "image",
        "vuln_type": ["os", "library"],
        "timeout": "10m0s",
        "severity": ["CRITICAL"],
        "exit_code": 1,
        "ignore_unfixed": False,
        "scanners": ["vuln"],
        "cache": False,
        "report_sha256": _SCAN,
    }
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
        build_mode="trusted-rebuild",
    )

    assert verify_architecture_attestation(
        _verification(predicate),
        **_common(),
        platform="linux/amd64",
        architecture="amd64",
        subject_name="ghcr.io/qianyi-sun/loom-capacity-manager",
        subject_digest=f"sha256:{_DIGEST}",
        build_mode="trusted-rebuild",
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
            build_mode="trusted-rebuild",
        )


def test_architecture_verification_rejects_a_different_registry_subject() -> None:
    predicate = architecture_predicate(
        **_common(),
        platform="linux/amd64",
        architecture="amd64",
        scan_report_sha256=_SCAN,
        build_mode="trusted-rebuild",
    )
    verification = _verification(predicate)
    statement: Any = verification[0]["verificationResult"]
    statement["statement"]["subject"][0]["name"] = (
        "ghcr.io/different-owner/loom-capacity-manager"
    )

    with pytest.raises(EvidenceError, match="expected release image"):
        verify_architecture_attestation(
            verification,
            **_common(),
            platform="linux/amd64",
            architecture="amd64",
            subject_name="ghcr.io/different-owner/loom-capacity-manager",
            subject_digest=f"sha256:{_DIGEST}",
            build_mode="trusted-rebuild",
        )


def test_candidate_predicate_additionally_binds_resolver_source() -> None:
    predicate = architecture_predicate(
        **_common(),
        platform="linux/arm64",
        architecture="arm64",
        scan_report_sha256=_SCAN,
        build_mode="verified-pr-candidate",
        candidate_head_sha=_CANDIDATE_HEAD,
        candidate_tree_sha=_CANDIDATE_TREE,
        candidate_run_id=456,
        candidate_run_attempt=3,
    )

    external: Any = predicate["buildDefinition"]
    external = external["externalParameters"]
    assert external["source"]["commit"] == _SHA
    assert external["source"]["tree"] == _TREE
    assert external["build"] == {
        "mode": "verified-pr-candidate",
        "candidate_source": {
            "commit": _CANDIDATE_HEAD,
            "tree": _CANDIDATE_TREE,
            "run_id": 456,
            "run_attempt": 3,
        },
    }


@pytest.mark.parametrize("build_mode", ["", "candidate", "trusted", "TRUSTED-REBUILD"])
def test_architecture_predicate_rejects_malformed_build_mode(build_mode: str) -> None:
    with pytest.raises(EvidenceError, match="build mode"):
        architecture_predicate(
            **_common(),
            platform="linux/amd64",
            architecture="amd64",
            scan_report_sha256=_SCAN,
            build_mode=build_mode,
        )


def test_candidate_mode_requires_complete_candidate_source() -> None:
    with pytest.raises(EvidenceError, match="candidate source"):
        architecture_predicate(
            **_common(),
            platform="linux/amd64",
            architecture="amd64",
            scan_report_sha256=_SCAN,
            build_mode="verified-pr-candidate",
            candidate_head_sha=_CANDIDATE_HEAD,
            candidate_tree_sha=_CANDIDATE_TREE,
            candidate_run_id=456,
        )


def test_trusted_rebuild_rejects_candidate_source() -> None:
    with pytest.raises(EvidenceError, match="candidate source"):
        architecture_predicate(
            **_common(),
            platform="linux/amd64",
            architecture="amd64",
            scan_report_sha256=_SCAN,
            build_mode="trusted-rebuild",
            candidate_head_sha=_CANDIDATE_HEAD,
            candidate_tree_sha=_CANDIDATE_TREE,
            candidate_run_id=456,
            candidate_run_attempt=3,
        )


def test_candidate_verification_rejects_tampered_source_mode() -> None:
    predicate = architecture_predicate(
        **_common(),
        platform="linux/amd64",
        architecture="amd64",
        scan_report_sha256=_SCAN,
        build_mode="verified-pr-candidate",
        candidate_head_sha=_CANDIDATE_HEAD,
        candidate_tree_sha=_CANDIDATE_TREE,
        candidate_run_id=456,
        candidate_run_attempt=3,
    )
    tampered = _verification(deepcopy(predicate))
    tampered_payload: Any = tampered
    tampered_payload[0]["verificationResult"]["statement"]["predicate"][
        "buildDefinition"
    ]["externalParameters"]["build"]["mode"] = "trusted-rebuild"

    with pytest.raises(EvidenceError):
        verify_architecture_attestation(
            tampered,
            **_common(),
            platform="linux/amd64",
            architecture="amd64",
            subject_name="ghcr.io/qianyi-sun/loom-capacity-manager",
            subject_digest=f"sha256:{_DIGEST}",
            build_mode="verified-pr-candidate",
            candidate_head_sha=_CANDIDATE_HEAD,
            candidate_tree_sha=_CANDIDATE_TREE,
            candidate_run_id=456,
            candidate_run_attempt=3,
        )


def test_manifest_attestation_binds_both_verified_architecture_subjects() -> None:
    predicate = manifest_predicate(
        **_common(),
        architecture_digests={
            "linux/amd64": f"sha256:{_AMD64_DIGEST}",
            "linux/arm64": f"sha256:{_ARM64_DIGEST}",
        },
        scan_report_digests={"linux/amd64": _SCAN, "linux/arm64": "1" * 64},
        architecture_builds={
            "linux/amd64": {"mode": "trusted-rebuild"},
            "linux/arm64": {
                "mode": "verified-pr-candidate",
                "candidate_source": {
                    "commit": _CANDIDATE_HEAD,
                    "tree": _CANDIDATE_TREE,
                    "run_id": 456,
                    "run_attempt": 3,
                },
            },
        },
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
        architecture_builds={
            "linux/amd64": {"mode": "trusted-rebuild"},
            "linux/arm64": {
                "mode": "verified-pr-candidate",
                "candidate_source": {
                    "commit": _CANDIDATE_HEAD,
                    "tree": _CANDIDATE_TREE,
                    "run_id": 456,
                    "run_attempt": 3,
                },
            },
        },
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
            architecture_builds={
                "linux/amd64": {"mode": "trusted-rebuild"},
                "linux/arm64": {
                    "mode": "verified-pr-candidate",
                    "candidate_source": {
                        "commit": _CANDIDATE_HEAD,
                        "tree": _CANDIDATE_TREE,
                        "run_id": 456,
                        "run_attempt": 3,
                    },
                },
            },
        )


def _cli_common() -> list[str]:
    common = _common()
    return [
        "--repository",
        common["repository"],
        "--ref-name",
        common["ref_name"],
        "--head-sha",
        common["head_sha"],
        "--tree-sha",
        common["tree_sha"],
        "--run-id",
        str(common["run_id"]),
        "--run-attempt",
        str(common["run_attempt"]),
        "--image",
        common["image"],
        "--image-name",
        common["image_name"],
        "--dockerfile",
        common["dockerfile"],
        "--build-context",
        common["build_context"],
    ]


def _run_cli(*arguments: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(_SCRIPT), *arguments],
        text=True,
        capture_output=True,
        check=False,
    )


def _write_record(
    output: Path,
    *,
    architecture: str,
    subject_digest: str,
    scan_digest: str,
) -> subprocess.CompletedProcess[str]:
    return _run_cli(
        "record-architecture",
        *_cli_common(),
        "--platform",
        f"linux/{architecture}",
        "--architecture",
        architecture,
        "--subject-name",
        "ghcr.io/qianyi-sun/loom-capacity-manager",
        "--subject-digest",
        f"sha256:{subject_digest}",
        "--scan-report-sha256",
        scan_digest,
        "--build-mode",
        "trusted-rebuild",
        "--output",
        str(output),
    )


def test_cli_validates_exact_canonical_architecture_record_set(tmp_path: Path) -> None:
    records = tmp_path / "records"
    records.mkdir()
    amd64 = records / "capacity-manager-amd64.json"
    arm64 = records / "capacity-manager-arm64.json"
    assert _write_record(
        amd64,
        architecture="amd64",
        subject_digest=_AMD64_DIGEST,
        scan_digest=_SCAN,
    ).returncode == 0
    assert _write_record(
        arm64,
        architecture="arm64",
        subject_digest=_ARM64_DIGEST,
        scan_digest="1" * 64,
    ).returncode == 0
    output = tmp_path / "validated.json"

    result = _run_cli(
        "validate-architecture-records",
        *_cli_common(),
        "--records-dir",
        str(records),
        "--output",
        str(output),
    )

    assert result.returncode == 0, result.stderr
    assert json.loads(output.read_text(encoding="utf-8")) == {
        "architectures": {
            "amd64": {
                "build": {"mode": "trusted-rebuild"},
                "platform": "linux/amd64",
                "scan_report_sha256": _SCAN,
                "subject_digest": f"sha256:{_AMD64_DIGEST}",
            },
            "arm64": {
                "build": {"mode": "trusted-rebuild"},
                "platform": "linux/arm64",
                "scan_report_sha256": "1" * 64,
                "subject_digest": f"sha256:{_ARM64_DIGEST}",
            },
        },
        "subject_name": "ghcr.io/qianyi-sun/loom-capacity-manager",
    }


def test_cli_validates_one_canonical_architecture_record(tmp_path: Path) -> None:
    record = tmp_path / "capacity-manager-amd64.json"
    assert _write_record(
        record,
        architecture="amd64",
        subject_digest=_AMD64_DIGEST,
        scan_digest=_SCAN,
    ).returncode == 0

    result = _run_cli(
        "validate-architecture-record",
        *_cli_common(),
        "--platform",
        "linux/amd64",
        "--architecture",
        "amd64",
        "--record",
        str(record),
    )

    assert result.returncode == 0, result.stderr


def test_cli_rejects_extra_or_tampered_architecture_records(tmp_path: Path) -> None:
    records = tmp_path / "records"
    records.mkdir()
    for architecture, digest, scan in (
        ("amd64", _AMD64_DIGEST, _SCAN),
        ("arm64", _ARM64_DIGEST, "1" * 64),
    ):
        result = _write_record(
            records / f"capacity-manager-{architecture}.json",
            architecture=architecture,
            subject_digest=digest,
            scan_digest=scan,
        )
        assert result.returncode == 0, result.stderr

    (records / "unexpected.json").write_text("{}\n", encoding="utf-8")
    result = _run_cli(
        "validate-architecture-records",
        *_cli_common(),
        "--records-dir",
        str(records),
    )
    assert result.returncode != 0
    assert "exactly the expected architecture files" in result.stderr
    (records / "unexpected.json").unlink()

    record = json.loads(
        (records / "capacity-manager-amd64.json").read_text(encoding="utf-8")
    )
    record["release"]["tree"] = "0" * 40
    (records / "capacity-manager-amd64.json").write_text(
        json.dumps(record),
        encoding="utf-8",
    )
    result = _run_cli(
        "validate-architecture-records",
        *_cli_common(),
        "--records-dir",
        str(records),
    )
    assert result.returncode != 0
    assert "canonical architecture record" in result.stderr


def test_cli_rejects_record_for_a_different_registry_subject(tmp_path: Path) -> None:
    result = _run_cli(
        "record-architecture",
        *_cli_common(),
        "--platform",
        "linux/amd64",
        "--architecture",
        "amd64",
        "--subject-name",
        "ghcr.io/different-owner/loom-capacity-manager",
        "--subject-digest",
        f"sha256:{_AMD64_DIGEST}",
        "--scan-report-sha256",
        _SCAN,
        "--build-mode",
        "trusted-rebuild",
        "--output",
        str(tmp_path / "record.json"),
    )

    assert result.returncode != 0
    assert "expected release image" in result.stderr
