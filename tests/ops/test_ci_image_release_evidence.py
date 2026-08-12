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
_TRIVY_RELEASE = "https://github.com/aquasecurity/trivy/releases/download/v0.70.0"
_TRIVY_AMD64 = "8b4376d5d6befe5c24d503f10ff136d9e0c49f9127a4279fd110b727929a5aa9"
_TRIVY_ARM64 = "2f6bb988b553a1bbac6bdd1ce890f5e412439564e17522b88a4541b4f364fc8d"
_SCRIPT = Path(__file__).resolve().parents[2] / "scripts/ci_image_release_evidence.py"
_PERL_PURLS = [
    "pkg:deb/debian/libperl5.36",
    "pkg:deb/debian/libperl5.40",
    "pkg:deb/debian/perl",
    "pkg:deb/debian/perl-base",
    "pkg:deb/debian/perl-modules-5.36",
    "pkg:deb/debian/perl-modules-5.40",
]
_PERL_STATEMENT = (
    "No fixed Debian package was available on 2026-08-12; these Perl packages are "
    "required by Debian base runtimes, the agent toolchain, and the staging-compatible "
    "PostgreSQL 17.4 rehearsal image."
)
_POSTGRES_STATEMENT = (
    "No fixed Debian package was available on 2026-08-12; this package is a required "
    "dependency of the staging-compatible PostgreSQL 17.4 rehearsal image."
)
_TRIVY_EXCEPTIONS = [
    {
        "id": "CVE-2023-45853",
        "purls": ["pkg:deb/debian/zlib1g"],
        "expires_at": "2026-09-12",
        "statement": (
            "Debian marked this finding will-not-fix on 2026-08-12; zlib1g is a "
            "required dependency of the staging-compatible PostgreSQL 17.4 rehearsal "
            "image."
        ),
    },
    {
        "id": "CVE-2025-7458",
        "purls": ["pkg:deb/debian/libsqlite3-0"],
        "expires_at": "2026-09-12",
        "statement": _POSTGRES_STATEMENT,
    },
    *[
    {
        "id": vulnerability_id,
        "purls": _PERL_PURLS,
        "expires_at": "2026-09-12",
        "statement": _PERL_STATEMENT,
    }
    for vulnerability_id in (
        "CVE-2026-13221",
        "CVE-2026-42496",
    )
    ],
    {
        "id": "CVE-2026-43185",
        "purls": ["pkg:deb/debian/linux-libc-dev"],
        "expires_at": "2026-09-12",
        "statement": (
            "No fixed Debian package was available on 2026-08-12; linux-libc-dev is "
            "required by the agent sandbox compiler toolchain."
        ),
    },
    {
        "id": "CVE-2026-57433",
        "purls": _PERL_PURLS,
        "expires_at": "2026-09-12",
        "statement": _PERL_STATEMENT,
    },
    {
        "id": "CVE-2026-6653",
        "purls": ["pkg:deb/debian/libxml2"],
        "expires_at": "2026-09-12",
        "statement": _POSTGRES_STATEMENT,
    },
    {
        "id": "CVE-2026-8376",
        "purls": _PERL_PURLS,
        "expires_at": "2026-09-12",
        "statement": _PERL_STATEMENT,
    },
]


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
        "scanner": {
            "name": "Trivy",
            "version": "v0.70.0",
            "release": _TRIVY_RELEASE,
            "archives": {"linux/amd64": _TRIVY_AMD64},
        },
        "config_sha256": ("35492da1d08b142bd1489ac54ecdedab62634b7b3095a37cebbe10b61df1adac"),
        "ignore_sha256": ("b09bd1a38036f5e4274586af64616a306590ec33b1e2ac8a73d67ab88d2e4d5a"),
        "scan_type": "image",
        "vuln_type": ["os", "library"],
        "timeout": "10m0s",
        "severity": ["CRITICAL"],
        "exit_code": 1,
        "ignore_unfixed": False,
        "exceptions": _TRIVY_EXCEPTIONS,
        "scanners": ["vuln"],
        "cache": False,
        "report_sha256": _SCAN,
    }
    run_details: Any = predicate["runDetails"]
    assert run_details["metadata"]["invocationId"].endswith("/actions/runs/123/attempts/2")


def test_official_architecture_evidence_rejects_pr_candidate_bytes() -> None:
    with pytest.raises(EvidenceError, match="build mode"):
        architecture_predicate(
            **_common(),
            platform="linux/arm64",
            architecture="arm64",
            scan_report_sha256=_SCAN,
            build_mode="verified-pr-candidate",
        )


def test_official_manifest_evidence_rejects_pr_candidate_build_identity() -> None:
    with pytest.raises(EvidenceError, match="build mode"):
        manifest_predicate(
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


def test_architecture_verification_is_exact_and_returns_the_scan_digest() -> None:
    predicate = architecture_predicate(
        **_common(),
        platform="linux/amd64",
        architecture="amd64",
        scan_report_sha256=_SCAN,
        build_mode="trusted-rebuild",
    )

    assert (
        verify_architecture_attestation(
            _verification(predicate),
            **_common(),
            platform="linux/amd64",
            architecture="amd64",
            subject_name="ghcr.io/qianyi-sun/loom-capacity-manager",
            subject_digest=f"sha256:{_DIGEST}",
            build_mode="trusted-rebuild",
        )
        == _SCAN
    )

    tampered = _verification(deepcopy(predicate))
    tampered_payload: Any = tampered
    tampered_payload[0]["verificationResult"]["statement"]["predicate"]["buildDefinition"][
        "externalParameters"
    ]["source"]["tree"] = "0" * 40
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
    statement["statement"]["subject"][0]["name"] = "ghcr.io/different-owner/loom-capacity-manager"

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


@pytest.mark.parametrize(
    ("path", "replacement"),
    [
        (("scanner", "name"), "Other"),
        (("scanner", "version"), "v0.70.1"),
        (("config_sha256",), "0" * 64),
        (("ignore_sha256",), "0" * 64),
    ],
)
def test_architecture_verification_rejects_scan_tool_or_policy_drift(
    path: tuple[str, ...],
    replacement: str,
) -> None:
    predicate = architecture_predicate(
        **_common(),
        platform="linux/amd64",
        architecture="amd64",
        scan_report_sha256=_SCAN,
        build_mode="trusted-rebuild",
    )
    tampered = _verification(deepcopy(predicate))
    tampered_payload: Any = tampered
    scan = tampered_payload[0]["verificationResult"]["statement"]["predicate"]["buildDefinition"][
        "externalParameters"
    ]["scan"]
    target = scan
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = replacement

    with pytest.raises(EvidenceError, match="exactly one"):
        verify_architecture_attestation(
            tampered,
            **_common(),
            platform="linux/amd64",
            architecture="amd64",
            subject_name="ghcr.io/qianyi-sun/loom-capacity-manager",
            subject_digest=f"sha256:{_DIGEST}",
            build_mode="trusted-rebuild",
        )


def test_architecture_verification_rejects_boolean_for_integer_policy_field() -> None:
    predicate = architecture_predicate(
        **_common(),
        platform="linux/amd64",
        architecture="amd64",
        scan_report_sha256=_SCAN,
        build_mode="trusted-rebuild",
    )
    tampered = _verification(deepcopy(predicate))
    tampered_payload: Any = tampered
    tampered_payload[0]["verificationResult"]["statement"]["predicate"]["buildDefinition"][
        "externalParameters"
    ]["scan"]["exit_code"] = True

    with pytest.raises(EvidenceError, match="exactly one"):
        verify_architecture_attestation(
            tampered,
            **_common(),
            platform="linux/amd64",
            architecture="amd64",
            subject_name="ghcr.io/qianyi-sun/loom-capacity-manager",
            subject_digest=f"sha256:{_DIGEST}",
            build_mode="trusted-rebuild",
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
            "linux/arm64": {"mode": "trusted-rebuild"},
        },
    )
    verification = _verification(predicate)
    external: Any = predicate["buildDefinition"]
    external = external["externalParameters"]
    assert external["scan"] == {
        "scanner": {
            "name": "Trivy",
            "version": "v0.70.0",
            "release": _TRIVY_RELEASE,
            "archives": {
                "linux/amd64": _TRIVY_AMD64,
                "linux/arm64": _TRIVY_ARM64,
            },
        },
        "config_sha256": ("35492da1d08b142bd1489ac54ecdedab62634b7b3095a37cebbe10b61df1adac"),
        "ignore_sha256": ("b09bd1a38036f5e4274586af64616a306590ec33b1e2ac8a73d67ab88d2e4d5a"),
        "scan_type": "image",
        "vuln_type": ["os", "library"],
        "timeout": "10m0s",
        "severity": ["CRITICAL"],
        "exit_code": 1,
        "ignore_unfixed": False,
        "exceptions": _TRIVY_EXCEPTIONS,
        "scanners": ["vuln"],
        "cache": False,
        "report_sha256": {"linux/amd64": _SCAN, "linux/arm64": "1" * 64},
    }

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
            "linux/arm64": {"mode": "trusted-rebuild"},
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
                "linux/arm64": {"mode": "trusted-rebuild"},
            },
        )


@pytest.mark.parametrize(
    ("path", "replacement"),
    [
        (("scanner", "name"), "Other"),
        (("scanner", "version"), "v0.70.1"),
        (("config_sha256",), "0" * 64),
        (("ignore_sha256",), "0" * 64),
    ],
)
def test_manifest_verification_rejects_scan_tool_or_policy_drift(
    path: tuple[str, ...],
    replacement: str,
) -> None:
    architecture_digests = {
        "linux/amd64": f"sha256:{_AMD64_DIGEST}",
        "linux/arm64": f"sha256:{_ARM64_DIGEST}",
    }
    scan_report_digests = {"linux/amd64": _SCAN, "linux/arm64": "1" * 64}
    architecture_builds = {
        "linux/amd64": {"mode": "trusted-rebuild"},
        "linux/arm64": {"mode": "trusted-rebuild"},
    }
    predicate = manifest_predicate(
        **_common(),
        architecture_digests=architecture_digests,
        scan_report_digests=scan_report_digests,
        architecture_builds=architecture_builds,
    )
    tampered = _verification(deepcopy(predicate))
    tampered_payload: Any = tampered
    scan = tampered_payload[0]["verificationResult"]["statement"]["predicate"]["buildDefinition"][
        "externalParameters"
    ]["scan"]
    assert path[0] in scan, f"manifest scan evidence does not bind {path[0]}"
    target = scan
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = replacement

    with pytest.raises(EvidenceError, match="exactly one"):
        verify_manifest_attestation(
            tampered,
            **_common(),
            subject_name="ghcr.io/qianyi-sun/loom-capacity-manager",
            subject_digest=f"sha256:{_DIGEST}",
            architecture_digests=architecture_digests,
            scan_report_digests=scan_report_digests,
            architecture_builds=architecture_builds,
        )


def test_manifest_verification_rejects_float_for_integer_run_id() -> None:
    architecture_digests = {
        "linux/amd64": f"sha256:{_AMD64_DIGEST}",
        "linux/arm64": f"sha256:{_ARM64_DIGEST}",
    }
    scan_report_digests = {"linux/amd64": _SCAN, "linux/arm64": "1" * 64}
    architecture_builds = {
        "linux/amd64": {"mode": "trusted-rebuild"},
        "linux/arm64": {"mode": "trusted-rebuild"},
    }
    predicate = manifest_predicate(
        **_common(),
        architecture_digests=architecture_digests,
        scan_report_digests=scan_report_digests,
        architecture_builds=architecture_builds,
    )
    tampered = _verification(deepcopy(predicate))
    tampered_payload: Any = tampered
    tampered_payload[0]["verificationResult"]["statement"]["predicate"]["buildDefinition"][
        "internalParameters"
    ]["github"]["run_id"] = 123.0

    with pytest.raises(EvidenceError, match="exactly one"):
        verify_manifest_attestation(
            tampered,
            **_common(),
            subject_name="ghcr.io/qianyi-sun/loom-capacity-manager",
            subject_digest=f"sha256:{_DIGEST}",
            architecture_digests=architecture_digests,
            scan_report_digests=scan_report_digests,
            architecture_builds=architecture_builds,
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


def _verify_architecture_cli(
    verification: Path,
    output: Path,
) -> subprocess.CompletedProcess[str]:
    return _run_cli(
        "verify-architecture",
        *_cli_common(),
        "--platform",
        "linux/amd64",
        "--architecture",
        "amd64",
        "--verification",
        str(verification),
        "--subject-name",
        "ghcr.io/qianyi-sun/loom-capacity-manager",
        "--subject-digest",
        f"sha256:{_DIGEST}",
        "--scan-report-sha256",
        _SCAN,
        "--build-mode",
        "trusted-rebuild",
        "--output",
        str(output),
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


def _artifact_record_path(records: Path, architecture: str) -> Path:
    directory = records / architecture
    directory.mkdir(exist_ok=True)
    return directory / f"capacity-manager-{architecture}.json"


def _manifest_fixture() -> dict[str, object]:
    return {
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.index.v1+json",
        "manifests": [
            {
                "mediaType": "application/vnd.oci.image.manifest.v1+json",
                "digest": f"sha256:{_AMD64_DIGEST}",
                "size": 1234,
                "platform": {"architecture": "amd64", "os": "linux"},
            },
            {
                "mediaType": "application/vnd.oci.image.manifest.v1+json",
                "digest": f"sha256:{_ARM64_DIGEST}",
                "size": 2345,
                "platform": {"architecture": "arm64", "os": "linux"},
            },
        ],
    }


def _validate_manifest_cli(manifest: Path) -> subprocess.CompletedProcess[str]:
    return _run_cli(
        "validate-manifest",
        "--manifest",
        str(manifest),
        "--architecture-digest",
        f"linux/amd64=sha256:{_AMD64_DIGEST}",
        "--architecture-digest",
        f"linux/arm64=sha256:{_ARM64_DIGEST}",
    )


def test_cli_validates_exact_two_descriptor_manifest(tmp_path: Path) -> None:
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps(_manifest_fixture()), encoding="utf-8")

    result = _validate_manifest_cli(manifest)

    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize(
    "case",
    ["duplicate-platform", "incomplete-extra", "unexpected-platform"],
)
def test_cli_manifest_validation_rejects_ignored_or_extra_descriptors(
    tmp_path: Path,
    case: str,
) -> None:
    payload = _manifest_fixture()
    manifests: Any = payload["manifests"]
    if case == "duplicate-platform":
        duplicate = deepcopy(manifests[0])
        duplicate["digest"] = f"sha256:{'0' * 64}"
        manifests[1] = duplicate
    elif case == "incomplete-extra":
        manifests.append(
            {
                "digest": f"sha256:{'0' * 64}",
                "platform": {"os": "linux"},
            }
        )
    else:
        manifests.append(
            {
                "digest": f"sha256:{'0' * 64}",
                "platform": {"architecture": "s390x", "os": "linux"},
            }
        )
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps(payload), encoding="utf-8")

    result = _validate_manifest_cli(manifest)

    assert result.returncode != 0
    if case == "duplicate-platform":
        assert "published manifest platform is duplicated" in result.stderr
    else:
        assert "published manifest" in result.stderr


def test_cli_validates_exact_canonical_architecture_record_set(tmp_path: Path) -> None:
    records = tmp_path / "records"
    records.mkdir()
    amd64 = _artifact_record_path(records, "amd64")
    arm64 = _artifact_record_path(records, "arm64")
    assert (
        _write_record(
            amd64,
            architecture="amd64",
            subject_digest=_AMD64_DIGEST,
            scan_digest=_SCAN,
        ).returncode
        == 0
    )
    assert (
        _write_record(
            arm64,
            architecture="arm64",
            subject_digest=_ARM64_DIGEST,
            scan_digest="1" * 64,
        ).returncode
        == 0
    )
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
    assert (
        _write_record(
            record,
            architecture="amd64",
            subject_digest=_AMD64_DIGEST,
            scan_digest=_SCAN,
        ).returncode
        == 0
    )

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


@pytest.mark.parametrize(
    ("path", "replacement"), [(("schema_version",), True), (("release", "run_attempt"), 2.0)]
)
def test_cli_record_validation_is_type_strict(
    tmp_path: Path,
    path: tuple[str, ...],
    replacement: object,
) -> None:
    record = tmp_path / "capacity-manager-amd64.json"
    assert (
        _write_record(
            record,
            architecture="amd64",
            subject_digest=_AMD64_DIGEST,
            scan_digest=_SCAN,
        ).returncode
        == 0
    )
    payload = json.loads(record.read_text(encoding="utf-8"))
    target = payload
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = replacement
    record.write_text(json.dumps(payload), encoding="utf-8")

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

    assert result.returncode != 0
    assert "canonical architecture record" in result.stderr


def test_cli_rejects_extra_or_tampered_architecture_records(tmp_path: Path) -> None:
    records = tmp_path / "records"
    records.mkdir()
    for architecture, digest, scan in (
        ("amd64", _AMD64_DIGEST, _SCAN),
        ("arm64", _ARM64_DIGEST, "1" * 64),
    ):
        result = _write_record(
            _artifact_record_path(records, architecture),
            architecture=architecture,
            subject_digest=digest,
            scan_digest=scan,
        )
        assert result.returncode == 0, result.stderr

    duplicate_path = records / "amd64" / "capacity-manager-arm64.json"
    duplicate_path.write_text("{}\n", encoding="utf-8")
    result = _run_cli(
        "validate-architecture-records",
        *_cli_common(),
        "--records-dir",
        str(records),
    )
    assert result.returncode != 0
    assert "exactly the expected architecture files" in result.stderr
    duplicate_path.unlink()

    record = json.loads(
        (records / "amd64" / "capacity-manager-amd64.json").read_text(encoding="utf-8")
    )
    record["release"]["tree"] = "0" * 40
    (records / "amd64" / "capacity-manager-amd64.json").write_text(
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


def test_cli_rejects_duplicate_key_in_architecture_verification(tmp_path: Path) -> None:
    predicate = architecture_predicate(
        **_common(),
        platform="linux/amd64",
        architecture="amd64",
        scan_report_sha256=_SCAN,
        build_mode="trusted-rebuild",
    )
    raw = json.dumps(_verification(predicate)).replace(
        '"verificationResult":',
        '"verificationResult":{},"verificationResult":',
        1,
    )
    verification = tmp_path / "verification.json"
    verification.write_text(raw, encoding="utf-8")

    result = _verify_architecture_cli(verification, tmp_path / "output.json")

    assert result.returncode != 0
    assert "duplicate JSON key" in result.stderr


def test_cli_rejects_exponent_overflow_in_architecture_verification(
    tmp_path: Path,
) -> None:
    predicate = architecture_predicate(
        **_common(),
        platform="linux/amd64",
        architecture="amd64",
        scan_report_sha256=_SCAN,
        build_mode="trusted-rebuild",
    )
    raw = json.dumps(_verification(predicate)).replace(
        '{"verificationResult":',
        '{"ignored":1e999,"verificationResult":',
        1,
    )
    verification = tmp_path / "verification.json"
    verification.write_text(raw, encoding="utf-8")

    result = _verify_architecture_cli(verification, tmp_path / "output.json")

    assert result.returncode != 0
    assert "non-finite JSON value" in result.stderr


def test_cli_rejects_nonfinite_value_in_manifest_verification(tmp_path: Path) -> None:
    architecture_digests = {
        "linux/amd64": f"sha256:{_AMD64_DIGEST}",
        "linux/arm64": f"sha256:{_ARM64_DIGEST}",
    }
    scan_report_digests = {"linux/amd64": _SCAN, "linux/arm64": "1" * 64}
    architecture_builds = {
        "linux/amd64": {"mode": "trusted-rebuild"},
        "linux/arm64": {"mode": "trusted-rebuild"},
    }
    predicate = manifest_predicate(
        **_common(),
        architecture_digests=architecture_digests,
        scan_report_digests=scan_report_digests,
        architecture_builds=architecture_builds,
    )
    raw = json.dumps(_verification(predicate)).replace(
        '{"verificationResult":',
        '{"ignored":NaN,"verificationResult":',
        1,
    )
    verification = tmp_path / "verification.json"
    verification.write_text(raw, encoding="utf-8")

    result = _run_cli(
        "verify-manifest",
        *_cli_common(),
        "--verification",
        str(verification),
        "--subject-name",
        "ghcr.io/qianyi-sun/loom-capacity-manager",
        "--subject-digest",
        f"sha256:{_DIGEST}",
        "--architecture-digest",
        f"linux/amd64=sha256:{_AMD64_DIGEST}",
        "--architecture-digest",
        f"linux/arm64=sha256:{_ARM64_DIGEST}",
        "--scan-report-digest",
        f"linux/amd64={_SCAN}",
        "--scan-report-digest",
        f"linux/arm64={'1' * 64}",
        "--architecture-build",
        'linux/amd64={"mode":"trusted-rebuild"}',
        "--architecture-build",
        'linux/arm64={"mode":"trusted-rebuild"}',
    )

    assert result.returncode != 0
    assert "non-finite JSON value" in result.stderr


@pytest.mark.parametrize("operation", ["single", "directory"])
def test_cli_rejects_duplicate_key_in_architecture_records(
    tmp_path: Path,
    operation: str,
) -> None:
    records = tmp_path / "records"
    records.mkdir()
    amd64 = _artifact_record_path(records, "amd64")
    assert (
        _write_record(
            amd64,
            architecture="amd64",
            subject_digest=_AMD64_DIGEST,
            scan_digest=_SCAN,
        ).returncode
        == 0
    )
    raw = amd64.read_text(encoding="utf-8").replace(
        '"schema_version":1',
        '"schema_version":1,"schema_version":1',
        1,
    )
    amd64.write_text(raw, encoding="utf-8")
    if operation == "single":
        result = _run_cli(
            "validate-architecture-record",
            *_cli_common(),
            "--platform",
            "linux/amd64",
            "--architecture",
            "amd64",
            "--record",
            str(amd64),
        )
    else:
        arm64 = _artifact_record_path(records, "arm64")
        assert (
            _write_record(
                arm64,
                architecture="arm64",
                subject_digest=_ARM64_DIGEST,
                scan_digest="1" * 64,
            ).returncode
            == 0
        )
        result = _run_cli(
            "validate-architecture-records",
            *_cli_common(),
            "--records-dir",
            str(records),
        )

    assert result.returncode != 0
    assert "duplicate JSON key" in result.stderr


@pytest.mark.parametrize("token", ["NaN", "Infinity", "-Infinity"])
def test_cli_rejects_nonfinite_manifest_json_values(
    tmp_path: Path,
    token: str,
) -> None:
    manifest = tmp_path / "manifest.json"
    raw = json.dumps(_manifest_fixture()).replace(
        "{",
        f'{{"ignored":{token},',
        1,
    )
    manifest.write_text(raw, encoding="utf-8")

    result = _validate_manifest_cli(manifest)

    assert result.returncode != 0
    assert "non-finite JSON value" in result.stderr


def test_cli_rejects_exponent_overflow_in_manifest_json(tmp_path: Path) -> None:
    manifest = tmp_path / "manifest.json"
    raw = json.dumps(_manifest_fixture()).replace(
        "{",
        '{"ignored":1e999,',
        1,
    )
    manifest.write_text(raw, encoding="utf-8")

    result = _validate_manifest_cli(manifest)

    assert result.returncode != 0
    assert "non-finite JSON value" in result.stderr


def test_cli_rejects_duplicate_key_in_manifest_json(tmp_path: Path) -> None:
    manifest = tmp_path / "manifest.json"
    raw = json.dumps(_manifest_fixture()).replace(
        '"manifests":',
        '"manifests":[],"manifests":',
        1,
    )
    manifest.write_text(raw, encoding="utf-8")

    result = _validate_manifest_cli(manifest)

    assert result.returncode != 0
    assert "duplicate JSON key" in result.stderr


def test_cli_rejects_nonfinite_architecture_record_value(tmp_path: Path) -> None:
    record = tmp_path / "capacity-manager-amd64.json"
    assert (
        _write_record(
            record,
            architecture="amd64",
            subject_digest=_AMD64_DIGEST,
            scan_digest=_SCAN,
        ).returncode
        == 0
    )
    raw = record.read_text(encoding="utf-8").replace(
        '"schema_version":1',
        '"schema_version":NaN',
        1,
    )
    record.write_text(raw, encoding="utf-8")

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

    assert result.returncode != 0
    assert "non-finite JSON value" in result.stderr


def test_cli_rejects_exponent_overflow_in_architecture_record(tmp_path: Path) -> None:
    record = tmp_path / "capacity-manager-amd64.json"
    assert (
        _write_record(
            record,
            architecture="amd64",
            subject_digest=_AMD64_DIGEST,
            scan_digest=_SCAN,
        ).returncode
        == 0
    )
    raw = record.read_text(encoding="utf-8").replace(
        '"schema_version":1',
        '"schema_version":1e999',
        1,
    )
    record.write_text(raw, encoding="utf-8")

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

    assert result.returncode != 0
    assert "non-finite JSON value" in result.stderr


def test_cli_rejects_duplicate_key_in_inline_architecture_build(tmp_path: Path) -> None:
    result = _run_cli(
        "predicate-manifest",
        *_cli_common(),
        "--architecture-digest",
        f"linux/amd64=sha256:{_AMD64_DIGEST}",
        "--architecture-digest",
        f"linux/arm64=sha256:{_ARM64_DIGEST}",
        "--scan-report-digest",
        f"linux/amd64={_SCAN}",
        "--scan-report-digest",
        f"linux/arm64={'1' * 64}",
        "--architecture-build",
        'linux/amd64={"mode":"trusted-rebuild","mode":"trusted-rebuild"}',
        "--architecture-build",
        'linux/arm64={"mode":"trusted-rebuild"}',
        "--output",
        str(tmp_path / "predicate.json"),
    )

    assert result.returncode != 0
    assert "duplicate JSON key" in result.stderr


def test_cli_rejects_nonfinite_inline_architecture_build(tmp_path: Path) -> None:
    result = _run_cli(
        "predicate-manifest",
        *_cli_common(),
        "--architecture-digest",
        f"linux/amd64=sha256:{_AMD64_DIGEST}",
        "--architecture-digest",
        f"linux/arm64=sha256:{_ARM64_DIGEST}",
        "--scan-report-digest",
        f"linux/amd64={_SCAN}",
        "--scan-report-digest",
        f"linux/arm64={'1' * 64}",
        "--architecture-build",
        'linux/amd64={"mode":"trusted-rebuild","ignored":NaN}',
        "--architecture-build",
        'linux/arm64={"mode":"trusted-rebuild"}',
        "--output",
        str(tmp_path / "predicate.json"),
    )

    assert result.returncode != 0
    assert "non-finite JSON value" in result.stderr


def test_cli_rejects_exponent_overflow_in_inline_architecture_build(
    tmp_path: Path,
) -> None:
    result = _run_cli(
        "predicate-manifest",
        *_cli_common(),
        "--architecture-digest",
        f"linux/amd64=sha256:{_AMD64_DIGEST}",
        "--architecture-digest",
        f"linux/arm64=sha256:{_ARM64_DIGEST}",
        "--scan-report-digest",
        f"linux/amd64={_SCAN}",
        "--scan-report-digest",
        f"linux/arm64={'1' * 64}",
        "--architecture-build",
        'linux/amd64={"mode":"trusted-rebuild","ignored":1e999}',
        "--architecture-build",
        'linux/arm64={"mode":"trusted-rebuild"}',
        "--output",
        str(tmp_path / "predicate.json"),
    )

    assert result.returncode != 0
    assert "non-finite JSON value" in result.stderr
