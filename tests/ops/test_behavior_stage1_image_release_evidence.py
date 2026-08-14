from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import pytest
from scripts.ci_behavior_stage1_image import (
    Stage1ImageEvidenceError,
    _parse_attestation_verification,
    _record,
    _record_index,
    _validate_index_record,
    _validate_record,
    _write,
)


def _digest(path: Path) -> str:
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def _canonical(path: Path, value: object) -> None:
    path.write_bytes(json.dumps(value, separators=(",", ":"), sort_keys=True).encode() + b"\n")


def _verification(
    *, predicate_type: str, subject_name: str, subject_digest: str
) -> list[dict[str, object]]:
    return [
        {
            "attestation": {
                "bundle": {"mediaType": "application/vnd.dev.sigstore.bundle.v0.3+json"},
                "bundle_url": "oci://ghcr.io/qianyi-sun/loom-behavior-stage1-sim",
                "initiator": "",
            },
            "verificationResult": {
                "mediaType": (
                    "application/vnd.dev.sigstore.verificationresult+json;version=0.1"
                ),
                "signature": {"certificate": {"issuer": "https://token.actions.githubusercontent.com"}},
                "statement": {
                    "_type": "https://in-toto.io/Statement/v1",
                    "predicate": {},
                    "predicateType": predicate_type,
                    "subject": [
                        {
                            "digest": {"sha256": subject_digest.removeprefix("sha256:")},
                            "name": subject_name,
                        }
                    ],
                },
                "verifiedIdentity": {
                    "issuer": "https://token.actions.githubusercontent.com",
                    "subjectAlternativeName": (
                        "https://github.com/qianyi-sun/loom/.github/workflows/images.yml@refs/heads/dev"
                    ),
                },
                "verifiedTimestamps": [{"type": "transparency-log"}],
            },
        }
    ]


def _args(tmp_path: Path) -> argparse.Namespace:
    tmp_path.mkdir(parents=True, exist_ok=True)
    source = tmp_path / "source.json"
    _canonical(
        source,
        {"schema_version": "loom.behavior-stage1-image-source-evidence.v1"},
    )
    manifest = tmp_path / "manifest.json"
    _canonical(
        manifest,
        {
            "build_sha": "a" * 40,
            "build_tree_sha": "b" * 40,
            "source_evidence_sha256": _digest(source),
        },
    )
    scan = tmp_path / "scan.json"
    _canonical(scan, {"Results": []})
    sbom = tmp_path / "sbom.json"
    _canonical(sbom, {"packages": [], "spdxVersion": "SPDX-2.3"})
    preflight = tmp_path / "preflight.py"
    preflight.write_text("pass\n", encoding="utf-8")
    return argparse.Namespace(
        compatibility_manifest=manifest,
        context=".",
        dockerfile="deploy/Dockerfile.behavior-stage1-sim",
        event_name="pull_request",
        head_sha="a" * 40,
        image="behavior-stage1-sim",
        image_name="loom-behavior-stage1-sim",
        mode="candidate",
        output=tmp_path / "record.json",
        platform="linux/amd64",
        preflight=preflight,
        ref_name="feature",
        repository="qianyi-sun/loom",
        run_attempt=1,
        run_id=2,
        sbom=sbom,
        scan_report=scan,
        source_evidence=source,
        subject_digest="sha256:" + "c" * 64,
        subject_name="loom-ci-loom-behavior-stage1-sim:candidate",
        tree_sha="b" * 40,
    )


def test_child_record_is_canonical_and_binds_all_evidence(tmp_path: Path) -> None:
    args = _args(tmp_path)
    record = _record(args)
    _write(args.output, record)

    _validate_record(args.output, mode="candidate")
    assert args.output.read_bytes().endswith(b"\n")
    assert record["evidence"] == {
        "compatibility_manifest_sha256": _digest(args.compatibility_manifest),
        "preflight_sha256": _digest(args.preflight),
        "sbom_sha256": _digest(args.sbom),
        "scan_report_sha256": _digest(args.scan_report),
        "source_evidence_sha256": _digest(args.source_evidence),
    }


def test_record_rejects_manifest_source_or_distribution_evidence_drift(tmp_path: Path) -> None:
    args = _args(tmp_path)
    args.head_sha = "d" * 40
    with pytest.raises(Stage1ImageEvidenceError, match="build SHA drift"):
        _record(args)

    args = _args(tmp_path / "second")
    args.sbom.write_text('{"packages":[]}\n', encoding="utf-8")
    with pytest.raises(Stage1ImageEvidenceError, match="SPDX SBOM contract drift"):
        _record(args)


def test_validate_rejects_noncanonical_or_wrong_mode_record(tmp_path: Path) -> None:
    args = _args(tmp_path)
    _write(args.output, _record(args))
    with pytest.raises(Stage1ImageEvidenceError, match="mode drift"):
        _validate_record(args.output, mode="trusted-publish")
    args.output.write_bytes(args.output.read_bytes().rstrip())
    with pytest.raises(Stage1ImageEvidenceError, match="canonical"):
        _validate_record(args.output, mode="candidate")


def test_index_record_requires_one_exact_amd64_child(tmp_path: Path) -> None:
    args = _args(tmp_path)
    args.mode = "trusted-publish"
    args.event_name = "push"
    args.ref_name = "dev"
    _write(args.output, _record(args))
    index = tmp_path / "index.json"
    _canonical(
        index,
        {
            "manifests": [
                {
                    "digest": args.subject_digest,
                    "platform": {"architecture": "amd64", "os": "linux"},
                }
            ],
            "mediaType": "application/vnd.oci.image.index.v1+json",
            "schemaVersion": 2,
        },
    )
    provenance = tmp_path / "provenance.json"
    sbom = tmp_path / "sbom-verification.json"
    _canonical(
        provenance,
        _verification(
            predicate_type="https://slsa.dev/provenance/v1",
            subject_name=args.subject_name,
            subject_digest=args.subject_digest,
        ),
    )
    _canonical(
        sbom,
        _verification(
            predicate_type="https://spdx.dev/Document/v2.3",
            subject_name=args.subject_name,
            subject_digest=args.subject_digest,
        ),
    )
    index_args = argparse.Namespace(
        child_record=args.output,
        index_manifest=index,
        output=tmp_path / "index-record.json",
        provenance_verification=provenance,
        sbom_verification=sbom,
        subject_digest="sha256:" + "d" * 64,
        subject_name="ghcr.io/qianyi-sun/loom-behavior-stage1-sim",
    )

    _write(index_args.output, _record_index(index_args))
    _validate_index_record(index_args.output)

    value = json.loads(index.read_bytes())
    value["manifests"].append(value["manifests"][0])
    _canonical(index, value)
    with pytest.raises(Stage1ImageEvidenceError, match="exactly one child"):
        _record_index(index_args)


@pytest.mark.parametrize(
    ("replacement", "message"),
    [
        ({}, "singleton verification array"),
        ([], "singleton verification array"),
        ([{}, {}], "singleton verification array"),
        (["not-an-object"], "result must be an object"),
    ],
)
def test_index_record_rejects_non_singleton_attestation_verification(
    tmp_path: Path,
    replacement: object,
    message: str,
) -> None:
    args = _args(tmp_path)
    args.mode = "trusted-publish"
    args.event_name = "push"
    args.ref_name = "dev"
    _write(args.output, _record(args))
    index = tmp_path / "index.json"
    _canonical(
        index,
        {
            "manifests": [
                {
                    "digest": args.subject_digest,
                    "platform": {"architecture": "amd64", "os": "linux"},
                }
            ],
            "mediaType": "application/vnd.oci.image.index.v1+json",
        },
    )
    provenance = tmp_path / "provenance.json"
    sbom = tmp_path / "sbom.json"
    _canonical(provenance, replacement)
    _canonical(
        sbom,
        _verification(
            predicate_type="https://spdx.dev/Document/v2.3",
            subject_name=args.subject_name,
            subject_digest=args.subject_digest,
        ),
    )
    index_args = argparse.Namespace(
        child_record=args.output,
        index_manifest=index,
        output=tmp_path / "index-record.json",
        provenance_verification=provenance,
        sbom_verification=sbom,
        subject_digest="sha256:" + "d" * 64,
        subject_name="ghcr.io/qianyi-sun/loom-behavior-stage1-sim",
    )

    with pytest.raises(Stage1ImageEvidenceError, match=message):
        _record_index(index_args)


@pytest.mark.parametrize("drift", ["predicate", "subject"])
def test_index_record_rejects_attestation_statement_drift(
    tmp_path: Path, drift: str
) -> None:
    args = _args(tmp_path)
    args.mode = "trusted-publish"
    args.event_name = "push"
    args.ref_name = "dev"
    _write(args.output, _record(args))
    index = tmp_path / "index.json"
    _canonical(
        index,
        {
            "manifests": [
                {
                    "digest": args.subject_digest,
                    "platform": {"architecture": "amd64", "os": "linux"},
                }
            ],
            "mediaType": "application/vnd.oci.image.index.v1+json",
        },
    )
    verification = _verification(
        predicate_type="https://slsa.dev/provenance/v1",
        subject_name=args.subject_name,
        subject_digest=args.subject_digest,
    )
    verification_result = verification[0]["verificationResult"]
    assert isinstance(verification_result, dict)
    statement = verification_result["statement"]
    assert isinstance(statement, dict)
    if drift == "predicate":
        statement["predicateType"] = "https://example.invalid/predicate"
    else:
        statement["subject"] = []
    provenance = tmp_path / "provenance.json"
    sbom = tmp_path / "sbom.json"
    _canonical(provenance, verification)
    _canonical(
        sbom,
        _verification(
            predicate_type="https://spdx.dev/Document/v2.3",
            subject_name=args.subject_name,
            subject_digest=args.subject_digest,
        ),
    )
    index_args = argparse.Namespace(
        child_record=args.output,
        index_manifest=index,
        output=tmp_path / "index-record.json",
        provenance_verification=provenance,
        sbom_verification=sbom,
        subject_digest="sha256:" + "d" * 64,
        subject_name="ghcr.io/qianyi-sun/loom-behavior-stage1-sim",
    )

    with pytest.raises(Stage1ImageEvidenceError, match=drift):
        _record_index(index_args)


def test_sbom_attestation_verification_is_bounded(tmp_path: Path) -> None:
    oversized = tmp_path / "oversized-sbom-verification.json"
    with oversized.open("wb") as handle:
        handle.truncate(32 * 1024 * 1024 + 1)

    with pytest.raises(Stage1ImageEvidenceError, match="bounded private regular file"):
        _parse_attestation_verification(
            oversized,
            label="SBOM verification",
            predicate_type="https://spdx.dev/Document/v2.3",
            subject_name="ghcr.io/qianyi-sun/loom-behavior-stage1-sim",
            subject_digest="sha256:" + "a" * 64,
            max_bytes=32 * 1024 * 1024,
        )


@pytest.mark.parametrize("event_name", ["push", "workflow_dispatch"])
def test_trusted_record_accepts_only_protected_branch_release_events(
    tmp_path: Path, event_name: str
) -> None:
    args = _args(tmp_path)
    args.mode = "trusted-publish"
    args.event_name = event_name
    args.ref_name = "dev"

    record = _record(args)

    build = record["build"]
    source = record["source"]
    assert isinstance(build, dict)
    assert isinstance(source, dict)
    assert build["mode"] == "trusted-publish"
    assert source["event_name"] == event_name
    args.ref_name = "feature"
    with pytest.raises(Stage1ImageEvidenceError, match="ref"):
        _record(args)
