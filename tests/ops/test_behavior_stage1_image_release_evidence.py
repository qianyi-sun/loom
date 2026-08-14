from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import pytest
from scripts.ci_behavior_stage1_image import (
    Stage1ImageEvidenceError,
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
    _canonical(provenance, {"verificationResult": "success"})
    _canonical(sbom, {"verificationResult": "success"})
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
