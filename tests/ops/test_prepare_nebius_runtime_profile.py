from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

import pytest
from scripts.ops import prepare_nebius_runtime_profile as prepare

from loom.execution_image_admission import (
    ImageAdmissionKeyring,
    verify_execution_image_admission,
)
from loom.service_execution_materialization import ServiceExecutionRuntimeProfileV1

SHA = "7" * 40
REGISTRY = "cr.eu-north1.nebius.cloud/e00example"
DIGESTS = {"service": "1" * 64, "execution_runtime": "2" * 64}


def _write_json(path: Path, payload: object) -> None:
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")


def _inputs(tmp_path: Path) -> argparse.Namespace:
    source_refs = {
        "service": f"ghcr.io/qianyi-sun/loom-service@sha256:{DIGESTS['service']}",
        "execution_runtime": (
            "ghcr.io/qianyi-sun/loom-execution-runtime@sha256:" + DIGESTS["execution_runtime"]
        ),
    }
    target_refs = {
        "service": f"{REGISTRY}/loom-service@sha256:{DIGESTS['service']}",
        "execution_runtime": (
            f"{REGISTRY}/loom-execution-runtime@sha256:" + DIGESTS["execution_runtime"]
        ),
    }
    mirror = tmp_path / "mirror.json"
    _write_json(
        mirror,
        {
            "schema_version": "loom.nebius-release-mirror.v1",
            "candidate_sha": SHA,
            "images": {
                key: {"source_ref": source_refs[key], "target_ref": target_refs[key]}
                for key in source_refs
            },
        },
    )
    evidence = tmp_path / "evidence.json"
    _write_json(
        evidence,
        {
            "schema_version": "loom.nebius-runtime-evidence.v1",
            "images": {
                "service": {
                    "image_ref": target_refs["service"],
                    "sbom_sha256": "sha256:" + "3" * 64,
                    "vulnerability_report_sha256": "sha256:" + "4" * 64,
                    "highest_vulnerability_severity": "unknown",
                },
                "execution_runtime": {
                    "image_ref": target_refs["execution_runtime"],
                    "sbom_sha256": "sha256:" + "5" * 64,
                    "vulnerability_report_sha256": "sha256:" + "6" * 64,
                    "highest_vulnerability_severity": "high",
                },
            },
            "runtime_binary_sha256": "sha256:" + "8" * 64,
        },
    )
    releases: dict[str, Path] = {}
    for key, component, image_name in prepare._COMPONENTS:
        path = tmp_path / f"{component}.json"
        _write_json(
            path,
            {
                "schema_version": 1,
                "image": {"component": component, "platform": "linux/amd64"},
                "release": {"commit": SHA, "ref": "refs/heads/dev"},
                "subject": {
                    "name": f"ghcr.io/qianyi-sun/{image_name}",
                    "digest": "sha256:" + DIGESTS[key],
                },
                "scan": {
                    "config_sha256": "a" * 64,
                    "ignore_sha256": "b" * 64,
                    "scanner": {"name": "Trivy", "version": "v0.74.0"},
                },
            },
        )
        releases[key] = path
    return argparse.Namespace(
        candidate_sha=SHA,
        mirror_record=mirror,
        evidence_summary=evidence,
        service_release_record=releases["service"],
        execution_runtime_release_record=releases["execution_runtime"],
        signing_key=tmp_path / "admission-signing-key.pem",
        signing_key_id="nebius-development-2026-09",
        create_signing_key=True,
        output_profile=tmp_path / "profile.json",
        output_keyring=tmp_path / "keyring.json",
        output_policy=tmp_path / "policy.json",
    )


def test_prepare_creates_reusable_key_and_verified_profile(tmp_path: Path) -> None:
    args = _inputs(tmp_path)
    result = prepare.prepare(args)

    assert result["candidate_sha"] == SHA
    assert os.stat(args.signing_key).st_mode & 0o777 == 0o600
    for path in (args.output_profile, args.output_keyring, args.output_policy):
        assert os.stat(path).st_mode & 0o777 == 0o600
    profile = ServiceExecutionRuntimeProfileV1.model_validate_json(
        args.output_profile.read_text(encoding="utf-8")
    )
    keyring = ImageAdmissionKeyring.from_json(args.output_keyring.read_text(encoding="utf-8"))
    verify_execution_image_admission(
        profile.image_admission,
        required_image_refs=(profile.task_image_ref, profile.runtime_image_ref),
        keyring=keyring,
    )
    assert {
        row.statement.highest_vulnerability_severity for row in profile.image_admission.admissions
    } == {"high", "unknown"}

    args.create_signing_key = False
    repeated = prepare.prepare(args)
    assert repeated["public_key_sha256"] == result["public_key_sha256"]


def test_prepare_refuses_to_replace_signing_key(tmp_path: Path) -> None:
    args = _inputs(tmp_path)
    prepare.prepare(args)
    with pytest.raises(ValueError, match="refusing to replace"):
        prepare.prepare(args)


def test_prepare_rejects_mirror_drift(tmp_path: Path) -> None:
    args = _inputs(tmp_path)
    mirror = json.loads(args.mirror_record.read_text(encoding="utf-8"))
    mirror["images"]["service"]["target_ref"] = f"{REGISTRY}/loom-service@sha256:" + "9" * 64
    _write_json(args.mirror_record, mirror)
    with pytest.raises(ValueError, match="source, mirror, and scan differ"):
        prepare.prepare(args)
