from __future__ import annotations

import argparse
import base64
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from scripts.ops import nebius_candidate as candidate


def inputs(tmp_path: Path) -> tuple[dict, Path, str]:
    key = Ed25519PrivateKey.generate()
    private = tmp_path / "signer.pem"
    private.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    private.chmod(0o600)
    keyring = json.dumps(
        {
            "schema_version": 1,
            "keys": [
                {
                    "signing_key_id": "publisher",
                    "public_key_base64": base64.b64encode(
                        key.public_key().public_bytes(
                            serialization.Encoding.Raw, serialization.PublicFormat.Raw
                        ),
                    ).decode(),
                }
            ],
        }
    )
    document = {
        "schema_version": "loom.nebius-candidate.v1",
        "repository": candidate.REPOSITORY,
        "source_ref": candidate.SOURCE_REF,
        "candidate_sha": "a" * 40,
        "workflow_path": candidate.WORKFLOW,
        "run_id": 123,
        "registry_prefix": "cr.eu-north1.nebius.cloud/e00example",
        "runtime_binary_sha256": "sha256:" + "c" * 64,
        "policy_sha256": "sha256:" + "d" * 64,
        "images": {},
    }
    for index, (component, name) in enumerate(candidate.COMPONENTS.items()):
        document["images"][component] = {
            "image_ref": f"{document['registry_prefix']}/{name}@sha256:{str(index) * 64}",
            "sbom_sha256": "sha256:" + "e" * 64,
            "vulnerability_report_sha256": "sha256:" + "f" * 64,
            "highest_vulnerability_severity": "high",
        }
    return document, private, keyring


def test_cli_create_plain_candidate_and_check_shape(tmp_path: Path) -> None:
    document, private, keyring = inputs(tmp_path)
    record, trust = tmp_path / "build.json", tmp_path / "trust.json"
    record.write_text(json.dumps(document))
    trust.write_text(keyring)
    output = tmp_path / "release"
    command = [sys.executable, str(candidate.ROOT / "scripts/ops/nebius_candidate.py")]
    environment = {**os.environ, "PYTHONPATH": str(candidate.ROOT / "src")}
    create = subprocess.run(
        [
            *command,
            "create",
            "--build-record",
            str(record),
            "--signing-key",
            str(private),
            "--signing-key-id",
            "publisher",
            "--trusted-keyring",
            str(trust),
            "--output",
            str(output),
        ],
        capture_output=True,
        text=True,
        env=environment,
    )
    assert create.returncode == 0, create.stderr
    verify = [
        *command,
        "check-shape",
        "--candidate",
        str(output / "candidate.json"),
    ]
    assert subprocess.run(verify, capture_output=True, env=environment).returncode == 0
    manifest = json.loads((output / "candidate.json").read_text())
    assert "signature" not in manifest and "profile_sha256" not in manifest
    assert "source_tree" not in manifest
    assert all(set(row) == {"image_ref"} for row in manifest["images"].values())
    manifest["images"]["web"]["image_ref"] = "image:mutable"
    (output / "candidate.json").write_text(json.dumps(manifest))
    result = subprocess.run(verify, capture_output=True, text=True, env=environment)
    assert result.returncode == 1
    assert private.read_text() not in result.stderr


@pytest.mark.parametrize(
    "change",
    ["dev", "foreign_registry", "missing_image", "bool_run", "mutable"],
)
def test_reject_invalid_build_binding(tmp_path: Path, change: str) -> None:
    document, private, keyring = inputs(tmp_path)
    if change == "dev":
        document["source_ref"] = "refs/heads/dev"
    elif change == "foreign_registry":
        document["registry_prefix"] = "ghcr.io/qianyi-sun"
    elif change == "missing_image":
        del document["images"]["gateway"]
    elif change == "bool_run":
        document["run_id"] = True
    else:
        document["images"]["web"]["image_ref"] = (
            "cr.eu-north1.nebius.cloud/e00example/loom-web:latest"
        )
    with pytest.raises(ValueError):
        candidate.create_candidate(
            document, signing_key=private, signing_key_id="publisher", keyring_json=keyring
        )


def test_profile_and_independent_signer_binding(tmp_path: Path) -> None:
    document, private, trust = inputs(tmp_path)
    manifest, profile = candidate.create_candidate(
        document, signing_key=private, signing_key_id="publisher", keyring_json=trust
    )
    from loom.execution_image_admission import (
        ImageAdmissionKeyring,
        verify_execution_image_admission,
    )
    from loom.service_execution_materialization import ServiceExecutionRuntimeProfileV1

    parsed = ServiceExecutionRuntimeProfileV1.model_validate(profile)
    assert parsed.candidate_sha == manifest["candidate_sha"]
    assert parsed.task_image_ref == manifest["images"]["service"]["image_ref"]
    assert parsed.runtime_image_ref == manifest["images"]["execution_runtime"]["image_ref"]
    verify_execution_image_admission(
        parsed.image_admission,
        required_image_refs=(parsed.task_image_ref, parsed.runtime_image_ref),
        keyring=ImageAdmissionKeyring.from_json(trust),
    )
    other = tmp_path / "other"
    other.mkdir()
    _, _, wrong_trust = inputs(other)
    with pytest.raises(ValueError, match="does not match"):
        candidate.create_candidate(
            document, signing_key=private, signing_key_id="publisher", keyring_json=wrong_trust
        )
    private.chmod(0o644)
    with pytest.raises(ValueError, match="owner-only"):
        candidate.create_candidate(
            document, signing_key=private, signing_key_id="publisher", keyring_json=trust
        )


def test_build_rejects_pr_before_any_process_or_output(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("GITHUB_EVENT_NAME", "pull_request")
    monkeypatch.setenv("GITHUB_REPOSITORY", candidate.REPOSITORY)
    monkeypatch.setenv("GITHUB_REF", candidate.SOURCE_REF)
    monkeypatch.setattr(candidate, "_run", lambda *args: pytest.fail("must not run a subprocess"))
    output = tmp_path / "release"
    with pytest.raises(ValueError, match="fixed protected Nebius workflow"):
        candidate.build(
            argparse.Namespace(
                output=output, registry_prefix="cr.eu-north1.nebius.cloud/e00example"
            )
        )
    assert not output.exists()


def test_duplicate_input_fields_rejected(tmp_path: Path) -> None:
    path = tmp_path / "bad.json"
    path.write_text('{"candidate_sha":"a","candidate_sha":"b"}')
    with pytest.raises(ValueError, match="duplicate"):
        candidate.read_json(path)


def oci_fixture(tmp_path: Path, *, revision: str = "a" * 40, corrupt: bool = False) -> Path:
    import io
    import tarfile

    binary = b"runtime executable payload"
    layer_bytes = io.BytesIO()
    with tarfile.open(fileobj=layer_bytes, mode="w") as layer:
        info = tarfile.TarInfo("loom-execution-runtime")
        info.mode, info.size = 0o755, len(binary)
        layer.addfile(info, io.BytesIO(binary))
    blobs = {}

    def descriptor(payload: bytes, media: str) -> dict:
        digest = candidate.sha256(payload)
        blobs["blobs/sha256/" + digest.split(":")[1]] = payload
        return {"digest": digest, "size": len(payload), "mediaType": media}

    config = descriptor(
        candidate.encoded(
            {
                "architecture": "amd64",
                "os": "linux",
                "config": {"Labels": {"org.opencontainers.image.revision": revision}},
            }
        ),
        "application/vnd.oci.image.config.v1+json",
    )
    layer = descriptor(layer_bytes.getvalue(), "application/vnd.oci.image.layer.v1.tar")
    manifest = descriptor(
        candidate.encoded({"schemaVersion": 2, "config": config, "layers": [layer]}),
        "application/vnd.oci.image.manifest.v1+json",
    )
    blobs["index.json"] = candidate.encoded({"schemaVersion": 2, "manifests": [manifest]})
    if corrupt:
        blobs["blobs/sha256/" + config["digest"].split(":")[1]] = b"corrupted"
    archive = tmp_path / "runtime.oci.tar"
    with tarfile.open(archive, "w") as bundle:
        for name, payload in blobs.items():
            info = tarfile.TarInfo(name)
            info.size = len(payload)
            bundle.addfile(info, io.BytesIO(payload))
    return archive


def test_oci_runtime_inspection_hashes_actual_scanned_layer(tmp_path: Path) -> None:
    archive = oci_fixture(tmp_path)
    manifest, binary = candidate.inspect_oci_archive(archive, candidate="a" * 40, runtime=True)
    assert candidate.DIGEST.fullmatch(manifest)
    assert binary == candidate.sha256(b"runtime executable payload")


@pytest.mark.parametrize("corrupt", [False, True])
def test_oci_inspection_rejects_other_source_and_corrupt_blobs(
    tmp_path: Path, corrupt: bool
) -> None:
    archive = oci_fixture(tmp_path, revision="b" * 40, corrupt=corrupt)
    with pytest.raises(ValueError, match=r"checksum|source/platform"):
        candidate.inspect_oci_archive(archive, candidate="a" * 40, runtime=True)


def test_builder_diagnostics_bound_output_and_remove_credentials(monkeypatch):
    monkeypatch.setenv("NEBIUS_SECRET", "secret-material-123")
    raw = (
        "old-output\n" * 3000
        + "failed to mount worker storage\n"
        + "secret-material-123\n"
        + "authorization: Bearer token-value\n"
        + "endpoint https://registry.invalid/path?token=value 192.0.2.123\n"
    )
    result = candidate.sanitize_diagnostic(raw)
    assert len(result) <= 16_384
    assert "failed to mount worker storage" in result
    for secret in ("secret-material-123", "token-value", "registry.invalid", "192.0.2.123"):
        assert secret not in result


def test_builder_diagnostics_remain_bounded_after_redaction_expands_lines():
    assert len(candidate.sanitize_diagnostic("password\n" * 3000)) <= 16_384


def test_oci_scan_layout_reuses_native_archive_and_cleans_up(tmp_path: Path) -> None:
    archive = oci_fixture(tmp_path)
    layout = tmp_path / "runtime.release.oci"
    with candidate.oci_scan_layout(archive, layout) as scan_input:
        index = json.loads((scan_input / "index.json").read_text())
        digest = index["manifests"][0]["digest"]
        assert (scan_input / "blobs/sha256" / digest.split(":")[1]).is_file()
        assert scan_input == layout
    assert not layout.exists()
    assert archive.is_file()


def test_oci_scan_layout_rejects_archive_path_escape(tmp_path: Path) -> None:
    import io
    import tarfile

    archive = tmp_path / "bad.oci.tar"
    with tarfile.open(archive, "w") as bundle:
        entry = tarfile.TarInfo("../outside")
        entry.size = 1
        bundle.addfile(entry, io.BytesIO(b"x"))
    layout = tmp_path / "scan"
    with pytest.raises(tarfile.FilterError):
        with candidate.oci_scan_layout(archive, layout):
            pytest.fail("unsafe archive was extracted")
    assert not layout.exists()
    assert not (tmp_path / "outside").exists()
