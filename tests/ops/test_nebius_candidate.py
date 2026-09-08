from __future__ import annotations

import argparse
import base64
import copy
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
from cryptography.exceptions import InvalidSignature
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
        "source_tree": "b" * 40,
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
            "source_sha": document["candidate_sha"],
            "platform": "linux/amd64",
            "sbom_sha256": "sha256:" + "e" * 64,
            "vulnerability_report_sha256": "sha256:" + "f" * 64,
            "highest_vulnerability_severity": "high",
        }
    return document, private, keyring


def test_cli_create_and_verify_then_reject_tampered_platform_image(tmp_path: Path) -> None:
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
        "verify",
        "--candidate",
        str(output / "candidate.json"),
        "--runtime-profile",
        str(output / "runtime-profile.json"),
        "--trusted-keyring",
        str(trust),
    ]
    assert subprocess.run(verify, capture_output=True, env=environment).returncode == 0
    manifest = json.loads((output / "candidate.json").read_text())
    manifest["images"]["web"]["image_ref"] = (
        manifest["images"]["web"]["image_ref"].split("@")[0] + "@sha256:" + "9" * 64
    )
    (output / "candidate.json").write_text(json.dumps(manifest))
    result = subprocess.run(verify, capture_output=True, text=True, env=environment)
    assert result.returncode == 1
    assert "InvalidSignature" in result.stderr
    assert private.read_text() not in result.stderr


@pytest.mark.parametrize(
    "change",
    ["dev", "foreign_registry", "mixed_sha", "missing_image", "critical", "bool_run", "mutable"],
)
def test_reject_invalid_build_binding(tmp_path: Path, change: str) -> None:
    document, private, keyring = inputs(tmp_path)
    if change == "dev":
        document["source_ref"] = "refs/heads/dev"
    elif change == "foreign_registry":
        document["registry_prefix"] = "ghcr.io/qianyi-sun"
    elif change == "mixed_sha":
        document["images"]["service"]["source_sha"] = "0" * 40
    elif change == "missing_image":
        del document["images"]["gateway"]
    elif change == "critical":
        document["images"]["service"]["highest_vulnerability_severity"] = "critical"
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
    assert candidate.validate_candidate(manifest, profile, trust) == manifest
    other = tmp_path / "other"
    other.mkdir()
    _, _, wrong_trust = inputs(other)
    with pytest.raises(InvalidSignature):
        candidate.validate_candidate(manifest, profile, wrong_trust)
    changed_profile = copy.deepcopy(profile)
    changed_profile["candidate_sha"] = "0" * 40
    with pytest.raises(ValueError, match="profile digest"):
        candidate.validate_candidate(manifest, changed_profile, trust)
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
