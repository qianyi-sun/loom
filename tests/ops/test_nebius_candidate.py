from __future__ import annotations

import argparse
import base64
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
import yaml
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from scripts.ops import nebius_candidate as candidate


def test_tooling_step_ignores_unrelated_apt_sources() -> None:
    workflow = yaml.safe_load((candidate.ROOT / candidate.WORKFLOW).read_text())
    script = next(
        step["run"]
        for job in workflow["jobs"].values()
        for step in job["steps"]
        if step.get("name") == "Install locked candidate tooling"
    )
    # Exercise the actual Bash step; an unscoped APT command simulates a broken
    # vendor repository. Package installation and the existing pin check remain.
    prelude = r"""
    uv() { :; }
    test() {
      if [[ "$*" == '-s /etc/apt/sources.list.d/ubuntu.sources' ]]; then return 0; fi
      builtin test "$@"
    }
    sudo() {
      [[ "$1" == apt-get ]] || return 1
      shift
      [[ "$1" == -o && "$2" == Dir::Etc::sourcelist=/etc/apt/sources.list.d/ubuntu.sources ]] || return 100
      shift 2
      [[ "$1" == -o && "$2" == Dir::Etc::sourceparts=- ]] || return 100
      shift 2
      printf '%s\n' "$*"
    }
    dpkg-query() { printf '%s' '1.13.3+ds1-2ubuntu0.24.04.3'; }
    skopeo() { printf '%s\n' 'skopeo version 1.13.3'; }
    """
    result = subprocess.run(
        ["bash", "--noprofile", "--norc", "-eo", "pipefail", "-c", prelude + script],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.splitlines() == [
        "update",
        "install -y --no-install-recommends skopeo=1.13.3+ds1-2ubuntu0.24.04.3",
        "skopeo version 1.13.3",
    ]


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
    assert "tb90_task" not in manifest["images"]
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
    assert parsed.agent_image_ref == manifest["images"]["harbor_runtime"]["image_ref"]
    assert {row.statement.image_ref for row in parsed.image_admission.admissions} == {
        parsed.task_image_ref, parsed.runtime_image_ref, parsed.agent_image_ref,
    }
    verify_execution_image_admission(
        parsed.image_admission,
        required_image_refs=tuple(
            manifest["images"][key]["image_ref"] for key in candidate.EXECUTION_COMPONENTS
        ),
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


@pytest.mark.parametrize("generation", ["six-images", "worker-tb90", "harbor-tb90"])
def test_historical_candidate_is_readable_but_new_publication_requires_current_images(
    tmp_path: Path, generation: str,
) -> None:
    document, private, trust = inputs(tmp_path)
    document["images"] = {
        key: value
        for key, value in document["images"].items()
        if key in candidate.LEGACY_COMPONENTS
    }
    if generation != "six-images":
        components = {"tb90_task": "loom-nebius-terminal-bench"}
        if generation == "worker-tb90":
            components["worker"] = "loom-worker"
        else:
            components["harbor_runtime"] = "loom-harbor-runtime"
        for component, name in components.items():
            document["images"][component] = {
                "image_ref": f"{document['registry_prefix']}/{name}@sha256:" + "a" * 64,
            }
    candidate.validate_identity(document)
    with pytest.raises(ValueError, match="configured platform and execution images"):
        candidate.create_candidate(
            document, signing_key=private, signing_key_id="publisher", keyring_json=trust
        )
    if generation != "six-images":
        document["images"]["tb90_task"]["image_ref"] = "docker.io/foreign@sha256:" + "a" * 64
        with pytest.raises(ValueError, match="candidate image identity"):
            candidate.validate_identity(document)


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


def runtime_metadata(version: str = "test-1") -> dict[str, str]:
    return {
        "agent_name": "terminus-2", "agent_version": version,
        "runtime_contract": "loom.terminus-controller.v1", "harbor_version": "0.18.0",
        "harbor_source_revision": "527d50deb63a5d279e8c20593c18a2cbc7f61f9e",
        "loom_bridge_revision": "1.0", "publisher_source_revision": "a" * 40,
    }


def test_runtime_release_cli_outputs_only_reusable_single_image_record(tmp_path: Path) -> None:
    document, private, keyring = inputs(tmp_path)
    document["images"] = {"harbor_runtime": document["images"]["harbor_runtime"]}
    document["runtime_metadata"] = runtime_metadata()
    record, trust = tmp_path / "build.json", tmp_path / "trust.json"
    record.write_text(json.dumps(document))
    trust.write_text(keyring)
    output = tmp_path / "release"
    result = subprocess.run(
        [sys.executable, str(candidate.ROOT / "scripts/ops/nebius_candidate.py"),
         "create-runtime-release", "--build-record", str(record), "--signing-key", str(private),
         "--signing-key-id", "publisher", "--trusted-keyring", str(trust), "--output", str(output)],
        capture_output=True, text=True, env={**os.environ, "PYTHONPATH": str(candidate.ROOT / "src")},
    )
    assert result.returncode == 0, result.stderr
    assert {path.name for path in output.iterdir()} == {"agent-runtime-release.json"}
    release = json.loads((output / "agent-runtime-release.json").read_text())
    assert release["schema_version"] == "loom.agent-runtime-release.v1"
    assert {key: release[key] for key in runtime_metadata()} == runtime_metadata()
    assert set(release) == set(runtime_metadata()) | {"schema_version", "agent_image_ref", "image_admission"}
    from loom.execution_image_admission import verify_execution_image_admission

    verify_execution_image_admission(
        candidate.ExecutionImageAdmissionBundleV1(
            schema_version="loom.execution-image-admission.v1",
            admissions=(candidate.SignedImageAdmissionV1.model_validate(release["image_admission"]),),
        ), required_image_refs=(release["agent_image_ref"],),
        keyring=candidate.ImageAdmissionKeyring.from_json(keyring),
    )


@pytest.mark.parametrize("fault", ["version", "source", "repository", "extra_image"])
def test_runtime_release_rejects_bad_binding(tmp_path: Path, fault: str) -> None:
    document, private, trust = inputs(tmp_path)
    document["images"] = {"harbor_runtime": document["images"]["harbor_runtime"]}
    document["runtime_metadata"] = runtime_metadata()
    if fault == "version":
        document["runtime_metadata"]["agent_version"] = "bad label"
    elif fault == "source":
        document["runtime_metadata"]["publisher_source_revision"] = "b" * 40
    elif fault == "repository":
        document["images"]["harbor_runtime"]["image_ref"] = "docker.io/untrusted@sha256:" + "a" * 64
    else:
        document["images"]["worker"] = document["images"]["harbor_runtime"]
    with pytest.raises(ValueError):
        candidate.create_runtime_release(document, signing_key=private,
            signing_key_id="publisher", keyring_json=trust)


@pytest.mark.parametrize("mode", ["harness-only", "platform"])
def test_publication_builds_selected_images_and_reuses_platform_admission(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mode: str,
) -> None:
    import shutil

    document, private, keyring = inputs(tmp_path)
    trust = tmp_path / "keyring.json"
    trust.write_text(keyring)
    version = "test-1" if mode == "harness-only" else "nebius-" + "a" * 40
    archive = oci_fixture(tmp_path, metadata=runtime_metadata(version))
    digest, _ = candidate.inspect_oci_archive(archive, candidate="a" * 40)
    calls: list[tuple[str, ...]] = []
    for key, value in {
        "GITHUB_SHA": "a" * 40, "GITHUB_REPOSITORY": candidate.REPOSITORY,
        "GITHUB_REF": candidate.SOURCE_REF, "GITHUB_EVENT_NAME": "workflow_dispatch",
        "GITHUB_WORKFLOW_REF": f"{candidate.REPOSITORY}/{candidate.WORKFLOW}@{candidate.SOURCE_REF}",
        "GITHUB_RUN_ID": "123", "NEBIUS_REGISTRY_CREDENTIALS_FILE": str(tmp_path / "fake-key"),
        "REGISTRY_AUTH_FILE": str(tmp_path / "fake-auth"),
    }.items():
        monkeypatch.setenv(key, value)

    def run(*args: str) -> str:
        calls.append(args)
        if args[:2] == ("git", "rev-parse"):
            return "a" * 40
        if args[0] == "uname":
            return "x86_64"
        if len(args) > 1 and args[1] == "build":
            destination = args[args.index("--output") + 1].split("dest=", 1)[1]
            shutil.copyfile(archive, destination)
        elif args[0] == "scanner":
            Path(args[args.index("--output") + 1]).write_text('{"Trivy":{"Version":"0.74.0"},"Results":[]}')
        elif args[:2] == ("skopeo", "inspect"):
            return digest
        return ""

    monkeypatch.setattr(candidate, "_run", run)
    monkeypatch.setattr(candidate, "install_trivy", lambda *args, **kwargs: Path("scanner"))
    validation_options = []
    monkeypatch.setattr(
        candidate, "validate_trivy_release_report",
        lambda *args, **kwargs: validation_options.append(kwargs),
    )
    monkeypatch.setattr(candidate, "refresh_registry_auth", lambda *args: None)
    output = tmp_path / "publication"
    candidate.build(argparse.Namespace(
        mode=mode, agent_version="test-1" if mode == "harness-only" else None, output=output,
        registry_prefix=document["registry_prefix"], signing_key=private,
        signing_key_id="publisher", trusted_keyring=trust,
    ))
    builds = [call for call in calls if len(call) > 1 and call[1] == "build"]
    expected = 1 if mode == "harness-only" else len(candidate.COMPONENTS)
    assert len(builds) == expected
    assert not any("nebius-terminal-bench" in arg for call in calls for arg in call)
    assert validation_options == [{"use_exceptions": False}] * expected
    harbor = next(call for call in builds if "filename=Dockerfile.harbor-runtime" in call)
    assert f"build-arg:LOOM_AGENT_VERSION={version}" in harbor
    assert len([call for call in calls if call[:2] == ("skopeo", "copy")]) == expected
    release = json.loads((output / "agent-runtime-release.json").read_text())
    if mode == "harness-only":
        assert not (output / "candidate.json").exists()
        assert not (output / "runtime-profile.json").exists()
    else:
        manifest = json.loads((output / "candidate.json").read_text())
        profile = json.loads((output / "runtime-profile.json").read_text())
        assert "worker" not in manifest["images"]
        assert "tb90_task" not in manifest["images"]
        assert profile["agent_image_ref"] == manifest["images"]["harbor_runtime"]["image_ref"]
        assert release["image_admission"] in profile["image_admission"]["admissions"]


def test_duplicate_input_fields_rejected(tmp_path: Path) -> None:
    path = tmp_path / "bad.json"
    path.write_text('{"candidate_sha":"a","candidate_sha":"b"}')
    with pytest.raises(ValueError, match="duplicate"):
        candidate.read_json(path)


def oci_fixture(
    tmp_path: Path, *, revision: str = "a" * 40, corrupt: bool = False,
    metadata: dict[str, str] | None = None,
) -> Path:
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
                "config": {"Labels": {"org.opencontainers.image.revision": revision,
                    **{"io.loom." + key: value for key, value in (metadata or {}).items()}}},
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
