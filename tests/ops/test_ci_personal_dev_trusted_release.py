from __future__ import annotations

import hashlib
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest
import yaml
from scripts.ci_image_release_evidence import architecture_record
from scripts.ci_personal_dev_trusted_release import (
    TrustedReleaseError,
    assemble_personal_dev_trusted_release,
)

_ROOT = Path(__file__).resolve().parents[2]
_SOURCE_SHA = "a" * 40
_SOURCE_TREE = "b" * 40
_RUN_ID = 123
_RUN_ATTEMPT = 2
_INTERNAL = {
    "service": {
        "release_key": "loom_service",
        "image_name": "loom-service",
        "dockerfile": "deploy/Dockerfile.service",
    },
    "personal-dev-builder": {
        "release_key": "personal_dev_builder",
        "image_name": "loom-personal-dev-builder",
        "dockerfile": "deploy/Dockerfile.personal-dev-builder",
    },
    "personal-dev-activation-agent": {
        "release_key": "personal_dev_activation_agent",
        "image_name": "loom-personal-dev-activation-agent",
        "dockerfile": "deploy/Dockerfile.personal-dev-activation-agent",
    },
}
_EXTERNAL_REPOSITORIES = {
    "postgres": "docker.io/library/postgres",
    "minio": "quay.io/minio/minio",
    "minio_client": "quay.io/minio/mc",
}


def _canonical(value: object) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")


def _manifest(members: dict[str, str]) -> dict[str, Any]:
    return {
        "manifests": [
            {
                "digest": members[platform],
                "mediaType": "application/vnd.oci.image.manifest.v1+json",
                "platform": {
                    "architecture": platform.removeprefix("linux/"),
                    "os": "linux",
                },
                "size": 1234,
            }
            for platform in ("linux/amd64", "linux/arm64")
        ],
        "mediaType": "application/vnd.oci.image.index.v1+json",
        "schemaVersion": 2,
    }


def _write_inputs(tmp_path: Path) -> tuple[Path, Path, Path, dict[str, str]]:
    records_dir = tmp_path / "records"
    manifests_dir = tmp_path / "manifests"
    records_dir.mkdir()
    manifests_dir.mkdir()
    expected_references: dict[str, str] = {}

    digest_counter = 1
    for component, contract in _INTERNAL.items():
        members: dict[str, str] = {}
        for architecture in ("amd64", "arm64"):
            platform = f"linux/{architecture}"
            child_digest = f"sha256:{digest_counter:064x}"
            scan_digest = f"{digest_counter + 100:064x}"
            members[platform] = child_digest
            record = architecture_record(
                repository="qianyi-sun/loom",
                ref_name="dev",
                head_sha=_SOURCE_SHA,
                tree_sha=_SOURCE_TREE,
                run_id=_RUN_ID,
                run_attempt=_RUN_ATTEMPT,
                event_name="workflow_dispatch",
                repository_id="123456789",
                repository_owner_id="987654321",
                runner_environment="github-hosted",
                image=component,
                image_name=str(contract["image_name"]),
                dockerfile=str(contract["dockerfile"]),
                build_context=".",
                platform=platform,
                architecture=architecture,
                subject_name=f"ghcr.io/qianyi-sun/{contract['image_name']}",
                subject_digest=child_digest,
                scan_report_sha256=scan_digest,
                build_mode="trusted-rebuild",
            )
            (records_dir / f"{component}-{architecture}.json").write_bytes(
                _canonical(record) + b"\n"
            )
            digest_counter += 1
        manifest_bytes = _canonical(_manifest(members))
        (manifests_dir / f"{component}.json").write_bytes(manifest_bytes)
        index_digest = hashlib.sha256(manifest_bytes).hexdigest()
        expected_references[str(contract["release_key"])] = (
            f"ghcr.io/qianyi-sun/{contract['image_name']}@sha256:{index_digest}"
        )

    external: dict[str, Any] = {"schema_version": 1, "images": {}}
    for key, repository in _EXTERNAL_REPOSITORIES.items():
        members = {
            "linux/amd64": f"sha256:{digest_counter:064x}",
            "linux/arm64": f"sha256:{digest_counter + 1:064x}",
        }
        manifest_bytes = _canonical(_manifest(members))
        (manifests_dir / f"{key}.json").write_bytes(manifest_bytes)
        index_digest = hashlib.sha256(manifest_bytes).hexdigest()
        reference = f"{repository}@sha256:{index_digest}"
        external["images"][key] = {"members": members, "reference": reference}
        expected_references[key] = reference
        digest_counter += 2
    external_path = tmp_path / "external-images.json"
    external_path.write_bytes(_canonical(external) + b"\n")
    return records_dir, manifests_dir, external_path, expected_references


def _assemble(tmp_path: Path) -> tuple[dict[str, object], dict[str, object]]:
    records, manifests, external, _references = _write_inputs(tmp_path)
    return _assemble_inputs(records, manifests, external)


def _assemble_inputs(
    records: Path,
    manifests: Path,
    external: Path,
    **overrides: object,
) -> tuple[dict[str, object], dict[str, object]]:
    arguments: dict[str, object] = {
        "records_dir": records,
        "manifests_dir": manifests,
        "external_images_file": external,
        "repository": "qianyi-sun/loom",
        "ref_name": "dev",
        "source_sha": _SOURCE_SHA,
        "source_tree": _SOURCE_TREE,
        "run_id": _RUN_ID,
        "run_attempt": _RUN_ATTEMPT,
        "event_name": "workflow_dispatch",
        "repository_id": "123456789",
        "repository_owner_id": "987654321",
        "runner_environment": "github-hosted",
    }
    arguments.update(overrides)
    return assemble_personal_dev_trusted_release(  # type: ignore[arg-type]
        **arguments,
    )


def test_assembly_binds_exact_internal_external_and_release_evidence(
    tmp_path: Path,
) -> None:
    records, manifests, external, references = _write_inputs(tmp_path)

    release, evidence = assemble_personal_dev_trusted_release(
        records_dir=records,
        manifests_dir=manifests,
        external_images_file=external,
        repository="qianyi-sun/loom",
        ref_name="dev",
        source_sha=_SOURCE_SHA,
        source_tree=_SOURCE_TREE,
        run_id=_RUN_ID,
        run_attempt=_RUN_ATTEMPT,
        event_name="workflow_dispatch",
        repository_id="123456789",
        repository_owner_id="987654321",
        runner_environment="github-hosted",
    )

    assert release == {
        "schema_version": 1,
        "source_sha": _SOURCE_SHA,
        "source_tree": _SOURCE_TREE,
        "images": references,
        "release_evidence_sha256": hashlib.sha256(_canonical(evidence)).hexdigest(),
    }
    assert evidence["release"] == {
        "repository": "qianyi-sun/loom",
        "ref": "refs/heads/dev",
        "commit": _SOURCE_SHA,
        "tree": _SOURCE_TREE,
        "run_id": _RUN_ID,
        "run_attempt": _RUN_ATTEMPT,
    }
    assert set(evidence["internal_images"]) == set(_INTERNAL)
    assert set(evidence["external_images"]) == set(_EXTERNAL_REPOSITORIES)
    for item in evidence["internal_images"].values():
        assert set(item["platforms"]) == {"linux/amd64", "linux/arm64"}
        assert all(
            platform["build"] == {"mode": "trusted-rebuild"}
            and len(platform["scan_report_sha256"]) == 64
            for platform in item["platforms"].values()
        )

    repeated_release, repeated_evidence = assemble_personal_dev_trusted_release(
        records_dir=records,
        manifests_dir=manifests,
        external_images_file=external,
        repository="qianyi-sun/loom",
        ref_name="dev",
        source_sha=_SOURCE_SHA,
        source_tree=_SOURCE_TREE,
        run_id=_RUN_ID,
        run_attempt=_RUN_ATTEMPT,
        event_name="workflow_dispatch",
        repository_id="123456789",
        repository_owner_id="987654321",
        runner_environment="github-hosted",
    )
    assert _canonical(repeated_release) == _canonical(release)
    assert _canonical(repeated_evidence) == _canonical(evidence)


@pytest.mark.parametrize(
    ("target", "mutate"),
    [
        (
            "external",
            lambda payload: payload + b"\n",
        ),
        (
            "external",
            lambda payload: payload.replace(b'"images":', b'"images":{},"images":', 1),
        ),
        (
            "external",
            lambda payload: payload.replace(b"@sha256:", b":mutable-tag@sha256:", 1),
        ),
        (
            "external",
            lambda payload: payload.replace(b"sha256:", b"sha256:A", 1)[:-1],
        ),
        (
            "record",
            lambda payload: payload.replace(_SOURCE_SHA.encode(), ("c" * 40).encode(), 1),
        ),
        (
            "record",
            lambda payload: payload.replace(b"trusted-rebuild", b"candidate-reuse", 1),
        ),
        (
            "manifest",
            lambda payload: payload.replace(b"sha256:", b"sha256:f", 1)[:-1],
        ),
    ],
)
def test_assembly_rejects_noncanonical_or_inconsistent_evidence(
    tmp_path: Path,
    target: str,
    mutate: Any,
) -> None:
    records, manifests, external, _references = _write_inputs(tmp_path)
    paths = {
        "external": external,
        "record": records / "service-amd64.json",
        "manifest": manifests / "service.json",
    }
    path = paths[target]
    path.write_bytes(mutate(path.read_bytes()))

    with pytest.raises(TrustedReleaseError):
        _assemble_inputs(records, manifests, external)


@pytest.mark.parametrize("directory", ["records", "manifests"])
def test_assembly_rejects_extra_handoff_files(tmp_path: Path, directory: str) -> None:
    records, manifests, external, _references = _write_inputs(tmp_path)
    selected = records if directory == "records" else manifests
    (selected / "unexpected.json").write_text("{}", encoding="utf-8")

    with pytest.raises(TrustedReleaseError, match="exactly the expected files"):
        _assemble_inputs(records, manifests, external)


def test_assembly_rejects_duplicate_final_index_digests(tmp_path: Path) -> None:
    records, manifests, external, _references = _write_inputs(tmp_path)
    postgres_manifest = manifests / "postgres.json"
    (manifests / "minio.json").write_bytes(postgres_manifest.read_bytes())
    binding = json.loads(external.read_bytes())
    binding["images"]["minio"]["members"] = binding["images"]["postgres"]["members"]
    binding["images"]["minio"]["reference"] = (
        "quay.io/minio/minio@sha256:"
        + hashlib.sha256(postgres_manifest.read_bytes()).hexdigest()
    )
    external.write_bytes(_canonical(binding) + b"\n")

    with pytest.raises(TrustedReleaseError, match="distinct"):
        _assemble_inputs(records, manifests, external)


def test_external_indexes_bind_target_members_without_forbidding_other_platforms(
    tmp_path: Path,
) -> None:
    records, manifests, external, _references = _write_inputs(tmp_path)
    postgres_manifest = manifests / "postgres.json"
    manifest = json.loads(postgres_manifest.read_bytes())
    manifest["manifests"].append(
        {
            "digest": "sha256:" + "d" * 64,
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "platform": {"architecture": "ppc64le", "os": "linux"},
            "size": 2345,
        }
    )
    manifest_bytes = _canonical(manifest)
    postgres_manifest.write_bytes(manifest_bytes)
    binding = json.loads(external.read_bytes())
    reference = (
        "docker.io/library/postgres@sha256:"
        + hashlib.sha256(manifest_bytes).hexdigest()
    )
    binding["images"]["postgres"]["reference"] = reference
    external.write_bytes(_canonical(binding) + b"\n")

    release, evidence = _assemble_inputs(records, manifests, external)

    assert release["images"]["postgres"] == reference
    assert set(evidence["external_images"]["postgres"]["platforms"]) == {
        "linux/amd64",
        "linux/arm64",
    }


def test_cli_writes_and_revalidates_only_the_three_canonical_outputs(
    tmp_path: Path,
) -> None:
    records, manifests, external, _references = _write_inputs(tmp_path)
    output = tmp_path / "output"
    common = [
        "--records-dir",
        str(records),
        "--manifests-dir",
        str(manifests),
        "--external-images-file",
        str(external),
        "--repository",
        "qianyi-sun/loom",
        "--ref-name",
        "dev",
        "--source-sha",
        _SOURCE_SHA,
        "--source-tree",
        _SOURCE_TREE,
        "--run-id",
        str(_RUN_ID),
        "--run-attempt",
        str(_RUN_ATTEMPT),
        "--event-name",
        "workflow_dispatch",
        "--repository-id",
        "123456789",
        "--repository-owner-id",
        "987654321",
        "--runner-environment",
        "github-hosted",
    ]
    assemble = subprocess.run(
        [
            sys.executable,
            str(_ROOT / "scripts/ci_personal_dev_trusted_release.py"),
            "assemble",
            *common,
            "--output-dir",
            str(output),
        ],
        cwd=_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )

    assert assemble.returncode == 0, assemble.stderr
    assert {path.name for path in output.iterdir()} == {
        "trusted-release.json",
        "trusted-release-evidence.json",
        "trusted-release.sha256",
    }
    assert all(path.stat().st_mode & 0o777 == 0o600 for path in output.iterdir())
    validate_command = [
        sys.executable,
        str(_ROOT / "scripts/ci_personal_dev_trusted_release.py"),
        "validate",
        *common,
        "--release-file",
        str(output / "trusted-release.json"),
        "--evidence-file",
        str(output / "trusted-release-evidence.json"),
        "--sha256-file",
        str(output / "trusted-release.sha256"),
    ]
    validate = subprocess.run(
        validate_command,
        cwd=_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    assert validate.returncode == 0, validate.stderr

    digest_file = output / "trusted-release.sha256"
    digest_payload = digest_file.read_bytes()
    linked_digest = tmp_path / "linked-digest"
    linked_digest.write_bytes(digest_payload)
    digest_file.unlink()
    digest_file.symlink_to(linked_digest)
    linked_validate = subprocess.run(
        validate_command,
        cwd=_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    assert linked_validate.returncode != 0


def test_checked_in_external_indexes_are_exact_reviewed_multi_arch_pins() -> None:
    value = json.loads(
        (_ROOT / "deploy/dev-fleet/personal-dev-external-images.json").read_bytes()
    )

    assert value == {
        "schema_version": 1,
        "images": {
            "postgres": {
                "reference": (
                    "docker.io/library/postgres@sha256:"
                    "60f4761b9035e0b8d5218f701a8c3382f641bf12b1604822574cf5be3baeb537"
                ),
                "members": {
                    "linux/amd64": (
                        "sha256:0933d60933003cb2d0e4f074ba8a83542fe203803fece22be97795e06ccbdfdc"
                    ),
                    "linux/arm64": (
                        "sha256:cc0b1748665c667b142432c3bf174cbe780d721627ffedac250130a95c0e952e"
                    ),
                },
            },
            "minio": {
                "reference": (
                    "quay.io/minio/minio@sha256:"
                    "a1ea29fa28355559ef137d71fc570e508a214ec84ff8083e39bc5428980b015e"
                ),
                "members": {
                    "linux/amd64": (
                        "sha256:3f97c5651cb6662b880c787a232b6b34fec8d8922e08d6617b25d241a21164bb"
                    ),
                    "linux/arm64": (
                        "sha256:54d3d6a0a58fb25b4e9943d1db3828d3b4de44666f911381b4fda57175488194"
                    ),
                },
            },
            "minio_client": {
                "reference": (
                    "quay.io/minio/mc@sha256:"
                    "aead63c77f9db9107f1696fb08ecb0faeda23729cde94b0f663edf4fe09728e3"
                ),
                "members": {
                    "linux/amd64": (
                        "sha256:2582c2f48b1e31545143ba5285c67d7b38c8b8f6912142d0630686dc7aaac28b"
                    ),
                    "linux/arm64": (
                        "sha256:d798ef4fe8f417b814a8968682c1e172cdfabe59da81b39e4d9cc108a355b271"
                    ),
                },
            },
        },
    }


def test_images_workflow_publishes_least_privilege_three_file_release() -> None:
    workflow = yaml.safe_load(
        (_ROOT / ".github/workflows/images.yml").read_text(encoding="utf-8")
    )
    jobs = workflow["jobs"]
    aggregate = jobs["personal-dev-trusted-release"]

    assert aggregate["needs"] == ["plan", "publish-manifest"]
    assert aggregate["runs-on"] == "ubuntu-24.04"
    assert aggregate["permissions"] == {
        "actions": "read",
        "attestations": "read",
        "contents": "read",
        "packages": "read",
    }
    condition = " ".join(aggregate["if"].split())
    assert "\\\"" not in condition
    for component in _INTERNAL:
        assert f"contains(needs.plan.outputs.images, '\"image\":\"{component}\"')" in condition
    downloads = [
        step
        for step in aggregate["steps"]
        if str(step.get("name", "")).startswith("Download exact ")
    ]
    assert len(downloads) == 6
    script = "\n".join(str(step.get("run", "")) for step in aggregate["steps"])
    for component in _INTERNAL:
        for architecture in ("amd64", "arm64"):
            assert (
                f"image-release-record-{component}-{architecture}-run-${{{{ github.run_id }}}}-"
                "attempt-${{ github.run_attempt }}"
            ) in str(aggregate)
    assert script.count("gh attestation verify") == 1
    assert "for component in service personal-dev-builder personal-dev-activation-agent" in script
    assert script.count("docker buildx imagetools inspect --raw") >= 2
    assert "ci_personal_dev_trusted_release.py assemble" in script
    assert "ci_personal_dev_trusted_release.py validate" in script
    upload = next(
        step
        for step in aggregate["steps"]
        if step.get("name") == "Upload protected personal-development trusted release"
    )
    assert upload["with"] == {
        "name": (
            "personal-dev-trusted-release-run-${{ github.run_id }}-"
            "attempt-${{ github.run_attempt }}"
        ),
        "path": (
            "/tmp/loom-personal-dev-trusted-release/trusted-release.json\n"
            "/tmp/loom-personal-dev-trusted-release/trusted-release-evidence.json\n"
            "/tmp/loom-personal-dev-trusted-release/trusted-release.sha256\n"
        ),
        "if-no-files-found": "error",
        "retention-days": 90,
    }
    assert "personal-dev-trusted-release" in jobs["images-gate"]["needs"]
