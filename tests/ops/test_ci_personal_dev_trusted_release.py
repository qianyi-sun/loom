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
_TRIVY_AMD64 = b"trivy-linux-amd64-v0.70.0"
_TRIVY_ARM64 = b"trivy-linux-arm64-v0.70.0"
_DATABASE = b"fixture-vulnerability-database"
_DATABASE_METADATA = b'{"DownloadedAt":"2026-08-18T00:00:00Z","NextUpdate":"2026-08-19T00:00:00Z","UpdatedAt":"2026-08-18T00:00:00Z","Version":2}'
_JAVA_DATABASE = b"fixture-java-database"
_JAVA_DATABASE_METADATA = b'{"DownloadedAt":"2026-08-18T00:00:00Z","NextUpdate":"2026-08-19T00:00:00Z","UpdatedAt":"2026-08-18T00:00:00Z","Version":1}'
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
    "personal-dev-scanner-cache": {
        "release_key": "personal_dev_scanner_cache",
        "image_name": "loom-personal-dev-scanner-cache",
        "dockerfile": "deploy/Dockerfile.personal-dev-scanner-cache",
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


def _scanner_paths(root: Path) -> dict[str, Path]:
    return {
        "scanner_cache_lock_file": root / "scanner-cache-lock.json",
        "scanner_cache_evidence_file": root / "scanner-cache-evidence.json",
        "scanner_binary_amd64_file": root / "trivy-linux-amd64",
        "scanner_binary_arm64_file": root / "trivy-linux-arm64",
    }


def _write_scanner_inputs(root: Path) -> dict[str, object]:
    paths = _scanner_paths(root)
    lock: dict[str, object] = {
        "binary_sha256": {
            "linux/amd64": hashlib.sha256(_TRIVY_AMD64).hexdigest(),
            "linux/arm64": hashlib.sha256(_TRIVY_ARM64).hexdigest(),
        },
        "database": {
            "image": (
                "ghcr.io/aquasecurity/trivy-db@sha256:"
                "01edd081af12fd613776b0db66ac23ce62c9d25802d8ee57671394c10ca3530b"
            ),
            "layer_sha256": (
                "cafb664d1c10b65e06b317f86171d65ed1f17b1f4de594a7232e16c0848f3590"
            ),
        },
        "java_database": {
            "image": (
                "ghcr.io/aquasecurity/trivy-java-db@sha256:"
                "58ef30d104106166d34f36c9861f2c5eb88d3279341fd4838bb5694d8998c436"
            ),
            "layer_sha256": (
                "bcc9ee0a8aa79524502cf892eda69e2180b54a3c7bd54c874b564201d2bdfc10"
            ),
        },
        "schema_version": 1,
        "trivy_version": "v0.70.0",
    }
    lock_bytes = _canonical(lock) + b"\n"
    paths["scanner_cache_lock_file"].write_bytes(lock_bytes)
    database_sha256 = hashlib.sha256(_DATABASE).hexdigest()
    database_metadata_sha256 = hashlib.sha256(_DATABASE_METADATA).hexdigest()
    java_database_sha256 = hashlib.sha256(_JAVA_DATABASE).hexdigest()
    java_database_metadata_sha256 = hashlib.sha256(_JAVA_DATABASE_METADATA).hexdigest()
    evidence = {
        "binary_platform": "linux/amd64",
        "binary_sha256": hashlib.sha256(_TRIVY_AMD64).hexdigest(),
        "database": {
            **lock["database"],  # type: ignore[dict-item]
            "metadata_sha256": database_metadata_sha256,
            "sha256": database_sha256,
        },
        "java_database": {
            **lock["java_database"],  # type: ignore[dict-item]
            "metadata_sha256": java_database_metadata_sha256,
            "sha256": java_database_sha256,
        },
        "lock_sha256": hashlib.sha256(lock_bytes).hexdigest(),
        "schema_version": 1,
        "trivy_version": "v0.70.0",
    }
    paths["scanner_cache_evidence_file"].write_bytes(_canonical(evidence) + b"\n")
    paths["scanner_binary_amd64_file"].write_bytes(_TRIVY_AMD64)
    paths["scanner_binary_arm64_file"].write_bytes(_TRIVY_ARM64)
    return evidence


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
    _write_scanner_inputs(tmp_path)
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
        **_scanner_paths(external.parent),
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
        **_scanner_paths(tmp_path),
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

    scanner_without_identity = {
        "binary_platform": "linux/amd64",
        "binary_sha256": hashlib.sha256(_TRIVY_AMD64).hexdigest(),
        "database_metadata_sha256": hashlib.sha256(_DATABASE_METADATA).hexdigest(),
        "database_sha256": hashlib.sha256(_DATABASE).hexdigest(),
        "java_database_metadata_sha256": hashlib.sha256(
            _JAVA_DATABASE_METADATA
        ).hexdigest(),
        "java_database_sha256": hashlib.sha256(_JAVA_DATABASE).hexdigest(),
        "lock_sha256": hashlib.sha256(
            _scanner_paths(tmp_path)["scanner_cache_lock_file"].read_bytes()
        ).hexdigest(),
        "trivy_version": "v0.70.0",
    }
    cache_identity_sha256 = hashlib.sha256(
        b"loom-personal-dev-scanner-cache-v1\0" + _canonical(scanner_without_identity)
    ).hexdigest()
    assert cache_identity_sha256 == (
        "372f0a621475d46ed71fb32a49e6768053049dd39cdd5686a0bd44a20d85a610"
    )
    scanner = {
        **scanner_without_identity,
        "cache_identity_sha256": cache_identity_sha256,
    }
    assert release == {
        "schema_version": 2,
        "source_sha": _SOURCE_SHA,
        "source_tree": _SOURCE_TREE,
        "images": references,
        "scanner": scanner,
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
    assert evidence["schema_version"] == 2
    assert set(evidence["internal_images"]) == set(_INTERNAL)
    assert set(evidence["external_images"]) == set(_EXTERNAL_REPOSITORIES)
    assert evidence["scanner"] == {
        "binary_sha256": {
            "linux/amd64": hashlib.sha256(_TRIVY_AMD64).hexdigest(),
            "linux/arm64": hashlib.sha256(_TRIVY_ARM64).hexdigest(),
        },
        "cache_identity_frame": "loom-personal-dev-scanner-cache-v1",
        "database": json.loads(
            (_scanner_paths(tmp_path)["scanner_cache_evidence_file"]).read_bytes()
        )["database"],
        "java_database": json.loads(
            (_scanner_paths(tmp_path)["scanner_cache_evidence_file"]).read_bytes()
        )["java_database"],
        "lock_sha256": scanner_without_identity["lock_sha256"],
        "trivy_version": "v0.70.0",
    }
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
        **_scanner_paths(tmp_path),
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


@pytest.mark.parametrize(
    "drift",
    [
        "amd64-binary",
        "arm64-binary",
        "evidence-binary",
        "evidence-lock",
        "database-source",
        "database-metadata",
        "unknown-evidence-field",
        "evidence-schema",
        "lock",
    ],
)
def test_assembly_rejects_scanner_input_drift(tmp_path: Path, drift: str) -> None:
    records, manifests, external, _references = _write_inputs(tmp_path)
    paths = _scanner_paths(tmp_path)
    if drift == "amd64-binary":
        paths["scanner_binary_amd64_file"].write_bytes(b"changed-amd64")
    elif drift == "arm64-binary":
        paths["scanner_binary_arm64_file"].write_bytes(b"changed-arm64")
    elif drift == "lock":
        lock = json.loads(paths["scanner_cache_lock_file"].read_bytes())
        lock["binary_sha256"]["linux/amd64"] = "f" * 64
        paths["scanner_cache_lock_file"].write_bytes(_canonical(lock) + b"\n")
    else:
        evidence = json.loads(paths["scanner_cache_evidence_file"].read_bytes())
        if drift == "evidence-binary":
            evidence["binary_sha256"] = "f" * 64
        elif drift == "evidence-lock":
            evidence["lock_sha256"] = "f" * 64
        elif drift == "database-source":
            evidence["database"]["layer_sha256"] = "f" * 64
        elif drift == "database-metadata":
            evidence["database"]["metadata_sha256"] = "0" * 64
        elif drift == "unknown-evidence-field":
            evidence["unexpected"] = True
        else:
            evidence["schema_version"] = 2
        paths["scanner_cache_evidence_file"].write_bytes(
            _canonical(evidence) + b"\n"
        )

    with pytest.raises(TrustedReleaseError):
        _assemble_inputs(records, manifests, external)


@pytest.mark.parametrize("kind", ["record", "manifest"])
def test_assembly_requires_both_platforms_of_cache_image(
    tmp_path: Path,
    kind: str,
) -> None:
    records, manifests, external, _references = _write_inputs(tmp_path)
    target = (
        records / "personal-dev-scanner-cache-arm64.json"
        if kind == "record"
        else manifests / "personal-dev-scanner-cache.json"
    )
    target.unlink()

    with pytest.raises(TrustedReleaseError, match="exactly the expected files"):
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
        "--scanner-cache-lock-file",
        str(_scanner_paths(tmp_path)["scanner_cache_lock_file"]),
        "--scanner-cache-evidence-file",
        str(_scanner_paths(tmp_path)["scanner_cache_evidence_file"]),
        "--scanner-binary-amd64-file",
        str(_scanner_paths(tmp_path)["scanner_binary_amd64_file"]),
        "--scanner-binary-arm64-file",
        str(_scanner_paths(tmp_path)["scanner_binary_arm64_file"]),
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

    release_file = output / "trusted-release.json"
    original_release = json.loads(release_file.read_bytes())
    for drift in ("schema", "cache-identity", "unknown-scanner-field"):
        changed_release = json.loads(_canonical(original_release))
        if drift == "schema":
            changed_release["schema_version"] = 1
        elif drift == "cache-identity":
            changed_release["scanner"]["cache_identity_sha256"] = "f" * 64
        else:
            changed_release["scanner"]["unexpected"] = True
        changed_bytes = _canonical(changed_release)
        release_file.write_bytes(changed_bytes)
        (output / "trusted-release.sha256").write_text(
            hashlib.sha256(changed_bytes).hexdigest() + "\n",
            encoding="ascii",
        )
        changed_validate = subprocess.run(
            validate_command,
            cwd=_ROOT,
            text=True,
            capture_output=True,
            check=False,
        )
        assert changed_validate.returncode != 0
    original_release_bytes = _canonical(original_release)
    release_file.write_bytes(original_release_bytes)
    (output / "trusted-release.sha256").write_text(
        hashlib.sha256(original_release_bytes).hexdigest() + "\n",
        encoding="ascii",
    )

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


def test_images_workflow_publishes_release_bound_scanner_cache() -> None:
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
    assert len(downloads) == 9
    script = "\n".join(str(step.get("run", "")) for step in aggregate["steps"])
    for component in _INTERNAL:
        for architecture in ("amd64", "arm64"):
            assert (
                f"image-release-record-{component}-{architecture}-run-${{{{ github.run_id }}}}-"
                "attempt-${{ github.run_attempt }}"
            ) in str(aggregate)
    assert script.count("gh attestation verify") == 1
    assert (
        "for component in service personal-dev-builder "
        "personal-dev-activation-agent personal-dev-scanner-cache" in script
    )
    assert "personal-dev-scanner-cache-assets-run-${{ github.run_id }}" in str(aggregate)
    assert "docker create" in script
    assert "docker cp" in script
    assert "--network none" not in script
    assert "--scanner-cache-lock-file" in script
    assert "--scanner-cache-evidence-file" in script
    assert "--scanner-binary-amd64-file" in script
    assert "--scanner-binary-arm64-file" in script
    assert script.count("prepare_personal_dev_scanner_cache_assets.py") >= 1
    assert "cmp --silent" in script
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
