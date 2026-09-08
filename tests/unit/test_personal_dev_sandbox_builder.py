from __future__ import annotations

import json
import os
import subprocess
import tarfile
from pathlib import Path

import pytest

from loom import personal_dev_sandbox_builder as sandbox_builder
from loom.personal_dev_builder_artifact import (
    PersonalDevBuildArtifactError,
    verify_personal_dev_build_artifact,
)
from loom.personal_dev_builder_manifest import (
    PersonalDevBuilderManifestConfig,
    personal_dev_builder_manifest_documents,
)
from loom.personal_dev_candidate import PERSONAL_DEV_COMPONENTS
from loom.personal_dev_sandbox_builder import (
    _DOCKERFILES,
    PersonalDevSandboxBuildContract,
    PersonalDevSandboxBuildError,
    _build_images,
    create_personal_dev_build_artifact,
)
from tests.unit.test_personal_dev_builder import _registration
from tests.unit.test_personal_dev_builder_artifact import _oci_archive


def _contract() -> PersonalDevSandboxBuildContract:
    documents = personal_dev_builder_manifest_documents(
        _registration(),
        platform="linux/amd64",
        config=PersonalDevBuilderManifestConfig(
            builder_image="registry.example/builder@sha256:" + "a" * 64,
            max_artifact_bytes=2 * 1024 * 1024,
            max_image_archive_bytes=256 * 1024,
        ),
    )
    config_map = next(document for document in documents if document["kind"] == "ConfigMap")
    return PersonalDevSandboxBuildContract.parse(
        config_map["data"]["contract.json"].encode("ascii")
    )


def _image_outputs(directory: Path) -> dict[str, tuple[Path, str]]:
    directory.mkdir(parents=True, exist_ok=True)
    image_payload, manifest_digest = _oci_archive(architecture="amd64")
    images: dict[str, tuple[Path, str]] = {}
    for component in PERSONAL_DEV_COMPONENTS:
        path = directory / f"{component}.oci.tar"
        path.write_bytes(image_payload)
        images[component] = (path, manifest_digest)
    return images


def test_sandbox_contract_and_output_round_trip_through_trusted_verifier(
    tmp_path: Path,
) -> None:
    contract = _contract()
    images = _image_outputs(tmp_path)
    artifact = tmp_path / "artifacts.tar"

    create_personal_dev_build_artifact(contract, images, artifact)

    extracted = tmp_path / "extracted"
    extracted.mkdir()
    verified = verify_personal_dev_build_artifact(
        artifact,
        _registration(),
        platform="linux/amd64",
        output_directory=extracted,
        max_artifact_bytes=contract.max_artifact_bytes,
        max_image_archive_bytes=contract.max_image_archive_bytes,
    )
    assert set(verified.images) == set(PERSONAL_DEV_COMPONENTS)
    assert all(path.is_file() for path, _digest in images.values())


def test_consuming_artifact_creation_bounds_packaging_disk_peak(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    contract = _contract()
    images = _image_outputs(tmp_path)
    artifact = tmp_path / "artifacts.tar"
    total_image_bytes = sum(path.stat().st_size for path, _digest in images.values())
    largest_image_bytes = max(path.stat().st_size for path, _digest in images.values())
    observed_bytes = [total_image_bytes]
    real_fsync = os.fsync

    def measure_fsync(descriptor: int) -> None:
        real_fsync(descriptor)
        live_image_bytes = sum(
            path.stat().st_size for path, _digest in images.values() if path.exists()
        )
        output_bytes = max(
            os.fstat(descriptor).st_size,
            os.lseek(descriptor, 0, os.SEEK_CUR),
        )
        observed_bytes.append(live_image_bytes + output_bytes)

    monkeypatch.setattr(os, "fsync", measure_fsync)

    create_personal_dev_build_artifact(
        contract,
        images,
        artifact,
        consume_image_archives=True,
    )

    assert all(not path.exists() for path, _digest in images.values())
    assert artifact.is_file()
    assert max(observed_bytes) <= (total_image_bytes + largest_image_bytes + 2 * tarfile.RECORDSIZE)
    assert max(observed_bytes) < total_image_bytes + artifact.stat().st_size


@pytest.mark.parametrize("failing_flush", [None, 2, 10])
def test_sandbox_build_reclaims_private_images_without_uploading_partial_artifacts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    failing_flush: int | None,
) -> None:
    contract = _contract()
    images = _image_outputs(tmp_path / "private-images")
    contract_file = tmp_path / "contract.json"
    contract_file.write_bytes(
        json.dumps(dict(contract.raw), sort_keys=True, separators=(",", ":")).encode("ascii")
    )
    capabilities = tmp_path / "capabilities"
    capabilities.mkdir()
    (capabilities / "source-get-url").write_text(
        "https://example.invalid/source.tar",
        encoding="utf-8",
    )
    (capabilities / "artifact-upload.json").write_text(
        json.dumps({"url": "https://example.invalid/upload"}),
        encoding="utf-8",
    )
    uploaded: list[Path] = []

    monkeypatch.setattr(sandbox_builder, "_verify_client_identity", lambda: None)
    monkeypatch.setattr(
        sandbox_builder,
        "_download_source",
        lambda _url, _destination, _contract: None,
    )
    monkeypatch.setattr(
        sandbox_builder,
        "_extract_verified_source",
        lambda _archive, destination, _contract: destination.mkdir(),
    )
    monkeypatch.setattr(sandbox_builder, "_build_images", lambda *args, **kwargs: images)
    monkeypatch.setattr(
        sandbox_builder,
        "_upload_artifact",
        lambda _upload, artifact, **_kwargs: uploaded.append(artifact),
    )
    workspace = tmp_path / "workspace"
    artifact = workspace / "artifacts.tar"
    real_fsync = os.fsync
    flush_count = 0

    def fail_later_flush(descriptor: int) -> None:
        nonlocal flush_count
        real_fsync(descriptor)
        flush_count += 1
        if flush_count == failing_flush:
            raise OSError("injected later packaging failure")

    monkeypatch.setattr(os, "fsync", fail_later_flush)
    if failing_flush is None:
        sandbox_builder.run_personal_dev_sandbox_build(
            contract_file=contract_file,
            capability_directory=capabilities,
            workspace=workspace,
        )
        assert uploaded == [artifact]
        assert artifact.is_file()
        assert all(not path.exists() for path, _digest in images.values())
    else:
        with pytest.raises(OSError, match="injected later packaging failure"):
            sandbox_builder.run_personal_dev_sandbox_build(
                contract_file=contract_file,
                capability_directory=capabilities,
                workspace=workspace,
            )
        assert flush_count == failing_flush
        assert not uploaded
        assert not artifact.exists()
        for index, component in enumerate(PERSONAL_DEV_COMPONENTS):
            path, _digest = images[component]
            assert path.exists() is (index >= failing_flush - 1)


@pytest.mark.parametrize("replacement_kind", ["symlink", "hardlink"])
def test_consuming_artifact_rejects_replaced_image_without_deleting_it(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    replacement_kind: str,
) -> None:
    contract = _contract()
    images = _image_outputs(tmp_path)
    target, _digest = images[PERSONAL_DEV_COMPONENTS[0]]
    protected = tmp_path / "protected"
    protected.write_bytes(b"must remain")
    artifact = tmp_path / "artifacts.tar"
    real_fsync = os.fsync
    replaced = False

    def replace_after_flush(descriptor: int) -> None:
        nonlocal replaced
        real_fsync(descriptor)
        if replaced:
            return
        replaced = True
        target.unlink()
        if replacement_kind == "symlink":
            target.symlink_to(protected)
        else:
            os.link(protected, target)

    monkeypatch.setattr(os, "fsync", replace_after_flush)

    with pytest.raises(PersonalDevSandboxBuildError, match="changed"):
        create_personal_dev_build_artifact(
            contract,
            images,
            artifact,
            consume_image_archives=True,
        )

    assert protected.read_bytes() == b"must remain"
    assert target.is_symlink() if replacement_kind == "symlink" else target.samefile(protected)
    assert all(
        path.exists()
        for component, (path, _digest) in images.items()
        if component != PERSONAL_DEV_COMPONENTS[0]
    )
    assert not artifact.exists()


def test_failed_artifact_creation_does_not_unlink_replaced_output(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    contract = _contract()
    images = _image_outputs(tmp_path)
    artifact = tmp_path / "artifacts.tar"
    displaced = tmp_path / "displaced-artifacts.tar"
    real_fsync = os.fsync
    replaced = False

    def replace_output_then_fail(descriptor: int) -> None:
        nonlocal replaced
        real_fsync(descriptor)
        if replaced:
            return
        replaced = True
        artifact.rename(displaced)
        artifact.write_bytes(b"unrelated output")
        raise OSError("injected output failure")

    monkeypatch.setattr(os, "fsync", replace_output_then_fail)

    with pytest.raises(OSError, match="injected output failure"):
        create_personal_dev_build_artifact(
            contract,
            images,
            artifact,
            consume_image_archives=True,
        )

    assert artifact.read_bytes() == b"unrelated output"
    assert displaced.is_file()
    assert all(path.exists() for path, _digest in images.values())


def test_consuming_artifact_validates_every_image_before_creating_output(
    tmp_path: Path,
) -> None:
    contract = _contract()
    images = _image_outputs(tmp_path)
    images[PERSONAL_DEV_COMPONENTS[-1]][0].write_bytes(b"not an OCI archive")
    artifact = tmp_path / "artifacts.tar"

    with pytest.raises(PersonalDevBuildArtifactError):
        create_personal_dev_build_artifact(
            contract,
            images,
            artifact,
            consume_image_archives=True,
        )

    assert all(path.exists() for path, _digest in images.values())
    assert not artifact.exists()


def test_consuming_mode_preserves_canonical_artifact_bytes(tmp_path: Path) -> None:
    contract = _contract()
    retained_images = _image_outputs(tmp_path / "retained")
    consumed_images = _image_outputs(tmp_path / "consumed")
    retained_artifact = tmp_path / "retained.tar"
    consumed_artifact = tmp_path / "consumed.tar"

    create_personal_dev_build_artifact(contract, retained_images, retained_artifact)
    create_personal_dev_build_artifact(
        contract,
        consumed_images,
        consumed_artifact,
        consume_image_archives=True,
    )

    assert consumed_artifact.read_bytes() == retained_artifact.read_bytes()
    assert all(path.exists() for path, _digest in retained_images.values())
    assert all(not path.exists() for path, _digest in consumed_images.values())


def test_sandbox_contract_rejects_noncanonical_or_changed_authority() -> None:
    contract = _contract()
    noncanonical = json.dumps(dict(contract.raw), indent=2).encode("ascii")
    with pytest.raises(PersonalDevSandboxBuildError, match="canonical"):
        PersonalDevSandboxBuildContract.parse(noncanonical)

    changed = dict(contract.raw)
    changed["components"] = ["service"]
    payload = json.dumps(changed, sort_keys=True, separators=(",", ":")).encode("ascii")
    with pytest.raises(PersonalDevSandboxBuildError, match="authority"):
        PersonalDevSandboxBuildContract.parse(payload)


@pytest.mark.parametrize(
    "buildkit_address",
    [
        "unix:///var/run/loom-buildkit/buildkitd.sock",
        "tcp://buildkit-012345abcdef:1234",
    ],
)
def test_build_images_uses_only_a_fixed_transport(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    buildkit_address: str,
) -> None:
    contract = _contract()
    source = tmp_path / "source"
    for dockerfile in _DOCKERFILES.values():
        path = source / dockerfile
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("FROM scratch\n", encoding="utf-8")
    image_payload, _manifest_digest = _oci_archive(architecture="amd64")
    calls: list[tuple[list[str], dict[str, object]]] = []

    def run(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        calls.append((command, kwargs))
        output_argument = next(
            argument for argument in command if argument.startswith("--output=type=oci,dest=")
        )
        Path(output_argument.removeprefix("--output=type=oci,dest=")).write_bytes(
            image_payload
        )
        return subprocess.CompletedProcess(command, 0, "", "")

    monkeypatch.setattr(subprocess, "run", run)
    monkeypatch.setenv("BUILDKIT_HOST", "tcp://attacker.example:9999")

    images = _build_images(
        contract,
        source_directory=source,
        output_directory=tmp_path / "images",
        buildctl_path=Path("/usr/bin/buildctl"),
        buildkit_address=buildkit_address,
    )

    assert set(images) == set(PERSONAL_DEV_COMPONENTS)
    assert len(calls) == len(PERSONAL_DEV_COMPONENTS)
    forbidden = (
        "buildctl-daemonless",
        "buildkitd_flags",
        "rootlesskit",
        "xdg_runtime_dir",
    )
    for command, kwargs in calls:
        assert command[:3] == [
            "/usr/bin/buildctl",
            f"--addr={buildkit_address}",
            "build",
        ]
        environment = kwargs.get("env", {})
        assert isinstance(environment, dict)
        assert "BUILDKIT_HOST" not in environment
        assert "BUILDKITD" not in environment
        serialized = repr((command, environment)).casefold()
        assert all(value not in serialized for value in forbidden)


def test_main_keeps_uds_default_and_accepts_explicit_native_address(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, object]] = []

    def run_personal_dev_sandbox_build(**kwargs: object) -> None:
        calls.append(kwargs)

    monkeypatch.setattr(
        sandbox_builder,
        "run_personal_dev_sandbox_build",
        run_personal_dev_sandbox_build,
    )
    arguments = [
        "build",
        "--contract-file",
        str(tmp_path / "contract.json"),
        "--capability-directory",
        str(tmp_path / "capabilities"),
        "--workspace",
        str(tmp_path / "workspace"),
    ]

    assert sandbox_builder.main(arguments) == 0
    assert sandbox_builder.main(
        [
            *arguments,
            "--native-buildkit-address",
            "tcp://buildkit-012345abcdef:1234",
        ]
    ) == 0

    assert [call["buildkit_address"] for call in calls] == [
        "unix:///var/run/loom-buildkit/buildkitd.sock",
        "tcp://buildkit-012345abcdef:1234",
    ]


@pytest.mark.parametrize(
    ("buildctl_path", "buildkit_address"),
    [
        (Path("buildctl"), "unix:///var/run/loom-buildkit/buildkitd.sock"),
        (Path("/tmp/buildctl"), "unix:///var/run/loom-buildkit/buildkitd.sock"),
        (Path("/usr/bin/buildctl"), "unix:///tmp/buildkitd.sock"),
        (Path("/usr/bin/buildctl"), "tcp://127.0.0.1:1234"),
        (Path("/usr/bin/buildctl"), "tcp://[::1]:1234"),
        (Path("/usr/bin/buildctl"), "tcp://buildkit-012345abcde:1234"),
        (Path("/usr/bin/buildctl"), "tcp://buildkit-012345abcdef0:1234"),
        (Path("/usr/bin/buildctl"), "tcp://buildkit-012345abcdeF:1234"),
        (Path("/usr/bin/buildctl"), "tcp://other-012345abcdef:1234"),
        (
            Path("/usr/bin/buildctl"),
            "tcp://user@buildkit-012345abcdef:1234",
        ),
        (
            Path("/usr/bin/buildctl"),
            "tcp://buildkit-012345abcdef:1234?changed=1",
        ),
        (
            Path("/usr/bin/buildctl"),
            "tcp://buildkit-012345abcdef:1234#changed",
        ),
        (Path("/usr/bin/buildctl"), "tcp://buildkit-012345abcdef:1235"),
        (Path("/usr/bin/buildctl"), "tcp://buildkit-012345abcdef:1234/"),
    ],
)
def test_build_images_rejects_untrusted_client_endpoints(
    tmp_path: Path,
    buildctl_path: Path,
    buildkit_address: str,
) -> None:
    with pytest.raises(PersonalDevSandboxBuildError, match="buildctl"):
        _build_images(
            _contract(),
            source_directory=tmp_path / "source",
            output_directory=tmp_path / "images",
            buildctl_path=buildctl_path,
            buildkit_address=buildkit_address,
        )


@pytest.mark.parametrize(
    ("option", "address"),
    [
        ("--buildkit-address", "tcp://buildkit-012345abcdef:1234"),
        ("--native-buildkit-address", "unix:///var/run/loom-buildkit/buildkitd.sock"),
        ("--native-buildkit-address", "tcp://127.0.0.1:1234"),
        ("--native-buildkit-address", "tcp://buildkit-012345abcdeF:1234"),
        ("--native-buildkit-address", "tcp://buildkit-012345abcdef:1235"),
        (
            "--native-buildkit-address",
            "tcp://user@buildkit-012345abcdef:1234",
        ),
        (
            "--native-buildkit-address",
            "tcp://buildkit-012345abcdef:1234?changed=1",
        ),
        (
            "--native-buildkit-address",
            "tcp://buildkit-012345abcdef:1234#changed",
        ),
    ],
)
def test_main_rejects_nonexplicit_or_malformed_native_address(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    option: str,
    address: str,
) -> None:
    def unexpected_build(**kwargs: object) -> None:
        pytest.fail(f"invalid endpoint reached builder: {kwargs!r}")

    monkeypatch.setattr(
        sandbox_builder,
        "run_personal_dev_sandbox_build",
        unexpected_build,
    )

    with pytest.raises(PersonalDevSandboxBuildError, match="buildctl"):
        sandbox_builder.main(
            [
                "build",
                "--contract-file",
                str(tmp_path / "contract.json"),
                "--capability-directory",
                str(tmp_path / "capabilities"),
                "--workspace",
                str(tmp_path / "workspace"),
                option,
                address,
            ]
        )


def _client_status(**changes: str) -> bytes:
    fields = {
        "Uid": "1000\t1000\t1000\t1000",
        "Gid": "1000\t1000\t1000\t1000",
        "CapInh": "0000000000000000",
        "CapPrm": "0000000000000000",
        "CapEff": "0000000000000000",
        "CapBnd": "0000000000000000",
        "CapAmb": "0000000000000000",
        "Seccomp": "2",
    }
    fields.update(changes)
    return "".join(f"{name}:\t{value}\n" for name, value in fields.items()).encode(
        "ascii"
    )


def test_client_identity_accepts_only_restricted_gvisor_process(
    tmp_path: Path,
) -> None:
    marker = tmp_path / "kernel_is_gvisor"
    marker.write_bytes(b"1\n")
    status = tmp_path / "status"
    status.write_bytes(_client_status())

    sandbox_builder._verify_client_identity(
        gvisor_marker=marker,
        status_file=status,
        no_new_privs=1,
    )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("Uid", "1001\t1001\t1001\t1001"),
        ("Gid", "1001\t1001\t1001\t1001"),
        ("CapInh", "00000000000000c0"),
        ("CapPrm", "00000000000000c0"),
        ("CapEff", "00000000000000c0"),
        ("CapBnd", "00000000000000c0"),
        ("CapAmb", "00000000000000c0"),
        ("Seccomp", "0"),
    ],
)
def test_client_identity_rejects_each_security_drift(
    tmp_path: Path,
    field: str,
    value: str,
) -> None:
    marker = tmp_path / "kernel_is_gvisor"
    marker.write_bytes(b"1\n")
    status = tmp_path / "status"
    status.write_bytes(_client_status(**{field: value}))

    with pytest.raises(PersonalDevSandboxBuildError, match="identity"):
        sandbox_builder._verify_client_identity(
            gvisor_marker=marker,
            status_file=status,
            no_new_privs=1,
        )


def test_client_identity_rejects_no_new_privs_drift(tmp_path: Path) -> None:
    marker = tmp_path / "kernel_is_gvisor"
    marker.write_bytes(b"1\n")
    status = tmp_path / "status"
    status.write_bytes(_client_status())

    with pytest.raises(PersonalDevSandboxBuildError, match="identity"):
        sandbox_builder._verify_client_identity(
            gvisor_marker=marker,
            status_file=status,
            no_new_privs=0,
        )


@pytest.mark.parametrize("result", [-1, 2])
def test_client_identity_rejects_invalid_prctl_result(result: int) -> None:
    with pytest.raises(PersonalDevSandboxBuildError, match="identity"):
        sandbox_builder._read_no_new_privs(
            prctl=lambda option, arg2, arg3, arg4, arg5: result,
        )


def test_client_identity_reads_no_new_privs_with_exact_prctl() -> None:
    calls: list[tuple[int, int, int, int, int]] = []

    def prctl(option: int, arg2: int, arg3: int, arg4: int, arg5: int) -> int:
        calls.append((option, arg2, arg3, arg4, arg5))
        return 1

    assert sandbox_builder._read_no_new_privs(prctl=prctl) == 1
    assert calls == [(39, 0, 0, 0, 0)]


def test_client_identity_requires_gvisor_marker(tmp_path: Path) -> None:
    status = tmp_path / "status"
    status.write_bytes(_client_status())

    with pytest.raises(PersonalDevSandboxBuildError, match="identity"):
        sandbox_builder._verify_client_identity(
            gvisor_marker=tmp_path / "missing",
            status_file=status,
            no_new_privs=1,
        )


def test_client_identity_is_checked_before_authority_files(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def reject_identity() -> None:
        raise PersonalDevSandboxBuildError("builder client runtime identity is invalid")

    monkeypatch.setattr(sandbox_builder, "_verify_client_identity", reject_identity)

    with pytest.raises(PersonalDevSandboxBuildError, match="identity"):
        sandbox_builder.run_personal_dev_sandbox_build(
            contract_file=tmp_path / "missing-contract",
            capability_directory=tmp_path / "missing-capabilities",
            workspace=tmp_path / "workspace",
        )
