from __future__ import annotations

import json
import subprocess
from pathlib import Path

import pytest

from loom import personal_dev_sandbox_builder as sandbox_builder
from loom.personal_dev_builder_artifact import verify_personal_dev_build_artifact
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


def test_sandbox_contract_and_output_round_trip_through_trusted_verifier(
    tmp_path: Path,
) -> None:
    contract = _contract()
    image_payload, manifest_digest = _oci_archive(architecture="amd64")
    images: dict[str, tuple[Path, str]] = {}
    for component in PERSONAL_DEV_COMPONENTS:
        path = tmp_path / f"{component}.oci.tar"
        path.write_bytes(image_payload)
        images[component] = (path, manifest_digest)
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


def test_build_images_uses_only_the_fixed_sidecar_socket(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
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

    images = _build_images(
        contract,
        source_directory=source,
        output_directory=tmp_path / "images",
        buildctl_path=Path("/usr/bin/buildctl"),
        buildkit_address="unix:///var/run/loom-buildkit/buildkitd.sock",
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
            "--addr=unix:///var/run/loom-buildkit/buildkitd.sock",
            "build",
        ]
        environment = kwargs.get("env", {})
        assert isinstance(environment, dict)
        assert "BUILDKITD" not in environment
        serialized = repr((command, environment)).casefold()
        assert all(value not in serialized for value in forbidden)


@pytest.mark.parametrize(
    ("buildctl_path", "buildkit_address"),
    [
        (Path("buildctl"), "unix:///var/run/loom-buildkit/buildkitd.sock"),
        (Path("/tmp/buildctl"), "unix:///var/run/loom-buildkit/buildkitd.sock"),
        (Path("/usr/bin/buildctl"), "unix:///tmp/buildkitd.sock"),
        (Path("/usr/bin/buildctl"), "tcp://127.0.0.1:1234"),
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
