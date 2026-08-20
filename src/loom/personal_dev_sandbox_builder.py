"""Credential-minimal entrypoint for one native personal-candidate build job."""

from __future__ import annotations

import argparse
import hashlib
import http.client
import io
import json
import os
import re
import secrets
import ssl
import stat
import subprocess
import tarfile
import urllib.request
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from urllib.parse import urlsplit

from loom.personal_dev_builder_artifact import (
    verify_personal_dev_oci_image_archive,
)
from loom.personal_dev_candidate import PERSONAL_DEV_COMPONENTS, PERSONAL_DEV_PLATFORMS
from loom.personal_dev_source import verify_personal_dev_source_snapshot

_DIGEST_RE = re.compile(r"[0-9a-f]{64}")
_GIT_SHA_RE = re.compile(r"[0-9a-f]{40}")
_DOCKERFILES = {
    "agent-sandbox": "deploy/Dockerfile.agent-sandbox",
    "control-plane": "deploy/Dockerfile.control-plane",
    "egress-xds": "deploy/Dockerfile.egress-xds",
    "family-orchestrator": "deploy/Dockerfile.family-orchestrator",
    "llm-gateway": "deploy/Dockerfile.gateway",
    "llm-gateway-sandbox": "deploy/Dockerfile.gateway-sandbox",
    "pipeline-orchestrator": "deploy/Dockerfile.pipeline-orchestrator",
    "service": "deploy/Dockerfile.service",
    "web": "deploy/Dockerfile.web",
    "worker": "deploy/Dockerfile.worker",
}
_BUILDCTL_PATH = Path("/usr/bin/buildctl")
_BUILDKIT_ADDRESS = "unix:///var/run/loom-buildkit/buildkitd.sock"
_CLIENT_GVISOR_MARKER = Path("/proc/gvisor/kernel_is_gvisor")
_CLIENT_STATUS_FILE = Path("/proc/self/status")


class PersonalDevSandboxBuildError(RuntimeError):
    """The bounded sandbox could not produce its exact output contract."""


@dataclass(frozen=True, slots=True)
class PersonalDevSandboxBuildContract:
    archive_sha256: str
    archive_size_bytes: int
    attempt_id: str
    build_contract_sha256: str
    candidate_sha: str
    lease_epoch: int
    max_artifact_bytes: int
    max_image_archive_bytes: int
    platform: str
    source_commit: str
    source_sha256: str
    raw: Mapping[str, object]

    @classmethod
    def parse(cls, payload: bytes) -> PersonalDevSandboxBuildContract:
        try:
            value = json.loads(payload)
            canonical = json.dumps(
                value,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
                allow_nan=False,
            ).encode("ascii")
        except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError) as exc:
            raise PersonalDevSandboxBuildError("builder contract is not canonical JSON") from exc
        expected_fields = {
            "archive_sha256",
            "archive_size_bytes",
            "attempt_id",
            "attempt_sequence",
            "build_contract_sha256",
            "candidate_id",
            "candidate_sha",
            "components",
            "lease_epoch",
            "max_artifact_bytes",
            "max_image_archive_bytes",
            "operation_epoch",
            "operation_id",
            "platform",
            "schema_version",
            "scope",
            "source_commit",
            "source_sha256",
            "subject_id",
            "subject_incarnation",
        }
        if canonical != payload:
            raise PersonalDevSandboxBuildError("builder contract is not canonical JSON")
        if not isinstance(value, dict) or set(value) != expected_fields:
            raise PersonalDevSandboxBuildError("builder contract fields are invalid")
        if (
            value["schema_version"] != 1
            or value["scope"] != "personal-dev-only"
            or value["components"] != list(PERSONAL_DEV_COMPONENTS)
            or value["platform"] not in PERSONAL_DEV_PLATFORMS
        ):
            raise PersonalDevSandboxBuildError("builder contract authority is invalid")
        for field in (
            "archive_sha256",
            "build_contract_sha256",
            "candidate_sha",
            "source_sha256",
        ):
            if not isinstance(value[field], str) or _DIGEST_RE.fullmatch(value[field]) is None:
                raise PersonalDevSandboxBuildError(f"builder contract {field} is invalid")
        if not isinstance(value["source_commit"], str) or _GIT_SHA_RE.fullmatch(
            value["source_commit"]
        ) is None:
            raise PersonalDevSandboxBuildError("builder contract source commit is invalid")
        integer_fields = (
            "archive_size_bytes",
            "lease_epoch",
            "max_artifact_bytes",
            "max_image_archive_bytes",
            "operation_epoch",
        )
        if any(type(value[field]) is not int or value[field] <= 0 for field in integer_fields):
            raise PersonalDevSandboxBuildError("builder contract integer binding is invalid")
        if type(value["attempt_sequence"]) is not int or value["attempt_sequence"] < 0:
            raise PersonalDevSandboxBuildError("builder contract attempt sequence is invalid")
        if (
            value["archive_size_bytes"] > value["max_artifact_bytes"]
            or value["max_image_archive_bytes"] > value["max_artifact_bytes"]
        ):
            raise PersonalDevSandboxBuildError("builder contract byte envelope is invalid")
        for field in (
            "attempt_id",
            "candidate_id",
            "operation_id",
            "subject_id",
            "subject_incarnation",
        ):
            raw_identifier = value[field]
            if (
                not isinstance(raw_identifier, str)
                or re.fullmatch(
                    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
                    raw_identifier,
                )
                is None
            ):
                raise PersonalDevSandboxBuildError(
                    f"builder contract {field} is invalid"
                )
        return cls(
            archive_sha256=str(value["archive_sha256"]),
            archive_size_bytes=int(value["archive_size_bytes"]),
            attempt_id=str(value["attempt_id"]),
            build_contract_sha256=str(value["build_contract_sha256"]),
            candidate_sha=str(value["candidate_sha"]),
            lease_epoch=int(value["lease_epoch"]),
            max_artifact_bytes=int(value["max_artifact_bytes"]),
            max_image_archive_bytes=int(value["max_image_archive_bytes"]),
            platform=str(value["platform"]),
            source_commit=str(value["source_commit"]),
            source_sha256=str(value["source_sha256"]),
            raw=MappingProxyType(value),
        )


def _read_file(path: Path, *, max_bytes: int) -> bytes:
    descriptor = os.open(path, os.O_RDONLY | os.O_CLOEXEC)
    try:
        value = bytearray()
        while chunk := os.read(descriptor, min(64 * 1024, max_bytes + 1 - len(value))):
            value.extend(chunk)
            if len(value) > max_bytes:
                raise PersonalDevSandboxBuildError("builder input file exceeds its limit")
        return bytes(value)
    finally:
        os.close(descriptor)


def _read_identity_file(path: Path) -> bytes:
    try:
        descriptor = os.open(
            path,
            os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW,
        )
        try:
            if not stat.S_ISREG(os.fstat(descriptor).st_mode):
                raise PersonalDevSandboxBuildError(
                    "builder client runtime identity is invalid"
                )
            value = bytearray()
            while chunk := os.read(descriptor, 64 * 1024 + 1 - len(value)):
                value.extend(chunk)
                if len(value) > 64 * 1024:
                    raise PersonalDevSandboxBuildError(
                        "builder client runtime identity is invalid"
                    )
            return bytes(value)
        finally:
            os.close(descriptor)
    except PersonalDevSandboxBuildError:
        raise
    except OSError as exc:
        raise PersonalDevSandboxBuildError(
            "builder client runtime identity is invalid"
        ) from exc


def _verify_client_identity(
    *,
    gvisor_marker: Path = _CLIENT_GVISOR_MARKER,
    status_file: Path = _CLIENT_STATUS_FILE,
) -> None:
    _read_identity_file(gvisor_marker)
    try:
        status_payload = _read_identity_file(status_file).decode("ascii")
    except UnicodeDecodeError as exc:
        raise PersonalDevSandboxBuildError(
            "builder client runtime identity is invalid"
        ) from exc
    required = {
        "Uid",
        "Gid",
        "CapInh",
        "CapPrm",
        "CapEff",
        "CapBnd",
        "CapAmb",
        "NoNewPrivs",
        "Seccomp",
    }
    observed: dict[str, str] = {}
    for line in status_payload.splitlines():
        name, separator, value = line.partition(":")
        if not separator or name not in required:
            continue
        if name in observed:
            raise PersonalDevSandboxBuildError(
                "builder client runtime identity is invalid"
            )
        observed[name] = value.strip()
    if (
        set(observed) != required
        or observed["Uid"].split() != ["1000"] * 4
        or observed["Gid"].split() != ["1000"] * 4
        or observed["CapInh"] != "0000000000000000"
        or observed["CapPrm"] != "0000000000000000"
        or observed["CapEff"] != "0000000000000000"
        or observed["CapBnd"] != "0000000000000000"
        or observed["CapAmb"] != "0000000000000000"
        or observed["NoNewPrivs"] != "1"
        or observed["Seccomp"] != "2"
    ):
        raise PersonalDevSandboxBuildError(
            "builder client runtime identity is invalid"
        )


def _download_source(url: str, destination: Path, contract: PersonalDevSandboxBuildContract) -> None:
    parsed = urlsplit(url)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.netloc
        or parsed.username is not None
        or parsed.password is not None
        or parsed.fragment
    ):
        raise PersonalDevSandboxBuildError("source capability URL is invalid")
    request = urllib.request.Request(url, method="GET")
    digest = hashlib.sha256()
    observed = 0
    descriptor = os.open(
        destination,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC | os.O_NOFOLLOW,
        0o600,
    )
    try:
        with urllib.request.urlopen(request, timeout=120) as response:
            while chunk := response.read(1024 * 1024):
                observed += len(chunk)
                if observed > contract.archive_size_bytes:
                    raise PersonalDevSandboxBuildError("source download exceeded its binding")
                digest.update(chunk)
                view = memoryview(chunk)
                while view:
                    written = os.write(descriptor, view)
                    view = view[written:]
        os.fsync(descriptor)
    except Exception as exc:
        if isinstance(exc, PersonalDevSandboxBuildError):
            raise
        raise PersonalDevSandboxBuildError("source download failed") from exc
    finally:
        os.close(descriptor)
    if (
        observed != contract.archive_size_bytes
        or digest.hexdigest() != contract.archive_sha256
    ):
        raise PersonalDevSandboxBuildError("source download digest is invalid")


def _extract_verified_source(
    archive_path: Path,
    source_directory: Path,
    contract: PersonalDevSandboxBuildContract,
) -> None:
    manifest = verify_personal_dev_source_snapshot(
        archive_path,
        expected_source_digest=contract.source_sha256,
        expected_archive_sha256=contract.archive_sha256,
    )
    if manifest.source_commit != contract.source_commit:
        raise PersonalDevSandboxBuildError("source manifest commit binding is invalid")
    source_directory.mkdir(mode=0o700)
    with tarfile.open(archive_path, mode="r:") as archive:
        first = archive.next()
        if first is None or first.name != "SOURCE-MANIFEST.json":
            raise PersonalDevSandboxBuildError("source manifest is unavailable")
        for expected in manifest.files:
            member = archive.next()
            if member is None or member.name != expected.path or not member.isreg():
                raise PersonalDevSandboxBuildError("source member binding changed")
            destination = source_directory.joinpath(*expected.path.split("/"))
            destination.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            stream = archive.extractfile(member)
            if stream is None:
                raise PersonalDevSandboxBuildError("source member is unavailable")
            descriptor = os.open(
                destination,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC | os.O_NOFOLLOW,
                expected.mode,
            )
            observed = 0
            digest = hashlib.sha256()
            try:
                while chunk := stream.read(1024 * 1024):
                    observed += len(chunk)
                    digest.update(chunk)
                    view = memoryview(chunk)
                    while view:
                        written = os.write(descriptor, view)
                        view = view[written:]
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
            if observed != expected.size or digest.hexdigest() != expected.sha256:
                raise PersonalDevSandboxBuildError("source member digest changed")


def _build_images(
    contract: PersonalDevSandboxBuildContract,
    *,
    source_directory: Path,
    output_directory: Path,
    buildctl_path: Path,
    buildkit_address: str,
) -> dict[str, tuple[Path, str]]:
    if buildctl_path != _BUILDCTL_PATH or buildkit_address != _BUILDKIT_ADDRESS:
        raise PersonalDevSandboxBuildError("buildctl endpoint is invalid")
    output_directory.mkdir(mode=0o700)
    outputs: dict[str, tuple[Path, str]] = {}
    total = 0
    environment = {
        "HOME": str(source_directory.parent / "home"),
        "PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
        "TMPDIR": str(source_directory.parent / "tmp"),
    }
    for directory in (Path(environment["HOME"]), Path(environment["TMPDIR"])):
        directory.mkdir(mode=0o700, exist_ok=True)
    for component in PERSONAL_DEV_COMPONENTS:
        dockerfile = source_directory / _DOCKERFILES[component]
        if not dockerfile.is_file() or dockerfile.is_symlink():
            raise PersonalDevSandboxBuildError(
                f"required Dockerfile is unavailable for {component}"
            )
        output = output_directory / f"{component}.oci.tar"
        try:
            result = subprocess.run(
                [
                    str(buildctl_path),
                    f"--addr={buildkit_address}",
                    "build",
                    "--frontend=dockerfile.v0",
                    f"--local=context={source_directory}",
                    f"--local=dockerfile={source_directory}",
                    f"--opt=filename={_DOCKERFILES[component]}",
                    f"--opt=platform={contract.platform}",
                    f"--opt=build-arg:LOOM_BUILD_SHA={contract.source_commit}",
                    (
                        "--opt=label:org.opencontainers.image.revision="
                        f"{contract.source_commit}"
                    ),
                    f"--output=type=oci,dest={output}",
                ],
                check=False,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                env=environment,
                timeout=3600,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise PersonalDevSandboxBuildError(f"native build failed for {component}") from exc
        if result.returncode != 0:
            raise PersonalDevSandboxBuildError(f"native build failed for {component}")
        size = output.stat(follow_symlinks=False).st_size
        total += size
        if size > contract.max_image_archive_bytes or total > contract.max_artifact_bytes:
            raise PersonalDevSandboxBuildError("native image output exceeds its byte envelope")
        manifest_digest = verify_personal_dev_oci_image_archive(
            output,
            platform=contract.platform,  # type: ignore[arg-type]
            max_bytes=contract.max_image_archive_bytes,
        )
        outputs[component] = (output, manifest_digest)
    return outputs


def _hash_regular_file(path: Path, *, max_bytes: int) -> tuple[str, int]:
    descriptor = os.open(path, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW)
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_nlink != 1
            or not 0 < before.st_size <= max_bytes
        ):
            raise PersonalDevSandboxBuildError("native image archive authority is invalid")
        digest = hashlib.sha256()
        observed = 0
        while chunk := os.read(descriptor, 1024 * 1024):
            observed += len(chunk)
            digest.update(chunk)
        after = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    if (
        observed != before.st_size
        or (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
        != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
    ):
        raise PersonalDevSandboxBuildError("native image archive binding changed")
    return digest.hexdigest(), observed


def create_personal_dev_build_artifact(
    contract: PersonalDevSandboxBuildContract,
    images: Mapping[str, tuple[Path, str]],
    output_path: Path,
) -> None:
    """Create the canonical outer bundle consumed by the trusted exporter."""
    if set(images) != set(PERSONAL_DEV_COMPONENTS):
        raise PersonalDevSandboxBuildError("native image output set is incomplete")
    components: dict[str, object] = {}
    total = 0
    for component in PERSONAL_DEV_COMPONENTS:
        path, expected_manifest_digest = images[component]
        manifest_digest = verify_personal_dev_oci_image_archive(
            path,
            platform=contract.platform,  # type: ignore[arg-type]
            max_bytes=contract.max_image_archive_bytes,
            expected_manifest_digest=expected_manifest_digest,
        )
        digest, size = _hash_regular_file(
            path,
            max_bytes=contract.max_image_archive_bytes,
        )
        total += size
        if total > contract.max_artifact_bytes:
            raise PersonalDevSandboxBuildError("native image output set is oversized")
        components[component] = {
            "archive_path": f"images/{component}.oci.tar",
            "archive_sha256": digest,
            "archive_size_bytes": size,
            "manifest_digest": manifest_digest,
        }
    manifest = {
        "schema_version": 1,
        "attestation_scope": "personal-dev-only",
        "candidate_sha": contract.candidate_sha,
        "source_sha256": contract.source_sha256,
        "archive_sha256": contract.archive_sha256,
        "build_contract_sha256": contract.build_contract_sha256,
        "attempt_id": contract.attempt_id,
        "lease_epoch": contract.lease_epoch,
        "platform": contract.platform,
        "components": components,
    }
    manifest_payload = json.dumps(
        manifest,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    descriptor = os.open(
        output_path,
        os.O_RDWR | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC | os.O_NOFOLLOW,
        0o600,
    )
    try:
        with os.fdopen(os.dup(descriptor), "w+b") as raw, tarfile.open(
            fileobj=raw,
            mode="w",
            format=tarfile.USTAR_FORMAT,
        ) as artifact:
            manifest_info = tarfile.TarInfo("manifest.json")
            manifest_info.size = len(manifest_payload)
            manifest_info.mode = 0o644
            artifact.addfile(manifest_info, io.BytesIO(manifest_payload))
            for component in PERSONAL_DEV_COMPONENTS:
                image_path, _manifest_digest = images[component]
                info = tarfile.TarInfo(f"images/{component}.oci.tar")
                _digest, image_size = _hash_regular_file(
                    image_path,
                    max_bytes=contract.max_image_archive_bytes,
                )
                info.size = image_size
                info.mode = 0o644
                image_descriptor = os.open(
                    image_path,
                    os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW,
                )
                with os.fdopen(image_descriptor, "rb") as image:
                    artifact.addfile(info, image)
                after_digest, after_size = _hash_regular_file(
                    image_path,
                    max_bytes=contract.max_image_archive_bytes,
                )
                if after_digest != _digest or after_size != image_size:
                    raise PersonalDevSandboxBuildError(
                        "native image archive changed during artifact creation"
                    )
            raw.flush()
            os.fsync(raw.fileno())
        metadata = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    if metadata.st_size > contract.max_artifact_bytes:
        output_path.unlink(missing_ok=True)
        raise PersonalDevSandboxBuildError("build artifact exceeds its byte envelope")


def _upload_artifact(
    upload: Mapping[str, object],
    artifact: Path,
    *,
    expected_max_bytes: int,
) -> None:
    url = upload.get("url")
    fields = upload.get("fields")
    max_bytes = upload.get("max_bytes")
    if (
        not isinstance(url, str)
        or not isinstance(fields, dict)
        or any(
            not isinstance(key, str)
            or re.fullmatch(r"[A-Za-z0-9_.-]{1,256}", key) is None
            or not isinstance(value, str)
            or len(value) > 8192
            or any(character in value for character in "\r\n\0")
            for key, value in fields.items()
        )
        or max_bytes != expected_max_bytes
        or artifact.stat(follow_symlinks=False).st_size > expected_max_bytes
    ):
        raise PersonalDevSandboxBuildError("artifact upload capability is invalid")
    parsed = urlsplit(url)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.fragment
    ):
        raise PersonalDevSandboxBuildError("artifact upload URL is invalid")
    boundary = "loom-personal-dev-" + secrets.token_hex(32)
    parts: list[bytes] = []
    for name, value in sorted(fields.items()):
        parts.append(
            (
                f"--{boundary}\r\n"
                f'Content-Disposition: form-data; name="{name}"\r\n\r\n'
                f"{value}\r\n"
            ).encode()
        )
    file_header = (
        f"--{boundary}\r\n"
        'Content-Disposition: form-data; name="file"; filename="artifacts.tar"\r\n'
        f"Content-Type: application/vnd.loom.personal-dev-build.v1+tar\r\n\r\n"
    ).encode("ascii")
    trailer = f"\r\n--{boundary}--\r\n".encode("ascii")
    content_length = (
        sum(len(part) for part in parts)
        + len(file_header)
        + artifact.stat(follow_symlinks=False).st_size
        + len(trailer)
    )
    connection: http.client.HTTPConnection
    if parsed.scheme == "https":
        connection = http.client.HTTPSConnection(
            parsed.hostname,
            parsed.port,
            timeout=300,
            context=ssl.create_default_context(),
        )
    else:
        connection = http.client.HTTPConnection(parsed.hostname, parsed.port, timeout=300)
    target = parsed.path or "/"
    if parsed.query:
        target += "?" + parsed.query
    try:
        connection.putrequest("POST", target)
        connection.putheader("Content-Type", f"multipart/form-data; boundary={boundary}")
        connection.putheader("Content-Length", str(content_length))
        connection.endheaders()
        for part in parts:
            connection.send(part)
        connection.send(file_header)
        artifact_descriptor = os.open(
            artifact,
            os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW,
        )
        with os.fdopen(artifact_descriptor, "rb") as stream:
            while chunk := stream.read(1024 * 1024):
                connection.send(chunk)
        connection.send(trailer)
        response = connection.getresponse()
        response.read(64 * 1024)
        if not 200 <= response.status < 300:
            raise PersonalDevSandboxBuildError("artifact upload was rejected")
    except (OSError, http.client.HTTPException) as exc:
        raise PersonalDevSandboxBuildError("artifact upload failed") from exc
    finally:
        connection.close()


def run_personal_dev_sandbox_build(
    *,
    contract_file: Path,
    capability_directory: Path,
    workspace: Path,
    buildctl_path: Path = _BUILDCTL_PATH,
    buildkit_address: str = _BUILDKIT_ADDRESS,
) -> None:
    _verify_client_identity()
    contract = PersonalDevSandboxBuildContract.parse(
        _read_file(contract_file, max_bytes=1024 * 1024)
    )
    source_url = _read_file(
        capability_directory / "source-get-url",
        max_bytes=8192,
    ).decode("utf-8")
    try:
        upload = json.loads(
            _read_file(
                capability_directory / "artifact-upload.json",
                max_bytes=64 * 1024,
            )
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PersonalDevSandboxBuildError("artifact upload capability is invalid") from exc
    if not isinstance(upload, dict):
        raise PersonalDevSandboxBuildError("artifact upload capability is invalid")
    workspace.mkdir(mode=0o700, exist_ok=True)
    source_archive = workspace / "source.tar"
    source_directory = workspace / "source"
    images_directory = workspace / "images"
    artifact = workspace / "artifacts.tar"
    _download_source(source_url, source_archive, contract)
    _extract_verified_source(source_archive, source_directory, contract)
    images = _build_images(
        contract,
        source_directory=source_directory,
        output_directory=images_directory,
        buildctl_path=buildctl_path,
        buildkit_address=buildkit_address,
    )
    create_personal_dev_build_artifact(contract, images, artifact)
    _upload_artifact(upload, artifact, expected_max_bytes=contract.max_artifact_bytes)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    build = subparsers.add_parser("build")
    build.add_argument("--contract-file", type=Path, required=True)
    build.add_argument("--capability-directory", type=Path, required=True)
    build.add_argument("--workspace", type=Path, required=True)
    build.add_argument(
        "--buildctl-path",
        type=Path,
        default=_BUILDCTL_PATH,
    )
    build.add_argument("--buildkit-address", default=_BUILDKIT_ADDRESS)
    args = parser.parse_args(argv)
    run_personal_dev_sandbox_build(
        contract_file=args.contract_file,
        capability_directory=args.capability_directory,
        workspace=args.workspace,
        buildctl_path=args.buildctl_path,
        buildkit_address=args.buildkit_address,
    )
    return 0


if __name__ == "__main__":  # pragma: no cover - exercised in the builder image
    try:
        raise SystemExit(main())
    except PersonalDevSandboxBuildError as exc:
        raise SystemExit(f"personal-dev build failed: {exc}") from None


__all__ = [
    "PersonalDevSandboxBuildContract",
    "PersonalDevSandboxBuildError",
    "create_personal_dev_build_artifact",
    "main",
    "run_personal_dev_sandbox_build",
]
