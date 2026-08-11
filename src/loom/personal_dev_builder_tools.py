"""Bounded trusted scanner and registry tools for personal candidate export."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import stat
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Protocol

from loom.personal_dev_builder_artifact import VerifiedPersonalDevImageArtifact
from loom.personal_dev_builder_exporter import PersonalDevImageScanResult
from loom.personal_dev_candidate import (
    PERSONAL_DEV_COMPONENTS,
    PERSONAL_DEV_PLATFORMS,
    CandidateRegistration,
)
from loom.personal_dev_candidate_gc import (
    PersonalDevArtifactGcManifest,
    personal_dev_registry_tag,
    validate_personal_dev_registry_prefix,
)

_OCI_DIGEST_RE = re.compile(r"sha256:[0-9a-f]{64}")
_REGISTRY_REPOSITORY_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/-]{0,511}")
_SCANNER_IDENTITY_RE = re.compile(r"[\x21-\x7e]{1,256}")
_MAX_TOOL_STDERR_BYTES = 256 * 1024


class BoundedCommandRunner(Protocol):
    async def run(
        self,
        argv: list[str],
        *,
        timeout_seconds: float,
        max_output_bytes: int,
    ) -> bytes: ...


class ExternalToolError(RuntimeError):
    """A trusted external tool failed without exposing its raw diagnostics."""

    def __init__(self, message: str, *, registry_object_absent: bool = False) -> None:
        super().__init__(message)
        self.registry_object_absent = registry_object_absent


async def _read_bounded(
    stream: asyncio.StreamReader,
    *,
    max_bytes: int,
) -> bytes:
    chunks: list[bytes] = []
    observed = 0
    while chunk := await stream.read(64 * 1024):
        observed += len(chunk)
        if observed > max_bytes:
            raise ExternalToolError("trusted external tool output exceeded its limit")
        chunks.append(chunk)
    return b"".join(chunks)


@dataclass(slots=True)
class AsyncBoundedCommandRunner:
    """Run fixed argv while bounding both output streams and wall time."""

    environment: Mapping[str, str] | None = None

    def __post_init__(self) -> None:
        if self.environment is None:
            return
        environment = dict(self.environment)
        if any(
            not isinstance(key, str)
            or not isinstance(value, str)
            or not key
            or "=" in key
            or "\0" in key + value
            for key, value in environment.items()
        ):
            raise ValueError("trusted external tool environment is invalid")
        self.environment = MappingProxyType(environment)

    async def run(
        self,
        argv: list[str],
        *,
        timeout_seconds: float,
        max_output_bytes: int,
    ) -> bytes:
        if (
            not argv
            or any(not argument or "\0" in argument for argument in argv)
            or timeout_seconds <= 0
            or max_output_bytes <= 0
        ):
            raise ValueError("trusted external tool invocation is invalid")
        try:
            process = await asyncio.create_subprocess_exec(
                *argv,
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=self.environment,
            )
        except OSError:
            raise ExternalToolError("trusted external tool could not start") from None
        assert process.stdout is not None
        assert process.stderr is not None
        stdout_task = asyncio.create_task(
            _read_bounded(process.stdout, max_bytes=max_output_bytes)
        )
        stderr_task = asyncio.create_task(
            _read_bounded(process.stderr, max_bytes=_MAX_TOOL_STDERR_BYTES)
        )
        try:
            stdout, stderr, return_code = await asyncio.wait_for(
                asyncio.gather(stdout_task, stderr_task, process.wait()),
                timeout=timeout_seconds,
            )
        except asyncio.CancelledError:
            if process.returncode is None:
                process.kill()
                await process.wait()
            await asyncio.gather(stdout_task, stderr_task, return_exceptions=True)
            raise
        except ExternalToolError:
            if process.returncode is None:
                process.kill()
                await process.wait()
            await asyncio.gather(stdout_task, stderr_task, return_exceptions=True)
            raise
        except TimeoutError:
            if process.returncode is None:
                process.kill()
                await process.wait()
            await asyncio.gather(stdout_task, stderr_task, return_exceptions=True)
            raise ExternalToolError("trusted external tool timed out") from None
        if return_code != 0:
            raise ExternalToolError(
                f"trusted external tool failed with exit code {return_code}",
                registry_object_absent=any(
                    marker in stderr.lower()
                    for marker in (
                        b"manifest unknown",
                        b"name unknown",
                        b"status code: 404",
                        b"status 404",
                    )
                ),
            )
        return stdout


def _executable(value: str, *, label: str) -> str:
    path = Path(value)
    if not path.is_absolute() or "\0" in value:
        raise ValueError(f"personal-dev {label} executable must be an absolute path")
    return value


def _running_attempt(registration: CandidateRegistration) -> None:
    attempt = registration.build_attempt
    if (
        attempt is None
        or attempt.candidate_id != registration.candidate.id
        or attempt.state != "running"
        or attempt.lease_epoch <= 0
        or registration.candidate.status != "building"
    ):
        raise ValueError("personal-dev trusted tool registration is not a running attempt")


def _verify_image_file_binding(image: VerifiedPersonalDevImageArtifact) -> None:
    try:
        descriptor = os.open(
            image.archive_path,
            os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW,
        )
    except OSError:
        raise ExternalToolError("personal-dev verified image archive is unavailable") from None
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_nlink != 1
            or before.st_size != image.archive_size_bytes
        ):
            raise ExternalToolError("personal-dev verified image archive authority changed")
        digest = hashlib.sha256()
        while chunk := os.read(descriptor, 1024 * 1024):
            digest.update(chunk)
        after = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    if (
        digest.hexdigest() != image.archive_sha256
        or (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
        != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
    ):
        raise ExternalToolError("personal-dev verified image archive binding changed")


@dataclass(slots=True)
class TrivyPersonalDevImageScanner:
    """Scan a verified OCI archive offline with a preloaded immutable DB."""

    runner: BoundedCommandRunner
    executable: str
    cache_directory: Path
    scanner_identity: str
    policy_sha256: str
    timeout_seconds: float = 900.0
    max_report_bytes: int = 16 * 1024 * 1024

    def __post_init__(self) -> None:
        self.executable = _executable(self.executable, label="scanner")
        if not self.cache_directory.is_absolute():
            raise ValueError("personal-dev scanner cache directory must be absolute")
        if _SCANNER_IDENTITY_RE.fullmatch(self.scanner_identity) is None:
            raise ValueError("personal-dev scanner identity is invalid")
        if re.fullmatch(r"[0-9a-f]{64}", self.policy_sha256) is None:
            raise ValueError("personal-dev scanner policy digest is invalid")
        if self.timeout_seconds <= 0 or self.max_report_bytes <= 0:
            raise ValueError("personal-dev scanner limits are invalid")

    async def scan(
        self,
        image: VerifiedPersonalDevImageArtifact,
        *,
        registration: CandidateRegistration,
    ) -> PersonalDevImageScanResult:
        _running_attempt(registration)
        if image.component not in PERSONAL_DEV_COMPONENTS or image.platform not in (
            PERSONAL_DEV_PLATFORMS
        ):
            raise ValueError("personal-dev scanner image binding is invalid")
        await asyncio.to_thread(_verify_image_file_binding, image)
        report = await self.runner.run(
            [
                self.executable,
                "image",
                "--input",
                str(image.archive_path),
                "--format",
                "json",
                "--scanners",
                "vuln,secret",
                "--severity",
                "HIGH,CRITICAL",
                "--exit-code",
                "1",
                "--no-progress",
                "--offline-scan",
                "--skip-db-update",
                "--skip-java-db-update",
                "--cache-dir",
                str(self.cache_directory),
            ],
            timeout_seconds=self.timeout_seconds,
            max_output_bytes=self.max_report_bytes,
        )
        await asyncio.to_thread(_verify_image_file_binding, image)
        try:
            value = json.loads(report)
            json.dumps(value, allow_nan=False)
        except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError) as exc:
            raise ExternalToolError("personal-dev scanner report is invalid") from exc
        results = value.get("Results") if isinstance(value, dict) else None
        if (
            not isinstance(value, dict)
            or value.get("ArtifactType") != "container_image"
            or not isinstance(results, list)
            or any(not isinstance(result, dict) for result in results)
        ):
            raise ExternalToolError("personal-dev scanner report shape is invalid")
        finding_fields = ("Vulnerabilities", "Secrets", "Misconfigurations", "Licenses")
        if any(
            result.get(field) is not None
            and (
                not isinstance(result[field], list)
                or bool(result[field])
            )
            for result in results
            for field in finding_fields
        ):
            raise ExternalToolError("personal-dev image scan reported a denied finding")
        return PersonalDevImageScanResult(
            report=report,
            evidence={
                "policy_sha256": self.policy_sha256,
                "report_sha256": hashlib.sha256(report).hexdigest(),
                "result": "clean",
                "scanner_identity": self.scanner_identity,
            },
        )


def _repository(value: str) -> str:
    if (
        _REGISTRY_REPOSITORY_RE.fullmatch(value) is None
        or value.endswith(":")
        or value.endswith("/")
        or "://" in value
        or "@" in value
    ):
        raise ValueError("personal-dev registry repository is invalid")
    return value


def _attempt_tag(registration: CandidateRegistration, *, suffix: str) -> str:
    attempt = registration.build_attempt
    assert attempt is not None
    return personal_dev_registry_tag(
        registration.candidate,
        attempt,
        lease_epoch=attempt.lease_epoch,
        suffix=suffix,
    )


@dataclass(slots=True)
class SkopeoBuildxPersonalDevRegistryPublisher:
    """Import verified OCI archives and join exact native digests."""

    runner: BoundedCommandRunner
    skopeo_executable: str
    docker_executable: str
    registry_auth_file: Path
    copy_timeout_seconds: float = 1800.0
    registry_timeout_seconds: float = 300.0

    def __post_init__(self) -> None:
        self.skopeo_executable = _executable(self.skopeo_executable, label="skopeo")
        self.docker_executable = _executable(self.docker_executable, label="docker")
        if (
            not self.registry_auth_file.is_absolute()
            or self.registry_auth_file.name != "config.json"
        ):
            raise ValueError("personal-dev registry authentication file is invalid")
        if self.copy_timeout_seconds <= 0 or self.registry_timeout_seconds <= 0:
            raise ValueError("personal-dev registry publisher timeouts are invalid")

    async def publish_platform(
        self,
        image: VerifiedPersonalDevImageArtifact,
        *,
        registration: CandidateRegistration,
        repository: str,
    ) -> str:
        _running_attempt(registration)
        repository = _repository(repository)
        if _OCI_DIGEST_RE.fullmatch(image.manifest_digest) is None:
            raise ValueError("personal-dev platform manifest digest is invalid")
        await asyncio.to_thread(_verify_image_file_binding, image)
        architecture = image.platform.rsplit("/", 1)[1]
        target = f"{repository}:{_attempt_tag(registration, suffix=architecture)}"
        await self.runner.run(
            [
                self.skopeo_executable,
                "copy",
                "--authfile",
                str(self.registry_auth_file),
                "--preserve-digests",
                f"oci-archive:{image.archive_path}",
                f"docker://{target}",
            ],
            timeout_seconds=self.copy_timeout_seconds,
            max_output_bytes=1024 * 1024,
        )
        raw = await self.runner.run(
            [
                self.skopeo_executable,
                "inspect",
                "--authfile",
                str(self.registry_auth_file),
                "--raw",
                f"docker://{repository}@{image.manifest_digest}",
            ],
            timeout_seconds=self.registry_timeout_seconds,
            max_output_bytes=4 * 1024 * 1024,
        )
        observed = "sha256:" + hashlib.sha256(raw).hexdigest()
        if observed != image.manifest_digest:
            raise ExternalToolError("personal-dev registry platform digest verification failed")
        await asyncio.to_thread(_verify_image_file_binding, image)
        return observed

    async def publish_index(
        self,
        *,
        registration: CandidateRegistration,
        repository: str,
        platform_digests: Mapping[str, str],
    ) -> tuple[str, str]:
        _running_attempt(registration)
        repository = _repository(repository)
        digests = dict(platform_digests)
        if set(digests) != set(PERSONAL_DEV_PLATFORMS) or any(
            _OCI_DIGEST_RE.fullmatch(digest) is None for digest in digests.values()
        ):
            raise ValueError("personal-dev registry platform digest set is invalid")
        target = f"{repository}:{_attempt_tag(registration, suffix='index')}"
        await self.runner.run(
            [
                self.docker_executable,
                "buildx",
                "imagetools",
                "create",
                "--tag",
                target,
                *(f"{repository}@{digests[platform]}" for platform in PERSONAL_DEV_PLATFORMS),
            ],
            timeout_seconds=self.registry_timeout_seconds,
            max_output_bytes=1024 * 1024,
        )
        raw = await self.runner.run(
            [
                self.docker_executable,
                "buildx",
                "imagetools",
                "inspect",
                "--raw",
                target,
            ],
            timeout_seconds=self.registry_timeout_seconds,
            max_output_bytes=4 * 1024 * 1024,
        )
        try:
            index = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ExternalToolError("personal-dev registry index is invalid") from exc
        manifests = index.get("manifests") if isinstance(index, dict) else None
        if not isinstance(manifests, list) or len(manifests) != len(PERSONAL_DEV_PLATFORMS):
            raise ExternalToolError("personal-dev registry index platform set is invalid")
        observed: dict[str, str] = {}
        for descriptor in manifests:
            if not isinstance(descriptor, dict) or not isinstance(
                descriptor.get("platform"), dict
            ):
                raise ExternalToolError("personal-dev registry index descriptor is invalid")
            platform_value = descriptor["platform"]
            platform = f"{platform_value.get('os')}/{platform_value.get('architecture')}"
            digest = descriptor.get("digest")
            if platform in observed or not isinstance(digest, str):
                raise ExternalToolError("personal-dev registry index descriptor is invalid")
            observed[platform] = digest
        if observed != digests:
            raise ExternalToolError("personal-dev registry index digest set changed")
        index_digest = "sha256:" + hashlib.sha256(raw).hexdigest()
        return f"{repository}@{index_digest}", index_digest


@dataclass(slots=True)
class SkopeoPersonalDevRegistryArtifactCollector:
    """Delete only attempt-isolated registry tags from one sealed manifest."""

    runner: BoundedCommandRunner
    skopeo_executable: str
    registry_auth_file: Path
    expected_registry_prefix: str
    timeout_seconds: float = 300.0

    def __post_init__(self) -> None:
        self.skopeo_executable = _executable(self.skopeo_executable, label="skopeo")
        if (
            not self.registry_auth_file.is_absolute()
            or self.registry_auth_file.name != "config.json"
        ):
            raise ValueError("personal-dev registry authentication file is invalid")
        try:
            validate_personal_dev_registry_prefix(self.expected_registry_prefix)
        except ValueError as exc:
            raise ValueError("personal-dev registry cleanup prefix is invalid") from exc
        if self.timeout_seconds <= 0:
            raise ValueError("personal-dev registry cleanup timeout is invalid")

    async def collect(self, manifest: PersonalDevArtifactGcManifest) -> None:
        manifest.validate()
        for reference in manifest.registry_tags:
            if not reference.startswith(f"{self.expected_registry_prefix}/"):
                raise RuntimeError("personal-dev registry cleanup prefix changed")
            try:
                await self.runner.run(
                    [
                        self.skopeo_executable,
                        "delete",
                        "--authfile",
                        str(self.registry_auth_file),
                        f"docker://{reference}",
                    ],
                    timeout_seconds=self.timeout_seconds,
                    max_output_bytes=1024 * 1024,
                )
            except ExternalToolError as exc:
                if not exc.registry_object_absent:
                    raise


__all__ = [
    "AsyncBoundedCommandRunner",
    "BoundedCommandRunner",
    "ExternalToolError",
    "SkopeoBuildxPersonalDevRegistryPublisher",
    "SkopeoPersonalDevRegistryArtifactCollector",
    "TrivyPersonalDevImageScanner",
]
