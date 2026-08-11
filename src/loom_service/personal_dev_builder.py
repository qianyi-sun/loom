"""Service-owned background loop for lease-fenced personal candidate builds."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import stat
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from loom.dev_instance_runtime import KubectlClient
from loom.personal_dev_builder import (
    PersonalDevBuildCoordinator,
    PersonalDevBuildExecutor,
    PersonalDevBuildSource,
    S3PersonalDevBuildSource,
)
from loom.personal_dev_builder_exporter import (
    S3TrustedPersonalDevBuildPublicationExporter,
)
from loom.personal_dev_builder_manifest import PersonalDevBuilderManifestConfig
from loom.personal_dev_builder_runtime import (
    KubectlPersonalDevBuildExecutor,
    S3PersonalDevBuildCapabilityProvider,
)
from loom.personal_dev_builder_tools import (
    AsyncBoundedCommandRunner,
    SkopeoBuildxPersonalDevRegistryPublisher,
    TrivyPersonalDevImageScanner,
)
from loom.personal_dev_candidate import PersonalDevCandidateLimits
from loom.personal_dev_candidate_store import SqlAlchemyPersonalDevCandidateStore
from loom_service.config import LoomServiceSettings

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class PersonalDevBuilderRuntime:
    source: S3PersonalDevBuildSource
    executor: KubectlPersonalDevBuildExecutor
    manifest_config: PersonalDevBuilderManifestConfig
    capabilities: S3PersonalDevBuildCapabilityProvider
    exporter: S3TrustedPersonalDevBuildPublicationExporter


def _required_executable(path: Path, *, label: str) -> str:
    try:
        metadata = path.stat(follow_symlinks=False)
    except OSError:
        raise RuntimeError(f"personal-dev builder {label} executable is unavailable") from None
    if (
        not stat.S_ISREG(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or not os.access(path, os.X_OK)
    ):
        raise RuntimeError(f"personal-dev builder {label} executable is unavailable")
    return str(path)


def _regular_file_sha256(path: Path, *, label: str) -> str:
    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW)
    except OSError:
        raise RuntimeError(f"personal-dev {label} is unavailable") from None
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1:
            raise RuntimeError(f"personal-dev {label} authority is invalid")
        digest = hashlib.sha256()
        while chunk := os.read(descriptor, 1024 * 1024):
            digest.update(chunk)
        after = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) != (
        after.st_dev,
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
    ):
        raise RuntimeError(f"personal-dev {label} changed during verification")
    return digest.hexdigest()


def _installed_scanner_identity(executable: Path, cache_directory: Path) -> str:
    binary = _regular_file_sha256(executable, label="scanner executable")
    database = _regular_file_sha256(
        cache_directory / "db" / "trivy.db",
        label="scanner vulnerability database",
    )
    java_database = _regular_file_sha256(
        cache_directory / "java-db" / "trivy-java.db",
        label="scanner Java vulnerability database",
    )
    return (
        f"trivy-bin-sha256:{binary}:db-sha256:{database}:"
        f"java-db-sha256:{java_database}"
    )


def _required_registry_auth_file(path: Path) -> Path:
    if path.name != "config.json":
        raise RuntimeError("personal-dev registry auth file must be named config.json")
    try:
        metadata = path.stat(follow_symlinks=False)
    except OSError:
        raise RuntimeError("personal-dev registry auth file is unavailable") from None
    if (
        not stat.S_ISREG(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_nlink != 1
        or metadata.st_mode & 0o007
    ):
        raise RuntimeError("personal-dev registry auth file authority is invalid")
    _regular_file_sha256(path, label="registry auth file")
    return path


def _protocol_versions(raw: str) -> dict[str, str]:
    try:
        value = json.loads(raw)
        canonical = json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        )
    except (json.JSONDecodeError, TypeError, ValueError) as exc:
        raise RuntimeError("personal-dev protocol versions must be canonical JSON") from exc
    if (
        canonical != raw
        or not isinstance(value, dict)
        or not value
        or any(not isinstance(key, str) or not isinstance(item, str) for key, item in value.items())
    ):
        raise RuntimeError("personal-dev protocol versions must be a non-empty string map")
    return {str(key): str(item) for key, item in value.items()}


def build_personal_dev_builder_runtime(
    settings: LoomServiceSettings,
    *,
    minio_client: Any,
) -> PersonalDevBuilderRuntime | None:
    """Build the inert-by-default restricted build and publication authority."""
    if not settings.personal_dev_builder_enabled:
        return None
    if not settings.personal_dev_trusted_launcher_profile_sha256:
        raise RuntimeError("personal-dev trusted launcher profile digest is required")
    scanner_cache = settings.personal_dev_builder_scanner_cache_dir
    try:
        cache_metadata = scanner_cache.stat(follow_symlinks=False)
    except OSError:
        raise RuntimeError("personal-dev scanner cache directory is unavailable") from None
    if not stat.S_ISDIR(cache_metadata.st_mode) or stat.S_ISLNK(cache_metadata.st_mode):
        raise RuntimeError("personal-dev scanner cache directory is unavailable")
    manifest_config = PersonalDevBuilderManifestConfig(
        builder_image=settings.personal_dev_builder_image,
        max_artifact_bytes=settings.personal_dev_builder_max_artifact_bytes,
        max_image_archive_bytes=(settings.personal_dev_builder_max_image_archive_bytes),
        runtime_class_name=settings.personal_dev_builder_runtime_class_name,
    )
    if settings.personal_dev_builder_lease_sec <= (
        manifest_config.active_deadline_seconds + 60
    ):
        raise RuntimeError("personal-dev builder lease must outlive the sandbox deadline")
    scanner_runner = AsyncBoundedCommandRunner(
        environment={"HOME": "/tmp", "TMPDIR": "/tmp"},
    )
    scanner = TrivyPersonalDevImageScanner(
        runner=scanner_runner,
        executable=_required_executable(
            settings.personal_dev_builder_scanner_path,
            label="scanner",
        ),
        cache_directory=scanner_cache,
        scanner_identity=_installed_scanner_identity(
            settings.personal_dev_builder_scanner_path,
            scanner_cache,
        ),
        policy_sha256=settings.personal_dev_builder_scanner_policy_sha256,
    )
    if scanner.scanner_identity != settings.personal_dev_builder_scanner_identity:
        raise RuntimeError("personal-dev installed scanner identity does not match configuration")
    registry_auth_file = _required_registry_auth_file(
        settings.personal_dev_builder_registry_auth_file
    )
    publisher_runner = AsyncBoundedCommandRunner(
        environment={
            "DOCKER_CONFIG": str(registry_auth_file.parent),
            "HOME": "/tmp",
            "PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
            "TMPDIR": "/tmp",
        },
    )
    publisher = SkopeoBuildxPersonalDevRegistryPublisher(
        runner=publisher_runner,
        skopeo_executable=_required_executable(
            settings.personal_dev_builder_skopeo_path,
            label="skopeo",
        ),
        docker_executable=_required_executable(
            settings.personal_dev_builder_docker_path,
            label="docker",
        ),
        registry_auth_file=registry_auth_file,
    )
    capabilities = S3PersonalDevBuildCapabilityProvider(
        object_store=minio_client,
        expected_bucket=settings.artifacts_bucket,
        expiry_seconds=settings.personal_dev_builder_lease_sec,
        max_artifact_bytes=manifest_config.max_artifact_bytes,
    )
    exporter = S3TrustedPersonalDevBuildPublicationExporter(
        object_store=minio_client,
        expected_bucket=settings.artifacts_bucket,
        max_artifact_bytes=manifest_config.max_artifact_bytes,
        max_image_archive_bytes=manifest_config.max_image_archive_bytes,
        scanner=scanner,
        publisher=publisher,
        registry_prefix=settings.personal_dev_builder_registry_prefix,
        publisher_identity=settings.personal_dev_builder_publisher_identity,
        trusted_launcher_profile_sha256=(
            settings.personal_dev_trusted_launcher_profile_sha256
        ),
        protocol_versions=_protocol_versions(settings.personal_dev_protocol_versions_json),
    )
    kubectl = KubectlClient(
        _required_executable(settings.dev_instance_kubectl_path, label="kubectl"),
        context=settings.dev_instance_kube_context,
        field_manager="loom-personal-dev-builder",
        job_wait_timeout_seconds=manifest_config.active_deadline_seconds + 60,
    )
    executor = KubectlPersonalDevBuildExecutor(
        cluster=kubectl,
        capabilities=capabilities,
        exporter=exporter,
        manifest_config=manifest_config,
    )
    return PersonalDevBuilderRuntime(
        source=S3PersonalDevBuildSource(
            object_store=minio_client,
            expected_bucket=settings.artifacts_bucket,
            max_archive_bytes=settings.personal_dev_source_max_archive_bytes,
        ),
        executor=executor,
        manifest_config=manifest_config,
        capabilities=capabilities,
        exporter=exporter,
    )


class SessionPersonalDevBuildAuthority:
    """Give every build lease transition a short independent DB session."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        limits: PersonalDevCandidateLimits,
    ) -> None:
        self._session_factory = session_factory
        self._limits = limits

    async def _call(self, method: str, **kwargs: Any) -> Any:
        async with self._session_factory() as session:
            store = SqlAlchemyPersonalDevCandidateStore(session, limits=self._limits)
            return await getattr(store, method)(**kwargs)

    async def claim_next_build(self, **kwargs: Any) -> Any:
        return await self._call("claim_next_build", **kwargs)

    async def start_build(self, **kwargs: Any) -> Any:
        return await self._call("start_build", **kwargs)

    async def heartbeat_build(self, **kwargs: Any) -> Any:
        return await self._call("heartbeat_build", **kwargs)

    async def finish_build(self, **kwargs: Any) -> Any:
        return await self._call("finish_build", **kwargs)


async def personal_dev_builder_run_loop(
    *,
    session_factory: async_sessionmaker[AsyncSession],
    source: PersonalDevBuildSource,
    executor: PersonalDevBuildExecutor,
    limits: PersonalDevCandidateLimits,
    builder_id: str,
    lease_seconds: int,
    registry_prefix: str,
    poll_interval_seconds: float,
) -> None:
    if poll_interval_seconds <= 0:
        raise ValueError("personal-dev builder poll interval must be positive")
    coordinator = PersonalDevBuildCoordinator(
        authority=SessionPersonalDevBuildAuthority(session_factory, limits=limits),
        source=source,
        executor=executor,
        builder_id=builder_id,
        lease_seconds=lease_seconds,
        registry_prefix=registry_prefix,
    )
    while True:
        try:
            progressed = await coordinator.build_once(now=datetime.now(UTC))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error(
                "personal_dev_builder_iteration_failed",
                extra={"error_type": type(exc).__name__},
            )
            progressed = False
        if not progressed:
            await asyncio.sleep(poll_interval_seconds)


__all__ = [
    "PersonalDevBuilderRuntime",
    "SessionPersonalDevBuildAuthority",
    "build_personal_dev_builder_runtime",
    "personal_dev_builder_run_loop",
]
