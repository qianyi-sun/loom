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
from loom.personal_dev_scanner_cache import (
    PersonalDevScannerCacheBinding,
    PersonalDevScannerCacheFiles,
)
from loom_service.config import LoomServiceSettings

logger = logging.getLogger(__name__)

_SCANNER_CACHE_ROOT_ENTRIES = frozenset(
    {"db", "fanal", "identity.json", "java-db"}
)
_SCANNER_CACHE_FILE_ENTRIES = {
    "db": frozenset({"metadata.json", "trivy.db"}),
    "java-db": frozenset({"metadata.json", "trivy-java.db"}),
}
_SCANNER_CACHE_IDENTITY_KEYS = frozenset(
    {
        "cache_identity_sha256",
        "database_metadata_sha256",
        "database_sha256",
        "java_database_metadata_sha256",
        "java_database_sha256",
        "scanner_binary_sha256",
        "schema_version",
    }
)
_MAX_SCANNER_DATABASE_BYTES = 4 * 1024 * 1024 * 1024
_MAX_SCANNER_METADATA_BYTES = 64 * 1024
_MAX_SCANNER_IDENTITY_BYTES = 4096
_SCANNER_CACHE_PROTECTED_UID = 65531
_SCANNER_CACHE_PROTECTED_GID = 65532
_DIRECTORY_OPEN_FLAGS = (
    os.O_RDONLY
    | getattr(os, "O_CLOEXEC", 0)
    | getattr(os, "O_NOFOLLOW", 0)
    | getattr(os, "O_DIRECTORY", 0)
)
_FILE_OPEN_FLAGS = (
    os.O_RDONLY
    | getattr(os, "O_CLOEXEC", 0)
    | getattr(os, "O_NOFOLLOW", 0)
    | getattr(os, "O_NONBLOCK", 0)
)
_STABLE_FILE_FIELDS = (
    "st_dev",
    "st_ino",
    "st_mode",
    "st_nlink",
    "st_uid",
    "st_gid",
    "st_size",
    "st_mtime_ns",
    "st_ctime_ns",
)


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


def _regular_file_sha256(
    path: Path,
    *,
    label: str,
    maximum_bytes: int | None = None,
) -> str:
    try:
        descriptor = os.open(path, _FILE_OPEN_FLAGS)
    except OSError:
        raise RuntimeError(f"personal-dev {label} is unavailable") from None
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_nlink != 1
            or (maximum_bytes is not None and before.st_size > maximum_bytes)
        ):
            raise RuntimeError(f"personal-dev {label} authority is invalid")
        digest = hashlib.sha256()
        total = 0
        while chunk := os.read(descriptor, 1024 * 1024):
            total += len(chunk)
            if maximum_bytes is not None and total > maximum_bytes:
                raise RuntimeError(f"personal-dev {label} authority is invalid")
            digest.update(chunk)
        after = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    if any(
        getattr(before, field) != getattr(after, field)
        for field in _STABLE_FILE_FIELDS
    ):
        raise RuntimeError(f"personal-dev {label} changed during verification")
    return digest.hexdigest()


def _scanner_cache_binding_error() -> RuntimeError:
    return RuntimeError("personal-dev scanner cache binding is invalid")


def _is_sha256(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and value != "0" * 64
        and all(character in "0123456789abcdef" for character in value)
    )


def _unique_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise _scanner_cache_binding_error()
        value[key] = item
    return value


def _canonical_json(value: object) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")


def _open_cache_directory(
    parent: int,
    name: str,
    *,
    protected_owner: int,
    protected_group: int,
) -> int:
    metadata = os.stat(name, dir_fd=parent, follow_symlinks=False)
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_uid != protected_owner
        or metadata.st_gid != protected_group
        or stat.S_IMODE(metadata.st_mode) != 0o555
    ):
        raise _scanner_cache_binding_error()
    descriptor = os.open(name, _DIRECTORY_OPEN_FLAGS, dir_fd=parent)
    opened = os.fstat(descriptor)
    if any(
        getattr(metadata, field) != getattr(opened, field)
        for field in _STABLE_FILE_FIELDS
    ):
        os.close(descriptor)
        raise _scanner_cache_binding_error()
    return descriptor


def _directory_entries_match(directory: int, expected: frozenset[str]) -> bool:
    observed: set[str] = set()
    with os.scandir(directory) as entries:
        for entry in entries:
            if entry.name not in expected or len(observed) >= len(expected):
                return False
            observed.add(entry.name)
    return observed == expected


def _read_cache_file(
    directory: int,
    name: str,
    *,
    protected_owner: int,
    protected_group: int,
    maximum_bytes: int,
    capture: bool,
) -> tuple[str, bytes | None]:
    metadata = os.stat(name, dir_fd=directory, follow_symlinks=False)
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_nlink != 1
        or metadata.st_uid != protected_owner
        or metadata.st_gid != protected_group
        or stat.S_IMODE(metadata.st_mode) != 0o444
        or not 0 < metadata.st_size <= maximum_bytes
    ):
        raise _scanner_cache_binding_error()
    descriptor = os.open(name, _FILE_OPEN_FLAGS, dir_fd=directory)
    try:
        opened = os.fstat(descriptor)
        if any(
            getattr(metadata, field) != getattr(opened, field)
            for field in _STABLE_FILE_FIELDS
        ):
            raise _scanner_cache_binding_error()
        digest = hashlib.sha256()
        payload = bytearray() if capture else None
        total = 0
        while total <= maximum_bytes:
            chunk = os.read(
                descriptor,
                min(1024 * 1024, maximum_bytes + 1 - total),
            )
            if not chunk:
                break
            total += len(chunk)
            if total > maximum_bytes:
                raise _scanner_cache_binding_error()
            digest.update(chunk)
            if payload is not None:
                payload.extend(chunk)
        closed = os.fstat(descriptor)
        if total != metadata.st_size or any(
            getattr(opened, field) != getattr(closed, field)
            for field in _STABLE_FILE_FIELDS
        ):
            raise _scanner_cache_binding_error()
        return digest.hexdigest(), bytes(payload) if payload is not None else None
    finally:
        os.close(descriptor)


def _installed_scanner_cache_binding(
    cache_directory: Path,
) -> PersonalDevScannerCacheBinding:
    """Revalidate one immutable cache generation from the management mount."""
    try:
        generation_name = cache_directory.name
        if not _is_sha256(generation_name):
            raise _scanner_cache_binding_error()
        metadata = cache_directory.stat(follow_symlinks=False)
        if (
            not stat.S_ISDIR(metadata.st_mode)
            or stat.S_ISLNK(metadata.st_mode)
            or metadata.st_uid != _SCANNER_CACHE_PROTECTED_UID
            or metadata.st_gid != _SCANNER_CACHE_PROTECTED_GID
            or metadata.st_uid == os.geteuid()
            or stat.S_IMODE(metadata.st_mode) != 0o555
        ):
            raise _scanner_cache_binding_error()
        root = os.open(cache_directory, _DIRECTORY_OPEN_FLAGS)
        try:
            opened = os.fstat(root)
            if (
                any(
                    getattr(metadata, field) != getattr(opened, field)
                    for field in _STABLE_FILE_FIELDS
                )
                or not _directory_entries_match(root, _SCANNER_CACHE_ROOT_ENTRIES)
            ):
                raise _scanner_cache_binding_error()
            protected_owner = _SCANNER_CACHE_PROTECTED_UID
            protected_group = _SCANNER_CACHE_PROTECTED_GID
            identity_digest, identity_payload = _read_cache_file(
                root,
                "identity.json",
                protected_owner=protected_owner,
                protected_group=protected_group,
                maximum_bytes=_MAX_SCANNER_IDENTITY_BYTES,
                capture=True,
            )
            del identity_digest
            if identity_payload is None:
                raise _scanner_cache_binding_error()
            try:
                identity = json.loads(
                    identity_payload,
                    object_pairs_hook=_unique_json_object,
                )
            except RuntimeError:
                raise
            except (TypeError, UnicodeError, ValueError):
                raise _scanner_cache_binding_error() from None
            if (
                not isinstance(identity, dict)
                or identity.keys() != _SCANNER_CACHE_IDENTITY_KEYS
                or _canonical_json(identity) != identity_payload
                or type(identity.get("schema_version")) is not int
                or identity["schema_version"] != 1
                or identity.get("cache_identity_sha256") != generation_name
                or any(
                    not _is_sha256(identity.get(key))
                    for key in _SCANNER_CACHE_IDENTITY_KEYS - {"schema_version"}
                )
            ):
                raise _scanner_cache_binding_error()

            observed: dict[str, str] = {}
            for directory_name, expected_entries in _SCANNER_CACHE_FILE_ENTRIES.items():
                child = _open_cache_directory(
                    root,
                    directory_name,
                    protected_owner=protected_owner,
                    protected_group=protected_group,
                )
                try:
                    if not _directory_entries_match(child, expected_entries):
                        raise _scanner_cache_binding_error()
                    database_name = (
                        "trivy.db" if directory_name == "db" else "trivy-java.db"
                    )
                    observed[directory_name + "-database"], _ = _read_cache_file(
                        child,
                        database_name,
                        protected_owner=protected_owner,
                        protected_group=protected_group,
                        maximum_bytes=_MAX_SCANNER_DATABASE_BYTES,
                        capture=False,
                    )
                    observed[directory_name + "-metadata"], _ = _read_cache_file(
                        child,
                        "metadata.json",
                        protected_owner=protected_owner,
                        protected_group=protected_group,
                        maximum_bytes=_MAX_SCANNER_METADATA_BYTES,
                        capture=False,
                    )
                finally:
                    os.close(child)
            fanal_metadata = os.stat("fanal", dir_fd=root, follow_symlinks=False)
            if not stat.S_ISDIR(fanal_metadata.st_mode) or stat.S_ISLNK(
                fanal_metadata.st_mode
            ):
                raise _scanner_cache_binding_error()
            after = os.fstat(root)
            if any(
                getattr(opened, field) != getattr(after, field)
                for field in _STABLE_FILE_FIELDS
            ):
                raise _scanner_cache_binding_error()
        finally:
            os.close(root)
        binding = PersonalDevScannerCacheBinding(
            cache_identity_sha256=generation_name,
            scanner_binary_sha256=identity["scanner_binary_sha256"],
            files=PersonalDevScannerCacheFiles(
                database_sha256=observed["db-database"],
                database_metadata_sha256=observed["db-metadata"],
                java_database_sha256=observed["java-db-database"],
                java_database_metadata_sha256=observed["java-db-metadata"],
            ),
        )
        if binding != PersonalDevScannerCacheBinding(
            cache_identity_sha256=identity["cache_identity_sha256"],
            scanner_binary_sha256=identity["scanner_binary_sha256"],
            files=PersonalDevScannerCacheFiles(
                database_sha256=identity["database_sha256"],
                database_metadata_sha256=identity["database_metadata_sha256"],
                java_database_sha256=identity["java_database_sha256"],
                java_database_metadata_sha256=identity[
                    "java_database_metadata_sha256"
                ],
            ),
        ):
            raise _scanner_cache_binding_error()
        return binding
    except RuntimeError:
        raise
    except (KeyError, OSError, TypeError, UnicodeError, ValueError):
        raise _scanner_cache_binding_error() from None


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
    installed_scanner = _installed_scanner_cache_binding(scanner_cache)
    try:
        scanner_binary_sha256 = _regular_file_sha256(
            settings.personal_dev_builder_scanner_path,
            label="scanner executable",
            maximum_bytes=512 * 1024 * 1024,
        )
    except RuntimeError:
        raise _scanner_cache_binding_error() from None
    configured_scanner_identity = (
        f"trivy-bin-sha256:{scanner_binary_sha256}:"
        f"db-sha256:{installed_scanner.files.database_sha256}:"
        f"java-db-sha256:{installed_scanner.files.java_database_sha256}"
    )
    if (
        installed_scanner.cache_identity_sha256
        != settings.personal_dev_builder_scanner_cache_identity_sha256
        or installed_scanner.scanner_binary_sha256 != scanner_binary_sha256
        or installed_scanner.files.database_metadata_sha256
        != settings.personal_dev_builder_scanner_database_metadata_sha256
        or installed_scanner.files.java_database_metadata_sha256
        != settings.personal_dev_builder_scanner_java_database_metadata_sha256
        or configured_scanner_identity
        != settings.personal_dev_builder_scanner_identity
    ):
        raise _scanner_cache_binding_error()
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
        scanner_identity=configured_scanner_identity,
        policy_sha256=settings.personal_dev_builder_scanner_policy_sha256,
    )
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
