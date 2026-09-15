"""Fixed installed native worker composition; not fleet activation or release.

Partial and completed scratch retain an exact recovery record. Production intake
must remain disabled until allocation-contained recovery and dependency networking
are installed and accepted. Empty runtime inventory is never physical release.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import os
import stat
import sys
from collections.abc import Callable, Iterator
from contextlib import ExitStack, contextmanager
from pathlib import Path
from typing import Annotated, Literal, Self
from uuid import UUID

from pydantic import Field, model_validator

from loom_capacity_agent.admission import PhysicalJobBindingV2
from loom_capacity_agent.build_admission import BuildOutcomeReceiptV1
from loom_capacity_executor.native_allocated_worker import (
    allocated_claim_request,
    allocated_native_packet_io,
    validate_native_worker_scope,
)
from loom_capacity_executor.native_build_source import _settled_io, _write_all
from loom_capacity_executor.native_installed_release import (
    NativeInstalledReleaseObservation,
    _Observation,
    _path,
    _require_original_identity,
    verify_native_installed_release,
)
from loom_capacity_executor.native_oci_material import _open_directory
from loom_capacity_executor.native_outer_build import run_native_outer_build
from loom_capacity_executor.native_rootless_material import NativeRootlessMaterialV1
from loom_capacity_executor.native_rootless_runtime import (
    NativeRootlessSpecV2,
    read_native_rootless_spec,
)
from loom_capacity_executor.native_worker_handoff import (
    NATIVE_WORKER_HANDOFF_ENV,
    NativeWorkerHandoffV1,
    consume_native_worker_handoff,
)
from loom_capacity_manager.contracts import Digest, StrictV1Model, canonical_bytes, canonical_digest
from loom_capacity_manager.executable_contracts import canonical_executable_bytes


class NativeInstalledWorkerConfigV1(StrictV1Model):
    release_manifest: str
    release_manifest_sha256: Digest
    tooling_source_sha: Annotated[str, Field(pattern=r"^[0-9a-f]{40}$")]
    trusted_fleet_release_sha256: Digest
    platform: Literal["linux/amd64", "linux/arm64"]
    scratch_root: str
    max_source_archive_bytes: int = Field(gt=0, le=8 * 1024**3)
    max_artifact_bytes: int = Field(gt=0, le=8 * 1024**3)
    max_image_archive_bytes: int = Field(gt=0, le=8 * 1024**3)
    max_unpacked_bytes: int = Field(gt=0, le=32 * 1024**3)
    max_rootfs_entries: int = Field(gt=0, le=100000)
    tmp_bytes: int = Field(ge=1024**2, le=64 * 1024**3)
    buildkit_state_bytes: int = Field(ge=1024**2, le=64 * 1024**3)
    timeout_seconds: int = Field(gt=0, le=7200)

    @model_validator(mode="after")
    def _paths(self) -> Self:
        release, scratch = _path(self.release_manifest), _path(self.scratch_root)
        if release == scratch or scratch in release.parents or release in scratch.parents:
            raise ValueError("native installed release and scratch must be disjoint")
        if self.max_image_archive_bytes > self.max_artifact_bytes:
            raise ValueError("native installed image archive bound exceeds artifact bound")
        return self


class NativeInstalledAttemptV1(StrictV1Model):
    """Local recovery locator, never a second allocation/claim authority."""

    physical: PhysicalJobBindingV2
    worker_id: UUID
    worker_incarnation: UUID
    config_sha256: Digest
    release_manifest_sha256: Digest
    directory: str
    device: int = Field(ge=0)
    inode: int = Field(gt=0)


def read_installed_worker_config(path: Path, *, expected_sha256: str) -> NativeInstalledWorkerConfigV1:
    _require_original_identity()
    _path(str(path))
    observation = _Observation()
    wire = observation.read(path, digest=expected_sha256, size=None, mode=0o444, bound=16384, collect=True)
    config = NativeInstalledWorkerConfigV1.model_validate_json(wire)
    if canonical_bytes(config) != wire:
        raise ValueError("native installed worker config must be canonical")
    observation.finish()
    return config


def _bind_running_installation(release: NativeInstalledReleaseObservation) -> None:
    manifest = release.manifest
    if (not sys.flags.isolated or sys.executable != manifest.python
        or manifest.rootlesskit != "/usr/bin/rootlesskit"):
        raise ValueError("native installed process differs from fixed protected executables")
    root = Path(manifest.python_root)
    if not sys.path or any(_path(entry) != root and root not in _path(entry).parents for entry in sys.path):
        raise ValueError("native installed process imports outside the protected Python tree")
    if str(Path(__file__)) not in {item.path for item in manifest.files}:
        raise ValueError("native installed entrypoint is absent from the protected inventory")
    if manifest.platform != {"x86_64": "linux/amd64", "aarch64": "linux/arm64"}.get(os.uname().machine):
        raise ValueError("native installed platform differs from the actual worker")
    _verify_host_lookup_paths()


def _verify_host_lookup_paths() -> None:
    """RootlessKit's fixed PATH cannot select helpers from owner-writable roots.

    Only conventional root-owned merged-/usr aliases are accepted. This checks
    search-directory protection; helper/dependency bytes remain release inventory
    and protected-installer responsibilities, not arbitrary PATH discovery.
    """
    observation = _Observation()
    aliases: dict[Path, tuple[int, int, str]] = {}
    with ExitStack() as stack:
        for name in ("/usr/local/bin", "/usr/sbin", "/usr/bin"):
            observation.directory(Path(name), stack)
        for name, target in (("/bin", "/usr/bin"), ("/sbin", "/usr/sbin")):
            path = Path(name)
            metadata = path.lstat()
            if not stat.S_ISLNK(metadata.st_mode):
                observation.directory(path, stack)
                continue
            link = os.readlink(path)
            if metadata.st_uid != 0 or metadata.st_gid != 0 or link not in (target, target.lstrip("/")):
                raise ValueError("native fixed helper alias is not protected")
            aliases[path] = metadata.st_dev, metadata.st_ino, link
        observation.finish()
        for path, expected in aliases.items():
            metadata = path.lstat()
            if ((metadata.st_dev, metadata.st_ino, os.readlink(path)) != expected
                or metadata.st_uid != 0 or metadata.st_gid != 0):
                raise ValueError("native fixed helper alias changed during verification")


def _private(descriptor: int) -> tuple[int, int]:
    observed = os.fstat(descriptor)
    if (not stat.S_ISDIR(observed.st_mode) or (observed.st_uid, observed.st_gid) != (os.geteuid(), os.getegid())
        or stat.S_IMODE(observed.st_mode) != 0o700):
        raise ValueError("native installed scratch must be private and worker-owned")
    return observed.st_dev, observed.st_ino


def _write_private(parent: int, name: str, wire: bytes) -> None:
    descriptor = os.open(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC | os.O_NOFOLLOW, 0o600, dir_fd=parent)
    try:
        _write_all(descriptor, wire)
        os.fchmod(descriptor, 0o400)
        os.fsync(descriptor)
        written = os.fstat(descriptor)
        visible = os.stat(name, dir_fd=parent, follow_symlinks=False)
        if ((written.st_dev, written.st_ino) != (visible.st_dev, visible.st_ino)
            or written.st_nlink != 1 or written.st_size != len(wire)):
            raise ValueError("native installed private record changed during creation")
    finally:
        os.close(descriptor)
    os.fsync(parent)


@contextmanager
def _attempt(packet: NativeWorkerHandoffV1, config: NativeInstalledWorkerConfigV1,
    config_sha256: str,
) -> Iterator[tuple[Path, Callable[[], None]]]:
    root = Path(config.scratch_root)
    name = "attempt-" + str(allocated_claim_request(packet.registration).operation_id)
    path = root / name
    with ExitStack() as stack:
        parent = _open_directory(root, stack)
        parent_identity = _private(parent)
        os.mkdir(name, mode=0o700, dir_fd=parent)  # Never reuse a crashed/previous allocation attempt.
        created = os.stat(name, dir_fd=parent, follow_symlinks=False)
        descriptor = _open_directory(path, stack)
        identity = _private(descriptor)
        if identity != (created.st_dev, created.st_ino):
            raise ValueError("native installed attempt changed before preparation")
        record = NativeInstalledAttemptV1(physical=packet.physical, worker_id=packet.registration.worker_id,
            worker_incarnation=packet.registration.worker_incarnation, config_sha256=config_sha256,
            release_manifest_sha256=config.release_manifest_sha256, directory=str(path),
            device=identity[0], inode=identity[1])
        _write_private(descriptor, "recovery.json", canonical_bytes(record))
        os.fsync(parent)  # Persist recovery identity before creating mapped scratch.
        children: dict[Path, tuple[int, int]] = {}

        def unchanged() -> None:
            with ExitStack() as checking:
                if (_private(_open_directory(root, checking)) != parent_identity
                    or _private(_open_directory(path, checking)) != identity):
                    raise ValueError("native installed attempt path changed")
                for child, expected in children.items():
                    if _private(_open_directory(child, checking)) != expected:
                        raise ValueError("native installed attempt child changed")

        for child in ("source", "work", "artifacts"):
            unchanged()
            os.mkdir(child, mode=0o700, dir_fd=descriptor)
            metadata = os.stat(child, dir_fd=descriptor, follow_symlinks=False)
            child_fd = _open_directory(path / child, stack)
            if _private(child_fd) != (metadata.st_dev, metadata.st_ino):
                raise ValueError("native installed attempt child changed before open")
            children[path / child] = _private(child_fd)
        os.fsync(descriptor)
        unchanged()
        # No recursive original-UID cleanup: mapped subordinate-owned data is
        # inaccessible here. Manager-owned recovery must retain this locator.
        yield path, unchanged


async def _execute(packet: NativeWorkerHandoffV1, *, config_path: Path, config_sha256: str,
    job_id: str,
) -> BuildOutcomeReceiptV1:
    config = await _settled_io(read_installed_worker_config, config_path, expected_sha256=config_sha256)
    if (config.platform != ("linux/arm64" if packet.physical.binding.pool_id == "gb10" else "linux/amd64")
        or config.trusted_fleet_release_sha256 != packet.physical.binding.execution.trusted_fleet_release_sha256):
        raise ValueError("native installed config differs from allocated platform/release")
    validate_native_worker_scope(packet, job_id=job_id)
    release = await _settled_io(verify_native_installed_release, Path(config.release_manifest),
        expected_sha256=config.release_manifest_sha256, expected_source_sha=config.tooling_source_sha,
        expected_platform=config.platform)
    _bind_running_installation(release)
    with _attempt(packet, config, config_sha256) as (attempt, unchanged):
        async with allocated_native_packet_io(packet, job_id=job_id, workspace=attempt / "source",
            max_archive_bytes=config.max_source_archive_bytes) as owner:
            unchanged()
            archive = next(item for item in release.manifest.files if item.path == release.manifest.rootfs)
            spec = NativeRootlessSpecV2(claim=owner.claim, context=owner.source.context,
                runsc=str(Path(release.manifest.runsc_root) / "runsc"), state_root=str(attempt / "runsc"),
                bundle_root=str(attempt / "material/bundles"), workspace=str(attempt / "work"),
                max_artifact_bytes=config.max_artifact_bytes, max_image_archive_bytes=config.max_image_archive_bytes,
                material=NativeRootlessMaterialV1(archive=archive.path, archive_sha256=archive.sha256,
                    archive_size_bytes=archive.size_bytes, max_unpacked_bytes=config.max_unpacked_bytes,
                    max_entries=config.max_rootfs_entries, client_seccomp=release.client_seccomp.decode("utf-8"),
                    client_seccomp_sha256=hashlib.sha256(release.client_seccomp).hexdigest(),
                    tmp_bytes=config.tmp_bytes, buildkit_state_bytes=config.buildkit_state_bytes))
            wire = canonical_executable_bytes(spec)
            digest = hashlib.sha256(wire).hexdigest()
            spec_path = attempt / "work/runtime-spec.json"
            with ExitStack() as stack:
                work = _open_directory(spec_path.parent, stack)
                _write_private(work, spec_path.name, wire)
            unchanged()
            read_native_rootless_spec(spec_path, expected_sha256=digest)
            return await run_native_outer_build(owner, spec_path=spec_path, expected_sha256=digest,
                artifact_workspace=attempt / "artifacts", timeout_seconds=config.timeout_seconds)


async def execute_installed_native_worker(descriptor: int, *, config_path: Path, config_sha256: str,
    job_id: str,
) -> BuildOutcomeReceiptV1:
    packet = consume_native_worker_handoff(descriptor)
    return await _execute(packet, config_path=config_path, config_sha256=config_sha256, job_id=job_id)


def main() -> None:
    # Consume even when argument/config parsing subsequently fails. No credential
    # descriptor survives an error or remains inheritable during verification.
    descriptor = int(os.environ.pop(NATIVE_WORKER_HANDOFF_ENV))
    packet = consume_native_worker_handoff(descriptor)
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--config-sha256", required=True)
    args = parser.parse_args()
    result = asyncio.run(_execute(packet, config_path=args.config, config_sha256=args.config_sha256,
        job_id=os.environ.get("SLURM_JOB_ID", "")))
    print(canonical_digest(result))  # Bounded public outcome identity, not a credential or release claim.


if __name__ == "__main__":
    main()
