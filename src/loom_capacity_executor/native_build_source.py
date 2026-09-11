"""Stage exact allocated source without exposing incomplete archives to builds."""

from __future__ import annotations

import asyncio
import hashlib
import os
import stat
import tempfile
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass
from functools import partial
from pathlib import Path
from typing import ParamSpec, Protocol, TypeVar

from loom.personal_dev_build_demand import personal_build_work_identity
from loom.personal_dev_build_platform_requests import canonical_build_source
from loom.personal_dev_builder import verify_personal_dev_build_source
from loom.personal_dev_candidate import (
    PERSONAL_DEV_BUILD_CONTRACT_SHA256,
    CandidateRegistration,
    PersonalDevPlatform,
)
from loom.personal_dev_source import PersonalDevSourceError, verify_personal_dev_source_snapshot
from loom_capacity_agent.build_admission import (
    BuildClaimExchangeV1,
    BuildClaimRequestV1,
    BuildSourceContextV1,
    BuildSourceReadReceiptV1,
)
from loom_capacity_manager.contracts import canonical_digest

_P = ParamSpec("_P")
_T = TypeVar("_T")
_CHUNK_BYTES = 1024 * 1024


class NativeBuildSourceClient(Protocol):
    """Implemented by both the pinned pool client and purpose-aware router."""

    async def read_source(self, claim: BuildClaimRequestV1, *, worker_credential: str,
        offset: int, length: int,
    ) -> BuildSourceReadReceiptV1: ...

    async def read_source_context(self, claim: BuildClaimRequestV1, *, worker_credential: str) -> BuildSourceContextV1: ...


@dataclass(frozen=True, slots=True)
class NativeStagedBuildSource:
    context: BuildSourceContextV1
    archive: Path


def _verify_context_source(context: BuildSourceContextV1, archive: Path) -> None:
    manifest = verify_personal_dev_source_snapshot(archive,
        expected_source_digest=context.source_sha256, expected_archive_sha256=context.archive_sha256)
    if manifest.source_commit != context.source_commit or manifest.dirty is not context.dirty:
        raise PersonalDevSourceError("native context source manifest binding changed")


async def _settled_io(function: Callable[_P, _T], *args: _P.args, **kwargs: _P.kwargs) -> _T:
    """Do not remove a workspace while cancelled filesystem work still uses it."""
    task = asyncio.create_task(asyncio.to_thread(function, *args, **kwargs))
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        while not task.done():
            with suppress(asyncio.CancelledError, Exception):
                await asyncio.shield(task)
        with suppress(Exception):
            task.result()
        raise


def _write_all(descriptor: int, data: bytes) -> None:
    remaining = memoryview(data)
    while remaining:
        written = os.write(descriptor, remaining)
        if written <= 0:
            raise OSError("native source file write made no progress")
        remaining = remaining[written:]


class NativeClaimBuildSource:
    """Consume protected registration or authenticated context, never feature metadata.

    The yielded path lives only within this context and references an open private
    workspace descriptor. Pass its contents into the sandbox; never extract on
    the host or treat successful staging as a fresh execution/lease permit.
    """

    def __init__(self, *, client: NativeBuildSourceClient, workspace: Path, max_archive_bytes: int) -> None:
        if type(max_archive_bytes) is not int or max_archive_bytes <= 0:
            raise ValueError("native source archive limit must be positive")
        if not workspace.is_absolute():
            raise ValueError("native source workspace must be absolute")
        self._client = client
        self._workspace = workspace
        self._max_archive_bytes = max_archive_bytes

    @asynccontextmanager
    async def __call__(self, registration: CandidateRegistration, *, claim: BuildClaimRequestV1,
        worker_credential: str, platform: PersonalDevPlatform,
    ) -> AsyncIterator[Path]:
        envelope = BuildClaimExchangeV1.model_validate_json(BuildClaimExchangeV1(
            claim=claim, worker_credential=worker_credential).model_dump_json())
        claim = envelope.claim
        candidate = registration.candidate
        if (claim.request_id != personal_build_work_identity(registration, platform)[1]
            or claim.binding.pool_id != ("gb10" if platform == "linux/arm64" else "oldlab")
            or type(candidate.archive_size_bytes) is not int
            or not 1 <= candidate.archive_size_bytes <= self._max_archive_bytes):
            raise ValueError("native source launch binding or size is invalid")
        source_digest = hashlib.sha256(canonical_build_source(registration)).hexdigest()
        async with self._stage(claim, worker_credential=envelope.worker_credential,
            archive_size=candidate.archive_size_bytes, archive_sha256=candidate.archive_sha256,
            source_digest=source_digest, verify=partial(verify_personal_dev_build_source, candidate)) as archive:
            yield archive

    @asynccontextmanager
    async def stage_claim(self, claim: BuildClaimRequestV1, *, worker_credential: str) -> AsyncIterator[NativeStagedBuildSource]:
        """Fetch current metadata and sealed source without application records."""
        envelope = BuildClaimExchangeV1.model_validate_json(BuildClaimExchangeV1(
            claim=claim, worker_credential=worker_credential).model_dump_json())
        context = await self._client.read_source_context(envelope.claim, worker_credential=envelope.worker_credential)
        context = BuildSourceContextV1.model_validate_json(context.model_dump_json())
        if (context.claim_digest != canonical_digest(envelope.claim) or context.request_id != envelope.claim.request_id
            or envelope.claim.binding.pool_id != ("gb10" if context.platform == "linux/arm64" else "oldlab")
            or context.build_contract_sha256 != PERSONAL_DEV_BUILD_CONTRACT_SHA256
            or context.archive_size_bytes > self._max_archive_bytes):
            raise ValueError("native context launch binding or size is invalid")
        async with self._stage(envelope.claim, worker_credential=envelope.worker_credential,
            archive_size=context.archive_size_bytes, archive_sha256=context.archive_sha256,
            source_digest=context.source_binding_sha256, verify=partial(_verify_context_source, context)) as archive:
            yield NativeStagedBuildSource(context=context, archive=archive)

    @asynccontextmanager
    async def _stage(self, claim: BuildClaimRequestV1, *, worker_credential: str,
        archive_size: int, archive_sha256: str, source_digest: str, verify: Callable[[Path], None],
    ) -> AsyncIterator[Path]:
        workspace = os.open(self._workspace, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW)
        try:
            metadata = os.fstat(workspace)
            if metadata.st_uid != os.geteuid() or stat.S_IMODE(metadata.st_mode) != 0o700:
                raise ValueError("native source workspace must be private and owner-controlled")
            # Anchor to the opened directory even if its original path is renamed.
            with tempfile.TemporaryDirectory(prefix="native-source-", dir=f"/proc/self/fd/{workspace}") as directory:
                archive = Path(directory) / "source.tar"
                descriptor = os.open(archive, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_CLOEXEC | os.O_NOFOLLOW, 0o600)
                try:
                    observed = 0
                    digest = hashlib.sha256()
                    while observed < archive_size:
                        count = min(_CHUNK_BYTES, archive_size - observed)
                        receipt = await self._client.read_source(claim, worker_credential=worker_credential,
                            offset=observed, length=count)
                        if (receipt.source_binding_sha256 != source_digest
                            or receipt.archive_sha256 != archive_sha256
                            or receipt.archive_size_bytes != archive_size
                            or receipt.claim_digest != canonical_digest(claim)
                            or receipt.offset != observed or len(receipt.data) != count):
                            raise ValueError("native source reply differs from protected launch")
                        data = receipt.data
                        await _settled_io(_write_all, descriptor, data)
                        observed += len(data)
                        digest.update(data)
                    if observed != archive_size or digest.hexdigest() != archive_sha256:
                        raise ValueError("native source complete archive digest changed")
                    await _settled_io(os.fsync, descriptor)
                finally:
                    os.close(descriptor)
                await _settled_io(verify, archive)
                yield archive
        finally:
            os.close(workspace)
