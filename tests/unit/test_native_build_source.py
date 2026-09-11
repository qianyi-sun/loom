"""The allocated consumer exposes only complete, verified personal source."""

import asyncio
import base64
import hashlib
import json
import subprocess
from dataclasses import asdict, replace
from datetime import UTC, datetime, timedelta
from importlib import import_module
from uuid import uuid4

import httpx
import pytest

from loom.personal_dev_build_demand import personal_build_work_identity
from loom.personal_dev_build_platform_requests import canonical_build_source
from loom.personal_dev_candidate import CandidateRegistration
from loom.personal_dev_source import create_personal_dev_source_snapshot
from loom_capacity_agent.build_admission import BuildClaimRequestV1
from loom_capacity_manager.contracts import canonical_digest
from tests.unit.test_capacity_build_admission_client import client_for, native_registration
from tests.unit.test_personal_dev_builder import _attempt, _candidate


@pytest.fixture
def sealed_source(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "pyproject.toml").write_text("[project]\nname='demo'\nversion='0.1'\n")
    subprocess.run(["git", "init", "-q", str(repo)], check=True)
    subprocess.run(["git", "-C", str(repo), "add", "pyproject.toml"], check=True)
    subprocess.run(["git", "-C", str(repo), "-c", "user.name=Test", "-c", "user.email=test@example.com",
        "commit", "-qm", "source"], check=True)
    # Untracked feature content, large enough to require multiple HTTP reads.
    (repo / "feature.txt").write_text("feature\n" * 140000)
    archive = tmp_path / "sealed.tar"
    snapshot = create_personal_dev_source_snapshot(repo, archive)
    candidate = _candidate(source_sha256=snapshot.source_digest, archive_sha256=snapshot.archive_sha256,
        source_commit=snapshot.manifest.source_commit, dirty=snapshot.manifest.dirty,
        manifest_json=asdict(snapshot.manifest), archive_size_bytes=archive.stat().st_size)
    candidate = replace(candidate, object_key=f"personal-dev/sources/{candidate.owner_team_id}/{candidate.owner_user_id}/"
        f"{candidate.candidate_sha}/{candidate.source_generation_id}/{candidate.archive_sha256}.tar")
    attempt = _attempt(state="running", lease_expires_at=datetime.now(UTC) + timedelta(minutes=5))
    workspace = tmp_path / "allocation"
    workspace.mkdir(mode=0o700)
    return CandidateRegistration(candidate=candidate, build_attempt=attempt, created=False), archive.read_bytes(), workspace


@pytest.mark.parametrize("pool", ["gb10", "oldlab"])
@pytest.mark.parametrize("boundary", ["exact", "renamed", "source-binding", "hash", "size", "corrupt", "cancel", "manifest", "request", "pool", "limit"])
async def test_native_source_stages_verified_archive_and_cleans_all_paths(sealed_source, pool, boundary):
    module = import_module("loom_capacity_executor.native_build_source")
    registration, archive, workspace = sealed_source
    retired = workspace.with_name("retired-allocation")
    platform = "linux/arm64" if pool == "gb10" else "linux/amd64"
    worker = native_registration(pool)
    claim = BuildClaimRequestV1(binding=worker.binding, operation_id=uuid4(),
        request_id=personal_build_work_identity(registration, platform)[1], worker_id=worker.worker_id,
        worker_incarnation=worker.worker_incarnation)
    if boundary == "request":
        claim = claim.model_copy(update={"request_id": uuid4()})
    if boundary == "pool":
        platform = "linux/amd64" if pool == "gb10" else "linux/arm64"
    if boundary == "manifest":
        registration = replace(registration, candidate=replace(registration.candidate, dirty=not registration.candidate.dirty))
    calls = []

    async def handle(outgoing):
        request = json.loads(outgoing.content)
        calls.append(request)
        offset, length = request["offset"], request["length"]
        if boundary == "renamed" and offset == 0:
            workspace.rename(retired)
            workspace.mkdir(mode=0o700)
            (workspace / "foreign-data").write_text("preserve")
        if boundary == "cancel" and offset:
            raise asyncio.CancelledError
        data = archive[offset:offset + length]
        if boundary == "corrupt" and offset:
            data = b"!" + data[1:]
        payload = {"schema_version": 1, "claim_digest": canonical_digest(claim),
            "source_binding_sha256": hashlib.sha256(canonical_build_source(registration)).hexdigest(),
            "archive_sha256": registration.candidate.archive_sha256, "archive_size_bytes": len(archive),
            "offset": offset, "data_base64": base64.b64encode(data).decode("ascii")}
        if boundary == "source-binding" and offset:
            payload["source_binding_sha256"] = "f" * 64
        elif boundary == "hash":
            payload["archive_sha256"] = "f" * 64
        elif boundary == "size":
            payload["archive_size_bytes"] += 1
        return httpx.Response(200, content=json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("ascii"))

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        source = module.NativeClaimBuildSource(client=client_for(http, claim), workspace=workspace,
            max_archive_bytes=1 if boundary == "limit" else 2 * 1024 * 1024)

        async def consume():
            async with source(registration, claim=claim, worker_credential="x" * 43, platform=platform) as path:
                assert boundary in {"exact", "renamed"}, "unverified source reached the sandbox boundary"
                assert path.read_bytes() == archive
                assert path.stat().st_mode & 0o777 == 0o600
                assert path.parent.parent.resolve() == (retired if boundary == "renamed" else workspace)
            assert not path.exists()

        if boundary in {"exact", "renamed"}:
            await consume()
        elif boundary == "cancel":
            with pytest.raises(asyncio.CancelledError):
                await consume()
        else:
            with pytest.raises((ValueError, RuntimeError)):
                await consume()
    if boundary == "renamed":
        assert list(retired.iterdir()) == []
        assert (workspace / "foreign-data").read_text() == "preserve"
    else:
        assert list(workspace.iterdir()) == []
    if boundary in {"request", "pool", "limit"}:
        assert calls == []
    elif boundary in {"exact", "renamed", "source-binding", "corrupt", "cancel", "manifest"}:
        assert len(calls) == 2


@pytest.mark.parametrize("boundary", ["symlink", "world-readable"])
async def test_source_workspace_fails_closed_before_network(sealed_source, monkeypatch, boundary):
    from unittest.mock import AsyncMock

    module = import_module("loom_capacity_executor.native_build_source")
    registration, _archive, workspace = sealed_source
    if boundary == "symlink":
        alias = workspace.with_name("alias")
        alias.symlink_to(workspace, target_is_directory=True)
        workspace = alias
    else:
        workspace.chmod(0o755)
    worker = native_registration()
    claim = BuildClaimRequestV1(binding=worker.binding, operation_id=uuid4(),
        request_id=personal_build_work_identity(registration, "linux/arm64")[1], worker_id=worker.worker_id,
        worker_incarnation=worker.worker_incarnation)
    async with httpx.AsyncClient() as http:
        client = client_for(http, claim)
        read = AsyncMock(side_effect=AssertionError("unsafe workspace reached network"))
        monkeypatch.setattr(client, "read_source", read)
        source = module.NativeClaimBuildSource(client=client, workspace=workspace, max_archive_bytes=2 * 1024 * 1024)
        with pytest.raises((OSError, ValueError)):
            async with source(registration, claim=claim, worker_credential="x" * 43, platform="linux/arm64"):
                pytest.fail("unsafe workspace accepted")
        read.assert_not_awaited()
    assert list(workspace.iterdir()) == []


@pytest.mark.parametrize("thread_failure", [False, True])
async def test_cancelled_verification_finishes_before_removing_source(sealed_source, monkeypatch, thread_failure):
    from threading import Event
    from unittest.mock import AsyncMock

    from loom_capacity_agent.build_admission import BuildSourceReadReceiptV1

    module = import_module("loom_capacity_executor.native_build_source")
    registration, archive, workspace = sealed_source
    worker = native_registration()
    claim = BuildClaimRequestV1(binding=worker.binding, operation_id=uuid4(),
        request_id=personal_build_work_identity(registration, "linux/arm64")[1], worker_id=worker.worker_id,
        worker_incarnation=worker.worker_incarnation)
    started, finish = Event(), Event()
    paths = []

    def verify(candidate, path):
        paths.append(path)
        started.set()
        assert finish.wait(5)
        assert path.read_bytes() == archive
        if thread_failure:
            raise RuntimeError("verification failed after cancellation")

    async def read_source(claim, *, offset, length, **kwargs):
        return BuildSourceReadReceiptV1(claim_digest=canonical_digest(claim),
            source_binding_sha256=hashlib.sha256(canonical_build_source(registration)).hexdigest(),
            archive_sha256=registration.candidate.archive_sha256, archive_size_bytes=len(archive), offset=offset,
            data_base64=base64.b64encode(archive[offset:offset+length]).decode("ascii"))

    monkeypatch.setattr(module, "verify_personal_dev_build_source", verify)
    async with httpx.AsyncClient() as http:
        client = client_for(http, claim)
        monkeypatch.setattr(client, "read_source", AsyncMock(side_effect=read_source))
        source = module.NativeClaimBuildSource(client=client, workspace=workspace, max_archive_bytes=2 * 1024 * 1024)

        async def consume():
            async with source(registration, claim=claim, worker_credential="x" * 43, platform="linux/arm64"):
                pytest.fail("cancelled source must never reach consumer")

        task = asyncio.create_task(consume())
        try:
            assert await asyncio.to_thread(started.wait, 5)
            task.cancel()
            # Once cancellation has been delivered, the pending verifier still
            # owns its file. A second cancellation cannot trigger early cleanup.
            await asyncio.sleep(0)
            assert paths[0].is_file() and not task.done()
            task.cancel()
            await asyncio.sleep(0)
            assert paths[0].is_file() and not task.done()
        finally:
            finish.set()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert list(workspace.iterdir()) == []
