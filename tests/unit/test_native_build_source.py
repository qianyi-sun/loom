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
@pytest.mark.parametrize("boundary", ["exact", "source-binding", "hash", "size", "corrupt", "cancel", "manifest", "request", "pool", "limit"])
async def test_native_source_stages_verified_archive_and_cleans_all_paths(sealed_source, pool, boundary):
    module = import_module("loom_capacity_executor.native_build_source")
    registration, archive, workspace = sealed_source
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
                assert boundary == "exact", "unverified source reached the sandbox boundary"
                assert path.read_bytes() == archive
                assert path.stat().st_mode & 0o777 == 0o600
                assert path.parent.parent == workspace
            assert not path.exists()

        if boundary == "exact":
            await consume()
        elif boundary == "cancel":
            with pytest.raises(asyncio.CancelledError):
                await consume()
        else:
            with pytest.raises((ValueError, RuntimeError)):
                await consume()
    assert list(workspace.iterdir()) == []
    if boundary in {"request", "pool", "limit"}:
        assert calls == []
    elif boundary in {"exact", "source-binding", "corrupt", "cancel", "manifest"}:
        assert len(calls) == 2
