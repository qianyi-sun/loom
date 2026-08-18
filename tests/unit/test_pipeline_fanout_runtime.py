from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import pytest

from loom.pipeline.keys import canonical_document, digest_bytes
from loom.pipeline.spec import FanoutManifestV1
from loom.trajectory.storage import FakeObjectStore
from loom_pipeline_orchestrator.fanout_runtime import (
    FanoutExpansionRuntime,
    FanoutRuntimeError,
)
from loom_pipeline_orchestrator.repository import FanoutSourceCandidate, RunLease


class Repository:
    def __init__(self, candidate: FanoutSourceCandidate) -> None:
        self.candidate = candidate
        self.expansions: list[dict[str, Any]] = []

    async def fanout_source_candidates(
        self, _lease: RunLease
    ) -> tuple[FanoutSourceCandidate, ...]:
        return (self.candidate,)

    async def expand_fanout(self, _lease: RunLease, **kwargs: Any) -> int:
        self.expansions.append(kwargs)
        return len(kwargs["manifest"].items)


def fixture() -> tuple[Repository, FakeObjectStore, RunLease, bytes]:
    artifact_id = uuid4()
    manifest = FanoutManifestV1.model_validate(
        {
            "schema_version": "loom.fanout-manifest.v1",
            "items": [
                {
                    "artifact_bindings": [
                        {
                            "artifact_id": uuid4(),
                            "artifact_type": "example.item.v1",
                            "name": "item",
                        }
                    ],
                    "parameters": {},
                    "shard_key": "slot-000",
                }
            ],
        }
    )
    payload = canonical_document(manifest)
    root = canonical_document({"kind": "root"})
    marker = canonical_document({"kind": "marker"})
    candidate = FanoutSourceCandidate(
        node_key="generate",
        source_kind="stage_output",
        source_stage_run_id=uuid4(),
        source_artifact_id=artifact_id,
        source_manifest_digest=digest_bytes(payload),
        source_file_key=f"runs/x/artifacts/{artifact_id}/artifact.json",
        source_file_size=len(payload),
        source_file_sha256=digest_bytes(payload),
        root_manifest_key="runs/x/_manifest.json",
        root_manifest_sha256=digest_bytes(root),
        committed_marker_key="runs/x/_COMMITTED",
        committed_marker_sha256=digest_bytes(marker),
        parameters_contract_digest=None,
    )
    store = FakeObjectStore()
    store.objects[("artifacts", candidate.source_file_key)] = payload
    store.objects[("artifacts", candidate.root_manifest_key)] = root
    store.objects[("artifacts", candidate.committed_marker_key)] = marker
    return (
        Repository(candidate),
        store,
        RunLease(
            pipeline_run_id=uuid4(),
            claimed_by="test",
            lease_epoch=1,
            lease_expires_at=datetime.now(UTC),
        ),
        payload,
    )


@pytest.mark.asyncio
async def test_marker_verified_canonical_manifest_expands_once() -> None:
    repository, store, lease, _payload = fixture()
    runtime = FanoutExpansionRuntime(
        repository=repository,  # type: ignore[arg-type]
        store=store,
        bucket="artifacts",
    )

    assert await runtime.reconcile(lease) == 1
    assert len(repository.expansions) == 1
    assert repository.expansions[0]["node_key"] == "generate"
    assert repository.expansions[0]["run_input_parameters_validated"] is True


@pytest.mark.asyncio
async def test_noncanonical_or_marker_drift_never_reaches_repository() -> None:
    repository, store, lease, payload = fixture()
    candidate = repository.candidate
    store.objects[("artifacts", candidate.source_file_key)] = payload.rstrip(b"\n")
    runtime = FanoutExpansionRuntime(
        repository=repository,  # type: ignore[arg-type]
        store=store,
        bucket="artifacts",
    )

    with pytest.raises(FanoutRuntimeError, match="digest drift"):
        await runtime.reconcile(lease)
    assert repository.expansions == []

    store.objects[("artifacts", candidate.source_file_key)] = payload
    store.objects[("artifacts", candidate.committed_marker_key)] = b"changed\n"
    with pytest.raises(FanoutRuntimeError, match="root marker drift"):
        await runtime.reconcile(lease)
    assert repository.expansions == []
