import hashlib
from collections.abc import Iterator
from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, delete, insert, select, update
from sqlalchemy.orm import sessionmaker

from loom.db.schema import (
    Artifact,
    ArtifactLineageEdge,
    Batch,
    Task,
    TaskSet,
    TaskSetManifest,
    TaskSetMaterializationJob,
    Team,
    TeamQuota,
    Token,
    Trial,
    Worker,
)
from loom_control_plane.app import create_app
from loom_control_plane.config import ControlPlaneSettings


@pytest.fixture
def traj_seed(postgres_url: str) -> Iterator[tuple[UUID, UUID, str]]:
    engine = create_engine(postgres_url)
    session_factory = sessionmaker(engine)
    team_id = uuid4()
    worker_id = uuid4()
    trial_id = uuid4()
    raw = f"w_{uuid4().hex}"
    with session_factory() as s:
        s.execute(insert(Team).values(id=team_id, name=f"x-{team_id}"))
        s.execute(insert(TeamQuota).values(team_id=team_id))
        s.execute(insert(Token).values(
            token_hash=hashlib.sha256(raw.encode()).digest(),
            type="worker", scopes=["worker:index"], team_id=None,
            issued_at=datetime.now(UTC), expires_at=None,
        ))
        s.execute(insert(Worker).values(
            id=worker_id, hostname="h", version="v", capabilities=[],
            registered_at=datetime.now(UTC),
            last_seen_at=datetime.now(UTC), status="active",
        ))
        s.execute(insert(Task).values(id="t", checksum="0" * 64, config={}))
        s.execute(insert(Trial).values(
            id=trial_id, team_id=team_id, task_id="t",
            config={}, requires_caps={}, state="running", worker_id=worker_id,
        ))
        s.commit()
    try:
        yield trial_id, worker_id, raw
    finally:
        with session_factory() as s:
            s.execute(delete(Trial))
            s.execute(delete(ArtifactLineageEdge))
            s.execute(delete(Artifact))
            s.execute(delete(Batch))
            s.execute(delete(TaskSetMaterializationJob))
            s.execute(delete(TaskSetManifest))
            s.execute(delete(TaskSet))
            s.execute(delete(Worker))
            s.execute(delete(Token))
            s.execute(delete(TeamQuota))
            s.execute(delete(Team))
            s.execute(delete(Task))
            s.commit()
        engine.dispose()


@pytest.fixture
def app(
    monkeypatch: pytest.MonkeyPatch, postgres_url: str,
    traj_seed: tuple[UUID, UUID, str],
):
    for k, v in {
        "LOOM_CP_DB_URL": postgres_url,
        "LOOM_CP_MINIO_ENDPOINT": "http://minio:9000",
        "LOOM_CP_MINIO_ACCESS_KEY": "x",
        "LOOM_CP_MINIO_SECRET_KEY": "x",
        "LOOM_CP_LLM_GATEWAY_URL": "http://gw:9100/",
    }.items():
        monkeypatch.setenv(k, v)
    return create_app(ControlPlaneSettings(_env_file=None))


def test_index_patch(app, traj_seed):  # type: ignore[no-untyped-def]
    trial_id, worker_id, raw = traj_seed
    with TestClient(app) as client:
        r = client.patch(
            f"/trials/{trial_id}/trajectory_index",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "worker_id": str(worker_id),
                "trajectory_uri": f"s3://trajectories/x/{trial_id}/events.jsonl",
                "bytes_uploaded": 1024,
                "events_count": 25,
                "checksum_sha256": "abcd",
            },
        )
        assert r.status_code == 200


def test_index_patch_populates_typed_artifacts_and_lineage(
    app,
    traj_seed,
    postgres_url: str,
) -> None:
    trial_id, worker_id, raw = traj_seed
    engine = create_engine(postgres_url)
    parent_artifact_id = uuid4()
    batch_id = uuid4()
    with engine.begin() as conn:
        team_id = conn.execute(
            select(Trial.team_id).where(Trial.id == trial_id),
        ).scalar_one()
        conn.execute(insert(Batch).values(
            id=batch_id,
            team_id=team_id,
            name="derived batch",
            description=None,
            task_filter={"subset_kind": "explicit", "task_ids": ["t"]},
            trial_config={},
            state="running",
            created_by_token_prefix="test",
            expected_trial_count=1,
            backend="docker",
            combinations=[],
            source_provenance=[{
                "kind": "reused_artifact",
                "relation": "reused_as_input",
                "source_artifact_id": str(parent_artifact_id),
            }],
        ))
        conn.execute(update(Trial).where(Trial.id == trial_id).values(
            batch_id=batch_id,
        ))
        conn.execute(insert(Artifact).values(
            id=parent_artifact_id,
            artifact_type="metric_table",
            artifact_schema_version="1.0",
            name="parent metrics",
            team_id=team_id,
            created_by={"kind": "manual_import"},
            content_hash="sha256:" + ("1" * 64),
            storage={
                "backend": "object_store",
                "bucket": "artifacts",
                "key": "parent/metrics.json",
                "media_type": "application/json",
                "size_bytes": 12,
            },
            visibility="org",
            share_status="shared",
            redaction_state="redacted",
            safety_state="safe",
            retention={"class": "shared_reusable"},
            provenance={},
            artifact_metadata={},
        ))

    with TestClient(app) as client:
        r = client.patch(
            f"/trials/{trial_id}/trajectory_index",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "worker_id": str(worker_id),
                "schema_version": "1",
                "trial_id": str(trial_id),
                "team_id": str(team_id),
                "task_id": "t",
                "trajectory_uri": (
                    f"s3://trajectories/{team_id}/{trial_id}/events.jsonl"
                ),
                "atif_uri": f"s3://trajectories/{team_id}/{trial_id}/atif.json",
                "atif_schema_version": "1.7",
                "artifacts": [{
                    "step_name": "main",
                    "bucket": "artifacts",
                    "key": f"{team_id}/{trial_id}/main/result.txt",
                    "size": 5,
                    "content_hash": "sha256:" + ("2" * 64),
                    "share_status": "shared",
                    "blocked_reason": None,
                }],
            },
        )

    assert r.status_code == 200, r.text

    sl = sessionmaker(engine)
    with sl() as s:
        artifacts = list(s.execute(
            select(Artifact).where(Artifact.trial_id == trial_id),
        ).scalars())
        edges = list(s.execute(
            select(ArtifactLineageEdge).where(
                ArtifactLineageEdge.parent_artifact_id == parent_artifact_id,
            ),
        ).scalars())
    engine.dispose()

    by_type = {artifact.artifact_type: artifact for artifact in artifacts}
    assert {"trajectory", "atif_projection", "evidence_bundle"}.issubset(by_type)
    assert by_type["evidence_bundle"].content_hash == "sha256:" + ("2" * 64)
    assert by_type["evidence_bundle"].storage["key"].endswith("/main/result.txt")
    assert all(edge.relation == "reused_as_input" for edge in edges)
    assert {edge.child_artifact_id for edge in edges} == {
        artifact.id for artifact in artifacts
    }


def test_index_patch_fenced(app, traj_seed):  # type: ignore[no-untyped-def]
    """A different worker_id → 409 (claim lost)."""
    trial_id, _worker_id, raw = traj_seed
    with TestClient(app) as client:
        r = client.patch(
            f"/trials/{trial_id}/trajectory_index",
            headers={"Authorization": f"Bearer {raw}"},
            json={
                "worker_id": str(uuid4()),
                "trajectory_uri": "s3://x",
            },
        )
        assert r.status_code == 409
