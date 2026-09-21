import hashlib
from collections.abc import Iterator
from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, delete, insert, select, update
from sqlalchemy.orm import sessionmaker

from loom.db.schema import (
    AdminAuditEvent,
    ModelSwitchPlan,
    Task,
    TaskImageMaterialization,
    Team,
    TeamQuota,
    Token,
    Trial,
    TrialTaskImageMaterialization,
    Worker,
)
from loom.models.model_switch_plan import PRNG_VERSION
from loom_control_plane.app import create_app
from loom_control_plane.config import ControlPlaneSettings

RAW_ADMIN_TOKEN = "loom_admin_" + "P" * 43


@pytest.fixture
def claim_seed(postgres_url: str) -> Iterator[tuple[UUID, str, UUID]]:
    engine = create_engine(postgres_url)
    session_factory = sessionmaker(engine)
    team_id = uuid4()
    worker_id = uuid4()
    raw_worker = f"loom_worker_{uuid4().hex}"
    with session_factory() as s:
        s.execute(insert(Team).values(id=team_id, name=f"x-{team_id}"))
        s.execute(insert(TeamQuota).values(team_id=team_id))
        s.execute(insert(Token).values(
            token_hash=hashlib.sha256(raw_worker.encode()).digest(),
            type="worker", scopes=["worker:claim", "worker:report"],
            team_id=None,
            issued_at=datetime.now(UTC), expires_at=None,
        ))
        s.execute(insert(Task).values(id="t", checksum="0" * 64, config={}))
        s.execute(insert(Trial).values(
            id=uuid4(), team_id=team_id, task_id="t",
            config={},
            requires_caps={
                "os": "linux", "gpu_vendor": "none",
                "network_policies": ["public"],
            },
            state="queued",
        ))
        s.execute(insert(Worker).values(
            id=worker_id, hostname="h", version="v",
            capabilities=[{
                "os": "linux", "gpu_vendor": "none",
                "network_policies": ["public"],
                "dynamic_network_policy": True, "mounted_fs": True,
                "resource_modes": ["auto"],
            }],
            registered_at=datetime.now(UTC),
            last_seen_at=datetime.now(UTC), status="active",
        ))
        s.commit()
    try:
        yield worker_id, raw_worker, team_id
    finally:
        with session_factory() as s:
            s.execute(delete(AdminAuditEvent))
            s.execute(delete(Trial))
            s.execute(delete(TaskImageMaterialization))
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
    claim_seed: tuple[UUID, str, UUID],
    tmp_path,
):
    for k, v in {
        "LOOM_ENV": "development",
        "LOOM_LOCAL_EXECUTION": "1",
        "LOOM_CP_DB_URL": postgres_url,
        "LOOM_CP_MINIO_ENDPOINT": "http://minio:9000",
        "LOOM_CP_MINIO_ACCESS_KEY": "x",
        "LOOM_CP_MINIO_SECRET_KEY": "x",
        "LOOM_CP_LLM_GATEWAY_URL": "http://gw:9100/",
    }.items():
        monkeypatch.setenv(k, v)
    secret_file = tmp_path / "secrets.toml"
    secret_file.write_text(
        f'[admin]\ntoken = "{RAW_ADMIN_TOKEN}"\n'
        'created_at = "2026-07-15T00:00:00Z"\nversion = 1\n',
        encoding="utf-8",
    )
    secret_file.chmod(0o600)
    monkeypatch.setenv("LOOM_CP_ADMIN_SECRET_FILE", str(secret_file))
    return create_app(ControlPlaneSettings(_env_file=None))


_LINUX_PUBLIC_CAP = {
    "os": "linux", "gpu_vendor": "none",
    "network_policies": ["public"],
    "dynamic_network_policy": True, "mounted_fs": True,
    "resource_modes": ["auto"],
}


def test_claim_returns_trial(app, claim_seed):  # type: ignore[no-untyped-def]
    worker_id, raw_worker, _ = claim_seed
    with TestClient(app) as client:
        r = client.post(
            "/trials/claim",
            headers={"Authorization": f"Bearer {raw_worker}"},
            json={"worker_id": str(worker_id), "caps": [_LINUX_PUBLIC_CAP]},
        )
        assert r.status_code == 200, r.text
        body = r.json()
        assert "trial_id" in body
        assert body["state"] == "claimed"
        assert body["attempt_count"] == 1
        # Ordinary trials stay on this endpoint; plan is optional/null.
        assert body["model_switch_plan"] is None


def test_claim_includes_persisted_model_switch_plan(
    app,
    claim_seed,
    postgres_url: str,
):  # type: ignore[no-untyped-def]
    worker_id, raw_worker, _ = claim_seed
    plan_id = uuid4()
    engine = create_engine(postgres_url)
    with sessionmaker(engine)() as session:
        trial_id = session.execute(select(Trial.id)).scalar_one()
        session.execute(
            insert(ModelSwitchPlan).values(
                id=plan_id,
                trial_id=trial_id,
                combination_idx=0,
                mix_mode="beta_mixture",
                k1=None,
                k2=None,
                teacher_episodes=None,
                beta=0.6,
                seed="42",
                prng_version=PRNG_VERSION,
                student_model_snapshot={
                    "provider": "openai",
                    "name": "glm-5.2",
                    "source": "api",
                },
                teacher_model_snapshot={
                    "provider": "openai",
                    "name": "glm-5.2-urg",
                    "source": "api",
                },
                pricing_snapshot={},
                capability_snapshot={},
            )
        )
        session.commit()
    engine.dispose()
    with TestClient(app) as client:
        r = client.post(
            "/trials/claim",
            headers={"Authorization": f"Bearer {raw_worker}"},
            json={"worker_id": str(worker_id), "caps": [_LINUX_PUBLIC_CAP]},
        )
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["trial_id"] == str(trial_id)
        assert body["state"] == "claimed"
        assert body["attempt_count"] == 1
        plan = body["model_switch_plan"]
        assert plan is not None
        assert plan["id"] == str(plan_id)
        assert plan["mix_mode"] == "beta_mixture"
        assert plan["beta"] == 0.6
        assert plan["seed"] == "42"
        assert plan["k1"] is None
        assert plan["k2"] is None


def test_claim_carries_frozen_task_image_execution_grant(
    app,
    claim_seed,
    postgres_url: str,
):  # type: ignore[no-untyped-def]
    worker_id, raw_worker, _ = claim_seed
    materialization_id = uuid4()
    main_ref = "registry.example/loom-task@sha256:" + "a" * 64
    sidecar_ref = "registry.example/loom-task@sha256:" + "b" * 64
    engine = create_engine(postgres_url)
    with sessionmaker(engine)() as session:
        trial_id = session.execute(select(Trial.id)).scalar_one()
        session.execute(
            insert(TaskImageMaterialization).values(
                id=materialization_id,
                materialization_key="1" * 64,
                task_id="t",
                task_checksum="0" * 64,
                cpu_arch="x86_64",
                task_config={
                    "schema_version": "1",
                    "task": {"id": "t", "name": "Frozen task"},
                    "environment": {
                        "os": "linux",
                        "cpu_arch": "x86_64",
                        "dockerfile": "environment/Dockerfile",
                        "sidecars": [
                            {
                                "name": "api",
                                "dockerfile": "sidecars/api/Dockerfile",
                            }
                        ],
                    },
                    "agent": {"name": "oracle"},
                    "verifier": {"name": "pytest"},
                    "steps": [{"name": "main"}],
                },
                task_source="s3://loom-task-bundles/frozen.tar.zst",
                task_source_provenance={"snapshot": "frozen"},
                state="ready",
                registry_images={"task": main_ref, "sidecar:api": sidecar_ref},
            )
        )
        session.execute(
            insert(TrialTaskImageMaterialization).values(
                trial_id=trial_id,
                materialization_id=materialization_id,
            )
        )
        session.commit()
    engine.dispose()

    cap = {**_LINUX_PUBLIC_CAP, "cpu_arch": "x86_64"}
    with TestClient(app) as client:
        response = client.post(
            "/trials/claim",
            headers={"Authorization": f"Bearer {raw_worker}"},
            json={"worker_id": str(worker_id), "caps": [cap]},
        )

    assert response.status_code == 200, response.text
    assert response.json()["task_image_materialization"] == {
        "schema_version": "loom.task-image-execution-grant.v1",
        "materialization_id": str(materialization_id),
        "materialization_key": "1" * 64,
        "cpu_arch": "x86_64",
        "task_checksum": "0" * 64,
        "task_config": {
            "schema_version": "1",
            "task": {"id": "t", "name": "Frozen task"},
            "environment": {
                "os": "linux",
                "cpu_arch": "x86_64",
                "dockerfile": "environment/Dockerfile",
                "sidecars": [
                    {
                        "name": "api",
                        "dockerfile": "sidecars/api/Dockerfile",
                    }
                ],
            },
            "agent": {"name": "oracle"},
            "verifier": {"name": "pytest"},
            "steps": [{"name": "main"}],
        },
        "task_source": "s3://loom-task-bundles/frozen.tar.zst",
        "task_source_provenance": {"snapshot": "frozen"},
        "registry_images": {"task": main_ref, "sidecar:api": sidecar_ref},
    }








@pytest.mark.legacy_pool




def test_claim_clears_stale_failure_diagnostic(
    app,
    claim_seed,
    postgres_url: str,
):  # type: ignore[no-untyped-def]
    worker_id, raw_worker, _ = claim_seed
    engine = create_engine(postgres_url)
    session_factory = sessionmaker(engine)
    with session_factory() as session:
        trial_id = session.execute(select(Trial.id)).scalar_one()
        session.execute(
            update(Trial)
            .where(Trial.id == trial_id)
            .values(
                failure_reason="worker_lost_claim",
                failure_message=(
                    "claimed_without_started_reclaimed trial_id="
                    f"{trial_id} worker_id={worker_id}"
                ),
            )
        )
        session.commit()
    engine.dispose()

    with TestClient(app) as client:
        r = client.post(
            "/trials/claim",
            headers={"Authorization": f"Bearer {raw_worker}"},
            json={"worker_id": str(worker_id), "caps": [_LINUX_PUBLIC_CAP]},
        )
        assert r.status_code == 200, r.text

    engine = create_engine(postgres_url)
    session_factory = sessionmaker(engine)
    with session_factory() as session:
        row = session.execute(
            select(Trial).where(Trial.id == trial_id),
        ).scalar_one()
    engine.dispose()

    assert row.state == "claimed"
    assert row.failure_reason is None
    assert row.failure_message is None


def test_pre_start_heartbeat_updates_claimed_unstarted_trial(
    app,
    claim_seed,
    postgres_url: str,
):  # type: ignore[no-untyped-def]
    worker_id, raw_worker, _ = claim_seed
    with TestClient(app) as client:
        claim = client.post(
            "/trials/claim",
            headers={"Authorization": f"Bearer {raw_worker}"},
            json={"worker_id": str(worker_id), "caps": [_LINUX_PUBLIC_CAP]},
        )
        assert claim.status_code == 200, claim.text
        trial_id = UUID(claim.json()["trial_id"])

        heartbeat = client.post(
            f"/trials/{trial_id}/pre-start-heartbeat",
            headers={"Authorization": f"Bearer {raw_worker}"},
            json={"worker_id": str(worker_id)},
        )
        assert heartbeat.status_code == 200, heartbeat.text
        assert heartbeat.json()["trial_id"] == str(trial_id)
        assert heartbeat.json()["pre_start_heartbeat_at"]

    engine = create_engine(postgres_url)
    session_factory = sessionmaker(engine)
    with session_factory() as session:
        row = session.execute(
            select(Trial).where(Trial.id == trial_id),
        ).scalar_one()
        assert row.pre_start_heartbeat_at is not None
        session.execute(
            update(Trial)
            .where(Trial.id == trial_id)
            .values(started_at=datetime.now(UTC)),
        )
        session.commit()
    engine.dispose()

    with TestClient(app) as client:
        fenced = client.post(
            f"/trials/{trial_id}/pre-start-heartbeat",
            headers={"Authorization": f"Bearer {raw_worker}"},
            json={"worker_id": str(worker_id)},
        )
        assert fenced.status_code == 409


def test_claim_no_match_returns_204(app, claim_seed):  # type: ignore[no-untyped-def]
    worker_id, raw_worker, _ = claim_seed
    with TestClient(app) as client:
        # First claim drains the only queued trial.
        client.post(
            "/trials/claim",
            headers={"Authorization": f"Bearer {raw_worker}"},
            json={"worker_id": str(worker_id), "caps": [_LINUX_PUBLIC_CAP]},
        )
        # Second claim has nothing.
        r = client.post(
            "/trials/claim",
            headers={"Authorization": f"Bearer {raw_worker}"},
            json={"worker_id": str(worker_id), "caps": [_LINUX_PUBLIC_CAP]},
        )
        assert r.status_code == 204


def test_draining_worker_cannot_claim_new_trial(
    app,
    claim_seed,
    postgres_url: str,
):  # type: ignore[no-untyped-def]
    assert hasattr(Worker, "drain_state")
    worker_id, raw_worker, _ = claim_seed
    engine = create_engine(postgres_url)
    session_factory = sessionmaker(engine)
    with session_factory() as session:
        session.execute(
            update(Worker)
            .where(Worker.id == worker_id)
            .values(
                drain_state="draining",
                drain_reason="autoscaler scale-down",
                drain_owner="worker-pool-autoscaler",
            ),
        )
        session.commit()
    engine.dispose()

    with TestClient(app) as client:
        r = client.post(
            "/trials/claim",
            headers={"Authorization": f"Bearer {raw_worker}"},
            json={"worker_id": str(worker_id), "caps": [_LINUX_PUBLIC_CAP]},
        )

    assert r.status_code == 204


def test_claim_rejects_unauth(app, claim_seed):  # type: ignore[no-untyped-def]
    worker_id, _, _ = claim_seed
    with TestClient(app) as client:
        r = client.post(
            "/trials/claim",
            json={"worker_id": str(worker_id), "caps": [_LINUX_PUBLIC_CAP]},
        )
        assert r.status_code == 401


def test_heartbeat_updates_last_seen(app, claim_seed):  # type: ignore[no-untyped-def]
    worker_id, raw_worker, _ = claim_seed
    with TestClient(app) as client:
        r = client.post(
            f"/workers/{worker_id}/heartbeat",
            headers={"Authorization": f"Bearer {raw_worker}"},
        )
        assert r.status_code == 200
        assert r.json()["status"] == "ok"


def test_heartbeat_can_mark_intentional_idle_exit(
    app,
    claim_seed,
    postgres_url: str,
):  # type: ignore[no-untyped-def]
    worker_id, raw_worker, _ = claim_seed
    with TestClient(app) as client:
        r = client.post(
            f"/workers/{worker_id}/heartbeat",
            headers={"Authorization": f"Bearer {raw_worker}"},
            json={"status": "idle-exit"},
        )
        assert r.status_code == 200
        assert r.json()["status"] == "ok"

    engine = create_engine(postgres_url)
    session_factory = sessionmaker(engine)
    with session_factory() as session:
        status = session.execute(
            select(Worker.status).where(Worker.id == worker_id),
        ).scalar_one()
    engine.dispose()

    assert status == "idle-exit"


def test_heartbeat_rejects_unknown_status(app, claim_seed):  # type: ignore[no-untyped-def]
    worker_id, raw_worker, _ = claim_seed
    with TestClient(app) as client:
        r = client.post(
            f"/workers/{worker_id}/heartbeat",
            headers={"Authorization": f"Bearer {raw_worker}"},
            json={"status": "surprise"},
        )
        assert r.status_code == 400
