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
    GB10WorkerNodeStatus,
    GB10WorkerPoolDesiredState,
    Task,
    Team,
    TeamQuota,
    Token,
    Trial,
    Worker,
    WorkerPoolAutoscalerPolicy,
)
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
            s.execute(delete(WorkerPoolAutoscalerPolicy))
            s.execute(delete(GB10WorkerNodeStatus))
            s.execute(delete(GB10WorkerPoolDesiredState))
            s.execute(delete(Trial))
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


def test_prod_pressure_controller_fences_claim_and_recovers_after_agent_confirmation(
    app,
    claim_seed,
    postgres_url: str,
):  # type: ignore[no-untyped-def]
    worker_id, raw_worker, _ = claim_seed
    admin_headers = {"Authorization": f"Bearer {RAW_ADMIN_TOKEN}"}
    worker_headers = {"Authorization": f"Bearer {raw_worker}"}
    with TestClient(app) as client:
        desired = client.put(
            "/admin/gb10-worker-pools/staging/default/desired-state",
            headers=admin_headers,
            json={
                "image_tag": "staging-local",
                "max_concurrent": 1,
                "env_config_version": "staging-local",
                "target_slots": 1,
                "host_intents": {"h": "active"},
            },
        )
        assert desired.status_code == 200, desired.text

        pressure = client.get(
            "/admin/worker-pools/default/prod-pressure",
            headers=admin_headers,
        )
        assert pressure.status_code == 200, pressure.text
        assert pressure.json()["prod_pending_count"] == 1
        assert pressure.json()["has_pressure"] is True

        drain = client.post(
            "/admin/gb10-worker-pools/staging/default/prod-pressure",
            headers=admin_headers,
            json={
                **pressure.json(),
                "source": "control-plane prod queue summary",
                "preemptible": True,
                "grace_period_seconds": 600,
            },
        )
        assert drain.status_code == 200, drain.text
        assert drain.json()["action"] == "draining"
        assert drain.json()["new_staging_claims_allowed"] is False
        assert drain.json()["host_intents"] == {"h": "stopped"}
        assert drain.json()["grace"]["action"] == "wait"

        engine = create_engine(postgres_url)
        session_factory = sessionmaker(engine)
        with session_factory() as session:
            worker = session.get(Worker, worker_id)
            assert worker is not None
            assert worker.drain_state == "drained"
            assert worker.drain_owner == "prod-pressure-controller"

        stale_active_report = client.post(
            "/admin/gb10-worker-pools/staging/default/nodes/h/report",
            headers=admin_headers,
            json={
                "current_image_tag": "staging-local",
                "current_max_concurrent": 1,
                "current_env_config_version": "staging-local",
                "current_intent": "active",
                "apply_state": "applied",
                "last_apply_result": "stale report while prod pressure remains active",
            },
        )
        assert stale_active_report.status_code == 200, stale_active_report.text

        fenced = client.post(
            "/trials/claim",
            headers=worker_headers,
            json={"worker_id": str(worker_id), "caps": [_LINUX_PUBLIC_CAP]},
        )
        assert fenced.status_code == 204

        recovery = client.post(
            "/admin/gb10-worker-pools/staging/default/prod-pressure",
            headers=admin_headers,
            json={
                "prod_pending_count": 0,
                "prod_active_count": 0,
                "prod_capacity_shortfall": 0,
                "source": "control-plane prod queue summary",
                "preemptible": True,
                "grace_period_seconds": 600,
            },
        )
        assert recovery.status_code == 200, recovery.text
        assert recovery.json()["action"] == "recovered"
        assert recovery.json()["host_intents"] == {"h": "active"}

        with session_factory() as session:
            worker = session.get(Worker, worker_id)
            assert worker is not None
            assert worker.drain_state == "drained"
            assert worker.drain_owner == "prod-pressure-controller"

        still_fenced = client.post(
            "/trials/claim",
            headers=worker_headers,
            json={"worker_id": str(worker_id), "caps": [_LINUX_PUBLIC_CAP]},
        )
        assert still_fenced.status_code == 204

        confirmed = client.post(
            "/admin/gb10-worker-pools/staging/default/nodes/h/report",
            headers=admin_headers,
            json={
                "current_image_tag": "staging-local",
                "current_max_concurrent": 1,
                "current_env_config_version": "staging-local",
                "current_intent": "active",
                "apply_state": "applied",
                "last_apply_result": "docker compose worker reconciled",
            },
        )
        assert confirmed.status_code == 200, confirmed.text

        with session_factory() as session:
            worker = session.get(Worker, worker_id)
            assert worker is not None
            assert worker.drain_state == "active"
            assert worker.drain_owner is None
        engine.dispose()

        claimed = client.post(
            "/trials/claim",
            headers=worker_headers,
            json={"worker_id": str(worker_id), "caps": [_LINUX_PUBLIC_CAP]},
        )
        assert claimed.status_code == 200, claimed.text


def test_prod_pressure_keeps_non_preemptible_busy_host_running(
    app,
    claim_seed,
    postgres_url: str,
):  # type: ignore[no-untyped-def]
    worker_id, raw_worker, _ = claim_seed
    admin_headers = {"Authorization": f"Bearer {RAW_ADMIN_TOKEN}"}
    with TestClient(app) as client:
        assert client.put(
            "/admin/gb10-worker-pools/staging/default/desired-state",
            headers=admin_headers,
            json={
                "image_tag": "staging-local",
                "max_concurrent": 1,
                "env_config_version": "staging-local",
                "target_slots": 1,
                "host_intents": {"h": "active"},
            },
        ).status_code == 200
        claimed = client.post(
            "/trials/claim",
            headers={"Authorization": f"Bearer {raw_worker}"},
            json={"worker_id": str(worker_id), "caps": [_LINUX_PUBLIC_CAP]},
        )
        assert claimed.status_code == 200, claimed.text

        drain = client.post(
            "/admin/gb10-worker-pools/staging/default/prod-pressure",
            headers=admin_headers,
            json={
                "prod_pending_count": 1,
                "prod_active_count": 1,
                "prod_capacity_shortfall": 1,
                "preemptible": False,
                "grace_period_seconds": 0,
            },
        )
        assert drain.status_code == 200, drain.text
        body = drain.json()
        assert body["new_staging_claims_allowed"] is False
        assert body["host_intents"] == {"h": "active"}
        assert body["running_staging_trials"] == 1
        assert body["retryable_preemption_trials"] == 0
        assert body["grace"]["action"] == "not_preemptible"

    engine = create_engine(postgres_url)
    with sessionmaker(engine)() as session:
        worker = session.get(Worker, worker_id)
        trial = session.execute(
            select(Trial).where(Trial.worker_id == worker_id),
        ).scalar_one()
        assert worker is not None
        assert worker.drain_state == "draining"
        assert trial.state == "claimed"
        assert trial.failure_reason is None
    engine.dispose()


def test_prod_pressure_preempts_busy_host_only_after_zero_grace(
    app,
    claim_seed,
    postgres_url: str,
):  # type: ignore[no-untyped-def]
    worker_id, raw_worker, _ = claim_seed
    admin_headers = {"Authorization": f"Bearer {RAW_ADMIN_TOKEN}"}
    with TestClient(app) as client:
        assert client.put(
            "/admin/gb10-worker-pools/staging/default/desired-state",
            headers=admin_headers,
            json={
                "image_tag": "staging-local",
                "max_concurrent": 1,
                "env_config_version": "staging-local",
                "target_slots": 1,
                "host_intents": {"h": "active"},
            },
        ).status_code == 200
        claimed = client.post(
            "/trials/claim",
            headers={"Authorization": f"Bearer {raw_worker}"},
            json={"worker_id": str(worker_id), "caps": [_LINUX_PUBLIC_CAP]},
        )
        assert claimed.status_code == 200, claimed.text

        preempt = client.post(
            "/admin/gb10-worker-pools/staging/default/prod-pressure",
            headers=admin_headers,
            json={
                "prod_pending_count": 1,
                "prod_active_count": 1,
                "prod_capacity_shortfall": 1,
                "preemptible": True,
                "grace_period_seconds": 0,
            },
        )
        assert preempt.status_code == 200, preempt.text
        body = preempt.json()
        assert body["action"] == "preempting_after_grace"
        assert body["host_intents"] == {"h": "stopped"}
        assert body["running_staging_trials"] == 1
        assert body["retryable_preemption_trials"] == 1
        assert body["grace"]["action"] == "cancel_retryable"

    engine = create_engine(postgres_url)
    with sessionmaker(engine)() as session:
        trial = session.execute(
            select(Trial).where(Trial.worker_id == worker_id),
        ).scalar_one()
        audit = session.execute(
            select(AdminAuditEvent).order_by(AdminAuditEvent.created_at.desc()),
        ).scalars().first()
        assert trial.state == "claimed"
        assert trial.failure_reason == "prod_capacity_pressure"
        assert "safe to retry" not in (trial.failure_message or "")
        assert "crash reclaim returns it to queued" in (trial.failure_message or "")
        assert audit is not None
        assert audit.event_metadata["retryable_preemption_trials"] == 1
    engine.dispose()


def test_neutral_prod_pressure_route_records_slurm_drain_intent(
    app,
    claim_seed,
    postgres_url: str,
):  # type: ignore[no-untyped-def]
    admin_headers = {"Authorization": f"Bearer {RAW_ADMIN_TOKEN}"}
    engine = create_engine(postgres_url)
    with sessionmaker(engine)() as session:
        session.execute(insert(WorkerPoolAutoscalerPolicy).values(
            environment="staging",
            pool_name="slurm-pool",
            actuator="slurm",
            enabled=True,
            min_slots=0,
            max_slots=6,
            actuator_config={"backend": "docker", "cpu_arch": "x86_64"},
        ))
        session.commit()
    engine.dispose()

    with TestClient(app) as client:
        # #892: the actuator-neutral route drives a Slurm pool and records
        # drain intent (no GB10 desired state exists for this pool).
        drain = client.post(
            "/admin/worker-pools/staging/slurm-pool/prod-pressure",
            headers=admin_headers,
            json={
                "prod_pending_count": 2,
                "prod_active_count": 0,
                "prod_capacity_shortfall": 2,
                "preemptible": True,
                "grace_period_seconds": 600,
            },
        )
        assert drain.status_code == 200, drain.text
        body = drain.json()
        assert body["action"] == "draining"
        assert body["actuator"] == "slurm"
        assert body["new_staging_claims_allowed"] is False
        assert body["drain_intent_active"] is True

    engine = create_engine(postgres_url)
    with sessionmaker(engine)() as session:
        policy = session.execute(
            select(WorkerPoolAutoscalerPolicy).where(
                WorkerPoolAutoscalerPolicy.pool_name == "slurm-pool",
            ),
        ).scalar_one()
        assert policy.prod_pressure_state is not None
        assert policy.prod_pressure_state["state"] == "draining"
    engine.dispose()


def test_gb10_prod_pressure_alias_route_still_drains_gb10_pool(
    app,
    claim_seed,
    postgres_url: str,
):  # type: ignore[no-untyped-def]
    admin_headers = {"Authorization": f"Bearer {RAW_ADMIN_TOKEN}"}
    with TestClient(app) as client:
        assert client.put(
            "/admin/gb10-worker-pools/staging/default/desired-state",
            headers=admin_headers,
            json={
                "image_tag": "staging-local",
                "max_concurrent": 1,
                "env_config_version": "staging-local",
                "target_slots": 1,
                "host_intents": {"h": "active"},
            },
        ).status_code == 200

        # The legacy alias keeps driving the GB10 desired-state path.
        drain = client.post(
            "/admin/gb10-worker-pools/staging/default/prod-pressure",
            headers=admin_headers,
            json={
                "prod_pending_count": 1,
                "prod_active_count": 0,
                "prod_capacity_shortfall": 1,
                "preemptible": True,
                "grace_period_seconds": 600,
            },
        )
        assert drain.status_code == 200, drain.text
        body = drain.json()
        assert body["action"] == "draining"
        assert body["new_staging_claims_allowed"] is False
        assert body["host_intents"] == {"h": "stopped"}


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
