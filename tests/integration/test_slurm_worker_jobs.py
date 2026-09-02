from __future__ import annotations

import hashlib
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, delete, select
from sqlalchemy.orm import sessionmaker

from loom.db.schema import SlurmWorkerJob, Token, Worker
from loom_control_plane.app import create_app
from loom_control_plane.config import ControlPlaneSettings

RAW_ADMIN_TOKEN = "loom_admin_" + "S" * 43


def _write_admin_secret(path: Path) -> None:
    path.write_text(
        "[admin]\n"
        f"token = \"{RAW_ADMIN_TOKEN}\"\n"
        "created_at = \"2026-06-24T00:00:00Z\"\n"
        "version = 1\n",
        encoding="utf-8",
    )
    path.chmod(0o600)


def _set_cp_env(monkeypatch: pytest.MonkeyPatch, postgres_url: str) -> None:
    for k, v in {
        "LOOM_CP_DB_URL": postgres_url,
        "LOOM_CP_MINIO_ENDPOINT": "http://minio:9000",
        "LOOM_CP_MINIO_ACCESS_KEY": "x",
        "LOOM_CP_MINIO_SECRET_KEY": "y",
        "LOOM_CP_LLM_GATEWAY_URL": "http://gw:9100/",
    }.items():
        monkeypatch.setenv(k, v)


@pytest.fixture
def app(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    postgres_url: str,
):
    secret_file = tmp_path / "secrets.toml"
    _write_admin_secret(secret_file)
    _set_cp_env(monkeypatch, postgres_url)
    monkeypatch.setenv("LOOM_CP_ADMIN_SECRET_FILE", str(secret_file))
    return create_app(ControlPlaneSettings(_env_file=None))


@pytest.fixture(autouse=True)
def clean_slurm_jobs(postgres_url: str) -> Iterator[None]:
    engine = create_engine(postgres_url)
    session_factory = sessionmaker(engine)
    with session_factory() as s:
        s.execute(delete(SlurmWorkerJob))
        s.execute(delete(Worker))
        s.execute(delete(Token))
        s.commit()
    try:
        yield
    finally:
        with session_factory() as s:
            s.execute(delete(SlurmWorkerJob))
            s.execute(delete(Worker))
            s.execute(delete(Token))
            s.commit()
        engine.dispose()


def _record_payload(job_id: str = "13441") -> dict[str, object]:
    return {
        "environment": "production",
        "pool_name": "oldlab",
        "nodelist": "oldlab-[1-3]",
        "requested_cpus": 12,
        "requested_memory_mib": 58000,
        "requested_pids": 512,
        "requested_gpu_tres": "gpu:a100:2",
        "requested_gpus": 2,
        "requested_concurrency": 6,
        "sandbox_identity": "production",
        "candidate_sha": "a" * 40,
        "compose_project": f"loom-production-aaaaaaaaaaaa-{job_id}",
        "job_id": job_id,
        "slurm_state": "PENDING",
        "pending_reason": "Resources",
        "env": {
            "LOOM_WORKER_TOKEN": "loom_w_secret",
            "LOOM_WORKER_MAX_CONCURRENT": "6",
            "PATH": "/usr/bin",
        },
    }


def test_record_submission_redacts_env_and_blocks_duplicate_active_capacity(
    app,
    postgres_url: str,
) -> None:
    with TestClient(app) as client:
        r = client.post(
            "/admin/slurm-worker-jobs",
            headers={"Authorization": f"Bearer {RAW_ADMIN_TOKEN}"},
            json=_record_payload(),
        )
        assert r.status_code == 201, r.text
        created = r.json()
        assert created["state"] == "pending"
        assert created["requested_pids"] == 512
        assert created["requested_gpu_tres"] == "gpu:a100:2"
        assert created["requested_gpus"] == 2
        assert created["sandbox_identity"] == "production"
        assert created["candidate_sha"] == "a" * 40
        assert created["compose_project"] == "loom-production-aaaaaaaaaaaa-13441"
        assert created["redacted_env"]["LOOM_WORKER_TOKEN"] == "<redacted>"

        dup = client.post(
            "/admin/slurm-worker-jobs",
            headers={"Authorization": f"Bearer {RAW_ADMIN_TOKEN}"},
            json=_record_payload(job_id="13442"),
        )
        assert dup.status_code == 409, dup.text
        assert dup.json()["existing_id"] == created["id"]

        same_job_id = _record_payload(job_id="13441")
        same_job_id["nodelist"] = "oldlab-4"
        same_job_id["requested_concurrency"] = 7
        dup_job_id = client.post(
            "/admin/slurm-worker-jobs",
            headers={"Authorization": f"Bearer {RAW_ADMIN_TOKEN}"},
            json=same_job_id,
        )
        assert dup_job_id.status_code == 409, dup_job_id.text
        assert dup_job_id.json()["existing_job_id"] == "13441"

    engine = create_engine(postgres_url)
    try:
        with sessionmaker(engine)() as s:
            row = s.execute(select(SlurmWorkerJob)).scalar_one()
    finally:
        engine.dispose()

    assert row.environment == "production"
    assert row.pool_name == "oldlab"
    assert row.requested_concurrency == 6
    assert row.requested_pids == 512
    assert row.requested_gpu_tres == "gpu:a100:2"
    assert row.requested_gpus == 2
    assert row.sandbox_identity == "production"
    assert row.candidate_sha == "a" * 40
    assert row.compose_project == "loom-production-aaaaaaaaaaaa-13441"
    assert row.slurm_state == "PENDING"
    assert row.state == "pending"
    assert row.pending_reason == "Resources"
    assert row.redacted_env == {
        "LOOM_WORKER_TOKEN": "<redacted>",
        "LOOM_WORKER_AUTH_FINGERPRINT": (
            f"sha256:{hashlib.sha256(b'loom_w_secret').hexdigest()[:12]} "
            "len=13"
        ),
        "LOOM_WORKER_MAX_CONCURRENT": "6",
        "PATH": "/usr/bin",
    }


def test_reconcile_updates_state_associates_worker_and_status_omits_secrets(
    app,
    postgres_url: str,
) -> None:
    worker_id = uuid4()
    now = datetime.now(UTC)
    engine = create_engine(postgres_url)
    try:
        with sessionmaker(engine)() as s:
            s.add(Worker(
                id=worker_id,
                hostname="oldlab-4",
                version="0.1",
                capabilities=[],
                registered_at=now,
                last_seen_at=now,
                status="active",
            ))
            s.commit()
    finally:
        engine.dispose()

    with TestClient(app) as client:
        created = client.post(
            "/admin/slurm-worker-jobs",
            headers={"Authorization": f"Bearer {RAW_ADMIN_TOKEN}"},
            json=_record_payload(job_id="200"),
        )
        assert created.status_code == 201, created.text

        engine = create_engine(postgres_url)
        try:
            with sessionmaker(engine)() as s:
                job = s.execute(select(SlurmWorkerJob)).scalar_one()
                job.worker_id = worker_id
                s.commit()
        finally:
            engine.dispose()

        reconciled = client.post(
            "/admin/slurm-worker-jobs/reconcile",
            headers={"Authorization": f"Bearer {RAW_ADMIN_TOKEN}"},
            json={
                "stale_after_seconds": 300,
                "observations": [
                    {
                        "job_id": "200",
                        "slurm_state": "RUNNING",
                        "nodelist": "oldlab-4",
                        "pending_reason": None,
                        "worker_id": str(worker_id),
                    },
                ],
            },
        )
        assert reconciled.status_code == 200, reconciled.text
        assert reconciled.json()["updated"] == 1

        status = client.get(
            "/admin/slurm-worker-jobs/status",
            headers={"Authorization": f"Bearer {RAW_ADMIN_TOKEN}"},
        )
        assert status.status_code == 200, status.text
        body = status.json()

    assert "loom_w_secret" not in str(body)
    assert body["summary"][0]["environment"] == "production"
    assert body["summary"][0]["pool_name"] == "oldlab"
    assert body["summary"][0]["active_slots"] == 6
    assert body["summary"][0]["running_jobs"] == 1
    assert body["jobs"][0]["job_id"] == "200"
    assert body["jobs"][0]["state"] == "running"
    assert body["jobs"][0]["worker_id"] == str(worker_id)


def test_reconcile_cannot_create_or_replace_slurm_worker_link(
    app,
    postgres_url: str,
) -> None:
    first_worker_id = uuid4()
    second_worker_id = uuid4()
    now = datetime.now(UTC)
    engine = create_engine(postgres_url)
    try:
        with sessionmaker(engine)() as s:
            s.add_all([
                Worker(
                    id=first_worker_id,
                    hostname="oldlab-1",
                    version="0.1",
                    capabilities=[],
                    registered_at=now,
                    last_seen_at=now,
                    status="active",
                ),
                Worker(
                    id=second_worker_id,
                    hostname="oldlab-2",
                    version="0.1",
                    capabilities=[],
                    registered_at=now,
                    last_seen_at=now,
                    status="active",
                ),
            ])
            s.commit()
    finally:
        engine.dispose()

    with TestClient(app) as client:
        created = client.post(
            "/admin/slurm-worker-jobs",
            headers={"Authorization": f"Bearer {RAW_ADMIN_TOKEN}"},
            json=_record_payload(job_id="201"),
        )
        assert created.status_code == 201, created.text
        reconciled = client.post(
            "/admin/slurm-worker-jobs/reconcile",
            headers={"Authorization": f"Bearer {RAW_ADMIN_TOKEN}"},
            json={
                "stale_after_seconds": 300,
                "observations": [{
                    "job_id": "201",
                    "slurm_state": "RUNNING",
                    "worker_id": str(first_worker_id),
                }],
            },
        )
        assert reconciled.status_code == 200, reconciled.text

    engine = create_engine(postgres_url)
    try:
        with sessionmaker(engine)() as s:
            job = s.execute(select(SlurmWorkerJob)).scalar_one()
            assert job.worker_id is None
            job.worker_id = first_worker_id
            s.commit()
    finally:
        engine.dispose()

    with TestClient(app) as client:
        reconciled = client.post(
            "/admin/slurm-worker-jobs/reconcile",
            headers={"Authorization": f"Bearer {RAW_ADMIN_TOKEN}"},
            json={
                "stale_after_seconds": 300,
                "observations": [{
                    "job_id": "201",
                    "slurm_state": "RUNNING",
                    "worker_id": str(second_worker_id),
                }],
            },
        )
        assert reconciled.status_code == 200, reconciled.text

    engine = create_engine(postgres_url)
    try:
        with sessionmaker(engine)() as s:
            job = s.execute(select(SlurmWorkerJob)).scalar_one()
            assert job.worker_id == first_worker_id
    finally:
        engine.dispose()


def test_reconcile_marks_missing_active_jobs_stale(
    app,
) -> None:
    with TestClient(app) as client:
        created = client.post(
            "/admin/slurm-worker-jobs",
            headers={"Authorization": f"Bearer {RAW_ADMIN_TOKEN}"},
            json=_record_payload(job_id="300"),
        )
        assert created.status_code == 201, created.text

        reconciled = client.post(
            "/admin/slurm-worker-jobs/reconcile",
            headers={"Authorization": f"Bearer {RAW_ADMIN_TOKEN}"},
            json={"stale_after_seconds": 0, "observations": []},
        )
        assert reconciled.status_code == 200, reconciled.text
        assert reconciled.json()["stale"] == 1

        status = client.get(
            "/admin/slurm-worker-jobs/status",
            headers={"Authorization": f"Bearer {RAW_ADMIN_TOKEN}"},
        )
        assert status.status_code == 200, status.text
        body = status.json()

    assert body["jobs"][0]["state"] == "stale"
    assert "not reported" in body["jobs"][0]["pending_reason"]


def test_status_active_only_excludes_terminal_job_history(app) -> None:
    with TestClient(app) as client:
        completed = client.post(
            "/admin/slurm-worker-jobs",
            headers={"Authorization": f"Bearer {RAW_ADMIN_TOKEN}"},
            json=_record_payload(job_id="401"),
        )
        assert completed.status_code == 201, completed.text

        reconciled = client.post(
            "/admin/slurm-worker-jobs/reconcile",
            headers={"Authorization": f"Bearer {RAW_ADMIN_TOKEN}"},
            json={
                "stale_after_seconds": 300,
                "observations": [
                    {
                        "job_id": "401",
                        "slurm_state": "COMPLETED",
                        "nodelist": "oldlab-1",
                        "pending_reason": None,
                        "worker_id": None,
                    },
                ],
            },
        )
        assert reconciled.status_code == 200, reconciled.text

        pending = client.post(
            "/admin/slurm-worker-jobs",
            headers={"Authorization": f"Bearer {RAW_ADMIN_TOKEN}"},
            json=_record_payload(job_id="402"),
        )
        assert pending.status_code == 201, pending.text

        full_status = client.get(
            "/admin/slurm-worker-jobs/status",
            headers={"Authorization": f"Bearer {RAW_ADMIN_TOKEN}"},
        )
        assert full_status.status_code == 200, full_status.text

        active_status = client.get(
            "/admin/slurm-worker-jobs/status?active_only=true",
            headers={"Authorization": f"Bearer {RAW_ADMIN_TOKEN}"},
        )
        assert active_status.status_code == 200, active_status.text

    assert {job["job_id"] for job in full_status.json()["jobs"]} == {"401", "402"}
    assert [job["job_id"] for job in active_status.json()["jobs"]] == ["402"]
    assert active_status.json()["summary"][0]["pending_jobs"] == 1
