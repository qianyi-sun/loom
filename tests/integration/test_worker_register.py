"""Bug 5 regression: POST /workers/register validates `capabilities`
against the Capabilities Pydantic model (extra=forbid), so garbage like
typo'd OS or non-list payload is rejected at the boundary."""

import hashlib
from collections.abc import Iterator
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, delete, insert, text
from sqlalchemy.orm import sessionmaker

from loom.db.schema import Token, Worker
from loom_control_plane.app import create_app
from loom_control_plane.config import ControlPlaneSettings


@pytest.fixture
def worker_token(postgres_url: str) -> Iterator[str]:
    engine = create_engine(postgres_url)
    session_factory = sessionmaker(engine)
    raw = f"w_{uuid4().hex}"
    with session_factory() as s:
        s.execute(insert(Token).values(
            token_hash=hashlib.sha256(raw.encode()).digest(),
            type="worker", scopes=["worker:report"], team_id=None,
            issued_at=datetime.now(UTC), expires_at=None,
        ))
        s.commit()
    try:
        yield raw
    finally:
        with session_factory() as s:
            s.execute(delete(Worker))
            s.execute(delete(Token))
            s.commit()
        engine.dispose()


@pytest.fixture
def app(monkeypatch: pytest.MonkeyPatch, postgres_url: str, worker_token: str):
    for k, v in {
        "LOOM_CP_DB_URL": postgres_url,
        "LOOM_CP_MINIO_ENDPOINT": "http://minio:9000",
        "LOOM_CP_MINIO_ACCESS_KEY": "x",
        "LOOM_CP_MINIO_SECRET_KEY": "y",
        "LOOM_CP_LLM_GATEWAY_URL": "http://gw:9100/",
    }.items():
        monkeypatch.setenv(k, v)
    return create_app(ControlPlaneSettings(_env_file=None))


_VALID_CAP = {
    "os": "linux",
    "gpu_vendor": "none",
    "network_policies": ["public"],
    "dynamic_network_policy": False,
    "mounted_fs": False,
    "resource_modes": ["auto"],
}


def test_register_with_valid_capabilities(app, worker_token):  # type: ignore[no-untyped-def]
    with TestClient(app) as client:
        r = client.post(
            "/workers/register",
            headers={"Authorization": f"Bearer {worker_token}"},
            json={
                "hostname": "host-1",
                "version": "0.1",
                "capabilities": [_VALID_CAP],
            },
        )
        assert r.status_code == 200, r.text
        body = r.json()
        assert "worker_id" in body
        assert body["heartbeat_interval_sec"] > 0


def test_register_persists_worker_capacity_and_pool(
    app,
    worker_token,
    postgres_url: str,
):  # type: ignore[no-untyped-def]
    with TestClient(app) as client:
        r = client.post(
            "/workers/register",
            headers={"Authorization": f"Bearer {worker_token}"},
            json={
                "hostname": "trt-gb10-7",
                "version": "0.1",
                "capabilities": [_VALID_CAP],
                "max_concurrent": 10,
                "pool_name": "gb10",
            },
        )
        assert r.status_code == 200, r.text
        worker_id = r.json()["worker_id"]

    engine = create_engine(postgres_url)
    try:
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT max_concurrent, pool_name "
                    "FROM workers WHERE id = :worker_id"
                ),
                {"worker_id": worker_id},
            ).one()
    finally:
        engine.dispose()

    assert row[0] == 10
    assert row[1] == "gb10"


def test_register_persists_closed_input_cache_capacity_snapshot(
    app,
    worker_token,
    postgres_url: str,
):  # type: ignore[no-untyped-def]
    with TestClient(app) as client:
        response = client.post(
            "/workers/register",
            headers={"Authorization": f"Bearer {worker_token}"},
            json={
                "hostname": "cache-worker",
                "version": "0.1",
                "capabilities": [_VALID_CAP],
                "supported_work_kinds": ["trial", "execution_attempt"],
                "input_cache_capacity_bytes": 1_649_267_441_664,
                "input_cache_reserved_bytes": 4096,
                "input_cache_ready_bytes": 8192,
            },
        )
        assert response.status_code == 200, response.text
        worker_id = response.json()["worker_id"]

    engine = create_engine(postgres_url)
    try:
        with engine.connect() as connection:
            row = connection.execute(
                text(
                    "SELECT input_cache_capacity_bytes,input_cache_reserved_bytes,"
                    "input_cache_ready_bytes FROM workers WHERE id=:worker_id"
                ),
                {"worker_id": worker_id},
            ).one()
    finally:
        engine.dispose()

    assert tuple(row) == (1_649_267_441_664, 4096, 8192)


def test_register_rejects_partial_or_overcommitted_cache_snapshot(
    app, worker_token
):  # type: ignore[no-untyped-def]
    with TestClient(app) as client:
        partial = client.post(
            "/workers/register",
            headers={"Authorization": f"Bearer {worker_token}"},
            json={
                "capabilities": [_VALID_CAP],
                "input_cache_capacity_bytes": 100,
            },
        )
        overcommitted = client.post(
            "/workers/register",
            headers={"Authorization": f"Bearer {worker_token}"},
            json={
                "capabilities": [_VALID_CAP],
                "input_cache_capacity_bytes": 100,
                "input_cache_reserved_bytes": 101,
                "input_cache_ready_bytes": 0,
            },
        )

    assert partial.status_code == 400
    assert overcommitted.status_code == 400


def test_gpu_registration_requires_exact_slurm_pool_and_persists_evidence(
    app,
    worker_token,
    postgres_url: str,
):  # type: ignore[no-untyped-def]
    evidence = {
        "allocation_id": "gb10:123",
        "slurm_cluster_id": "gb10",
        "job_id": "123",
        "node_name": "trt-gb10-3",
        "partition": "gb10",
        "gpu_tres": "gpu:gb10:1",
        "allocated_device_ids": [0],
        "device_uuids": ["GPU-GB10"],
        "variant_id": "gb10-shared-1gpu",
    }
    snapshot = {
        "schema_version": "loom.worker-capabilities.v1",
        "cpu_arch": "arm64",
        "cpu_cores": 20,
        "memory_bytes": 128 << 30,
        "scratch_bytes": 200 << 30,
        "network_profiles": ["gateway", "none"],
        "container_runtime_features": [
            "egl",
            "loom-secret-tmpfs-v1",
            "nvidia-container-runtime",
        ],
        "gpu_devices": [
            {
                "allocation_id": "gb10:123",
                "device_uuid": "GPU-GB10",
                "vendor": "nvidia",
                "model": "NVIDIA GB10",
                "memory_kind": "unified",
                "memory_mb": None,
                "unified_memory_mb": 124_000,
                "nvidia_driver_version": "580.12.0",
                "mig_mode": "not_supported",
            }
        ],
        "input_cache_capacity_bytes": 1_649_267_441_664,
        "input_cache_reserved_bytes": 0,
        "input_cache_ready_bytes": 0,
    }
    payload = {
        "hostname": "trt-gb10-3",
        "version": "0.1",
        "capabilities": [
            {
                **_VALID_CAP,
                "cpu_arch": "arm64",
                "gpu_vendor": "nvidia",
                "network_policies": ["allowlist", "no-network"],
            }
        ],
        "supported_work_kinds": ["trial", "execution_attempt"],
        "max_concurrent": 1,
        "pool_name": "behavior-gpu-gb10",
        "capability_snapshot": snapshot,
        "slurm_gpu_allocation_evidence": evidence,
    }
    with TestClient(app) as client:
        rejected = client.post(
            "/workers/register",
            headers={"Authorization": f"Bearer {worker_token}"},
            json={**payload, "pool_name": "gb10"},
        )
        assert rejected.status_code == 409
        response = client.post(
            "/workers/register",
            headers={"Authorization": f"Bearer {worker_token}"},
            json=payload,
        )
        assert response.status_code == 200, response.text
        worker_id = response.json()["worker_id"]

    engine = create_engine(postgres_url)
    try:
        with engine.connect() as connection:
            row = connection.execute(
                text(
                    "SELECT capability_snapshot_json->>'cpu_arch',"
                    "slurm_gpu_allocation_evidence_json->>'allocation_id',"
                    "slurm_gpu_allocation_evidence_digest "
                    "FROM workers WHERE id=:worker_id"
                ),
                {"worker_id": worker_id},
            ).one()
    finally:
        engine.dispose()
    assert row[0] == "arm64"
    assert row[1] == "gb10:123"
    assert row[2].startswith("sha256:")


def test_register_rejects_missing_capabilities(app, worker_token):  # type: ignore[no-untyped-def]
    with TestClient(app) as client:
        r = client.post(
            "/workers/register",
            headers={"Authorization": f"Bearer {worker_token}"},
            json={"hostname": "host-2", "version": "0.1"},
        )
        assert r.status_code == 400
        assert "capabilities" in r.json()["detail"]


def test_register_rejects_empty_capabilities(app, worker_token):  # type: ignore[no-untyped-def]
    with TestClient(app) as client:
        r = client.post(
            "/workers/register",
            headers={"Authorization": f"Bearer {worker_token}"},
            json={"hostname": "h", "version": "v", "capabilities": []},
        )
        assert r.status_code == 400


def test_register_rejects_non_list_capabilities(app, worker_token):  # type: ignore[no-untyped-def]
    with TestClient(app) as client:
        r = client.post(
            "/workers/register",
            headers={"Authorization": f"Bearer {worker_token}"},
            json={
                "hostname": "h", "version": "v",
                "capabilities": "linux",
            },
        )
        assert r.status_code == 400


def test_register_rejects_typo_os(app, worker_token):  # type: ignore[no-untyped-def]
    """Bug 5 regression: a typo'd OS value would silently never match any
    DRF claim queries — must be caught at the boundary."""
    bad = dict(_VALID_CAP, os="lunix")
    with TestClient(app) as client:
        r = client.post(
            "/workers/register",
            headers={"Authorization": f"Bearer {worker_token}"},
            json={"hostname": "h", "version": "v", "capabilities": [bad]},
        )
        assert r.status_code == 400
        assert "invalid capabilities" in r.json()["detail"]


def test_register_rejects_extra_keys(app, worker_token):  # type: ignore[no-untyped-def]
    """Bug 5 regression: Capabilities is extra='forbid', so unknown keys
    are caught — protects against future fields drifting silently."""
    bad = dict(_VALID_CAP, hax="yes")
    with TestClient(app) as client:
        r = client.post(
            "/workers/register",
            headers={"Authorization": f"Bearer {worker_token}"},
            json={"hostname": "h", "version": "v", "capabilities": [bad]},
        )
        assert r.status_code == 400
