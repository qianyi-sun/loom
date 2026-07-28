"""CI-safe A3 cross-sandbox credential / data crossover negatives.

Two synthetic control-plane stacks (separate DBs + admin secrets + worker
token tables) and one shared MinIO prove that foreign worker/admin tokens and
foreign object-store credentials are rejected without contacting oldlab-2.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID, uuid4

import boto3
import pytest
from alembic import command
from alembic.config import Config as AlembicConfig
from botocore.exceptions import ClientError
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, insert, text
from sqlalchemy.engine import make_url
from sqlalchemy.orm import sessionmaker
from testcontainers.minio import MinioContainer

from loom.db.schema import Task, Team, TeamQuota, Token, Trial, Worker
from loom_control_plane.app import create_app
from loom_control_plane.config import ControlPlaneSettings

ADMIN_A = "loom_admin_" + ("A" * 43)
ADMIN_B = "loom_admin_" + ("B" * 43)
REPO_ROOT = Path(__file__).resolve().parents[2]

_LINUX_PUBLIC_CAP = {
    "os": "linux",
    "gpu_vendor": "none",
    "network_policies": ["public"],
    "dynamic_network_policy": True,
    "mounted_fs": True,
    "resource_modes": ["auto"],
}


def _create_isolated_db(postgres_url: str) -> tuple[str, str]:
    source_url = make_url(postgres_url)
    database_name = f"loom_crossover_{uuid4().hex}"
    admin_url = source_url.set(database="postgres")
    admin_engine = create_engine(admin_url, isolation_level="AUTOCOMMIT")
    quoted = admin_engine.dialect.identifier_preparer.quote(database_name)
    with admin_engine.connect() as conn:
        conn.exec_driver_sql(
            f"CREATE DATABASE {quoted} TEMPLATE template0",
        )
    admin_engine.dispose()
    isolated = source_url.set(database=database_name).render_as_string(
        hide_password=False,
    )
    cfg = AlembicConfig(str(REPO_ROOT / "migrations" / "alembic.ini"))
    cfg.set_main_option("script_location", str(REPO_ROOT / "migrations"))
    cfg.set_main_option("sqlalchemy.url", isolated)
    command.upgrade(cfg, "head")
    return isolated, database_name


def _drop_isolated_db(postgres_url: str, database_name: str) -> None:
    source_url = make_url(postgres_url)
    admin_url = source_url.set(database="postgres")
    admin_engine = create_engine(admin_url, isolation_level="AUTOCOMMIT")
    quoted = admin_engine.dialect.identifier_preparer.quote(database_name)
    try:
        with admin_engine.connect() as conn:
            conn.execute(
                text(
                    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                    "WHERE datname = :db AND pid <> pg_backend_pid()",
                ),
                {"db": database_name},
            )
            conn.exec_driver_sql(f"DROP DATABASE IF EXISTS {quoted}")
    finally:
        admin_engine.dispose()


def _seed_stack(db_url: str, *, admin_token: str) -> tuple[UUID, str]:
    engine = create_engine(db_url)
    session_factory = sessionmaker(engine)
    team_id = uuid4()
    worker_id = uuid4()
    raw_worker = f"loom_w_{uuid4().hex}"
    with session_factory() as session:
        session.execute(insert(Team).values(id=team_id, name=f"t-{team_id}"))
        session.execute(insert(TeamQuota).values(team_id=team_id))
        session.execute(
            insert(Token).values(
                token_hash=hashlib.sha256(raw_worker.encode()).digest(),
                type="worker",
                scopes=["worker:claim", "worker:report"],
                team_id=None,
                issued_at=datetime.now(UTC),
                expires_at=None,
            ),
        )
        session.execute(insert(Task).values(id=f"t-{team_id}", checksum="0" * 64, config={}))
        session.execute(
            insert(Trial).values(
                id=uuid4(),
                team_id=team_id,
                task_id=f"t-{team_id}",
                config={},
                requires_caps={
                    "os": "linux",
                    "gpu_vendor": "none",
                    "network_policies": ["public"],
                },
                state="queued",
            ),
        )
        session.execute(
            insert(Worker).values(
                id=worker_id,
                hostname=f"h-{worker_id}",
                version="v",
                capabilities=[_LINUX_PUBLIC_CAP],
                registered_at=datetime.now(UTC),
                last_seen_at=datetime.now(UTC),
                status="active",
            ),
        )
        session.commit()
    engine.dispose()
    _ = admin_token  # admin lives in secret file, not DB
    return worker_id, raw_worker


def _write_admin_secret(path: Path, token: str) -> None:
    path.write_text(
        f'[admin]\ntoken = "{token}"\ncreated_at = "2026-07-28T00:00:00Z"\nversion = 1\n',
        encoding="utf-8",
    )
    path.chmod(0o600)


def _make_app(
    monkeypatch: pytest.MonkeyPatch,
    *,
    db_url: str,
    admin_secret: Path,
    minio_endpoint: str,
    minio_access: str,
    minio_secret: str,
) -> object:
    for key, value in {
        "LOOM_CP_DB_URL": db_url,
        "LOOM_CP_MINIO_ENDPOINT": minio_endpoint,
        "LOOM_CP_MINIO_ACCESS_KEY": minio_access,
        "LOOM_CP_MINIO_SECRET_KEY": minio_secret,
        "LOOM_CP_LLM_GATEWAY_URL": "http://gw:9100/",
        "LOOM_CP_ADMIN_SECRET_FILE": str(admin_secret),
    }.items():
        monkeypatch.setenv(key, value)
    return create_app(ControlPlaneSettings(_env_file=None))


@pytest.fixture
def dual_cp_stacks(
    monkeypatch: pytest.MonkeyPatch,
    postgres_url: str,
    tmp_path: Path,
) -> Iterator[dict[str, object]]:
    db_a, name_a = _create_isolated_db(postgres_url)
    db_b, name_b = _create_isolated_db(postgres_url)
    secret_a = tmp_path / "admin-a.toml"
    secret_b = tmp_path / "admin-b.toml"
    _write_admin_secret(secret_a, ADMIN_A)
    _write_admin_secret(secret_b, ADMIN_B)
    worker_a_id, worker_a_token = _seed_stack(db_a, admin_token=ADMIN_A)
    worker_b_id, worker_b_token = _seed_stack(db_b, admin_token=ADMIN_B)

    app_a = _make_app(
        monkeypatch,
        db_url=db_a,
        admin_secret=secret_a,
        minio_endpoint="http://minio-a:9000",
        minio_access="a",
        minio_secret="a",
    )
    # Recreate env for B after A so settings are independent per app build.
    app_b = _make_app(
        monkeypatch,
        db_url=db_b,
        admin_secret=secret_b,
        minio_endpoint="http://minio-b:9000",
        minio_access="b",
        minio_secret="b",
    )
    try:
        yield {
            "app_a": app_a,
            "app_b": app_b,
            "worker_a_id": worker_a_id,
            "worker_a_token": worker_a_token,
            "worker_b_id": worker_b_id,
            "worker_b_token": worker_b_token,
        }
    finally:
        _drop_isolated_db(postgres_url, name_a)
        _drop_isolated_db(postgres_url, name_b)


def test_foreign_worker_token_rejected_on_claim_register_heartbeat(
    dual_cp_stacks: dict[str, object],
) -> None:
    app_a = dual_cp_stacks["app_a"]
    app_b = dual_cp_stacks["app_b"]
    worker_a_id = dual_cp_stacks["worker_a_id"]
    worker_a_token = dual_cp_stacks["worker_a_token"]
    worker_b_id = dual_cp_stacks["worker_b_id"]
    worker_b_token = dual_cp_stacks["worker_b_token"]
    assert isinstance(worker_a_token, str)
    assert isinstance(worker_b_token, str)
    assert isinstance(worker_a_id, UUID)
    assert isinstance(worker_b_id, UUID)

    with TestClient(app_a) as client_a, TestClient(app_b) as client_b:
        own = client_a.post(
            "/trials/claim",
            headers={"Authorization": f"Bearer {worker_a_token}"},
            json={"worker_id": str(worker_a_id), "caps": [_LINUX_PUBLIC_CAP]},
        )
        assert own.status_code == 200, own.text

        foreign_claim = client_b.post(
            "/trials/claim",
            headers={"Authorization": f"Bearer {worker_a_token}"},
            json={"worker_id": str(worker_a_id), "caps": [_LINUX_PUBLIC_CAP]},
        )
        assert foreign_claim.status_code == 401

        foreign_register = client_b.post(
            "/workers/register",
            headers={"Authorization": f"Bearer {worker_a_token}"},
            json={
                "hostname": "foreign-host",
                "version": "0.1",
                "capabilities": [_LINUX_PUBLIC_CAP],
            },
        )
        assert foreign_register.status_code == 401

        foreign_heartbeat = client_b.post(
            f"/workers/{worker_b_id}/heartbeat",
            headers={"Authorization": f"Bearer {worker_a_token}"},
        )
        assert foreign_heartbeat.status_code == 401

        # Positive control: B's own token still works on B.
        own_b = client_b.post(
            f"/workers/{worker_b_id}/heartbeat",
            headers={"Authorization": f"Bearer {worker_b_token}"},
        )
        assert own_b.status_code == 200, own_b.text


def test_foreign_admin_token_rejected_on_worker_token_mint(
    dual_cp_stacks: dict[str, object],
) -> None:
    app_b = dual_cp_stacks["app_b"]
    with TestClient(app_b) as client_b:
        foreign = client_b.post(
            "/admin/worker-tokens",
            headers={"Authorization": f"Bearer {ADMIN_A}"},
            json={"expires_in_days": 1},
        )
        # Foreign admin must be rejected (401 unauthenticated or 403 forbidden).
        assert foreign.status_code in {401, 403}, foreign.text

        own = client_b.post(
            "/admin/worker-tokens",
            headers={"Authorization": f"Bearer {ADMIN_B}"},
            json={"expires_in_days": 1},
        )
        assert own.status_code in {200, 201}, own.text
        body = own.json()
        assert "token" in body
        # Response may contain a raw token; do not assert its value into logs.
        assert body["token"].startswith("loom_w_")


def test_minio_foreign_creds_and_foreign_bucket_rejected(
    shared_minio: MinioContainer,
) -> None:
    cfg = shared_minio.get_config()
    endpoint = f"http://{cfg['endpoint']}"
    access = cfg["access_key"]
    secret = cfg["secret_key"]
    client = shared_minio.get_client()
    own_bucket = f"loom-sandbox-a-{uuid4().hex[:8]}"
    foreign_bucket = f"loom-sandbox-b-{uuid4().hex[:8]}"
    client.make_bucket(own_bucket)

    good = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access,
        aws_secret_access_key=secret,
        region_name="us-east-1",
    )
    good.put_object(Bucket=own_bucket, Key="results/ok.txt", Body=b"ok")
    listed = good.list_objects_v2(Bucket=own_bucket, MaxKeys=1)
    assert listed["KeyCount"] == 1

    bad = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id="foreign-access-key",
        aws_secret_access_key="foreign-secret-key",
        region_name="us-east-1",
    )
    with pytest.raises(ClientError) as foreign_creds:
        bad.list_objects_v2(Bucket=own_bucket, MaxKeys=1)
    code = foreign_creds.value.response["Error"]["Code"]
    assert code in {"InvalidAccessKeyId", "AccessDenied", "InvalidArgument"}

    with pytest.raises(ClientError) as foreign_name:
        good.list_objects_v2(Bucket=foreign_bucket, MaxKeys=1)
    assert foreign_name.value.response["Error"]["Code"] == "NoSuchBucket"

    # Foreign artifact/result key prefix under a nonexistent peer namespace.
    with pytest.raises(ClientError) as foreign_path:
        good.get_object(Bucket=own_bucket, Key="peer-sandbox/results/secret.txt")
    assert foreign_path.value.response["Error"]["Code"] in {"NoSuchKey", "404"}
