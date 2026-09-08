"""Actual empty PostgreSQL bootstrap, repeated migration and role boundaries."""

from __future__ import annotations

import hashlib
import io
import json
import os
import sys
from collections.abc import Iterator
from pathlib import Path

import psycopg
import pytest
from sqlalchemy.engine import make_url
from testcontainers.postgres import PostgresContainer

from loom import nebius_platform_bootstrap as bootstrap
from tests.unit.test_nebius_platform_render import platform_inputs  # noqa: F401


@pytest.fixture(scope="module")
def platform_database() -> Iterator[str]:
    with PostgresContainer("postgres:16", dbname="loom") as postgres:
        yield (
            make_url(postgres.get_connection_url())
            .set(drivername="postgresql")
            .render_as_string(hide_password=False)
        )


def test_fresh_bootstrap_repeat_and_database_privileges(
    platform_database: str,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    request: pytest.FixtureRequest,
) -> None:
    monkeypatch.setattr(bootstrap, "database_url", lambda _value, _namespace: platform_database)
    monkeypatch.setenv("LOOM_DB_URL", platform_database)
    token = "loom_ecc_" + "f" * 64
    monkeypatch.setenv("LOOM_COLLECTOR_TOKEN", token)
    for role in ("SERVICE", "CONTROL_PLANE", "GATEWAY", "ACTUATOR"):
        monkeypatch.setenv("LOOM_DB_" + role + "_PASSWORD", "test-password-" + role + "-" * 30)
    config = {"namespace": "loom-nebius-platform"}
    bootstrap.bootstrap_database(config)
    bootstrap.bootstrap_database(config)
    with psycopg.connect(platform_database) as connection:
        row = connection.execute(
            "SELECT count(*) FROM tokens WHERE token_hash=%s",
            (hashlib.sha256(token.encode()).digest(),),
        ).fetchone()
        assert row == (1,)
        roles = connection.execute(
            "SELECT rolname, rolsuper, rolcreatedb, rolcreaterole, rolbypassrls FROM pg_roles WHERE rolname = ANY(%s) ORDER BY rolname",
            (["loom_service", "loom_control_plane", "loom_gateway", "loom_actuator"],),
        ).fetchall()
        assert len(roles) == 4 and all(not any(row[1:]) for row in roles)
    for role in ("gateway", "actuator"):
        url = make_url(platform_database).set(
            username="loom_" + role, password=os.environ["LOOM_DB_" + role.upper() + "_PASSWORD"]
        )
        with psycopg.connect(url.render_as_string(hide_password=False)) as connection:
            assert connection.execute("SELECT count(*) FROM execution_targets").fetchone() == (0,)
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                connection.execute("DELETE FROM users")
            connection.rollback()
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                connection.execute("CREATE ROLE bootstrap_escape")
    # Exercise actual CP HTTP handlers and actual database policy writes. Only
    # the transport is adapted from in-cluster HTTP to an in-process ASGI app.
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from loom.admin_secret import AdminSecretVerifier
    from loom.nebius_platform_render import build_platform
    from loom_control_plane.routes import admin, service_executions

    environment, candidate, profile = request.getfixturevalue("platform_inputs")
    files = build_platform(
        environment, candidate, profile, {}, repo_root=Path(__file__).resolve().parents[2]
    )
    data = files["10-config-network.yaml"][0]["data"]
    (tmp_path / "catalog.json").write_text(data["catalog.json"])
    admin_token = "loom_admin_" + "e" * 64
    admin_path = tmp_path / "secrets.toml"
    admin_path.write_text('[admin]\ntoken = "' + admin_token + '"\n')
    app = FastAPI()
    engine = create_async_engine(make_url(platform_database).set(drivername="postgresql+psycopg"))
    app.state.session_factory = async_sessionmaker(engine, expire_on_commit=False)
    app.state.admin_secret_verifier = AdminSecretVerifier.from_token(admin_token)
    app.include_router(admin.router)
    app.include_router(service_executions.router)
    with TestClient(app) as client:

        def open_request(request: object, timeout: int) -> io.BytesIO:
            from urllib.parse import urlsplit

            response = client.request(
                request.method,
                urlsplit(request.full_url).path,
                content=request.data,
                headers=dict(request.header_items()),
            )
            assert response.status_code == 200, response.text
            return io.BytesIO(response.content)

        monkeypatch.setattr(bootstrap.urllib.request, "urlopen", open_request)
        bootstrap.configure_platform(environment, config_dir=tmp_path, admin_secret=admin_path)
        bootstrap.configure_platform(environment, config_dir=tmp_path, admin_secret=admin_path)
        client.portal.call(engine.dispose)
    with psycopg.connect(platform_database) as connection:
        assert connection.execute(
            "SELECT desired_state, health_status FROM execution_targets WHERE id=%s",
            (environment["target_id"],),
        ).fetchone() == ("active", "unknown")
        assert connection.execute("SELECT count(*) FROM execution_price_snapshots").fetchone() == (
            1,
        )
        assert connection.execute(
            "SELECT enabled FROM execution_target_price_bindings WHERE target_id=%s",
            (environment["target_id"],),
        ).fetchone() == (True,)
    # A revoked collector identity must not be silently revived on upgrade.
    with psycopg.connect(platform_database) as connection:
        connection.execute(
            "UPDATE tokens SET revoked_at=now() WHERE token_hash=%s",
            (hashlib.sha256(token.encode()).digest(),),
        )
    with pytest.raises(ValueError, match="revoked"):
        bootstrap.bootstrap_database(config)
    # Real runtime SQL failure remains diagnosable without emitting URLs,
    # credential values, SQL parameters or driver exception bodies.
    with psycopg.connect(platform_database) as connection:
        connection.execute("DROP TABLE execution_events")
    config_path = tmp_path / "environment.json"
    config_path.write_text(json.dumps(config))
    monkeypatch.setenv("LOOM_PLATFORM_CONFIG", str(config_path))
    monkeypatch.setattr(sys, "argv", ["bootstrap", "database"])
    assert bootstrap.main() == 1
    diagnostic = json.loads(capsys.readouterr().err)
    assert diagnostic == {"phase": "database", "error_type": "UndefinedTable", "sqlstate": "42P01"}


@pytest.mark.parametrize(
    ("metadata", "size_delta", "accepted"),
    [
        ({"sha256": "match"}, 0, True),
        ({"Sha256": "match"}, 0, True),
        ({"SHA256": "match"}, 0, True),
        ({"Sha256": "mismatch"}, 0, False),
        ({}, 0, False),
        ({"Sha256": "match"}, 1, False),
        ({"sha256": "match", "Sha256": "mismatch"}, 0, False),
    ],
)
def test_backup_native_s3_metadata_case_preserves_hash_and_size_checks(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    metadata: dict[str, str],
    size_delta: int,
    accepted: bool,
) -> None:
    dump = tmp_path / "loom.dump"
    dump.write_bytes(b"bounded backup payload")
    checksum = hashlib.sha256(dump.read_bytes()).hexdigest()

    class Storage:
        def upload_file(self, filename: str, bucket: str, key: str, **kwargs: dict) -> None:
            assert filename == str(dump)
            assert bucket == "dedicated-backups"
            assert key.startswith("loom-nebius-platform/")
            assert kwargs["ExtraArgs"] == {"Metadata": {"sha256": checksum}}

        def head_object(self, **_kwargs: str) -> dict:
            return {
                "ContentLength": dump.stat().st_size + size_delta,
                "Metadata": {
                    key: checksum if value == "match" else "wrong"
                    for key, value in metadata.items()
                },
            }

    monkeypatch.setattr(bootstrap, "Path", lambda _path: dump)
    monkeypatch.setattr(bootstrap.boto3, "client", lambda *_args, **_kwargs: Storage())
    monkeypatch.setenv("LOOM_BACKUP_ACCESS_KEY", "fixture-access-key")
    monkeypatch.setenv("LOOM_BACKUP_SECRET_KEY", "fixture-secret-key")
    config = {
        "namespace": "loom-nebius-platform",
        "storage_endpoint": "https://storage.eu-north1.nebius.cloud",
        "region": "eu-north1",
        "buckets": {"backup": "dedicated-backups"},
    }
    if accepted:
        bootstrap.upload_backup(config)
        assert json.loads(capsys.readouterr().out)["sha256"] == checksum
    else:
        with pytest.raises(ValueError, match="readback mismatch"):
            bootstrap.upload_backup(config)
        assert not capsys.readouterr().out
