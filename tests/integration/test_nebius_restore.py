"""Real migrated DB / ACL-bearing dump / isolated socket restore / S3 records."""

from __future__ import annotations

import hashlib
import io
import json
import os
import subprocess
import sys
import tarfile
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import boto3
import docker
import pytest
from sqlalchemy import create_engine, insert, text
from sqlalchemy.engine import make_url
from testcontainers.core.wait_strategies import HttpWaitStrategy
from testcontainers.minio import MinioContainer
from testcontainers.postgres import PostgresContainer

from loom.db.schema import Artifact, LlmCall, Task, Team, Trial
from loom.nebius_restore import (
    RESTORE_SCRIPT,
    RestoreError,
    download_backup,
    snapshot_sql,
    verify_restored_records,
)
from tests.support.minio import MINIO_TEST_IMAGE
from tests.support.minio_images import prepare_test_image
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs

pytestmark = pytest.mark.docker


def archive(files):
    output = io.BytesIO()
    with tarfile.open(fileobj=output, mode="w") as tar:
        for name, data in files.items():
            info = tarfile.TarInfo(name)
            info.size, info.uid, info.gid, info.mode = len(data), 999, 999, 0o600
            tar.addfile(info, io.BytesIO(data))
    return output.getvalue()


def test_real_acl_dump_restores_without_source_roles_and_verifies_s3(tmp_path, monkeypatch):
    root = Path(__file__).resolve().parents[2]
    with (
        PostgresContainer("postgres:16") as source,
        MinioContainer(prepare_test_image(MINIO_TEST_IMAGE)).with_kwargs(tmpfs={"/data": "rw,size=536870912"}).waiting_for(
            HttpWaitStrategy(9000, "/minio/health/cluster")
        ) as storage,
    ):
        url = make_url(source.get_connection_url()).set(drivername="postgresql+psycopg")
        monkeypatch.setenv("LOOM_DB_URL", url.render_as_string(hide_password=False))
        subprocess.run(
            [sys.executable, "-m", "alembic", "-c", "database/migrations/alembic.ini", "upgrade", "head"],
            cwd=root,
            env=os.environ.copy(),
            check=True,
            capture_output=True,
        )
        cfg = storage.get_config()
        s3 = boto3.client(
            "s3",
            endpoint_url="http://" + cfg["endpoint"],
            aws_access_key_id=cfg["access_key"],
            aws_secret_access_key=cfg["secret_key"],
            region_name="us-east-1",
        )
        buckets = {name: "restore-" + name for name in ("backup", "artifacts", "trajectories")}
        for bucket in buckets.values():
            s3.create_bucket(Bucket=bucket)
            s3.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
        refs = {}
        for name, bucket in (
            ("output", buckets["artifacts"]),
            ("trajectory", buckets["trajectories"]),
            ("atif", buckets["trajectories"]),
        ):
            data = (name + "-saved-data").encode()
            version = s3.put_object(Bucket=bucket, Key=name, Body=data)["VersionId"]
            refs[name] = {
                "bucket": bucket,
                "key": name,
                "size_bytes": len(data),
                "version_id": version,
            }
        trajectory = {}
        for name in ("trajectory", "atif"):
            ref = refs[name]
            trajectory.update(
                {
                    name + "_uri": "s3://" + ref["bucket"] + "/" + name,
                    name + "_size_bytes": ref["size_bytes"],
                    name + "_version_id": ref["version_id"],
                }
            )
        engine = create_engine(url)
        trial, team = uuid4(), uuid4()
        try:
            with engine.begin() as db:
                db.execute(insert(Team).values(id=team, name="restore-local-test"))
                db.execute(
                    insert(Task).values(
                        id="restore-test", checksum="a" * 64, config={}, source="test"
                    )
                )
                db.execute(
                    insert(Trial).values(
                        id=trial,
                        team_id=team,
                        task_id="restore-test",
                        config={},
                        requires_caps={},
                        state="succeeded",
                        submitted_at=datetime.now(UTC),
                        result={"aggregate_reward": 0},
                        trajectory_index=trajectory,
                    )
                )
                db.execute(
                    insert(LlmCall).values(
                        id=uuid4(),
                        team_id=team,
                        trial_id=trial,
                        step_id="restore",
                        model="fixture",
                        dialect="openai",
                        input_tokens=5,
                        output_tokens=2,
                        cost_usd=0,
                        rate_card_hash="fixture",
                    )
                )
                db.execute(
                    insert(Artifact).values(
                        id=uuid4(),
                        trial_id=trial,
                        team_id=team,
                        artifact_type="file",
                        name="output",
                        content_hash="b" * 64,
                        storage={"files": [refs["output"]]},
                    )
                )
                # --no-owner alone still dumps ACLs referencing these source-only roles.
                for role in ("loom_service", "loom_control_plane", "loom_gateway", "loom_actuator"):
                    db.execute(text(f"CREATE ROLE {role} NOLOGIN"))
                    db.execute(text(f"GRANT SELECT ON public.trials TO {role}"))
                snapshot = db.execute(text(snapshot_sql([str(trial)]))).scalar_one()
                generated = subprocess.run(
                    [
                        sys.executable,
                        "-m",
                        "loom.nebius_restore",
                        "baseline-sql",
                        "--trial-id",
                        str(trial),
                    ],
                    cwd=root,
                    check=True,
                    capture_output=True,
                    text=True,
                ).stdout
                cli_baseline = db.execute(text(generated)).scalar_one()
        finally:
            engine.dispose()
        baseline = {
            "schema_revision": snapshot["schema_revision"],
            "trials": [
                {
                    key: value
                    for key, value in row.items()
                    if key not in {"objects", "trajectory_index"}
                }
                for row in snapshot["trials"]
            ],
        }
        assert baseline == cli_baseline
        pg = source.get_wrapped_container()
        dump = pg.exec_run(
            ["pg_dump", "-U", source.username, "-d", source.dbname, "-Fc", "--no-owner"]
        )
        assert dump.exit_code == 0
        key = "loom-nebius-platform/test.dump"
        s3.put_object(
            Bucket=buckets["backup"],
            Key=key,
            Body=dump.output,
            Metadata={"sha256": hashlib.sha256(dump.output).hexdigest()},
        )
        request = {
            "namespace": "loom-nebius-platform",
            "buckets": buckets,
            "backup_key": key,
            "max_backup_bytes": 16 * 1024 * 1024,
            "baseline": baseline,
        }
        download_backup(request, s3, tmp_path)
        client = docker.from_env()
        restored = client.containers.run(
            "postgres:16",
            command=["sleep", "300"],
            entrypoint=[],
            detach=True,
            user="999:999",
            read_only=True,
            network_mode="none",
            tmpfs={
                "/restore": "rw,uid=999,gid=999,size=536870912",
                "/code": "rw,uid=999,gid=999,size=1048576",
            },
        )
        try:
            for directory, files in (
                ("/restore", {"loom.dump": (tmp_path / "loom.dump").read_bytes()}),
                (
                    "/code",
                    {
                        "restore.sh": RESTORE_SCRIPT.encode(),
                        "records.sql": snapshot_sql([str(trial)]).encode(),
                    },
                ),
            ):
                subprocess.run(
                    ["docker", "exec", "-i", restored.id, "tar", "-x", "-C", directory],
                    input=archive(files),
                    check=True,
                    capture_output=True,
                )
            result = restored.exec_run(["sh", "/code/restore.sh"])
            if result.exit_code:
                # Private test temp files retain diagnostics, never dump rows into CI logs.
                for name in ("pg_restore", "records", "initdb", "start"):
                    log = restored.exec_run(["cat", "/restore/" + name + ".log"])
                    (tmp_path / (name + ".log")).write_bytes(log.output)
            assert result.exit_code == 0, result.output.decode()
            assert restored.exec_run(["test", "-f", "/restore/database-stopped"]).exit_code == 0
            actual = json.loads(restored.exec_run(["cat", "/restore/records.json"]).output)
            assert actual == snapshot
            summary = verify_restored_records(request, actual, s3)
            assert (
                summary["trial_count"]
                == summary["llm_call_count"]
                == summary["artifact_count"]
                == 1
            )
            assert summary["canonical_object_count"] == 3
            # Changed canonical contents fail without GET or alternative endpoints.
            changed = json.loads(json.dumps(actual))
            changed["trials"][0]["objects"][0]["files"][0]["size_bytes"] += 1
            with pytest.raises(RestoreError, match="canonical-reference-size-mismatch"):
                verify_restored_records(request, changed, s3)
        finally:
            restored.remove(force=True)
            client.close()


def test_management_backup_restores_two_owner_identities_and_environment_registry(tmp_path, monkeypatch, platform_inputs):
    from scripts.ops.nebius_management_proofs import verify_backup_object

    from loom import nebius_platform_bootstrap as bootstrap
    from loom.db.nebius_environment_schema import NebiusEnvironment, NebiusPlatformBudget
    from loom.db.schema import TeamMembership, User
    from loom.nebius_environment_contract import new_environment_registration
    from loom_service.password_auth import hash_password, verify_password
    from tests.unit.test_nebius_environment_contract import foundation_from

    tables = ("users", "teams", "team_memberships", "nebius_environments", "nebius_platform_budgets")
    snapshot = "SELECT jsonb_build_object('schema', (SELECT version_num FROM alembic_version)," + ",".join(
        f"'{name}', (SELECT jsonb_agg(row ORDER BY row::text) FROM (SELECT to_jsonb(t) row FROM {name} t) q)"
        for name in tables) + ");"
    with (PostgresContainer("postgres:16", dbname="loom") as source,
          MinioContainer(prepare_test_image(MINIO_TEST_IMAGE)).with_kwargs(tmpfs={"/data": "rw,size=536870912"}).waiting_for(
              HttpWaitStrategy(9000, "/minio/health/cluster")) as storage):
        url = make_url(source.get_connection_url()).set(drivername="postgresql+psycopg")
        connection = url.set(drivername="postgresql").render_as_string(hide_password=False)
        monkeypatch.setattr(bootstrap, "database_url", lambda *_args: connection)
        monkeypatch.setenv("LOOM_DB_URL", connection)
        monkeypatch.setenv("LOOM_DB_SERVICE_PASSWORD", "management-restore-test-only-" + "x" * 24)
        bootstrap.bootstrap_management_database({"namespace": "loom-nebius-management"})
        foundation = foundation_from(platform_inputs[0])
        engine = create_engine(url)
        try:
            with engine.begin() as db:
                for name in ("alice", "bob"):
                    owner, team = uuid4(), uuid4()
                    db.execute(insert(Team).values(id=team, name=name))
                    db.execute(insert(User).values(id=owner, username=name, username_normalized=name, status="active",
                                                  password_hash=hash_password("restore-test-" + name)))
                    db.execute(insert(TeamMembership).values(team_id=team, user_id=owner, role="owner"))
                    registration = new_environment_registration(foundation, environment_id=uuid4(), incarnation=uuid4(),
                        owner_user_id=owner, owner_team_id=team, slug=name)
                    row = registration.model_dump()
                    db.execute(insert(NebiusEnvironment).values(**{key: row[key] for key in row if key in NebiusEnvironment.__table__.columns}))
                db.execute(insert(NebiusPlatformBudget).values(cluster_id=foundation.platform_config["cluster_id"],
                    cpu_millis=3000, memory_mib=8192, storage_mib=20480, ephemeral_storage_mib=32768))
                baseline = db.execute(text(snapshot)).scalar_one()
        finally:
            engine.dispose()
        dump = source.get_wrapped_container().exec_run(["pg_dump", "-U", source.username, "-d", source.dbname, "-Fc", "--no-owner"])
        assert dump.exit_code == 0
        digest = hashlib.sha256(dump.output).hexdigest()
        cfg = storage.get_config()
        s3 = boto3.client("s3", endpoint_url="http://" + cfg["endpoint"], aws_access_key_id=cfg["access_key"],
                          aws_secret_access_key=cfg["secret_key"], region_name="us-east-1")
        bucket, namespace = "management-recovery-test", "loom-nebius-management"
        s3.create_bucket(Bucket=bucket)
        s3.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={"Status": "Enabled"})
        key = namespace + "/2026/09/24/000000-" + digest[:12] + ".dump"
        s3.put_object(Bucket=bucket, Key=key, Body=dump.output, Metadata={"sha256": digest})
        proof = verify_backup_object(client=s3, bucket=bucket, namespace=namespace, job_uid=str(uuid4()),
            report={"backup_key": key, "sha256": digest, "bytes": len(dump.output)}, max_bytes=16 * 1024**2)
        request = {"namespace": namespace, "buckets": {"backup": bucket}, "backup_key": proof["key"], "max_backup_bytes": 16 * 1024**2}
        download_backup(request, s3, tmp_path)
        client = docker.from_env()
        restored = client.containers.run("postgres:16", command=["sleep", "300"], entrypoint=[], detach=True,
            user="999:999", read_only=True, network_mode="none",
            tmpfs={"/restore": "rw,uid=999,gid=999,size=536870912", "/code": "rw,uid=999,gid=999,size=1048576"})
        try:
            for directory, files in (("/restore", {"loom.dump": (tmp_path / "loom.dump").read_bytes()}),
                                     ("/code", {"restore.sh": RESTORE_SCRIPT.encode(), "records.sql": snapshot.encode()})):
                subprocess.run(["docker", "exec", "-i", restored.id, "tar", "-x", "-C", directory],
                               input=archive(files), check=True, capture_output=True)
            result = restored.exec_run(["sh", "/code/restore.sh"])
            assert result.exit_code == 0
            actual = json.loads(restored.exec_run(["cat", "/restore/records.json"]).output)
            assert actual == baseline
            assert {row["application_namespace"] for row in actual["nebius_environments"]} == {"loom-dev-alice", "loom-dev-bob"}
            assert len({row["owner_user_id"] for row in actual["nebius_environments"]}) == 2
            owners = {row["username"]: row for row in actual["users"] if row["username"] in {"alice", "bob"}}
            assert set(owners) == {"alice", "bob"}
            assert all(verify_password("restore-test-" + name, row["password_hash"]) for name, row in owners.items())
            assert restored.exec_run(["test", "-f", "/restore/database-stopped"]).exit_code == 0
        finally:
            restored.remove(force=True)
            client.close()
            s3.close()
