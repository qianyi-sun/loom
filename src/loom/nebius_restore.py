"""Isolated PostgreSQL backup restore verification for the Nebius platform.

The worker downloads only a named backup, restores through a local Unix socket,
and checks representative records and existing canonical object references.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import sys
from pathlib import Path
from typing import Any
from uuid import UUID

import boto3
from botocore.config import Config

MAX_SNAPSHOT_BYTES = 2 * 1024 * 1024
DEFAULT_MAX_BACKUP_BYTES = 1024 * 1024 * 1024


class RestoreError(ValueError):
    """A short, safe failure code, never a database row or provider message."""


def validate_baseline(baseline: dict[str, Any]) -> None:
    if set(baseline) != {"schema_revision", "trials"}:
        raise RestoreError("invalid-baseline-fields")
    if re.fullmatch(r"[0-9]{4}", str(baseline.get("schema_revision", ""))) is None:
        raise RestoreError("invalid-schema-revision")
    trials = baseline.get("trials")
    if not isinstance(trials, list) or not 1 <= len(trials) <= 20:
        raise RestoreError("invalid-baseline-trials")
    ids = set()
    for trial in trials:
        if set(trial) != {
            "id",
            "state",
            "aggregate_reward",
            "llm_call_count",
            "input_tokens",
            "output_tokens",
            "artifact_count",
        }:
            raise RestoreError("invalid-baseline-trial-fields")
        identity = str(UUID(trial["id"]))
        if identity != trial["id"] or identity in ids or trial.get("state") != "succeeded":
            raise RestoreError("invalid-baseline-trial-identity")
        ids.add(identity)
        reward = trial["aggregate_reward"]
        if reward is not None and (
            isinstance(reward, bool)
            or not isinstance(reward, (int, float))
            or not math.isfinite(reward)
        ):
            raise RestoreError("invalid-baseline-reward")
        for field in ("llm_call_count", "input_tokens", "output_tokens", "artifact_count"):
            value = trial.get(field)
            minimum = 1 if field in {"llm_call_count", "artifact_count"} else 0
            if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
                raise RestoreError("invalid-baseline-record-count")


def snapshot_sql(trial_ids: list[str]) -> str:
    ids = ",".join("'" + str(UUID(value)) + "'::uuid" for value in trial_ids)
    if not 1 <= len(trial_ids) <= 20:
        raise RestoreError("invalid-trial-selection")
    return f"""SELECT jsonb_build_object(
  'schema_revision', (SELECT version_num FROM public.alembic_version),
  'trials', COALESCE((SELECT jsonb_agg(jsonb_build_object(
    'id', t.id, 'state', t.state, 'aggregate_reward', t.result->'aggregate_reward',
    'llm_call_count', (SELECT count(*) FROM public.llm_calls c WHERE c.trial_id=t.id),
    'input_tokens', (SELECT COALESCE(sum(input_tokens),0) FROM public.llm_calls c WHERE c.trial_id=t.id),
    'output_tokens', (SELECT COALESCE(sum(output_tokens),0) FROM public.llm_calls c WHERE c.trial_id=t.id),
    'artifact_count', (SELECT count(*) FROM public.artifacts a WHERE a.trial_id=t.id),
    'objects', COALESCE((SELECT jsonb_agg(jsonb_build_object(
      'id', a.id, 'files', a.storage->'files', 'source_evidence', a.storage->'source_evidence'
    ) ORDER BY a.id) FROM public.artifacts a WHERE a.trial_id=t.id), '[]'::jsonb),
    'trajectory_index', t.trajectory_index
  ) ORDER BY t.id) FROM public.trials t WHERE t.id IN ({ids})), '[]'::jsonb)
);
"""


RESTORE_SCRIPT = """#!/bin/sh
set -eu
umask 077
# Never inherit an ambient connection, service file, or client startup script.
unset PGHOST PGHOSTADDR PGPORT PGUSER PGDATABASE PGPASSWORD PGPASSFILE PGSERVICE PGSERVICEFILE PGOPTIONS
export HOME=/restore
phase=initdb
started=0
finish() {
    code=$?
    trap - EXIT
    if [ "$started" = 1 ]; then
        if pg_ctl -D /restore/pgdata -m immediate -w stop >/restore/stop.log 2>&1; then
            touch /restore/database-stopped
        else
            code=1
            phase=stop
        fi
    fi
    if [ "$code" != 0 ]; then
        printf '{"status":"failed","phase":"%s","exit_code":%s}\\n' "$phase" "$code"
    fi
    exit "$code"
}
trap finish EXIT
trap 'exit 143' TERM
trap 'exit 130' INT
mkdir /restore/socket
initdb -D /restore/pgdata --username=postgres --auth-local=trust --auth-host=reject >/restore/initdb.log 2>&1
phase=start
pg_ctl -D /restore/pgdata -l /restore/postgres.log -w start -o "-c listen_addresses='' -c unix_socket_directories=/restore/socket -c unix_socket_permissions=0700 -c shared_buffers=32MB -c max_connections=10 -c work_mem=4MB -c maintenance_work_mem=64MB" >/restore/start.log 2>&1
started=1
createdb --host=/restore/socket --username=postgres loom_restore >/restore/createdb.log 2>&1
phase=pg_restore
pg_restore --host=/restore/socket --username=postgres --dbname=loom_restore --no-owner --no-privileges --exit-on-error --jobs=1 /restore/loom.dump >/restore/pg_restore.log 2>&1
phase=records
psql --no-psqlrc --host=/restore/socket --username=postgres --dbname=loom_restore --set=ON_ERROR_STOP=1 --tuples-only --no-align --file=/code/records.sql >/restore/records.json 2>/restore/records.log
"""


def validate_backup_request(request: dict[str, Any]) -> None:
    key = request["backup_key"]
    namespace = request["namespace"]
    if not isinstance(key, str) or not key.startswith(namespace + "/") or not key.endswith(".dump"):
        raise RestoreError("backup-target-mismatch")
    if any(part in {"", ".", ".."} for part in key.split("/")):
        raise RestoreError("invalid-backup-key")
    maximum = request["max_backup_bytes"]
    if (
        isinstance(maximum, bool)
        or not isinstance(maximum, int)
        or not 1 <= maximum <= 4 * DEFAULT_MAX_BACKUP_BYTES
    ):
        raise RestoreError("invalid-backup-limit")


def download_backup(request: dict[str, Any], client: Any, workdir: Path) -> dict[str, Any]:
    validate_backup_request(request)
    key, maximum = request["backup_key"], request["max_backup_bytes"]
    target = {"Bucket": request["buckets"]["backup"], "Key": key}
    if request.get("backup_version_id"):
        target["VersionId"] = request["backup_version_id"]
    head = client.head_object(**target)
    size = head["ContentLength"]
    if not 0 < size <= maximum:
        raise RestoreError("backup-size-limit")
    # Same case-insensitive, unambiguous native S3 metadata as upload_backup.
    checksums = [
        value for name, value in head.get("Metadata", {}).items() if name.lower() == "sha256"
    ]
    if len(checksums) != 1 or re.fullmatch(r"[0-9a-f]{64}", checksums[0]) is None:
        raise RestoreError("backup-checksum-metadata")
    if head.get("VersionId"):
        target["VersionId"] = head["VersionId"]
    body = client.get_object(**target)["Body"]
    checksum = hashlib.sha256()
    received = 0
    path = workdir / "loom.dump"
    if path.exists():
        body.close()
        raise RestoreError("backup-workdir-not-empty")
    created = False
    try:
        with path.open("xb") as output:
            created = True
            for chunk in iter(lambda: body.read(1024 * 1024), b""):
                received += len(chunk)
                if received > size:
                    raise RestoreError("backup-content-mismatch")
                checksum.update(chunk)
                output.write(chunk)
        if received != size or checksum.hexdigest() != checksums[0]:
            raise RestoreError("backup-content-mismatch")
    except Exception:
        if created:
            path.unlink(missing_ok=True)
        raise
    finally:
        body.close()
    metadata = {
        "key": key,
        "version_id": target.get("VersionId"),
        "bytes": size,
        "sha256": checksums[0],
    }
    (workdir / "backup.json").write_text(json.dumps(metadata))
    return metadata


def verify_restored_records(
    request: dict[str, Any], snapshot: dict[str, Any], client: Any
) -> dict[str, Any]:
    baseline = request["baseline"]
    validate_baseline(baseline)
    if snapshot.get("schema_revision") != baseline["schema_revision"]:
        raise RestoreError("restored-schema-mismatch")
    expected = {row["id"]: row for row in baseline["trials"]}
    rows = snapshot.get("trials", [])
    if len(rows) != len(expected) or {row["id"] for row in rows} != set(expected):
        raise RestoreError("restored-trials-mismatch")
    allowed_buckets = {request["buckets"]["artifacts"], request["buckets"]["trajectories"]}
    references: dict[tuple[str, str, str | None], int] = {}
    for row in rows:
        for field in (
            "state",
            "aggregate_reward",
            "llm_call_count",
            "input_tokens",
            "output_tokens",
            "artifact_count",
        ):
            if row.get(field) != expected[row["id"]].get(field):
                raise RestoreError("restored-records-mismatch")
        objects = row.get("objects", [])
        if len(objects) != row["artifact_count"]:
            raise RestoreError("restored-artifact-records-mismatch")
        for artifact in objects:
            files = artifact.get("files")
            if not isinstance(files, list) or not files:
                raise RestoreError("restored-artifact-files-missing")
            for ref in [*files, *(artifact.get("source_evidence") or [])]:
                bucket, key = ref.get("bucket"), ref.get("key")
                if bucket not in allowed_buckets or not isinstance(key, str) or not key:
                    raise RestoreError("canonical-reference-target-mismatch")
                references[(bucket, key, ref.get("version_id"))] = ref["size_bytes"]
        index = row.get("trajectory_index") or {}
        for kind in ("trajectory", "atif"):
            prefix = "s3://" + request["buckets"]["trajectories"] + "/"
            uri = index.get(kind + "_uri", "")
            if not uri.startswith(prefix) or not uri.removeprefix(prefix):
                raise RestoreError("canonical-trajectory-target-mismatch")
            references[
                (
                    request["buckets"]["trajectories"],
                    uri.removeprefix(prefix),
                    index.get(kind + "_version_id"),
                )
            ] = index[kind + "_size_bytes"]
    if not 1 <= len(references) <= 2000:
        raise RestoreError("canonical-reference-limit")
    for (bucket, key, version), size in references.items():
        if isinstance(size, bool) or not isinstance(size, int) or size < 0:
            raise RestoreError("canonical-reference-size-invalid")
        target = {"Bucket": bucket, "Key": key}
        if version:
            target["VersionId"] = version
        head = client.head_object(**target)
        if head["ContentLength"] != size:
            raise RestoreError("canonical-reference-size-mismatch")
    return {
        "schema_revision": baseline["schema_revision"],
        "trial_count": len(rows),
        "llm_call_count": sum(row["llm_call_count"] for row in rows),
        "artifact_count": sum(row["artifact_count"] for row in rows),
        "canonical_object_count": len(references),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=("download", "verify", "baseline-sql"))
    parser.add_argument("--request", type=Path, default=Path("/code/request.json"))
    parser.add_argument("--trial-id", action="append", default=[])
    args = parser.parse_args()
    try:
        if args.phase == "baseline-sql":
            query = snapshot_sql(args.trial_id).rstrip().removesuffix(";")
            print(
                "SELECT jsonb_build_object('schema_revision', snapshot->'schema_revision', "
                "'trials', (SELECT jsonb_agg(row - 'objects' - 'trajectory_index') "
                "FROM jsonb_array_elements(snapshot->'trials') row)) FROM ("
                + query
                + ") source(snapshot);"
            )
            return 0
        request = json.loads(args.request.read_text())
        client = boto3.client(
            "s3",
            endpoint_url=request["storage_endpoint"],
            region_name=request["region"],
            aws_access_key_id=os.environ["LOOM_RESTORE_ACCESS_KEY"],
            aws_secret_access_key=os.environ["LOOM_RESTORE_SECRET_KEY"],
            config=Config(connect_timeout=10, read_timeout=30, retries={"max_attempts": 2}),
        )
        workdir = Path("/restore")
        if args.phase == "download":
            download_backup(request, client, workdir)
        else:
            if not (workdir / "database-stopped").is_file():
                raise RestoreError("local-database-not-stopped")
            records = workdir / "records.json"
            if records.stat().st_size > MAX_SNAPSHOT_BYTES:
                raise RestoreError("restored-snapshot-size-limit")
            result = verify_restored_records(request, json.loads(records.read_text()), client)
            print(
                json.dumps(
                    {
                        "status": "verified",
                        **result,
                        "backup": json.loads((workdir / "backup.json").read_text()),
                        "local_database_stopped": True,
                    }
                )
            )
        return 0
    except Exception as exc:
        code = str(exc) if isinstance(exc, RestoreError) else type(exc).__name__
        print(json.dumps({"status": "failed", "phase": args.phase, "error_code": code}))
        return 1


if __name__ == "__main__":
    sys.exit(main())
