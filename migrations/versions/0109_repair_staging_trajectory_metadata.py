"""Repair exact staging trajectory metadata and failed-smoke object registrations.

Revision ID: 0109
Revises: 0108

The trajectory objects themselves already contain the final bytes.  A worker
retry reused local artifact bookkeeping and left PostgreSQL describing the
earlier bytes.  Two later failed smoke trials also uploaded exact attempt and
verifier objects before projection failed, leaving those objects unregistered.
This one-way migration repairs only the independently verified staging state.
It is deliberately fail closed for any partial, mixed, or otherwise drifted
state and never writes to object storage.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import Any

import sqlalchemy as sa
from alembic import op

revision = "0109"
down_revision = "0108"
branch_labels = None
depends_on = None

_TEAM_ID = "bbce1c49-8d6b-429c-a338-de37a6b533b7"
_BATCH_ID = "6d646469-6f67-4f9e-81f8-735ca90c2c76"
_ENVIRONMENT = "staging"
_NAMESPACE = "loom-staging"
_BUCKET = "trajectories"
_ORPHAN_BATCH_ID = "a918a140-b1c7-4093-ba02-1640c8a9e71c"


class _RepairTarget:
    __slots__ = (
        "artifact_id",
        "authority_id",
        "created_at",
        "expires_at",
        "new_sha256",
        "new_size_bytes",
        "object_id",
        "old_sha256",
        "old_size_bytes",
        "trial_id",
    )

    def __init__(
        self,
        *,
        trial_id: str,
        artifact_id: str,
        authority_id: str,
        object_id: str,
        old_sha256: str,
        old_size_bytes: int,
        new_sha256: str,
        new_size_bytes: int,
        created_at: str,
        expires_at: str,
    ) -> None:
        self.trial_id = trial_id
        self.artifact_id = artifact_id
        self.authority_id = authority_id
        self.object_id = object_id
        self.old_sha256 = old_sha256
        self.old_size_bytes = old_size_bytes
        self.new_sha256 = new_sha256
        self.new_size_bytes = new_size_bytes
        self.created_at = created_at
        self.expires_at = expires_at

    @property
    def object_key(self) -> str:
        return f"{_TEAM_ID}/{self.trial_id}/events.jsonl"


_TARGETS = (
    _RepairTarget(
        trial_id="0347026e-3322-4f88-ba71-3bf09ae68034",
        artifact_id="450edc6c-00b9-4544-ac58-dd714c1c9ddc",
        authority_id="ae6d8ea8-ede7-489d-81e6-d75d21d7e3ea",
        object_id="b8a7f492-78ee-4915-a7ee-174fdf060031",
        old_sha256="1c4bb4a2e7fa76687bc456082c473a2a98c7e6bda35e4de9846e3d210a1c4e1e",
        old_size_bytes=548555,
        new_sha256="857dc17e8ddd6bf56efe59bd30f2debd443509dfc2a120cc8b16ee869f0208c5",
        new_size_bytes=282275,
        created_at="2026-08-21T03:47:48.202812+00:00",
        expires_at="2026-08-28T03:47:48.202812+00:00",
    ),
    _RepairTarget(
        trial_id="193d6992-3522-41f0-bcb5-27411fdeaccd",
        artifact_id="62ea9240-b58f-48e3-9d64-7dbe14387c63",
        authority_id="b21627d0-9b05-4814-9087-e228b1e44af4",
        object_id="ff01e8ed-e215-4656-9703-147164f83a2e",
        old_sha256="32a9303e62cdc94dca24f66025b97d7f7f89e0287f293945fdc0b6cf7ee32510",
        old_size_bytes=213116,
        new_sha256="91b9566b6b60b1d794cf83366bd6c990968e08fdb90b7bd2619bc2ff1a87653e",
        new_size_bytes=189870,
        created_at="2026-08-21T03:50:45.397884+00:00",
        expires_at="2026-08-28T03:50:45.397884+00:00",
    ),
    _RepairTarget(
        trial_id="50394796-8537-400d-af62-112f2b2191cf",
        artifact_id="a355183e-af55-4066-aa2b-17f05f788348",
        authority_id="844ea300-78ba-4c70-bbb6-43e488cf279b",
        object_id="5f13c704-9744-4af9-962a-59edd3403b0c",
        old_sha256="bc5874903448cb5b3ddbba29165ca54422ca6b53d3100d78b60962e0069bacf1",
        old_size_bytes=630330,
        new_sha256="cb2460e17276bf62f95afdb33382fcadc6b2457d7df008959cd74b882457e5ea",
        new_size_bytes=490483,
        created_at="2026-08-21T03:24:36.650509+00:00",
        expires_at="2026-08-28T03:24:36.650509+00:00",
    ),
)


class _OrphanTrial:
    __slots__ = ("authority_id", "finished_at", "submitted_at", "trial_id")

    def __init__(
        self,
        *,
        trial_id: str,
        authority_id: str,
        submitted_at: str,
        finished_at: str,
    ) -> None:
        self.trial_id = trial_id
        self.authority_id = authority_id
        self.submitted_at = submitted_at
        self.finished_at = finished_at


_ORPHAN_TRIALS = (
    _OrphanTrial(
        trial_id="758f4c7c-2510-43e7-b9ae-9af0fcb31451",
        authority_id="f77e1999-a436-44c3-91c1-0c46e1b6dd7f",
        submitted_at="2026-08-24T20:09:33.132678+00:00",
        finished_at="2026-08-24T20:49:45.75801+00:00",
    ),
    _OrphanTrial(
        trial_id="a28be4d2-fc3d-4301-8177-d01f964cac97",
        authority_id="004d05b9-799f-4933-b7cc-10f5f2121177",
        submitted_at="2026-08-24T20:09:33.182307+00:00",
        finished_at="2026-08-24T20:49:45.75801+00:00",
    ),
)


class _OrphanObject:
    __slots__ = (
        "bucket",
        "content_sha256",
        "created_at",
        "size_bytes",
        "suffix",
        "trial_id",
    )

    def __init__(
        self,
        trial_id: str,
        bucket: str,
        suffix: str,
        content_sha256: str,
        size_bytes: int,
        created_at: str,
    ) -> None:
        self.trial_id = trial_id
        self.bucket = bucket
        self.suffix = suffix
        self.content_sha256 = content_sha256
        self.size_bytes = size_bytes
        self.created_at = created_at

    @property
    def authority_id(self) -> str:
        return next(
            trial.authority_id for trial in _ORPHAN_TRIALS if trial.trial_id == self.trial_id
        )

    @property
    def object_key(self) -> str:
        return f"{_TEAM_ID}/{self.trial_id}/{self.suffix}"


_ORPHAN_OBJECTS = (
    _OrphanObject(
        _ORPHAN_TRIALS[0].trial_id,
        "artifacts",
        "main/.loom/verifier/junit.xml",
        "5a82c0d9c0f02dd7fa228a7b588460136c54af8ffdf05108a17746431f08653e",
        336,
        "2026-08-24T20:37:18.058000+00:00",
    ),
    _OrphanObject(
        _ORPHAN_TRIALS[0].trial_id,
        "artifacts",
        "main/.loom/verifier/pytest.log",
        "4e62dcb444bed1b95b65c323a32f682337e0344556bb3b4277f5a5eab0e8209e",
        113,
        "2026-08-24T20:37:17.896000+00:00",
    ),
    _OrphanObject(
        _ORPHAN_TRIALS[0].trial_id,
        "artifacts",
        "main/.loom/verifier/pytest.log.meta.json",
        "696564789c10a527879e6966597c9c9e65aa920a2a2a999b78173e45db3a0ffb",
        328,
        "2026-08-24T20:37:17.992000+00:00",
    ),
    _OrphanObject(
        _ORPHAN_TRIALS[0].trial_id,
        "artifacts",
        "main/result.txt",
        "5891b5b522d5df086d0ff0b110fbd9d21bb4fc7163af34d08286a2e846f6be03",
        6,
        "2026-08-24T20:37:17.803000+00:00",
    ),
    _OrphanObject(
        _ORPHAN_TRIALS[0].trial_id,
        "trajectories",
        "attempts/1/atif.json",
        "89a9b6c1385cd7ca612451234b022c580def15cd9d68821a57973dd7300f05b3",
        704,
        "2026-08-24T20:11:53.288000+00:00",
    ),
    _OrphanObject(
        _ORPHAN_TRIALS[0].trial_id,
        "trajectories",
        "attempts/1/events.jsonl",
        "a28606071c5a680e54080f8fdd9231fa6b06f496ac5c010ff57998f6dbeab315",
        1282,
        "2026-08-24T20:11:53.194000+00:00",
    ),
    _OrphanObject(
        _ORPHAN_TRIALS[0].trial_id,
        "trajectories",
        "attempts/2/atif.json",
        "dc4fb8842ed57bafdc0fe5174411148e3389b2cd3a90f70fd1edf5f5eaff4cc7",
        704,
        "2026-08-24T20:24:26.920000+00:00",
    ),
    _OrphanObject(
        _ORPHAN_TRIALS[0].trial_id,
        "trajectories",
        "attempts/2/events.jsonl",
        "a6945344c9d73f345400013a36c029116346a3e8ddbdc88e17fb91fbab7150d2",
        1283,
        "2026-08-24T20:24:26.863000+00:00",
    ),
    _OrphanObject(
        _ORPHAN_TRIALS[0].trial_id,
        "trajectories",
        "attempts/3/atif.json",
        "89a9b6c1385cd7ca612451234b022c580def15cd9d68821a57973dd7300f05b3",
        704,
        "2026-08-24T20:37:18.470000+00:00",
    ),
    _OrphanObject(
        _ORPHAN_TRIALS[0].trial_id,
        "trajectories",
        "attempts/3/events.jsonl",
        "7114a3b7588b4b46e51727178b1c035854d15918883fae539fe42274ea69aa04",
        1282,
        "2026-08-24T20:37:18.398000+00:00",
    ),
    _OrphanObject(
        _ORPHAN_TRIALS[1].trial_id,
        "artifacts",
        "main/.loom/verifier/junit.xml",
        "404966d9ea82e8721bdf7b1601c891e86bf140bd3085ccd23bf759ce34cd4812",
        336,
        "2026-08-24T20:37:18.351000+00:00",
    ),
    _OrphanObject(
        _ORPHAN_TRIALS[1].trial_id,
        "artifacts",
        "main/.loom/verifier/pytest.log",
        "4e62dcb444bed1b95b65c323a32f682337e0344556bb3b4277f5a5eab0e8209e",
        113,
        "2026-08-24T20:37:17.899000+00:00",
    ),
    _OrphanObject(
        _ORPHAN_TRIALS[1].trial_id,
        "artifacts",
        "main/.loom/verifier/pytest.log.meta.json",
        "7486b1420ef6b466cc5b757fe2134964a458f9b0944dba015d56dd9262e2b598",
        326,
        "2026-08-24T20:37:18.247000+00:00",
    ),
    _OrphanObject(
        _ORPHAN_TRIALS[1].trial_id,
        "artifacts",
        "main/result.txt",
        "5891b5b522d5df086d0ff0b110fbd9d21bb4fc7163af34d08286a2e846f6be03",
        6,
        "2026-08-24T20:37:17.804000+00:00",
    ),
    _OrphanObject(
        _ORPHAN_TRIALS[1].trial_id,
        "trajectories",
        "attempts/1/atif.json",
        "45d72cf6e80268953341ac703c1649096e495fb608e527488d241c8b1fb8b5eb",
        704,
        "2026-08-24T20:11:53.157000+00:00",
    ),
    _OrphanObject(
        _ORPHAN_TRIALS[1].trial_id,
        "trajectories",
        "attempts/1/events.jsonl",
        "52e3759d33e1871725e2a9888ecd559a972a696aae057976538ecef858d63afd",
        1283,
        "2026-08-24T20:11:53.052000+00:00",
    ),
    _OrphanObject(
        _ORPHAN_TRIALS[1].trial_id,
        "trajectories",
        "attempts/2/atif.json",
        "9a255ad01e852169bfb641ea3edc44f923a11eee6291de169ac50043d6fb82c4",
        704,
        "2026-08-24T20:24:26.951000+00:00",
    ),
    _OrphanObject(
        _ORPHAN_TRIALS[1].trial_id,
        "trajectories",
        "attempts/2/events.jsonl",
        "4ccc6ed170b513cf6b3c459e9e2552c3a3af4f60da6467b6c83155a57bfbd3cb",
        1282,
        "2026-08-24T20:24:26.871000+00:00",
    ),
    _OrphanObject(
        _ORPHAN_TRIALS[1].trial_id,
        "trajectories",
        "attempts/3/atif.json",
        "45d72cf6e80268953341ac703c1649096e495fb608e527488d241c8b1fb8b5eb",
        704,
        "2026-08-24T20:37:18.790000+00:00",
    ),
    _OrphanObject(
        _ORPHAN_TRIALS[1].trial_id,
        "trajectories",
        "attempts/3/events.jsonl",
        "bb5e0f9da9ac1a32cccc417d2a6908a730a8da1061f0c13e8a1f26b6b6e4664a",
        1283,
        "2026-08-24T20:37:18.742000+00:00",
    ),
)

_ARTIFACT_NULL_FIELDS = (
    "project_id",
    "blocked_reason",
    "actor_user_id",
    "producer_kind",
    "manifest_sha256",
    "pipeline_run_id",
    "acceptance_action",
    "stored_size_bytes",
    "control_producer_id",
    "unpacked_size_bytes",
    "execution_attempt_id",
    "control_producer_kind",
    "pipeline_stage_run_id",
    "acceptance_result_kind",
    "pipeline_input_import_id",
    "artifact_upload_session_id",
    "acceptance_candidate_sha256",
    "acceptance_termination_reason",
    "profile_calibration_result_kind",
    "profile_calibration_run_ordinal",
    "profile_calibration_scenario_id",
    "profile_calibration_spec_sha256",
    "pipeline_input_materialization_id",
    "pipeline_acceptance_authorization_id",
    "profile_calibration_termination_reason",
    "profile_calibration_source_pipeline_run_id",
    "pipeline_profile_calibration_authorization_id",
    "profile_calibration_candidate_identity_sha256",
    "file_count",
)


def _artifact_snapshot(
    target: _RepairTarget,
    *,
    sha256: str,
    size_bytes: int,
) -> dict[str, Any]:
    snapshot: dict[str, Any] = dict.fromkeys(_ARTIFACT_NULL_FIELDS)
    snapshot.update(
        {
            "id": target.artifact_id,
            "artifact_type": "trajectory",
            "artifact_schema_version": "1.0",
            "name": "Trajectory events",
            "team_id": _TEAM_ID,
            "batch_id": _BATCH_ID,
            "trial_id": target.trial_id,
            "created_by": {
                "kind": "trial",
                "batch_id": _BATCH_ID,
                "trial_id": target.trial_id,
            },
            "content_hash": f"sha256:{sha256}",
            "storage": {
                "key": target.object_key,
                "bucket": _BUCKET,
                "backend": "object_store",
                "media_type": "application/x-ndjson",
                "size_bytes": size_bytes,
            },
            "visibility": "org",
            "share_status": "shared",
            "redaction_state": "not_required",
            "safety_state": "safe",
            "retention": {"class": "release_evidence", "expires_at": None},
            "provenance": {
                "batch_id": _BATCH_ID,
                "relation": "produced_from",
                "trial_id": target.trial_id,
                "source_trial_ids": [target.trial_id],
            },
            "metadata": {},
            "created_at": target.created_at,
            "access_class": "team_runtime",
            "lifecycle_authority_id": target.authority_id,
        }
    )
    return snapshot


def _authority_snapshot(target: _RepairTarget) -> dict[str, Any]:
    return {
        "id": target.authority_id,
        "environment": _ENVIRONMENT,
        "namespace": _NAMESPACE,
        "team_id": _TEAM_ID,
        "data_class": "artifact",
        "owner_kind": "artifact",
        "owner_id": target.artifact_id,
        "created_at": target.created_at,
        "expires_at": target.expires_at,
        "pinned": False,
        "state": "active",
        "deletion_token": None,
        "metadata": {},
    }


def _object_snapshot(
    target: _RepairTarget,
    *,
    sha256: str,
    size_bytes: int,
) -> dict[str, Any]:
    return {
        "id": target.object_id,
        "authority_id": target.authority_id,
        "environment": _ENVIRONMENT,
        "namespace": _NAMESPACE,
        "bucket": _BUCKET,
        "object_key": target.object_key,
        "version_id": None,
        "content_sha256": sha256,
        "size_bytes": size_bytes,
        "created_at": target.created_at,
        "state": "active",
        "deletion_token": None,
        "verified_deleted_at": None,
    }


def _expected_state(*, repaired: bool) -> dict[str, dict[str, dict[str, Any]]]:
    artifacts: dict[str, dict[str, Any]] = {}
    authorities: dict[str, dict[str, Any]] = {}
    objects: dict[str, dict[str, Any]] = {}
    for target in _TARGETS:
        sha256 = target.new_sha256 if repaired else target.old_sha256
        size_bytes = target.new_size_bytes if repaired else target.old_size_bytes
        artifacts[target.artifact_id] = _artifact_snapshot(
            target,
            sha256=sha256,
            size_bytes=size_bytes,
        )
        authorities[target.authority_id] = _authority_snapshot(target)
        objects[target.object_id] = _object_snapshot(
            target,
            sha256=sha256,
            size_bytes=size_bytes,
        )
    return {
        "artifacts": artifacts,
        "authorities": authorities,
        "objects": objects,
    }


def _load_rows(
    bind: sa.engine.Connection,
    *,
    table: str,
    ids: list[str],
) -> dict[str, dict[str, Any]]:
    rows = bind.execute(
        sa.text(
            f"SELECT to_jsonb(target) FROM {table} AS target "
            "WHERE id = ANY(CAST(:ids AS uuid[])) ORDER BY id"
        ),
        {"ids": ids},
    ).scalars()
    return {str(row["id"]): row for row in rows}


def _orphan_trial_snapshot(trial: _OrphanTrial) -> dict[str, Any]:
    return {
        "id": trial.trial_id,
        "team_id": _TEAM_ID,
        "batch_id": _ORPHAN_BATCH_ID,
        "state": "failed",
        "attempt_count": 3,
        "failure_reason": "retry_exhausted",
        "result": None,
        "trajectory_index": None,
        "worker_id": None,
        "submitted_at": trial.submitted_at,
        "finished_at": trial.finished_at,
        "lifecycle_authority_id": trial.authority_id,
    }


def _orphan_authority_snapshot(trial: _OrphanTrial) -> dict[str, Any]:
    submitted_at = datetime.fromisoformat(trial.submitted_at)
    return {
        "id": trial.authority_id,
        "environment": _ENVIRONMENT,
        "namespace": _NAMESPACE,
        "team_id": _TEAM_ID,
        "data_class": "trial",
        "owner_kind": "trial",
        "owner_id": trial.trial_id,
        "created_at": trial.submitted_at,
        "expires_at": (submitted_at + timedelta(days=7)).isoformat(),
        "pinned": False,
        "state": "active",
        "deletion_token": None,
        "metadata": {},
    }


def _postgres_json_timestamp(value: str) -> str:
    timestamp, offset = value.rsplit("+", 1)
    if "." in timestamp:
        timestamp = timestamp.rstrip("0").rstrip(".")
    return f"{timestamp}+{offset}"


def _orphan_object_snapshot(item: _OrphanObject) -> dict[str, Any]:
    return {
        "authority_id": item.authority_id,
        "environment": _ENVIRONMENT,
        "namespace": _NAMESPACE,
        "bucket": item.bucket,
        "object_key": item.object_key,
        "version_id": None,
        "content_sha256": item.content_sha256,
        "size_bytes": item.size_bytes,
        "created_at": _postgres_json_timestamp(item.created_at),
        "state": "active",
        "deletion_token": None,
        "verified_deleted_at": None,
    }


def _orphan_object_key(item: _OrphanObject | dict[str, Any]) -> str:
    if isinstance(item, _OrphanObject):
        bucket = item.bucket
        object_key = item.object_key
        version_id = None
    else:
        bucket = item["bucket"]
        object_key = item["object_key"]
        version_id = item["version_id"]
    return f"{bucket}\0{object_key}\0{version_id or ''}"


def _expected_orphan_state(*, repaired: bool) -> dict[str, Any]:
    return {
        "trials": {trial.trial_id: _orphan_trial_snapshot(trial) for trial in _ORPHAN_TRIALS},
        "authorities": {
            trial.authority_id: _orphan_authority_snapshot(trial) for trial in _ORPHAN_TRIALS
        },
        "artifact_count": 0,
        "objects": (
            {_orphan_object_key(item): _orphan_object_snapshot(item) for item in _ORPHAN_OBJECTS}
            if repaired
            else {}
        ),
    }


def _empty_orphan_state() -> dict[str, Any]:
    return {"trials": {}, "authorities": {}, "artifact_count": 0, "objects": {}}


def _load_orphan_state(
    bind: sa.engine.Connection,
) -> tuple[dict[str, Any], int, int]:
    trial_rows = bind.execute(
        sa.text(
            "SELECT jsonb_build_object("
            "'id',id,'team_id',team_id,'batch_id',batch_id,'state',state,"
            "'attempt_count',attempt_count,'failure_reason',failure_reason,"
            "'result',result,'trajectory_index',trajectory_index,"
            "'worker_id',worker_id,'submitted_at',submitted_at,'finished_at',finished_at,"
            "'lifecycle_authority_id',lifecycle_authority_id) "
            "FROM trials WHERE id = ANY(CAST(:ids AS uuid[])) ORDER BY id"
        ),
        {"ids": [trial.trial_id for trial in _ORPHAN_TRIALS]},
    ).scalars()
    authorities = _load_rows(
        bind,
        table="data_lifecycle_authorities",
        ids=[trial.authority_id for trial in _ORPHAN_TRIALS],
    )
    artifact_count = int(
        bind.execute(
            sa.text(
                "SELECT count(*) FROM artifacts WHERE trial_id = ANY(CAST(:trial_ids AS uuid[]))"
            ),
            {"trial_ids": [trial.trial_id for trial in _ORPHAN_TRIALS]},
        ).scalar_one()
    )
    prefixes = [f"{_TEAM_ID}/{trial.trial_id}/%" for trial in _ORPHAN_TRIALS]
    object_rows = bind.execute(
        sa.text(
            "SELECT to_jsonb(target) - 'id' FROM data_lifecycle_objects AS target "
            "WHERE authority_id = ANY(CAST(:authority_ids AS uuid[])) "
            "OR (bucket IN ('artifacts','trajectories') "
            "AND (object_key LIKE :first_prefix OR object_key LIKE :second_prefix)) "
            "ORDER BY bucket,object_key,COALESCE(version_id,'')"
        ),
        {
            "authority_ids": [trial.authority_id for trial in _ORPHAN_TRIALS],
            "first_prefix": prefixes[0],
            "second_prefix": prefixes[1],
        },
    ).scalars()
    objects = {_orphan_object_key(row): row for row in object_rows}
    state = {
        "trials": {str(row["id"]): row for row in trial_rows},
        "authorities": authorities,
        "artifact_count": artifact_count,
        "objects": objects,
    }
    gc_item_count = int(
        bind.execute(
            sa.text(
                "SELECT count(*) FROM data_lifecycle_gc_items AS item "
                "WHERE item.authority_id = ANY(CAST(:authority_ids AS uuid[])) "
                "OR item.object_id IN ("
                "SELECT object_row.id FROM data_lifecycle_objects AS object_row "
                "WHERE object_row.authority_id = ANY(CAST(:authority_ids AS uuid[])) "
                "OR (object_row.bucket IN ('artifacts','trajectories') "
                "AND (object_row.object_key LIKE :first_prefix "
                "OR object_row.object_key LIKE :second_prefix)))"
            ),
            {
                "authority_ids": [trial.authority_id for trial in _ORPHAN_TRIALS],
                "first_prefix": prefixes[0],
                "second_prefix": prefixes[1],
            },
        ).scalar_one()
    )
    gc_authority_count = int(
        bind.execute(
            sa.text(
                "SELECT count(*) FROM data_lifecycle_gc_authorities "
                "WHERE authority_id = ANY(CAST(:authority_ids AS uuid[]))"
            ),
            {"authority_ids": [trial.authority_id for trial in _ORPHAN_TRIALS]},
        ).scalar_one()
    )
    return state, gc_item_count, gc_authority_count


def _load_state(bind: sa.engine.Connection) -> tuple[dict[str, Any], int, int]:
    state = {
        "artifacts": _load_rows(
            bind,
            table="artifacts",
            ids=[target.artifact_id for target in _TARGETS],
        ),
        "authorities": _load_rows(
            bind,
            table="data_lifecycle_authorities",
            ids=[target.authority_id for target in _TARGETS],
        ),
        "objects": _load_rows(
            bind,
            table="data_lifecycle_objects",
            ids=[target.object_id for target in _TARGETS],
        ),
    }
    gc_item_count = int(
        bind.execute(
            sa.text(
                "SELECT count(*) FROM data_lifecycle_gc_items "
                "WHERE object_id = ANY(CAST(:object_ids AS uuid[])) "
                "OR authority_id = ANY(CAST(:authority_ids AS uuid[]))"
            ),
            {
                "object_ids": [target.object_id for target in _TARGETS],
                "authority_ids": [target.authority_id for target in _TARGETS],
            },
        ).scalar_one()
    )
    gc_authority_count = int(
        bind.execute(
            sa.text(
                "SELECT count(*) FROM data_lifecycle_gc_authorities "
                "WHERE authority_id = ANY(CAST(:authority_ids AS uuid[]))"
            ),
            {"authority_ids": [target.authority_id for target in _TARGETS]},
        ).scalar_one()
    )
    return state, gc_item_count, gc_authority_count


def _lock_target_tables(bind: sa.engine.Connection) -> None:
    bind.execute(
        sa.text(
            "LOCK TABLE artifacts, data_lifecycle_authorities, "
            "data_lifecycle_objects, data_lifecycle_gc_items, "
            "data_lifecycle_gc_authorities, trials "
            "IN SHARE ROW EXCLUSIVE MODE"
        )
    )


def upgrade() -> None:
    bind = op.get_bind()
    _lock_target_tables(bind)
    state, gc_item_count, gc_authority_count = _load_state(bind)
    orphan_state, orphan_gc_item_count, orphan_gc_authority_count = _load_orphan_state(bind)
    present_rows = sum(len(rows) for rows in state.values())
    metadata_absent = present_rows == 0
    metadata_repaired = state == _expected_state(repaired=True)
    expected_old = _expected_state(repaired=False)
    metadata_prerepair = state == expected_old
    orphan_absent = orphan_state == _empty_orphan_state()
    orphan_repaired = orphan_state == _expected_orphan_state(repaired=True)
    orphan_prerepair = orphan_state == _expected_orphan_state(repaired=False)
    if (
        not (metadata_absent or metadata_repaired or metadata_prerepair)
        or gc_item_count != 0
        or gc_authority_count != 0
        or not (orphan_absent or orphan_repaired or orphan_prerepair)
        or orphan_gc_item_count != 0
        or orphan_gc_authority_count != 0
    ):
        raise RuntimeError(
            "0109 lifecycle artifact metadata repair refused: "
            "target state is partial, mixed, or drifted"
        )

    if metadata_prerepair:
        for target in _TARGETS:
            old_artifact = json.dumps(expected_old["artifacts"][target.artifact_id], sort_keys=True)
            artifact_result = bind.execute(
                sa.text(
                    "UPDATE artifacts SET content_hash=:content_hash, "
                    "storage=jsonb_set(storage, '{size_bytes}', "
                    "to_jsonb(CAST(:size_bytes AS bigint)), false) "
                    "WHERE id=CAST(:id AS uuid) "
                    "AND to_jsonb(artifacts)=CAST(:old_snapshot AS jsonb)"
                ),
                {
                    "id": target.artifact_id,
                    "content_hash": f"sha256:{target.new_sha256}",
                    "size_bytes": target.new_size_bytes,
                    "old_snapshot": old_artifact,
                },
            )
            old_object = json.dumps(expected_old["objects"][target.object_id], sort_keys=True)
            object_result = bind.execute(
                sa.text(
                    "UPDATE data_lifecycle_objects "
                    "SET content_sha256=:content_sha256, size_bytes=:size_bytes "
                    "WHERE id=CAST(:id AS uuid) "
                    "AND to_jsonb(data_lifecycle_objects)=CAST(:old_snapshot AS jsonb)"
                ),
                {
                    "id": target.object_id,
                    "content_sha256": target.new_sha256,
                    "size_bytes": target.new_size_bytes,
                    "old_snapshot": old_object,
                },
            )
            if artifact_result.rowcount != 1 or object_result.rowcount != 1:
                raise RuntimeError(
                    "0109 lifecycle artifact metadata repair refused: "
                    "target state changed during the atomic repair"
                )

    if orphan_prerepair:
        for item in _ORPHAN_OBJECTS:
            result = bind.execute(
                sa.text(
                    "INSERT INTO data_lifecycle_objects ("
                    "authority_id,environment,namespace,bucket,object_key,version_id,"
                    "content_sha256,size_bytes,created_at,state,deletion_token,"
                    "verified_deleted_at) VALUES ("
                    "CAST(:authority_id AS uuid),:environment,:namespace,:bucket,"
                    ":object_key,NULL,:content_sha256,:size_bytes,"
                    "CAST(:created_at AS timestamptz),'active',NULL,NULL)"
                ),
                {
                    "authority_id": item.authority_id,
                    "environment": _ENVIRONMENT,
                    "namespace": _NAMESPACE,
                    "bucket": item.bucket,
                    "object_key": item.object_key,
                    "content_sha256": item.content_sha256,
                    "size_bytes": item.size_bytes,
                    "created_at": item.created_at,
                },
            )
            if result.rowcount != 1:
                raise RuntimeError(
                    "0109 lifecycle artifact metadata repair refused: "
                    "target state changed during the atomic repair"
                )

    repaired_state, repaired_gc_item_count, repaired_gc_authority_count = _load_state(bind)
    repaired_orphan_state, repaired_orphan_gc_items, repaired_orphan_gc_authorities = (
        _load_orphan_state(bind)
    )
    expected_metadata_after = _expected_state(repaired=True) if not metadata_absent else state
    expected_orphan_after = (
        _expected_orphan_state(repaired=True) if not orphan_absent else orphan_state
    )
    if (
        repaired_state != expected_metadata_after
        or repaired_gc_item_count != 0
        or repaired_gc_authority_count != 0
        or repaired_orphan_state != expected_orphan_after
        or repaired_orphan_gc_items != 0
        or repaired_orphan_gc_authorities != 0
    ):
        raise RuntimeError(
            "0109 lifecycle artifact metadata repair refused: post-update verification failed"
        )


def downgrade() -> None:
    bind = op.get_bind()
    _lock_target_tables(bind)
    state, gc_item_count, gc_authority_count = _load_state(bind)
    orphan_state, orphan_gc_item_count, orphan_gc_authority_count = _load_orphan_state(bind)
    if (
        sum(len(rows) for rows in state.values()) != 0
        or gc_item_count != 0
        or gc_authority_count != 0
        or orphan_state != _empty_orphan_state()
        or orphan_gc_item_count != 0
        or orphan_gc_authority_count != 0
    ):
        raise RuntimeError("cannot downgrade 0109 after metadata repair")
