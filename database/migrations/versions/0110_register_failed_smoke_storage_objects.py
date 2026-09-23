"""Register exact legacy-bucket objects from one failed staging smoke batch.

Revision ID: 0110
Revises: 0109

The worker uploaded these immutable attempt and verifier objects to the legacy
default buckets before storage-identity validation rejected projection.  This
one-way migration registers only the independently inventoried objects from
that exact terminal batch.  It fails closed on partial or drifted state and
never writes to object storage.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

import sqlalchemy as sa
from alembic import op

revision = "0110"
down_revision = "0109"
branch_labels = None
depends_on = None

_TEAM_ID = "bbce1c49-8d6b-429c-a338-de37a6b533b7"
_BATCH_ID = "5c79cb94-4892-4e73-9c72-b4276eb4f440"
_RUN_AUTHORITY_ID = "0572462f-a7f0-4baa-aee6-a9a63683b407"
_TASK_ID = "loom-smoke/gb10-oracle-hello-world"
_ENVIRONMENT = "staging"
_NAMESPACE = "loom-staging"


class _Trial:
    __slots__ = (
        "autoscaler_pool_assigned_at",
        "autoscaler_pool_name",
        "event_authority_id",
        "finished_at",
        "sample_idx",
        "submitted_at",
        "trial_authority_id",
        "trial_id",
    )

    def __init__(
        self,
        *,
        trial_id: str,
        trial_authority_id: str,
        event_authority_id: str,
        submitted_at: str,
        finished_at: str,
        sample_idx: int,
        autoscaler_pool_name: str | None,
        autoscaler_pool_assigned_at: str | None,
    ) -> None:
        self.trial_id = trial_id
        self.trial_authority_id = trial_authority_id
        self.event_authority_id = event_authority_id
        self.submitted_at = submitted_at
        self.finished_at = finished_at
        self.sample_idx = sample_idx
        self.autoscaler_pool_name = autoscaler_pool_name
        self.autoscaler_pool_assigned_at = autoscaler_pool_assigned_at


_TRIALS = (
    _Trial(
        trial_id="9f71f6c2-4979-48d2-a7a6-2690622786c4",
        trial_authority_id="05189a12-6e53-4b89-ae9f-26a14863225d",
        event_authority_id="c87781df-dda6-423e-861f-9d6c439c96f0",
        submitted_at="2026-08-25T14:05:19.391987+00:00",
        finished_at="2026-08-25T14:45:04.723460+00:00",
        sample_idx=0,
        autoscaler_pool_name="gb10",
        autoscaler_pool_assigned_at="2026-08-25T14:05:44.346889+00:00",
    ),
    _Trial(
        trial_id="b321c1fa-63b5-4187-9a9a-2d6e43495846",
        trial_authority_id="b977ff87-446d-4bed-ab30-402acc3cfe8c",
        event_authority_id="ac508ce0-825d-4803-b15f-acf450a9e4e4",
        submitted_at="2026-08-25T14:05:19.535689+00:00",
        finished_at="2026-08-25T14:46:10.889995+00:00",
        sample_idx=1,
        autoscaler_pool_name=None,
        autoscaler_pool_assigned_at=None,
    ),
)


class _LifecycleObject:
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
            trial.trial_authority_id for trial in _TRIALS if trial.trial_id == self.trial_id
        )

    @property
    def object_key(self) -> str:
        return f"{_TEAM_ID}/{self.trial_id}/{self.suffix}"


_OBJECTS = (
    _LifecycleObject(
        _TRIALS[0].trial_id,
        "trajectories",
        "attempts/1/atif.json",
        "80165612bc8ec1f98ef4e298e1e5316f8b648acef313f1a1bdfb545b174a8a72",
        704,
        "2026-08-25T14:06:47+00:00",
    ),
    _LifecycleObject(
        _TRIALS[0].trial_id,
        "trajectories",
        "attempts/1/events.jsonl",
        "799c084917dea6bfadc6bb05abf6def1f2d82deb50390ff7277b475136f65a23",
        1282,
        "2026-08-25T14:06:47+00:00",
    ),
    _LifecycleObject(
        _TRIALS[0].trial_id,
        "trajectories",
        "attempts/2/atif.json",
        "24708329dc23ac1d9a746503c1c2116c3110a056ceae69f0468797d9de5eda56",
        704,
        "2026-08-25T14:19:47+00:00",
    ),
    _LifecycleObject(
        _TRIALS[0].trial_id,
        "trajectories",
        "attempts/2/events.jsonl",
        "215ceebb90aad3f7a6e8564c59d65b9d7b15859d82b3d148f4e637a9c8f5084e",
        1283,
        "2026-08-25T14:19:47+00:00",
    ),
    _LifecycleObject(
        _TRIALS[0].trial_id,
        "trajectories",
        "attempts/3/atif.json",
        "24708329dc23ac1d9a746503c1c2116c3110a056ceae69f0468797d9de5eda56",
        704,
        "2026-08-25T14:32:37+00:00",
    ),
    _LifecycleObject(
        _TRIALS[0].trial_id,
        "trajectories",
        "attempts/3/events.jsonl",
        "eb2b8d768d4c05ab8a7eb5e9aa011d45231a735bf980c891740009935747d8bc",
        1282,
        "2026-08-25T14:32:37+00:00",
    ),
    _LifecycleObject(
        _TRIALS[0].trial_id,
        "artifacts",
        "main/.loom/verifier/junit.xml",
        "b56416cb3a60a8fbdd4eaab778902b7014d9dc34aec0bbc8956a4fc4d45b0226",
        336,
        "2026-08-25T14:32:37+00:00",
    ),
    _LifecycleObject(
        _TRIALS[0].trial_id,
        "artifacts",
        "main/.loom/verifier/pytest.log",
        "4e62dcb444bed1b95b65c323a32f682337e0344556bb3b4277f5a5eab0e8209e",
        113,
        "2026-08-25T14:32:37+00:00",
    ),
    _LifecycleObject(
        _TRIALS[0].trial_id,
        "artifacts",
        "main/.loom/verifier/pytest.log.meta.json",
        "6b7906963347db7cb48ee1ec94c8610cfca6b9bd64dd7a535c2747bcb136ab45",
        328,
        "2026-08-25T14:32:37+00:00",
    ),
    _LifecycleObject(
        _TRIALS[0].trial_id,
        "artifacts",
        "main/result.txt",
        "5891b5b522d5df086d0ff0b110fbd9d21bb4fc7163af34d08286a2e846f6be03",
        6,
        "2026-08-25T14:32:37+00:00",
    ),
    _LifecycleObject(
        _TRIALS[1].trial_id,
        "trajectories",
        "attempts/1/atif.json",
        "c79c95cc26bf7ea9c21114eb2e8af21399e5174f03e6d0472df976c5a036d240",
        704,
        "2026-08-25T14:06:50+00:00",
    ),
    _LifecycleObject(
        _TRIALS[1].trial_id,
        "trajectories",
        "attempts/1/events.jsonl",
        "7fde081ad9ee853e8621c709748fea35a58468b99874da848eab6fc6677aa8b0",
        1282,
        "2026-08-25T14:06:49+00:00",
    ),
    _LifecycleObject(
        _TRIALS[1].trial_id,
        "trajectories",
        "attempts/2/atif.json",
        "ab6b296452e4390ef4c65eddf62886366099f465ca8aef45f28f21d2b12941aa",
        704,
        "2026-08-25T14:20:40+00:00",
    ),
    _LifecycleObject(
        _TRIALS[1].trial_id,
        "trajectories",
        "attempts/2/events.jsonl",
        "5e1fe2ce09c061a0763d7199f811d57af3fbb189ec059ef71fd2ccaa8cb2aea3",
        1283,
        "2026-08-25T14:20:40+00:00",
    ),
    _LifecycleObject(
        _TRIALS[1].trial_id,
        "trajectories",
        "attempts/3/atif.json",
        "ab6b296452e4390ef4c65eddf62886366099f465ca8aef45f28f21d2b12941aa",
        704,
        "2026-08-25T14:33:48+00:00",
    ),
    _LifecycleObject(
        _TRIALS[1].trial_id,
        "trajectories",
        "attempts/3/events.jsonl",
        "ef570b6e0a73a97ebc8e399e57a0329a700fc64df78cf417077b6eebda0fce2f",
        1282,
        "2026-08-25T14:33:48+00:00",
    ),
    _LifecycleObject(
        _TRIALS[1].trial_id,
        "artifacts",
        "main/.loom/verifier/junit.xml",
        "2b1d33d7dc80387db9d8198df1711d33c24252d8d2cf6bb8f7fcce23a3b151da",
        336,
        "2026-08-25T14:33:48+00:00",
    ),
    _LifecycleObject(
        _TRIALS[1].trial_id,
        "artifacts",
        "main/.loom/verifier/pytest.log",
        "4e62dcb444bed1b95b65c323a32f682337e0344556bb3b4277f5a5eab0e8209e",
        113,
        "2026-08-25T14:33:48+00:00",
    ),
    _LifecycleObject(
        _TRIALS[1].trial_id,
        "artifacts",
        "main/.loom/verifier/pytest.log.meta.json",
        "c8c3a65969413e8f71ab6d3e7818998b4169ceac23ed499b88a0079faf9b45c2",
        327,
        "2026-08-25T14:33:48+00:00",
    ),
    _LifecycleObject(
        _TRIALS[1].trial_id,
        "artifacts",
        "main/result.txt",
        "5891b5b522d5df086d0ff0b110fbd9d21bb4fc7163af34d08286a2e846f6be03",
        6,
        "2026-08-25T14:33:48+00:00",
    ),
)


def _postgres_json_timestamp(value: str) -> str:
    timestamp, offset = value.rsplit("+", 1)
    if "." in timestamp:
        timestamp = timestamp.rstrip("0").rstrip(".")
    return f"{timestamp}+{offset}"


def _authority_snapshot(
    *,
    authority_id: str,
    data_class: str,
    owner_kind: str,
    owner_id: str,
    created_at: str,
) -> dict[str, Any]:
    created = datetime.fromisoformat(created_at)
    return {
        "id": authority_id,
        "environment": _ENVIRONMENT,
        "namespace": _NAMESPACE,
        "team_id": _TEAM_ID,
        "data_class": data_class,
        "owner_kind": owner_kind,
        "owner_id": owner_id,
        "created_at": _postgres_json_timestamp(created_at),
        "expires_at": _postgres_json_timestamp((created + timedelta(days=7)).isoformat()),
        "pinned": False,
        "state": "active",
        "deletion_token": None,
        "metadata": {},
    }


def _expected_owner_state() -> dict[str, Any]:
    authorities = {
        _RUN_AUTHORITY_ID: _authority_snapshot(
            authority_id=_RUN_AUTHORITY_ID,
            data_class="run",
            owner_kind="batch",
            owner_id=_BATCH_ID,
            created_at="2026-08-25T14:05:19.221168+00:00",
        )
    }
    trials: dict[str, dict[str, Any]] = {}
    for trial in _TRIALS:
        authorities[trial.trial_authority_id] = _authority_snapshot(
            authority_id=trial.trial_authority_id,
            data_class="trial",
            owner_kind="trial",
            owner_id=trial.trial_id,
            created_at=trial.submitted_at,
        )
        authorities[trial.event_authority_id] = _authority_snapshot(
            authority_id=trial.event_authority_id,
            data_class="event",
            owner_kind="trial",
            owner_id=trial.trial_id,
            created_at=trial.submitted_at,
        )
        trials[trial.trial_id] = {
            "id": trial.trial_id,
            "team_id": _TEAM_ID,
            "task_id": _TASK_ID,
            "batch_id": _BATCH_ID,
            "state": "failed",
            "failure_reason": "retry_exhausted",
            "attempt_count": 3,
            "result": None,
            "trajectory_index": None,
            "worker_id": None,
            "submit_priority": 100,
            "sample_idx": trial.sample_idx,
            "combination_idx": 0,
            "autoscaler_pool_name": trial.autoscaler_pool_name,
            "autoscaler_pool_assigned_at": (
                _postgres_json_timestamp(trial.autoscaler_pool_assigned_at)
                if trial.autoscaler_pool_assigned_at is not None
                else None
            ),
            "submitted_at": _postgres_json_timestamp(trial.submitted_at),
            "finished_at": _postgres_json_timestamp(trial.finished_at),
            "lifecycle_authority_id": trial.trial_authority_id,
        }
    return {
        "batch": {
            "id": _BATCH_ID,
            "team_id": _TEAM_ID,
            "name": "rollout-538c01ce68b24be6-1",
            "state": "finished",
            "created_at": "2026-08-25T14:05:19.221168+00:00",
            "finished_at": "2026-08-25T14:46:12.07702+00:00",
            "expected_trial_count": 2,
            "result_status": "all_failed",
            "backend": "docker",
            "n_per_task": 1,
            "required_worker_pools": ["gb10"],
            "fanout_errors": [],
            "lifecycle_authority_id": _RUN_AUTHORITY_ID,
        },
        "trials": trials,
        "authorities": authorities,
    }


def _object_snapshot(item: _LifecycleObject | dict[str, Any]) -> dict[str, Any]:
    if isinstance(item, _LifecycleObject):
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
    return item


def _object_identity(item: _LifecycleObject | dict[str, Any]) -> str:
    snapshot = _object_snapshot(item)
    return f"{snapshot['bucket']}\0{snapshot['object_key']}\0{snapshot['version_id'] or ''}"


def _expected_objects() -> dict[str, dict[str, Any]]:
    return {_object_identity(item): _object_snapshot(item) for item in _OBJECTS}


def _empty_state() -> dict[str, Any]:
    return {
        "batch": None,
        "trials": {},
        "authorities": {},
        "artifact_count": 0,
        "objects": {},
    }


def _expected_state(*, repaired: bool) -> dict[str, Any]:
    state = _expected_owner_state()
    state["artifact_count"] = 0
    state["objects"] = _expected_objects() if repaired else {}
    return state


def _authority_ids() -> list[str]:
    return [
        _RUN_AUTHORITY_ID,
        *(trial.trial_authority_id for trial in _TRIALS),
        *(trial.event_authority_id for trial in _TRIALS),
    ]


def _load_state(bind: sa.engine.Connection) -> tuple[dict[str, Any], int, int]:
    batch = bind.execute(
        sa.text(
            "SELECT jsonb_build_object("
            "'id',id,'team_id',team_id,'name',name,'state',state,"
            "'created_at',created_at,'finished_at',finished_at,"
            "'expected_trial_count',expected_trial_count,'result_status',result_status,"
            "'backend',backend,'n_per_task',n_per_task,"
            "'required_worker_pools',required_worker_pools,'fanout_errors',fanout_errors,"
            "'lifecycle_authority_id',lifecycle_authority_id) "
            "FROM batches WHERE id=CAST(:id AS uuid)"
        ),
        {"id": _BATCH_ID},
    ).scalar_one_or_none()
    trial_rows = bind.execute(
        sa.text(
            "SELECT jsonb_build_object("
            "'id',id,'team_id',team_id,'task_id',task_id,'batch_id',batch_id,"
            "'state',state,'failure_reason',failure_reason,'attempt_count',attempt_count,"
            "'result',result,'trajectory_index',trajectory_index,'worker_id',worker_id,"
            "'submit_priority',submit_priority,'sample_idx',sample_idx,"
            "'combination_idx',combination_idx,'autoscaler_pool_name',autoscaler_pool_name,"
            "'autoscaler_pool_assigned_at',autoscaler_pool_assigned_at,"
            "'submitted_at',submitted_at,'finished_at',finished_at,"
            "'lifecycle_authority_id',lifecycle_authority_id) "
            "FROM trials WHERE id=ANY(CAST(:ids AS uuid[])) ORDER BY id"
        ),
        {"ids": [trial.trial_id for trial in _TRIALS]},
    ).scalars()
    authority_rows = bind.execute(
        sa.text(
            "SELECT to_jsonb(target) FROM data_lifecycle_authorities AS target "
            "WHERE id=ANY(CAST(:ids AS uuid[])) ORDER BY id"
        ),
        {"ids": _authority_ids()},
    ).scalars()
    artifact_count = int(
        bind.execute(
            sa.text(
                "SELECT count(*) FROM artifacts "
                "WHERE batch_id=CAST(:batch_id AS uuid) "
                "OR trial_id=ANY(CAST(:trial_ids AS uuid[]))"
            ),
            {
                "batch_id": _BATCH_ID,
                "trial_ids": [trial.trial_id for trial in _TRIALS],
            },
        ).scalar_one()
    )
    prefixes = [f"{_TEAM_ID}/{trial.trial_id}/%" for trial in _TRIALS]
    object_rows = bind.execute(
        sa.text(
            "SELECT to_jsonb(target) - 'id' FROM data_lifecycle_objects AS target "
            "WHERE authority_id=ANY(CAST(:authority_ids AS uuid[])) "
            "OR (bucket IN ('artifacts','trajectories') "
            "AND (object_key LIKE :first_prefix OR object_key LIKE :second_prefix)) "
            "ORDER BY bucket,object_key,COALESCE(version_id,'')"
        ),
        {
            "authority_ids": _authority_ids(),
            "first_prefix": prefixes[0],
            "second_prefix": prefixes[1],
        },
    ).scalars()
    objects = {_object_identity(row): row for row in object_rows}
    gc_item_count = int(
        bind.execute(
            sa.text(
                "SELECT count(*) FROM data_lifecycle_gc_items AS item "
                "WHERE item.authority_id=ANY(CAST(:authority_ids AS uuid[])) "
                "OR (item.bucket IN ('artifacts','trajectories') "
                "AND (item.object_key LIKE :first_prefix OR item.object_key LIKE :second_prefix)) "
                "OR item.object_id IN ("
                "SELECT object_row.id FROM data_lifecycle_objects AS object_row "
                "WHERE object_row.authority_id=ANY(CAST(:authority_ids AS uuid[])) "
                "OR (object_row.bucket IN ('artifacts','trajectories') "
                "AND (object_row.object_key LIKE :first_prefix "
                "OR object_row.object_key LIKE :second_prefix)))"
            ),
            {
                "authority_ids": _authority_ids(),
                "first_prefix": prefixes[0],
                "second_prefix": prefixes[1],
            },
        ).scalar_one()
    )
    gc_authority_count = int(
        bind.execute(
            sa.text(
                "SELECT count(*) FROM data_lifecycle_gc_authorities "
                "WHERE authority_id=ANY(CAST(:authority_ids AS uuid[]))"
            ),
            {"authority_ids": _authority_ids()},
        ).scalar_one()
    )
    return (
        {
            "batch": batch,
            "trials": {str(row["id"]): row for row in trial_rows},
            "authorities": {str(row["id"]): row for row in authority_rows},
            "artifact_count": artifact_count,
            "objects": objects,
        },
        gc_item_count,
        gc_authority_count,
    )


def _lock_target_tables(bind: sa.engine.Connection) -> None:
    bind.execute(
        sa.text(
            "LOCK TABLE batches, trials, artifacts, data_lifecycle_authorities, "
            "data_lifecycle_objects, data_lifecycle_gc_items, "
            "data_lifecycle_gc_authorities IN SHARE ROW EXCLUSIVE MODE"
        )
    )


def upgrade() -> None:
    bind = op.get_bind()
    _lock_target_tables(bind)
    state, gc_item_count, gc_authority_count = _load_state(bind)
    absent = state == _empty_state()
    prerepair = state == _expected_state(repaired=False)
    repaired = state == _expected_state(repaired=True)
    if not (absent or prerepair or repaired) or gc_item_count != 0 or gc_authority_count != 0:
        raise RuntimeError(
            "0110 failed-smoke storage repair refused: "
            "target state is partial, mixed, drifted, or GC-touched"
        )

    if prerepair:
        for item in _OBJECTS:
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
                    "0110 failed-smoke storage repair refused: "
                    "target state changed during the atomic repair"
                )

    after, after_gc_items, after_gc_authorities = _load_state(bind)
    expected_after = _expected_state(repaired=True) if prerepair else state
    if after != expected_after or after_gc_items != 0 or after_gc_authorities != 0:
        raise RuntimeError(
            "0110 failed-smoke storage repair refused: post-insert verification failed"
        )


def downgrade() -> None:
    bind = op.get_bind()
    _lock_target_tables(bind)
    state, gc_item_count, gc_authority_count = _load_state(bind)
    if state != _empty_state() or gc_item_count != 0 or gc_authority_count != 0:
        raise RuntimeError("cannot downgrade 0110 after storage repair")
