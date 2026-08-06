"""Unit tests for the dev-instance provisioner orchestration.

All step seams are in-memory fakes, so these exercise the create/destroy
ordering, fail-closed guardrail, and idempotency with no live fixture.
"""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID

import pytest

from loom.dev_instance import PER_INSTANCE_CAP
from loom.dev_instance_provisioner import (
    DevInstanceProvisioner,
    DevInstanceRecord,
    DevInstanceRejectedError,
    InstanceReservation,
    OwnerAccessSnapshot,
)

_NOW = datetime(2026, 8, 5, 12, 0, tzinfo=UTC)
_PW = "a" * 20  # 20 hex chars → satisfies provisioning_plan's _HEX_PASSWORD
_USER_ID = UUID("00000000-0000-0000-0000-000000000001")
_TEAM_ID = UUID("00000000-0000-0000-0000-000000000002")
_SHA = "a" * 40


def _access() -> OwnerAccessSnapshot:
    return OwnerAccessSnapshot(
        user_id=_USER_ID,
        email="alice@example.test",
        username="alice",
        username_normalized="alice",
        display_name="Alice",
        password_hash="hash",
        password_set_at=_NOW,
        user_status="active",
        user_disabled_at=None,
        user_created_at=_NOW,
        user_last_login_at=_NOW,
        team_id=_TEAM_ID,
        team_name="alice-team",
        team_created_at=_NOW,
        membership_role="owner",
        membership_created_at=_NOW,
        fair_share_weight=1.0,
        max_attempts_ceiling=3,
        license_allowlist=("MIT",),
        taskset_max_count=None,
        taskset_max_storage_bytes=None,
        allow_private_endpoints=False,
    )


class _FakeStore:
    def __init__(self) -> None:
        self._rows: dict[str, DevInstanceRecord] = {}

    async def get(self, name: str) -> DevInstanceRecord | None:
        return self._rows.get(name)

    async def list_active(self) -> list[DevInstanceRecord]:
        return [r for r in self._rows.values() if r.status != "deleted"]

    async def claim_create(self, record: DevInstanceRecord) -> InstanceReservation:
        current = self._rows.get(record.name)
        if current is not None and current.status in {"ready", "provisioning", "deleting"}:
            return InstanceReservation(current, acquired=False)
        self._rows[record.name] = record
        return InstanceReservation(record, acquired=True)

    async def claim_destroy(
        self, name, *, operation_id, keep_data, now
    ) -> InstanceReservation | None:
        current = self._rows.get(name)
        if current is None or current.status == "deleted":
            return None
        claimed = DevInstanceRecord(
            **{
                **current.__dict__,
                "status": "deleting",
                "operation_id": operation_id,
                "operation_epoch": current.operation_epoch + 1,
                "operation_step": (
                    current.operation_step
                    if current.failure_reason == "deletion_failed"
                    else "claimed"
                ),
                "keep_data": keep_data,
                "updated_at": now,
            }
        )
        self._rows[name] = claimed
        return InstanceReservation(claimed, acquired=True)

    async def assert_operation(self, name, operation_id) -> None:
        assert self._rows[name].operation_id == operation_id

    async def set_secret_ref(self, name, operation_id, secret_ref) -> None:
        await self.assert_operation(name, operation_id)
        self._rows[name] = DevInstanceRecord(
            **{**self._rows[name].__dict__, "secret_ref": secret_ref}
        )

    async def set_operation_step(self, name, operation_id, step) -> None:
        await self.assert_operation(name, operation_id)
        self._rows[name] = DevInstanceRecord(
            **{**self._rows[name].__dict__, "operation_step": step}
        )

    async def complete_operation(
        self, name, operation_id, *, status, now, failure_reason=None
    ) -> DevInstanceRecord:
        await self.assert_operation(name, operation_id)
        values = {
            **self._rows[name].__dict__,
            "status": status,
            "updated_at": now,
            "failure_reason": failure_reason,
        }
        if status == "ready":
            values["ready_at"] = now
            values["operation_step"] = "complete"
        if status == "deleted":
            values["deleted_at"] = now
            values["operation_step"] = "complete"
        self._rows[name] = DevInstanceRecord(**values)
        return self._rows[name]

    async def checkpoint(self) -> None:
        return None


def _record(name: str, *, status: str = "ready") -> DevInstanceRecord:
    return DevInstanceRecord(
        name=name,
        owner_user_id=_USER_ID,
        owner_team_id=_TEAM_ID,
        min_slots=0,
        max_slots=PER_INSTANCE_CAP,
        status=status,
        deployment_generation=1,
        candidate_sha=_SHA,
        operation_epoch=1,
        operation_id=UUID("00000000-0000-0000-0000-000000000003"),
        created_at=_NOW,
        updated_at=_NOW,
    )


class _Recorder:
    """Records every step call so ordering + presence can be asserted."""

    def __init__(self) -> None:
        self.calls: list[str] = []

    # SqlExecutor
    async def apply_role_and_database(self, identity, *, role_sql, create_database_sql):
        assert "CREATE ROLE" in role_sql
        assert create_database_sql.startswith("CREATE DATABASE")
        self.calls.append(f"sql.apply:{identity.name}")

    async def drop_database_and_role(self, identity):
        self.calls.append(f"sql.drop:{identity.name}")

    # BucketEnsurer
    async def ensure_buckets(self, identity, buckets):
        assert buckets == [
            identity.task_bucket,
            identity.trajectories_bucket,
            identity.artifacts_bucket,
        ]
        self.calls.append(f"buckets.ensure:{identity.name}")

    async def remove_buckets(self, identity, buckets):
        self.calls.append(f"buckets.remove:{identity.name}")

    # PolicyRegistrar
    async def upsert_dev_policy(self, identity, requested):
        assert requested.actuator == "slurm"
        self.calls.append(f"policy.upsert:{identity.name}:{requested.max_slots}")

    async def drop_dev_policy(self, identity):
        self.calls.append(f"policy.drop:{identity.name}")

    # ClusterProvisioner
    async def deploy(self, identity, *, deployment_generation, candidate_sha):
        assert deployment_generation == 1
        assert candidate_sha == _SHA
        self.calls.append(f"cluster.deploy:{identity.name}")

    async def destroy(self, identity, *, keep_data):
        self.calls.append(f"cluster.destroy:{identity.name}:keep={keep_data}")

    # SecretVault
    async def store(self, identity, password):
        assert password == _PW
        self.calls.append(f"vault.store:{identity.name}")
        return f"secret://{identity.name}"

    async def delete(self, identity):
        self.calls.append(f"vault.delete:{identity.name}")

    # AccessBootstrap
    async def bootstrap(self, identity, *, password, access):
        assert password == _PW
        assert access.user_id == _USER_ID
        self.calls.append(f"access.bootstrap:{identity.name}")


class _TenantRecorder:
    def __init__(self, recorder: _Recorder) -> None:
        self.recorder = recorder

    async def converge(self, identity):
        self.recorder.calls.append(f"tenant.converge:{identity.name}")

    async def delete(self, identity):
        self.recorder.calls.append(f"tenant.delete:{identity.name}")


class _FailBucketsOnceRecorder(_Recorder):
    def __init__(self) -> None:
        super().__init__()
        self.failed = False

    async def remove_buckets(self, identity, buckets):
        self.calls.append(f"buckets.remove:{identity.name}")
        if not self.failed:
            self.failed = True
            raise RuntimeError("injected bucket cleanup failure")


def _provisioner(store: _FakeStore, rec: _Recorder) -> DevInstanceProvisioner:
    return DevInstanceProvisioner(
        store=store,
        sql=rec,
        buckets=rec,
        object_store_tenant=_TenantRecorder(rec),
        policy=rec,
        cluster=rec,
        vault=rec,
        access=rec,
        candidate_sha=_SHA,
        password_factory=lambda: _PW,
    )


async def test_create_happy_path_runs_all_steps_in_order() -> None:
    store, rec = _FakeStore(), _Recorder()
    result = await _provisioner(store, rec).create(
        "alice",
        owner_user_id=_USER_ID,
        owner_team_id=_TEAM_ID,
        min_slots=0,
        max_slots=PER_INSTANCE_CAP,
        access=_access(),
        now=_NOW,
    )
    assert result.status == "ready"
    assert result.owner_user_id == _USER_ID
    assert result.secret_ref == "secret://alice"
    # data plane before control plane before cluster, then ready.
    assert rec.calls == [
        "sql.apply:alice",
        "buckets.ensure:alice",
        "vault.store:alice",
        "tenant.converge:alice",
        "cluster.deploy:alice",
        "access.bootstrap:alice",
        f"policy.upsert:alice:{PER_INSTANCE_CAP}",
    ]
    persisted = await store.get("alice")
    assert persisted is not None and persisted.status == "ready"


async def test_create_rejects_over_cap_without_mutating() -> None:
    store, rec = _FakeStore(), _Recorder()
    with pytest.raises(DevInstanceRejectedError) as exc:
        await _provisioner(store, rec).create(
            "alice",
            owner_user_id=_USER_ID,
            owner_team_id=_TEAM_ID,
            min_slots=0,
            max_slots=PER_INSTANCE_CAP + 1,
            access=_access(),
        )
    assert any("PER_INSTANCE_CAP" in r for r in exc.value.reasons)
    assert rec.calls == []  # fail-closed: nothing ran
    assert await store.get("alice") is None


async def test_create_allows_many_instances_to_share_the_runtime_budget() -> None:
    store, rec = _FakeStore(), _Recorder()
    # Policy maxima express demand. They do not reserve slots at provisioning.
    for i in range(4):
        store._rows[f"peer{i}"] = _record(f"peer{i}")
    result = await _provisioner(store, rec).create(
        "alice",
        owner_user_id=_USER_ID,
        owner_team_id=_TEAM_ID,
        min_slots=0,
        max_slots=PER_INSTANCE_CAP,
        access=_access(),
    )
    assert result.status == "ready"
    assert f"policy.upsert:alice:{PER_INSTANCE_CAP}" in rec.calls


async def test_create_is_idempotent_when_already_ready() -> None:
    store, rec = _FakeStore(), _Recorder()
    prov = _provisioner(store, rec)
    await prov.create(
        "alice",
        owner_user_id=_USER_ID,
        owner_team_id=_TEAM_ID,
        min_slots=0,
        max_slots=1,
        access=_access(),
        now=_NOW,
    )
    rec.calls.clear()
    again = await prov.create(
        "alice",
        owner_user_id=_USER_ID,
        owner_team_id=_TEAM_ID,
        min_slots=0,
        max_slots=1,
        access=_access(),
        now=_NOW,
    )
    assert again.status == "ready"
    assert rec.calls == []  # no re-provisioning of a ready instance


async def test_destroy_reverses_and_drops_data() -> None:
    store, rec = _FakeStore(), _Recorder()
    prov = _provisioner(store, rec)
    await prov.create(
        "alice",
        owner_user_id=_USER_ID,
        owner_team_id=_TEAM_ID,
        min_slots=0,
        max_slots=1,
        access=_access(),
        now=_NOW,
    )
    rec.calls.clear()
    result = await prov.destroy("alice", keep_data=False)
    assert result is not None and result.status == "deleted"
    assert rec.calls == [
        "policy.drop:alice",
        "cluster.destroy:alice:keep=False",
        "sql.drop:alice",
        "buckets.remove:alice",
        "tenant.delete:alice",
        "vault.delete:alice",
    ]
    assert (await store.get("alice")).status == "deleted"  # type: ignore[union-attr]


async def test_destroy_keep_data_preserves_storage() -> None:
    store, rec = _FakeStore(), _Recorder()
    prov = _provisioner(store, rec)
    await prov.create(
        "alice",
        owner_user_id=_USER_ID,
        owner_team_id=_TEAM_ID,
        min_slots=0,
        max_slots=1,
        access=_access(),
        now=_NOW,
    )
    rec.calls.clear()
    await prov.destroy("alice", keep_data=True)
    assert rec.calls == [
        "policy.drop:alice",
        "cluster.destroy:alice:keep=True",
    ]  # no sql.drop / buckets.remove / vault.delete


async def test_destroy_missing_instance_returns_none() -> None:
    store, rec = _FakeStore(), _Recorder()
    assert await _provisioner(store, rec).destroy("ghost") is None
    assert rec.calls == []


async def test_destroy_resumes_after_namespace_checkpoint_without_policy_access() -> None:
    store = _FakeStore()
    rec = _FailBucketsOnceRecorder()
    prov = _provisioner(store, rec)
    await prov.create(
        "alice",
        owner_user_id=_USER_ID,
        owner_team_id=_TEAM_ID,
        min_slots=0,
        max_slots=1,
        access=_access(),
        now=_NOW,
    )
    rec.calls.clear()

    with pytest.raises(RuntimeError, match="injected"):
        await prov.destroy("alice")
    assert (await store.get("alice")).operation_step == "database_deleted"  # type: ignore[union-attr]

    rec.calls.clear()
    result = await prov.destroy("alice")
    assert result is not None and result.status == "deleted"
    assert rec.calls == [
        "buckets.remove:alice",
        "tenant.delete:alice",
        "vault.delete:alice",
    ]
