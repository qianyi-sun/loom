"""Unit tests for the dev-instance provisioner orchestration.

All step seams are in-memory fakes, so these exercise the create/destroy
ordering, fail-closed guardrail, and idempotency with no live fixture.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from loom.dev_instance import PER_INSTANCE_CAP
from loom.dev_instance_provisioner import (
    DevInstanceProvisioner,
    DevInstanceRecord,
    DevInstanceRejectedError,
)

_NOW = datetime(2026, 8, 5, 12, 0, tzinfo=UTC)
_PW = "a" * 20  # 20 hex chars → satisfies provisioning_plan's _HEX_PASSWORD


class _FakeStore:
    def __init__(self) -> None:
        self._rows: dict[str, DevInstanceRecord] = {}

    async def get(self, name: str) -> DevInstanceRecord | None:
        return self._rows.get(name)

    async def list_active(self) -> list[DevInstanceRecord]:
        return [r for r in self._rows.values() if r.status != "deleted"]

    async def upsert(self, record: DevInstanceRecord) -> None:
        self._rows[record.name] = record

    async def set_status(self, name: str, status: str) -> None:
        self._rows[name] = DevInstanceRecord(**{**self._rows[name].__dict__, "status": status})

    async def soft_delete(self, name: str) -> None:
        await self.set_status(name, "deleted")


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
    async def deploy(self, identity):
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


def _provisioner(store: _FakeStore, rec: _Recorder) -> DevInstanceProvisioner:
    return DevInstanceProvisioner(
        store=store,
        sql=rec,
        buckets=rec,
        policy=rec,
        cluster=rec,
        vault=rec,
        password_factory=lambda: _PW,
    )


async def test_create_happy_path_runs_all_steps_in_order() -> None:
    store, rec = _FakeStore(), _Recorder()
    result = await _provisioner(store, rec).create(
        "alice", owner_user_id="u1", min_slots=0, max_slots=PER_INSTANCE_CAP, now=_NOW
    )
    assert result.status == "ready"
    assert result.owner_user_id == "u1"
    assert result.secret_ref == "secret://alice"
    # data plane before control plane before cluster, then ready.
    assert rec.calls == [
        "sql.apply:alice",
        "buckets.ensure:alice",
        "vault.store:alice",
        f"policy.upsert:alice:{PER_INSTANCE_CAP}",
        "cluster.deploy:alice",
    ]
    persisted = await store.get("alice")
    assert persisted is not None and persisted.status == "ready"


async def test_create_rejects_over_cap_without_mutating() -> None:
    store, rec = _FakeStore(), _Recorder()
    with pytest.raises(DevInstanceRejectedError) as exc:
        await _provisioner(store, rec).create(
            "alice", owner_user_id="u1", min_slots=0, max_slots=PER_INSTANCE_CAP + 1
        )
    assert any("PER_INSTANCE_CAP" in r for r in exc.value.reasons)
    assert rec.calls == []  # fail-closed: nothing ran
    assert await store.get("alice") is None


async def test_create_allows_many_instances_to_share_the_runtime_budget() -> None:
    store, rec = _FakeStore(), _Recorder()
    # Policy maxima express demand. They do not reserve slots at provisioning.
    for i in range(4):
        await store.upsert(
            DevInstanceRecord(
                name=f"peer{i}",
                owner_user_id="u",
                min_slots=0,
                max_slots=PER_INSTANCE_CAP,
                status="ready",
                created_at=_NOW,
            )
        )
    result = await _provisioner(store, rec).create(
        "alice", owner_user_id="u1", min_slots=0, max_slots=PER_INSTANCE_CAP
    )
    assert result.status == "ready"
    assert f"policy.upsert:alice:{PER_INSTANCE_CAP}" in rec.calls


async def test_create_is_idempotent_when_already_ready() -> None:
    store, rec = _FakeStore(), _Recorder()
    prov = _provisioner(store, rec)
    await prov.create("alice", owner_user_id="u1", min_slots=0, max_slots=1, now=_NOW)
    rec.calls.clear()
    again = await prov.create("alice", owner_user_id="u1", min_slots=0, max_slots=1, now=_NOW)
    assert again.status == "ready"
    assert rec.calls == []  # no re-provisioning of a ready instance


async def test_destroy_reverses_and_drops_data() -> None:
    store, rec = _FakeStore(), _Recorder()
    prov = _provisioner(store, rec)
    await prov.create("alice", owner_user_id="u1", min_slots=0, max_slots=1, now=_NOW)
    rec.calls.clear()
    result = await prov.destroy("alice", keep_data=False)
    assert result is not None and result.status == "deleted"
    assert rec.calls == [
        "policy.drop:alice",
        "cluster.destroy:alice:keep=False",
        "sql.drop:alice",
        "buckets.remove:alice",
        "vault.delete:alice",
    ]
    assert (await store.get("alice")).status == "deleted"  # type: ignore[union-attr]


async def test_destroy_keep_data_preserves_storage() -> None:
    store, rec = _FakeStore(), _Recorder()
    prov = _provisioner(store, rec)
    await prov.create("alice", owner_user_id="u1", min_slots=0, max_slots=1, now=_NOW)
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
