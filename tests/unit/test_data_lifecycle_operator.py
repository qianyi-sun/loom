from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest

from loom.data_lifecycle_gc import (
    AuthorityInventory,
    GcResumeSnapshot,
    GcScope,
    LifecycleGcPlanError,
    RegisteredObject,
)
from loom.data_lifecycle_inventory_sql import LifecycleInventorySnapshot
from loom.data_lifecycle_operator import (
    LifecycleOperatorRequest,
    OperatorAction,
    run_lifecycle_operator,
)
from loom.staging_mutation_epoch import MutationEpochState

NOW = datetime(2026, 7, 20, tzinfo=UTC)
SCOPE = GcScope(environment="staging", namespace="loom-staging")


def _snapshot(*, unclassified: int = 0) -> LifecycleInventorySnapshot:
    authority = AuthorityInventory(
        id=uuid4(),
        environment="staging",
        namespace="loom-staging",
        owner_kind="trial",
        owner_id="trial-1",
        expires_at=NOW - timedelta(days=1),
        pinned=False,
        state="active",
    )
    item = RegisteredObject(
        id=uuid4(),
        authority_id=authority.id,
        environment="staging",
        namespace="loom-staging",
        bucket="loom-staging-artifacts",
        object_key="team/trial/result.json",
        version_id=None,
        content_sha256="a" * 64,
        size_bytes=12,
        state="active",
    )
    return LifecycleInventorySnapshot(
        scope=SCOPE,
        mutation_epoch=4,
        authorities=(authority,),
        objects=(item,),
        unclassified_rows=(("trials", unclassified),),
    )


class _Inventory:
    def __init__(self, snapshot: LifecycleInventorySnapshot) -> None:
        self.snapshot = snapshot
        self.calls = 0

    def load(self, *, scope: GcScope) -> LifecycleInventorySnapshot:
        assert scope == SCOPE
        self.calls += 1
        return self.snapshot


class _Journal:
    def __init__(self, snapshot: LifecycleInventorySnapshot) -> None:
        self.plan = snapshot.build_plan(now=NOW)
        self.run_id = uuid4()
        self.token = uuid4()
        self.events: list[str] = []

    def record_dry_run(self, *, plan, requested_by: str) -> UUID:
        assert plan == self.plan
        assert requested_by == "qianyi"
        self.events.append("dry-run")
        return self.run_id

    def begin_apply(self, *, plan, requested_by: str, deletion_token: UUID) -> UUID:
        assert plan == self.plan
        assert requested_by == "qianyi"
        self.token = deletion_token
        self.events.append("begin")
        return self.run_id

    def record_object_deleted(self, **_kwargs) -> None:
        self.events.append("deleted")

    def record_object_verified(self, **_kwargs) -> None:
        self.events.append("verified")

    def delete_business_metadata(self, **_kwargs) -> None:
        self.events.append("metadata")

    def complete_apply(self, *, mutation, **_kwargs) -> MutationEpochState:
        self.events.append("complete")
        return MutationEpochState(
            environment="staging",
            namespace="loom-staging",
            epoch=5,
            mutation_class=mutation.mutation_class,
            request_id=mutation.request_id,
            evidence_sha256=mutation.evidence_sha256,
            updated_at=mutation.occurred_at,
        )

    def fail_apply(self, **_kwargs) -> None:
        self.events.append("failed")

    def load_resume(self, run_id: UUID) -> GcResumeSnapshot:
        assert run_id == self.run_id
        return GcResumeSnapshot(
            run_id=run_id,
            deletion_token=self.token,
            plan=self.plan,
            item_states=((self.plan.objects[0].id, "object_deleted"),),
        )

    def begin_resume(self, **_kwargs) -> None:
        self.events.append("resume")


class _Deleter:
    deleted = False

    def delete_exact(self, _item) -> None:
        self.deleted = True

    def exact_absent(self, _item) -> bool:
        return self.deleted


def _request(action: OperatorAction, **overrides: object) -> LifecycleOperatorRequest:
    values: dict[str, object] = {
        "action": action,
        "requested_by": "qianyi",
        "now": NOW,
    }
    values.update(overrides)
    return LifecycleOperatorRequest(**values)  # type: ignore[arg-type]


def test_inventory_reports_all_blockers_without_journal_mutation() -> None:
    snapshot = _snapshot(unclassified=3)
    inventory = _Inventory(snapshot)
    journal = _Journal(_snapshot())

    document = run_lifecycle_operator(
        request=_request(OperatorAction.INVENTORY),
        scope=SCOPE,
        inventory=inventory,
        journal=journal,
        object_deleter=_Deleter(),
    )

    assert document["action"] == "inventory"
    assert document["applicable"] is False
    assert document["unclassified_rows"] == {"trials": 3}
    assert "unclassified" in str(document["denial"])
    assert journal.events == []


def test_dry_run_records_exact_plan_without_object_delete() -> None:
    snapshot = _snapshot()
    journal = _Journal(snapshot)
    deleter = _Deleter()

    document = run_lifecycle_operator(
        request=_request(OperatorAction.DRY_RUN),
        scope=SCOPE,
        inventory=_Inventory(snapshot),
        journal=journal,
        object_deleter=deleter,
    )

    assert document["execution"]["dry_run"] is True  # type: ignore[index]
    assert journal.events == ["dry-run"]
    assert not deleter.deleted


def test_apply_requires_approved_live_inventory_digest() -> None:
    snapshot = _snapshot()
    journal = _Journal(snapshot)
    with pytest.raises(LifecycleGcPlanError, match="does not match"):
        run_lifecycle_operator(
            request=_request(
                OperatorAction.APPLY,
                approved_inventory_digest="0" * 64,
                request_id="req-gcoperator0",
            ),
            scope=SCOPE,
            inventory=_Inventory(snapshot),
            journal=journal,
            object_deleter=_Deleter(),
        )
    assert journal.events == []


def test_resume_skips_live_replanning_and_uses_exact_run() -> None:
    snapshot = _snapshot()
    inventory = _Inventory(snapshot)
    journal = _Journal(snapshot)
    journal.token = UUID(int=9)
    deleter = _Deleter()
    deleter.deleted = True

    document = run_lifecycle_operator(
        request=_request(
            OperatorAction.RESUME,
            request_id="req-gcresume0",
            resume_run_id=journal.run_id,
        ),
        scope=SCOPE,
        inventory=inventory,
        journal=journal,
        object_deleter=deleter,
    )

    assert inventory.calls == 0
    assert document["execution"]["mutation_epoch_after"] == 5  # type: ignore[index]
    assert journal.events == ["resume", "verified", "metadata", "complete"]
