from __future__ import annotations

import hashlib
import threading
import time
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import UUID

import pytest
from botocore.exceptions import ClientError

from loom.data_lifecycle import DataClass, OwnerKind
from loom.data_lifecycle_gc import GcScope
from loom.data_lifecycle_legacy import (
    LegacyAbsentObject,
    LegacyClassificationError,
    LegacyObject,
    LegacyRow,
    build_legacy_classification_plan,
    classification_plan_document,
    classification_plan_summary,
)
from loom.data_lifecycle_legacy_s3 import S3LegacyObjectInspector
from loom.data_lifecycle_legacy_sql import (
    _artifact_object,
    _inspect_artifacts,
    _legacy_artifact_authority,
)

NOW = datetime(2026, 7, 19, 12, tzinfo=UTC)
SCOPE = GcScope(environment="staging", namespace="loom-staging")
TEAM_ID = UUID("11111111-1111-1111-1111-111111111111")
TRIAL_ID = UUID("22222222-2222-2222-2222-222222222222")
ARTIFACT_ID = UUID("33333333-3333-3333-3333-333333333333")
EVENT_ID = UUID(int=4)


def _trial_event(*, row_id: UUID = EVENT_ID, created_at: datetime = NOW) -> LegacyRow:
    return LegacyRow(
        table="trial_events",
        row_id=row_id,
        team_id=TEAM_ID,
        data_class=DataClass.EVENT,
        owner_kind=OwnerKind.TRIAL,
        owner_id=str(TRIAL_ID),
        created_at=created_at,
        source_fingerprint="a" * 64,
    )


def _artifact() -> LegacyRow:
    return LegacyRow(
        table="artifacts",
        row_id=ARTIFACT_ID,
        team_id=TEAM_ID,
        data_class=DataClass.ARTIFACT,
        owner_kind=OwnerKind.ARTIFACT,
        owner_id=str(ARTIFACT_ID),
        created_at=NOW,
        source_fingerprint="b" * 64,
    )


def _pinned_benchmark() -> LegacyRow:
    return LegacyRow(
        table="artifacts",
        row_id=ARTIFACT_ID,
        team_id=TEAM_ID,
        data_class=DataClass.BENCHMARK,
        owner_kind=OwnerKind.BENCHMARK,
        owner_id=str(ARTIFACT_ID),
        created_at=NOW,
        source_fingerprint="b" * 64,
        pinned=True,
    )


def _object() -> LegacyObject:
    return LegacyObject(
        row_table="artifacts",
        row_id=ARTIFACT_ID,
        bucket="loom-staging-artifacts",
        object_key=f"teams/{TEAM_ID}/artifacts/{ARTIFACT_ID}.json",
        version_id=None,
        content_sha256="c" * 64,
        size_bytes=17,
        created_at=NOW,
    )


def _absent_object() -> LegacyAbsentObject:
    return LegacyAbsentObject(
        row_table="artifacts",
        row_id=ARTIFACT_ID,
        bucket="loom-staging-artifacts",
        object_key=f"teams/{TEAM_ID}/artifacts/{ARTIFACT_ID}.json",
        version_id=None,
        created_at=NOW,
    )


def test_plan_is_deterministic_and_groups_event_rows_by_trial() -> None:
    earlier = NOW - timedelta(hours=3)
    rows = [
        _trial_event(),
        _trial_event(row_id=UUID(int=5), created_at=earlier),
        _artifact(),
    ]
    first = build_legacy_classification_plan(
        scope=SCOPE,
        mutation_epoch=7,
        planned_at=NOW,
        rows=rows,
        objects=[_object()],
    )
    second = build_legacy_classification_plan(
        scope=SCOPE,
        mutation_epoch=7,
        planned_at=NOW + timedelta(minutes=5),
        rows=reversed(rows),
        objects=[_object()],
    )

    first.require_applicable()
    assert first.inventory_digest == second.inventory_digest
    assert len(first.authorities) == 2
    event_authority = next(item for item in first.authorities if item.data_class is DataClass.EVENT)
    assert event_authority.owner_id == str(TRIAL_ID)
    assert event_authority.created_at == earlier
    assert event_authority.expires_at == earlier + timedelta(days=7)
    assert classification_plan_document(first)["inventory_digest"] == first.inventory_digest


def test_grouped_legacy_authority_rejects_cross_team_owner_facts() -> None:
    first = _trial_event()
    plan = build_legacy_classification_plan(
        scope=SCOPE,
        mutation_epoch=7,
        planned_at=NOW,
        rows=[first, replace(first, row_id=UUID(int=6), team_id=UUID(int=7))],
        objects=[],
    )

    with pytest.raises(LegacyClassificationError, match="authority facts conflict"):
        plan.require_applicable()


def test_artifact_without_exact_object_evidence_fails_closed() -> None:
    plan = build_legacy_classification_plan(
        scope=SCOPE,
        mutation_epoch=7,
        planned_at=NOW,
        rows=[_artifact()],
        objects=[],
    )

    with pytest.raises(LegacyClassificationError, match="lacks exact object evidence"):
        plan.require_applicable()


def test_verified_absent_object_is_digest_bound_and_applicable() -> None:
    first = build_legacy_classification_plan(
        scope=SCOPE,
        mutation_epoch=7,
        planned_at=NOW,
        rows=[_artifact()],
        objects=[],
        absent_objects=[_absent_object()],
    )
    second = build_legacy_classification_plan(
        scope=SCOPE,
        mutation_epoch=7,
        planned_at=NOW + timedelta(minutes=5),
        rows=[_artifact()],
        objects=[],
        absent_objects=[_absent_object()],
    )

    first.require_applicable()
    assert first.inventory_digest == second.inventory_digest
    assert first.objects == ()
    assert first.absent_objects == (_absent_object(),)
    document = classification_plan_document(first)
    assert document["objects"] == []
    assert document["expire_created_before"] is None
    assert document["absent_objects"] == [
        {
            "row_table": "artifacts",
            "row_id": str(ARTIFACT_ID),
            "bucket": "loom-staging-artifacts",
            "object_key": f"teams/{TEAM_ID}/artifacts/{ARTIFACT_ID}.json",
            "version_id": None,
            "created_at": NOW.isoformat(),
        }
    ]


def test_explicit_legacy_cutoff_is_digest_bound_without_changing_default_ttl() -> None:
    cutoff = NOW + timedelta(hours=1)
    default_plan = build_legacy_classification_plan(
        scope=SCOPE,
        mutation_epoch=7,
        planned_at=NOW,
        rows=[_artifact()],
        objects=[_object()],
    )
    purge_plan = build_legacy_classification_plan(
        scope=SCOPE,
        mutation_epoch=7,
        planned_at=NOW,
        rows=[_artifact()],
        objects=[_object()],
        expire_created_before=cutoff,
    )

    assert default_plan.authorities[0].expires_at == NOW + timedelta(days=7)
    assert purge_plan.authorities[0].expires_at == cutoff
    assert purge_plan.expire_created_before == cutoff
    assert purge_plan.inventory_digest != default_plan.inventory_digest
    assert classification_plan_document(purge_plan)["expire_created_before"] == cutoff.isoformat()


def test_legacy_cutoff_requires_timezone_authority() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        build_legacy_classification_plan(
            scope=SCOPE,
            mutation_epoch=7,
            planned_at=NOW,
            rows=[_artifact()],
            objects=[_object()],
            expire_created_before=datetime(2026, 7, 20),
        )


def test_pinned_legacy_object_is_registered_without_gc_expiry() -> None:
    plan = build_legacy_classification_plan(
        scope=SCOPE,
        mutation_epoch=7,
        planned_at=NOW,
        rows=[_pinned_benchmark()],
        objects=[_object()],
    )

    plan.require_applicable()
    assert len(plan.authorities) == 1
    authority = plan.authorities[0]
    assert authority.data_class is DataClass.BENCHMARK
    assert authority.pinned is True
    assert authority.expires_at is None
    document = classification_plan_document(plan)
    assert document["authorities"][0]["pinned"] is True  # type: ignore[index]
    assert document["authorities"][0]["expires_at"] is None  # type: ignore[index]


def test_operator_summary_is_bounded_but_binds_complete_plan() -> None:
    event_rows = [
        _trial_event(row_id=UUID(int=index + 10), created_at=NOW - timedelta(seconds=index))
        for index in range(1000)
    ]
    plan = build_legacy_classification_plan(
        scope=SCOPE,
        mutation_epoch=7,
        planned_at=NOW,
        rows=[*event_rows, _artifact()],
        objects=[_object()],
    )

    summary = classification_plan_summary(plan)

    assert summary["schema_version"] == 2
    assert summary["inventory_digest"] == plan.inventory_digest
    assert summary["row_count"] == 1001
    assert summary["row_table_counts"] == {"artifacts": 1, "trial_events": 1000}
    assert summary["authority_count"] == 2
    assert summary["present_object_count"] == 1
    assert summary["present_object_bytes"] == 17
    assert "rows" not in summary
    assert "objects" not in summary


@pytest.mark.parametrize("artifact_type", ("benchmark", "catalog", "bootstrap", "system"))
def test_only_durable_legacy_data_classes_are_pinned(artifact_type: str) -> None:
    _data_class, _owner_kind, pinned = _legacy_artifact_authority(artifact_type)

    assert pinned is True


@pytest.mark.parametrize(
    "artifact_type",
    ("evidence_bundle", "debug_bundle", "atif_projection", "trajectory_bundle"),
)
def test_per_run_legacy_artifacts_remain_ephemeral_even_when_shared(
    artifact_type: str,
) -> None:
    assert _legacy_artifact_authority(artifact_type) == (
        DataClass.ARTIFACT,
        OwnerKind.ARTIFACT,
        False,
    )


def test_unknown_object_owner_and_duplicate_identity_are_reported_together() -> None:
    unknown = LegacyObject(
        row_table="artifacts",
        row_id=UUID(int=9),
        bucket="loom-staging-artifacts",
        object_key="unknown.json",
        version_id="v1",
        content_sha256="d" * 64,
        size_bytes=1,
        created_at=NOW,
    )
    plan = build_legacy_classification_plan(
        scope=SCOPE,
        mutation_epoch=7,
        planned_at=NOW,
        rows=[_artifact()],
        objects=[_object(), _object(), unknown],
        additional_blockers=["trial owner missing"],
    )

    assert any("duplicate legacy object" in blocker for blocker in plan.blockers)
    assert any("no classified artifact" in blocker for blocker in plan.blockers)
    assert "trial owner missing" in plan.blockers


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("table", "users"),
        ("source_fingerprint", "not-a-digest"),
    ],
)
def test_row_contract_rejects_unbounded_inputs(field: str, value: str) -> None:
    values = {
        "table": "trials",
        "row_id": TRIAL_ID,
        "team_id": TEAM_ID,
        "data_class": DataClass.TRIAL,
        "owner_kind": OwnerKind.TRIAL,
        "owner_id": str(TRIAL_ID),
        "created_at": NOW,
        "source_fingerprint": "e" * 64,
    }
    values[field] = value
    with pytest.raises(ValueError):
        LegacyRow(**values)  # type: ignore[arg-type]


def test_s3_inspector_hashes_one_get_response_without_head_race() -> None:
    class Body:
        chunks = iter((b"legacy ", b"body", b""))
        closed = False

        def read(self, _size: int) -> bytes:
            return next(self.chunks)

        def close(self) -> None:
            self.closed = True

    body = Body()

    class Client:
        def __init__(self) -> None:
            self.calls: list[dict[str, str]] = []

        def get_object(self, **kwargs):
            self.calls.append(kwargs)
            return {"Body": body, "ContentLength": 11, "VersionId": "version-1"}

    client = Client()
    version, digest, size = S3LegacyObjectInspector(client).inspect(
        bucket="loom-staging-artifacts",
        object_key="exact.json",
        version_id="version-1",
    )

    assert client.calls == [
        {
            "Bucket": "loom-staging-artifacts",
            "Key": "exact.json",
            "VersionId": "version-1",
        }
    ]
    assert version == "version-1"
    assert digest == hashlib.sha256(b"legacy body").hexdigest()
    assert size == 11
    assert body.closed


@pytest.mark.parametrize("error_code", ("404", "NoSuchKey", "NoSuchVersion"))
def test_s3_inspector_returns_exact_absence_evidence(error_code: str) -> None:
    class Client:
        def get_object(self, **_kwargs: object) -> object:
            raise ClientError(
                {"Error": {"Code": error_code, "Message": "absent"}},
                "GetObject",
            )

    observed = S3LegacyObjectInspector(Client()).inspect(
        bucket="loom-staging-artifacts",
        object_key="missing.json",
        version_id=None,
    )

    assert observed is None


def test_s3_inspector_does_not_treat_authority_failure_as_absence() -> None:
    class Client:
        def get_object(self, **_kwargs: object) -> object:
            raise ClientError(
                {"Error": {"Code": "AccessDenied", "Message": "denied"}},
                "GetObject",
            )

    with pytest.raises(LegacyClassificationError, match="cannot be inspected"):
        S3LegacyObjectInspector(Client()).inspect(
            bucket="loom-staging-artifacts",
            object_key="unknown.json",
            version_id=None,
        )


def test_legacy_migration_sentinels_bind_exact_observed_object() -> None:
    class Inspector:
        def inspect(self, **kwargs: object) -> tuple[None, str, int]:
            assert kwargs["bucket"] == "loom-staging-artifacts"
            return None, "f" * 64, 8192

    source = SimpleNamespace(
        storage={"bucket": "artifacts", "key": "run/output.json", "size_bytes": 0},
        content_hash="pending:legacy-unhashed",
    )

    observed = _artifact_object(
        _artifact(),
        source,
        Inspector(),
        bucket_aliases={"artifacts": "loom-staging-artifacts"},
    )

    assert isinstance(observed, LegacyObject)
    assert observed.bucket == "loom-staging-artifacts"
    assert observed.content_sha256 == "f" * 64
    assert observed.size_bytes == 8192


def test_legacy_missing_object_is_explicit_absence_evidence() -> None:
    source = SimpleNamespace(
        storage={"bucket": "artifacts", "key": "run/missing.json", "size_bytes": 0},
        content_hash="pending:legacy-unhashed",
    )
    observed = _artifact_object(
        _artifact(),
        source,
        SimpleNamespace(inspect=lambda **_kwargs: None),
        bucket_aliases={"artifacts": "loom-staging-artifacts"},
    )

    assert observed == LegacyAbsentObject(
        row_table="artifacts",
        row_id=ARTIFACT_ID,
        bucket="loom-staging-artifacts",
        object_key="run/missing.json",
        version_id=None,
        created_at=NOW,
    )


def test_legacy_inspection_is_bounded_parallel_and_collects_all_blockers() -> None:
    lock = threading.Lock()
    active = 0
    peak = 0

    class Inspector:
        def inspect(self, *, object_key: str, **_kwargs: object):
            nonlocal active, peak
            with lock:
                active += 1
                peak = max(peak, active)
            time.sleep(0.01)
            with lock:
                active -= 1
            if object_key.endswith("missing.json"):
                return None
            if object_key.endswith("denied.json"):
                raise LegacyClassificationError("legacy object authority denied")
            return None, "f" * 64, 8192

    artifacts: list[tuple[LegacyRow, SimpleNamespace]] = []
    for number, suffix in enumerate(("one", "two", "missing", "denied"), start=10):
        row_id = UUID(int=number)
        artifacts.append(
            (
                LegacyRow(
                    table="artifacts",
                    row_id=row_id,
                    team_id=TEAM_ID,
                    data_class=DataClass.ARTIFACT,
                    owner_kind=OwnerKind.ARTIFACT,
                    owner_id=str(row_id),
                    created_at=NOW,
                    source_fingerprint=f"{number:x}" * 64,
                ),
                SimpleNamespace(
                    storage={
                        "bucket": "artifacts",
                        "key": f"run/{suffix}.json",
                        "size_bytes": 0,
                    },
                    content_hash="pending:legacy-unhashed",
                ),
            )
        )

    objects, absent, blockers = _inspect_artifacts(
        artifacts,
        Inspector(),
        bucket_aliases={"artifacts": "loom-staging-artifacts"},
        workers=2,
    )

    assert peak == 2
    assert len(objects) == 2
    assert [item.object_key for item in absent] == ["run/missing.json"]
    assert blockers == ["legacy object authority denied"]


@pytest.mark.parametrize("workers", (0, 65))
def test_legacy_inspection_rejects_unbounded_worker_counts(workers: int) -> None:
    with pytest.raises(ValueError, match="workers"):
        _inspect_artifacts([], SimpleNamespace(), bucket_aliases={}, workers=workers)


def test_legacy_bucket_alias_authority_rejects_unnormalized_values() -> None:
    source = SimpleNamespace(
        storage={"bucket": "artifacts", "key": "run/output.json", "size_bytes": 1},
        content_hash="sha256:" + "f" * 64,
    )

    with pytest.raises(LegacyClassificationError, match="bucket alias authority"):
        _artifact_object(
            _artifact(),
            source,
            SimpleNamespace(inspect=lambda **_kwargs: (None, "f" * 64, 1)),
            bucket_aliases={"artifacts": " loom-staging-artifacts"},
        )


@pytest.mark.parametrize(
    ("content_hash", "size_bytes", "message"),
    [
        ("pending:other", 0, "content hash is unclassified"),
        ("sha256:" + "f" * 64, 0, "object size drifted"),
        ("sha256:" + "e" * 64, 8192, "object digest drifted"),
    ],
)
def test_legacy_object_placeholder_exceptions_remain_fail_closed(
    content_hash: str,
    size_bytes: int,
    message: str,
) -> None:
    class Inspector:
        def inspect(self, **_kwargs: object) -> tuple[None, str, int]:
            return None, "f" * 64, 8192

    source = SimpleNamespace(
        storage={
            "bucket": "artifacts",
            "key": "run/output.json",
            "size_bytes": size_bytes,
        },
        content_hash=content_hash,
    )

    with pytest.raises(LegacyClassificationError, match=message):
        _artifact_object(_artifact(), source, Inspector())
