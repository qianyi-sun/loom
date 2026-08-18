from __future__ import annotations

from collections import Counter

import pytest
from pydantic import ValidationError

from loom.integrations.terminalgen.contracts import (
    AtomicVariantBucket,
    AtomicWeaknessCardV1,
    AuthoringCatalogV1,
    AuthoringImageLockV1,
    AuthoringParametersV1,
    CanonicalSourceLockV1,
    LicenseAuthorityV1,
    SlotTerminalRecordV1,
)
from loom.integrations.terminalgen.planning import build_authoring_plan
from loom.pipeline.keys import canonical_document

DIGEST = "sha256:" + "a" * 64
IMAGE = "registry.example.invalid/loom/terminalgen@sha256:" + "c" * 64


def _authority() -> LicenseAuthorityV1:
    return LicenseAuthorityV1(
        schema_version="terminalgen.license-authority.v1",
        spdx_expression="Apache-2.0",
        license_url="https://example.invalid/license",
        copyright_notice="Copyright the authorized contributors",
        derivative_use_authorized=True,
        approved_by="dataset-owner",
    )


def _card(index: int) -> AtomicWeaknessCardV1:
    return AtomicWeaknessCardV1(
        source_task=f"source-task-{index:02d}",
        capability_id=f"capability-{index:02d}",
        primary_domain="filesystem",
        allowed_domains=["database", "filesystem"],
        summary=f"Capability {index}",
        atomic_chain=["inspect state", "commit result"],
        failure_signatures=["result missing"],
        required_gates=["result is durable"],
        forbidden_shortcuts=["do not hard-code output"],
        variation_axes=["input", "layout", "ordering"],
        oracle_requirements=["replay succeeds"],
    )


def _catalog() -> AuthoringCatalogV1:
    return AuthoringCatalogV1(
        schema_version="terminalgen.authoring-catalog.v1",
        catalog_id="authorized-top18",
        catalog_version=1,
        source_lock=CanonicalSourceLockV1(
            schema_version="terminalgen.source-lock.v1",
            repository_url="https://github.com/example/terminalgen",
            commit_sha="b" * 40,
            tree_sha256=DIGEST,
            delivery_snapshot_sha256=DIGEST,
            dependency_lock_sha256=DIGEST,
            sbom_sha256=DIGEST,
            images=AuthoringImageLockV1(
                schema_version="terminalgen.image-lock.v1",
                planner=IMAGE,
                generator=IMAGE,
                static_validator=IMAGE,
                dynamic_validator=IMAGE,
                task_base=IMAGE,
                dependency_resolver=IMAGE,
                packager=IMAGE,
            ),
            code_authority=_authority(),
        ),
        derivative_data_authority=_authority(),
        cards=[_card(index) for index in range(18)],
    )


def _parameters(*, count: int = 500) -> AuthoringParametersV1:
    return AuthoringParametersV1(
        slots_per_card=count,
        difficulty="mixed",
        random_seed=7,
        dynamic_validation_repetitions=2,
        package_format="tar.zst",
    )


def test_complete_plan_is_deterministic_and_partitioned_below_fanout_limit() -> None:
    first = build_authoring_plan(_catalog(), _parameters())
    replay = build_authoring_plan(_catalog(), _parameters())

    assert canonical_document(first) == canonical_document(replay)
    assert first.plan_identity_sha256 == replay.plan_identity_sha256
    assert first.expected_partitions == 18
    assert first.expected_slots == 9_000
    assert len(first.partitions) == 18
    assert {partition.expected_slots for partition in first.partitions} == {500}
    assert len({slot.slot_id for part in first.partitions for slot in part.slots}) == 9_000
    assert len(canonical_document(first)) < 16_777_216


def test_each_partition_preserves_bucket_quota_difficulty_and_domain_rules() -> None:
    plan = build_authoring_plan(_catalog(), _parameters())
    first = plan.partitions[0]

    assert Counter(slot.variant_bucket for slot in first.slots) == {
        bucket: 100 for bucket in AtomicVariantBucket
    }
    assert Counter(slot.difficulty for slot in first.slots) == {
        "medium": 100,
        "hard": 250,
        "expert": 150,
    }
    for slot in first.slots:
        assert slot.slot_id == slot.template_family_id
        if slot.variant_bucket in {
            AtomicVariantBucket.PARAMETRIC,
            AtomicVariantBucket.STRUCTURAL,
        }:
            assert slot.domain == "filesystem"
            assert slot.domain_candidates == ["filesystem"]
        if slot.variant_bucket is AtomicVariantBucket.CROSS_DOMAIN:
            assert slot.domain == "database"
            assert slot.domain_candidates == ["database"]


def test_remainder_distribution_is_stable_for_nonproduction_contract_tests() -> None:
    plan = build_authoring_plan(_catalog(), _parameters(count=7))
    counts = Counter(slot.variant_bucket for slot in plan.partitions[0].slots)
    assert [counts[bucket] for bucket in AtomicVariantBucket] == [2, 2, 1, 1, 1]


def test_catalog_fails_closed_without_exact_card_count_or_license_authority() -> None:
    payload = _catalog().model_dump(mode="json")
    payload["cards"] = payload["cards"][:-1]
    with pytest.raises(ValidationError, match="at least 18"):
        AuthoringCatalogV1.model_validate(payload)

    authority = _authority().model_dump(mode="json")
    authority["spdx_expression"] = "NOASSERTION"
    with pytest.raises(ValidationError, match="asserted SPDX"):
        LicenseAuthorityV1.model_validate(authority)


def test_catalog_rejects_floating_or_credential_bearing_source_urls() -> None:
    source = _catalog().source_lock.model_dump(mode="json")
    source["repository_url"] = "https://token@example.invalid/repository"
    with pytest.raises(ValidationError, match="credential-free HTTPS"):
        CanonicalSourceLockV1.model_validate(source)
    source = _catalog().source_lock.model_dump(mode="json")
    source["commit_sha"] = "main"
    with pytest.raises(ValidationError):
        CanonicalSourceLockV1.model_validate(source)


def test_catalog_rejects_floating_images_and_secret_literals() -> None:
    payload = _catalog().model_dump(mode="json")
    payload["source_lock"]["images"]["generator"] = "registry.example.invalid/loom/gen:latest"
    with pytest.raises(ValidationError):
        AuthoringCatalogV1.model_validate(payload)

    payload = _catalog().model_dump(mode="json")
    payload["cards"][0]["summary"] = "Bearer abcdefghijklmnopqrstuvwxyz"
    with pytest.raises(ValidationError, match="secret-looking literal"):
        AuthoringCatalogV1.model_validate(payload)


def test_slot_terminal_record_requires_artifact_only_for_acceptance() -> None:
    plan = build_authoring_plan(_catalog(), _parameters(count=1))
    slot_id = plan.partitions[0].slots[0].slot_id
    accepted = SlotTerminalRecordV1(
        schema_version="terminalgen.slot-terminal.v1",
        slot_id=slot_id,
        outcome="accepted",
        reason_code="accepted",
        task_artifact_sha256=DIGEST,
        provider_ledger_reference="loom://provider-ledger/attempt-1",
    )
    assert accepted.outcome == "accepted"

    with pytest.raises(ValidationError, match="only accepted slots"):
        SlotTerminalRecordV1(
            schema_version="terminalgen.slot-terminal.v1",
            slot_id=slot_id,
            outcome="rejected",
            reason_code="policy_rejected",
            task_artifact_sha256=DIGEST,
            provider_ledger_reference=None,
        )

    with pytest.raises(ValidationError, match="provider ledger"):
        SlotTerminalRecordV1(
            schema_version="terminalgen.slot-terminal.v1",
            slot_id=slot_id,
            outcome="rejected",
            reason_code="policy_rejected",
            task_artifact_sha256=None,
            provider_ledger_reference=None,
        )


def test_contracts_reject_unknown_fields() -> None:
    payload = _parameters().model_dump(mode="json") | {"workers": 150}
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        AuthoringParametersV1.model_validate(payload)
