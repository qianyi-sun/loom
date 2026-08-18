"""Deterministic, replay-safe construction of the complete authoring plan."""

from __future__ import annotations

import hashlib
from typing import Literal, cast

from loom.integrations.terminalgen.contracts import (
    EXPECTED_CARD_COUNT,
    MAX_PLAN_BYTES,
    AtomicVariantBucket,
    AtomicWeaknessCardV1,
    AuthoringCatalogV1,
    AuthoringParametersV1,
    AuthoringPlanV1,
    Difficulty,
    PartitionPlanV1,
    SlotSpecV1,
)
from loom.pipeline.keys import canonical_digest, canonical_document

SlotDifficulty = Literal["medium", "hard", "expert"]


def _difficulty_cycle(difficulty: str) -> tuple[SlotDifficulty, ...]:
    if difficulty != Difficulty.MIXED.value:
        return (cast(SlotDifficulty, difficulty),)
    return (
        Difficulty.MEDIUM.value,
        Difficulty.MEDIUM.value,
        Difficulty.HARD.value,
        Difficulty.HARD.value,
        Difficulty.HARD.value,
        Difficulty.HARD.value,
        Difficulty.HARD.value,
        Difficulty.EXPERT.value,
        Difficulty.EXPERT.value,
        Difficulty.EXPERT.value,
    )


def _domain_scope(
    card: AtomicWeaknessCardV1,
    bucket: AtomicVariantBucket,
    variant_index: int,
) -> tuple[str, list[str]]:
    if bucket in {AtomicVariantBucket.PARAMETRIC, AtomicVariantBucket.STRUCTURAL}:
        return card.primary_domain, [card.primary_domain]
    if bucket is AtomicVariantBucket.CROSS_DOMAIN:
        alternatives = [item for item in card.allowed_domains if item != card.primary_domain]
        if not alternatives:
            raise ValueError("cross-domain card requires one non-primary allowed domain")
        selected = alternatives[(variant_index - 1) % len(alternatives)]
        return selected, [selected]
    return card.primary_domain, list(card.allowed_domains)


def _slot_seed(*, catalog_sha256: str, parameters_sha256: str, template_family_id: str) -> int:
    material = "\x00".join((catalog_sha256, parameters_sha256, template_family_id)).encode()
    return int.from_bytes(hashlib.sha256(material).digest()[:8], "big") & ((1 << 53) - 1)


def build_authoring_plan(
    catalog: AuthoringCatalogV1,
    parameters: AuthoringParametersV1,
) -> AuthoringPlanV1:
    """Build all expected slots before any provider-authorized StageRun is ready."""

    catalog_sha256 = canonical_digest(catalog)
    parameters_sha256 = canonical_digest(parameters)
    buckets = tuple(AtomicVariantBucket)
    difficulty_cycle = _difficulty_cycle(parameters.difficulty)
    base_quota, remainder = divmod(parameters.slots_per_card, len(buckets))
    partitions: list[PartitionPlanV1] = []
    global_ordinal = 0

    for card_ordinal, card in enumerate(catalog.cards):
        slots: list[SlotSpecV1] = []
        for bucket_ordinal, bucket in enumerate(buckets):
            quota = base_quota + (1 if bucket_ordinal < remainder else 0)
            for variant_index in range(1, quota + 1):
                domain, candidates = _domain_scope(card, bucket, variant_index)
                family = f"{card.capability_id}__{bucket.value}__{variant_index:04d}"
                difficulty_index = (
                    parameters.random_seed + card_ordinal + bucket_ordinal + variant_index - 1
                ) % len(difficulty_cycle)
                slots.append(
                    SlotSpecV1(
                        schema_version="terminalgen.slot.v1",
                        slot_id=family,
                        slot_ordinal=global_ordinal,
                        partition_id=card.capability_id,
                        source_task=card.source_task,
                        capability_id=card.capability_id,
                        primary_domain=card.primary_domain,
                        domain=domain,
                        domain_candidates=sorted(candidates, key=str.encode),
                        variant_bucket=bucket,
                        variant_index=variant_index,
                        difficulty=difficulty_cycle[difficulty_index],
                        seed=_slot_seed(
                            catalog_sha256=catalog_sha256,
                            parameters_sha256=parameters_sha256,
                            template_family_id=family,
                        ),
                        template_family_id=family,
                        catalog_sha256=catalog_sha256,
                        parameters_sha256=parameters_sha256,
                    )
                )
                global_ordinal += 1
        slots.sort(key=lambda item: item.slot_id.encode())
        partitions.append(
            PartitionPlanV1(
                schema_version="terminalgen.partition-plan.v1",
                partition_id=card.capability_id,
                partition_ordinal=card_ordinal,
                capability_id=card.capability_id,
                source_task=card.source_task,
                expected_slots=parameters.slots_per_card,
                slots=slots,
            )
        )

    identity = {
        "schema_version": "terminalgen.authoring-plan.v1",
        "catalog_sha256": catalog_sha256,
        "parameters_sha256": parameters_sha256,
        "expected_partitions": EXPECTED_CARD_COUNT,
        "expected_slots": global_ordinal,
        "partitions": partitions,
    }
    plan = AuthoringPlanV1(
        schema_version="terminalgen.authoring-plan.v1",
        catalog_sha256=catalog_sha256,
        parameters_sha256=parameters_sha256,
        expected_partitions=18,
        expected_slots=global_ordinal,
        partitions=partitions,
        plan_identity_sha256=canonical_digest(identity, persisted=False),
    )
    if len(canonical_document(plan)) > MAX_PLAN_BYTES:
        raise ValueError("authoring plan exceeds the persisted byte limit")
    return plan


__all__ = ["build_authoring_plan"]
