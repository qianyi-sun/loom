from __future__ import annotations

import json
from pathlib import Path

from pydantic import TypeAdapter

from terminalgen.models import (
    AtomicVariantBucket,
    AtomicWeaknessCard,
    Difficulty,
    GenerationMode,
    GenerationRequest,
)


DEFAULT_ATOMIC_CARDS_PATH = Path(__file__).with_name("atomic_weaknesses.top18.json")


def load_atomic_weakness_cards(
    path: Path | None = DEFAULT_ATOMIC_CARDS_PATH,
) -> list[AtomicWeaknessCard]:
    resolved = path or DEFAULT_ATOMIC_CARDS_PATH
    try:
        payload = json.loads(resolved.read_text(encoding="utf-8"))
    except OSError as exc:
        raise ValueError(f"unable to read atomic weakness cards: {resolved}") from exc
    except json.JSONDecodeError as exc:
        raise ValueError(f"invalid atomic weakness card JSON at {resolved}: {exc}") from exc
    cards = TypeAdapter(list[AtomicWeaknessCard]).validate_python(payload)
    if not cards:
        raise ValueError("atomic weakness card catalog cannot be empty")
    source_tasks = [card.source_task for card in cards]
    capability_ids = [card.capability_id for card in cards]
    if len(set(source_tasks)) != len(source_tasks):
        raise ValueError("atomic weakness card source_task values must be unique")
    if len(set(capability_ids)) != len(capability_ids):
        raise ValueError("atomic weakness card capability_id values must be unique")
    return cards


def build_atomic_generation_requests(
    cards: list[AtomicWeaknessCard],
    *,
    per_card_count: int,
    difficulty: Difficulty,
    random_seed: int,
) -> list[GenerationRequest]:
    if per_card_count <= 0:
        raise ValueError("per_card_count must be > 0")
    buckets = list(AtomicVariantBucket)
    base_quota, remainder = divmod(per_card_count, len(buckets))
    difficulty_cycle = _difficulty_cycle(difficulty)
    requests: list[GenerationRequest] = []
    sample_index = 0
    for card_index, card in enumerate(cards):
        for bucket_index, bucket in enumerate(buckets):
            quota = base_quota + (1 if bucket_index < remainder else 0)
            for variant_index in range(1, quota + 1):
                target_domain, domain_candidates = _domain_scope_for_bucket(
                    card,
                    bucket,
                    variant_index=variant_index,
                )
                difficulty_index = (
                    random_seed + card_index + bucket_index + variant_index - 1
                ) % len(difficulty_cycle)
                requests.append(
                    GenerationRequest(
                        sample_index=sample_index,
                        generation_mode=GenerationMode.ATOMIC_TARGET,
                        domain=target_domain,
                        difficulty=difficulty_cycle[difficulty_index],
                        domain_candidates=domain_candidates,
                        atomic_card=card,
                        variant_bucket=bucket,
                        variant_index=variant_index,
                        template_family_id=(
                            f"{card.capability_id}__{bucket.value}__{variant_index:04d}"
                        ),
                    )
                )
                sample_index += 1
    return requests


def _domain_scope_for_bucket(
    card: AtomicWeaknessCard,
    bucket: AtomicVariantBucket,
    *,
    variant_index: int,
) -> tuple[str, list[str]]:
    if bucket in {AtomicVariantBucket.PARAMETRIC, AtomicVariantBucket.STRUCTURAL}:
        return card.primary_domain, [card.primary_domain]
    if bucket == AtomicVariantBucket.CROSS_DOMAIN:
        alternatives = [
            domain for domain in card.allowed_domains if domain != card.primary_domain
        ]
        if not alternatives:
            raise ValueError(
                f"cross-domain variants require a non-primary allowed domain: {card.source_task}"
            )
        selected = alternatives[(variant_index - 1) % len(alternatives)]
        return selected, [selected]
    return card.primary_domain, list(card.allowed_domains)


def _difficulty_cycle(difficulty: Difficulty) -> list[str]:
    if difficulty != Difficulty.MIXED:
        return [difficulty.value]
    # A learnable SFT curriculum: 20% medium, 50% hard, 30% expert.
    return [
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
    ]
