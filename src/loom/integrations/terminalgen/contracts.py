"""Closed v1 contracts for durable synthetic terminal-task authoring.

The delivered TerminalGen snapshot is deliberately not imported here.  These
contracts are Loom-owned and require an independently auditable source and data
authority before a catalog can enter an official authoring run.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Annotated, Literal
from urllib.parse import urlsplit

from pydantic import Field, StringConstraints, field_validator, model_validator

from loom.pipeline.keys import canonical_digest
from loom.pipeline.spec import (
    IMAGE_PATTERN,
    Digest,
    NonNegativeSafeInt,
    PipelineModel,
    PositiveSafeInt,
    reject_secret_literals,
)

EXPECTED_CARD_COUNT = 18
MAX_SLOTS_PER_CARD = 500
MAX_PLAN_SLOTS = EXPECTED_CARD_COUNT * MAX_SLOTS_PER_CARD
MAX_PLAN_BYTES = 16_777_216

_Kebab = Annotated[str, StringConstraints(pattern=r"^[a-z0-9]+(?:-[a-z0-9]+)*$")]
_ShortText = Annotated[str, StringConstraints(min_length=1, max_length=512)]
_CommitSha = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{40}$")]
_SpdxExpression = Annotated[
    str,
    StringConstraints(
        pattern=(
            r"^[A-Za-z0-9][A-Za-z0-9.+-]*"
            r"(?: (?:AND|OR) [A-Za-z0-9][A-Za-z0-9.+-]*)*$"
        )
    ),
]
_TemplateFamilyId = Annotated[
    str,
    StringConstraints(pattern=r"^[a-z0-9]+(?:-[a-z0-9]+)*__[a-z0-9-]+__[0-9]{4}$"),
]


class Difficulty(StrEnum):
    MEDIUM = "medium"
    HARD = "hard"
    EXPERT = "expert"
    MIXED = "mixed"


class AtomicVariantBucket(StrEnum):
    PARAMETRIC = "same-domain-parametric"
    STRUCTURAL = "same-domain-structural"
    CROSS_DOMAIN = "cross-domain-isomorph"
    DIAGNOSE_REPAIR = "diagnose-and-repair"
    ADVERSARIAL_ROLLBACK = "adversarial-rollback"


def _https_url(value: str, label: str) -> str:
    parsed = urlsplit(value)
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError(f"{label} must be a credential-free HTTPS URL")
    return value


def _ordered_unique(values: list[str], label: str) -> list[str]:
    if values != sorted(values, key=str.encode) or len(values) != len(set(values)):
        raise ValueError(f"{label} must be bytewise sorted and unique")
    return values


class LicenseAuthorityV1(PipelineModel):
    schema_version: Literal["terminalgen.license-authority.v1"]
    spdx_expression: _SpdxExpression
    license_url: str
    copyright_notice: _ShortText
    derivative_use_authorized: Literal[True]
    approved_by: _ShortText

    @field_validator("spdx_expression")
    @classmethod
    def license_is_asserted(cls, value: str) -> str:
        if value.upper() in {"NONE", "NOASSERTION"}:
            raise ValueError("an asserted SPDX license expression is required")
        return value

    @field_validator("license_url")
    @classmethod
    def license_url_is_https(cls, value: str) -> str:
        return _https_url(value, "license_url")


class AuthoringImageLockV1(PipelineModel):
    schema_version: Literal["terminalgen.image-lock.v1"]
    planner: Annotated[str, StringConstraints(pattern=IMAGE_PATTERN)]
    generator: Annotated[str, StringConstraints(pattern=IMAGE_PATTERN)]
    static_validator: Annotated[str, StringConstraints(pattern=IMAGE_PATTERN)]
    dynamic_validator: Annotated[str, StringConstraints(pattern=IMAGE_PATTERN)]
    task_base: Annotated[str, StringConstraints(pattern=IMAGE_PATTERN)]
    dependency_resolver: Annotated[str, StringConstraints(pattern=IMAGE_PATTERN)]
    packager: Annotated[str, StringConstraints(pattern=IMAGE_PATTERN)]


class CanonicalSourceLockV1(PipelineModel):
    schema_version: Literal["terminalgen.source-lock.v1"]
    repository_url: str
    commit_sha: _CommitSha
    tree_sha256: Digest
    delivery_snapshot_sha256: Digest
    dependency_lock_sha256: Digest
    sbom_sha256: Digest
    images: AuthoringImageLockV1
    code_authority: LicenseAuthorityV1

    @field_validator("repository_url")
    @classmethod
    def repository_is_https(cls, value: str) -> str:
        return _https_url(value, "repository_url")


class AtomicWeaknessCardV1(PipelineModel):
    source_task: _Kebab
    capability_id: _Kebab
    primary_domain: _Kebab
    allowed_domains: Annotated[list[_Kebab], Field(min_length=1, max_length=32)]
    summary: Annotated[str, StringConstraints(min_length=1, max_length=2_000)]
    atomic_chain: Annotated[list[_ShortText], Field(min_length=2, max_length=16)]
    failure_signatures: Annotated[list[_ShortText], Field(min_length=1, max_length=32)]
    required_gates: Annotated[list[_ShortText], Field(min_length=1, max_length=32)]
    forbidden_shortcuts: Annotated[list[_ShortText], Field(min_length=1, max_length=32)]
    variation_axes: Annotated[list[_ShortText], Field(min_length=3, max_length=32)]
    oracle_requirements: Annotated[list[_ShortText], Field(min_length=1, max_length=32)]

    @field_validator("allowed_domains")
    @classmethod
    def domains_are_canonical(cls, values: list[str]) -> list[str]:
        return _ordered_unique(values, "allowed_domains")

    @field_validator(
        "atomic_chain",
        "failure_signatures",
        "required_gates",
        "forbidden_shortcuts",
        "variation_axes",
        "oracle_requirements",
    )
    @classmethod
    def prose_lists_are_unique(cls, values: list[str]) -> list[str]:
        if len(values) != len(set(values)):
            raise ValueError("card lists must not contain duplicate entries")
        return values

    @model_validator(mode="after")
    def primary_domain_is_allowed(self) -> AtomicWeaknessCardV1:
        if self.primary_domain not in self.allowed_domains:
            raise ValueError("primary_domain must appear in allowed_domains")
        return self


class AuthoringCatalogV1(PipelineModel):
    schema_version: Literal["terminalgen.authoring-catalog.v1"]
    catalog_id: _Kebab
    catalog_version: PositiveSafeInt
    source_lock: CanonicalSourceLockV1
    derivative_data_authority: LicenseAuthorityV1
    cards: Annotated[
        list[AtomicWeaknessCardV1],
        Field(min_length=EXPECTED_CARD_COUNT, max_length=EXPECTED_CARD_COUNT),
    ]

    @field_validator("cards")
    @classmethod
    def cards_are_canonical(cls, values: list[AtomicWeaknessCardV1]) -> list[AtomicWeaknessCardV1]:
        capabilities = [item.capability_id for item in values]
        sources = [item.source_task for item in values]
        if capabilities != sorted(capabilities, key=str.encode):
            raise ValueError("cards must be bytewise sorted by capability_id")
        if len(capabilities) != len(set(capabilities)):
            raise ValueError("card capability_id values must be unique")
        if len(sources) != len(set(sources)):
            raise ValueError("card source_task values must be unique")
        return values

    @model_validator(mode="after")
    def contains_no_secret_literals(self) -> AuthoringCatalogV1:
        reject_secret_literals(self)
        return self


class AuthoringParametersV1(PipelineModel):
    slots_per_card: Annotated[int, Field(strict=True, ge=1, le=MAX_SLOTS_PER_CARD)]
    difficulty: Literal["medium", "hard", "expert", "mixed"]
    random_seed: Annotated[int, Field(strict=True, ge=0, le=4_294_967_295)]
    dynamic_validation_repetitions: Literal[2]
    package_format: Literal["tar.zst"]


class SlotSpecV1(PipelineModel):
    schema_version: Literal["terminalgen.slot.v1"]
    slot_id: _TemplateFamilyId
    slot_ordinal: NonNegativeSafeInt
    partition_id: _Kebab
    source_task: _Kebab
    capability_id: _Kebab
    primary_domain: _Kebab
    domain: _Kebab
    domain_candidates: Annotated[list[_Kebab], Field(min_length=1, max_length=32)]
    variant_bucket: AtomicVariantBucket
    variant_index: Annotated[int, Field(strict=True, ge=1, le=MAX_SLOTS_PER_CARD)]
    difficulty: Literal["medium", "hard", "expert"]
    seed: NonNegativeSafeInt
    template_family_id: _TemplateFamilyId
    catalog_sha256: Digest
    parameters_sha256: Digest

    @field_validator("domain_candidates")
    @classmethod
    def candidates_are_canonical(cls, values: list[str]) -> list[str]:
        return _ordered_unique(values, "domain_candidates")

    @model_validator(mode="after")
    def identity_is_exact(self) -> SlotSpecV1:
        expected = f"{self.capability_id}__{self.variant_bucket.value}__{self.variant_index:04d}"
        if self.template_family_id != expected or self.slot_id != expected:
            raise ValueError("slot and template-family identity drift")
        if self.domain not in self.domain_candidates:
            raise ValueError("selected domain must appear in domain_candidates")
        return self


class PartitionPlanV1(PipelineModel):
    schema_version: Literal["terminalgen.partition-plan.v1"]
    partition_id: _Kebab
    partition_ordinal: Annotated[int, Field(strict=True, ge=0, lt=EXPECTED_CARD_COUNT)]
    capability_id: _Kebab
    source_task: _Kebab
    expected_slots: Annotated[int, Field(strict=True, ge=1, le=MAX_SLOTS_PER_CARD)]
    slots: Annotated[list[SlotSpecV1], Field(min_length=1, max_length=MAX_SLOTS_PER_CARD)]

    @model_validator(mode="after")
    def slots_match_partition(self) -> PartitionPlanV1:
        ids = [item.slot_id for item in self.slots]
        if ids != sorted(ids, key=str.encode) or len(ids) != len(set(ids)):
            raise ValueError("partition slots must be bytewise sorted and unique")
        if len(self.slots) != self.expected_slots:
            raise ValueError("partition slot count drift")
        if any(
            item.partition_id != self.partition_id
            or item.capability_id != self.capability_id
            or item.source_task != self.source_task
            for item in self.slots
        ):
            raise ValueError("slot escaped its partition identity")
        return self


class AuthoringPlanV1(PipelineModel):
    schema_version: Literal["terminalgen.authoring-plan.v1"]
    catalog_sha256: Digest
    parameters_sha256: Digest
    expected_partitions: Literal[18]
    expected_slots: Annotated[int, Field(strict=True, ge=EXPECTED_CARD_COUNT, le=MAX_PLAN_SLOTS)]
    partitions: Annotated[
        list[PartitionPlanV1],
        Field(min_length=EXPECTED_CARD_COUNT, max_length=EXPECTED_CARD_COUNT),
    ]
    plan_identity_sha256: Digest

    @model_validator(mode="after")
    def plan_is_complete(self) -> AuthoringPlanV1:
        ordinals = [item.partition_ordinal for item in self.partitions]
        if ordinals != list(range(EXPECTED_CARD_COUNT)):
            raise ValueError("partition ordinals must be complete and ordered")
        partition_ids = [item.partition_id for item in self.partitions]
        if len(partition_ids) != len(set(partition_ids)):
            raise ValueError("partition identities must be unique")
        if sum(item.expected_slots for item in self.partitions) != self.expected_slots:
            raise ValueError("authoring plan slot count drift")
        identity = self.model_dump(mode="python", exclude={"plan_identity_sha256"})
        if canonical_digest(identity, persisted=False) != self.plan_identity_sha256:
            raise ValueError("authoring plan identity digest drift")
        return self


class SlotTerminalRecordV1(PipelineModel):
    schema_version: Literal["terminalgen.slot-terminal.v1"]
    slot_id: _TemplateFamilyId
    outcome: Literal["accepted", "rejected", "exhausted", "cancelled", "cleanup_failed"]
    reason_code: Annotated[str, StringConstraints(pattern=r"^[a-z][a-z0-9_]{0,127}$")]
    task_artifact_sha256: Digest | None
    provider_ledger_reference: Annotated[
        str | None,
        StringConstraints(pattern=r"^loom://provider-ledger/[A-Za-z0-9._:@/-]+$"),
    ]

    @model_validator(mode="after")
    def artifact_matches_outcome(self) -> SlotTerminalRecordV1:
        if (self.outcome == "accepted") != (self.task_artifact_sha256 is not None):
            raise ValueError("only accepted slots may bind a task artifact")
        if self.outcome in {"accepted", "rejected"} and self.provider_ledger_reference is None:
            raise ValueError("provider-bound slot outcomes require a provider ledger reference")
        return self


__all__ = [
    "EXPECTED_CARD_COUNT",
    "MAX_PLAN_BYTES",
    "MAX_PLAN_SLOTS",
    "MAX_SLOTS_PER_CARD",
    "AtomicVariantBucket",
    "AtomicWeaknessCardV1",
    "AuthoringCatalogV1",
    "AuthoringImageLockV1",
    "AuthoringParametersV1",
    "AuthoringPlanV1",
    "CanonicalSourceLockV1",
    "Difficulty",
    "LicenseAuthorityV1",
    "PartitionPlanV1",
    "SlotSpecV1",
    "SlotTerminalRecordV1",
]
