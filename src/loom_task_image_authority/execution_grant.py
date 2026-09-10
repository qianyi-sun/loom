"""Pure signed execution evidence. No issuance, persistence, source I/O or start.

The root and expected claim/purpose come from independent trusted authority.
Even a valid result requires verified source bytes, current durable grant revision,
worker capability and online one-use start before any task or sidecar runtime.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from typing import Annotated, Any, Literal, TypeVar

import rfc8785
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
from pydantic import Field, TypeAdapter, model_validator

from loom.models.task import TaskConfig
from loom.task_image_build_plan import (
    MAX_TASK_IMAGE_BUILD_PLAN_BYTES,
    TaskImageBuildPlanV2,
    _derived_components,
    _raw_environment,
    parse_task_image_build_plan,
)
from loom.task_image_materialization import (
    MAX_TASK_IMAGE_COMPONENTS,
    ImmutableRegistryImage,
    TaskImageExecutionGrantV1,
    task_bundle_content_manifest_digest,
)
from loom_task_image_authority.contracts import BuildPurpose, Digest, Identifier, TaskImageComponent
from loom_task_image_authority.publication_contracts import (
    MAX_SIGNER_REPLY_BYTES,
    CanonicalUUID,
    PublicationTimestamp,
    SafeNonnegativeInteger,
    SafePositiveInteger,
    _ClosedPublicationModel,
    _reject_constant,
    _unique_object,
    decode_publication_envelope,
    decode_publication_statement,
)
from loom_task_image_authority.publication_keyset import (
    MAX_KEYSET_ENVELOPE_BYTES,
    ExecutionGrantTrustRoot,
    _base64url,
    _instant,
    verify_publication_keyset,
)
from loom_task_image_authority.publication_set import (
    ExpectedPublication,
    VerifiedPublicationSet,
    verify_publication_set,
)
from loom_task_image_authority.publication_signing import (
    MAX_DISTRIBUTION_SNAPSHOT_LIFETIME,
    PublicationState,
    _time,
)

EXECUTION_GRANT_DOMAIN = b"loom-task-image-execution-grant-v2\x00"
MAX_EXECUTION_GRANT_BYTES = 256 * 1024
MAX_EXECUTION_GRANT_ENVELOPE_BYTES = 512 * 1024
MAX_EXECUTION_CLAIM_BYTES = 4096
MAX_FROZEN_SNAPSHOT_BYTES = 64 * 1024


class _ExecutionClaim(_ClosedPublicationModel):
    trial_id: CanonicalUUID
    team_id: CanonicalUUID
    worker_id: CanonicalUUID
    worker_lease_epoch: SafePositiveInteger
    trial_attempt_count: SafePositiveInteger


class LegacyExecutionClaim(_ExecutionClaim):
    kind: Literal["legacy"]


class ProtectedExecutionClaim(_ExecutionClaim):
    kind: Literal["protected"]
    receipt_sha256: Digest
    worker_incarnation: CanonicalUUID
    claim_high_water: SafePositiveInteger


ExecutionClaim = Annotated[
    LegacyExecutionClaim | ProtectedExecutionClaim, Field(discriminator="kind")
]
_CLAIM: TypeAdapter[LegacyExecutionClaim | ProtectedExecutionClaim] = TypeAdapter(ExecutionClaim)


class ExecutionImageBinding(_ClosedPublicationModel):
    component: TaskImageComponent
    envelope_sha256: Digest
    image: Annotated[ImmutableRegistryImage, Field(max_length=2048)]


def _canonical_object(raw: bytes, maximum: int) -> dict[str, Any]:
    if type(raw) is not bytes or not 0 < len(raw) <= maximum:
        raise ValueError("execution evidence exceeds byte ceiling")
    try:
        value = json.loads(
            raw.decode("utf-8"), object_pairs_hook=_unique_object, parse_constant=_reject_constant
        )
        if type(value) is not dict or rfc8785.dumps(value) != raw:
            raise ValueError("noncanonical execution evidence")
        return value
    except (ValueError, UnicodeError, RecursionError, OverflowError):
        raise ValueError("invalid canonical execution evidence") from None


def decode_execution_claim(wire: bytes) -> LegacyExecutionClaim | ProtectedExecutionClaim:
    return _CLAIM.validate_python(_canonical_object(wire, MAX_EXECUTION_CLAIM_BYTES))


class TaskImageExecutionGrantV2(_ClosedPublicationModel):
    schema_name: Literal["loom.task-image-execution-grant/v2"] = Field(alias="schema")
    grant_id: CanonicalUUID
    revision: SafePositiveInteger
    claim: ExecutionClaim
    environment: Identifier
    purpose: BuildPurpose
    shadow_campaign_id: CanonicalUUID | None = None
    materialization_id: CanonicalUUID
    materialization_key: Digest
    task_checksum: Digest
    cpu_arch: Literal["x86_64", "arm64"]
    # Canonical strings preserve the original signed snapshot without returning
    # mutable nested TaskConfig/provenance collections as verified authority.
    canonical_task_config: Annotated[str, Field(min_length=1, max_length=MAX_FROZEN_SNAPSHOT_BYTES)]
    canonical_source_provenance: Annotated[
        str, Field(min_length=1, max_length=MAX_FROZEN_SNAPSHOT_BYTES)
    ]
    task_source: Annotated[str, Field(min_length=1, max_length=8192)]
    frozen_plan_sha256: Digest
    components: Annotated[
        tuple[ExecutionImageBinding, ...], Field(min_length=1, max_length=MAX_TASK_IMAGE_COMPONENTS)
    ]
    keyset_sha256: Digest
    keyset_version: SafePositiveInteger
    revocation_epoch: SafeNonnegativeInteger
    issued_at: PublicationTimestamp
    expires_at: PublicationTimestamp

    @model_validator(mode="after")
    def _bindings(self) -> TaskImageExecutionGrantV2:
        issued, expires = _instant(self.issued_at), _instant(self.expires_at)
        names = tuple(item.component for item in self.components)
        if (
            (self.purpose == "production") != (self.shadow_campaign_id is None)
            or not issued < expires
            or expires - issued > MAX_DISTRIBUTION_SNAPSHOT_LIFETIME
            or names != tuple(sorted(set(names), key=lambda name: (name != "task", name)))
        ):
            raise ValueError("execution grant purpose, lifetime or components disagree")
        task, provenance = self.snapshots()
        if not task_bundle_content_manifest_digest(provenance):
            raise ValueError("execution V2 requires strong source provenance")
        # Reuse legacy snapshot compatibility, but explicitly forbid its absent-
        # manifest fallback. Never return its mutable/coerced result as evidence.
        TaskImageExecutionGrantV1.model_validate(
            dict(
                schema_version="loom.task-image-execution-grant.v1",
                materialization_id=self.materialization_id,
                materialization_key=self.materialization_key,
                cpu_arch=self.cpu_arch,
                task_checksum=self.task_checksum,
                task_config=task,
                task_source=self.task_source,
                task_source_provenance=provenance,
                registry_images={item.component: item.image for item in self.components},
            )
        )
        return self

    def snapshots(self) -> tuple[dict[str, Any], dict[str, Any]]:
        """Return fresh untrusted-to-mutation copies, never stored mutable state."""
        return (
            _canonical_object(
                self.canonical_task_config.encode("utf-8"), MAX_FROZEN_SNAPSHOT_BYTES
            ),
            _canonical_object(
                self.canonical_source_provenance.encode("utf-8"), MAX_FROZEN_SNAPSHOT_BYTES
            ),
        )


class ExecutionGrantEnvelope(_ClosedPublicationModel):
    canonical_grant: Annotated[str, Field(min_length=1, max_length=MAX_EXECUTION_GRANT_BYTES)]
    grant_sha256: Digest
    key_id: Identifier
    algorithm: Literal["Ed25519"]
    signature: Annotated[str, Field(pattern=r"^[A-Za-z0-9_-]{86}$")]


_WireModel = TypeVar("_WireModel", TaskImageExecutionGrantV2, ExecutionGrantEnvelope)


def canonical_execution_grant_bytes(
    model: TaskImageExecutionGrantV2 | ExecutionGrantEnvelope,
) -> bytes:
    if type(model) not in {TaskImageExecutionGrantV2, ExecutionGrantEnvelope}:
        raise ValueError("invalid execution wire model")
    checked = type(model).model_validate(
        model.model_dump(mode="json", by_alias=True, exclude_none=True)
    )
    wire = rfc8785.dumps(checked.model_dump(mode="json", by_alias=True, exclude_none=True))
    maximum = (
        MAX_EXECUTION_GRANT_BYTES
        if type(model) is TaskImageExecutionGrantV2
        else MAX_EXECUTION_GRANT_ENVELOPE_BYTES
    )
    if len(wire) > maximum:
        raise ValueError("execution wire exceeds byte ceiling")
    return wire


def _decode(wire: bytes, model: type[_WireModel], maximum: int) -> _WireModel:
    result = model.model_validate(_canonical_object(wire, maximum))
    if canonical_execution_grant_bytes(result) != wire:
        raise ValueError("execution evidence changed during validation")
    return result


@dataclass(frozen=True)
class VerifiedExecutionGrant:
    grant: TaskImageExecutionGrantV2
    grant_sha256: str
    publication_set: VerifiedPublicationSet

    @property
    def registry_images(self) -> tuple[tuple[str, str], ...]:
        return self.publication_set.registry_images


def verify_execution_grant(
    *,
    wire: bytes,
    plan_wire: bytes,
    publication_wires: tuple[bytes, ...],
    keyset_wire: bytes,
    trust_root: ExecutionGrantTrustRoot,
    expected_claim: LegacyExecutionClaim | ProtectedExecutionClaim,
    expected_purpose: BuildPurpose,
    expected_shadow_campaign_id: str | None,
    now: datetime,
) -> VerifiedExecutionGrant:
    """Authenticate immutable complete evidence, not a committed/current start."""
    if type(trust_root) is not ExecutionGrantTrustRoot or type(expected_claim) not in {
        LegacyExecutionClaim,
        ProtectedExecutionClaim,
    }:
        raise ValueError("execution grant requires independent root and claim")
    trust_root.__post_init__()
    _time(now)
    claim = decode_execution_claim(
        rfc8785.dumps(expected_claim.model_dump(mode="json", exclude_none=True))
    )
    TypeAdapter(BuildPurpose).validate_python(expected_purpose, strict=True)
    if expected_shadow_campaign_id is not None:
        TypeAdapter(CanonicalUUID).validate_python(expected_shadow_campaign_id, strict=True)
    if (expected_purpose == "production") != (expected_shadow_campaign_id is None):
        raise ValueError("invalid independent execution purpose")
    if (
        type(plan_wire) is not bytes
        or not 0 < len(plan_wire) <= MAX_TASK_IMAGE_BUILD_PLAN_BYTES
        or type(keyset_wire) is not bytes
        or not 0 < len(keyset_wire) <= MAX_KEYSET_ENVELOPE_BYTES
        or type(publication_wires) is not tuple
        or not 1 <= len(publication_wires) <= MAX_TASK_IMAGE_COMPONENTS
        or any(
            type(item) is not bytes or not 0 < len(item) <= MAX_SIGNER_REPLY_BYTES
            for item in publication_wires
        )
    ):
        raise ValueError("invalid bounded execution attachments")
    envelope = _decode(wire, ExecutionGrantEnvelope, MAX_EXECUTION_GRANT_ENVELOPE_BYTES)
    canonical = envelope.canonical_grant.encode("utf-8")
    if (
        envelope.key_id != trust_root.key_id
        or envelope.grant_sha256 != hashlib.sha256(canonical).hexdigest()
    ):
        raise ValueError("execution grant root or digest differs")
    try:
        Ed25519PublicKey.from_public_bytes(trust_root.public_key).verify(
            _base64url(envelope.signature, 64),
            EXECUTION_GRANT_DOMAIN + canonical,
        )
    except (InvalidSignature, ValueError):
        raise ValueError("invalid execution grant signature") from None
    grant = _decode(canonical, TaskImageExecutionGrantV2, MAX_EXECUTION_GRANT_BYTES)
    issued, expires = _instant(grant.issued_at), _instant(grant.expires_at)
    if (
        grant.claim != claim
        or grant.environment != trust_root.environment
        or grant.purpose != expected_purpose
        or grant.shadow_campaign_id != expected_shadow_campaign_id
        or not trust_root.activated_at <= issued <= now < expires <= trust_root.expires_at
        or grant.frozen_plan_sha256 != hashlib.sha256(plan_wire).hexdigest()
        or grant.keyset_sha256 != hashlib.sha256(keyset_wire).hexdigest()
        or len(grant.components) != len(publication_wires)
    ):
        raise ValueError("execution grant claim, lifetime or attachment binding differs")
    _canonical_object(plan_wire, MAX_TASK_IMAGE_BUILD_PLAN_BYTES)
    plan = parse_task_image_build_plan(plan_wire)
    if (
        type(plan) is not TaskImageBuildPlanV2
        or rfc8785.dumps(plan.model_dump(mode="json", exclude_none=False)) != plan_wire
    ):
        raise ValueError("execution grant requires original canonical V2 build plan")
    raw_task, provenance = grant.snapshots()
    task = TaskConfig.model_validate(raw_task)
    if (
        task.environment.os != "linux"
        or plan.task_id != task.task.id
        or str(plan.materialization_id) != grant.materialization_id
        or plan.task_checksum != grant.task_checksum
        or plan.cpu_arch != grant.cpu_arch
        or plan.bundle_content_manifest_sha256 != task_bundle_content_manifest_digest(provenance)
        or provenance.get("bundle_file_metadata_sha256")
        != f"sha256:{plan.bundle_file_metadata_sha256}"
        or grant.task_source != f"s3://{plan.bundle_bucket}/{plan.bundle_prefix}"
        or plan.components != _derived_components(task, _raw_environment(raw_task))
    ):
        raise ValueError("execution grant frozen source, task or component plan differs")
    # Build authorization expiry is historical evidence, not current trial time.
    state = PublicationState(
        revocation_epoch=grant.revocation_epoch, keyset_version=grant.keyset_version
    )
    keyset = verify_publication_keyset(
        keyset_wire, trust_root=trust_root, expected_state=state, now=now
    )
    if (
        not _instant(keyset.keyset.issued_at)
        <= issued
        < expires
        <= _instant(keyset.keyset.expires_at)
    ):
        raise ValueError("execution grant outlives its authenticated keyset")
    expected = []
    for binding, publication in zip(grant.components, publication_wires, strict=True):
        # Authenticate complete original bytes BEFORE deriving unsigned inputs.
        if hashlib.sha256(publication).hexdigest() != binding.envelope_sha256:
            raise ValueError("execution publication envelope differs from signed pin")
        publication_envelope = decode_publication_envelope(publication)
        unsigned = decode_publication_statement(
            publication_envelope.canonical_statement.encode("utf-8")
        ).unsigned_input()
        if (
            unsigned.component != binding.component
            or unsigned.materialization_id != grant.materialization_id
            or unsigned.materialization_key != grant.materialization_key
            or unsigned.task_id != plan.task_id
            or unsigned.task_checksum != grant.task_checksum
            or unsigned.platform != plan.platform
            or unsigned.frozen_plan_sha256 != grant.frozen_plan_sha256
            or unsigned.grant_id != str(plan.grant_id)
            or unsigned.original_claim_session_id != str(plan.session_id)
            or unsigned.original_claim_session_generation != plan.session_generation
            or unsigned.environment != grant.environment
            or unsigned.purpose != grant.purpose
            or unsigned.shadow_campaign_id != grant.shadow_campaign_id
        ):
            raise ValueError("execution publication build authority differs")
        expected.append(ExpectedPublication(unsigned, binding.envelope_sha256))
    publications = verify_publication_set(
        publication_wires=publication_wires,
        expected=tuple(expected),
        task=task,
        keyset_wire=keyset_wire,
        trust_root=trust_root,
        expected_state=state,
        expected_snapshot_sha256=grant.keyset_sha256,
        now=now,
    )
    if publications.registry_images != tuple(
        (item.component, item.image) for item in grant.components
    ):
        raise ValueError("execution native image mapping differs from verified publications")
    return VerifiedExecutionGrant(grant, hashlib.sha256(wire).hexdigest(), publications)
