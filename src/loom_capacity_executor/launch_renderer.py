"""Trusted operator-profile rendering for executable worker launches."""

from __future__ import annotations

import base64
import hashlib
import hmac
import posixpath
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Annotated

from pydantic import Field, field_validator, model_validator

from loom_capacity_executor.keys import ExecutorOwnershipKey
from loom_capacity_executor.slurm_contracts import (
    MAX_SLURM_FEATURES,
    MAX_SLURM_NODES,
    MEBIBYTE,
    SlurmExecutableIdentityV2,
    SlurmLaunchRequestV2,
    SlurmTresValueV2,
)
from loom_capacity_manager.contracts import (
    MAX_DOMAINS_PER_POOL,
    Digest,
    Identifier,
    ResourceVectorV1,
)
from loom_capacity_manager.executable_contracts import (
    ExecutableIntentBindingV2,
    ExecutableOwnershipMetadataV2,
    PoolControllerAuthorityV2,
    SignedExecutableOwnershipProofV2,
    StrictV2Model,
    canonical_executable_bytes,
    canonical_executable_digest,
)
from loom_capacity_manager.ownership import sign_executable_ownership

_SLURM_IDENTIFIER_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$"
_CONTROLLER_HOST_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9.-]{0,252}[A-Za-z0-9]$"
_FEATURE_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")
_GENERIC_RESOURCE_PATTERN = r"^[a-z][a-z0-9_.-]{0,62}$"
_IMAGE_DIGEST_PATTERN = (
    r"^[a-z0-9]+(?:[._-][a-z0-9]+)*(?::[0-9]+)?"
    r"(?:/[a-z0-9]+(?:[._-][a-z0-9]+)*)+@sha256:[0-9a-f]{64}$"
)
_MAX_DIAGNOSTIC_BYTES = 64 * 1024
_DIAGNOSTIC_HEX_LENGTH = 12

SlurmIdentifier = Annotated[
    str,
    Field(min_length=1, max_length=128, pattern=_SLURM_IDENTIFIER_PATTERN),
]
ControllerHost = Annotated[
    str,
    Field(min_length=3, max_length=254, pattern=_CONTROLLER_HOST_PATTERN),
]
Feature = Annotated[str, Field(min_length=1, max_length=128)]
GenericResourceName = Annotated[
    str,
    Field(min_length=1, max_length=63, pattern=_GENERIC_RESOURCE_PATTERN),
]


class TrustedLaunchRenderError(ValueError):
    """An authorized intent cannot be rendered under the trusted profile."""


class OperatorResourceDomainV2(StrictV2Model):
    """Operator-owned mapping from one resource domain to physical nodes."""

    domain_id: Identifier
    node_ids: Annotated[tuple[Identifier, ...], Field(min_length=1, max_length=MAX_SLURM_NODES)]
    features: Annotated[tuple[Feature, ...], Field(max_length=MAX_SLURM_FEATURES)] = ()

    @field_validator("node_ids", "features")
    @classmethod
    def _canonical_strings(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if len(value) != len(set(value)):
            raise ValueError("operator resource domain contains duplicate values")
        return tuple(sorted(value))

    @field_validator("features")
    @classmethod
    def _valid_features(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if any(_FEATURE_PATTERN.fullmatch(item) is None for item in value):
            raise ValueError("operator resource domain feature is invalid")
        return value


class OperatorGenericTresMappingV2(StrictV2Model):
    """Exact manager-resource to scheduler-GRES name mapping."""

    resource_name: GenericResourceName
    tres_name: Annotated[str, Field(min_length=1, max_length=128)]

    @field_validator("tres_name")
    @classmethod
    def _valid_tres_name(cls, value: str) -> str:
        return SlurmTresValueV2(name=value, value=1).name


class OperatorLaunchProfileV2(StrictV2Model):
    """Immutable operator-owned launch policy committed by profile digest."""

    pool_id: Identifier
    pool_generation: Annotated[int, Field(gt=0, le=(1 << 63) - 1)]
    profile_id: Identifier
    profile_generation: Annotated[int, Field(gt=0, le=(1 << 63) - 1)]
    shape_id: Identifier
    concurrency_slots: Annotated[int, Field(gt=0, le=(1 << 63) - 1)]
    controller_authority_sha256: Digest
    slurm_cluster: SlurmIdentifier
    controller_host: ControllerHost
    partition: SlurmIdentifier
    association: SlurmIdentifier
    submitter: SlurmIdentifier
    qos: SlurmIdentifier
    job_name_prefix: Annotated[
        str,
        Field(min_length=1, max_length=90, pattern=_SLURM_IDENTIFIER_PATTERN),
    ]
    resource_domains: Annotated[
        tuple[OperatorResourceDomainV2, ...],
        Field(min_length=1, max_length=MAX_DOMAINS_PER_POOL),
    ]
    cpus: Annotated[int, Field(gt=0, le=65_536)]
    resources: ResourceVectorV1
    generic_tres: Annotated[tuple[OperatorGenericTresMappingV2, ...], Field(max_length=64)] = ()
    time_limit_seconds: Annotated[int, Field(gt=0, le=7 * 24 * 60 * 60)]
    launcher: SlurmExecutableIdentityV2
    trusted_launcher_release_sha256: Digest
    image_digest: Annotated[str, Field(max_length=512, pattern=_IMAGE_DIGEST_PATTERN)]

    @field_validator("resource_domains")
    @classmethod
    def _canonical_domains(
        cls,
        value: tuple[OperatorResourceDomainV2, ...],
    ) -> tuple[OperatorResourceDomainV2, ...]:
        domain_ids = [item.domain_id for item in value]
        if len(domain_ids) != len(set(domain_ids)):
            raise ValueError("operator profile contains duplicate resource domains")
        nodes = [node for domain in value for node in domain.node_ids]
        if len(nodes) != len(set(nodes)):
            raise ValueError("operator node appears in multiple resource domains")
        return tuple(sorted(value, key=lambda item: item.domain_id))

    @field_validator("generic_tres")
    @classmethod
    def _canonical_tres(
        cls,
        value: tuple[OperatorGenericTresMappingV2, ...],
    ) -> tuple[OperatorGenericTresMappingV2, ...]:
        resource_names = [item.resource_name for item in value]
        tres_names = [item.tres_name for item in value]
        if len(resource_names) != len(set(resource_names)):
            raise ValueError("operator profile contains duplicate generic resource mapping")
        if len(tres_names) != len(set(tres_names)):
            raise ValueError("operator profile contains duplicate Slurm TRES mapping")
        return tuple(sorted(value, key=lambda item: item.resource_name))

    @field_validator("launcher")
    @classmethod
    def _canonical_launcher(
        cls,
        value: SlurmExecutableIdentityV2,
    ) -> SlurmExecutableIdentityV2:
        path = value.path
        components = path.split("/")[1:]
        if (
            path == "/"
            or path.startswith("//")
            or path.endswith("/")
            or any(component in {"", ".", ".."} for component in components)
            or posixpath.normpath(path) != path
        ):
            raise ValueError("trusted launcher path must be canonical and absolute")
        return value

    @model_validator(mode="after")
    def _exact_resource_translation(self) -> OperatorLaunchProfileV2:
        if self.resources.slots != self.concurrency_slots:
            raise ValueError("operator profile slots do not match concurrency")
        if self.resources.cpu_millicores != self.cpus * 1_000:
            raise ValueError("operator profile CPU translation is not exact")
        if self.resources.memory_bytes <= 0 or self.resources.memory_bytes % MEBIBYTE:
            raise ValueError("operator profile memory must be a positive whole MiB")
        if self.resources.gpu_count > 1_024:
            raise ValueError("operator profile GPU quantity exceeds Slurm bounds")
        mapped_resources = {item.resource_name for item in self.generic_tres}
        if mapped_resources != set(self.resources.generic):
            raise ValueError("operator profile generic resource mapping is incomplete")
        if any(value <= 0 for value in self.resources.generic.values()):
            raise ValueError("operator profile generic resource quantities must be positive")
        rendered_tres = tuple(
            SlurmTresValueV2(
                name=item.tres_name,
                value=self.resources.generic[item.resource_name],
            )
            for item in self.generic_tres
        )
        typed_gpu_total = sum(
            item.value for item in rendered_tres if item.name.startswith("gres/gpu:")
        )
        if typed_gpu_total and typed_gpu_total != self.resources.gpu_count:
            raise ValueError("operator profile typed GPU mapping is not exact")
        return self


@dataclass(frozen=True, slots=True)
class TrustedLaunchContextV2:
    """All and only trusted inputs used to render and sign one launch."""

    binding: ExecutableIntentBindingV2
    profile: OperatorLaunchProfileV2
    controller_authority: PoolControllerAuthorityV2
    ownership_key: ExecutorOwnershipKey
    submitted_at: datetime
    candidate_diagnostic: str = ""
    display_diagnostic: str = ""

    def __post_init__(self) -> None:
        if not isinstance(self.binding, ExecutableIntentBindingV2):
            raise TrustedLaunchRenderError("launch binding is not executable-v2")
        if not isinstance(self.profile, OperatorLaunchProfileV2):
            raise TrustedLaunchRenderError("operator launch profile is invalid")
        if not isinstance(self.controller_authority, PoolControllerAuthorityV2):
            raise TrustedLaunchRenderError("controller authority is invalid")
        if not isinstance(self.ownership_key, ExecutorOwnershipKey):
            raise TrustedLaunchRenderError("controller ownership key is invalid")
        if self.submitted_at.tzinfo is None or self.submitted_at.utcoffset() is None:
            raise TrustedLaunchRenderError("launch submission time must be timezone-aware")
        _diagnostic_digest(self.candidate_diagnostic, label="candidate")
        _diagnostic_digest(self.display_diagnostic, label="display")


@dataclass(frozen=True, slots=True)
class RenderedTrustedLaunchV2:
    """Exact Task 6 scheduler request plus its complete signed sidecar proof."""

    request: SlurmLaunchRequestV2
    ownership_proof: SignedExecutableOwnershipProofV2


def canonical_operator_profile_digest(profile: OperatorLaunchProfileV2) -> str:
    """Commit every operator-owned scheduler and trusted-launch field."""

    if not isinstance(profile, OperatorLaunchProfileV2):
        raise TrustedLaunchRenderError("operator launch profile is invalid")
    return canonical_executable_digest(profile)


def _diagnostic_digest(value: str, *, label: str) -> str:
    if not isinstance(value, str):
        raise TrustedLaunchRenderError(f"{label} diagnostic is invalid")
    encoded = value.encode("utf-8")
    if len(encoded) > _MAX_DIAGNOSTIC_BYTES:
        raise TrustedLaunchRenderError(f"{label} diagnostic exceeds its byte bound")
    return hashlib.sha256(encoded).hexdigest()[:_DIAGNOSTIC_HEX_LENGTH]


def _assert_profile_binding(context: TrustedLaunchContextV2) -> OperatorResourceDomainV2:
    binding = context.binding
    profile = context.profile
    digest = canonical_operator_profile_digest(profile)
    if not hmac.compare_digest(digest, binding.profile_digest):
        raise TrustedLaunchRenderError("operator profile digest does not match intent")

    # Revalidate after the digest check so model_copy cannot bypass profile invariants.
    OperatorLaunchProfileV2.model_validate(profile.model_dump(mode="python"))
    if (
        binding.pool_id != profile.pool_id
        or binding.pool_generation != profile.pool_generation
        or binding.profile_id != profile.profile_id
        or binding.profile_generation != profile.profile_generation
        or binding.shape_id != profile.shape_id
        or binding.concurrency_slots != profile.concurrency_slots
        or binding.resources != profile.resources
    ):
        raise TrustedLaunchRenderError("operator profile identity does not match intent")
    authority = context.controller_authority
    if (
        authority.pool_id != profile.pool_id
        or authority.pool_id != binding.pool_id
        or not hmac.compare_digest(
            authority.controller_authority_sha256,
            profile.controller_authority_sha256,
        )
    ):
        raise TrustedLaunchRenderError("controller authority does not match operator profile")
    if not hmac.compare_digest(
        binding.execution.trusted_fleet_release_sha256,
        profile.trusted_launcher_release_sha256,
    ):
        raise TrustedLaunchRenderError("trusted launcher release does not match execution fence")

    selected = tuple(
        domain
        for domain in profile.resource_domains
        if set(binding.node_ids) <= set(domain.node_ids)
    )
    if len(selected) != 1:
        raise TrustedLaunchRenderError(
            "intent nodes do not resolve to one operator resource domain"
        )
    if len(binding.node_ids) > MAX_SLURM_NODES:
        raise TrustedLaunchRenderError("intent node set exceeds the scheduler bound")
    return selected[0]


def build_executable_ownership_metadata(
    context: TrustedLaunchContextV2,
) -> ExecutableOwnershipMetadataV2:
    """Build direct cross-checks around the digest-committed intent/profile pair."""

    _assert_profile_binding(context)
    profile = context.profile
    return ExecutableOwnershipMetadataV2(
        binding=context.binding,
        controller_authority_sha256=context.controller_authority.controller_authority_sha256,
        trusted_launcher_sha256=profile.trusted_launcher_release_sha256,
        slurm_cluster=profile.slurm_cluster,
        submitter_identity=profile.submitter,
        association=profile.association,
        submitted_at=context.submitted_at,
    )


def _ownership_token(proof: SignedExecutableOwnershipProofV2) -> str:
    digest = hashlib.sha256(canonical_executable_bytes(proof)).digest()
    return base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")


def render_signed_launch(context: TrustedLaunchContextV2) -> RenderedTrustedLaunchV2:
    """Render one exact trusted launch and its deterministic Ed25519 proof."""

    if not isinstance(context, TrustedLaunchContextV2):
        raise TypeError("trusted launch rendering requires TrustedLaunchContextV2")
    domain = _assert_profile_binding(context)
    metadata = build_executable_ownership_metadata(context)
    proof = sign_executable_ownership(
        context.ownership_key.private_key,
        signing_key_id=context.ownership_key.signing_key_id,
        metadata=metadata,
    )
    profile = context.profile
    generic_tres = tuple(
        SlurmTresValueV2(
            name=item.tres_name,
            value=context.binding.resources.generic[item.resource_name],
        )
        for item in profile.generic_tres
    )
    candidate_digest = _diagnostic_digest(
        context.candidate_diagnostic,
        label="candidate",
    )
    display_digest = _diagnostic_digest(context.display_diagnostic, label="display")
    request = SlurmLaunchRequestV2(
        cluster=profile.slurm_cluster,
        partition=profile.partition,
        account=profile.association,
        submitter=profile.submitter,
        qos=profile.qos,
        job_name=f"{profile.job_name_prefix}-{candidate_digest}-{display_digest}",
        operation_id=context.binding.intent_id,
        nodes=context.binding.node_ids,
        features=domain.features,
        cpus=profile.cpus,
        memory_bytes=context.binding.resources.memory_bytes,
        gpus=context.binding.resources.gpu_count,
        generic_tres=generic_tres,
        time_limit_seconds=profile.time_limit_seconds,
        launcher=profile.launcher,
        launcher_release_sha256=profile.trusted_launcher_release_sha256,
        image_digest=profile.image_digest,
        ownership_token=_ownership_token(proof),
    )
    return RenderedTrustedLaunchV2(request=request, ownership_proof=proof)


def render_launch_request(context: TrustedLaunchContextV2) -> SlurmLaunchRequestV2:
    """Return the exact Task 6 scheduler request for one trusted launch context."""

    return render_signed_launch(context).request


__all__ = [
    "OperatorGenericTresMappingV2",
    "OperatorLaunchProfileV2",
    "OperatorResourceDomainV2",
    "RenderedTrustedLaunchV2",
    "TrustedLaunchContextV2",
    "TrustedLaunchRenderError",
    "build_executable_ownership_metadata",
    "canonical_operator_profile_digest",
    "render_launch_request",
    "render_signed_launch",
]
