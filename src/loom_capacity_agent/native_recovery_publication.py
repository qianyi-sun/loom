"""Protected recovery history; receipts convey no execution or deletion authority."""

from typing import Annotated, Self
from uuid import UUID

from pydantic import Field, model_validator

from loom_capacity_agent.build_admission import BuildClaimRequestV1, build_installation_id
from loom_capacity_agent.native_recovery import (
    NativeInstalledAttemptV2,
    NativeRecoveryPreparationV1,
)
from loom_capacity_manager.contracts import Digest, Identifier, StrictV1Model, canonical_digest


class NativeRecoveryHostIdentityV1(StrictV1Model):
    """Boot-specific host facts archived by the protected installer, never a worker."""

    node_id: Identifier
    boot_id: UUID
    original_uid: int = Field(gt=0, lt=2**32 - 1)
    original_gid: int = Field(gt=0, lt=2**32 - 1)
    cgroup_namespace_device: int = Field(ge=0)
    cgroup_namespace_inode: int = Field(gt=0)


class NativeRecoveryProfileV1(StrictV1Model):
    """Immutable pre-execution admission; reboot facts are retained separately."""

    installation_id: UUID
    pool_id: Identifier
    launch_profile_sha256: Digest
    worker_config_sha256: Digest
    release_manifest_sha256: Digest


class NativeRecoveryAdmissionRequestV1(StrictV1Model):
    claim: BuildClaimRequestV1
    node_id: Identifier
    boot_id: UUID

    @model_validator(mode="after")
    def _node(self) -> Self:
        if self.node_id not in self.claim.binding.node_ids:
            raise ValueError("native recovery admission node is outside allocation")
        return self


class NativeRecoveryAdmissionV1(StrictV1Model):
    request: NativeRecoveryAdmissionRequestV1
    profile: NativeRecoveryProfileV1
    host: NativeRecoveryHostIdentityV1

    @model_validator(mode="after")
    def _binding(self) -> Self:
        binding = self.request.claim.binding
        if (self.profile.installation_id != build_installation_id(binding.subject_id, binding.subject_incarnation, binding.deployment_generation)
            or self.profile.pool_id != binding.pool_id
            or self.host.node_id != self.request.node_id or self.host.boot_id != self.request.boot_id):
            raise ValueError("native recovery admission binding changed")
        return self


class NativeRecoveryAdmissionExchangeV1(StrictV1Model):
    request: NativeRecoveryAdmissionRequestV1
    worker_credential: str = Field(min_length=43, max_length=512, pattern=r"^[A-Za-z0-9_-]+$", repr=False)


class NativeRecoveryPublicationV1(StrictV1Model):
    claim: BuildClaimRequestV1
    record: Annotated[NativeRecoveryPreparationV1 | NativeInstalledAttemptV2, Field(discriminator="schema_version")]

    @model_validator(mode="after")
    def _scope(self) -> Self:
        prepared = self.record.preparation if isinstance(self.record, NativeInstalledAttemptV2) else self.record
        locator = prepared.locator
        if (locator.physical.binding != self.claim.binding or locator.worker_id != self.claim.worker_id
            or locator.worker_incarnation != self.claim.worker_incarnation):
            raise ValueError("native recovery claim scope changed")
        return self


class NativeRecoveryReceiptV1(StrictV1Model):
    request: NativeRecoveryPublicationV1
    request_digest: Digest

    @model_validator(mode="after")
    def _digest(self) -> Self:
        if self.request_digest != canonical_digest(self.request):
            raise ValueError("native recovery receipt digest changed")
        return self


class NativeRecoveryExchangeV1(StrictV1Model):
    request: NativeRecoveryPublicationV1
    worker_credential: str = Field(min_length=43, max_length=512, pattern=r"^[A-Za-z0-9_-]+$", repr=False)


class NativeRecoveryHistoryV1(StrictV1Model):
    preparation: NativeRecoveryReceiptV1 | None
    finalization: NativeRecoveryReceiptV1 | None

    @model_validator(mode="after")
    def _phases(self) -> Self:
        if self.preparation is not None and not isinstance(self.preparation.request.record, NativeRecoveryPreparationV1):
            raise ValueError("native recovery preparation phase changed")
        if self.finalization is not None:
            final = self.finalization.request
            if (self.preparation is None or not isinstance(final.record, NativeInstalledAttemptV2)
                or final.record.preparation != self.preparation.request.record
                or final.claim != self.preparation.request.claim):
                raise ValueError("native recovery finalization phase changed")
        return self
