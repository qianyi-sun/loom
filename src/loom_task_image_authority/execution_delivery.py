"""Closed signed attachment delivery for explicitly capable shared-queue readers.

This extension is never sent to an old worker. It carries no root or purpose
selection, and must not be issued before online start authority is composed.
"""

from typing import Annotated, Literal

from pydantic import Field, model_validator

from loom.pipeline.spec import PipelineModel
from loom.pipeline.work_protocol import TrialClaimV1
from loom.task_image_build_plan import MAX_TASK_IMAGE_BUILD_PLAN_BYTES
from loom.task_image_materialization import MAX_TASK_IMAGE_COMPONENTS
from loom_task_image_authority.execution_grant import (
    MAX_EXECUTION_GRANT_ENVELOPE_BYTES,
    LegacyExecutionClaim,
)
from loom_task_image_authority.publication_contracts import (
    MAX_SIGNER_REPLY_BYTES,
    _ClosedPublicationModel,
)
from loom_task_image_authority.publication_keyset import MAX_KEYSET_ENVELOPE_BYTES


class TaskImageExecutionDelivery(_ClosedPublicationModel):
    schema_name: Literal["loom.task-image-execution-delivery/v2"] = Field(alias="schema")
    # Independently stamped by the authenticated claim transaction; never
    # extracted from an unverified signed grant to create its own expectation.
    claim: LegacyExecutionClaim
    grant_envelope: Annotated[str, Field(min_length=1, max_length=MAX_EXECUTION_GRANT_ENVELOPE_BYTES)]
    frozen_plan: Annotated[str, Field(min_length=1, max_length=MAX_TASK_IMAGE_BUILD_PLAN_BYTES)]
    publications: Annotated[
        tuple[Annotated[str, Field(min_length=1, max_length=MAX_SIGNER_REPLY_BYTES)], ...],
        Field(min_length=1, max_length=MAX_TASK_IMAGE_COMPONENTS),
    ]
    keyset: Annotated[str, Field(min_length=1, max_length=MAX_KEYSET_ENVELOPE_BYTES)]

    @model_validator(mode="after")
    def _bytes(self) -> "TaskImageExecutionDelivery":
        for value, maximum in (
            (self.grant_envelope, MAX_EXECUTION_GRANT_ENVELOPE_BYTES),
            (self.frozen_plan, MAX_TASK_IMAGE_BUILD_PLAN_BYTES),
            (self.keyset, MAX_KEYSET_ENVELOPE_BYTES),
            *((item, MAX_SIGNER_REPLY_BYTES) for item in self.publications),
        ):
            if len(value.encode("utf-8")) > maximum:
                raise ValueError("execution delivery exceeds UTF-8 byte ceiling")
        return self


class SignedTrialClaim(TrialClaimV1):
    task_image_execution: TaskImageExecutionDelivery

    @model_validator(mode="after")
    def _identity(self) -> "SignedTrialClaim":
        claim = self.task_image_execution.claim
        if (
            self.task_image_materialization is not None
            or claim.trial_id != str(self.trial_id)
            or claim.team_id != str(self.team_id)
            or claim.trial_attempt_count != self.attempt_count
        ):
            raise ValueError("signed delivery differs from trial claim or mixes V1 and V2")
        return self


class SignedWorkClaim(PipelineModel):
    schema_version: Literal["loom.work-claim.v1"]
    work_kind: Literal["trial"]
    payload: SignedTrialClaim
