"""Version-pinned native Slurm allocation observation, separate from V2 inventory.

This is scheduler evidence only. It is not a signed delegation or root authority.
"""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from loom_capacity_executor.native_containment_protocol import (
    NativeSlurmObservationError as NativeSlurmObservationError,
)
from loom_capacity_executor.native_containment_protocol import (
    parse_native_scheduler_record,
)
from loom_capacity_executor.slurm_contracts import (
    OwnershipToken,
    PositiveSlurmQuantity,
    SlurmIdentifier,
    SlurmJobId,
    SlurmLaunchRequestV2,
)


class NativeSlurmAllocationV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    schema_version: Literal[1] = 1
    parser_version: Literal["v0.0.40"] = "v0.0.40"
    cluster: SlurmIdentifier
    job_id: SlurmJobId
    hostname: SlurmIdentifier
    submitter: SlurmIdentifier
    uid: Annotated[int, Field(ge=0, le=(1 << 31) - 1)]
    account: SlurmIdentifier
    partition: SlurmIdentifier
    qos: SlurmIdentifier
    cpus: Annotated[int, Field(gt=0, le=65_536)]
    memory_bytes: PositiveSlurmQuantity
    ownership_token: OwnershipToken
    submitted_at: datetime
    started_at: datetime
    observed_at: datetime
    state: Literal["RUNNING"] = "RUNNING"
    requeue: Literal[False] = False
    restart_count: Literal[0] = 0

    @field_validator("submitted_at", "started_at", "observed_at", mode="before")
    @classmethod
    def _timestamp(cls, value: object) -> object:
        # The before-model literal checks expose JSON values to strict Python
        # validation. Restore only the explicitly declared timestamp conversion.
        return datetime.fromisoformat(value) if isinstance(value, str) else value

    @model_validator(mode="before")
    @classmethod
    def _literal_types(cls, value: object) -> object:
        if isinstance(value, dict):
            for field, expected_type in (("schema_version", int), ("restart_count", int), ("requeue", bool)):
                if field in value and type(value[field]) is not expected_type:
                    raise ValueError("native scheduler literals require exact JSON types")
        return value

    @model_validator(mode="after")
    def _ordered_times(self) -> NativeSlurmAllocationV1:
        for value in (self.submitted_at, self.started_at, self.observed_at):
            if value.tzinfo is None or value.utcoffset() is None:
                raise ValueError("native scheduler times must be timezone-aware")
        if not self.submitted_at <= self.started_at <= self.observed_at:
            raise ValueError("native scheduler incarnation times are inconsistent")
        return self


def parse_native_allocation(
    raw: str, *, request: SlurmLaunchRequestV2, job_id: str, expected_uid: int,
    observed_at: datetime,
) -> NativeSlurmAllocationV1:
    """Apply the shared standalone parser to independently expected launch facts."""
    try:
        request = SlurmLaunchRequestV2.model_validate(request.model_dump())
        if request.gpus or request.generic_tres or len(request.nodes) != 1:
            raise NativeSlurmObservationError("native allocation supports single-node CPU-only requests")
        facts = parse_native_scheduler_record(raw, observed_at=observed_at, expected={
            "cluster": request.cluster, "job_id": job_id, "hostname": request.nodes[0],
            "submitter": request.submitter, "uid": expected_uid, "account": request.account,
            "partition": request.partition, "qos": request.qos, "cpus": request.cpus,
            "memory_bytes": request.memory_bytes, "ownership_token": request.ownership_token,
        })
        return NativeSlurmAllocationV1.model_validate(facts)
    except NativeSlurmObservationError:
        raise
    except (ValueError, TypeError, AttributeError):
        raise NativeSlurmObservationError("native scheduler observation is malformed") from None
