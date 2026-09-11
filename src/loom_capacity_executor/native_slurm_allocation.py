"""Version-pinned native Slurm allocation observation, separate from V2 inventory.

This is scheduler evidence only. It is not a signed delegation or root authority.
"""

from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from loom_capacity_executor.slurm_contracts import (
    OwnershipToken,
    PositiveSlurmQuantity,
    SlurmIdentifier,
    SlurmJobId,
    SlurmLaunchRequestV2,
)

_MAX_BYTES = 1024 * 1024
_MEMORY = re.compile(r"([1-9][0-9]{0,18})([KMGT])", re.ASCII)
_DECIMAL = re.compile(r"[0-9]+(?:\.[0-9]+)?", re.ASCII)


class NativeSlurmObservationError(ValueError):
    """The exact current native allocation could not be established."""


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

    @model_validator(mode="after")
    def _ordered_times(self) -> NativeSlurmAllocationV1:
        for value in (self.submitted_at, self.started_at, self.observed_at):
            if value.tzinfo is None or value.utcoffset() is None:
                raise ValueError("native scheduler times must be timezone-aware")
        if not self.submitted_at <= self.started_at <= self.observed_at:
            raise ValueError("native scheduler incarnation times are inconsistent")
        return self


def _object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise NativeSlurmObservationError("duplicate native scheduler JSON key")
        result[key] = value
    return result


def _integer(value: object, *, minimum: int = 0) -> int:
    if type(value) is not int or not minimum <= value < 1 << 63:
        raise NativeSlurmObservationError("invalid native scheduler integer")
    return value


def _number(value: object, *, minimum: int = 0) -> int:
    if (not isinstance(value, dict) or set(value) != {"set", "infinite", "number"}
        or value["set"] is not True or value["infinite"] is not False):
        raise NativeSlurmObservationError("native scheduler quantity is unset or infinite")
    return _integer(value["number"], minimum=minimum)


def _constant(value: str) -> object:
    raise NativeSlurmObservationError("nonfinite native scheduler JSON constant")


def _allocation_tres(value: object) -> tuple[int, int]:
    if not isinstance(value, str) or len(value) > 4096:
        raise NativeSlurmObservationError("native allocated TRES are unavailable")
    fields: dict[str, str] = {}
    for component in value.split(","):
        key, separator, quantity = component.partition("=")
        if not separator or key in fields or key not in {"cpu", "mem", "node", "billing"}:
            raise NativeSlurmObservationError("native allocated TRES are ambiguous or unsupported")
        fields[key] = quantity
    if not {"cpu", "mem", "node"} <= fields.keys() or fields["node"] != "1":
        raise NativeSlurmObservationError("native allocation requires exactly one node")
    if re.fullmatch(r"[1-9][0-9]{0,4}", fields["cpu"], re.ASCII) is None:
        raise NativeSlurmObservationError("native allocated CPUs are malformed")
    if "billing" in fields and _DECIMAL.fullmatch(fields["billing"]) is None:
        raise NativeSlurmObservationError("native billing TRES are malformed")
    memory = _MEMORY.fullmatch(fields["mem"])
    if memory is None:
        raise NativeSlurmObservationError("native allocated memory lacks exact units")
    units = {"K": 1024, "M": 1024**2, "G": 1024**3, "T": 1024**4}
    return int(fields["cpu"]), _integer(int(memory[1]) * units[memory[2]], minimum=1)


def parse_native_allocation(
    raw: str, *, request: SlurmLaunchRequestV2, job_id: str, expected_uid: int,
    observed_at: datetime,
) -> NativeSlurmAllocationV1:
    """Parse one live v0.0.40 scheduler record against independently expected facts."""
    try:
        if (not isinstance(raw, str) or len(raw) > _MAX_BYTES
            or len(raw.encode("utf-8")) > _MAX_BYTES):
            raise NativeSlurmObservationError("native scheduler output exceeds its bound")
        request = SlurmLaunchRequestV2.model_validate(request.model_dump())
        if request.gpus or request.generic_tres or len(request.nodes) != 1:
            raise NativeSlurmObservationError("native allocation supports single-node CPU-only requests")
        document = json.loads(raw, object_pairs_hook=_object, parse_constant=_constant)
        if (not isinstance(document, dict)
            or document["meta"]["plugin"]["data_parser"] != "v0.0.40"
            or document["errors"] != [] or document["warnings"] != []
            or not isinstance(document["jobs"], list) or len(document["jobs"]) != 1):
            raise NativeSlurmObservationError("native scheduler response is partial or has the wrong parser")
        job = document["jobs"][0]
        if not isinstance(job, dict):
            raise NativeSlurmObservationError("native scheduler record is not an object")
        for name in ("array_job_id", "het_job_id", "het_job_offset"):
            if _number(job[name]) != 0:
                raise NativeSlurmObservationError("native allocation refuses array or heterogeneous jobs")
        task = job["array_task_id"]
        if (not isinstance(task, dict) or set(task) != {"set", "infinite", "number"}
            or task["set"] is not False or task["infinite"] is not False
            or _integer(task["number"]) != 0
            or job["array_task_string"] != "" or job["het_job_id_set"] != ""
            or job["requeue"] is not False or _integer(job["restart_cnt"]) != 0
            or job["job_state"] != ["RUNNING"] or _number(job["node_count"]) != 1):
            raise NativeSlurmObservationError("native allocation is not one running non-requeue batch job")
        cpus, memory = _allocation_tres(job["tres_alloc_str"])
        if (_number(job["cpus"], minimum=1) != cpus or cpus != request.cpus
            or memory != request.memory_bytes
            or str(_integer(job["job_id"], minimum=1)) != job_id
            or _integer(job["user_id"]) != expected_uid
            or job["nodes"] != request.nodes[0]
            or any(job[key] != expected for key, expected in (
                ("cluster", request.cluster), ("user_name", request.submitter),
                ("account", request.account), ("partition", request.partition),
                ("qos", request.qos), ("comment", request.ownership_token),
            ))):
            raise NativeSlurmObservationError("native scheduler allocation differs from expected launch")
        return NativeSlurmAllocationV1(
            cluster=request.cluster, job_id=job_id, hostname=request.nodes[0],
            submitter=request.submitter, uid=expected_uid, account=request.account,
            partition=request.partition, qos=request.qos, cpus=cpus, memory_bytes=memory,
            ownership_token=request.ownership_token,
            submitted_at=datetime.fromtimestamp(_number(job["submit_time"], minimum=1), UTC),
            started_at=datetime.fromtimestamp(_number(job["start_time"], minimum=1), UTC),
            observed_at=observed_at,
        )
    except NativeSlurmObservationError:
        raise
    except (ValueError, TypeError, KeyError, AttributeError, OverflowError, OSError, RecursionError):
        raise NativeSlurmObservationError("native scheduler observation is malformed") from None
