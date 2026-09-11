"""Stdlib-only native containment protocol primitives.

This file may be installed byte-identically in an immutable root-owned bundle
and loaded using Python -I -S -B. Keep it free of package/site imports. Scheduler
facts alone are not authority to delegate a cgroup or start a runtime.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from datetime import UTC, datetime

_MAX_BYTES = 1024 * 1024
_MEMORY = re.compile(r"([1-9][0-9]{0,18})([KMGT])", re.ASCII)
_DECIMAL = re.compile(r"[0-9]+(?:\.[0-9]+)?", re.ASCII)
_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", re.ASCII)
_JOB_ID = re.compile(r"[1-9][0-9]{0,19}", re.ASCII)
_OWNERSHIP = re.compile(r"[A-Za-z0-9_-]{43,4096}", re.ASCII)
_EXPECTATION_FIELDS = frozenset({
    "job_id", "cluster", "hostname", "submitter", "uid", "account", "partition",
    "qos", "cpus", "memory_bytes", "ownership_token",
})


class NativeSlurmObservationError(ValueError):
    """The exact current native allocation could not be established."""


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


def _validate_expectation(expected: Mapping[str, object]) -> dict[str, object]:
    if not isinstance(expected, Mapping) or set(expected) != _EXPECTATION_FIELDS:
        raise NativeSlurmObservationError("native scheduler expectation fields are invalid")
    result = dict(expected)
    for key in ("cluster", "hostname", "submitter", "account", "partition", "qos"):
        value = result[key]
        if not isinstance(value, str) or _IDENTIFIER.fullmatch(value) is None:
            raise NativeSlurmObservationError("native scheduler expectation identifier is invalid")
    for key, pattern in (("job_id", _JOB_ID), ("ownership_token", _OWNERSHIP)):
        value = result[key]
        if not isinstance(value, str) or pattern.fullmatch(value) is None:
            raise NativeSlurmObservationError("native scheduler expectation identity is invalid")
    if _integer(result["uid"]) > (1 << 31) - 1 or _integer(result["cpus"], minimum=1) > 65_536:
        raise NativeSlurmObservationError("native scheduler expectation quantity is invalid")
    _integer(result["memory_bytes"], minimum=1)
    return result


def parse_native_scheduler_record(
    raw: str, *, expected: Mapping[str, object], observed_at: datetime,
) -> dict[str, object]:
    """One shared parser for executor and immutable node-side verifier.

    Expected facts must come from a separately authenticated launch/delegation;
    matching scheduler bytes does not authenticate whoever supplied expectations.
    The observation timestamp precedes the scheduler query, never its completion.
    """
    try:
        if (not isinstance(raw, str) or len(raw) > _MAX_BYTES
            or len(raw.encode("utf-8")) > _MAX_BYTES):
            raise NativeSlurmObservationError("native scheduler output exceeds its bound")
        facts = _validate_expectation(expected)
        if not isinstance(observed_at, datetime) or observed_at.tzinfo is None or observed_at.utcoffset() is None:
            raise NativeSlurmObservationError("native scheduler observation time must be timezone-aware")
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
            or job["batch_flag"] is not True
            or job["requeue"] is not False or _integer(job["restart_cnt"]) != 0
            or job["job_state"] != ["RUNNING"] or _number(job["node_count"]) != 1):
            raise NativeSlurmObservationError("native allocation is not one running non-requeue batch job")
        cpus, memory = _allocation_tres(job["tres_alloc_str"])
        if (_number(job["cpus"], minimum=1) != cpus or cpus != facts["cpus"]
            or memory != facts["memory_bytes"]
            or str(_integer(job["job_id"], minimum=1)) != facts["job_id"]
            or _integer(job["user_id"]) != facts["uid"]
            or any(job[key] != facts[field] for key, field in (
                ("nodes", "hostname"), ("cluster", "cluster"), ("user_name", "submitter"),
                ("account", "account"), ("partition", "partition"), ("qos", "qos"),
                ("comment", "ownership_token"),
            ))):
            raise NativeSlurmObservationError("native scheduler allocation differs from expected launch")
        submitted_at = datetime.fromtimestamp(_number(job["submit_time"], minimum=1), UTC)
        started_at = datetime.fromtimestamp(_number(job["start_time"], minimum=1), UTC)
        if not submitted_at <= started_at <= observed_at:
            raise NativeSlurmObservationError("native scheduler incarnation times are inconsistent")
        return facts | {
            "submitted_at": submitted_at, "started_at": started_at, "observed_at": observed_at,
            "schema_version": 1, "parser_version": "v0.0.40", "state": "RUNNING",
            "requeue": False, "restart_count": 0,
        }
    except NativeSlurmObservationError:
        raise
    except (ValueError, TypeError, KeyError, AttributeError, OverflowError, OSError, RecursionError):
        raise NativeSlurmObservationError("native scheduler observation is malformed") from None
