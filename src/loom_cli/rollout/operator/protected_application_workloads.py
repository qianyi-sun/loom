"""Saved identities and compare-and-swap changes for owned staging writers.

The handoff journal must persist each original record before its first patch.
The enclosing protected operation serializes spec/scale writers; these records
neither establish that exclusion nor certify that old Pods or SQL have exited.
"""

from __future__ import annotations

import copy
import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import asdict, dataclass, fields

APPLICATION_DEPLOYMENTS = frozenset({
    "loom-capacity-agent", "loom-control-plane", "loom-family-orchestrator",
    "loom-llm-gateway", "loom-pgbouncer", "loom-pipeline-orchestrator", "loom-service",
})
APPLICATION_CRONJOB = "loom-staging-data-lifecycle"
_NAMESPACE = "loom-staging"
_UID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
_JOB = re.compile(r"(?:loom-staging-data-lifecycle-[0-9]{1,20}|loom-staging-migrate|loom-migrate-staging-[a-z0-9-]{1,40})")


def _mapping(value: object) -> dict[str, object]:
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise ValueError("application workload object is invalid")
    return value


def _digest(value: object) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def _identity(document: Mapping[str, object]) -> tuple[str, str, str]:
    metadata = _mapping(document.get("metadata"))
    kind, name, uid = document.get("kind"), metadata.get("name"), metadata.get("uid")
    if (
        not isinstance(name, str) or not isinstance(kind, str)
        or not isinstance(uid, str) or _UID.fullmatch(uid) is None
        or metadata.get("namespace") != _NAMESPACE or metadata.get("deletionTimestamp") is not None
        or document.get("apiVersion") != ("apps/v1" if kind == "Deployment" else "batch/v1")
        or not ((kind == "Deployment" and name in APPLICATION_DEPLOYMENTS)
                or (kind == "CronJob" and name == APPLICATION_CRONJOB)
                or (kind == "Job" and _JOB.fullmatch(name) is not None))
        or not isinstance(metadata.get("resourceVersion"), str)
        or re.fullmatch(r"[1-9][0-9]{0,31}", str(metadata["resourceVersion"])) is None
        or type(metadata.get("generation")) is not int or int(str(metadata["generation"])) < 1
    ):
        raise ValueError("application workload identity is unsupported or changed")
    _mapping(metadata.get("labels", {}))
    if not isinstance(metadata.get("ownerReferences", []), list):
        raise ValueError("application workload ownership is invalid")
    return kind, name, uid


def _field(kind: str) -> str:
    return "replicas" if kind == "Deployment" else "suspend"


def _shape(document: Mapping[str, object], kind: str) -> str:
    metadata = _mapping(document.get("metadata"))
    spec = dict(_mapping(document.get("spec")))
    spec.pop(_field(kind), None)
    return _digest({"spec": spec, "owners": metadata.get("ownerReferences", []),
                    "labels": metadata.get("labels", {})})


def _value(spec: Mapping[str, object], kind: str) -> int | bool:
    value = spec.get(_field(kind), 1 if kind == "Deployment" else False)
    if ((kind == "Deployment" and (type(value) is not int or not 0 <= int(str(value)) <= 128))
            or (kind != "Deployment" and type(value) is not bool)):
        raise ValueError("application workload scale or suspend value is invalid")
    assert isinstance(value, (int, bool))
    return value


@dataclass(frozen=True, slots=True)
class ApplicationWorkload:
    kind: str
    name: str
    uid: str
    shape_digest: str
    original_value: int | bool
    original_present: bool

    def __post_init__(self) -> None:
        if (
            not isinstance(self.shape_digest, str)
            or re.fullmatch(r"[0-9a-f]{64}", self.shape_digest) is None
            or type(self.original_present) is not bool
        ):
            raise ValueError("application workload saved shape is invalid")
        _identity({"kind": self.kind, "apiVersion": "apps/v1" if self.kind == "Deployment" else "batch/v1",
                   "metadata": {"name": self.name, "uid": self.uid, "namespace": _NAMESPACE,
                                "resourceVersion": "1", "generation": 1}})
        _value({_field(self.kind): self.original_value}, self.kind)
        if not self.original_present and self.original_value != (1 if self.kind == "Deployment" else False):
            raise ValueError("application workload omitted value differs from its default")

    @classmethod
    def capture(cls, document: Mapping[str, object]) -> ApplicationWorkload:
        kind, name, uid = _identity(document)
        spec = _mapping(document.get("spec"))
        return cls(kind, name, uid, _shape(document, kind), _value(spec, kind), _field(kind) in spec)

    @property
    def resource(self) -> str:
        return {"Deployment": "deployments.apps", "CronJob": "cronjobs.batch", "Job": "jobs.batch"}[self.kind]

    def to_dict(self) -> dict[str, object]:
        return {"schema_version": 1, **asdict(self)}

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> ApplicationWorkload:
        if (set(value) != {"schema_version", *(field.name for field in fields(cls))}
                or type(value.get("schema_version")) is not int or value["schema_version"] != 1):
            raise ValueError("application workload saved fields are invalid")
        if any(not isinstance(value[key], str) for key in ("kind", "name", "uid", "shape_digest")):
            raise ValueError("application workload saved identity is invalid")
        original = value["original_value"]
        if not isinstance(original, (int, bool)) or type(value["original_present"]) is not bool:
            raise ValueError("application workload saved value is invalid")
        return cls(str(value["kind"]), str(value["name"]), str(value["uid"]),
                   str(value["shape_digest"]), original, bool(value["original_present"]))

    def patch(self, document: Mapping[str, object], *, recovering: bool) -> list[dict[str, object]] | None:
        """Prepare only the original-to-paused or paused-to-original transition.

        UID, resourceVersion and the entire live spec are tested atomically at
        the API server. A lost reply can be reconciled by either exact endpoint;
        a replacement object or third value cannot become a new baseline.
        """
        if (_identity(document) != (self.kind, self.name, self.uid)
                or _shape(document, self.kind) != self.shape_digest):
            raise ValueError("application workload saved identity or spec changed")
        metadata, spec = _mapping(document["metadata"]), _mapping(document.get("spec"))
        field = _field(self.kind)
        current, present = _value(spec, self.kind), field in spec
        paused = 0 if self.kind == "Deployment" else True
        if (current, present) not in ((self.original_value, self.original_present), (paused, True)):
            raise ValueError("application workload has an unowned scale or suspend transition")
        desired, desired_present = (self.original_value, self.original_present) if recovering else (paused, True)
        if (current, present) == (desired, desired_present):
            return None
        change: dict[str, object] = {"op": "add" if desired_present else "remove", "path": f"/spec/{field}"}
        if desired_present:
            change["value"] = desired
        return [
            {"op": "test", "path": "/metadata/uid", "value": self.uid},
            {"op": "test", "path": "/metadata/resourceVersion", "value": metadata["resourceVersion"]},
            {"op": "test", "path": "/spec", "value": copy.deepcopy(spec)},
            change,
        ]


def validate_workload_inventory(workloads: tuple[ApplicationWorkload, ...]) -> tuple[ApplicationWorkload, ...]:
    """Require the complete fixed writer set and unique names and UIDs."""
    if (not 8 <= len(workloads) <= 128
            or any(type(item) is not ApplicationWorkload for item in workloads)):
        raise ValueError("application workload inventory is incomplete or unbounded")
    for item in workloads:
        ApplicationWorkload.from_dict(item.to_dict())
    names = {(item.kind, item.name) for item in workloads}
    required = {("Deployment", name) for name in APPLICATION_DEPLOYMENTS} | {("CronJob", APPLICATION_CRONJOB)}
    if (not required <= names or len(names) != len(workloads)
            or len({item.uid for item in workloads}) != len(workloads)):
        raise ValueError("application workload inventory is incomplete or repeats identities")
    return tuple(sorted(workloads, key=lambda item: (item.kind, item.name)))
