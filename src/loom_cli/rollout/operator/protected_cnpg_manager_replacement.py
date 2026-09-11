"""Exact same-process CNPG replacement records for the protected handoff.

These records do not establish administrator exclusion, process admission or
SQL retirement. The composer must retain those prerequisites across dispatch
and reconciliation. A dispatched but unobserved PUT is never safe to repeat.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import asdict, dataclass, fields, replace

CNPG_MANAGER_IMAGE = (
    "ghcr.io/cloudnative-pg/cloudnative-pg@sha256:"
    "b5210df46c05bed3c5dbb67d316dece0ed67f4d148acac169416079dc10e4a91"
)
# Independently extracted /operator/manager_amd64 from the never-started pinned
# image. The handoff currently supports only OLDLAB PostgreSQL primaries.
CNPG_MANAGER_SHA256 = "0a8f22a9c14805f67b92f6994d6487da7570929108443d1a70a66b8d47a51b2f"
CNPG_MANAGER_SIZE = 61_046_968
_SHA = re.compile(r"[0-9a-f]{64}\Z")
_UID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\Z")


def _hash(value: Mapping[str, object]) -> str:
    return hashlib.sha256(json.dumps(
        dict(value), sort_keys=True, separators=(",", ":"), allow_nan=False,
    ).encode()).hexdigest()


def _string(value: Mapping[str, object], name: str) -> str:
    item = value[name]
    if not isinstance(item, str):
        raise ValueError("CNPG manager record string is invalid")
    return item


def _mapping(value: object) -> dict[str, object]:
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise ValueError("CNPG manager record object is invalid")
    return value


@dataclass(frozen=True, slots=True)
class CNPGManagerIdentity:
    pod_name: str
    pod_uid: str
    container_id: str
    node_name: str
    restart_count: int
    process_started_ticks: int
    executable_device: int
    executable_inode: int

    def __post_init__(self) -> None:
        if (
            not isinstance(self.pod_name, str)
            or re.fullmatch(r"loom-postgres-[1-9][0-9]{0,5}", self.pod_name) is None
            or not isinstance(self.pod_uid, str) or _UID.fullmatch(self.pod_uid) is None
            or not isinstance(self.container_id, str)
            or re.fullmatch(r"containerd://[0-9a-f]{64}", self.container_id) is None
            or self.node_name not in {"trt-eai-oldlab-3", "trt-eai-oldlab-4", "trt-eai-oldlab-5"}
            or type(self.restart_count) is not int or not 0 <= self.restart_count < 2**31
            or any(type(value) is not int or not 0 < value < 2**64 for value in (
                self.process_started_ticks, self.executable_device, self.executable_inode,
            ))
        ):
            raise ValueError("CNPG manager identity is unsupported")

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> CNPGManagerIdentity:
        if set(value) != {field.name for field in fields(cls)}:
            raise ValueError("CNPG manager identity fields are invalid")
        names = ("restart_count", "process_started_ticks", "executable_device", "executable_inode")
        integers = [value[name] for name in names]
        if any(type(item) is not int for item in integers):
            raise ValueError("CNPG manager identity numbers are invalid")
        return cls(
            pod_name=_string(value, "pod_name"),
            pod_uid=_string(value, "pod_uid"),
            container_id=_string(value, "container_id"),
            node_name=_string(value, "node_name"),
            restart_count=int(str(integers[0])),
            process_started_ticks=int(str(integers[1])),
            executable_device=int(str(integers[2])),
            executable_inode=int(str(integers[3])),
        )


@dataclass(frozen=True, slots=True)
class CNPGManagerReplacementIntent:
    component_intent_digest: str
    admission_digest: str
    identity: CNPGManagerIdentity

    def __post_init__(self) -> None:
        if type(self.identity) is not CNPGManagerIdentity or any(
            not isinstance(value, str) or _SHA.fullmatch(value) is None
            for value in (self.component_intent_digest, self.admission_digest)
        ):
            raise ValueError("CNPG manager replacement intent is invalid")

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": 1, **asdict(self),
            "manager_image": CNPG_MANAGER_IMAGE,
            "manager_sha256": CNPG_MANAGER_SHA256,
            "manager_size": CNPG_MANAGER_SIZE,
        }

    @property
    def digest(self) -> str:
        return _hash(self.to_dict())

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> CNPGManagerReplacementIntent:
        expected = {
            "schema_version", "component_intent_digest", "admission_digest", "identity",
            "manager_image", "manager_sha256", "manager_size",
        }
        if (set(value) != expected or type(value["schema_version"]) is not int
                or value["schema_version"] != 1 or type(value["manager_size"]) is not int
                or value["manager_size"] != CNPG_MANAGER_SIZE
                or value["manager_image"] != CNPG_MANAGER_IMAGE
                or value["manager_sha256"] != CNPG_MANAGER_SHA256):
            raise ValueError("CNPG manager replacement intent profile changed")
        return cls(
            _string(value, "component_intent_digest"), _string(value, "admission_digest"),
            CNPGManagerIdentity.from_dict(_mapping(value["identity"])),
        )


@dataclass(frozen=True, slots=True)
class CNPGManagerReplacementReceipt:
    intent_digest: str
    identity: CNPGManagerIdentity

    def to_dict(self) -> dict[str, object]:
        return {"schema_version": 1, **asdict(self)}

    @classmethod
    def validate(
        cls, intent: CNPGManagerReplacementIntent, identity: CNPGManagerIdentity,
    ) -> CNPGManagerReplacementReceipt:
        if (type(identity) is not CNPGManagerIdentity
                or identity.executable_inode == intent.identity.executable_inode
                or replace(identity, executable_inode=intent.identity.executable_inode) != intent.identity):
            raise ValueError("CNPG manager executable transition changed process or volume")
        return cls(intent.digest, identity)

    @classmethod
    def from_dict(
        cls, value: Mapping[str, object], *, intent: CNPGManagerReplacementIntent,
    ) -> CNPGManagerReplacementReceipt:
        if (set(value) != {"schema_version", "intent_digest", "identity"}
                or type(value["schema_version"]) is not int or value["schema_version"] != 1
                or value["intent_digest"] != intent.digest):
            raise ValueError("CNPG manager replacement receipt binding changed")
        return cls.validate(intent, CNPGManagerIdentity.from_dict(_mapping(value["identity"])))
