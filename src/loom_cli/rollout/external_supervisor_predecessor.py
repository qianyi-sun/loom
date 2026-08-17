"""Durable predecessor authority for protected external supervisors."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from importlib import resources
from pathlib import Path
from types import MappingProxyType
from typing import cast

from loom_cli.rollout.external_supervisor_readiness import (
    PROTECTED_EXTERNAL_SUPERVISOR_UNIT_RE,
    ExternalSupervisorArtifact,
)

DEFAULT_PREDECESSOR_MANIFEST = resources.files("loom_cli.data").joinpath(
    "staging-external-supervisor-predecessor.json"
)
OLDLAB_PREDECESSOR_MANIFEST = resources.files("loom_cli.data").joinpath(
    "staging-oldlab-external-supervisor-predecessor.json"
)

_SHA_RE = re.compile(r"^(?:[0-9a-f]{40}|[0-9a-f]{64})$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_TRANSITION_GROUP_RE = re.compile(r"^[0-9a-f]{32}$")
_MAX_BYTES = 4 * 1024 * 1024
ABSENT_PREDECESSOR_DIGEST = hashlib.sha256(b"loom-external-supervisor-absent-v1").hexdigest()
NO_TRANSITION_GROUP_ID = hashlib.sha256(b"loom-external-supervisor-no-transition-v1").hexdigest()[
    :32
]
PROTECTED_CANONICAL_UNIT_DIR = "/var/lib/loom-staging-rollout/.config/systemd/user"
GB10_CANONICAL_UNIT_DIR = "/var/lib/loom-rollout/.config/systemd/user"
EXTERNAL_SUPERVISOR_CONTROLLER_HOSTS = frozenset(
    {"gx10-01c7", "TRT-EAI-OLDLAB-1"}
)
_CONTROLLER_UNIT_DIRECTORIES = {
    "gx10-01c7": GB10_CANONICAL_UNIT_DIR,
    "trt-eai-oldlab-1": PROTECTED_CANONICAL_UNIT_DIR,
}

# This is the exact canonical file rendered from the merged PR #1342 tree.  The
# self-describing digest inside the JSON proves internal consistency, but it
# must not let a later candidate redefine which predecessor has authority.
PR1342_PREDECESSOR_BYTES_SHA256 = (
    "da532309565f9111f032debdc2d3e6677f9a85c89d077ebd45bc4b88117f6370"
)
PR1342_PREDECESSOR_SOURCE_COMMIT = "39bc56eb29b89c9c9da6247ec513cb619e356960"
PR1342_PREDECESSOR_SOURCE_TREE = "3e936b139de1344e1a6120375b9e065a2c458c6b"
PR1342_PREDECESSOR_MANIFEST_DIGEST = (
    "217acbf8ceaecf7967bbb5da4bd6464fe252377fdc822319b3bd643387f9ffee"
)
PR1342_PREDECESSOR_UNIT_SET_DIGEST = (
    "6061630942ec642562306683b623286116370d7e16ef5be3a9e23fea019f071a"
)
PR1197_PREDECESSOR_BYTES_SHA256 = "2539d6d8f7bb3a8ef1e68dd915ddaeca8eb86fdd2e28673a5277f280305115bf"
PR1197_PREDECESSOR_SOURCE_COMMIT = "78fbcbacb6dcdebb577692c1257f6e2226b73de6"
PR1197_PREDECESSOR_SOURCE_TREE = "bfabdac8be9c7e4b187addf3e6fa099ced0a7122"
PR1197_PREDECESSOR_MANIFEST_DIGEST = (
    "f36284e8220ee514e6c057e767bc01b296212bd7894dcfccd9e277234adaf253"
)
PR1197_PREDECESSOR_UNIT_SET_DIGEST = (
    "9b7f83352ebbe7e4af996094462c9241cb71b642c3c64ee6dbc7de8086c750a9"
)

_OLDLAB_EXECUTION_HOST = "trt-eai-oldlab-1"

_POOL_IDENTITY_TABLES = (
    "gb10_worker_node_statuses",
    "gb10_worker_pool_desired_states",
    "slurm_worker_jobs",
    "worker_pool_autoscaler_policies",
    "workers",
)


def _hash_json(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def external_supervisor_unit_directory(execution_host: str) -> str:
    """Return the fixed service-owned user-systemd directory for one controller."""

    if type(execution_host) is not str or not execution_host:
        raise ValueError("external supervisor execution host is invalid")
    normalized = execution_host.split(".", 1)[0].casefold()
    try:
        return _CONTROLLER_UNIT_DIRECTORIES[normalized]
    except KeyError as exc:
        raise ValueError("external supervisor execution host is unknown") from exc


def external_supervisor_unit_set_digest(unit_sha256: Mapping[str, str]) -> str:
    """Return the canonical digest for one complete paired supervisor unit set."""

    names = set(unit_sha256)
    services = {name.removesuffix(".service") for name in names if name.endswith(".service")}
    timers = {name.removesuffix(".timer") for name in names if name.endswith(".timer")}
    if (
        not names
        or services != timers
        or any(
            type(name) is not str
            or PROTECTED_EXTERNAL_SUPERVISOR_UNIT_RE.fullmatch(name) is None
            for name in names
        )
        or any(
            type(value) is not str or _SHA256_RE.fullmatch(value) is None
            for value in unit_sha256.values()
        )
    ):
        raise ValueError("external supervisor unit set is invalid")
    return _hash_json({"units": dict(sorted(unit_sha256.items()))})


# Canonical digest of the empty unit set, used only by an absent predecessor
# (first introduction of the supervisor). ``external_supervisor_unit_set_digest``
# deliberately rejects an empty set so a *present* predecessor can never claim
# zero units; the absent case carries this sentinel instead.
EMPTY_EXTERNAL_SUPERVISOR_UNIT_SET_DIGEST = _hash_json({"units": {}})


def external_supervisor_unit_set_digest_or_empty(unit_sha256: Mapping[str, str]) -> str:
    """Unit-set digest tolerant of an absent predecessor (empty unit set)."""

    if not unit_sha256:
        return EMPTY_EXTERNAL_SUPERVISOR_UNIT_SET_DIGEST
    return external_supervisor_unit_set_digest(unit_sha256)


def _strict_json_object(payload: bytes, *, label: str) -> dict[str, object]:
    def reject_duplicates(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"{label} contains duplicate keys")
            result[key] = value
        return result

    try:
        raw = json.loads(payload.decode("utf-8"), object_pairs_hook=reject_duplicates)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"{label} is invalid") from exc
    if not isinstance(raw, dict):
        raise ValueError(f"{label} is invalid")
    return raw


def _canonical_maps(
    payloads: Mapping[str, str],
    digests: Mapping[str, str],
) -> tuple[Mapping[str, str], Mapping[str, str]]:
    raw_payloads = dict(payloads)
    raw_digests = dict(digests)
    names = set(raw_payloads)
    services = {name.removesuffix(".service") for name in names if name.endswith(".service")}
    timers = {name.removesuffix(".timer") for name in names if name.endswith(".timer")}
    if (
        not names
        or names != set(raw_digests)
        or services != timers
        or any(
            type(name) is not str
            or PROTECTED_EXTERNAL_SUPERVISOR_UNIT_RE.fullmatch(name) is None
            for name in names
        )
        or any(
            not isinstance(payload, str)
            or not payload.endswith("\n")
            or "\x00" in payload
            or len(payload.encode()) > 128 * 1024
            or type(raw_digests[name]) is not str
            or _SHA256_RE.fullmatch(raw_digests[name]) is None
            or hashlib.sha256(payload.encode()).hexdigest() != raw_digests[name]
            for name, payload in raw_payloads.items()
        )
    ):
        raise ValueError("external supervisor predecessor unit identity is invalid")
    return (
        MappingProxyType(dict(sorted(raw_payloads.items()))),
        MappingProxyType(dict(sorted(raw_digests.items()))),
    )


@dataclass(frozen=True, slots=True)
class ExternalSupervisorPredecessorManifest:
    schema_version: int
    authority_id: str
    source_commit: str
    source_tree: str
    source_file_sha256: Mapping[str, str]
    environment: str
    unit_payloads: Mapping[str, str]
    unit_sha256: Mapping[str, str]
    unit_set_digest: str
    manifest_digest: str

    def __post_init__(self) -> None:
        payloads, digests = _canonical_maps(self.unit_payloads, self.unit_sha256)
        source_files = dict(self.source_file_sha256)
        if (
            self.schema_version != 1
            or type(self.authority_id) is not str
            or self.authority_id not in {"merged-pr-1197", "merged-pr-1342"}
            or type(self.source_commit) is not str
            or _SHA_RE.fullmatch(self.source_commit) is None
            or type(self.source_tree) is not str
            or _SHA_RE.fullmatch(self.source_tree) is None
            or type(self.environment) is not str
            or self.environment != "staging"
            or set(source_files)
            != {
                "deploy/environment-state/staging.toml",
                "scripts/ops/worker_pool_autoscaler_external_once.py",
                "src/loom_cli/environment_state.py",
            }
            or any(
                type(value) is not str or _SHA256_RE.fullmatch(value) is None
                for value in source_files.values()
            )
            or type(self.manifest_digest) is not str
            or _SHA256_RE.fullmatch(self.manifest_digest) is None
            or type(self.unit_set_digest) is not str
            or _SHA256_RE.fullmatch(self.unit_set_digest) is None
        ):
            raise ValueError("external supervisor predecessor manifest identity is invalid")
        object.__setattr__(self, "unit_payloads", payloads)
        object.__setattr__(self, "unit_sha256", digests)
        object.__setattr__(
            self,
            "source_file_sha256",
            MappingProxyType(dict(sorted(source_files.items()))),
        )
        if external_supervisor_unit_set_digest(digests) != self.unit_set_digest:
            raise ValueError("external supervisor predecessor unit set drifted")
        if _hash_json(self.payload()) != self.manifest_digest:
            raise ValueError("external supervisor predecessor manifest digest drifted")

    def payload(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "authority_id": self.authority_id,
            "source_commit": self.source_commit,
            "source_tree": self.source_tree,
            "source_file_sha256": dict(self.source_file_sha256),
            "environment": self.environment,
            "unit_payloads": dict(self.unit_payloads),
            "unit_sha256": dict(self.unit_sha256),
            "unit_set_digest": self.unit_set_digest,
        }

    def to_bytes(self) -> bytes:
        return (
            json.dumps(
                {**self.payload(), "manifest_digest": self.manifest_digest},
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
            + b"\n"
        )

    @property
    def pool_identity_predecessor_kind(self) -> str:
        """Return the GB10 rename authority expected for this supervisor snapshot."""

        if self.authority_id in {"merged-pr-1197", "merged-pr-1342"}:
            return "canonical"
        raise ValueError("external supervisor predecessor authority is invalid")

    @classmethod
    def from_bytes(cls, payload: bytes) -> ExternalSupervisorPredecessorManifest:
        if not 1 <= len(payload) <= _MAX_BYTES:
            raise ValueError("external supervisor predecessor manifest bytes are invalid")
        raw = _strict_json_object(
            payload,
            label="external supervisor predecessor manifest",
        )
        if set(raw) != set(cls.__dataclass_fields__):
            raise ValueError("external supervisor predecessor manifest fields are invalid")
        payloads = raw.get("unit_payloads")
        digests = raw.get("unit_sha256")
        source_files = raw.get("source_file_sha256")
        if (
            not isinstance(payloads, dict)
            or not isinstance(digests, dict)
            or not isinstance(source_files, dict)
        ):
            raise ValueError("external supervisor predecessor manifest maps are invalid")
        manifest = cls(
            schema_version=cast(int, raw.get("schema_version")),
            authority_id=cast(str, raw.get("authority_id")),
            source_commit=cast(str, raw.get("source_commit")),
            source_tree=cast(str, raw.get("source_tree")),
            source_file_sha256=source_files,
            environment=cast(str, raw.get("environment")),
            unit_payloads=payloads,
            unit_sha256=digests,
            unit_set_digest=cast(str, raw.get("unit_set_digest")),
            manifest_digest=cast(str, raw.get("manifest_digest")),
        )
        if payload != manifest.to_bytes():
            raise ValueError("external supervisor predecessor manifest encoding is not canonical")
        return manifest


@dataclass(frozen=True, slots=True)
class ExternalSupervisorPoolIdentity:
    """Read-only DB identity governing the one-way 0067 supervisor transition."""

    schema_revision: str
    legacy_rows: Mapping[str, int]
    target_rows: Mapping[str, int]
    evidence_digest: str

    def __post_init__(self) -> None:
        legacy = dict(self.legacy_rows)
        target = dict(self.target_rows)
        if (
            type(self.schema_revision) is not str
            or re.fullmatch(r"[0-9]{4}", self.schema_revision) is None
            or set(legacy) != set(_POOL_IDENTITY_TABLES)
            or set(target) != set(_POOL_IDENTITY_TABLES)
            or any(
                type(count) is not int or count < 0
                for count in (*legacy.values(), *target.values())
            )
            or type(self.evidence_digest) is not str
            or _SHA256_RE.fullmatch(self.evidence_digest) is None
        ):
            raise ValueError("external supervisor database pool identity is invalid")
        object.__setattr__(self, "legacy_rows", MappingProxyType(dict(sorted(legacy.items()))))
        object.__setattr__(self, "target_rows", MappingProxyType(dict(sorted(target.items()))))
        if _hash_json(self.payload()) != self.evidence_digest:
            raise ValueError("external supervisor database pool identity digest drifted")

    @classmethod
    def build(
        cls,
        *,
        schema_revision: str,
        legacy_rows: Mapping[str, int],
        target_rows: Mapping[str, int],
    ) -> ExternalSupervisorPoolIdentity:
        payload = {
            "schema_revision": schema_revision,
            "legacy_rows": dict(sorted(legacy_rows.items())),
            "target_rows": dict(sorted(target_rows.items())),
        }
        return cls(**payload, evidence_digest=_hash_json(payload))  # type: ignore[arg-type]

    def payload(self) -> dict[str, object]:
        return {
            "schema_revision": self.schema_revision,
            "legacy_rows": dict(self.legacy_rows),
            "target_rows": dict(self.target_rows),
        }

    def require_predecessor_kind(self, kind: str) -> None:
        """Reject target collisions before 0067 and every legacy rollback after it."""

        if type(kind) is not str:
            raise ValueError("external supervisor predecessor kind is invalid")
        # An absent predecessor (first introduction of the supervisor: no units
        # live, no canonical record) has no supervisor to place in the worker
        # pool, so there is no predecessor pool identity to enforce. The
        # gb10-arm64->gb10 rename state is validated by the rollout migration and
        # the *target* pool identity, not by an absent predecessor -- gating it
        # here would wedge the first rollout on a live database whose lineage
        # predates the rename (the migration that reconciles it runs, atomically
        # with the deploy, only after this admission gate).
        if kind == "absent":
            return
        revision = int(self.schema_revision)
        if revision <= 66:
            if kind != "legacy-manifest" or any(self.target_rows.values()):
                raise ValueError("external supervisor pre-0067 pool identity drifted")
            return
        if kind != "canonical" or any(self.legacy_rows.values()):
            raise ValueError("external supervisor post-0067 pool identity drifted")


@dataclass(frozen=True, slots=True)
class ExternalSupervisorCanonicalIdentity:
    schema_version: int
    record_kind: str
    artifact_digest: str
    candidate_sha: str
    candidate_tree: str
    environment: str
    plan_digest: str
    attestation_digest: str
    transition_group_id: str
    unit_dir: str
    runtime_evidence_digest: str
    unit_payloads: Mapping[str, str]
    unit_sha256: Mapping[str, str]
    unit_set_digest: str
    evidence_digest: str

    def __post_init__(self) -> None:
        payloads, digests = _canonical_maps(self.unit_payloads, self.unit_sha256)
        if (
            self.schema_version != 3
            or type(self.record_kind) is not str
            or self.record_kind not in {"activation", "legacy-snapshot"}
            or type(self.artifact_digest) is not str
            or _SHA256_RE.fullmatch(self.artifact_digest) is None
            or type(self.candidate_sha) is not str
            or _SHA_RE.fullmatch(self.candidate_sha) is None
            or type(self.candidate_tree) is not str
            or _SHA_RE.fullmatch(self.candidate_tree) is None
            or self.environment != "staging"
            or type(self.transition_group_id) is not str
            or _TRANSITION_GROUP_RE.fullmatch(self.transition_group_id) is None
            or (
                self.record_kind == "activation"
                and self.transition_group_id == NO_TRANSITION_GROUP_ID
            )
            or (
                self.record_kind == "legacy-snapshot"
                and self.transition_group_id != NO_TRANSITION_GROUP_ID
            )
            or any(
                type(value) is not str or _SHA256_RE.fullmatch(value) is None
                for value in (
                    self.plan_digest,
                    self.attestation_digest,
                    self.runtime_evidence_digest,
                    self.unit_set_digest,
                    self.evidence_digest,
                )
            )
            or self.unit_dir not in set(_CONTROLLER_UNIT_DIRECTORIES.values())
            or not Path(self.unit_dir).is_absolute()
        ):
            raise ValueError("external supervisor canonical identity is invalid")
        object.__setattr__(self, "unit_payloads", payloads)
        object.__setattr__(self, "unit_sha256", digests)
        if external_supervisor_unit_set_digest(digests) != self.unit_set_digest:
            raise ValueError("external supervisor canonical unit set drifted")
        if _hash_json(self.payload()) != self.evidence_digest:
            raise ValueError("external supervisor canonical identity digest drifted")

    @classmethod
    def build(
        cls,
        artifact: ExternalSupervisorArtifact,
        *,
        plan_digest: str,
        attestation_digest: str,
        transition_group_id: str,
        runtime_evidence_digest: str,
        unit_dir: str = PROTECTED_CANONICAL_UNIT_DIR,
    ) -> ExternalSupervisorCanonicalIdentity:
        payload = {
            "schema_version": 3,
            "record_kind": "activation",
            "artifact_digest": artifact.artifact_digest,
            "candidate_sha": artifact.candidate_sha,
            "candidate_tree": artifact.candidate_tree,
            "environment": artifact.environment,
            "plan_digest": plan_digest,
            "attestation_digest": attestation_digest,
            "transition_group_id": transition_group_id,
            "unit_dir": unit_dir,
            "runtime_evidence_digest": runtime_evidence_digest,
            "unit_payloads": {
                name: unit
                for supervisor in artifact.supervisors
                for name, unit in (
                    (supervisor.service_name, supervisor.service_unit),
                    (supervisor.timer_name, supervisor.timer_unit),
                )
            },
            "unit_sha256": dict(artifact.unit_sha256),
            "unit_set_digest": external_supervisor_unit_set_digest(artifact.unit_sha256),
        }
        return cls(**payload, evidence_digest=_hash_json(payload))  # type: ignore[arg-type]

    @classmethod
    def from_manifest(
        cls,
        manifest: ExternalSupervisorPredecessorManifest,
        *,
        unit_dir: str = PROTECTED_CANONICAL_UNIT_DIR,
    ) -> ExternalSupervisorCanonicalIdentity:
        payload = {
            "schema_version": 3,
            "record_kind": "legacy-snapshot",
            "artifact_digest": manifest.manifest_digest,
            "candidate_sha": manifest.source_commit,
            "candidate_tree": manifest.source_tree,
            "environment": manifest.environment,
            "plan_digest": manifest.manifest_digest,
            "attestation_digest": manifest.manifest_digest,
            "transition_group_id": NO_TRANSITION_GROUP_ID,
            "unit_dir": unit_dir,
            "runtime_evidence_digest": manifest.manifest_digest,
            "unit_payloads": dict(manifest.unit_payloads),
            "unit_sha256": dict(manifest.unit_sha256),
            "unit_set_digest": manifest.unit_set_digest,
        }
        return cls(**payload, evidence_digest=_hash_json(payload))  # type: ignore[arg-type]

    def payload(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "record_kind": self.record_kind,
            "artifact_digest": self.artifact_digest,
            "candidate_sha": self.candidate_sha,
            "candidate_tree": self.candidate_tree,
            "environment": self.environment,
            "plan_digest": self.plan_digest,
            "attestation_digest": self.attestation_digest,
            "transition_group_id": self.transition_group_id,
            "unit_dir": self.unit_dir,
            "runtime_evidence_digest": self.runtime_evidence_digest,
            "unit_payloads": dict(self.unit_payloads),
            "unit_sha256": dict(self.unit_sha256),
            "unit_set_digest": self.unit_set_digest,
        }

    def to_bytes(self) -> bytes:
        return (
            json.dumps(
                {**self.payload(), "evidence_digest": self.evidence_digest},
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
            + b"\n"
        )

    @classmethod
    def from_bytes(cls, payload: bytes) -> ExternalSupervisorCanonicalIdentity:
        if not 1 <= len(payload) <= _MAX_BYTES:
            raise ValueError("external supervisor canonical bytes are invalid")
        raw = _strict_json_object(payload, label="external supervisor canonical record")
        if set(raw) != set(cls.__dataclass_fields__):
            raise ValueError("external supervisor canonical fields are invalid")
        payloads = raw.get("unit_payloads")
        digests = raw.get("unit_sha256")
        if not isinstance(payloads, dict) or not isinstance(digests, dict):
            raise ValueError("external supervisor canonical maps are invalid")
        record = cls(
            schema_version=cast(int, raw.get("schema_version")),
            record_kind=cast(str, raw.get("record_kind")),
            artifact_digest=cast(str, raw.get("artifact_digest")),
            candidate_sha=cast(str, raw.get("candidate_sha")),
            candidate_tree=cast(str, raw.get("candidate_tree")),
            environment=cast(str, raw.get("environment")),
            plan_digest=cast(str, raw.get("plan_digest")),
            attestation_digest=cast(str, raw.get("attestation_digest")),
            transition_group_id=cast(str, raw.get("transition_group_id")),
            unit_dir=cast(str, raw.get("unit_dir")),
            runtime_evidence_digest=cast(str, raw.get("runtime_evidence_digest")),
            unit_payloads=payloads,
            unit_sha256=digests,
            unit_set_digest=cast(str, raw.get("unit_set_digest")),
            evidence_digest=cast(str, raw.get("evidence_digest")),
        )
        if payload != record.to_bytes():
            raise ValueError("external supervisor canonical encoding is not canonical")
        return record


@dataclass(frozen=True, slots=True)
class ExternalSupervisorCanonicalPointer:
    schema_version: int
    activation_digest: str
    pointer_digest: str

    def __post_init__(self) -> None:
        if (
            self.schema_version != 1
            or type(self.activation_digest) is not str
            or _SHA256_RE.fullmatch(self.activation_digest) is None
            or type(self.pointer_digest) is not str
            or _SHA256_RE.fullmatch(self.pointer_digest) is None
            or _hash_json(self.payload()) != self.pointer_digest
        ):
            raise ValueError("external supervisor canonical pointer is invalid")

    @classmethod
    def build(
        cls,
        activation: ExternalSupervisorCanonicalIdentity,
    ) -> ExternalSupervisorCanonicalPointer:
        if activation.record_kind != "activation":
            raise ValueError("external supervisor canonical activation is invalid")
        payload = {
            "schema_version": 1,
            "activation_digest": activation.evidence_digest,
        }
        return cls(**payload, pointer_digest=_hash_json(payload))  # type: ignore[arg-type]

    def payload(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "activation_digest": self.activation_digest,
        }

    def to_bytes(self) -> bytes:
        return (
            json.dumps(
                {**self.payload(), "pointer_digest": self.pointer_digest},
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
            + b"\n"
        )

    @classmethod
    def from_bytes(cls, payload: bytes) -> ExternalSupervisorCanonicalPointer:
        if not 1 <= len(payload) <= 4096:
            raise ValueError("external supervisor canonical pointer bytes are invalid")
        raw = _strict_json_object(payload, label="external supervisor canonical pointer")
        if set(raw) != set(cls.__dataclass_fields__):
            raise ValueError("external supervisor canonical pointer fields are invalid")
        pointer = cls(
            schema_version=cast(int, raw.get("schema_version")),
            activation_digest=cast(str, raw.get("activation_digest")),
            pointer_digest=cast(str, raw.get("pointer_digest")),
        )
        if payload != pointer.to_bytes():
            raise ValueError("external supervisor canonical pointer encoding is not canonical")
        return pointer


@dataclass(frozen=True, slots=True)
class ExternalSupervisorPredecessorAuthority:
    kind: str
    authority_digest: str
    unit_sha256: Mapping[str, str]

    def __post_init__(self) -> None:
        digests = dict(self.unit_sha256)
        if (
            type(self.kind) is not str
            or self.kind not in {"absent", "legacy-manifest", "canonical"}
            or _SHA256_RE.fullmatch(self.authority_digest) is None
            or (self.kind == "absent") != (not digests)
            or any(
                type(name) is not str
                or PROTECTED_EXTERNAL_SUPERVISOR_UNIT_RE.fullmatch(name) is None
                or type(digest) is not str
                or _SHA256_RE.fullmatch(digest) is None
                for name, digest in digests.items()
            )
        ):
            raise ValueError("external supervisor predecessor authority is invalid")
        object.__setattr__(self, "unit_sha256", MappingProxyType(dict(sorted(digests.items()))))

    def to_dict(self) -> dict[str, object]:
        return {
            "kind": self.kind,
            "authority_digest": self.authority_digest,
            "unit_sha256": dict(self.unit_sha256),
        }

    @property
    def unit_set_digest(self) -> str:
        return external_supervisor_unit_set_digest(self.unit_sha256)

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> ExternalSupervisorPredecessorAuthority:
        if set(value) != {"kind", "authority_digest", "unit_sha256"}:
            raise ValueError("external supervisor predecessor authority fields are invalid")
        units = value.get("unit_sha256")
        if not isinstance(units, dict):
            raise ValueError("external supervisor predecessor authority units are invalid")
        return cls(
            kind=cast(str, value.get("kind")),
            authority_digest=cast(str, value.get("authority_digest")),
            unit_sha256=units,
        )


def load_predecessor_manifest(
    *,
    execution_host: str | None = None,
) -> ExternalSupervisorPredecessorManifest:
    normalized_host = None if execution_host is None else execution_host.split(".", 1)[0].casefold()
    if normalized_host in {None, "gx10-01c7"}:
        path = DEFAULT_PREDECESSOR_MANIFEST
        expected_bytes = PR1342_PREDECESSOR_BYTES_SHA256
        expected_commit = PR1342_PREDECESSOR_SOURCE_COMMIT
        expected_tree = PR1342_PREDECESSOR_SOURCE_TREE
        expected_manifest = PR1342_PREDECESSOR_MANIFEST_DIGEST
        expected_units = PR1342_PREDECESSOR_UNIT_SET_DIGEST
        label = "PR #1342"
    elif normalized_host == _OLDLAB_EXECUTION_HOST:
        path = OLDLAB_PREDECESSOR_MANIFEST
        expected_bytes = PR1197_PREDECESSOR_BYTES_SHA256
        expected_commit = PR1197_PREDECESSOR_SOURCE_COMMIT
        expected_tree = PR1197_PREDECESSOR_SOURCE_TREE
        expected_manifest = PR1197_PREDECESSOR_MANIFEST_DIGEST
        expected_units = PR1197_PREDECESSOR_UNIT_SET_DIGEST
        label = "PR #1197"
    else:
        raise ValueError("external supervisor predecessor execution host is unknown")

    payload = path.read_bytes()
    if hashlib.sha256(payload).hexdigest() != expected_bytes:
        raise ValueError(f"external supervisor {label} predecessor byte identity drifted")
    manifest = ExternalSupervisorPredecessorManifest.from_bytes(payload)
    if (
        manifest.source_commit != expected_commit
        or manifest.source_tree != expected_tree
        or manifest.manifest_digest != expected_manifest
        or manifest.unit_set_digest != expected_units
    ):
        raise ValueError(f"external supervisor {label} predecessor identity drifted")
    return manifest


__all__ = [
    "ABSENT_PREDECESSOR_DIGEST",
    "DEFAULT_PREDECESSOR_MANIFEST",
    "EXTERNAL_SUPERVISOR_CONTROLLER_HOSTS",
    "GB10_CANONICAL_UNIT_DIR",
    "NO_TRANSITION_GROUP_ID",
    "OLDLAB_PREDECESSOR_MANIFEST",
    "PR1197_PREDECESSOR_BYTES_SHA256",
    "PR1197_PREDECESSOR_MANIFEST_DIGEST",
    "PR1197_PREDECESSOR_SOURCE_COMMIT",
    "PR1197_PREDECESSOR_SOURCE_TREE",
    "PR1197_PREDECESSOR_UNIT_SET_DIGEST",
    "PR1342_PREDECESSOR_BYTES_SHA256",
    "PR1342_PREDECESSOR_MANIFEST_DIGEST",
    "PR1342_PREDECESSOR_SOURCE_COMMIT",
    "PR1342_PREDECESSOR_SOURCE_TREE",
    "PR1342_PREDECESSOR_UNIT_SET_DIGEST",
    "PROTECTED_CANONICAL_UNIT_DIR",
    "ExternalSupervisorCanonicalIdentity",
    "ExternalSupervisorCanonicalPointer",
    "ExternalSupervisorPoolIdentity",
    "ExternalSupervisorPredecessorAuthority",
    "ExternalSupervisorPredecessorManifest",
    "external_supervisor_unit_directory",
    "external_supervisor_unit_set_digest",
    "load_predecessor_manifest",
]
