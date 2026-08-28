"""Controller-scoped external-supervisor transition authority."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType

from loom_cli.rollout.external_supervisor_predecessor import (
    ABSENT_PREDECESSOR_DIGEST,
    EXTERNAL_SUPERVISOR_CONTROLLER_HOSTS,
    external_supervisor_unit_directory,
    external_supervisor_unit_set_digest_or_empty,
)

_HOST_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9.-]{0,252}$")
_UNIT_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.@-]*[.](?:service|timer)$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_UNIT_PREFIX = "unit/"
_REQUIRED_FIELDS = frozenset(
    {
        "authority-kind",
        "authority-digest",
        "pointer-digest",
        "unit-set-digest",
        "live-evidence-digest",
        "pending-transition-digest",
        "runtime-state",
        "transition-digest",
        "unit-directory",
    }
)


@dataclass(frozen=True, slots=True)
class ExternalSupervisorControllerBinding:
    """One controller's exact predecessor-to-candidate transition."""

    execution_host: str
    predecessor_kind: str
    predecessor_digest: str
    predecessor_pointer_digest: str
    predecessor_unit_sha256: Mapping[str, str]
    predecessor_unit_set_digest: str
    predecessor_live_evidence_digest: str
    predecessor_pending_transition_digest: str
    predecessor_runtime_state: str
    unit_directory: str
    transition_digest: str

    def __post_init__(self) -> None:
        units = dict(self.predecessor_unit_sha256)
        absent = self.predecessor_kind == "absent"
        try:
            expected_unit_directory = external_supervisor_unit_directory(self.execution_host)
        except ValueError as exc:
            raise ValueError("external supervisor controller binding is invalid") from exc
        if (
            _HOST_RE.fullmatch(self.execution_host) is None
            or self.predecessor_kind not in {"legacy-manifest", "canonical", "absent"}
            or self.predecessor_runtime_state not in {"ready", "repairable"}
            or (
                self.predecessor_runtime_state == "repairable"
                and self.predecessor_kind != "canonical"
            )
            or self.unit_directory != expected_unit_directory
            or bool(units) == absent
            or any(
                _UNIT_RE.fullmatch(name) is None or _SHA256_RE.fullmatch(digest) is None
                for name, digest in units.items()
            )
            or any(
                _SHA256_RE.fullmatch(value) is None
                for value in (
                    self.predecessor_digest,
                    self.predecessor_pointer_digest,
                    self.predecessor_unit_set_digest,
                    self.predecessor_live_evidence_digest,
                    self.predecessor_pending_transition_digest,
                    self.transition_digest,
                )
            )
            or (self.predecessor_digest == ABSENT_PREDECESSOR_DIGEST) != absent
            or external_supervisor_unit_set_digest_or_empty(units)
            != self.predecessor_unit_set_digest
            or (
                self.predecessor_kind == "legacy-manifest"
                and self.predecessor_pointer_digest != ABSENT_PREDECESSOR_DIGEST
            )
            or (
                self.predecessor_kind == "canonical"
                and self.predecessor_pointer_digest == ABSENT_PREDECESSOR_DIGEST
            )
            or (absent and self.predecessor_pointer_digest != ABSENT_PREDECESSOR_DIGEST)
        ):
            raise ValueError("external supervisor controller binding is invalid")
        object.__setattr__(
            self,
            "predecessor_unit_sha256",
            MappingProxyType(dict(sorted(units.items()))),
        )

    @classmethod
    def build(
        cls,
        *,
        execution_host: str,
        candidate_sha: str,
        candidate_tree: str,
        environment: str,
        predecessor_kind: str,
        predecessor_digest: str,
        predecessor_pointer_digest: str,
        predecessor_unit_sha256: Mapping[str, str],
        predecessor_unit_set_digest: str,
        predecessor_live_evidence_digest: str,
        predecessor_pending_transition_digest: str,
        predecessor_runtime_state: str,
        unit_directory: str,
        target_artifact_digest: str,
        target_profile_sha256: str,
        target_script_sha256: Mapping[str, str],
        target_unit_sha256: Mapping[str, str],
        target_unit_set_digest: str,
    ) -> ExternalSupervisorControllerBinding:
        from loom_cli.rollout.preflight_contract import external_supervisor_transition_digest

        return cls(
            execution_host=execution_host,
            predecessor_kind=predecessor_kind,
            predecessor_digest=predecessor_digest,
            predecessor_pointer_digest=predecessor_pointer_digest,
            predecessor_unit_sha256=predecessor_unit_sha256,
            predecessor_unit_set_digest=predecessor_unit_set_digest,
            predecessor_live_evidence_digest=predecessor_live_evidence_digest,
            predecessor_pending_transition_digest=predecessor_pending_transition_digest,
            predecessor_runtime_state=predecessor_runtime_state,
            unit_directory=unit_directory,
            transition_digest=external_supervisor_transition_digest(
                unit_directory=unit_directory,
                candidate_sha=candidate_sha,
                candidate_tree=candidate_tree,
                environment=environment,
                predecessor_kind=predecessor_kind,
                predecessor_digest=predecessor_digest,
                predecessor_pointer_digest=predecessor_pointer_digest,
                predecessor_unit_sha256=predecessor_unit_sha256,
                predecessor_unit_set_digest=predecessor_unit_set_digest,
                predecessor_live_evidence_digest=predecessor_live_evidence_digest,
                predecessor_pending_transition_digest=predecessor_pending_transition_digest,
                target_artifact_digest=target_artifact_digest,
                target_profile_sha256=target_profile_sha256,
                target_script_sha256=target_script_sha256,
                target_unit_sha256=target_unit_sha256,
                target_unit_set_digest=target_unit_set_digest,
            ),
        )

    def to_flat_map(self) -> dict[str, str]:
        prefix = f"{self.execution_host}/"
        return {
            f"{prefix}authority-kind": self.predecessor_kind,
            f"{prefix}authority-digest": self.predecessor_digest,
            f"{prefix}pointer-digest": self.predecessor_pointer_digest,
            f"{prefix}unit-set-digest": self.predecessor_unit_set_digest,
            f"{prefix}live-evidence-digest": self.predecessor_live_evidence_digest,
            f"{prefix}pending-transition-digest": (self.predecessor_pending_transition_digest),
            f"{prefix}runtime-state": self.predecessor_runtime_state,
            f"{prefix}unit-directory": self.unit_directory,
            f"{prefix}transition-digest": self.transition_digest,
            **{
                f"{prefix}{_UNIT_PREFIX}{name}": digest
                for name, digest in self.predecessor_unit_sha256.items()
            },
        }

    def predecessor_identity_flat_map(self) -> dict[str, str]:
        """Return only durable predecessor authority for admission comparison."""
        prefix = f"{self.execution_host}/"
        return {
            f"{prefix}authority-kind": self.predecessor_kind,
            f"{prefix}authority-digest": self.predecessor_digest,
            f"{prefix}pointer-digest": self.predecessor_pointer_digest,
            f"{prefix}unit-set-digest": self.predecessor_unit_set_digest,
            f"{prefix}pending-transition-digest": (self.predecessor_pending_transition_digest),
            f"{prefix}unit-directory": self.unit_directory,
            **{
                f"{prefix}{_UNIT_PREFIX}{name}": digest
                for name, digest in self.predecessor_unit_sha256.items()
            },
        }


def encode_external_supervisor_controller_bindings(
    bindings: Mapping[str, ExternalSupervisorControllerBinding],
) -> dict[str, str]:
    if set(bindings) != EXTERNAL_SUPERVISOR_CONTROLLER_HOSTS or any(
        host != binding.execution_host for host, binding in bindings.items()
    ):
        raise ValueError("external supervisor controller binding set is invalid")
    encoded: dict[str, str] = {}
    for host in sorted(bindings):
        for key, value in bindings[host].to_flat_map().items():
            if key in encoded:
                raise ValueError("external supervisor controller binding fields overlap")
            encoded[key] = value
    return encoded


def encode_external_supervisor_predecessor_identities(
    bindings: Mapping[str, ExternalSupervisorControllerBinding],
) -> dict[str, str]:
    """Encode the durable identity projection of a complete controller set."""
    if set(bindings) != EXTERNAL_SUPERVISOR_CONTROLLER_HOSTS or any(
        host != binding.execution_host for host, binding in bindings.items()
    ):
        raise ValueError("external supervisor controller binding set is invalid")
    encoded: dict[str, str] = {}
    for host in sorted(bindings):
        for key, value in bindings[host].predecessor_identity_flat_map().items():
            if key in encoded:
                raise ValueError("external supervisor predecessor identity fields overlap")
            encoded[key] = value
    return encoded


def parse_external_supervisor_controller_bindings(
    values: Mapping[str, str],
) -> Mapping[str, ExternalSupervisorControllerBinding]:
    if not values or len(values) > 64:
        raise ValueError("external supervisor controller binding set is invalid")
    grouped: dict[str, dict[str, str]] = {}
    for key, value in values.items():
        host, separator, field = key.partition("/")
        if (
            not separator
            or _HOST_RE.fullmatch(host) is None
            or not field
            or not isinstance(value, str)
        ):
            raise ValueError("external supervisor controller binding field is invalid")
        fields = grouped.setdefault(host, {})
        if field in fields:
            raise ValueError("external supervisor controller binding field is duplicated")
        fields[field] = value

    if set(grouped) != EXTERNAL_SUPERVISOR_CONTROLLER_HOSTS:
        raise ValueError("external supervisor controller binding set is invalid")

    result: dict[str, ExternalSupervisorControllerBinding] = {}
    for host, fields in grouped.items():
        unit_fields = {key: value for key, value in fields.items() if key.startswith(_UNIT_PREFIX)}
        scalar_fields = set(fields) - set(unit_fields)
        if scalar_fields != _REQUIRED_FIELDS:
            raise ValueError("external supervisor controller binding fields are incomplete")
        units = {key.removeprefix(_UNIT_PREFIX): value for key, value in unit_fields.items()}
        result[host] = ExternalSupervisorControllerBinding(
            execution_host=host,
            predecessor_kind=fields["authority-kind"],
            predecessor_digest=fields["authority-digest"],
            predecessor_pointer_digest=fields["pointer-digest"],
            predecessor_unit_sha256=units,
            predecessor_unit_set_digest=fields["unit-set-digest"],
            predecessor_live_evidence_digest=fields["live-evidence-digest"],
            predecessor_pending_transition_digest=fields["pending-transition-digest"],
            predecessor_runtime_state=fields["runtime-state"],
            unit_directory=fields["unit-directory"],
            transition_digest=fields["transition-digest"],
        )
    return MappingProxyType(dict(sorted(result.items())))


__all__ = [
    "ExternalSupervisorControllerBinding",
    "encode_external_supervisor_controller_bindings",
    "encode_external_supervisor_predecessor_identities",
    "parse_external_supervisor_controller_bindings",
]
