"""Typed forced-SSH transport for the GB10 autoscaler controller supervisor."""

from __future__ import annotations

import base64
import binascii
import hashlib
import json
import os
import re
import stat
import sys
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Protocol, cast

from loom_cli.rollout.external_supervisor_predecessor import (
    ExternalSupervisorCanonicalIdentity,
    ExternalSupervisorPredecessorAuthority,
)
from loom_cli.rollout.external_supervisor_readiness import (
    STAGING_GB10_CONTROLLER_EXECUTION_HOST,
    ExternalSupervisorArtifact,
)
from loom_cli.rollout.gb10_slurm_acceptance import (
    GB10_SLURM_WORKER_HOSTS,
    GB10SlurmAcceptanceEvidence,
    validate_gb10_slurm_acceptance,
)

from .protected_capacity_execution_preparation_component import PreparedControllerRequest
from .protected_controller_discovery import ControllerDiscoveryRequest
from .protected_controller_prerequisite_component import ControllerPrerequisiteRequest
from .protected_external_supervisor_credential_transport import (
    ExternalSupervisorCredentialEvidence,
    FixedLocalExternalSupervisorCredentialTransport,
    ProtectedExternalSupervisorCredentialTransport,
)
from .protected_external_supervisor_transport import (
    COMPENSATION_RECONCILIATION_FAILURE_CODES,
    EXTERNAL_SUPERVISOR_APPLY_FAILURE_CODES,
    AtomicUserUnitStore,
    ExternalSupervisorApplyError,
    ExternalSupervisorCompensationError,
    ExternalSupervisorLiveObservation,
    FixedExternalSupervisorTransport,
    FixedUserSystemdControl,
    ProtectedExternalSupervisorTransport,
    ServiceRuntimeStatus,
    TimerRuntimeStatus,
)
from .protected_pool_credential_transport import PoolExecutionCredentialPayload

GB10_CONTROLLER_EXECUTION_HOST = STAGING_GB10_CONTROLLER_EXECUTION_HOST
GB10_CONTROLLER_SSH_HOST = "207.35.188.227"
GB10_CONTROLLER_SSH_PORT = 2221
GB10_CONTROLLER_SSH_USER = "qianyi"
GB10_CONTROLLER_SERVICE_UID = 995
GB10_CONTROLLER_SERVICE_GID = 2007
GB10_CONTROLLER_HOME = Path("/var/lib/loom-rollout")
GB10_CONTROLLER_UNIT_DIR = GB10_CONTROLLER_HOME / ".config/systemd/user"

_IDENTITY = Path("/var/lib/loom-staging-rollout/gb10-controller-supervisor-ed25519")
_KNOWN_HOSTS = Path("/etc/loom/staging-rollout-gb10-known-hosts")
_KNOWN_HOSTS_OWNER_UID = 0
_REMOTE_COMMAND = "loom-external-supervisor-v1"
_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_MAX_WIRE_BYTES = 8 * 1024 * 1024
_MAX_AUTHORITY_FILE_BYTES = 64 * 1024
_CREDENTIAL_MISSING = object()
CAPACITY_ACCEPTANCE_FAILURE_CODES = frozenset(
    {
        "acceptance-failed",
        "busy-accounting-unverified",
        "busy-node-state-unverified",
        "node-allocation-failed",
        "node-evidence-invalid",
    }
)


class CommandResult(Protocol):
    @property
    def returncode(self) -> int: ...

    @property
    def stdout(self) -> str: ...

    @property
    def stderr(self) -> str: ...


CommandRunner = Callable[[Sequence[str], str], CommandResult]


class ExternalSupervisorCapacityError(RuntimeError):
    """Secret-safe GB10 capacity failure classification."""

    def __init__(self, failure_code: str, *, node: str) -> None:
        if (
            failure_code not in CAPACITY_ACCEPTANCE_FAILURE_CODES
            or node not in GB10_SLURM_WORKER_HOSTS
        ):
            raise ValueError("GB10 capacity failure is invalid")
        self.failure_code = failure_code
        self.node = node
        super().__init__("GB10 capacity acceptance failed safely")


def _canonical_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True) + "\n"


def _strict_json(payload: str, *, label: str) -> dict[str, object]:
    if not payload or len(payload.encode()) > _MAX_WIRE_BYTES or not payload.endswith("\n"):
        raise ValueError(f"{label} bytes are invalid")

    def reject_duplicates(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"{label} contains duplicate keys")
            result[key] = value
        return result

    try:
        value = json.loads(payload, object_pairs_hook=reject_duplicates)
    except (json.JSONDecodeError, UnicodeError) as exc:
        raise ValueError(f"{label} is invalid") from exc
    if not isinstance(value, dict) or _canonical_json(value) != payload:
        raise ValueError(f"{label} encoding is not canonical")
    return value


def _digest(value: object, *, label: str) -> str:
    if type(value) is not str or _SHA256_RE.fullmatch(value) is None:
        raise ValueError(f"{label} is invalid")
    return value


def _sha(value: object, *, label: str) -> str:
    if type(value) is not str or _SHA_RE.fullmatch(value) is None:
        raise ValueError(f"{label} is invalid")
    return value


def _artifact_value(artifact: ExternalSupervisorArtifact) -> dict[str, object]:
    return cast(dict[str, object], json.loads(artifact.to_bytes()))


def _artifact_from_value(value: object) -> ExternalSupervisorArtifact:
    if not isinstance(value, dict):
        raise ValueError("GB10 controller artifact is invalid")
    try:
        return ExternalSupervisorArtifact.from_bytes(_canonical_json(value).encode())
    except ValueError as exc:
        raise ValueError("GB10 controller artifact is invalid") from exc


def _validate_controller_artifact(
    artifact: ExternalSupervisorArtifact,
    *,
    candidate_sha: str,
    candidate_tree: str,
) -> None:
    execution_hosts = {item.execution_host for item in artifact.supervisors}
    pool_names = {item.pool_name for item in artifact.supervisors}
    if (
        artifact.candidate_sha != candidate_sha
        or artifact.candidate_tree != candidate_tree
        or len(artifact.supervisors) != 2
        or execution_hosts != {GB10_CONTROLLER_EXECUTION_HOST}
        or pool_names != {"gb10", "task-image-builder-gb10"}
    ):
        raise ValueError("GB10 controller artifact exceeds fixed authority")


def _status_map(
    value: object,
    *,
    timer: bool,
) -> Mapping[str, TimerRuntimeStatus] | Mapping[str, ServiceRuntimeStatus]:
    if not isinstance(value, dict):
        raise ValueError("GB10 controller observation status is invalid")
    parsed: dict[str, TimerRuntimeStatus | ServiceRuntimeStatus] = {}
    expected = (
        {"active_state", "fragment_path", "load_state", "need_daemon_reload", "unit_file_state"}
        if timer
        else {
            "exec_main_status",
            "fragment_path",
            "load_state",
            "need_daemon_reload",
            "result",
        }
    )
    for name, raw in value.items():
        if type(name) is not str or not isinstance(raw, dict) or set(raw) != expected:
            raise ValueError("GB10 controller observation status is invalid")
        parsed[name] = TimerRuntimeStatus(**raw) if timer else ServiceRuntimeStatus(**raw)
    return MappingProxyType(parsed)  # type: ignore[return-value]


def _observation_value(observation: ExternalSupervisorLiveObservation) -> dict[str, object]:
    authority = observation.predecessor_authority
    assert authority is not None
    return {
        "canonical_identity": (
            None
            if observation.canonical_identity is None
            else json.loads(observation.canonical_identity.to_bytes())
        ),
        "compensation_blockers": dict(observation.compensation_blockers),
        "predecessor_authority": authority.to_dict(),
        "service_statuses": {
            name: status.to_dict() for name, status in observation.service_statuses.items()
        },
        "timer_statuses": {
            name: status.to_dict() for name, status in observation.timer_statuses.items()
        },
        "unit_payloads": {
            name: None if payload is None else base64.b64encode(payload).decode("ascii")
            for name, payload in observation.unit_payloads.items()
        },
    }


def _observation_from_value(value: object) -> ExternalSupervisorLiveObservation:
    if not isinstance(value, dict) or set(value) != {
        "canonical_identity",
        "compensation_blockers",
        "predecessor_authority",
        "service_statuses",
        "timer_statuses",
        "unit_payloads",
    }:
        raise ValueError("GB10 controller observation is invalid")
    units = value["unit_payloads"]
    blockers = value["compensation_blockers"]
    authority = value["predecessor_authority"]
    if (
        not isinstance(units, dict)
        or not isinstance(blockers, dict)
        or not isinstance(authority, dict)
    ):
        raise ValueError("GB10 controller observation is invalid")
    decoded_units: dict[str, bytes | None] = {}
    for name, payload in units.items():
        if type(name) is not str or (payload is not None and type(payload) is not str):
            raise ValueError("GB10 controller observation units are invalid")
        if payload is None:
            decoded_units[name] = None
            continue
        try:
            decoded = base64.b64decode(payload, validate=True)
        except (binascii.Error, ValueError) as exc:
            raise ValueError("GB10 controller observation units are invalid") from exc
        if base64.b64encode(decoded).decode("ascii") != payload:
            raise ValueError("GB10 controller observation units are not canonical")
        decoded_units[name] = decoded
    canonical_value = value["canonical_identity"]
    canonical = None
    if canonical_value is not None:
        if not isinstance(canonical_value, dict):
            raise ValueError("GB10 controller canonical identity is invalid")
        canonical = ExternalSupervisorCanonicalIdentity.from_bytes(
            _canonical_json(canonical_value).encode()
        )
    if any(type(name) is not str or type(digest) is not str for name, digest in blockers.items()):
        raise ValueError("GB10 controller compensation evidence is invalid")
    return ExternalSupervisorLiveObservation(
        unit_payloads=decoded_units,
        timer_statuses=cast(
            Mapping[str, TimerRuntimeStatus],
            _status_map(value["timer_statuses"], timer=True),
        ),
        service_statuses=cast(
            Mapping[str, ServiceRuntimeStatus],
            _status_map(value["service_statuses"], timer=False),
        ),
        canonical_identity=canonical,
        predecessor_authority=ExternalSupervisorPredecessorAuthority.from_dict(authority),
        compensation_blockers=cast(Mapping[str, str], blockers),
    )


def _encode_helper_request(
    *,
    operation: str,
    candidate_sha: str,
    candidate_tree: str,
    artifact: ExternalSupervisorArtifact | None = None,
    predecessor_authority: ExternalSupervisorPredecessorAuthority | None = None,
    expected: ExternalSupervisorLiveObservation | None = None,
    plan_digest: str | None = None,
    attestation_digest: str | None = None,
    transition_digest: str | None = None,
    profile_sha256: str | None = None,
    nodes: Sequence[str] | None = None,
    controller_discovery: ControllerDiscoveryRequest | None = None,
    controller_prerequisite: ControllerPrerequisiteRequest | None = None,
    prepared_controller: PreparedControllerRequest | None = None,
) -> str:
    payload: dict[str, object] = {
        "candidate_sha": _sha(candidate_sha, label="GB10 controller candidate SHA"),
        "candidate_tree": _sha(candidate_tree, label="GB10 controller candidate tree"),
        "operation": operation,
        "schema_version": 1,
    }
    if operation == "observe":
        if artifact is None or any(
            item is not None
            for item in (
                expected,
                plan_digest,
                attestation_digest,
                transition_digest,
                profile_sha256,
                nodes,
                controller_discovery,
                controller_prerequisite,
                prepared_controller,
            )
        ):
            raise ValueError("GB10 controller observe request is invalid")
        payload.update(
            {
                "artifact": _artifact_value(artifact),
                "predecessor_authority": (
                    None if predecessor_authority is None else predecessor_authority.to_dict()
                ),
            }
        )
    elif operation == "apply":
        if (
            artifact is None
            or expected is None
            or predecessor_authority is not None
            or profile_sha256 is not None
            or nodes is not None
            or controller_discovery is not None
            or controller_prerequisite is not None
            or prepared_controller is not None
        ):
            raise ValueError("GB10 controller apply request is invalid")
        payload.update(
            {
                "artifact": _artifact_value(artifact),
                "attestation_digest": _digest(
                    attestation_digest,
                    label="GB10 controller attestation digest",
                ),
                "expected": _observation_value(expected),
                "plan_digest": _digest(plan_digest, label="GB10 controller plan digest"),
                "transition_digest": _digest(
                    transition_digest,
                    label="GB10 controller transition digest",
                ),
            }
        )
    elif operation == "accept_capacity":
        if (
            artifact is not None
            or predecessor_authority is not None
            or expected is not None
            or plan_digest is not None
            or attestation_digest is not None
            or transition_digest is not None
            or nodes is None
            or controller_discovery is not None
            or controller_prerequisite is not None
            or prepared_controller is not None
        ):
            raise ValueError("GB10 controller capacity request is invalid")
        payload.update(
            {
                "nodes": list(nodes),
                "profile_sha256": _digest(
                    profile_sha256,
                    label="GB10 controller profile digest",
                ),
            }
        )
    elif operation in {"observe_credential", "publish_credential"}:
        if any(
            item is not None
            for item in (
                artifact,
                predecessor_authority,
                expected,
                plan_digest,
                attestation_digest,
                transition_digest,
                profile_sha256,
                nodes,
                controller_discovery,
                controller_prerequisite,
                prepared_controller,
            )
        ):
            raise ValueError("GB10 controller credential request is invalid")
    elif operation == "discover_controller":
        if (
            controller_discovery is None
            or controller_discovery.pool_id != "gb10"
            or any(
                item is not None
                for item in (
                    artifact,
                    predecessor_authority,
                    expected,
                    plan_digest,
                    attestation_digest,
                    transition_digest,
                    profile_sha256,
                    nodes,
                    controller_prerequisite,
                    prepared_controller,
                )
            )
        ):
            raise ValueError("GB10 controller discovery request is invalid")
        payload["controller_discovery"] = json.loads(controller_discovery.to_bytes())
    elif operation in {
        "observe_controller_prerequisite",
        "converge_controller_prerequisite",
    }:
        if (
            controller_prerequisite is None
            or controller_prerequisite.pool_id != "gb10"
            or controller_prerequisite.source_sha != candidate_sha
            or any(
                item is not None
                for item in (
                    artifact,
                    predecessor_authority,
                    expected,
                    plan_digest,
                    attestation_digest,
                    transition_digest,
                    profile_sha256,
                    nodes,
                    controller_discovery,
                    prepared_controller,
                )
            )
        ):
            raise ValueError("GB10 controller prerequisite request is invalid")
        payload["controller_prerequisite"] = controller_prerequisite.to_dict()
    elif operation in {
        "observe_prepared_controller",
        "converge_prepared_files",
        "enable_prepared_timer",
        "run_prepared_tick",
        "disable_prepared_timer",
    }:
        if (
            prepared_controller is None
            or prepared_controller.pool_id != "gb10"
            or prepared_controller.prerequisite.source_sha != candidate_sha
            or any(
                item is not None
                for item in (
                    artifact,
                    predecessor_authority,
                    expected,
                    plan_digest,
                    attestation_digest,
                    transition_digest,
                    profile_sha256,
                    nodes,
                    controller_discovery,
                    controller_prerequisite,
                )
            )
        ):
            raise ValueError("GB10 prepared controller request is invalid")
        payload["prepared_controller"] = prepared_controller.to_dict()
    elif operation != "reconcile_compensations" or any(
        item is not None
        for item in (
            artifact,
            predecessor_authority,
            expected,
            plan_digest,
            attestation_digest,
            transition_digest,
            profile_sha256,
            nodes,
            controller_discovery,
            controller_prerequisite,
            prepared_controller,
        )
    ):
        raise ValueError("GB10 controller helper operation is invalid")
    rendered = _canonical_json(payload)
    if len(rendered.encode()) > _MAX_WIRE_BYTES:
        raise ValueError("GB10 controller request is too large")
    return rendered


def _encode_pool_credential_request(
    *,
    operation: str,
    candidate_sha: str,
    candidate_tree: str,
    pool_credential: PoolExecutionCredentialPayload,
) -> str:
    if (
        operation not in {"observe_pool_credential", "publish_pool_credential"}
        or not isinstance(pool_credential, PoolExecutionCredentialPayload)
        or pool_credential.pool_id != "gb10"
    ):
        raise ValueError("GB10 controller pool credential request is invalid")
    payload = {
        "candidate_sha": _sha(candidate_sha, label="GB10 controller candidate SHA"),
        "candidate_tree": _sha(candidate_tree, label="GB10 controller candidate tree"),
        "operation": operation,
        "pool_credential": json.loads(pool_credential.to_bytes()),
        "schema_version": 1,
    }
    rendered = _canonical_json(payload)
    if len(rendered.encode()) > _MAX_WIRE_BYTES:
        raise ValueError("GB10 controller pool credential request is too large")
    return rendered


def _encode_helper_response(
    operation: str,
    *,
    observation: ExternalSupervisorLiveObservation | None = None,
    credential: ExternalSupervisorCredentialEvidence | None | object = _CREDENTIAL_MISSING,
) -> str:
    if operation == "observe":
        if observation is None:
            raise ValueError("GB10 controller observe response is invalid")
        payload: dict[str, object] = {
            "observation": _observation_value(observation),
            "operation": operation,
            "schema_version": 1,
            "status": "ok",
        }
    elif operation in {"apply", "reconcile_compensations"} and (
        observation is None and credential is _CREDENTIAL_MISSING
    ):
        payload = {"operation": operation, "schema_version": 1, "status": "ok"}
    elif operation in {"observe_credential", "publish_credential"} and observation is None:
        if credential is _CREDENTIAL_MISSING or (
            operation == "publish_credential" and credential is None
        ):
            raise ValueError("GB10 controller credential response is invalid")
        if credential is not None and not isinstance(
            credential, ExternalSupervisorCredentialEvidence
        ):
            raise ValueError("GB10 controller credential response is invalid")
        payload = {
            "credential": None if credential is None else credential.to_dict(),
            "operation": operation,
            "schema_version": 1,
            "status": "ok",
        }
    else:
        raise ValueError("GB10 controller helper response is invalid")
    return _canonical_json(payload)


def _decode_helper_response(payload: str, *, operation: str) -> dict[str, object]:
    value = _strict_json(payload, label="GB10 controller response")
    expected = (
        {"observation", "operation", "schema_version", "status"}
        if operation == "observe"
        else (
            {"acceptance", "operation", "schema_version", "status"}
            if operation == "accept_capacity"
            else (
                {"credential", "operation", "schema_version", "status"}
                if operation in {"observe_credential", "publish_credential"}
                else {"operation", "schema_version", "status"}
            )
        )
    )
    if (
        set(value) != expected
        or type(value.get("schema_version")) is not int
        or value.get("schema_version") != 1
        or value.get("operation") != operation
        or value.get("status") != "ok"
    ):
        raise RuntimeError("GB10 controller response drifted")
    return value


def _decode_helper_observation(payload: str) -> ExternalSupervisorLiveObservation:
    return _observation_from_value(
        _decode_helper_response(payload, operation="observe")["observation"]
    )


def _decode_helper_credential(
    payload: str,
    *,
    operation: str,
) -> ExternalSupervisorCredentialEvidence | None:
    if operation not in {"observe_credential", "publish_credential"}:
        raise ValueError("GB10 controller credential response operation is invalid")
    value = _decode_helper_response(payload, operation=operation)["credential"]
    if value is None:
        if operation != "observe_credential":
            raise RuntimeError("GB10 controller credential response drifted")
        return None
    if not isinstance(value, dict):
        raise RuntimeError("GB10 controller credential response drifted")
    try:
        return ExternalSupervisorCredentialEvidence.from_dict(value)
    except ValueError as exc:
        raise RuntimeError("GB10 controller credential response drifted") from exc


def _encode_helper_failure_response(*, operation: str, failure_code: str) -> str:
    if (
        operation != "reconcile_compensations"
        or failure_code not in COMPENSATION_RECONCILIATION_FAILURE_CODES
    ):
        raise ValueError("GB10 controller failure response is invalid")
    return _canonical_json(
        {
            "failure_code": failure_code,
            "operation": operation,
            "schema_version": 1,
            "status": "failed",
        }
    )


def _decode_helper_failure_response(payload: str, *, operation: str) -> str:
    value = _strict_json(payload, label="GB10 controller failure response")
    failure_code = value.get("failure_code")
    if (
        operation != "reconcile_compensations"
        or set(value) != {"failure_code", "operation", "schema_version", "status"}
        or type(value.get("schema_version")) is not int
        or value.get("schema_version") != 1
        or value.get("operation") != operation
        or value.get("status") != "failed"
        or type(failure_code) is not str
        or failure_code not in COMPENSATION_RECONCILIATION_FAILURE_CODES
    ):
        raise ValueError("GB10 controller failure response drifted")
    return failure_code


def _decode_capacity_failure_response(payload: str) -> tuple[str, str]:
    value = _strict_json(payload, label="GB10 capacity failure response")
    failure_code = value.get("failure_code")
    node = value.get("node")
    if (
        set(value) != {"failure_code", "node", "operation", "schema_version", "status"}
        or type(value.get("schema_version")) is not int
        or value.get("schema_version") != 1
        or value.get("operation") != "accept_capacity"
        or value.get("status") != "failed"
        or type(failure_code) is not str
        or failure_code not in CAPACITY_ACCEPTANCE_FAILURE_CODES
        or type(node) is not str
        or node not in GB10_SLURM_WORKER_HOSTS
    ):
        raise ValueError("GB10 capacity failure response drifted")
    return failure_code, node


def _encode_helper_apply_failure_response(error: ExternalSupervisorApplyError) -> str:
    return _canonical_json(
        {
            "compensation_failure_code": error.compensation_failure_code,
            "failure_code": error.failure_code,
            "operation": "apply",
            "schema_version": 1,
            "status": "failed",
        }
    )


def _decode_helper_apply_failure_response(payload: str) -> tuple[str, str | None]:
    value = _strict_json(payload, label="GB10 controller apply failure response")
    failure_code = value.get("failure_code")
    compensation_failure_code = value.get("compensation_failure_code")
    if (
        set(value)
        != {
            "compensation_failure_code",
            "failure_code",
            "operation",
            "schema_version",
            "status",
        }
        or type(value.get("schema_version")) is not int
        or value.get("schema_version") != 1
        or value.get("operation") != "apply"
        or value.get("status") != "failed"
        or type(failure_code) is not str
        or failure_code not in EXTERNAL_SUPERVISOR_APPLY_FAILURE_CODES
        or (
            compensation_failure_code is not None
            and (
                type(compensation_failure_code) is not str
                or compensation_failure_code not in COMPENSATION_RECONCILIATION_FAILURE_CODES
            )
        )
    ):
        raise ValueError("GB10 controller apply failure response drifted")
    return failure_code, compensation_failure_code


@dataclass(frozen=True, slots=True)
class FixedGB10ExternalSupervisorTransport:
    """Invoke only fixed typed controller operations through one forced key."""

    candidate_sha: str
    candidate_tree: str
    identity: Path
    run: CommandRunner

    def __post_init__(self) -> None:
        if (
            _SHA_RE.fullmatch(self.candidate_sha) is None
            or _SHA_RE.fullmatch(self.candidate_tree) is None
            or not self.identity.is_absolute()
            or ".." in self.identity.parts
            or not callable(self.run)
        ):
            raise ValueError("GB10 controller transport authority is invalid")

    @property
    def controller_prerequisite_authority_sha256(self) -> str:
        """Bind prerequisite requests to the exact forced-SSH trust route."""

        identity_sha256 = _stable_authority_file_sha256(
            self.identity,
            expected_mode=0o600,
            expected_uid=os.geteuid(),
        )[1]
        known_hosts_sha256 = _controller_known_hosts_sha256(_KNOWN_HOSTS)
        value = {
            "candidate_sha": self.candidate_sha,
            "candidate_tree": self.candidate_tree,
            "channel": "forced-ssh-v1",
            "execution_host": GB10_CONTROLLER_EXECUTION_HOST,
            "identity_path": str(self.identity),
            "identity_sha256": identity_sha256,
            "known_hosts_path": str(_KNOWN_HOSTS),
            "known_hosts_sha256": known_hosts_sha256,
            "schema_version": 1,
            "ssh_argv": list(self._ssh_argv()),
        }
        return hashlib.sha256(
            json.dumps(value, sort_keys=True, separators=(",", ":")).encode("ascii")
        ).hexdigest()

    def observe(
        self,
        artifact: ExternalSupervisorArtifact,
        predecessor_authority: ExternalSupervisorPredecessorAuthority | None = None,
    ) -> ExternalSupervisorLiveObservation:
        self._validate_artifact(artifact)
        request = _encode_helper_request(
            operation="observe",
            candidate_sha=self.candidate_sha,
            candidate_tree=self.candidate_tree,
            artifact=artifact,
            predecessor_authority=predecessor_authority,
        )
        return _decode_helper_observation(self._invoke(request, operation="observe"))

    def apply(
        self,
        artifact: ExternalSupervisorArtifact,
        expected: ExternalSupervisorLiveObservation,
        *,
        plan_digest: str,
        attestation_digest: str,
        transition_digest: str,
    ) -> None:
        self._validate_artifact(artifact)
        request = _encode_helper_request(
            operation="apply",
            candidate_sha=self.candidate_sha,
            candidate_tree=self.candidate_tree,
            artifact=artifact,
            expected=expected,
            plan_digest=plan_digest,
            attestation_digest=attestation_digest,
            transition_digest=transition_digest,
        )
        _decode_helper_response(self._invoke(request, operation="apply"), operation="apply")

    def reconcile_compensations(self) -> None:
        request = _encode_helper_request(
            operation="reconcile_compensations",
            candidate_sha=self.candidate_sha,
            candidate_tree=self.candidate_tree,
        )
        _decode_helper_response(
            self._invoke(request, operation="reconcile_compensations"),
            operation="reconcile_compensations",
        )

    def observe_credential(self) -> ExternalSupervisorCredentialEvidence | None:
        request = _encode_helper_request(
            operation="observe_credential",
            candidate_sha=self.candidate_sha,
            candidate_tree=self.candidate_tree,
        )
        return _decode_helper_credential(
            self._invoke(request, operation="observe_credential"),
            operation="observe_credential",
        )

    def publish_credential(self) -> ExternalSupervisorCredentialEvidence:
        request = _encode_helper_request(
            operation="publish_credential",
            candidate_sha=self.candidate_sha,
            candidate_tree=self.candidate_tree,
        )
        evidence = _decode_helper_credential(
            self._invoke(request, operation="publish_credential"),
            operation="publish_credential",
        )
        if evidence is None:  # narrowed by the decoder; retains explicit fail-closed type.
            raise RuntimeError("GB10 controller credential response drifted")
        return evidence

    def accept_capacity(
        self,
        *,
        profile_sha256: str,
        nodes: Sequence[str],
    ) -> GB10SlurmAcceptanceEvidence:
        expected_nodes = tuple(nodes)
        if expected_nodes != GB10_SLURM_WORKER_HOSTS:
            raise ValueError("GB10 controller capacity request is invalid")
        request = _encode_helper_request(
            operation="accept_capacity",
            candidate_sha=self.candidate_sha,
            candidate_tree=self.candidate_tree,
            profile_sha256=profile_sha256,
            nodes=expected_nodes,
        )
        response = _decode_helper_response(
            self._invoke(request, operation="accept_capacity"),
            operation="accept_capacity",
        )
        acceptance = response.get("acceptance")
        if not isinstance(acceptance, dict):
            raise ValueError("GB10 Slurm acceptance evidence is invalid")
        return validate_gb10_slurm_acceptance(
            acceptance,
            candidate_sha=self.candidate_sha,
            candidate_tree=self.candidate_tree,
            profile_sha256=profile_sha256,
            nodes=expected_nodes,
        )

    def invoke_controller_prerequisite(
        self,
        operation: str,
        payload: bytes,
    ) -> CommandResult:
        operations = {
            "discover-controller": "discover_controller",
            "observe-prerequisite": "observe_controller_prerequisite",
            "converge-prerequisite": "converge_controller_prerequisite",
        }
        try:
            remote_operation = operations[operation]
            request: ControllerDiscoveryRequest | ControllerPrerequisiteRequest
            if operation == "discover-controller":
                request = ControllerDiscoveryRequest.from_bytes(payload)
            else:
                request = ControllerPrerequisiteRequest.from_bytes(payload)
        except (KeyError, ValueError) as exc:
            label = "discovery" if operation == "discover-controller" else "prerequisite"
            raise ValueError(f"GB10 controller {label} request is invalid") from exc
        if (
            request.pool_id != "gb10"
            or request.transport_authority_sha256 != self.controller_prerequisite_authority_sha256
            or (
                isinstance(request, ControllerPrerequisiteRequest)
                and request.source_sha != self.candidate_sha
            )
        ):
            label = "discovery" if operation == "discover-controller" else "prerequisite"
            raise ValueError(f"GB10 controller {label} request is invalid")
        rendered = _encode_helper_request(
            operation=remote_operation,
            candidate_sha=self.candidate_sha,
            candidate_tree=self.candidate_tree,
            controller_discovery=(
                request if isinstance(request, ControllerDiscoveryRequest) else None
            ),
            controller_prerequisite=(
                request if isinstance(request, ControllerPrerequisiteRequest) else None
            ),
        )
        return self.run(self._ssh_argv(), rendered)

    def invoke_prepared_controller(
        self,
        operation: str,
        payload: bytes,
    ) -> CommandResult:
        operations = {
            "observe-prepared": "observe_prepared_controller",
            "converge-prepared-files": "converge_prepared_files",
            "enable-prepared-timer": "enable_prepared_timer",
            "run-prepared-tick": "run_prepared_tick",
            "disable-prepared-timer": "disable_prepared_timer",
        }
        try:
            request = PreparedControllerRequest.from_bytes(payload)
            remote_operation = operations[operation]
        except (KeyError, ValueError) as exc:
            raise ValueError("GB10 prepared controller request is invalid") from exc
        if (
            request.pool_id != "gb10"
            or request.transport_authority_sha256 != self.controller_prerequisite_authority_sha256
            or request.prerequisite.source_sha != self.candidate_sha
        ):
            raise ValueError("GB10 prepared controller request is invalid")
        rendered = _encode_helper_request(
            operation=remote_operation,
            candidate_sha=self.candidate_sha,
            candidate_tree=self.candidate_tree,
            prepared_controller=request,
        )
        return self.run(self._ssh_argv(), rendered)

    def invoke_pool_credential(
        self,
        operation: str,
        payload: bytes,
    ) -> CommandResult:
        operations = {
            "observe-credential": "observe_pool_credential",
            "publish-credential": "publish_pool_credential",
        }
        try:
            request = PoolExecutionCredentialPayload.from_bytes(payload)
            remote_operation = operations[operation]
        except (KeyError, ValueError) as exc:
            raise ValueError("GB10 controller pool credential request is invalid") from exc
        if request.pool_id != "gb10":
            raise ValueError("GB10 controller pool credential request is invalid")
        rendered = _encode_pool_credential_request(
            operation=remote_operation,
            candidate_sha=self.candidate_sha,
            candidate_tree=self.candidate_tree,
            pool_credential=request,
        )
        return self.run(self._ssh_argv(), rendered)

    def _validate_artifact(self, artifact: ExternalSupervisorArtifact) -> None:
        _validate_controller_artifact(
            artifact,
            candidate_sha=self.candidate_sha,
            candidate_tree=self.candidate_tree,
        )

    def _invoke(self, request: str, *, operation: str) -> str:
        result = self.run(self._ssh_argv(), request)
        if result.stderr or not result.stdout or len(result.stdout.encode()) > _MAX_WIRE_BYTES:
            raise RuntimeError("GB10 controller operation failed safely")
        if result.returncode == 1 and operation == "reconcile_compensations":
            try:
                failure_code = _decode_helper_failure_response(
                    result.stdout,
                    operation=operation,
                )
            except ValueError as exc:
                raise RuntimeError("GB10 controller operation failed safely") from exc
            raise ExternalSupervisorCompensationError(failure_code)
        if result.returncode == 1 and operation == "apply":
            try:
                failure_code, compensation_failure_code = _decode_helper_apply_failure_response(
                    result.stdout
                )
            except ValueError as exc:
                raise RuntimeError("GB10 controller operation failed safely") from exc
            raise ExternalSupervisorApplyError(
                failure_code,
                compensation_failure_code=compensation_failure_code,
            )
        if result.returncode == 1 and operation == "accept_capacity":
            try:
                failure_code, node = _decode_capacity_failure_response(result.stdout)
            except ValueError as exc:
                raise RuntimeError("GB10 controller operation failed safely") from exc
            raise ExternalSupervisorCapacityError(failure_code, node=node)
        if result.returncode != 0:
            raise RuntimeError("GB10 controller operation failed safely")
        return result.stdout

    def _ssh_argv(self) -> tuple[str, ...]:
        return (
            "ssh",
            "-F",
            "/dev/null",
            "-i",
            str(self.identity),
            "-o",
            "IdentitiesOnly=yes",
            "-o",
            "BatchMode=yes",
            "-o",
            "ConnectTimeout=10",
            "-o",
            "StrictHostKeyChecking=yes",
            "-o",
            f"UserKnownHostsFile={_KNOWN_HOSTS}",
            "-o",
            "GlobalKnownHostsFile=/dev/null",
            "-o",
            "UpdateHostKeys=no",
            "-p",
            str(GB10_CONTROLLER_SSH_PORT),
            "-l",
            GB10_CONTROLLER_SSH_USER,
            GB10_CONTROLLER_SSH_HOST,
            _REMOTE_COMMAND,
        )


def _stable_authority_file_sha256(
    path: Path,
    *,
    expected_mode: int,
    expected_uid: int,
) -> tuple[bytes, str]:
    try:
        descriptor = os.open(
            path,
            os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
        )
    except OSError as exc:
        raise ValueError("GB10 controller transport authority is unavailable") from exc
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or stat.S_IMODE(before.st_mode) != expected_mode
            or before.st_uid != expected_uid
            or before.st_nlink != 1
            or not 0 < before.st_size <= _MAX_AUTHORITY_FILE_BYTES
        ):
            raise ValueError("GB10 controller transport authority metadata is unsafe")
        chunks: list[bytes] = []
        total = 0
        while total <= _MAX_AUTHORITY_FILE_BYTES:
            chunk = os.read(descriptor, min(65536, _MAX_AUTHORITY_FILE_BYTES + 1 - total))
            if not chunk:
                break
            chunks.append(chunk)
            total += len(chunk)
        after = os.fstat(descriptor)
        stable_before = (
            before.st_dev,
            before.st_ino,
            before.st_mode,
            before.st_uid,
            before.st_gid,
            before.st_nlink,
            before.st_size,
            before.st_mtime_ns,
            before.st_ctime_ns,
        )
        stable_after = (
            after.st_dev,
            after.st_ino,
            after.st_mode,
            after.st_uid,
            after.st_gid,
            after.st_nlink,
            after.st_size,
            after.st_mtime_ns,
            after.st_ctime_ns,
        )
        payload = b"".join(chunks)
        if stable_before != stable_after or not payload or len(payload) > _MAX_AUTHORITY_FILE_BYTES:
            raise ValueError("GB10 controller transport authority changed while reading")
        return payload, hashlib.sha256(payload).hexdigest()
    finally:
        os.close(descriptor)


def _controller_known_hosts_sha256(path: Path) -> str:
    payload, digest = _stable_authority_file_sha256(
        path,
        expected_mode=0o644,
        expected_uid=_KNOWN_HOSTS_OWNER_UID,
    )
    try:
        lines = payload.decode("ascii").splitlines()
    except UnicodeDecodeError as exc:
        raise ValueError("GB10 controller known-host authority is invalid") from exc
    controller_alias = f"[{GB10_CONTROLLER_SSH_HOST}]:{GB10_CONTROLLER_SSH_PORT}"
    expected_aliases = {controller_alias, "trt-gb10-1"}
    matches = 0
    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        fields = stripped.split()
        aliases = set(fields[0].split(",")) if fields else set()
        if controller_alias not in aliases:
            continue
        matches += 1
        try:
            key = base64.b64decode(fields[2], validate=True) if len(fields) == 3 else b""
        except (binascii.Error, ValueError) as exc:
            raise ValueError("GB10 controller known-host authority is invalid") from exc
        if (
            aliases != expected_aliases
            or fields[1] != "ssh-ed25519"
            or base64.b64encode(key).decode("ascii") != fields[2]
            or not key.startswith(b"\x00\x00\x00\x0bssh-ed25519")
        ):
            raise ValueError("GB10 controller known-host authority is invalid")
    if matches != 1:
        raise ValueError("GB10 controller known-host authority is invalid")
    return digest


@dataclass(frozen=True, slots=True)
class GB10ExternalSupervisorCredentialTransport:
    """Adapt the fixed GB10 credential operations to the journal protocol."""

    transport: FixedGB10ExternalSupervisorTransport

    def observe(self) -> ExternalSupervisorCredentialEvidence | None:
        return self.transport.observe_credential()

    def publish(self) -> ExternalSupervisorCredentialEvidence:
        return self.transport.publish_credential()


def _handle_helper_request(
    payload: str,
    *,
    transport: ProtectedExternalSupervisorTransport | None,
    credential_transport: ProtectedExternalSupervisorCredentialTransport | None = None,
) -> str:
    request = _strict_json(payload, label="GB10 controller request")
    operation = request.get("operation")
    common = {"candidate_sha", "candidate_tree", "operation", "schema_version"}
    if request.get("schema_version") != 1 or operation not in {
        "observe",
        "apply",
        "reconcile_compensations",
        "observe_credential",
        "publish_credential",
    }:
        raise ValueError("GB10 controller request is invalid")
    candidate_sha = _sha(request.get("candidate_sha"), label="GB10 controller candidate SHA")
    candidate_tree = _sha(
        request.get("candidate_tree"),
        label="GB10 controller candidate tree",
    )
    if operation in {"observe_credential", "publish_credential"}:
        if set(request) != common or credential_transport is None:
            raise ValueError("GB10 controller credential request is invalid")
        evidence = (
            credential_transport.observe()
            if operation == "observe_credential"
            else credential_transport.publish()
        )
        return _encode_helper_response(operation, credential=evidence)
    if transport is None:
        raise ValueError("GB10 controller supervisor transport is unavailable")
    if operation == "reconcile_compensations":
        if set(request) != common:
            raise ValueError("GB10 controller reconcile request is invalid")
        transport.reconcile_compensations()
        return _encode_helper_response(operation)
    artifact = _artifact_from_value(request.get("artifact"))
    _validate_controller_artifact(
        artifact,
        candidate_sha=candidate_sha,
        candidate_tree=candidate_tree,
    )
    if operation == "observe":
        if set(request) != common | {"artifact", "predecessor_authority"}:
            raise ValueError("GB10 controller observe request is invalid")
        raw_authority = request.get("predecessor_authority")
        if raw_authority is None:
            authority = None
        elif isinstance(raw_authority, dict):
            authority = ExternalSupervisorPredecessorAuthority.from_dict(raw_authority)
        else:
            raise ValueError("GB10 controller predecessor authority is invalid")
        observation = transport.observe(artifact, authority)
        return _encode_helper_response(operation, observation=observation)
    if set(request) != common | {
        "artifact",
        "attestation_digest",
        "expected",
        "plan_digest",
        "transition_digest",
    }:
        raise ValueError("GB10 controller apply request is invalid")
    transport.apply(
        artifact,
        _observation_from_value(request.get("expected")),
        plan_digest=_digest(request.get("plan_digest"), label="GB10 controller plan digest"),
        attestation_digest=_digest(
            request.get("attestation_digest"),
            label="GB10 controller attestation digest",
        ),
        transition_digest=_digest(
            request.get("transition_digest"),
            label="GB10 controller transition digest",
        ),
    )
    return _encode_helper_response(operation)


def _fixed_local_transport() -> FixedExternalSupervisorTransport:
    if os.geteuid() != GB10_CONTROLLER_SERVICE_UID or os.getegid() != GB10_CONTROLLER_SERVICE_GID:
        raise ValueError("GB10 controller helper identity is invalid")
    return FixedExternalSupervisorTransport(
        store=AtomicUserUnitStore(
            unit_dir=GB10_CONTROLLER_UNIT_DIR,
            service_uid=GB10_CONTROLLER_SERVICE_UID,
            creation_anchor=GB10_CONTROLLER_HOME,
        ),
        control=FixedUserSystemdControl(
            service_uid=GB10_CONTROLLER_SERVICE_UID,
            service_home=GB10_CONTROLLER_HOME,
        ),
        unit_dir=GB10_CONTROLLER_UNIT_DIR,
    )


def _fixed_local_credential_transport(
    candidate_sha: str,
) -> FixedLocalExternalSupervisorCredentialTransport:
    candidate_root = Path("/opt/loom-staging-runner/candidates") / candidate_sha / "repo"
    return FixedLocalExternalSupervisorCredentialTransport(
        candidate_root=candidate_root,
        execution_host=GB10_CONTROLLER_EXECUTION_HOST,
        service_uid=GB10_CONTROLLER_SERVICE_UID,
        service_gid=GB10_CONTROLLER_SERVICE_GID,
        promote_existing_source=True,
    )


def build_fixed_gb10_external_supervisor_transport(
    *,
    candidate_sha: str,
    candidate_tree: str,
    run: CommandRunner,
) -> FixedGB10ExternalSupervisorTransport:
    """Bind the installed transport to one dedicated key and exact candidate."""

    try:
        metadata = os.lstat(_IDENTITY)
    except OSError as exc:
        raise ValueError("GB10 controller identity is unavailable") from exc
    if (
        not stat.S_ISREG(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_uid != os.geteuid()
        or stat.S_IMODE(metadata.st_mode) != 0o600
        or metadata.st_nlink != 1
        or not 1 <= metadata.st_size <= 16 * 1024
    ):
        raise ValueError("GB10 controller identity metadata is unsafe")
    return FixedGB10ExternalSupervisorTransport(
        candidate_sha=candidate_sha,
        candidate_tree=candidate_tree,
        identity=_IDENTITY,
        run=run,
    )


def _helper_runtime_matches(candidate_sha: str) -> bool:
    expected = Path("/opt/loom-staging-runner/candidates") / candidate_sha / "venv"
    return Path(sys.prefix) == expected


def main() -> int:
    operation: object = None
    try:
        payload = sys.stdin.buffer.read(_MAX_WIRE_BYTES + 1)
        if not payload or len(payload) > _MAX_WIRE_BYTES:
            raise ValueError("GB10 controller helper input is invalid")
        text = payload.decode("utf-8")
        request = _strict_json(text, label="GB10 controller request")
        operation = request.get("operation")
        candidate_sha = _sha(
            request.get("candidate_sha"),
            label="GB10 controller candidate SHA",
        )
        if not _helper_runtime_matches(candidate_sha):
            raise ValueError("GB10 controller helper runtime drifted")
        response = _handle_helper_request(
            text,
            transport=_fixed_local_transport(),
            credential_transport=_fixed_local_credential_transport(candidate_sha),
        )
    except ExternalSupervisorApplyError as exc:
        if operation == "apply":
            sys.stdout.write(_encode_helper_apply_failure_response(exc))
        return 1
    except ExternalSupervisorCompensationError as exc:
        if operation == "reconcile_compensations":
            sys.stdout.write(
                _encode_helper_failure_response(
                    operation=operation,
                    failure_code=exc.failure_code,
                )
            )
        return 1
    except (OSError, RuntimeError, UnicodeError, ValueError):
        return 1
    sys.stdout.write(response)
    return 0


if __name__ == "__main__":  # pragma: no cover - exercised through installed broker
    raise SystemExit(main())


__all__ = [
    "CAPACITY_ACCEPTANCE_FAILURE_CODES",
    "GB10_CONTROLLER_EXECUTION_HOST",
    "GB10_CONTROLLER_HOME",
    "GB10_CONTROLLER_UNIT_DIR",
    "ExternalSupervisorCapacityError",
    "FixedGB10ExternalSupervisorTransport",
    "GB10ExternalSupervisorCredentialTransport",
    "build_fixed_gb10_external_supervisor_transport",
]
