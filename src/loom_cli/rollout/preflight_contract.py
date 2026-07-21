"""Single-source staged preflight checks and immutable attestations.

Rollout predicates are registered once as :class:`RegisteredCheck` instances.
The same implementation may expose probe, plan, apply, and verify operations;
the preflight DAG and final rollout consume those operations instead of
reimplementing the predicate in step-specific code.
"""

from __future__ import annotations

import hashlib
import inspect
import json
import re
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from dataclasses import dataclass, fields, replace
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from types import MappingProxyType
from typing import TypeAlias, cast

from loom_cli.rollout.operator.redaction import redact_rollout_mapping, redact_rollout_text

SafeScalar: TypeAlias = str | int | float | bool | None
SafeValue: TypeAlias = SafeScalar | list["SafeValue"] | dict[str, "SafeValue"]
EvidenceValue: TypeAlias = SafeScalar | dict[str, str]

_ID_RE = re.compile(r"^[a-z][a-z0-9.-]{2,95}$")
_VERSION_RE = re.compile(r"^[a-z][a-z0-9.-]{0,31}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_SECRET_KEY_RE = re.compile(
    r"(?:^|[._-])(secret|token|password|credential|private[_-]?key)(?:$|[._-])"
)


class StageCapability(StrEnum):
    STATIC = "static"
    BASELINE_LIVE_READONLY = "baseline-live-readonly"
    ISOLATED_REHEARSAL = "isolated-rehearsal"
    FINAL_ONLY = "final-only"


class MutationClass(StrEnum):
    NONE = "none"
    ISOLATED = "isolated"
    PROTECTED_STAGING = "protected-staging"


class SecretRedactionPolicy(StrEnum):
    NO_SECRET_INPUTS = "no-secret-inputs"
    METADATA_FINGERPRINTS_ONLY = "metadata-fingerprints-only"
    REDACT_ALL = "redact-all"


class CheckOperation(StrEnum):
    PROBE = "probe"
    PLAN = "plan"
    APPLY = "apply"
    VERIFY = "verify"


class CheckOutcome(StrEnum):
    PASS = "pass"
    FAIL = "fail"
    BLOCKED = "blocked"
    TIMEOUT = "timeout"


@dataclass(frozen=True, slots=True)
class EvidenceField:
    name: str
    value_type: str

    def __post_init__(self) -> None:
        if _ID_RE.fullmatch(self.name) is None:
            raise ValueError("evidence field name is invalid")
        if self.value_type not in {
            "string",
            "integer",
            "number",
            "boolean",
            "sha256",
            "string-map",
        }:
            raise ValueError("evidence field type is invalid")


@dataclass(frozen=True, slots=True)
class CheckSpec:
    check_id: str
    failure_code: str
    tier: int
    stage: StageCapability
    dependencies: tuple[str, ...]
    mutation_class: MutationClass
    input_keys: tuple[str, ...]
    evidence_schema: tuple[EvidenceField, ...]
    timeout_seconds: int
    freshness_ttl_seconds: int
    remediation: str
    secret_redaction_policy: SecretRedactionPolicy
    final_only_justification: str | None = None
    run_after_failed_dependencies: bool = False

    def __post_init__(self) -> None:
        if _ID_RE.fullmatch(self.check_id) is None:
            raise ValueError("check_id is invalid")
        if _ID_RE.fullmatch(self.failure_code) is None:
            raise ValueError("failure_code is invalid")
        if self.check_id in self.dependencies or len(set(self.dependencies)) != len(
            self.dependencies
        ):
            raise ValueError("check dependencies are invalid")
        if any(_ID_RE.fullmatch(value) is None for value in self.dependencies):
            raise ValueError("check dependency id is invalid")
        if self.tier not in {0, 1, 2, 3, 4}:
            raise ValueError("preflight tier must be in [0, 4]")
        expected_tiers = {
            StageCapability.STATIC: {0, 1},
            StageCapability.BASELINE_LIVE_READONLY: {2},
            StageCapability.ISOLATED_REHEARSAL: {3},
            StageCapability.FINAL_ONLY: {4},
        }
        if self.tier not in expected_tiers[self.stage]:
            raise ValueError("preflight tier does not match stage capability")
        allowed_mutations = {
            StageCapability.STATIC: {
                MutationClass.NONE,
                MutationClass.ISOLATED,
            },
            StageCapability.BASELINE_LIVE_READONLY: {MutationClass.NONE},
            StageCapability.ISOLATED_REHEARSAL: {
                MutationClass.NONE,
                MutationClass.ISOLATED,
            },
            StageCapability.FINAL_ONLY: {
                MutationClass.NONE,
                MutationClass.PROTECTED_STAGING,
            },
        }
        if self.mutation_class not in allowed_mutations[self.stage]:
            raise ValueError("mutation class is not permitted for this stage")
        if not self.input_keys or len(set(self.input_keys)) != len(self.input_keys):
            raise ValueError("check input keys must be unique and non-empty")
        if any(_ID_RE.fullmatch(value) is None for value in self.input_keys):
            raise ValueError("check input key is invalid")
        if not self.evidence_schema or len({field.name for field in self.evidence_schema}) != len(
            self.evidence_schema
        ):
            raise ValueError("evidence schema must be unique and non-empty")
        if not 1 <= self.timeout_seconds <= 3600:
            raise ValueError("check timeout is outside the supported range")
        if not 1 <= self.freshness_ttl_seconds <= 86400:
            raise ValueError("check freshness TTL is outside the supported range")
        if (
            not self.remediation.strip()
            or len(self.remediation) > 300
            or redact_rollout_text(self.remediation) != self.remediation
        ):
            raise ValueError("check remediation is not bounded and secret-safe")
        if self.stage is StageCapability.FINAL_ONLY:
            if not self.final_only_justification or len(self.final_only_justification) < 20:
                raise ValueError("final-only checks require a technical justification")
        elif self.final_only_justification is not None:
            raise ValueError("only final-only checks may carry a final-only justification")
        if self.run_after_failed_dependencies and (
            self.stage is not StageCapability.ISOLATED_REHEARSAL
            or self.mutation_class is not MutationClass.ISOLATED
            or not self.dependencies
        ):
            raise ValueError(
                "failed-dependency execution is reserved for dependent isolated cleanup"
            )

    @property
    def contract_digest(self) -> str:
        payload = {
            "check_id": self.check_id,
            "dependencies": self.dependencies,
            "evidence_schema": [
                {"name": field.name, "value_type": field.value_type}
                for field in self.evidence_schema
            ],
            "failure_code": self.failure_code,
            "final_only_justification": self.final_only_justification,
            "freshness_ttl_seconds": self.freshness_ttl_seconds,
            "input_keys": self.input_keys,
            "mutation_class": self.mutation_class.value,
            "remediation": self.remediation,
            "run_after_failed_dependencies": self.run_after_failed_dependencies,
            "secret_redaction_policy": self.secret_redaction_policy.value,
            "stage": self.stage.value,
            "tier": self.tier,
            "timeout_seconds": self.timeout_seconds,
        }
        return _hash_json(payload)


@dataclass(frozen=True, slots=True)
class CheckContext:
    bindings: Mapping[str, SafeValue]

    def __post_init__(self) -> None:
        object.__setattr__(self, "bindings", MappingProxyType(dict(self.bindings)))


@dataclass(frozen=True, slots=True)
class CheckProbe:
    passed: bool
    evidence: Mapping[str, EvidenceValue]


CheckImplementation = Callable[[CheckContext], CheckProbe]


@dataclass(frozen=True, slots=True)
class RegisteredCheck:
    spec: CheckSpec
    implementation_version: str
    operations: Mapping[CheckOperation, CheckImplementation]

    def __post_init__(self) -> None:
        if _VERSION_RE.fullmatch(self.implementation_version) is None:
            raise ValueError("check implementation version is invalid")
        if CheckOperation.PROBE not in self.operations:
            raise ValueError("every check must expose a probe operation")
        if (
            self.spec.mutation_class is MutationClass.NONE
            and CheckOperation.APPLY in self.operations
        ):
            raise ValueError("non-mutating checks may not expose apply")
        object.__setattr__(self, "operations", MappingProxyType(dict(self.operations)))

    @property
    def implementation_digest(self) -> str:
        digest = hashlib.sha256()
        digest.update(self.spec.contract_digest.encode())
        digest.update(b"\0")
        digest.update(self.implementation_version.encode())
        for operation, implementation in sorted(self.operations.items()):
            digest.update(b"\0")
            digest.update(operation.value.encode())
            digest.update(b"\0")
            digest.update(implementation.__module__.encode())
            digest.update(b"\0")
            digest.update(implementation.__qualname__.encode())
            try:
                source = inspect.getsource(implementation).encode()
            except (OSError, TypeError):
                source = b"source-unavailable"
            digest.update(b"\0")
            digest.update(hashlib.sha256(source).digest())
        return digest.hexdigest()

    def input_fingerprint(self, context: CheckContext) -> str:
        missing = [key for key in self.spec.input_keys if key not in context.bindings]
        if missing:
            raise ValueError(f"missing check inputs: {','.join(sorted(missing))}")
        selected = {key: context.bindings[key] for key in self.spec.input_keys}
        _assert_secret_safe_mapping(
            selected,
            policy=self.spec.secret_redaction_policy,
            allow_redaction=False,
        )
        return _hash_json(selected)


@dataclass(frozen=True, slots=True)
class CheckExecution:
    check_id: str
    failure_code: str
    tier: int
    stage: StageCapability
    operation: CheckOperation
    outcome: CheckOutcome
    input_fingerprint: str
    implementation_digest: str
    evidence: Mapping[str, EvidenceValue]
    evidence_hash: str
    started_at: datetime
    finished_at: datetime
    expires_at: datetime
    remediation: str | None
    blocked_by: tuple[str, ...] = ()

    @property
    def passed(self) -> bool:
        return self.outcome is CheckOutcome.PASS

    def to_dict(self) -> dict[str, object]:
        return {
            "blocked_by": list(self.blocked_by),
            "check_id": self.check_id,
            "evidence": dict(self.evidence),
            "evidence_hash": self.evidence_hash,
            "expires_at": self.expires_at.isoformat(),
            "failure_code": self.failure_code,
            "finished_at": self.finished_at.isoformat(),
            "implementation_digest": self.implementation_digest,
            "input_fingerprint": self.input_fingerprint,
            "operation": self.operation.value,
            "outcome": self.outcome.value,
            "remediation": self.remediation,
            "stage": self.stage.value,
            "started_at": self.started_at.isoformat(),
            "tier": self.tier,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, object]) -> CheckExecution:
        expected = {
            "blocked_by",
            "check_id",
            "evidence",
            "evidence_hash",
            "expires_at",
            "failure_code",
            "finished_at",
            "implementation_digest",
            "input_fingerprint",
            "operation",
            "outcome",
            "remediation",
            "stage",
            "started_at",
            "tier",
        }
        if set(data) != expected:
            raise ValueError("check execution fields are invalid")
        check_id = data["check_id"]
        failure_code = data["failure_code"]
        tier = data["tier"]
        evidence = data["evidence"]
        blocked_by = data["blocked_by"]
        remediation = data["remediation"]
        if (
            not isinstance(check_id, str)
            or _ID_RE.fullmatch(check_id) is None
            or not isinstance(failure_code, str)
            or _ID_RE.fullmatch(failure_code) is None
            or type(tier) is not int
            or tier not in {0, 1, 2, 3, 4}
            or not isinstance(evidence, Mapping)
            or not all(
                isinstance(key, str) and _is_evidence_value(value)
                for key, value in evidence.items()
            )
            or not isinstance(blocked_by, list)
            or not all(isinstance(value, str) and _ID_RE.fullmatch(value) for value in blocked_by)
            or (remediation is not None and not isinstance(remediation, str))
        ):
            raise ValueError("check execution identity or evidence is invalid")
        try:
            stage = StageCapability(cast(str, data["stage"]))
            operation = CheckOperation(cast(str, data["operation"]))
            outcome = CheckOutcome(cast(str, data["outcome"]))
            started_at = datetime.fromisoformat(cast(str, data["started_at"]))
            finished_at = datetime.fromisoformat(cast(str, data["finished_at"]))
            expires_at = datetime.fromisoformat(cast(str, data["expires_at"]))
        except (TypeError, ValueError) as exc:
            raise ValueError("check execution enum or timestamp is invalid") from exc
        if (
            any(
                value.tzinfo is None or value.utcoffset() is None
                for value in (
                    started_at,
                    finished_at,
                    expires_at,
                )
            )
            or not started_at <= finished_at <= expires_at
        ):
            raise ValueError("check execution timestamps are invalid")
        stage_tiers = {
            StageCapability.STATIC: {0, 1},
            StageCapability.BASELINE_LIVE_READONLY: {2},
            StageCapability.ISOLATED_REHEARSAL: {3},
            StageCapability.FINAL_ONLY: {4},
        }
        raw_evidence = dict(cast(Mapping[str, EvidenceValue], evidence))
        for field in ("input_fingerprint", "implementation_digest", "evidence_hash"):
            value = data[field]
            if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
                raise ValueError("check execution digest is invalid")
        if _hash_json(raw_evidence) != data["evidence_hash"] or tier not in stage_tiers[stage]:
            raise ValueError("check execution evidence hash or stage is invalid")
        return cls(
            check_id=check_id,
            failure_code=failure_code,
            tier=tier,
            stage=stage,
            operation=operation,
            outcome=outcome,
            input_fingerprint=cast(str, data["input_fingerprint"]),
            implementation_digest=cast(str, data["implementation_digest"]),
            evidence=MappingProxyType(raw_evidence),
            evidence_hash=data["evidence_hash"],
            started_at=started_at,
            finished_at=finished_at,
            expires_at=expires_at,
            remediation=remediation,
            blocked_by=tuple(cast(list[str], blocked_by)),
        )


def _hash_json(value: object) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def _assert_secret_safe_mapping(
    value: Mapping[str, object],
    *,
    policy: SecretRedactionPolicy,
    allow_redaction: bool,
) -> None:
    inspectable = dict(value)
    for key in value:
        normalized = key.lower()
        if _SECRET_KEY_RE.search(normalized) is None:
            continue
        allowed_fingerprint = normalized.endswith("fingerprint") or normalized.endswith(
            "fingerprints"
        )
        if policy is SecretRedactionPolicy.METADATA_FINGERPRINTS_ONLY and allowed_fingerprint:
            fingerprint_value = value[key]
            flattened = (
                fingerprint_value.values()
                if isinstance(fingerprint_value, Mapping)
                else (fingerprint_value,)
            )
            if not all(
                isinstance(item, str) and item.startswith("sha256:") and len(item) <= 96
                for item in flattened
            ):
                raise ValueError("secret metadata must contain bounded fingerprints only")
            inspectable.pop(key)
            continue
        raise ValueError("check input or evidence contains forbidden secret fields")
    rendered = redact_rollout_mapping(inspectable)
    if rendered != inspectable and not (
        policy is SecretRedactionPolicy.REDACT_ALL and allow_redaction
    ):
        raise ValueError("check input or evidence contains secret-like data")


def _validate_evidence(
    spec: CheckSpec, evidence: Mapping[str, EvidenceValue]
) -> dict[str, EvidenceValue]:
    schema = {field.name: field.value_type for field in spec.evidence_schema}
    if set(evidence) != set(schema):
        raise ValueError("check evidence does not match its declared schema")
    source: Mapping[str, EvidenceValue]
    if spec.secret_redaction_policy is SecretRedactionPolicy.REDACT_ALL:
        redacted = redact_rollout_mapping(dict(evidence))
        if not all(_is_evidence_value(value) for value in redacted.values()):
            raise ValueError("redacted evidence is not scalar")
        source = cast(Mapping[str, EvidenceValue], redacted)
    else:
        source = evidence
    _assert_secret_safe_mapping(
        source,
        policy=spec.secret_redaction_policy,
        allow_redaction=True,
    )
    normalized: dict[str, EvidenceValue] = {}
    for name, expected in schema.items():
        value = source[name]
        valid = {
            "string": isinstance(value, str),
            "integer": type(value) is int,
            "number": type(value) in {int, float},
            "boolean": type(value) is bool,
            "sha256": isinstance(value, str) and _SHA256_RE.fullmatch(value) is not None,
            "string-map": _is_bounded_string_map(value),
        }[expected]
        if not valid:
            raise ValueError(f"evidence field {name} has the wrong type")
        normalized[name] = value
    return normalized


def _is_bounded_string_map(value: object) -> bool:
    if not isinstance(value, Mapping) or len(value) > 64:
        return False
    return all(
        isinstance(key, str)
        and isinstance(item, str)
        and 1 <= len(key) <= 96
        and len(item) <= 256
        and not any(ord(char) < 32 for char in key + item)
        and redact_rollout_text(key) == key
        and redact_rollout_text(item) == item
        for key, item in value.items()
    )


def _is_evidence_value(value: object) -> bool:
    return (
        isinstance(value, (str, int, float, bool)) or value is None or _is_bounded_string_map(value)
    )


class PreflightDag:
    """Run dependency waves concurrently and retain every independent blocker."""

    def __init__(
        self,
        checks: Sequence[RegisteredCheck],
        *,
        max_concurrency: int = 8,
        attested_dependencies: frozenset[str] = frozenset(),
    ) -> None:
        if not 1 <= max_concurrency <= 32:
            raise ValueError("max_concurrency is outside the supported range")
        self._checks = {check.spec.check_id: check for check in checks}
        if len(self._checks) != len(checks) or not checks:
            raise ValueError("preflight checks must be non-empty and unique")
        if any(_ID_RE.fullmatch(value) is None for value in attested_dependencies):
            raise ValueError("attested dependency identity is invalid")
        if set(self._checks) & attested_dependencies:
            raise ValueError("attested dependencies must be external to the DAG")
        self._attested_dependencies = attested_dependencies
        for check in checks:
            missing = (
                set(check.spec.dependencies)
                - self._checks.keys()
                - self._attested_dependencies
            )
            if missing:
                raise ValueError(f"check has unknown dependencies: {sorted(missing)}")
        self._assert_acyclic()
        self._max_concurrency = max_concurrency

    def _assert_acyclic(self) -> None:
        pending = {name: set(check.spec.dependencies) for name, check in self._checks.items()}
        completed: set[str] = set(self._attested_dependencies)
        while pending:
            ready = {name for name, dependencies in pending.items() if dependencies <= completed}
            if not ready:
                raise ValueError("preflight check graph contains a dependency cycle")
            completed.update(ready)
            for name in ready:
                pending.pop(name)

    def run(
        self,
        context: CheckContext,
        *,
        operation: CheckOperation | Mapping[str, CheckOperation] = CheckOperation.PROBE,
        through_tier: int = 3,
        now: Callable[[], datetime] | None = None,
        prior_executions: Mapping[str, CheckExecution] | None = None,
        on_execution: Callable[[CheckExecution], None] | None = None,
    ) -> tuple[CheckExecution, ...]:
        if through_tier not in {0, 1, 2, 3, 4}:
            raise ValueError("through_tier must be in [0, 4]")
        clock = now or (lambda: datetime.now(UTC))
        selected = {
            name: check for name, check in self._checks.items() if check.spec.tier <= through_tier
        }
        if any(
            set(check.spec.dependencies)
            - selected.keys()
            - self._attested_dependencies
            for check in selected.values()
        ):
            raise ValueError("selected tier omits a required dependency")
        if isinstance(operation, Mapping):
            if set(operation) != set(selected) or not all(
                isinstance(value, CheckOperation) for value in operation.values()
            ):
                raise ValueError("per-check operation map is incomplete or invalid")
            operations = dict(operation)
        else:
            operations = {check_id: operation for check_id in selected}
        validation_time = clock()
        prior = dict(prior_executions or {})
        if not set(prior) <= set(selected):
            raise ValueError("prior check executions are outside the selected DAG")
        for check_id, execution in prior.items():
            check = selected[check_id]
            if (
                not execution.passed
                or execution.check_id != check_id
                or execution.failure_code != check.spec.failure_code
                or execution.tier != check.spec.tier
                or execution.stage is not check.spec.stage
                or execution.operation is not operations[check_id]
                or execution.input_fingerprint != check.input_fingerprint(context)
                or execution.implementation_digest != check.implementation_digest
                or execution.expires_at <= validation_time
            ):
                raise ValueError("prior check execution is expired or drifted")
        pending = {
            check_id: check for check_id, check in selected.items() if check_id not in prior
        }
        results: dict[str, CheckExecution] = dict(prior)
        while pending:
            ready = [
                check
                for check in pending.values()
                if set(check.spec.dependencies)
                <= (results.keys() | self._attested_dependencies)
            ]
            if not ready:
                raise RuntimeError("preflight DAG made no progress")
            runnable: list[RegisteredCheck] = []
            for check in ready:
                blocked_by = tuple(
                    dependency
                    for dependency in check.spec.dependencies
                    if dependency not in self._attested_dependencies
                    and not results[dependency].passed
                )
                if blocked_by and not check.spec.run_after_failed_dependencies:
                    results[check.spec.check_id] = self._blocked_execution(
                        check,
                        context=context,
                        operation=operations[check.spec.check_id],
                        blocked_by=blocked_by,
                        at=clock(),
                    )
                    if on_execution is not None:
                        on_execution(results[check.spec.check_id])
                else:
                    runnable.append(check)
            executor = ThreadPoolExecutor(
                max_workers=min(self._max_concurrency, max(1, len(runnable))),
                thread_name_prefix="loom-preflight",
            )
            try:
                futures = {
                    check.spec.check_id: executor.submit(
                        self._run_one,
                        check,
                        context,
                        operations[check.spec.check_id],
                        clock,
                    )
                    for check in runnable
                }
                for check in runnable:
                    future = futures[check.spec.check_id]
                    try:
                        result = future.result(timeout=check.spec.timeout_seconds)
                    except TimeoutError:
                        future.cancel()
                        at = clock()
                        result = self._failure_execution(
                            check,
                            context=context,
                            operation=operations[check.spec.check_id],
                            outcome=CheckOutcome.TIMEOUT,
                            evidence={
                                field.name: _empty_value(field)
                                for field in check.spec.evidence_schema
                            },
                            started_at=at,
                            finished_at=at,
                        )
                    results[check.spec.check_id] = result
                    if on_execution is not None:
                        on_execution(result)
            finally:
                executor.shutdown(wait=False, cancel_futures=True)
            for check in ready:
                pending.pop(check.spec.check_id)
        return tuple(sorted(results.values(), key=lambda result: (result.tier, result.check_id)))

    def _run_one(
        self,
        check: RegisteredCheck,
        context: CheckContext,
        operation: CheckOperation,
        clock: Callable[[], datetime],
    ) -> CheckExecution:
        started_at = clock()
        implementation = check.operations.get(operation)
        if implementation is None:
            probe = CheckProbe(
                passed=False,
                evidence={field.name: _empty_value(field) for field in check.spec.evidence_schema},
            )
        else:
            try:
                probe = implementation(context)
            except Exception:
                probe = CheckProbe(
                    passed=False,
                    evidence={
                        field.name: _empty_value(field) for field in check.spec.evidence_schema
                    },
                )
        finished_at = clock()
        evidence = _validate_evidence(check.spec, probe.evidence)
        return self._failure_execution(
            check,
            context=context,
            operation=operation,
            outcome=CheckOutcome.PASS if probe.passed else CheckOutcome.FAIL,
            evidence=evidence,
            started_at=started_at,
            finished_at=finished_at,
        )

    def _failure_execution(
        self,
        check: RegisteredCheck,
        *,
        context: CheckContext,
        operation: CheckOperation,
        outcome: CheckOutcome,
        evidence: Mapping[str, EvidenceValue],
        started_at: datetime,
        finished_at: datetime,
    ) -> CheckExecution:
        return CheckExecution(
            check_id=check.spec.check_id,
            failure_code=check.spec.failure_code,
            tier=check.spec.tier,
            stage=check.spec.stage,
            operation=operation,
            outcome=outcome,
            input_fingerprint=check.input_fingerprint(context),
            implementation_digest=check.implementation_digest,
            evidence=MappingProxyType(dict(evidence)),
            evidence_hash=_hash_json(evidence),
            started_at=started_at,
            finished_at=finished_at,
            expires_at=finished_at + timedelta(seconds=check.spec.freshness_ttl_seconds),
            remediation=None if outcome is CheckOutcome.PASS else check.spec.remediation,
        )

    def _blocked_execution(
        self,
        check: RegisteredCheck,
        *,
        context: CheckContext,
        operation: CheckOperation,
        blocked_by: tuple[str, ...],
        at: datetime,
    ) -> CheckExecution:
        empty = {field.name: _empty_value(field) for field in check.spec.evidence_schema}
        result = self._failure_execution(
            check,
            context=context,
            operation=operation,
            outcome=CheckOutcome.BLOCKED,
            evidence=empty,
            started_at=at,
            finished_at=at,
        )
        return replace(result, blocked_by=blocked_by)


def _empty_value(field: EvidenceField) -> EvidenceValue:
    if field.value_type == "string":
        return "unavailable"
    if field.value_type == "boolean":
        return False
    if field.value_type == "sha256":
        return "0" * 64
    if field.value_type == "string-map":
        return {}
    return 0


@dataclass(frozen=True, slots=True)
class AttestationBindings:
    candidate_sha: str
    candidate_tree: str
    image_digests: Mapping[str, str]
    runner_source_sha: str
    runner_source_tree: str
    runner_install_hash: str
    runner_config_hash: str
    staging_mutation_epoch: int
    backup_lease_id: str
    backup_lease_digest: str
    backup_manifest_sha256: str
    backup_component_set_digest: str
    db_snapshot_identity: str
    schema_revision: str
    object_inventory_root: str
    migration_plan_digest: str
    environment: str
    namespace: str
    route: str
    secret_metadata_fingerprints: Mapping[str, str]
    gb10_inventory_digest: str
    gb10_boot_ids: Mapping[str, str]
    gb10_mount_digest: str
    gb10_unit_digest: str
    browser_image_digest: str
    browser_report_schema: str

    def __post_init__(self) -> None:
        git_identities = (
            self.candidate_sha,
            self.candidate_tree,
            self.runner_source_sha,
            self.runner_source_tree,
        )
        if any(
            len(value) not in {40, 64} or any(char not in "0123456789abcdef" for char in value)
            for value in git_identities
        ):
            raise ValueError("attestation Git identity is invalid")
        for name, value in (
            ("runner install", self.runner_install_hash),
            ("runner config", self.runner_config_hash),
            ("backup lease", self.backup_lease_digest),
            ("backup manifest", self.backup_manifest_sha256),
            ("backup component set", self.backup_component_set_digest),
            ("object inventory", self.object_inventory_root),
            ("migration plan", self.migration_plan_digest),
            ("GB10 inventory", self.gb10_inventory_digest),
            ("GB10 mount", self.gb10_mount_digest),
            ("GB10 unit", self.gb10_unit_digest),
        ):
            if _SHA256_RE.fullmatch(value) is None:
                raise ValueError(f"attestation {name} hash is invalid")
        image_digests = dict(self.image_digests)
        if not image_digests or any(
            not name
            or not isinstance(value, str)
            or not value.startswith("sha256:")
            or _SHA256_RE.fullmatch(value.removeprefix("sha256:")) is None
            for name, value in image_digests.items()
        ):
            raise ValueError("attestation image digest binding is invalid")
        if (
            not self.browser_image_digest.startswith("sha256:")
            or _SHA256_RE.fullmatch(self.browser_image_digest.removeprefix("sha256:")) is None
        ):
            raise ValueError("attestation browser image digest is invalid")
        if self.staging_mutation_epoch < 0 or self.environment != "staging":
            raise ValueError("attestation staging epoch binding is invalid")
        for value in (
            self.backup_lease_id,
            self.db_snapshot_identity,
            self.schema_revision,
            self.namespace,
            self.route,
            self.browser_report_schema,
        ):
            if not value or value != value.strip():
                raise ValueError("attestation string binding is invalid")
        if not self.gb10_boot_ids or any(
            not host or not boot_id for host, boot_id in self.gb10_boot_ids.items()
        ):
            raise ValueError("attestation GB10 boot binding is invalid")
        if not self.secret_metadata_fingerprints:
            raise ValueError("attestation secret metadata binding is missing")
        if any(
            not name
            or not isinstance(value, str)
            or not value.startswith("sha256:")
            or len(value) > 96
            for name, value in self.secret_metadata_fingerprints.items()
        ):
            raise ValueError("attestation secret metadata fingerprint is invalid")
        object.__setattr__(self, "image_digests", MappingProxyType(image_digests))
        object.__setattr__(
            self,
            "secret_metadata_fingerprints",
            MappingProxyType(dict(self.secret_metadata_fingerprints)),
        )
        object.__setattr__(
            self,
            "gb10_boot_ids",
            MappingProxyType(dict(self.gb10_boot_ids)),
        )

    def to_dict(self) -> dict[str, object]:
        return _attestation_bindings_payload(self)

    @classmethod
    def from_dict(cls, data: Mapping[str, object]) -> AttestationBindings:
        expected = {field.name for field in fields(cls)}
        if set(data) != expected:
            raise ValueError("attestation bindings fields are invalid")

        def require_string(name: str) -> str:
            value = data[name]
            if not isinstance(value, str):
                raise ValueError(f"attestation binding {name} must be a string")
            return value

        def require_string_map(name: str) -> dict[str, str]:
            value = data[name]
            if not isinstance(value, Mapping) or not all(
                isinstance(key, str) and isinstance(item, str) for key, item in value.items()
            ):
                raise ValueError(f"attestation binding {name} must be a string map")
            return dict(value)

        epoch = data["staging_mutation_epoch"]
        if type(epoch) is not int:
            raise ValueError("attestation staging epoch must be an integer")
        return cls(
            candidate_sha=require_string("candidate_sha"),
            candidate_tree=require_string("candidate_tree"),
            image_digests=require_string_map("image_digests"),
            runner_source_sha=require_string("runner_source_sha"),
            runner_source_tree=require_string("runner_source_tree"),
            runner_install_hash=require_string("runner_install_hash"),
            runner_config_hash=require_string("runner_config_hash"),
            staging_mutation_epoch=epoch,
            backup_lease_id=require_string("backup_lease_id"),
            backup_lease_digest=require_string("backup_lease_digest"),
            backup_manifest_sha256=require_string("backup_manifest_sha256"),
            backup_component_set_digest=require_string("backup_component_set_digest"),
            db_snapshot_identity=require_string("db_snapshot_identity"),
            schema_revision=require_string("schema_revision"),
            object_inventory_root=require_string("object_inventory_root"),
            migration_plan_digest=require_string("migration_plan_digest"),
            environment=require_string("environment"),
            namespace=require_string("namespace"),
            route=require_string("route"),
            secret_metadata_fingerprints=require_string_map("secret_metadata_fingerprints"),
            gb10_inventory_digest=require_string("gb10_inventory_digest"),
            gb10_boot_ids=require_string_map("gb10_boot_ids"),
            gb10_mount_digest=require_string("gb10_mount_digest"),
            gb10_unit_digest=require_string("gb10_unit_digest"),
            browser_image_digest=require_string("browser_image_digest"),
            browser_report_schema=require_string("browser_report_schema"),
        )


@dataclass(frozen=True, slots=True)
class PreflightAttestation:
    schema_version: int
    bindings: AttestationBindings
    registry_digest: str
    coverage_digest: str
    check_implementation_digests: Mapping[str, str]
    evidence_hashes: Mapping[str, str]
    issued_at: datetime
    expires_at: datetime
    attestation_digest: str

    def __post_init__(self) -> None:
        if self.schema_version != 1:
            raise ValueError("preflight attestation schema is unsupported")
        if not isinstance(self.bindings, AttestationBindings):
            raise ValueError("preflight attestation bindings are invalid")
        if (
            _SHA256_RE.fullmatch(self.registry_digest) is None
            or _SHA256_RE.fullmatch(self.coverage_digest) is None
        ):
            raise ValueError("preflight attestation registry identity is invalid")
        if self.issued_at.tzinfo is None or self.expires_at.tzinfo is None:
            raise ValueError("preflight attestation timestamps must be timezone-aware")
        if self.expires_at <= self.issued_at:
            raise ValueError("preflight attestation expiry is invalid")
        implementation = _validate_digest_map(
            self.check_implementation_digests,
            "implementation",
        )
        evidence = _validate_digest_map(self.evidence_hashes, "evidence")
        if implementation.keys() != evidence.keys():
            raise ValueError("preflight attestation check maps differ")
        if _SHA256_RE.fullmatch(self.attestation_digest) is None:
            raise ValueError("preflight attestation digest is invalid")
        object.__setattr__(
            self,
            "check_implementation_digests",
            MappingProxyType(implementation),
        )
        object.__setattr__(self, "evidence_hashes", MappingProxyType(evidence))

    @classmethod
    def issue(
        cls,
        *,
        bindings: AttestationBindings,
        executions: Sequence[CheckExecution],
        issued_at: datetime,
        registry_digest: str,
        coverage_digest: str,
    ) -> PreflightAttestation:
        if issued_at.tzinfo is None:
            raise ValueError("attestation issue time must be timezone-aware")
        required = [
            result for result in executions if result.stage is not StageCapability.FINAL_ONLY
        ]
        if len({result.check_id for result in required}) != len(required):
            raise ValueError("attestation contains duplicate check evidence")
        if not required or any(not result.passed for result in required):
            raise ValueError("attestation requires every non-final check to pass")
        if any(
            result.finished_at > issued_at or result.expires_at <= issued_at for result in required
        ):
            raise ValueError("attestation contains future or expired evidence")
        if bindings.environment != "staging" or not bindings.namespace:
            raise ValueError("attestation is staging-only")
        digests = {result.check_id: result.implementation_digest for result in required}
        evidence = {result.check_id: result.evidence_hash for result in required}
        expires_at = min(result.expires_at for result in required)
        payload = {
            "schema_version": 1,
            "bindings": _attestation_bindings_payload(bindings),
            "registry_digest": registry_digest,
            "coverage_digest": coverage_digest,
            "check_implementation_digests": digests,
            "evidence_hashes": evidence,
            "issued_at": issued_at.isoformat(),
            "expires_at": expires_at.isoformat(),
        }
        return cls(
            schema_version=1,
            bindings=bindings,
            registry_digest=registry_digest,
            coverage_digest=coverage_digest,
            check_implementation_digests=MappingProxyType(digests),
            evidence_hashes=MappingProxyType(evidence),
            issued_at=issued_at,
            expires_at=expires_at,
            attestation_digest=_hash_json(payload),
        )

    def to_dict(self) -> dict[str, object]:
        payload = _preflight_attestation_payload(self)
        payload["attestation_digest"] = self.attestation_digest
        return payload

    @classmethod
    def from_dict(cls, data: Mapping[str, object]) -> PreflightAttestation:
        expected = {
            "schema_version",
            "bindings",
            "registry_digest",
            "coverage_digest",
            "check_implementation_digests",
            "evidence_hashes",
            "issued_at",
            "expires_at",
            "attestation_digest",
        }
        if set(data) != expected:
            raise ValueError("preflight attestation fields are invalid")
        schema_version = data["schema_version"]
        raw_bindings = data["bindings"]
        raw_registry = data["registry_digest"]
        raw_coverage = data["coverage_digest"]
        raw_implementation = data["check_implementation_digests"]
        raw_evidence = data["evidence_hashes"]
        raw_issued = data["issued_at"]
        raw_expires = data["expires_at"]
        raw_digest = data["attestation_digest"]
        if (
            type(schema_version) is not int
            or not isinstance(raw_bindings, Mapping)
            or not isinstance(raw_registry, str)
            or not isinstance(raw_coverage, str)
        ):
            raise ValueError("preflight attestation schema or bindings are invalid")
        if not isinstance(raw_issued, str) or not isinstance(raw_expires, str):
            raise ValueError("preflight attestation timestamps are invalid")
        if not isinstance(raw_digest, str):
            raise ValueError("preflight attestation digest is invalid")
        try:
            issued_at = datetime.fromisoformat(raw_issued)
            expires_at = datetime.fromisoformat(raw_expires)
        except ValueError as exc:
            raise ValueError("preflight attestation timestamps are invalid") from exc
        attestation = cls(
            schema_version=schema_version,
            bindings=AttestationBindings.from_dict(raw_bindings),
            registry_digest=raw_registry,
            coverage_digest=raw_coverage,
            check_implementation_digests=_validate_digest_map(
                raw_implementation,
                "implementation",
            ),
            evidence_hashes=_validate_digest_map(raw_evidence, "evidence"),
            issued_at=issued_at,
            expires_at=expires_at,
            attestation_digest=raw_digest,
        )
        if _hash_json(_preflight_attestation_payload(attestation)) != raw_digest:
            raise ValueError("preflight attestation digest does not match its payload")
        return attestation

    def valid_for(self, bindings: AttestationBindings, *, now: datetime) -> bool:
        if now.tzinfo is None:
            raise ValueError("attestation validation time must be timezone-aware")
        return now < self.expires_at and _attestation_bindings_payload(bindings) == (
            _attestation_bindings_payload(self.bindings)
        )


def _attestation_bindings_payload(bindings: AttestationBindings) -> dict[str, object]:
    payload: dict[str, object] = {}
    for field in fields(bindings):
        value = getattr(bindings, field.name)
        payload[field.name] = dict(value) if isinstance(value, Mapping) else value
    _assert_secret_safe_mapping(
        payload,
        policy=SecretRedactionPolicy.METADATA_FINGERPRINTS_ONLY,
        allow_redaction=False,
    )
    return payload


def _validate_digest_map(value: object, label: str) -> dict[str, str]:
    if not isinstance(value, Mapping) or not value or len(value) > 128:
        raise ValueError(f"preflight attestation {label} map is invalid")
    result: dict[str, str] = {}
    for key, item in value.items():
        if (
            not isinstance(key, str)
            or _ID_RE.fullmatch(key) is None
            or not isinstance(item, str)
            or _SHA256_RE.fullmatch(item) is None
        ):
            raise ValueError(f"preflight attestation {label} map is invalid")
        result[key] = item
    return result


def _preflight_attestation_payload(attestation: PreflightAttestation) -> dict[str, object]:
    return {
        "schema_version": attestation.schema_version,
        "bindings": attestation.bindings.to_dict(),
        "registry_digest": attestation.registry_digest,
        "coverage_digest": attestation.coverage_digest,
        "check_implementation_digests": dict(attestation.check_implementation_digests),
        "evidence_hashes": dict(attestation.evidence_hashes),
        "issued_at": attestation.issued_at.isoformat(),
        "expires_at": attestation.expires_at.isoformat(),
    }


__all__ = [
    "AttestationBindings",
    "CheckContext",
    "CheckExecution",
    "CheckOperation",
    "CheckOutcome",
    "CheckProbe",
    "CheckSpec",
    "EvidenceField",
    "MutationClass",
    "PreflightAttestation",
    "PreflightDag",
    "RegisteredCheck",
    "SecretRedactionPolicy",
    "StageCapability",
]
