"""Digest-approved convergence of exact backup-rotation retirements."""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import stat
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, replace
from pathlib import Path
from typing import cast

from loom_cli.rollout.lifecycle_protocol import LifecyclePhase

from .backup_retirement import BackupPayloadRetirer
from .backup_rotation import (
    BackupPayloadPhase,
    BackupPayloadRecord,
    BackupRetirementRecord,
    BackupRotationState,
    acknowledge_retirement,
    promote_candidate,
    record_restore_verified,
    recover_failed_retirement,
)
from .config import OperatorConfig
from .store import RequestStore, RequestStoreError

_PRIVATE_DIRECTORY_MODE = 0o700
_PRIVATE_FILE_MODE = 0o600


class InstalledBackupRetentionError(RuntimeError):
    """Raised when exact rotation retirement cannot proceed safely."""


class InstalledBackupRecoveryError(RuntimeError):
    """Raised when a verified candidate cannot be recovered safely."""


@dataclass(frozen=True, slots=True)
class VerifiedBackupCandidateRecoveryPlan:
    """Digest-approved authority to converge one restore-verified candidate."""

    rotation_generation: int
    rotation_sha256: str
    candidate_before: BackupPayloadRecord
    recovered_active: BackupPayloadRecord
    previous_active: BackupPayloadRecord | None
    lease_digest: str
    preflight_attestation_sha256: str
    job_sha256: str
    job_state_sha256: str
    request_sha256: str | None
    attempt_sha256: str | None
    job_phase: str
    environment: str = "staging"
    namespace: str = "loom-staging"

    def __post_init__(self) -> None:
        before = self.candidate_before
        recovered = self.recovered_active
        lease = recovered.lease
        if (
            self.rotation_generation < 0
            or len(self.rotation_sha256) != 64
            or any(character not in "0123456789abcdef" for character in self.rotation_sha256)
            or before.phase
            not in {
                BackupPayloadPhase.MANIFEST_VERIFIED,
                BackupPayloadPhase.RESTORE_VERIFIED,
            }
            or recovered.phase is not BackupPayloadPhase.ACTIVE
            or lease is None
            or before.payload_id != recovered.payload_id
            or before.request_id != recovered.request_id
            or before.bundle_name != recovered.bundle_name
            or before.created_at != recovered.created_at
            or before.manifest_sha256 != recovered.manifest_sha256
            or (
                before.phase is BackupPayloadPhase.RESTORE_VERIFIED
                and before.lease != recovered.lease
            )
            or lease.evidence_digest != self.lease_digest
            or lease.source_request_id != before.request_id
            or lease.manifest_sha256 != before.manifest_sha256
            or lease.environment != self.environment
            or lease.namespace != self.namespace
            or self.job_phase
            not in {
                LifecyclePhase.BACKUP_VERIFIED.value,
                LifecyclePhase.LAUNCH_PENDING.value,
                LifecyclePhase.LAUNCH_RUNNING.value,
            }
            or len(self.preflight_attestation_sha256) != 64
            or any(
                character not in "0123456789abcdef"
                for character in self.preflight_attestation_sha256
            )
            or any(
                len(digest) != 64
                or any(character not in "0123456789abcdef" for character in digest)
                for digest in (self.job_sha256, self.job_state_sha256)
            )
            or (
                self.job_phase
                in {
                    LifecyclePhase.LAUNCH_PENDING.value,
                    LifecyclePhase.LAUNCH_RUNNING.value,
                }
                and (
                    self.request_sha256 is None
                    or self.attempt_sha256 is None
                    or len(self.request_sha256) != 64
                    or len(self.attempt_sha256) != 64
                    or any(
                        character not in "0123456789abcdef"
                        for digest in (self.request_sha256, self.attempt_sha256)
                        for character in digest
                    )
                )
            )
            or (
                self.job_phase == LifecyclePhase.BACKUP_VERIFIED.value
                and (self.request_sha256 is not None or self.attempt_sha256 is not None)
            )
            or self.environment != "staging"
            or self.namespace != "loom-staging"
            or (
                self.previous_active is not None
                and (
                    self.previous_active.phase is not BackupPayloadPhase.ACTIVE
                    or self.previous_active.payload_id == recovered.payload_id
                )
            )
        ):
            raise ValueError("verified backup candidate recovery plan is invalid")

    def to_dict(self) -> dict[str, object]:
        return {
            "candidate_before": self.candidate_before.to_dict(),
            "environment": self.environment,
            "attempt_sha256": self.attempt_sha256,
            "job_phase": self.job_phase,
            "job_sha256": self.job_sha256,
            "job_state_sha256": self.job_state_sha256,
            "lease_digest": self.lease_digest,
            "namespace": self.namespace,
            "preflight_attestation_sha256": self.preflight_attestation_sha256,
            "previous_active": (
                self.previous_active.to_dict() if self.previous_active is not None else None
            ),
            "recovered_active": self.recovered_active.to_dict(),
            "request_sha256": self.request_sha256,
            "rotation_generation": self.rotation_generation,
            "rotation_sha256": self.rotation_sha256,
            "schema_version": 1,
        }

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> VerifiedBackupCandidateRecoveryPlan:
        expected = {
            "candidate_before",
            "environment",
            "attempt_sha256",
            "job_phase",
            "job_sha256",
            "job_state_sha256",
            "lease_digest",
            "namespace",
            "preflight_attestation_sha256",
            "previous_active",
            "recovered_active",
            "request_sha256",
            "rotation_generation",
            "rotation_sha256",
            "schema_version",
        }
        if (
            set(data) != expected
            or data["schema_version"] != 1
            or type(data["rotation_generation"]) is not int
            or not isinstance(data["rotation_sha256"], str)
            or not isinstance(data["candidate_before"], dict)
            or not isinstance(data["recovered_active"], dict)
            or (
                data["previous_active"] is not None
                and not isinstance(data["previous_active"], dict)
            )
            or not all(
                isinstance(data[field], str)
                for field in (
                    "environment",
                    "job_phase",
                    "lease_digest",
                    "namespace",
                    "preflight_attestation_sha256",
                    "job_sha256",
                    "job_state_sha256",
                )
            )
            or any(
                data[field] is not None and not isinstance(data[field], str)
                for field in ("request_sha256", "attempt_sha256")
            )
        ):
            raise ValueError("verified backup candidate recovery plan schema is invalid")
        return cls(
            rotation_generation=data["rotation_generation"],
            rotation_sha256=data["rotation_sha256"],
            candidate_before=BackupPayloadRecord.from_dict(data["candidate_before"]),
            recovered_active=BackupPayloadRecord.from_dict(data["recovered_active"]),
            previous_active=(
                BackupPayloadRecord.from_dict(data["previous_active"])
                if isinstance(data["previous_active"], dict)
                else None
            ),
            lease_digest=cast(str, data["lease_digest"]),
            preflight_attestation_sha256=cast(
                str,
                data["preflight_attestation_sha256"],
            ),
            job_sha256=cast(str, data["job_sha256"]),
            job_state_sha256=cast(str, data["job_state_sha256"]),
            request_sha256=cast(str | None, data["request_sha256"]),
            attempt_sha256=cast(str | None, data["attempt_sha256"]),
            job_phase=cast(str, data["job_phase"]),
            environment=cast(str, data["environment"]),
            namespace=cast(str, data["namespace"]),
        )

    @property
    def plan_digest(self) -> str:
        return hashlib.sha256(_json_bytes(self.to_dict())).hexdigest()


def converge_verified_backup_candidate(
    store: RequestStore,
    *,
    request_id: str,
    payload_id: str,
    bundle_name: str,
    manifest_sha256: str,
    lease_digest: str,
    activate_payload: Callable[[BackupPayloadRecord], None],
) -> BackupPayloadRecord:
    """CAS-converge the rotation ledger before a verified job may launch."""
    if store.read_backup_retention_claim() is not None:
        raise InstalledBackupRecoveryError("backup maintenance blocks verified convergence")
    lease = store.read_backup_lease(lease_digest)
    state = store.read_backup_rotation()
    active = state.active
    if active is not None and active.payload_id == payload_id and state.candidate is None:
        if (
            active.request_id != request_id
            or active.bundle_name != bundle_name
            or active.manifest_sha256 != manifest_sha256
            or active.lease != lease
        ):
            raise InstalledBackupRecoveryError("verified active backup authority drifted")
        activate_payload(active)
        return active
    candidate = state.candidate
    if (
        candidate is None
        or candidate.payload_id != payload_id
        or candidate.request_id != request_id
        or candidate.bundle_name != bundle_name
        or candidate.manifest_sha256 != manifest_sha256
        or candidate.phase
        not in {
            BackupPayloadPhase.MANIFEST_VERIFIED,
            BackupPayloadPhase.RESTORE_VERIFIED,
        }
    ):
        raise InstalledBackupRecoveryError("verified backup candidate authority drifted")
    if candidate.phase is BackupPayloadPhase.MANIFEST_VERIFIED:
        restored = record_restore_verified(
            state,
            payload_id=payload_id,
            lease=lease,
        )
        store.replace_backup_rotation(
            restored.state,
            expected_generation=state.generation,
        )
        state = store.read_backup_rotation()
    promoted = promote_candidate(
        state,
        payload_id=payload_id,
        referenced_payload_ids=store.referenced_backup_payload_ids(),
    )
    store.replace_backup_rotation(
        promoted.state,
        expected_generation=state.generation,
    )
    active = store.read_backup_rotation().active
    if active is None or active.payload_id != payload_id or active.lease != lease:
        raise InstalledBackupRecoveryError("verified backup promotion did not converge")
    activate_payload(active)
    return active


@dataclass(frozen=True, slots=True)
class BackupRotationRetentionPlan:
    """Immutable authority for pending activation and exact retirements."""

    rotation_generation: int
    rotation_sha256: str
    active_payload_id: str | None
    active_bundle_name: str | None
    active_action: str
    desired_latest_bundle: str | None
    latest_bundle: str | None
    retirements: tuple[BackupRetirementRecord, ...]
    recovered_active: BackupPayloadRecord | None = None
    environment: str = "staging"
    namespace: str = "loom-staging"

    def __post_init__(self) -> None:
        payload_ids = tuple(record.payload_id for record in self.retirements)
        recovery = self.recovered_active
        recovery_retirements = tuple(
            record
            for record in self.retirements
            if recovery is not None and record.payload_id == recovery.payload_id
        )
        action_valid = (
            self.active_payload_id is None
            and self.active_bundle_name is None
            and self.active_action == "none"
            and self.desired_latest_bundle is None
            and recovery is None
        ) or (
            self.active_payload_id is not None
            and self.active_bundle_name is not None
            and self.desired_latest_bundle == self.active_bundle_name
            and (
                (self.active_action == "activate-and-verify" and recovery is None)
                or (
                    self.active_action == "recover-and-verify"
                    and recovery is not None
                    and recovery.phase is BackupPayloadPhase.ACTIVE
                    and recovery.payload_id == self.active_payload_id
                    and recovery.bundle_name == self.active_bundle_name
                    and recovery.lease is not None
                    and recovery.lease.environment == self.environment
                    and recovery.lease.namespace == self.namespace
                    and len(recovery_retirements) == 1
                    and recovery_retirements[0].reason == "failed"
                    and recovery_retirements[0].request_id == recovery.request_id
                    and recovery_retirements[0].bundle_name == recovery.bundle_name
                    and recovery_retirements[0].manifest_sha256 == recovery.manifest_sha256
                    and self.latest_bundle == recovery.bundle_name
                )
            )
        )
        if (
            self.rotation_generation < 0
            or len(self.rotation_sha256) != 64
            or any(character not in "0123456789abcdef" for character in self.rotation_sha256)
            or self.environment != "staging"
            or self.namespace != "loom-staging"
            or not action_valid
            or (
                not self.retirements
                and (
                    self.desired_latest_bundle is None
                    or self.latest_bundle == self.desired_latest_bundle
                )
            )
            or len(set(payload_ids)) != len(payload_ids)
            or (
                self.active_payload_id in set(payload_ids)
                and (recovery is None or self.active_payload_id != recovery.payload_id)
            )
            or (self.active_payload_id is None) != (self.active_bundle_name is None)
            or any(
                value is not None and (not value or "/" in value or value in {".", ".."})
                for value in (
                    self.active_bundle_name,
                    self.desired_latest_bundle,
                    self.latest_bundle,
                )
            )
            or self.latest_bundle
            not in {
                None,
                self.active_bundle_name,
                *(record.bundle_name for record in self.retirements),
            }
        ):
            raise ValueError("backup rotation retention plan is invalid")

    def to_dict(self) -> dict[str, object]:
        return {
            "active_payload_id": self.active_payload_id,
            "active_bundle_name": self.active_bundle_name,
            "active_action": self.active_action,
            "desired_latest_bundle": self.desired_latest_bundle,
            "environment": self.environment,
            "namespace": self.namespace,
            "latest_bundle": self.latest_bundle,
            "recovered_active": (
                self.recovered_active.to_dict() if self.recovered_active is not None else None
            ),
            "retirements": [record.to_dict() for record in self.retirements],
            "rotation_generation": self.rotation_generation,
            "rotation_sha256": self.rotation_sha256,
            "schema_version": 3,
        }

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> BackupRotationRetentionPlan:
        expected = {
            "active_payload_id",
            "active_bundle_name",
            "active_action",
            "desired_latest_bundle",
            "environment",
            "namespace",
            "latest_bundle",
            "recovered_active",
            "retirements",
            "rotation_generation",
            "rotation_sha256",
            "schema_version",
        }
        if (
            set(data) != expected
            or data["schema_version"] != 3
            or type(data["rotation_generation"]) is not int
            or not isinstance(data["rotation_sha256"], str)
            or not isinstance(data["environment"], str)
            or not isinstance(data["namespace"], str)
            or not isinstance(data["active_action"], str)
            or (
                data["active_payload_id"] is not None
                and not isinstance(data["active_payload_id"], str)
            )
            or (
                data["active_bundle_name"] is not None
                and not isinstance(data["active_bundle_name"], str)
            )
            or (
                data["desired_latest_bundle"] is not None
                and not isinstance(data["desired_latest_bundle"], str)
            )
            or (data["latest_bundle"] is not None and not isinstance(data["latest_bundle"], str))
            or (
                data["recovered_active"] is not None
                and not isinstance(data["recovered_active"], dict)
            )
            or not isinstance(data["retirements"], list)
            or not all(isinstance(item, dict) for item in data["retirements"])
        ):
            raise ValueError("backup rotation retention plan schema is invalid")
        return cls(
            rotation_generation=data["rotation_generation"],
            rotation_sha256=data["rotation_sha256"],
            active_payload_id=data["active_payload_id"],
            active_bundle_name=data["active_bundle_name"],
            active_action=data["active_action"],
            desired_latest_bundle=data["desired_latest_bundle"],
            latest_bundle=data["latest_bundle"],
            recovered_active=(
                BackupPayloadRecord.from_dict(data["recovered_active"])
                if isinstance(data["recovered_active"], dict)
                else None
            ),
            retirements=tuple(
                BackupRetirementRecord.from_dict(item) for item in data["retirements"]
            ),
            environment=data["environment"],
            namespace=data["namespace"],
        )

    @property
    def plan_digest(self) -> str:
        return hashlib.sha256(_json_bytes(self.to_dict())).hexdigest()


@dataclass(slots=True)
class InstalledBackupRetentionService:
    """Claim, retire, and acknowledge only exact persisted rotation records."""

    config: OperatorConfig
    service_uid: int
    store: RequestStore
    retirer: BackupPayloadRetirer
    activate_payload: Callable[[BackupPayloadRecord], None]

    def __post_init__(self) -> None:
        if self.service_uid < 1 or not self.config.backup_rotation_maintenance_permitted():
            raise ValueError("installed backup retention authority is invalid")

    @property
    def evidence_root(self) -> Path:
        return self.config.state_root / "backup-retention-maintenance"

    def inventory(self) -> BackupRotationRetentionPlan:
        existing_claim = self.store.read_backup_retention_claim()
        if existing_claim is not None:
            return self.load_claim(existing_claim[0])
        if self.store.read_active() is not None:
            raise InstalledBackupRetentionError("active rollout blocks backup retirement")
        state = self.store.read_backup_rotation()
        if state.candidate is not None:
            raise InstalledBackupRetentionError("backup candidate blocks backup retirement")
        self._assert_no_nonterminal_backup_job(state)
        latest_bundle = self._read_latest_bundle(state)
        if not state.retirements and (
            state.active is None or latest_bundle == state.active.bundle_name
        ):
            raise InstalledBackupRetentionError(
                "backup rotation has no queued retirements or pending activation"
            )
        referenced = self.store.referenced_backup_payload_ids()
        if referenced & {record.payload_id for record in state.retirements}:
            raise InstalledBackupRetentionError("referenced backup payload blocks retirement")
        retirements = tuple(
            self.store.resolve_backup_retirement(record) for record in state.retirements
        )
        normalized_state = replace(state, retirements=retirements)
        recovered_active: BackupPayloadRecord | None = None
        if state.active is None and latest_bundle is not None:
            latest_retirements = tuple(
                record for record in retirements if record.bundle_name == latest_bundle
            )
            if (
                len(latest_retirements) != 1
                or latest_retirements[0].reason != "failed"
                or latest_retirements[0].manifest_sha256 is None
            ):
                raise InstalledBackupRetentionError("latest backup retirement is not recoverable")
            try:
                recovered_active = self.store.resolve_failed_retirement_active(
                    latest_retirements[0]
                )
            except RequestStoreError as exc:
                raise InstalledBackupRetentionError(
                    "latest backup retirement lease authority is unavailable"
                ) from exc
            self._assert_payload_job_terminal(recovered_active)
        desired_active = state.active or recovered_active
        plan = BackupRotationRetentionPlan(
            rotation_generation=state.generation,
            rotation_sha256=normalized_state.evidence_digest,
            active_payload_id=(None if desired_active is None else desired_active.payload_id),
            active_bundle_name=(None if desired_active is None else desired_active.bundle_name),
            active_action=(
                "none"
                if desired_active is None
                else (
                    "recover-and-verify" if recovered_active is not None else "activate-and-verify"
                )
            ),
            desired_latest_bundle=(None if desired_active is None else desired_active.bundle_name),
            latest_bundle=latest_bundle,
            retirements=retirements,
            recovered_active=recovered_active,
            namespace=self.config.namespace,
        )
        _publish_exact(self._plan_path(plan.plan_digest), plan.to_dict(), self.service_uid)
        return plan

    def load_claim(self, approved_plan_digest: str) -> BackupRotationRetentionPlan:
        if len(approved_plan_digest) != 64 or any(
            character not in "0123456789abcdef" for character in approved_plan_digest
        ):
            raise InstalledBackupRetentionError("backup retention approval is invalid")
        try:
            plan = BackupRotationRetentionPlan.from_dict(
                _read_exact(self._plan_path(approved_plan_digest), self.service_uid)
            )
        except (FileNotFoundError, ValueError) as exc:
            raise InstalledBackupRetentionError("backup retention approval is unavailable") from exc
        if plan.plan_digest != approved_plan_digest:
            raise InstalledBackupRetentionError("backup retention claim digest drifted")
        existing_claim = self.store.read_backup_retention_claim()
        if existing_claim is not None and existing_claim[0] != approved_plan_digest:
            raise InstalledBackupRetentionError("another backup retention claim is active")
        self._validate_current(plan)
        return plan

    def claim(self, plan: BackupRotationRetentionPlan) -> None:
        """Persist the exact start/resume fence before releasing launch.lock."""
        self._validate_current(plan)
        try:
            self.store.claim_backup_retention(
                plan.plan_digest,
                tuple(record.payload_id for record in plan.retirements),
            )
        except RequestStoreError as exc:
            raise InstalledBackupRetentionError(
                "backup retention claim could not be acquired"
            ) from exc

    def apply(self, plan: BackupRotationRetentionPlan) -> dict[str, object]:
        with self._execution_guard():
            self.claim(plan)
            result = self._apply_claimed(plan)
            self.store.clear_backup_retention_claim(plan.plan_digest)
            return result

    def _apply_claimed(self, plan: BackupRotationRetentionPlan) -> dict[str, object]:
        self._require_execution_claim(plan)
        applied_path = self.evidence_root / f"{plan.plan_digest}.applied.json"
        try:
            existing = _read_exact(applied_path, self.service_uid)
        except FileNotFoundError:
            existing = None
        if existing is not None:
            self._validate_applied_document(plan, existing)
            state = self.store.read_backup_rotation()
            self._validate_current(plan, state=state)
            state = self._converge_active(plan, state)
            state = self._reconcile_retired_absence(plan, state)
            self._require_execution_claim(plan)
            self._validate_current(plan, state=state)
            self._assert_expected_latest(plan, state)
            self._validate_applied_document(plan, existing, state=state)
            return existing
        state = self.store.read_backup_rotation()
        self._validate_current(plan, state=state)
        state = self._converge_active(plan, state)
        recovered_payload_id = (
            None if plan.recovered_active is None else plan.recovered_active.payload_id
        )
        for planned in plan.retirements:
            if planned.payload_id == recovered_payload_id:
                continue
            self._require_execution_claim(plan)
            state = self.store.read_backup_rotation()
            self._validate_current(plan, state=state)
            self._assert_expected_latest(plan, state)
            if planned.bundle_name == self._read_latest_bundle(state):
                continue
            present = tuple(
                record for record in state.retirements if record.payload_id == planned.payload_id
            )
            if not present:
                if not self.store.has_backup_retirement_receipt(planned.payload_id):
                    raise InstalledBackupRetentionError(
                        "backup retirement disappeared without an exact receipt"
                    )
                self.retirer(planned)
                self._require_execution_claim(plan)
                state = self.store.read_backup_rotation()
                self._validate_current(plan, state=state)
                self._assert_expected_latest(plan, state)
                continue
            if len(present) != 1 or self.store.resolve_backup_retirement(present[0]) != planned:
                raise InstalledBackupRetentionError("backup retirement identity drifted")
            self.retirer(planned)
            self._require_execution_claim(plan)
            current = self.store.read_backup_rotation()
            self._validate_current(plan, state=current)
            self._assert_expected_latest(plan, current)
            result = acknowledge_retirement(current, payload_id=planned.payload_id)
            self.store.replace_backup_rotation(
                result.state,
                expected_generation=current.generation,
            )
        final = self.store.read_backup_rotation()
        self._require_execution_claim(plan)
        self._validate_current(plan, state=final)
        self._assert_expected_latest(plan, final)
        if any(
            record.payload_id
            in {
                planned.payload_id
                for planned in plan.retirements
                if planned.payload_id != recovered_payload_id
            }
            for record in final.retirements
        ):
            raise InstalledBackupRetentionError("backup retirements remain after convergence")
        remaining = {record.payload_id for record in final.retirements}
        result_document: dict[str, object] = {
            "approved_plan_sha256": plan.plan_digest,
            "environment": "staging",
            "final_rotation_generation": final.generation,
            "final_rotation_sha256": final.evidence_digest,
            "namespace": self.config.namespace,
            "recovered_payload_id": recovered_payload_id,
            "retired_payload_ids": [
                record.payload_id
                for record in plan.retirements
                if record.payload_id != recovered_payload_id and record.payload_id not in remaining
            ],
            "retained_payload_ids": [
                record.payload_id
                for record in plan.retirements
                if record.payload_id != recovered_payload_id and record.payload_id in remaining
            ],
            "schema_version": 3,
        }
        self._validate_applied_document(plan, result_document, state=final)
        _publish_exact(
            applied_path,
            result_document,
            self.service_uid,
        )
        return result_document

    def _reconcile_retired_absence(
        self,
        plan: BackupRotationRetentionPlan,
        state: BackupRotationState,
    ) -> BackupRotationState:
        present = {record.payload_id for record in state.retirements}
        for planned in plan.retirements:
            if (
                plan.recovered_active is not None
                and planned.payload_id == plan.recovered_active.payload_id
            ):
                continue
            if planned.payload_id in present:
                continue
            if not self.store.has_backup_retirement_receipt(planned.payload_id):
                raise InstalledBackupRetentionError(
                    "backup retirement disappeared without an exact receipt"
                )
            self.retirer(planned)
            self._require_execution_claim(plan)
            state = self.store.read_backup_rotation()
            self._validate_current(plan, state=state)
            self._assert_expected_latest(plan, state)
        return state

    def _converge_active(
        self,
        plan: BackupRotationRetentionPlan,
        state: BackupRotationState,
    ) -> BackupRotationState:
        desired_latest = plan.desired_latest_bundle
        activated = False
        if plan.recovered_active is not None and state.active is None:
            try:
                self.activate_payload(plan.recovered_active)
            except Exception:
                raise InstalledBackupRetentionError(
                    "active backup payload activation did not complete"
                ) from None
            activated = True
            state = self.store.read_backup_rotation()
            self._require_execution_claim(plan)
            self._validate_current(plan, state=state)
            self._assert_expected_latest(plan, state)
            recovered = recover_failed_retirement(
                state,
                active=plan.recovered_active,
            )
            try:
                self.store.replace_backup_rotation(
                    recovered.state,
                    expected_generation=state.generation,
                )
            except RequestStoreError as exc:
                raise InstalledBackupRetentionError(
                    "backup rotation recovery changed concurrently"
                ) from exc
            state = self.store.read_backup_rotation()
            self._require_execution_claim(plan)
            self._validate_current(plan, state=state)
        if state.active is not None and not activated:
            if (
                plan.active_action not in {"activate-and-verify", "recover-and-verify"}
                or desired_latest is None
            ):
                raise InstalledBackupRetentionError(
                    "backup retention activation authority is invalid"
                )
            try:
                self.activate_payload(state.active)
            except Exception:
                raise InstalledBackupRetentionError(
                    "active backup payload activation did not complete"
                ) from None
            state = self.store.read_backup_rotation()
            self._require_execution_claim(plan)
            self._validate_current(plan, state=state)
        self._assert_expected_latest(plan, state)
        return state

    def _assert_expected_latest(
        self,
        plan: BackupRotationRetentionPlan,
        state: BackupRotationState,
    ) -> None:
        expected = (
            plan.desired_latest_bundle
            if plan.active_action in {"activate-and-verify", "recover-and-verify"}
            else plan.latest_bundle
        )
        if self._read_latest_bundle(state) != expected:
            raise InstalledBackupRetentionError("backup retention latest pointer drifted")

    def _validate_applied_document(
        self,
        plan: BackupRotationRetentionPlan,
        document: dict[str, object],
        *,
        state: BackupRotationState | None = None,
    ) -> None:
        expected_keys = {
            "approved_plan_sha256",
            "environment",
            "final_rotation_generation",
            "final_rotation_sha256",
            "namespace",
            "recovered_payload_id",
            "retired_payload_ids",
            "retained_payload_ids",
            "schema_version",
        }
        retired = document.get("retired_payload_ids")
        retained = document.get("retained_payload_ids")
        final_sha256 = document.get("final_rotation_sha256")
        if (
            set(document) != expected_keys
            or document.get("schema_version") != 3
            or document.get("approved_plan_sha256") != plan.plan_digest
            or document.get("environment") != self.config.environment
            or document.get("namespace") != self.config.namespace
            or document.get("recovered_payload_id")
            != (None if plan.recovered_active is None else plan.recovered_active.payload_id)
            or type(document.get("final_rotation_generation")) is not int
            or not isinstance(final_sha256, str)
            or len(final_sha256) != 64
            or any(character not in "0123456789abcdef" for character in final_sha256)
            or not isinstance(retired, list)
            or not all(isinstance(payload_id, str) for payload_id in retired)
            or not isinstance(retained, list)
            or not all(isinstance(payload_id, str) for payload_id in retained)
        ):
            raise InstalledBackupRetentionError("backup retention applied evidence is invalid")
        if state is None:
            return
        remaining = {record.payload_id for record in state.retirements}
        recovered_payload_id = (
            None if plan.recovered_active is None else plan.recovered_active.payload_id
        )
        expected_retired = [
            record.payload_id
            for record in plan.retirements
            if record.payload_id != recovered_payload_id and record.payload_id not in remaining
        ]
        expected_retained = [
            record.payload_id
            for record in plan.retirements
            if record.payload_id != recovered_payload_id and record.payload_id in remaining
        ]
        if (
            retired != expected_retired
            or retained != expected_retained
            or document["final_rotation_generation"] != state.generation
            or document["final_rotation_sha256"] != state.evidence_digest
            or any(
                not self.store.has_backup_retirement_receipt(payload_id)
                for payload_id in expected_retired
            )
        ):
            raise InstalledBackupRetentionError("backup retention applied evidence drifted")

    def _require_execution_claim(self, plan: BackupRotationRetentionPlan) -> None:
        expected = (
            plan.plan_digest,
            tuple(sorted(record.payload_id for record in plan.retirements)),
        )
        if self.store.read_backup_retention_claim() != expected:
            raise InstalledBackupRetentionError("backup retention execution claim drifted")

    @contextmanager
    def _execution_guard(self) -> Iterator[None]:
        """Serialize destructive execution without holding broker launch.lock."""
        _ensure_private_directory(self.evidence_root, self.service_uid)
        path = self.evidence_root / ".apply.lock"
        flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        try:
            fd = os.open(path, flags, _PRIVATE_FILE_MODE)
        except OSError as exc:
            raise InstalledBackupRetentionError(
                "backup retention execution lock is unavailable"
            ) from exc
        locked = False
        try:
            os.fchmod(fd, _PRIVATE_FILE_MODE)
            metadata = os.fstat(fd)
            if (
                not stat.S_ISREG(metadata.st_mode)
                or metadata.st_uid != self.service_uid
                or stat.S_IMODE(metadata.st_mode) != _PRIVATE_FILE_MODE
                or metadata.st_nlink != 1
            ):
                raise InstalledBackupRetentionError("backup retention execution lock is unsafe")
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                locked = True
            except BlockingIOError:
                raise InstalledBackupRetentionError(
                    "backup retention execution is already running"
                ) from None
            yield
        finally:
            if locked:
                fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)

    def _validate_current(
        self,
        plan: BackupRotationRetentionPlan,
        *,
        state: BackupRotationState | None = None,
    ) -> None:
        if self.store.read_active() is not None:
            raise InstalledBackupRetentionError("active rollout blocks backup retirement")
        current = state or self.store.read_backup_rotation()
        self._assert_no_nonterminal_backup_job(current)
        planned = {record.payload_id: record for record in plan.retirements}
        resolved_current = tuple(
            self.store.resolve_backup_retirement(record) for record in current.retirements
        )
        observed = {record.payload_id: record for record in resolved_current}
        observed_latest = self._read_latest_bundle(current)
        allowed_latest = {plan.latest_bundle}
        if plan.active_bundle_name is not None:
            allowed_latest.add(plan.active_bundle_name)
        recovery = plan.recovered_active
        recovered = recovery is not None and current.active == recovery
        recovery_pending = (
            recovery is not None and current.active is None and recovery.payload_id in observed
        )
        if recovery is None:
            active_matches = (
                None if current.active is None else current.active.payload_id
            ) == plan.active_payload_id and (
                None if current.active is None else current.active.bundle_name
            ) == plan.active_bundle_name
        else:
            active_matches = recovered or recovery_pending
        if (
            current.candidate is not None
            or not active_matches
            or observed_latest not in allowed_latest
            or not set(observed).issubset(planned)
            or any(planned[payload_id] != record for payload_id, record in observed.items())
            or self.store.referenced_backup_payload_ids() & set(planned)
        ):
            raise InstalledBackupRetentionError("backup rotation authority drifted")
        missing = set(planned) - set(observed)
        recovered_payload_ids = (
            {recovery.payload_id} if recovery is not None and recovered else set()
        )
        deleted = missing - recovered_payload_ids
        if any(not self.store.has_backup_retirement_receipt(payload_id) for payload_id in deleted):
            raise InstalledBackupRetentionError("backup retirement receipt is missing")
        remaining = tuple(record for record in plan.retirements if record.payload_id in observed)
        normalized_current = replace(current, retirements=resolved_current)
        initial = BackupRotationState(
            generation=plan.rotation_generation,
            active=(None if recovery is not None else normalized_current.active),
            candidate=None,
            retirements=plan.retirements,
        )
        expected_current = BackupRotationState(
            generation=(plan.rotation_generation + len(deleted) + int(bool(recovered_payload_ids))),
            active=(recovery if recovered else initial.active),
            candidate=None,
            retirements=remaining,
        )
        if (
            initial.evidence_digest != plan.rotation_sha256
            or resolved_current != remaining
            or normalized_current != expected_current
        ):
            raise InstalledBackupRetentionError("backup rotation generation or digest drifted")

    def _assert_no_nonterminal_backup_job(self, state: BackupRotationState) -> None:
        active = state.active
        if active is None:
            return
        self._assert_payload_job_terminal(active)

    def _assert_payload_job_terminal(self, active: BackupPayloadRecord) -> None:
        for read_envelope, read_state in (
            (
                self.store.read_preflight_backup_job,
                self.store.read_preflight_backup_job_state,
            ),
            (self.store.read_backup_job, self.store.read_backup_job_state),
        ):
            try:
                envelope = read_envelope(active.request_id)
            except RequestStoreError as exc:
                if "does not exist" in str(exc) or "not promoted" in str(exc):
                    continue
                raise InstalledBackupRetentionError(
                    "backup job activation authority is unreadable"
                ) from exc
            if (
                envelope.payload_id != active.payload_id
                or envelope.bundle_name != active.bundle_name
            ):
                raise InstalledBackupRetentionError("backup job activation authority is unreadable")
            try:
                job_state = read_state(active.request_id)
            except RequestStoreError as exc:
                raise InstalledBackupRetentionError(
                    "backup job activation authority is unreadable"
                ) from exc
            if job_state.phase not in {
                LifecyclePhase.BACKUP_FAILED,
                LifecyclePhase.LAUNCH_RUNNING,
            }:
                raise InstalledBackupRetentionError(
                    "nonterminal detached backup job blocks retention"
                )

    def _read_latest_bundle(self, state: BackupRotationState) -> str | None:
        latest = self.config.rollout_root / "backups" / "latest"
        try:
            metadata = latest.lstat()
        except FileNotFoundError:
            return None
        if (
            not stat.S_ISLNK(metadata.st_mode)
            or metadata.st_uid != self.service_uid
            or metadata.st_nlink != 1
        ):
            raise InstalledBackupRetentionError("latest backup pointer is unsafe")
        target = os.readlink(latest)
        observed = latest.lstat()
        if (observed.st_dev, observed.st_ino) != (metadata.st_dev, metadata.st_ino):
            raise InstalledBackupRetentionError("latest backup pointer changed during read")
        known = {
            *(record.bundle_name for record in state.retirements),
            None if state.active is None else state.active.bundle_name,
        }
        if not target or "/" in target or target not in known:
            raise InstalledBackupRetentionError("latest backup pointer is outside rotation")
        return target

    def _plan_path(self, digest: str) -> Path:
        return self.evidence_root / f"{digest}.plan.json"


@dataclass(slots=True)
class InstalledBackupRecoveryService:
    """Recover one cross-ledger verified candidate without deleting payloads."""

    config: OperatorConfig
    service_uid: int
    store: RequestStore
    activate_payload: Callable[[BackupPayloadRecord], None]

    def __post_init__(self) -> None:
        if self.service_uid < 1 or not self.config.backup_rotation_maintenance_permitted():
            raise ValueError("installed backup recovery authority is invalid")

    @property
    def evidence_root(self) -> Path:
        return self.config.state_root / "backup-recovery-maintenance"

    def _plan_path(self, plan_digest: str) -> Path:
        return self.evidence_root / f"{plan_digest}.plan.json"

    def inventory(self) -> VerifiedBackupCandidateRecoveryPlan:
        existing_claim = self.store.read_backup_retention_claim()
        if existing_claim is not None:
            return self.load_claim(existing_claim[0])
        plan = self._build_plan()
        _publish_exact(self._plan_path(plan.plan_digest), plan.to_dict(), self.service_uid)
        return plan

    def load_claim(self, approved_plan_digest: str) -> VerifiedBackupCandidateRecoveryPlan:
        if len(approved_plan_digest) != 64 or any(
            character not in "0123456789abcdef" for character in approved_plan_digest
        ):
            raise InstalledBackupRecoveryError("backup recovery approval is invalid")
        try:
            plan = VerifiedBackupCandidateRecoveryPlan.from_dict(
                _read_exact(self._plan_path(approved_plan_digest), self.service_uid)
            )
        except (FileNotFoundError, ValueError) as exc:
            raise InstalledBackupRecoveryError("backup recovery approval is unavailable") from exc
        if plan.plan_digest != approved_plan_digest:
            raise InstalledBackupRecoveryError("backup recovery plan digest drifted")
        existing_claim = self.store.read_backup_retention_claim()
        if existing_claim is not None and existing_claim != (approved_plan_digest, ()):
            raise InstalledBackupRecoveryError("another backup maintenance claim is active")
        self._validate_current(plan, allow_recovered=True)
        return plan

    def claim(self, plan: VerifiedBackupCandidateRecoveryPlan) -> None:
        self._validate_current(plan, allow_recovered=True)
        try:
            self.store.claim_backup_retention(plan.plan_digest, ())
        except RequestStoreError as exc:
            raise InstalledBackupRecoveryError(
                "backup recovery claim could not be acquired"
            ) from exc

    def apply(self, plan: VerifiedBackupCandidateRecoveryPlan) -> dict[str, object]:
        with self._execution_guard():
            self.claim(plan)
            result = self._apply_claimed(plan)
            self.store.clear_backup_retention_claim(plan.plan_digest)
            return result

    def _apply_claimed(
        self,
        plan: VerifiedBackupCandidateRecoveryPlan,
    ) -> dict[str, object]:
        self._require_claim(plan)
        applied_path = self.evidence_root / f"{plan.plan_digest}.applied.json"
        try:
            existing = _read_exact(applied_path, self.service_uid)
        except FileNotFoundError:
            existing = None
        if existing is not None:
            self._validate_applied(plan, existing)
            self._validate_current(plan, allow_recovered=True)
            self.activate_payload(plan.recovered_active)
            return existing

        state = self.store.read_backup_rotation()
        self._validate_current(plan, state=state, allow_recovered=True)
        candidate = state.candidate
        if candidate is not None and candidate.phase is BackupPayloadPhase.MANIFEST_VERIFIED:
            lease = self.store.read_backup_lease(plan.lease_digest)
            restored = record_restore_verified(
                state,
                payload_id=candidate.payload_id,
                lease=lease,
            )
            self.store.replace_backup_rotation(
                restored.state,
                expected_generation=state.generation,
            )
            state = self.store.read_backup_rotation()
            self._require_claim(plan)
        if state.candidate is not None:
            promoted = promote_candidate(
                state,
                payload_id=plan.recovered_active.payload_id,
                referenced_payload_ids=frozenset(),
            )
            self.store.replace_backup_rotation(
                promoted.state,
                expected_generation=state.generation,
            )
        final = self.store.read_backup_rotation()
        self._require_claim(plan)
        self._validate_current(plan, state=final, allow_recovered=True)
        try:
            self.activate_payload(plan.recovered_active)
        except Exception:
            raise InstalledBackupRecoveryError(
                "recovered backup activation did not complete"
            ) from None
        result: dict[str, object] = {
            "approved_plan_sha256": plan.plan_digest,
            "environment": self.config.environment,
            "final_rotation_generation": final.generation,
            "final_rotation_sha256": final.evidence_digest,
            "namespace": self.config.namespace,
            "queued_retirement_payload_ids": [record.payload_id for record in final.retirements],
            "recovered_payload_id": plan.recovered_active.payload_id,
            "schema_version": 1,
        }
        self._validate_applied(plan, result)
        _publish_exact(applied_path, result, self.service_uid)
        return result

    def _build_plan(self) -> VerifiedBackupCandidateRecoveryPlan:
        if self.store.read_active() is not None:
            raise InstalledBackupRecoveryError("active rollout blocks backup recovery")
        state = self.store.read_backup_rotation()
        if state.retirements:
            raise InstalledBackupRecoveryError(
                "queued backup retirements must converge before candidate recovery"
            )
        candidate = state.candidate
        if candidate is None or candidate.phase not in {
            BackupPayloadPhase.MANIFEST_VERIFIED,
            BackupPayloadPhase.RESTORE_VERIFIED,
        }:
            raise InstalledBackupRecoveryError(
                "backup rotation has no recoverable verified candidate"
            )
        job = self.store.read_preflight_backup_job(candidate.request_id)
        job_state = self.store.read_preflight_backup_job_state(candidate.request_id)
        if (
            job.payload_id != candidate.payload_id
            or job.request_id != candidate.request_id
            or job.bundle_name != candidate.bundle_name
            or job_state.job_id != job.job_id
            or job_state.request_id != job.request_id
            or job_state.phase
            not in {
                LifecyclePhase.BACKUP_VERIFIED,
                LifecyclePhase.LAUNCH_PENDING,
                LifecyclePhase.LAUNCH_RUNNING,
            }
            or job_state.manifest_sha256 != candidate.manifest_sha256
            or job_state.lease_digest is None
            or job_state.preflight_attestation_sha256 is None
        ):
            raise InstalledBackupRecoveryError("verified candidate backup job authority drifted")
        try:
            lease = self.store.read_backup_lease(job_state.lease_digest)
        except RequestStoreError as exc:
            raise InstalledBackupRecoveryError(
                "verified candidate lease authority is unavailable"
            ) from exc
        recovered_active = replace(
            candidate,
            phase=BackupPayloadPhase.ACTIVE,
            lease=lease,
        )
        request_sha256: str | None = None
        attempt_sha256: str | None = None
        if job_state.phase in {
            LifecyclePhase.LAUNCH_PENDING,
            LifecyclePhase.LAUNCH_RUNNING,
        }:
            try:
                request = self.store.read_request(candidate.request_id)
                attempt = self.store.read_attempt_envelope(candidate.request_id, 1)
            except RequestStoreError as exc:
                raise InstalledBackupRecoveryError(
                    "launched candidate attempt authority is unavailable"
                ) from exc
            if (
                request.candidate.resolved_sha != job.candidate_sha
                or attempt.request_id != candidate.request_id
                or attempt.attempt_number != 1
                or attempt.resolved_sha != job.candidate_sha
                or attempt.backup_manifest_sha256 != candidate.manifest_sha256
                or attempt.preflight_attestation_sha256 != job_state.preflight_attestation_sha256
            ):
                raise InstalledBackupRecoveryError("launched candidate attempt authority drifted")
            request_sha256 = hashlib.sha256(_json_bytes(request.to_dict())).hexdigest()
            attempt_sha256 = hashlib.sha256(_json_bytes(attempt.to_dict())).hexdigest()
        return VerifiedBackupCandidateRecoveryPlan(
            rotation_generation=state.generation,
            rotation_sha256=state.evidence_digest,
            candidate_before=candidate,
            recovered_active=recovered_active,
            previous_active=state.active,
            lease_digest=job_state.lease_digest,
            preflight_attestation_sha256=job_state.preflight_attestation_sha256,
            job_sha256=job.evidence_digest,
            job_state_sha256=hashlib.sha256(_json_bytes(job_state.to_dict())).hexdigest(),
            request_sha256=request_sha256,
            attempt_sha256=attempt_sha256,
            job_phase=job_state.phase.value,
            namespace=self.config.namespace,
        )

    def _validate_current(
        self,
        plan: VerifiedBackupCandidateRecoveryPlan,
        *,
        state: BackupRotationState | None = None,
        allow_recovered: bool,
    ) -> None:
        if self.store.read_active() is not None:
            raise InstalledBackupRecoveryError("active rollout blocks backup recovery")
        current = state or self.store.read_backup_rotation()
        initial = BackupRotationState(
            generation=plan.rotation_generation,
            active=plan.previous_active,
            candidate=plan.candidate_before,
        )
        if initial.evidence_digest != plan.rotation_sha256:
            raise InstalledBackupRecoveryError("backup recovery plan authority drifted")
        restored = initial
        if plan.candidate_before.phase is BackupPayloadPhase.MANIFEST_VERIFIED:
            lease = plan.recovered_active.lease
            if lease is None:
                raise InstalledBackupRecoveryError("backup recovery lease is unavailable")
            restored = record_restore_verified(
                initial,
                payload_id=plan.candidate_before.payload_id,
                lease=lease,
            ).state
        promoted = promote_candidate(
            restored,
            payload_id=plan.recovered_active.payload_id,
            referenced_payload_ids=frozenset(),
        ).state
        allowed = (initial, restored, promoted) if allow_recovered else (initial, restored)
        if current not in allowed:
            if not (
                allow_recovered
                and current.candidate is None
                and current.active == plan.recovered_active
                and all(
                    record.payload_id
                    != (None if plan.previous_active is None else plan.previous_active.payload_id)
                    or self.store.has_backup_retirement_receipt(record.payload_id)
                    for record in promoted.retirements
                    if record not in current.retirements
                )
            ):
                raise InstalledBackupRecoveryError("backup recovery state changed concurrently")
        self._validate_job_authority(plan)

    def _validate_job_authority(self, plan: VerifiedBackupCandidateRecoveryPlan) -> None:
        candidate = plan.candidate_before
        job = self.store.read_preflight_backup_job(candidate.request_id)
        state = self.store.read_preflight_backup_job_state(candidate.request_id)
        lease = self.store.read_backup_lease(plan.lease_digest)
        request_sha256: str | None = None
        attempt_sha256: str | None = None
        if state.phase in {
            LifecyclePhase.LAUNCH_PENDING,
            LifecyclePhase.LAUNCH_RUNNING,
        }:
            request = self.store.read_request(candidate.request_id)
            attempt = self.store.read_attempt_envelope(candidate.request_id, 1)
            request_sha256 = hashlib.sha256(_json_bytes(request.to_dict())).hexdigest()
            attempt_sha256 = hashlib.sha256(_json_bytes(attempt.to_dict())).hexdigest()
        if (
            job.payload_id != candidate.payload_id
            or job.bundle_name != candidate.bundle_name
            or state.job_id != job.job_id
            or state.phase.value != plan.job_phase
            or state.manifest_sha256 != candidate.manifest_sha256
            or state.lease_digest != plan.lease_digest
            or state.preflight_attestation_sha256 != plan.preflight_attestation_sha256
            or lease != plan.recovered_active.lease
            or job.evidence_digest != plan.job_sha256
            or hashlib.sha256(_json_bytes(state.to_dict())).hexdigest() != plan.job_state_sha256
            or request_sha256 != plan.request_sha256
            or attempt_sha256 != plan.attempt_sha256
        ):
            raise InstalledBackupRecoveryError("backup recovery job authority drifted")

    def _validate_applied(
        self,
        plan: VerifiedBackupCandidateRecoveryPlan,
        document: dict[str, object],
    ) -> None:
        queued = document.get("queued_retirement_payload_ids")
        if (
            set(document)
            != {
                "approved_plan_sha256",
                "environment",
                "final_rotation_generation",
                "final_rotation_sha256",
                "namespace",
                "queued_retirement_payload_ids",
                "recovered_payload_id",
                "schema_version",
            }
            or document.get("schema_version") != 1
            or document.get("approved_plan_sha256") != plan.plan_digest
            or document.get("environment") != self.config.environment
            or document.get("namespace") != self.config.namespace
            or document.get("recovered_payload_id") != plan.recovered_active.payload_id
            or type(document.get("final_rotation_generation")) is not int
            or not isinstance(document.get("final_rotation_sha256"), str)
            or not isinstance(queued, list)
            or not all(isinstance(item, str) for item in queued)
        ):
            raise InstalledBackupRecoveryError("backup recovery applied evidence is invalid")

    def _require_claim(self, plan: VerifiedBackupCandidateRecoveryPlan) -> None:
        if self.store.read_backup_retention_claim() != (plan.plan_digest, ()):
            raise InstalledBackupRecoveryError("backup recovery execution claim drifted")

    @contextmanager
    def _execution_guard(self) -> Iterator[None]:
        _ensure_private_directory(self.evidence_root, self.service_uid)
        path = self.evidence_root / ".apply.lock"
        flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        fd = os.open(path, flags, _PRIVATE_FILE_MODE)
        locked = False
        try:
            os.fchmod(fd, _PRIVATE_FILE_MODE)
            metadata = os.fstat(fd)
            if (
                not stat.S_ISREG(metadata.st_mode)
                or metadata.st_uid != self.service_uid
                or stat.S_IMODE(metadata.st_mode) != _PRIVATE_FILE_MODE
                or metadata.st_nlink != 1
            ):
                raise InstalledBackupRecoveryError("backup recovery execution lock is unsafe")
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                locked = True
            except BlockingIOError:
                raise InstalledBackupRecoveryError(
                    "backup recovery execution is already running"
                ) from None
            yield
        finally:
            if locked:
                fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)


def _json_bytes(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode() + b"\n"


def _ensure_private_directory(path: Path, service_uid: int) -> None:
    path.mkdir(parents=True, exist_ok=True, mode=_PRIVATE_DIRECTORY_MODE)
    metadata = path.lstat()
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_uid != service_uid
        or stat.S_IMODE(metadata.st_mode) != _PRIVATE_DIRECTORY_MODE
    ):
        raise InstalledBackupRetentionError("backup retention evidence directory is unsafe")


def _publish_exact(path: Path, value: dict[str, object], service_uid: int) -> None:
    _ensure_private_directory(path.parent, service_uid)
    payload = _json_bytes(value)
    try:
        descriptor = os.open(
            path,
            os.O_WRONLY
            | os.O_CREAT
            | os.O_EXCL
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0),
            _PRIVATE_FILE_MODE,
        )
    except FileExistsError as exc:
        if _read_exact(path, service_uid) != value:
            raise InstalledBackupRetentionError("backup retention evidence drifted") from exc
        return
    try:
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != service_uid
            or stat.S_IMODE(metadata.st_mode) != _PRIVATE_FILE_MODE
            or metadata.st_nlink != 1
        ):
            raise InstalledBackupRetentionError("backup retention evidence file is unsafe")
        written = 0
        while written < len(payload):
            written += os.write(descriptor, payload[written:])
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    directory = os.open(
        path.parent,
        os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_DIRECTORY", 0),
    )
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def _read_exact(path: Path, service_uid: int) -> dict[str, object]:
    metadata = path.lstat()
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != service_uid
        or stat.S_IMODE(metadata.st_mode) != _PRIVATE_FILE_MODE
        or metadata.st_nlink != 1
        or metadata.st_size > 1024 * 1024
    ):
        raise InstalledBackupRetentionError("backup retention evidence file is unsafe")
    descriptor = os.open(
        path,
        os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
    )
    try:
        opened = os.fstat(descriptor)
        if (opened.st_dev, opened.st_ino) != (metadata.st_dev, metadata.st_ino):
            raise InstalledBackupRetentionError("backup retention evidence changed during open")
        chunks: list[bytes] = []
        remaining = 1024 * 1024 + 1
        while remaining > 0:
            chunk = os.read(descriptor, min(64 * 1024, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        payload = b"".join(chunks)
    finally:
        os.close(descriptor)
    if len(payload) > 1024 * 1024:
        raise InstalledBackupRetentionError("backup retention evidence is oversized")
    try:
        value = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise InstalledBackupRetentionError("backup retention evidence is invalid") from exc
    if not isinstance(value, dict):
        raise InstalledBackupRetentionError("backup retention evidence is invalid")
    return value


__all__ = [
    "BackupRotationRetentionPlan",
    "InstalledBackupRecoveryError",
    "InstalledBackupRecoveryService",
    "InstalledBackupRetentionError",
    "InstalledBackupRetentionService",
    "VerifiedBackupCandidateRecoveryPlan",
    "converge_verified_backup_candidate",
]
