from __future__ import annotations

import base64
import hashlib
import json
import os
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import ClassVar
from uuid import UUID

import pytest
import yaml
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ed25519, rsa
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

from loom.personal_dev_capacity_runtime import (
    CapacityDatabaseCredentials,
    CapacityDatabaseInstallation,
)
from loom.staging_capacity_database_bootstrap import (
    StagingCapacityDatabaseBootstrapSettings,
    bootstrap_staging_capacity_database,
)
from loom_capacity_agent.contracts import AgentRegistrationV1, ReporterConfigurationV1
from loom_capacity_guard.contracts import GuardFenceV1, canonical_bytes
from loom_capacity_manager.contracts import FleetManifestV1, canonical_digest_excluding
from loom_cli.rollout.operator import protected_staging_capacity_runtime as protected_runtime
from loom_cli.rollout.operator.checkpoint_database_authority import DatabaseAuthorityEvidence
from loom_cli.rollout.operator.final_gate_plan import FinalGatePlan, FinalGatePlanStore
from loom_cli.rollout.operator.protected_apply_journal import (
    ComponentObservation,
    ComponentState,
    ProtectedApplyJournal,
    ProtectedApplyJournalError,
)
from loom_cli.rollout.operator.protected_staging_capacity_database_component import (
    KubernetesProtectedStagingCapacityDatabaseComponent,
    build_staging_reporter_configuration_for_candidate,
    derive_staging_reporter_incarnation,
)
from loom_cli.rollout.operator.protected_staging_capacity_runtime import (
    KubernetesProtectedStagingCapacityRuntime,
)
from tests.loom_cli.rollout.operator.protected_execution_prerequisite_fixtures import (
    execution_prerequisite_artifact,
)
from tests.loom_cli.rollout.operator.test_final_gate_plan import _execution_plan, _lease, _plan
from tests.loom_cli.rollout.operator.test_protected_staging_capacity_manager_runtime_component import (
    _candidate,
    _ManagerCluster,
)


class _NoCommandRunner:
    environment: ClassVar[dict[str, str]] = {}

    def __getattr__(self, name: str):
        raise AssertionError(f"protected convergence unexpectedly used {name}")


class _DatabaseRunner:
    def __init__(
        self,
        plan,
        seed: dict[str, object],
        *,
        database_state: str,
        fail_checked: str | None = None,
    ) -> None:
        self.environment = {"KUBECONFIG": "/fixed"}
        self.plan = plan
        self.seed = seed
        self.database_state = database_state
        self.runtime_credentials_durable = database_state == "exact"
        self.active_protected_sessions = {
            "loom_cap_staging_agent": 0,
            "loom_cap_staging_executor": 0,
            "loom_cap_staging_migrator": 0,
            "loom_cap_staging_observer": 0,
            "loom_cap_staging_owner": 0,
            "loom_cap_staging_runtime": 0,
        }
        self.protected_database_privileges = {
            "loom_cap_staging_agent": {
                "acl": [{"grantable": False, "grantor": "loom", "privilege": "CONNECT"}],
                "connect": True,
                "create": False,
                "temporary": False,
            },
            "loom_cap_staging_executor": {
                "acl": [],
                "connect": False,
                "create": False,
                "temporary": False,
            },
            "loom_cap_staging_migrator": {
                "acl": [],
                "connect": False,
                "create": False,
                "temporary": False,
            },
            "loom_cap_staging_observer": {
                "acl": [{"grantable": False, "grantor": "loom", "privilege": "CONNECT"}],
                "connect": True,
                "create": False,
                "temporary": False,
            },
            "loom_cap_staging_owner": {
                "acl": [],
                "connect": False,
                "create": False,
                "temporary": False,
            },
            "loom_cap_staging_runtime": {
                "acl": [{"grantable": False, "grantor": "loom", "privilege": "CONNECT"}],
                "connect": True,
                "create": False,
                "temporary": False,
            },
        }
        self.objects: dict[str, dict[str, object]] = {}
        self.created_objects: dict[str, dict[str, object]] = {}
        self.calls: list[tuple[str, ...]] = []
        self.checked_inputs: list[tuple[tuple[str, ...], bytes]] = []
        self.patch_inputs: list[tuple[tuple[str, ...], bytes]] = []
        self.delete_inputs: list[tuple[tuple[str, ...], bytes]] = []
        self.fail_checked = fail_checked
        self.failed_checked = False
        self._sequence = 0
        self.replace_after_patch_kind: str | None = None
        self.replace_after_selector_match_kind: str | None = None
        self.replace_before_patch_kind: str | None = None
        self.disappear_before_patch_kind: str | None = None
        self.fail_patch_after_mutation_kind: str | None = None
        self.patch_churn_counts: dict[str, int] = {}
        self.delete_wait_counts: dict[str, int] = {}
        self.api_default_jobs = False
        self.create_failure_leaves_all = False
        self.fail_database_verification_after_wait = False
        self.database_verification_failed = False
        self.events: list[str] = []
        self.fail_peer_phase_counts: dict[str, int] = {}
        self.fail_peer_phase_after_mutation_counts: dict[str, int] = {}
        self.transient_credentials_disabled = False
        self.transient_sessions_terminated = False
        self.protected_roles_sealed = False
        self.allow_sealed_runtime_impersonation = False
        self.fail_delete_job_before_mutation = 0
        self.reject_diff_validate_flag = False
        self.require_supported_patch_stdin = False
        self.activate_job_after_diff_count: int | None = None
        self.churn_job_after_diff_count: int | None = None
        self.replace_job_after_diff_count: int | None = None
        self.disappear_secret_after_diff_count: int | None = None
        self.diff_count = 0
        self.registration_overrides: dict[str, object] = {}
        self.authority_rebind_safe = True
        self.authority_rebound = False
        self.authority_rebind_audit_integrity = True
        self.authority_rebind_extra_event = False
        self.authority_rebind_incomplete = False
        self.authority_rebind_audit_read_failure = False
        self.authority_rebind_trigger_integrity = True
        self.activity_before_authority_restore = False
        self.fail_verification_after_authority_restore = False
        self.authority_restore_verification_failed = False

    def _registration(self) -> dict[str, object]:
        registration = {
            "agent_incarnation": self.seed["agent_incarnation"],
            "allocation_epoch": 0,
            "authority_incarnation": self.seed["authority_incarnation"],
            "authority_mode": "disabled",
            "candidate_digest": self.plan.artifact_bundle_digest,
            "candidate_identity": self.plan.candidate_sha,
            "candidate_identity_algorithm": "git-sha1",
            "candidate_publication_sha256": self.plan.artifact_bundle_digest,
            "configuration_generation": self.plan.starting_mutation_epoch + 1,
            "deployment_generation": self.plan.starting_mutation_epoch + 1,
            "environment_id": "staging",
            "reporter_high_water": 0,
            "reporter_incarnation": str(
                derive_staging_reporter_incarnation(
                    self.seed["reporter_incarnation"],
                    target_generation=self.plan.starting_mutation_epoch + 1,
                )
            ),
            "schema_version": 1,
            "subject_id": self.seed["subject_id"],
            "subject_incarnation": self.seed["subject_incarnation"],
        }
        registration.update(self.registration_overrides)
        return registration

    @staticmethod
    def _role(
        *,
        login: bool,
        inherit: bool,
        password: bool,
        credential_validity: str = "infinite",
    ) -> dict[str, object]:
        return {
            "bypass_rls": False,
            "can_login": login,
            "credential_validity": credential_validity,
            "create_db": False,
            "create_role": False,
            "has_password": password,
            "inherit": inherit,
            "memberships": 0,
            "replication": False,
            "superuser": False,
        }

    def _details(self) -> dict[str, object]:
        registration = self._registration()
        authority = {
            field: registration[field]
            for field in (
                "allocation_epoch",
                "authority_incarnation",
                "authority_mode",
                "candidate_digest",
                "configuration_generation",
                "deployment_generation",
                "environment_id",
                "reporter_high_water",
                "reporter_incarnation",
                "schema_version",
                "subject_id",
                "subject_incarnation",
            )
        }
        roles = {
            "loom_cap_staging_agent": self._role(
                login=True,
                inherit=False,
                password=True,
                credential_validity=(
                    "infinite" if self.runtime_credentials_durable else "finite-valid"
                ),
            ),
            "loom_cap_staging_executor": self._role(login=False, inherit=False, password=False),
            "loom_cap_staging_migrator": self._role(login=False, inherit=True, password=False),
            "loom_cap_staging_observer": self._role(
                login=True,
                inherit=False,
                password=True,
                credential_validity=(
                    "infinite" if self.runtime_credentials_durable else "finite-valid"
                ),
            ),
            "loom_cap_staging_owner": self._role(login=False, inherit=False, password=False),
            "loom_cap_staging_runtime": self._role(
                login=True,
                inherit=False,
                password=True,
                credential_validity=(
                    "infinite" if self.runtime_credentials_durable else "finite-valid"
                ),
            ),
        }
        if self.protected_roles_sealed:
            roles = {
                name: self._role(
                    login=False,
                    inherit=name == "loom_cap_staging_migrator",
                    password=False,
                )
                for name in roles
            }
        return {
            "active_protected_sessions": self.active_protected_sessions,
            "agent_role": "loom_cap_staging_agent",
            "authority": authority,
            "database_privileges": self.protected_database_privileges,
            "registration": registration,
            "roles": roles,
            "runtime_role": "loom_cap_staging_runtime",
        }

    @staticmethod
    def _audit_row(
        event_id: int,
        event_type: str,
        payload: GuardFenceV1 | AgentRegistrationV1,
    ) -> dict[str, object]:
        return {
            "event_id": event_id,
            "event_type": event_type,
            "trial_id": None,
            "protected_attempt_id": None,
            "payload": payload.model_dump(mode="json", exclude_none=False),
            "payload_digest": hashlib.sha256(canonical_bytes(payload)).hexdigest(),
        }

    def _audit_history(self) -> list[dict[str, object]]:
        current_registration = AgentRegistrationV1.model_validate_json(
            json.dumps(self._registration(), sort_keys=True)
        )
        current_fence = GuardFenceV1(
            environment_id=current_registration.environment_id,
            subject_id=current_registration.subject_id,
            subject_incarnation=current_registration.subject_incarnation,
            authority_incarnation=current_registration.authority_incarnation,
            reporter_incarnation=current_registration.reporter_incarnation,
            candidate_digest=current_registration.candidate_digest,
            deployment_generation=current_registration.deployment_generation,
            configuration_generation=current_registration.configuration_generation,
        )
        legacy_authority = UUID("558afea6-2a37-55a1-9f7c-3399695da966")
        if current_registration.authority_incarnation == legacy_authority:
            legacy_registration = current_registration
            legacy_fence = current_fence
        else:
            legacy_registration = current_registration.model_copy(
                update={"authority_incarnation": legacy_authority}
            )
            legacy_fence = current_fence.model_copy(
                update={"authority_incarnation": legacy_authority}
            )
        initial_registration = legacy_registration.model_copy(
            update={
                "candidate_digest": "d" * 64,
                "candidate_identity": "f" * 40,
                "candidate_publication_sha256": "d" * 64,
                "configuration_generation": legacy_registration.configuration_generation - 1,
                "deployment_generation": legacy_registration.deployment_generation - 1,
                "reporter_incarnation": UUID("00000000-0000-4000-8000-0000000000aa"),
            }
        )
        initial_fence = GuardFenceV1(
            environment_id=initial_registration.environment_id,
            subject_id=initial_registration.subject_id,
            subject_incarnation=initial_registration.subject_incarnation,
            authority_incarnation=initial_registration.authority_incarnation,
            reporter_incarnation=initial_registration.reporter_incarnation,
            candidate_digest=initial_registration.candidate_digest,
            deployment_generation=initial_registration.deployment_generation,
            configuration_generation=initial_registration.configuration_generation,
        )
        if (
            current_registration.authority_incarnation != legacy_authority
            and not self.authority_rebound
            and not self.authority_rebind_incomplete
        ):
            rows = [
                self._audit_row(1, "authority_initialized.v1", current_fence),
                self._audit_row(2, "agent_registered.v1", current_registration),
            ]
        else:
            rows = [
                self._audit_row(1, "authority_initialized.v1", initial_fence),
                self._audit_row(2, "agent_registered.v1", initial_registration),
                self._audit_row(3, "authority_reconfigured.v1", legacy_fence),
                self._audit_row(4, "agent_reconfigured.v1", legacy_registration),
            ]
            if self.authority_rebound:
                rows.extend(
                    (
                        self._audit_row(5, "authority_reconfigured.v1", current_fence),
                        self._audit_row(6, "agent_reconfigured.v1", current_registration),
                    )
                )
        if not self.authority_rebind_audit_integrity:
            rows[0]["payload_digest"] = "0" * 64
        if self.authority_rebind_extra_event:
            rows.append(
                {
                    "event_id": len(rows) + 1,
                    "event_type": "unexpected.v1",
                    "trial_id": None,
                    "protected_attempt_id": None,
                    "payload": {},
                    "payload_digest": "0" * 64,
                }
            )
        return rows

    def _stored(self, document: dict[str, object]) -> dict[str, object]:
        stored = deepcopy(document)
        metadata = stored["metadata"]
        assert isinstance(metadata, dict)
        self._sequence += 1
        metadata["resourceVersion"] = str(self._sequence)
        metadata["uid"] = f"11111111-1111-4111-8111-{self._sequence:012d}"
        if stored.get("kind") == "Job":
            spec = stored["spec"]
            assert isinstance(spec, dict)
            template = spec["template"]
            assert isinstance(template, dict)
            template_metadata = template["metadata"]
            assert isinstance(template_metadata, dict)
            labels = deepcopy(template_metadata["labels"])
            assert isinstance(labels, dict)
            template_metadata["labels"] = labels
            if self.api_default_jobs:
                controller_uid = metadata["uid"]
                spec.update(
                    {
                        "completionMode": "NonIndexed",
                        "completions": 1,
                        "manualSelector": False,
                        "parallelism": 1,
                        "podReplacementPolicy": "TerminatingOrFailed",
                        "selector": {
                            "matchLabels": {
                                "batch.kubernetes.io/controller-uid": controller_uid,
                            }
                        },
                        "suspend": False,
                    }
                )
                labels.update(
                    {
                        "batch.kubernetes.io/controller-uid": controller_uid,
                        "batch.kubernetes.io/job-name": metadata["name"],
                        "controller-uid": controller_uid,
                        "job-name": metadata["name"],
                    }
                )
        return stored

    @staticmethod
    def _projection(document: dict[str, object]) -> dict[str, object]:
        value = json.loads(json.dumps(document))
        value.pop("status", None)
        metadata = value["metadata"]
        assert isinstance(metadata, dict)
        for field in ("creationTimestamp", "generation", "managedFields"):
            metadata.pop(field, None)
        metadata.pop("resourceVersion", None)
        metadata.pop("uid", None)
        labels = metadata.get("labels")
        if isinstance(labels, dict):
            labels.pop("loom.carin.dev/protected-cleanup", None)
        if value.get("kind") == "Job":
            spec = value.get("spec")
            if isinstance(spec, dict):
                for field in (
                    "completionMode",
                    "manualSelector",
                    "podReplacementPolicy",
                    "selector",
                    "suspend",
                ):
                    spec.pop(field, None)
                template = spec.get("template")
                template_metadata = template.get("metadata") if isinstance(template, dict) else None
                template_labels = (
                    template_metadata.get("labels") if isinstance(template_metadata, dict) else None
                )
                if isinstance(template_labels, dict):
                    for field in (
                        "batch.kubernetes.io/controller-uid",
                        "batch.kubernetes.io/job-name",
                        "controller-uid",
                        "job-name",
                    ):
                        template_labels.pop(field, None)
        return value

    @classmethod
    def _ssa_contains(cls, desired: object, observed: object) -> bool:
        if isinstance(desired, dict) and isinstance(observed, dict):
            return all(
                key in observed and cls._ssa_contains(value, observed[key])
                for key, value in desired.items()
            )
        if isinstance(desired, list) and isinstance(observed, list):
            return len(desired) == len(observed) and all(
                cls._ssa_contains(expected, actual)
                for expected, actual in zip(desired, observed, strict=True)
            )
        return desired == observed

    @staticmethod
    def _peer_phase(payload: bytes) -> str:
        if b"protected staging capacity authority rebind" in payload:
            return "authority-rebind"
        if b"protected staging capacity authority runtime restore" in payload:
            return "authority-runtime-restore"
        if b"GRANT loom TO loom_cap_staging_migrator" in payload:
            return "arm"
        disables_migrator = b"loom_cap_staging_migrator NOLOGIN" in payload
        disables_runtime = b"loom_cap_staging_runtime NOLOGIN" in payload
        terminates = b"pg_catalog.pg_terminate_backend" in payload
        revokes = b"REVOKE ALL PRIVILEGES ON DATABASE loom" in payload
        finalizes = (
            b"loom_cap_staging_agent LOGIN" in payload and b"VALID UNTIL 'infinity'" in payload
        )
        verifies = b"transient authority is not sealed" in payload
        phases = [
            name
            for name, present in (
                ("disable-all" if disables_runtime else "disable-migrator", disables_migrator),
                ("terminate", terminates),
                ("cleanup", revokes),
                ("finalize", finalizes),
                ("verify", verifies),
            )
            if present
        ]
        return "+".join(phases) if phases else "peer"

    def capture_stdout(self, argv, *, env, timeout_seconds):
        assert env == self.environment
        assert timeout_seconds == 30.0
        command = tuple(argv)
        self.calls.append(command)
        joined = " ".join(command)
        if "authority_rebind_foundation_state" in joined:
            return b"exact\n" if self.authority_rebind_safe else b"drifted\n"
        if "authority_rebind_trigger_state" in joined:
            return b"exact\n" if self.authority_rebind_trigger_integrity else b"drifted\n"
        if "authority_rebind_audit_history" in joined:
            if self.authority_rebind_audit_read_failure:
                raise RuntimeError("injected authority rebind audit read failure")
            return json.dumps(self._audit_history(), sort_keys=True).encode()
        if "authority_rebind_recovery_ready" in joined:
            return (
                b"ready\n"
                if self.authority_rebind_safe and self.authority_rebound
                else b"blocked\n"
            )
        if "authority_rebind_ready" in joined:
            return b"ready\n" if self.authority_rebind_safe else b"blocked\n"
        if "'credentials_disabled'" in joined and "'sessions_terminated'" in joined:
            return json.dumps(
                {
                    "credentials_disabled": self.transient_credentials_disabled,
                    "sessions_terminated": self.transient_sessions_terminated,
                },
                sort_keys=True,
            ).encode()
        if "to_regclass" in joined:
            self.events.append("database-verification")
            if (
                self.fail_verification_after_authority_restore
                and "authority-runtime-restore" in self.events
                and not self.authority_restore_verification_failed
            ):
                self.authority_restore_verification_failed = True
                raise RuntimeError("injected post-restore verification failure")
            if (
                self.fail_database_verification_after_wait
                and self.database_state == "exact"
                and not self.database_verification_failed
            ):
                self.database_verification_failed = True
                raise RuntimeError("injected protected database verification failure")
            if self.database_state == "absent":
                return b"absent\n"
            return b"loom_capacity_guard.capacity_guard_alembic_version\n"
        if "version_num" in joined:
            return b"guard_0030\n"
        if "current_protected_runtime_registration" in joined:
            if self.protected_roles_sealed and not self.allow_sealed_runtime_impersonation:
                raise RuntimeError("injected sealed runtime role")
            return json.dumps(self._registration(), sort_keys=True).encode()
        if "agent_runtime_authority" in joined:
            details = self._details()
            if self.database_state == "drifted":
                details["runtime_role"] = "loom_cap_other_runtime"
            return json.dumps(details, sort_keys=True).encode()
        if "get secret,job" in joined:
            items = []
            for item in self.objects.values():
                metadata = item.get("metadata")
                labels = metadata.get("labels") if isinstance(metadata, dict) else None
                if (
                    isinstance(labels, dict)
                    and labels.get("loom.carin.dev/protected-component")
                    == "staging-capacity-database"
                ):
                    items.append(item)
            return json.dumps(
                {"apiVersion": "v1", "kind": "List", "items": items},
                sort_keys=True,
            ).encode()
        if "get secret/loom-staging-capacity-database-bootstrap" in joined:
            item = self.objects.get("Secret")
            return b"" if item is None else json.dumps(item, sort_keys=True).encode()
        if "get job/loom-staging-capacity-database-bootstrap" in joined:
            item = self.objects.get("Job")
            return b"" if item is None else json.dumps(item, sort_keys=True).encode()
        raise AssertionError(f"unexpected capture: {command}")

    def run_status(self, argv, *, env, input_payload, timeout_seconds):
        assert env == self.environment
        command = tuple(argv)
        self.calls.append(command)
        if self.reject_diff_validate_flag and "diff" in command and "--validate=strict" in command:
            raise RuntimeError("injected kubectl diff unsupported flag")
        if "wait" in command and "--for=delete" in command:
            assert timeout_seconds in {30.0, 60.0}
            assert input_payload is None
            kind = "Job" if any("job/" in item for item in command) else "Secret"
            self.events.append(f"wait-delete-{kind.lower()}")
            remaining_waits = self.delete_wait_counts.get(kind, 0)
            if remaining_waits > 0:
                self.delete_wait_counts[kind] = remaining_waits - 1
                if remaining_waits == 1:
                    self.objects.pop(kind, None)
                    return 0
                return 1
            return 0 if kind not in self.objects else 1
        if "wait" in command:
            self.events.append("wait")
            assert timeout_seconds == 30.0
            assert input_payload is None
            if self.fail_checked == "wait" and not self.failed_checked:
                self.failed_checked = True
                self.objects["Job"]["status"] = {"failed": 1}
                return 1
            self.database_state = "exact"
            self.runtime_credentials_durable = False
            self.objects["Job"]["status"] = {"succeeded": 1}
            return 0
        assert timeout_seconds == 60.0
        assert input_payload is not None
        expected = {
            document["kind"]: document
            for document in yaml.safe_load_all(input_payload)
            if document is not None
        }
        if set(expected) != set(self.objects):
            return 1
        status = (
            0
            if all(
                self._ssa_contains(
                    self._projection(expected[kind]),
                    self._projection(observed),
                )
                for kind, observed in self.objects.items()
            )
            else 1
        )
        self.diff_count += 1
        if self.disappear_secret_after_diff_count == self.diff_count:
            self.objects.pop("Secret")
        if self.churn_job_after_diff_count == self.diff_count:
            job_metadata = self.objects["Job"]["metadata"]
            assert isinstance(job_metadata, dict)
            self._sequence += 1
            job_metadata["resourceVersion"] = str(self._sequence)
        if self.replace_job_after_diff_count == self.diff_count:
            previous_job = self.objects["Job"]
            previous_status = deepcopy(previous_job["status"])
            replacement = self._stored(self._projection(previous_job))
            replacement["status"] = previous_status
            self.objects["Job"] = replacement
        if self.activate_job_after_diff_count == self.diff_count:
            job = self.objects["Job"]
            metadata = job["metadata"]
            assert isinstance(metadata, dict)
            self._sequence += 1
            metadata["resourceVersion"] = str(self._sequence)
            job["status"] = {
                "active": 1,
                "failed": 1,
                "conditions": [{"status": "True", "type": "Failed"}],
            }
        return status

    def run_checked(self, argv, *, env, input_payload, timeout_seconds):
        assert env == self.environment
        command = tuple(argv)
        self.calls.append(command)
        if "exec" in command:
            assert timeout_seconds == 60.0
            assert input_payload is not None
            self.checked_inputs.append((command, input_payload))
            phase = self._peer_phase(input_payload)
            self.events.append(phase)
            remaining_failures = self.fail_peer_phase_counts.get(phase, 0)
            if remaining_failures > 0:
                self.fail_peer_phase_counts[phase] = remaining_failures - 1
                raise RuntimeError("injected protected compensation phase failure")
            if phase == "arm":
                self.protected_roles_sealed = False
                self.transient_credentials_disabled = False
                self.transient_sessions_terminated = False
            elif phase == "disable-all":
                self.protected_roles_sealed = True
                self.transient_credentials_disabled = True
            elif phase == "terminate":
                self.transient_sessions_terminated = True
            elif phase == "authority-rebind":
                self.registration_overrides.pop("authority_incarnation", None)
                self.authority_rebound = True
            elif phase == "authority-runtime-restore":
                if (
                    self.activity_before_authority_restore
                    and b"authority restore committed-state precondition failed" in input_payload
                ):
                    raise RuntimeError("injected authority restore revalidation failure")
                self.protected_roles_sealed = False
                self.runtime_credentials_durable = True
            remaining_post_mutation_failures = self.fail_peer_phase_after_mutation_counts.get(
                phase, 0
            )
            if remaining_post_mutation_failures > 0:
                self.fail_peer_phase_after_mutation_counts[phase] = (
                    remaining_post_mutation_failures - 1
                )
                raise RuntimeError("injected protected compensation response loss")
            if phase == "finalize":
                self.runtime_credentials_durable = True
            if self.fail_checked == "exec" and phase == "arm" and not self.failed_checked:
                self.failed_checked = True
                raise RuntimeError("injected protected database mutation failure")
            return
        if "create" in command:
            self.events.append("create")
            assert timeout_seconds == 60.0
            assert input_payload is not None
            documents = [
                document for document in yaml.safe_load_all(input_payload) if document is not None
            ]
            if self.fail_checked == "create" and not self.failed_checked:
                self.failed_checked = True
                selected = documents if self.create_failure_leaves_all else documents[:1]
                self.objects = {document["kind"]: self._stored(document) for document in selected}
                raise RuntimeError("injected protected database mutation failure")
            self.objects = {document["kind"]: self._stored(document) for document in documents}
            self.created_objects = deepcopy(self.objects)
            return
        if "wait" in command:
            raise AssertionError("protected database used an unbounded Job wait")
        if "patch" in command:
            assert timeout_seconds == 60.0
            assert input_payload is not None
            if self.require_supported_patch_stdin and "--patch-file=/dev/stdin" not in command:
                raise RuntimeError("injected kubectl unsupported patch stdin path")
            kind = "Job" if "job/" in " ".join(command) else "Secret"
            observed = self.objects[kind]
            observed_metadata = observed["metadata"]
            assert isinstance(observed_metadata, dict)
            self.events.append(f"patch-{kind.lower()}")
            if self.disappear_before_patch_kind == kind:
                self.disappear_before_patch_kind = None
                self.objects.pop(kind)
                raise RuntimeError("injected protected cleanup patch race")
            if self.replace_before_patch_kind == kind:
                self.replace_before_patch_kind = None
                replacement = self._projection(observed)
                self.objects[kind] = self._stored(replacement)
                raise RuntimeError("injected protected cleanup patch race")
            remaining_churn = self.patch_churn_counts.get(kind, 0)
            if remaining_churn > 0:
                self.patch_churn_counts[kind] = remaining_churn - 1
                self._sequence += 1
                observed_metadata["resourceVersion"] = str(self._sequence)
                raise RuntimeError("injected protected cleanup patch race")
            operations = json.loads(input_payload)
            tests = {
                (operation["path"], json.dumps(operation["value"], sort_keys=True))
                for operation in operations
                if operation["op"] == "test"
            }
            assert ("/metadata/uid", json.dumps(observed_metadata["uid"])) in tests
            assert (
                "/metadata/resourceVersion",
                json.dumps(observed_metadata["resourceVersion"], sort_keys=True),
            ) in tests
            cleanup_label = None
            for operation in operations:
                if operation["op"] == "add" and operation["path"].startswith("/metadata/labels/"):
                    cleanup_label = (
                        operation["path"].removeprefix("/metadata/labels/").replace("~1", "/")
                    )
                    labels = deepcopy(observed_metadata.setdefault("labels", {}))
                    assert isinstance(labels, dict)
                    labels[cleanup_label] = operation["value"]
                    observed_metadata["labels"] = labels
            assert cleanup_label == "loom.carin.dev/protected-cleanup"
            self.patch_inputs.append((command, input_payload))
            if self.replace_after_patch_kind == kind:
                replacement = self._projection(observed)
                replacement_metadata = replacement["metadata"]
                assert isinstance(replacement_metadata, dict)
                labels = replacement_metadata["labels"]
                assert isinstance(labels, dict)
                labels.pop(cleanup_label, None)
                self.objects[kind] = self._stored(replacement)
            else:
                self._sequence += 1
                observed_metadata["resourceVersion"] = str(self._sequence)
            if self.fail_patch_after_mutation_kind == kind:
                self.fail_patch_after_mutation_kind = None
                raise RuntimeError("injected protected cleanup patch response loss")
            return
        if "delete" in command and "--raw" in command:
            assert timeout_seconds == 60.0
            assert input_payload is not None
            resource_path = next(item for item in command if item.startswith("/api"))
            kind = "Job" if "/jobs/" in resource_path else "Secret"
            self.events.append(f"delete-{kind.lower()}")
            self.delete_inputs.append((command, input_payload))
            if kind == "Job" and self.fail_delete_job_before_mutation > 0:
                self.fail_delete_job_before_mutation -= 1
                raise RuntimeError("injected protected compensation phase failure")
            observed = self.objects.get(kind)
            if observed is None:
                raise RuntimeError("injected protected cleanup delete absence")
            if self.replace_after_selector_match_kind == kind:
                self.replace_after_selector_match_kind = None
                replacement = self._projection(observed)
                replacement_metadata = replacement["metadata"]
                assert isinstance(replacement_metadata, dict)
                replacement_labels = replacement_metadata["labels"]
                assert isinstance(replacement_labels, dict)
                replacement_labels.pop("loom.carin.dev/protected-cleanup", None)
                self.objects[kind] = self._stored(replacement)
                observed = self.objects[kind]
            observed_metadata = observed["metadata"]
            assert isinstance(observed_metadata, dict)
            delete_options = json.loads(input_payload)
            assert delete_options["apiVersion"] == "v1"
            assert delete_options["kind"] == "DeleteOptions"
            assert delete_options["propagationPolicy"] == "Foreground"
            if delete_options["preconditions"] != {
                "resourceVersion": observed_metadata["resourceVersion"],
                "uid": observed_metadata["uid"],
            }:
                raise RuntimeError("injected protected cleanup delete precondition failure")
            if self.delete_wait_counts.get(kind, 0) > 0:
                observed_metadata["deletionTimestamp"] = "2026-09-05T12:00:00Z"
                observed_metadata["finalizers"] = ["foregroundDeletion"]
            else:
                self.objects.pop(kind)
            if self.fail_checked == "delete" and not self.failed_checked:
                self.failed_checked = True
                raise RuntimeError("injected protected database mutation failure")
            return
        if "delete" in command:
            assert timeout_seconds == 60.0
            assert input_payload is None
            kind = "Job" if "job" in command else "Secret"
            self.events.append(f"delete-{kind.lower()}")
            if kind == "Job" and self.fail_delete_job_before_mutation > 0:
                self.fail_delete_job_before_mutation -= 1
                raise RuntimeError("injected protected compensation phase failure")
            observed = self.objects.get(kind)
            if observed is None:
                return
            observed_metadata = observed["metadata"]
            assert isinstance(observed_metadata, dict)
            labels = observed_metadata.get("labels")
            assert isinstance(labels, dict)
            selector = next(item for item in command if item.startswith("--selector="))
            key, value = selector.removeprefix("--selector=").split("=", 1)
            if labels.get(key) != value:
                return
            if self.replace_after_selector_match_kind == kind:
                self.replace_after_selector_match_kind = None
                replacement = self._projection(observed)
                replacement_metadata = replacement["metadata"]
                assert isinstance(replacement_metadata, dict)
                replacement_labels = replacement_metadata["labels"]
                assert isinstance(replacement_labels, dict)
                replacement_labels.pop("loom.carin.dev/protected-cleanup", None)
                self.objects[kind] = self._stored(replacement)
            if kind == "Job":
                assert "--cascade=foreground" in command
                assert "--wait=true" in command
            self.objects.pop(kind)
            if self.fail_checked == "delete" and not self.failed_checked:
                self.failed_checked = True
                raise RuntimeError("injected protected database mutation failure")
            return
        raise AssertionError(f"unexpected mutation: {command}")


class _ControllerPrerequisiteTransport:
    authority_sha256 = "a" * 64

    def observe(self, _request):
        raise AssertionError("controller prerequisite transport unexpectedly observed")

    def converge(self, _request):
        raise AssertionError("controller prerequisite transport unexpectedly converged")


def _controller_prerequisite_transports() -> dict[str, _ControllerPrerequisiteTransport]:
    return {
        "gb10": _ControllerPrerequisiteTransport(),
        "oldlab": _ControllerPrerequisiteTransport(),
    }


class _PoolCredentialTransport:
    def observe(self, _request):
        raise AssertionError("pool credential transport unexpectedly observed")

    def publish(self, _request):
        raise AssertionError("pool credential transport unexpectedly published")


def _pool_credential_transports() -> dict[str, _PoolCredentialTransport]:
    return {
        "gb10": _PoolCredentialTransport(),
        "oldlab": _PoolCredentialTransport(),
    }


class _PreparedControllerTransport:
    def observe(self, _request):
        raise AssertionError("prepared controller transport unexpectedly observed")

    def converge_files(self, _request):
        raise AssertionError("prepared controller transport unexpectedly converged")

    def enable_timer(self, _request):
        raise AssertionError("prepared controller timer unexpectedly enabled")

    def run_tick(self, _request):
        raise AssertionError("prepared controller tick unexpectedly ran")

    def disable_timer(self, _request):
        raise AssertionError("prepared controller timer unexpectedly disabled")


def _prepared_controller_transports() -> dict[str, _PreparedControllerTransport]:
    return {
        "gb10": _PreparedControllerTransport(),
        "oldlab": _PreparedControllerTransport(),
    }


def _runtime(
    tmp_path: Path,
    *,
    controller_prerequisite_transports: dict[str, _ControllerPrerequisiteTransport] | None = None,
    pool_credential_transports: dict[str, _PoolCredentialTransport] | None = None,
    prepared_controller_transports: dict[str, _PreparedControllerTransport] | None = None,
) -> KubernetesProtectedStagingCapacityRuntime:
    return KubernetesProtectedStagingCapacityRuntime(
        runner=_NoCommandRunner(),  # type: ignore[arg-type]
        state_root=tmp_path / "state",
        candidate_root=tmp_path / "candidate",
        service_uid=os.geteuid(),
        service_gid=os.getegid(),
        container_registry="registry.example.test/loom",
        controller_prerequisite_transports=controller_prerequisite_transports or {},
        pool_credential_transports=pool_credential_transports or {},
        prepared_controller_transports=prepared_controller_transports or {},
        execution_preparation_dependency_guard=lambda _plan, _artifact: "d" * 64,
    )


def test_replacement_deployment_rotates_reporter_incarnation_stably() -> None:
    seed = {
        "agent_incarnation": "00000000-0000-4000-8000-000000000101",
        "authority_incarnation": "00000000-0000-4000-8000-000000000102",
        "reporter_incarnation": "00000000-0000-4000-8000-000000000103",
        "subject_id": "00000000-0000-4000-8000-000000000104",
        "subject_incarnation": "00000000-0000-4000-8000-000000000105",
    }
    predecessor = build_staging_reporter_configuration_for_candidate(
        candidate_sha="a" * 40,
        artifact_bundle_digest="b" * 64,
        mutation_epoch=40,
        seed=seed,
    )
    replacement = build_staging_reporter_configuration_for_candidate(
        candidate_sha="c" * 40,
        artifact_bundle_digest="d" * 64,
        mutation_epoch=41,
        seed=seed,
    )
    retry = build_staging_reporter_configuration_for_candidate(
        candidate_sha="c" * 40,
        artifact_bundle_digest="d" * 64,
        mutation_epoch=41,
        seed=seed,
    )

    assert replacement.reporter_incarnation != predecessor.reporter_incarnation
    assert retry.reporter_incarnation == replacement.reporter_incarnation


def test_runtime_builds_fixed_chain_and_epoch_drift_blocks_every_component(
    tmp_path: Path,
) -> None:
    plan = _plan(tmp_path)
    epoch = ComponentObservation(
        state=ComponentState.READY,
        evidence_digest="e" * 64,
        observed_epoch=plan.starting_mutation_epoch,
    )
    components = _runtime(tmp_path).components(plan, epoch_guard=lambda _plan: epoch)

    assert tuple(component.component_id for component in components) == (
        "staging-capacity-credentials",
        "staging-capacity-database",
        "staging-protected-runtime-secret",
        "capacity-manager-runtime",
        "capacity-manager-configuration",
        "staging-capacity-agent",
    )
    assert [component.classify(plan).state for component in components] == [
        ComponentState.DRIFTED,
    ] * 6
    for component in components:
        with pytest.raises(RuntimeError, match="epoch ownership changed"):
            component.apply(plan)


def test_runtime_scopes_terminal_recovery_to_legacy_capacity_foundations(
    tmp_path: Path,
) -> None:
    """Break caught: authority-forward recovery is missing or exposed to later components."""
    plan = _plan(tmp_path)
    epoch = ComponentObservation(
        state=ComponentState.EXACT,
        evidence_digest="e" * 64,
        observed_epoch=plan.starting_mutation_epoch + 1,
    )

    components = _runtime(tmp_path).components(plan, epoch_guard=lambda _plan: epoch)

    assert tuple(
        component.component_id
        for component in components
        if component.terminal_recovery_authority is not None
    ) == (
        "staging-capacity-credentials",
        "staging-capacity-database",
        "staging-protected-runtime-secret",
    )


def test_execution_plan_converges_both_controller_prerequisites_before_credentials(
    tmp_path: Path,
) -> None:
    plan = _execution_plan(tmp_path)
    epoch = ComponentObservation(
        state=ComponentState.READY,
        evidence_digest="e" * 64,
        observed_epoch=plan.starting_mutation_epoch,
    )

    components = _runtime(
        tmp_path,
        controller_prerequisite_transports=_controller_prerequisite_transports(),
        pool_credential_transports=_pool_credential_transports(),
        prepared_controller_transports=_prepared_controller_transports(),
    ).components(plan, epoch_guard=lambda _plan: epoch)

    assert tuple(component.component_id for component in components) == (
        "staging-capacity-credentials",
        "staging-capacity-database",
        "staging-protected-runtime-secret",
        "oldlab-controller-prerequisite",
        "gb10-controller-prerequisite",
        "staging-capacity-execution-credentials",
        "capacity-manager-runtime",
        "capacity-manager-configuration",
        "staging-capacity-agent",
        "capacity-execution-preparation",
    )


def test_execution_plan_rejects_missing_controller_prerequisite_transport(
    tmp_path: Path,
) -> None:
    plan = _execution_plan(tmp_path)
    epoch = ComponentObservation(
        state=ComponentState.EXACT,
        evidence_digest="e" * 64,
        observed_epoch=plan.starting_mutation_epoch + 1,
    )

    with pytest.raises(ValueError, match="controller prerequisite transports are incomplete"):
        _runtime(
            tmp_path,
            pool_credential_transports=_pool_credential_transports(),
            prepared_controller_transports=_prepared_controller_transports(),
        ).components(plan, epoch_guard=lambda _plan: epoch)


def test_execution_plan_rejects_missing_pool_credential_transport(tmp_path: Path) -> None:
    plan = _execution_plan(tmp_path)
    epoch = ComponentObservation(
        state=ComponentState.EXACT,
        evidence_digest="e" * 64,
        observed_epoch=plan.starting_mutation_epoch + 1,
    )

    with pytest.raises(ValueError, match="pool credential transports are incomplete"):
        _runtime(
            tmp_path,
            controller_prerequisite_transports=_controller_prerequisite_transports(),
            prepared_controller_transports=_prepared_controller_transports(),
        ).components(plan, epoch_guard=lambda _plan: epoch)


def test_controller_prerequisite_components_dispatch_pool_specific_authority(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan = _execution_plan(tmp_path)
    epoch = ComponentObservation(
        state=ComponentState.EXACT,
        evidence_digest="e" * 64,
        observed_epoch=plan.starting_mutation_epoch + 1,
    )
    classified: list[tuple[str, FinalGatePlan]] = []

    class _ControllerComponent:
        def __init__(self, pool_id: str) -> None:
            self.pool_id = pool_id

        def classify(self, bound_plan: FinalGatePlan) -> tuple[ComponentState, str]:
            classified.append((self.pool_id, bound_plan))
            return ComponentState.EXACT, self.pool_id

    monkeypatch.setattr(
        KubernetesProtectedStagingCapacityRuntime,
        "_controller_prerequisite_component",
        lambda _runtime, pool_id: _ControllerComponent(pool_id),
        raising=False,
    )
    components = {
        component.component_id: component
        for component in _runtime(
            tmp_path,
            controller_prerequisite_transports=_controller_prerequisite_transports(),
            pool_credential_transports=_pool_credential_transports(),
            prepared_controller_transports=_prepared_controller_transports(),
        ).components(plan, epoch_guard=lambda _plan: epoch)
    }

    assert components["oldlab-controller-prerequisite"].classify(plan).state is ComponentState.EXACT
    assert components["gb10-controller-prerequisite"].classify(plan).state is ComponentState.EXACT
    assert classified == [("oldlab", plan), ("gb10", plan)]


def test_preparation_dependency_rechecks_every_task_43_to_45_component(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan = _execution_plan(tmp_path)
    runtime = _runtime(
        tmp_path,
        controller_prerequisite_transports=_controller_prerequisite_transports(),
        pool_credential_transports=_pool_credential_transports(),
        prepared_controller_transports=_prepared_controller_transports(),
    )
    calls: list[str] = []

    class _ExactComponent:
        def __init__(self, component_id: str) -> None:
            self.component_id = component_id

        def classify(self, _plan: FinalGatePlan) -> tuple[ComponentState, str]:
            calls.append(self.component_id)
            return ComponentState.EXACT, self.component_id

    monkeypatch.setattr(
        KubernetesProtectedStagingCapacityRuntime,
        "_controller_prerequisite_component",
        lambda _runtime, pool_id: _ExactComponent(f"controller-{pool_id}"),
    )
    monkeypatch.setattr(
        KubernetesProtectedStagingCapacityRuntime,
        "_execution_credential_component",
        lambda _runtime: _ExactComponent("execution-credentials"),
    )
    monkeypatch.setattr(
        KubernetesProtectedStagingCapacityRuntime,
        "_manager_runtime_component",
        lambda _runtime, _plan: _ExactComponent("manager-runtime"),
    )
    monkeypatch.setattr(
        KubernetesProtectedStagingCapacityRuntime,
        "_manager_configuration_component",
        lambda _runtime, _plan: _ExactComponent("manager-configuration"),
    )

    digest = runtime._execution_preparation_dependency(
        plan,
        execution_prerequisite_artifact(),
    )

    assert len(digest) == 64
    assert calls == [
        "controller-gb10",
        "controller-oldlab",
        "execution-credentials",
        "manager-runtime",
        "manager-configuration",
    ]


def _certificate(
    *,
    common_name: str,
    issuer_key: rsa.RSAPrivateKey,
    issuer_name: x509.Name,
    subject_key: rsa.RSAPrivateKey,
    is_ca: bool = False,
    uri_san: str | None = None,
) -> x509.Certificate:
    now = datetime.now(UTC)
    builder = (
        x509.CertificateBuilder()
        .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common_name)]))
        .issuer_name(issuer_name)
        .public_key(subject_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=1))
        .not_valid_after(now + timedelta(days=30))
        .add_extension(x509.BasicConstraints(ca=is_ca, path_length=None), critical=True)
    )
    if not is_ca:
        builder = builder.add_extension(
            x509.ExtendedKeyUsage([ExtendedKeyUsageOID.CLIENT_AUTH]), critical=False
        )
    if uri_san is not None:
        builder = builder.add_extension(
            x509.SubjectAlternativeName([x509.UniformResourceIdentifier(uri_san)]),
            critical=False,
        )
    return builder.sign(issuer_key, hashes.SHA256())


def _write_bootstrap(
    runtime: KubernetesProtectedStagingCapacityRuntime,
) -> dict[str, rsa.RSAPrivateKey]:
    runtime.state_root.mkdir(mode=0o700)
    runtime.credentials_root.parent.mkdir(mode=0o700)
    runtime.credentials_root.mkdir(mode=0o700)
    client_ca_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    client_ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "manager-client-ca")])
    client_ca_cert = _certificate(
        common_name="manager-client-ca",
        issuer_key=client_ca_key,
        issuer_name=client_ca_name,
        subject_key=client_ca_key,
        is_ca=True,
    )
    manager_ca_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    manager_ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "manager-server-ca")])
    manager_ca_cert = _certificate(
        common_name="manager-server-ca",
        issuer_key=manager_ca_key,
        issuer_name=manager_ca_name,
        subject_key=manager_ca_key,
        is_ca=True,
    )
    client_ca_path = runtime.credentials_root / "client-ca.pem"
    client_ca_path.write_bytes(client_ca_cert.public_bytes(serialization.Encoding.PEM))
    client_ca_path.chmod(0o600)
    clients = {
        "configuration-read": (
            "bearer-token",
            "certificate.pem",
            "manager-ca.pem",
            "private-key.pem",
        ),
        "configuration-fleet": (
            "bearer-token",
            "certificate.pem",
            "manager-ca.pem",
            "private-key.pem",
        ),
        "configuration-subject": (
            "bearer-token",
            "certificate.pem",
            "manager-ca.pem",
            "private-key.pem",
        ),
        "configuration-activate": (
            "bearer-token",
            "certificate.pem",
            "manager-ca.pem",
            "private-key.pem",
        ),
        "staging-reporter": (
            "certificate.pem",
            "manager-ca.pem",
            "private-key.pem",
        ),
        **{
            principal: (
                "bearer-token",
                "certificate.pem",
                "manager-ca.pem",
                "private-key.pem",
            )
            for principal in (
                "manager-read",
                "manager-prepare",
                "manager-activate",
                "manager-drain",
                "manager-retire",
                "manager-abort",
                "pool-executor-gb10",
                "pool-executor-oldlab",
            )
        },
    }
    private_keys: dict[str, rsa.RSAPrivateKey] = {"client-ca": client_ca_key}
    for directory_name, file_names in clients.items():
        directory = runtime.credentials_root / directory_name
        directory.mkdir(mode=0o700)
        private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        private_keys[directory_name] = private_key
        certificate = _certificate(
            common_name=directory_name,
            issuer_key=client_ca_key,
            issuer_name=client_ca_cert.subject,
            subject_key=private_key,
            uri_san=(
                f"spiffe://loom.openai.dev/staging/capacity/{directory_name}"
                if directory_name.startswith(("manager-", "pool-executor-"))
                else None
            ),
        )
        for file_name in file_names:
            path = directory / file_name
            if file_name == "certificate.pem":
                payload = certificate.public_bytes(serialization.Encoding.PEM)
            elif file_name == "manager-ca.pem":
                payload = manager_ca_cert.public_bytes(serialization.Encoding.PEM)
            elif file_name == "private-key.pem":
                payload = private_key.private_bytes(
                    serialization.Encoding.PEM,
                    serialization.PrivateFormat.PKCS8,
                    serialization.NoEncryption(),
                )
            else:
                payload = f"token-{directory_name}-{'x' * 48}".encode("ascii")
            path.write_bytes(payload)
            path.chmod(0o600)
    for pool in ("gb10", "oldlab"):
        directory = runtime.credentials_root / f"pool-ownership-{pool}"
        directory.mkdir(mode=0o700)
        path = directory / "ownership-private-key"
        path.write_bytes(
            ed25519.Ed25519PrivateKey.generate().private_bytes(
                serialization.Encoding.Raw,
                serialization.PrivateFormat.Raw,
                serialization.NoEncryption(),
            )
        )
        path.chmod(0o600)
    return private_keys


def test_runtime_exposes_exact_execution_credential_metadata(tmp_path: Path) -> None:
    runtime = _runtime(tmp_path)
    _write_bootstrap(runtime)

    metadata_reader = getattr(runtime, "read_execution_credential_metadata", None)
    assert metadata_reader is not None, "runtime execution credential metadata reader is missing"
    metadata = metadata_reader()

    assert set(metadata) == {
        "manager-abort",
        "manager-activate",
        "manager-drain",
        "manager-prepare",
        "manager-read",
        "manager-retire",
        "pool-executor-gb10",
        "pool-executor-oldlab",
        "pool-ownership-gb10",
        "pool-ownership-oldlab",
    }


def test_credentials_component_persists_one_candidate_independent_seed(
    tmp_path: Path,
) -> None:
    runtime = _runtime(tmp_path)
    _write_bootstrap(runtime)
    plan = _plan(tmp_path)
    epoch = ComponentObservation(
        state=ComponentState.EXACT,
        evidence_digest="e" * 64,
        observed_epoch=plan.starting_mutation_epoch + 1,
    )
    component = runtime.components(plan, epoch_guard=lambda _plan: epoch)[0]

    assert component.classify(plan).state is ComponentState.READY
    component.apply(plan)
    exact = component.classify(plan)

    assert exact.state is ComponentState.EXACT
    assert runtime.credential_seed_path.stat().st_mode & 0o777 == 0o600
    seed = json.loads(runtime.credential_seed_path.read_text())
    assert set(seed) == {
        "agent_database_password",
        "agent_incarnation",
        "authority_incarnation",
        "migrator_database_password",
        "observer_database_password",
        "reporter_incarnation",
        "reporter_token",
        "runtime_database_password",
        "schema_version",
        "subject_id",
        "subject_incarnation",
    }
    assert seed["schema_version"] == 1
    assert seed["authority_incarnation"] == plan.manager_authority_incarnation
    assert plan.candidate_sha not in runtime.credential_seed_path.read_text()
    before = runtime.credential_seed_path.read_bytes()
    with pytest.raises(RuntimeError, match="state changed"):
        component.apply(plan)
    assert runtime.credential_seed_path.read_bytes() == before


def test_credentials_component_repairs_only_legacy_deterministic_authority(
    tmp_path: Path,
) -> None:
    """Break caught: a partial bootstrap retaining an authority foreign to its frozen plan."""
    runtime = _runtime(tmp_path)
    _write_bootstrap(runtime)
    runtime._create_credential_seed(UUID("558afea6-2a37-55a1-9f7c-3399695da966"))
    legacy = json.loads(runtime.credential_seed_path.read_text())
    assert legacy["authority_incarnation"] == "558afea6-2a37-55a1-9f7c-3399695da966"
    plan = _plan(tmp_path)
    epoch = ComponentObservation(
        state=ComponentState.EXACT,
        evidence_digest="e" * 64,
        observed_epoch=plan.starting_mutation_epoch + 1,
    )
    component = runtime.components(plan, epoch_guard=lambda _plan: epoch)[0]

    assert component.classify(plan).state is ComponentState.READY
    component.apply(plan)

    repaired = json.loads(runtime.credential_seed_path.read_text())
    assert repaired == {
        **legacy,
        "authority_incarnation": plan.manager_authority_incarnation,
    }
    assert runtime.credential_seed_path.stat().st_mode & 0o777 == 0o600
    assert component.classify(plan).state is ComponentState.EXACT


def test_journal_recovers_immutable_legacy_capacity_terminal_by_authority_forward(
    tmp_path: Path,
) -> None:
    """Break caught: a legacy terminal blocks its narrowly authorized authority repair."""
    runtime = _runtime(tmp_path)
    _write_bootstrap(runtime)
    legacy_authority = "558afea6-2a37-55a1-9f7c-3399695da966"
    runtime._create_credential_seed(UUID(legacy_authority))
    plan = _plan(tmp_path)
    epoch = ComponentObservation(
        state=ComponentState.EXACT,
        evidence_digest="e" * 64,
        observed_epoch=plan.starting_mutation_epoch + 1,
    )
    component = runtime.components(plan, epoch_guard=lambda _plan: epoch)[0]
    legacy_observation = component.classify(plan)
    assert legacy_observation.state is ComponentState.READY

    historical_component = replace(
        component,
        classify=lambda _plan: ComponentObservation(
            state=ComponentState.EXACT,
            evidence_digest=legacy_observation.evidence_digest,
            observed_epoch=legacy_observation.observed_epoch,
        ),
        apply=lambda _plan: pytest.fail("historical exact state must not mutate"),
    )
    attempt_root = runtime.state_root / f"requests/{plan.request_id}/attempts/{plan.attempt_number}"
    attempt_root.mkdir(parents=True, mode=0o700)
    journal = ProtectedApplyJournal(
        runtime.state_root,
        request_id=plan.request_id,
        attempt_number=plan.attempt_number,
        service_uid=os.geteuid(),
    )
    historical_terminal = journal.execute(plan, (historical_component,))[component.component_id]
    component_root = journal.root / f"00-{component.component_id}"
    original_terminal = (component_root / "terminal.json").read_bytes()

    recovered_terminal = journal.execute(plan, (component,))[component.component_id]
    repaired_seed = runtime.credential_seed_path.read_bytes()
    replayed_terminal = journal.execute(plan, (component,))[component.component_id]

    assert json.loads(repaired_seed)["authority_incarnation"] == plan.manager_authority_incarnation
    assert (component_root / "terminal.json").read_bytes() == original_terminal
    assert recovered_terminal == replayed_terminal
    assert recovered_terminal.evidence_digest != historical_terminal.evidence_digest
    assert runtime.credential_seed_path.read_bytes() == repaired_seed

    recovery_intent = json.loads((component_root / "terminal-recovery-intent.json").read_text())
    recovery = json.loads((component_root / "terminal-recovery.json").read_text())
    historical_record = json.loads(original_terminal)
    assert recovery_intent["request_id"] == plan.request_id
    assert recovery_intent["attempt_number"] == plan.attempt_number
    assert recovery_intent["plan_digest"] == plan.plan_digest
    assert recovery_intent["candidate_sha"] == plan.candidate_sha
    assert recovery_intent["candidate_tree"] == plan.candidate_tree
    assert recovery_intent["component_id"] == component.component_id
    assert recovery_intent["ordinal"] == 0
    assert recovery_intent["component_intent_digest"] == historical_record["intent_digest"]
    assert recovery_intent["prior_terminal_digest"] == historical_record["terminal_digest"]
    assert recovery_intent["source_authority_incarnation"] == legacy_authority
    assert recovery_intent["target_authority_incarnation"] == plan.manager_authority_incarnation
    assert recovery_intent["observed_epoch"] == plan.starting_mutation_epoch + 1
    assert recovery["recovery_intent_digest"] == recovery_intent["recovery_intent_digest"]
    assert recovery["evidence_digest"] == recovered_terminal.evidence_digest
    assert recovery["observed_epoch"] == recovered_terminal.observed_epoch
    assert recovery["effective_terminal_digest"] == recovered_terminal.terminal_digest


def test_journal_recovers_authority_mutation_without_repeating_after_publish_crash(
    tmp_path: Path,
) -> None:
    """Break caught: a crash after authority mutation causes replay to mutate it again."""
    runtime = _runtime(tmp_path)
    _write_bootstrap(runtime)
    runtime._create_credential_seed(UUID("558afea6-2a37-55a1-9f7c-3399695da966"))
    plan = _plan(tmp_path)
    epoch = ComponentObservation(
        state=ComponentState.EXACT,
        evidence_digest="e" * 64,
        observed_epoch=plan.starting_mutation_epoch + 1,
    )
    component = runtime.components(plan, epoch_guard=lambda _plan: epoch)[0]
    legacy_observation = component.classify(plan)
    historical_component = replace(
        component,
        classify=lambda _plan: ComponentObservation(
            state=ComponentState.EXACT,
            evidence_digest=legacy_observation.evidence_digest,
            observed_epoch=legacy_observation.observed_epoch,
        ),
        apply=lambda _plan: pytest.fail("historical exact state must not mutate"),
    )
    attempt_root = runtime.state_root / f"requests/{plan.request_id}/attempts/{plan.attempt_number}"
    attempt_root.mkdir(parents=True, mode=0o700)
    journal = ProtectedApplyJournal(
        runtime.state_root,
        request_id=plan.request_id,
        attempt_number=plan.attempt_number,
        service_uid=os.geteuid(),
    )
    journal.execute(plan, (historical_component,))
    original_publish = journal._publish_or_match
    crashed = False

    def crash_before_recovery_terminal(path, value):
        nonlocal crashed
        if path.name == "terminal-recovery.json" and not crashed:
            crashed = True
            raise RuntimeError("simulated recovery terminal publication crash")
        original_publish(path, value)

    journal._publish_or_match = crash_before_recovery_terminal  # type: ignore[method-assign]
    with pytest.raises(RuntimeError, match="publication crash"):
        journal.execute(plan, (component,))
    repaired_seed = runtime.credential_seed_path.read_bytes()
    assert json.loads(repaired_seed)["authority_incarnation"] == plan.manager_authority_incarnation
    component_root = journal.root / f"00-{component.component_id}"
    assert (component_root / "terminal-recovery-intent.json").exists()
    assert not (component_root / "terminal-recovery.json").exists()

    journal._publish_or_match = original_publish  # type: ignore[method-assign]
    recovered = journal.execute(plan, (component,))[component.component_id]

    assert runtime.credential_seed_path.read_bytes() == repaired_seed
    assert recovered.applied is False
    assert (component_root / "terminal-recovery.json").exists()


def test_credentials_component_rejects_unrelated_authority_drift(tmp_path: Path) -> None:
    """Break caught: treating an arbitrary valid UUID as the known partial-bootstrap seed."""
    runtime = _runtime(tmp_path)
    _write_bootstrap(runtime)
    runtime._create_credential_seed(UUID("558afea6-2a37-55a1-9f7c-3399695da966"))
    drifted = json.loads(runtime.credential_seed_path.read_text())
    drifted["authority_incarnation"] = "00000000-0000-4000-8000-0000000000ff"
    runtime.credential_seed_path.write_text(
        json.dumps(drifted, sort_keys=True, separators=(",", ":")) + "\n"
    )
    runtime.credential_seed_path.chmod(0o600)
    before = runtime.credential_seed_path.read_bytes()
    plan = _plan(tmp_path)
    epoch = ComponentObservation(
        state=ComponentState.EXACT,
        evidence_digest="e" * 64,
        observed_epoch=plan.starting_mutation_epoch + 1,
    )
    component = runtime.components(plan, epoch_guard=lambda _plan: epoch)[0]

    assert component.classify(plan).state is ComponentState.DRIFTED
    with pytest.raises(RuntimeError, match="state changed"):
        component.apply(plan)
    assert runtime.credential_seed_path.read_bytes() == before


def test_runtime_issues_bootstrap_authority_for_replayable_credentials_and_frozen_shadow(
    tmp_path: Path,
) -> None:
    """Catch partial credential convergence deadlocking a frozen bootstrap replay."""
    runtime = _runtime(tmp_path)
    _write_bootstrap(runtime)
    lease = _lease()

    digest = runtime.zero_ceiling_bootstrap_authority(lease)

    assert len(digest) == 64
    assert digest != "0" * 64
    assert runtime.zero_ceiling_bootstrap_authority(lease) == digest
    with pytest.raises(RuntimeError, match="unavailable"):
        runtime.zero_ceiling_bootstrap_authority(object())  # type: ignore[arg-type]
    runtime._create_credential_seed(UUID("558afea6-2a37-55a1-9f7c-3399695da966"))
    replay_digest = runtime.zero_ceiling_bootstrap_authority(lease)

    assert replay_digest != digest
    assert replay_digest != "0" * 64
    assert runtime.zero_ceiling_bootstrap_authority(lease) == replay_digest
    runtime.credential_seed_path.write_text("{}")
    with pytest.raises(RuntimeError, match="unavailable"):
        runtime.zero_ceiling_bootstrap_authority(lease)


def test_credentials_component_rejects_bootstrap_mode_drift(tmp_path: Path) -> None:
    runtime = _runtime(tmp_path)
    _write_bootstrap(runtime)
    (runtime.credentials_root / "configuration-read" / "bearer-token").chmod(0o640)
    plan = _plan(tmp_path)
    epoch = ComponentObservation(
        state=ComponentState.EXACT,
        evidence_digest="e" * 64,
        observed_epoch=plan.starting_mutation_epoch + 1,
    )

    component = runtime.components(plan, epoch_guard=lambda _plan: epoch)[0]

    assert component.classify(plan).state is ComponentState.DRIFTED


def test_credentials_component_rejects_certificate_key_mismatch(tmp_path: Path) -> None:
    runtime = _runtime(tmp_path)
    private_keys = _write_bootstrap(runtime)
    mismatched = private_keys["configuration-fleet"].private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )
    path = runtime.credentials_root / "configuration-read" / "private-key.pem"
    path.write_bytes(mismatched)
    path.chmod(0o600)
    plan = _plan(tmp_path)
    epoch = ComponentObservation(
        state=ComponentState.EXACT,
        evidence_digest="e" * 64,
        observed_epoch=plan.starting_mutation_epoch + 1,
    )

    component = runtime.components(plan, epoch_guard=lambda _plan: epoch)[0]

    assert component.classify(plan).state is ComponentState.DRIFTED


def test_credentials_component_rejects_distinct_ca_certificates_with_one_key(
    tmp_path: Path,
) -> None:
    runtime = _runtime(tmp_path)
    private_keys = _write_bootstrap(runtime)
    client_ca_key = private_keys["client-ca"]
    manager_ca_name = x509.Name(
        [x509.NameAttribute(NameOID.COMMON_NAME, "manager-server-ca-with-client-key")]
    )
    manager_ca = _certificate(
        common_name="manager-server-ca-with-client-key",
        issuer_key=client_ca_key,
        issuer_name=manager_ca_name,
        subject_key=client_ca_key,
        is_ca=True,
    )
    manager_ca_payload = manager_ca.public_bytes(serialization.Encoding.PEM)
    for directory_name in (
        "configuration-read",
        "configuration-fleet",
        "configuration-subject",
        "configuration-activate",
        "staging-reporter",
    ):
        path = runtime.credentials_root / directory_name / "manager-ca.pem"
        path.write_bytes(manager_ca_payload)
        path.chmod(0o600)
    plan = _plan(tmp_path)
    epoch = ComponentObservation(
        state=ComponentState.EXACT,
        evidence_digest="e" * 64,
        observed_epoch=plan.starting_mutation_epoch + 1,
    )

    component = runtime.components(plan, epoch_guard=lambda _plan: epoch)[0]

    assert component.classify(plan).state is ComponentState.DRIFTED


def test_credentials_component_rejects_certificate_from_another_ca(tmp_path: Path) -> None:
    runtime = _runtime(tmp_path)
    private_keys = _write_bootstrap(runtime)
    foreign_ca_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    foreign_ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "foreign-ca")])
    foreign_certificate = _certificate(
        common_name="configuration-read",
        issuer_key=foreign_ca_key,
        issuer_name=foreign_ca_name,
        subject_key=private_keys["configuration-read"],
    )
    path = runtime.credentials_root / "configuration-read" / "certificate.pem"
    path.write_bytes(foreign_certificate.public_bytes(serialization.Encoding.PEM))
    path.chmod(0o600)
    plan = _plan(tmp_path)
    epoch = ComponentObservation(
        state=ComponentState.EXACT,
        evidence_digest="e" * 64,
        observed_epoch=plan.starting_mutation_epoch + 1,
    )

    component = runtime.components(plan, epoch_guard=lambda _plan: epoch)[0]

    assert component.classify(plan).state is ComponentState.DRIFTED


def test_credentials_component_rejects_reused_reporter_key(tmp_path: Path) -> None:
    runtime = _runtime(tmp_path)
    _write_bootstrap(runtime)
    source = runtime.credentials_root / "configuration-read" / "private-key.pem"
    target = runtime.credentials_root / "staging-reporter" / "private-key.pem"
    target.write_bytes(source.read_bytes())
    target.chmod(0o600)
    plan = _plan(tmp_path)
    epoch = ComponentObservation(
        state=ComponentState.EXACT,
        evidence_digest="e" * 64,
        observed_epoch=plan.starting_mutation_epoch + 1,
    )

    component = runtime.components(plan, epoch_guard=lambda _plan: epoch)[0]

    assert component.classify(plan).state is ComponentState.DRIFTED


def test_manager_runtime_component_is_reachable_through_protected_chain(
    tmp_path: Path,
) -> None:
    seed_runtime = _runtime(tmp_path)
    _write_bootstrap(seed_runtime)
    plan = replace(
        _plan(tmp_path),
        image_digests={
            **_plan(tmp_path).image_digests,
            "loom-capacity-manager": "sha256:" + "9" * 64,
        },
    )
    epoch = ComponentObservation(
        state=ComponentState.EXACT,
        evidence_digest="e" * 64,
        observed_epoch=plan.starting_mutation_epoch + 1,
    )
    seed_runtime.components(plan, epoch_guard=lambda _plan: epoch)[0].apply(plan)
    candidate = _candidate(tmp_path)
    cluster = _ManagerCluster(candidate)
    runtime = KubernetesProtectedStagingCapacityRuntime(
        runner=cluster,  # type: ignore[arg-type]
        state_root=seed_runtime.state_root,
        candidate_root=candidate,
        service_uid=os.geteuid(),
        service_gid=os.getegid(),
        container_registry="registry.example.test/loom",
    )
    component = runtime.components(plan, epoch_guard=lambda _plan: epoch)[3]

    assert component.classify(plan).state is ComponentState.READY
    component.apply(plan)

    assert component.classify(plan).state is ComponentState.EXACT
    seed = json.loads(seed_runtime.credential_seed_path.read_text())
    registry = json.loads(base64.b64decode(cluster.secret_data["principals.json"], validate=True))
    principal = next(
        item for item in registry["principals"] if item["principal_id"] == "staging-demand-reporter"
    )
    assert principal["demand_reporter_incarnation"] == str(
        derive_staging_reporter_incarnation(
            seed["reporter_incarnation"],
            target_generation=plan.starting_mutation_epoch + 1,
        )
    )


def test_manager_configuration_component_is_reachable_after_manager_runtime(
    tmp_path: Path,
) -> None:
    from tests.loom_cli.rollout.operator.test_protected_staging_capacity_manager_configuration_component import (
        _active_document,
        _Client,
        _live_fleet,
    )

    seed_runtime = _runtime(tmp_path)
    _write_bootstrap(seed_runtime)
    plan = _plan(tmp_path)
    epoch = ComponentObservation(
        state=ComponentState.EXACT,
        evidence_digest="e" * 64,
        observed_epoch=plan.starting_mutation_epoch + 1,
    )
    seed_runtime.components(plan, epoch_guard=lambda _plan: epoch)[0].apply(plan)
    seed = json.loads(seed_runtime.credential_seed_path.read_text())
    fleet_payload = _live_fleet().model_dump(mode="python")
    fleet_payload["authority_incarnation"] = UUID(str(seed["authority_incarnation"]))
    fleet_payload["fleet_digest"] = "0" * 64
    provisional = FleetManifestV1.model_validate(fleet_payload)
    fleet_payload["fleet_digest"] = canonical_digest_excluding(provisional, "fleet_digest")
    fleet = FleetManifestV1.model_validate(fleet_payload)
    client = _Client(_active_document(fleet, ()))

    @contextmanager
    def client_context(**_kwargs):
        yield client

    class _ConfigurationRunner:
        environment: ClassVar[dict[str, str]] = {"KUBECONFIG": "/protected/kubeconfig"}

    runtime = KubernetesProtectedStagingCapacityRuntime(
        runner=_ConfigurationRunner(),  # type: ignore[arg-type]
        state_root=seed_runtime.state_root,
        candidate_root=seed_runtime.candidate_root,
        service_uid=os.geteuid(),
        service_gid=os.getegid(),
        container_registry="registry.example.test/loom",
        manager_configuration_client_context=client_context,
    )
    component = runtime.components(plan, epoch_guard=lambda _plan: epoch)[4]

    assert component.classify(plan).state is ComponentState.READY
    component.apply(plan)
    assert component.classify(plan).state is ComponentState.EXACT


def _database_component(
    tmp_path: Path,
    *,
    database_state: str,
    fail_checked: str | None = None,
):
    seed_runtime = _runtime(tmp_path)
    _write_bootstrap(seed_runtime)
    plan = _plan(tmp_path)
    epoch = ComponentObservation(
        state=ComponentState.EXACT,
        evidence_digest="e" * 64,
        observed_epoch=plan.starting_mutation_epoch + 1,
    )
    seed_runtime.components(plan, epoch_guard=lambda _plan: epoch)[0].apply(plan)
    seed = json.loads(seed_runtime.credential_seed_path.read_text())
    runner = _DatabaseRunner(
        plan,
        seed,
        database_state=database_state,
        fail_checked=fail_checked,
    )
    runtime = KubernetesProtectedStagingCapacityRuntime(
        runner=runner,  # type: ignore[arg-type]
        state_root=seed_runtime.state_root,
        candidate_root=seed_runtime.candidate_root,
        service_uid=os.geteuid(),
        service_gid=os.getegid(),
        container_registry="registry.example.test/loom",
    )
    return plan, runner, runtime.components(plan, epoch_guard=lambda _plan: epoch)[1]


def test_database_component_rejects_seed_authority_foreign_to_plan(tmp_path: Path) -> None:
    """Break caught: a downstream component trusting a seed changed after credential apply."""
    seed_runtime = _runtime(tmp_path)
    _write_bootstrap(seed_runtime)
    plan = _plan(tmp_path)
    epoch = ComponentObservation(
        state=ComponentState.EXACT,
        evidence_digest="e" * 64,
        observed_epoch=plan.starting_mutation_epoch + 1,
    )
    seed_runtime.components(plan, epoch_guard=lambda _plan: epoch)[0].apply(plan)
    drifted = json.loads(seed_runtime.credential_seed_path.read_text())
    drifted["authority_incarnation"] = "00000000-0000-4000-8000-0000000000ff"
    seed_runtime.credential_seed_path.write_text(
        json.dumps(drifted, sort_keys=True, separators=(",", ":")) + "\n"
    )
    seed_runtime.credential_seed_path.chmod(0o600)
    runner = _DatabaseRunner(plan, drifted, database_state="exact")
    runtime = KubernetesProtectedStagingCapacityRuntime(
        runner=runner,  # type: ignore[arg-type]
        state_root=seed_runtime.state_root,
        candidate_root=seed_runtime.candidate_root,
        service_uid=os.geteuid(),
        service_gid=os.getegid(),
        container_registry="registry.example.test/loom",
    )

    component = runtime.components(plan, epoch_guard=lambda _plan: epoch)[1]

    assert component.classify(plan).state is ComponentState.DRIFTED


def _prior_database_plan(plan: FinalGatePlan, **updates: object) -> FinalGatePlan:
    payload = plan.to_dict()
    payload.update(
        {
            "request_id": "req-prior01",
            "rollout_id": "20260905t181433z-staging-prior01",
            "starting_mutation_epoch": plan.starting_mutation_epoch - 1,
        }
    )
    payload.update(updates)
    authority = DatabaseAuthorityEvidence(
        public_schema_revision=str(payload["public_schema_revision"]),
        capacity_guard_schema_revision=payload["capacity_guard_schema_revision"],  # type: ignore[arg-type]
        configuration_epoch=payload["manager_configuration_epoch"],  # type: ignore[arg-type]
        configuration_digest=str(payload["manager_configuration_digest"]),
        authority_incarnation=UUID(str(payload["manager_authority_incarnation"])),
        writer_epoch=payload["manager_writer_epoch"],  # type: ignore[arg-type]
        execution_state=payload["manager_execution_state"],  # type: ignore[arg-type]
        execution_epoch=payload["manager_execution_epoch"],  # type: ignore[arg-type]
        execution_manifest_sha256=payload["manager_execution_manifest_sha256"],  # type: ignore[arg-type]
        executable_new_capacity_ceiling=payload[  # type: ignore[arg-type]
            "manager_executable_new_capacity_ceiling"
        ],
        increase_freeze=payload["manager_increase_freeze"],  # type: ignore[arg-type]
    )
    payload["database_authority_digest"] = authority.digest
    checkpoint_components = dict(payload["checkpoint_component_sha256"])  # type: ignore[arg-type]
    checkpoint_components["database_authority"] = authority.digest
    payload["checkpoint_component_sha256"] = checkpoint_components
    payload_without_digest = {key: value for key, value in payload.items() if key != "plan_digest"}
    payload["plan_digest"] = hashlib.sha256(
        json.dumps(payload_without_digest, sort_keys=True, separators=(",", ":")).encode("ascii")
    ).hexdigest()
    return FinalGatePlan.from_dict(payload)


def _legacy_database_bootstrap_objects(
    component: KubernetesProtectedStagingCapacityDatabaseComponent,
    runner: _DatabaseRunner,
    plan: FinalGatePlan,
) -> dict[str, dict[str, object]]:
    documents = {
        document["kind"]: document
        for document in yaml.safe_load_all(component._manifest(plan, runner.seed))
        if document is not None
    }
    secret = documents["Secret"]
    secret_data = secret["data"]
    assert isinstance(secret_data, dict)
    secret_data.pop("admin-password")
    secret_data.pop("admin-username")
    job = documents["Job"]
    pod_spec = job["spec"]["template"]["spec"]
    postgres_admin = next(
        volume for volume in pod_spec["volumes"] if volume["name"] == "postgres-admin"
    )
    postgres_admin["secret"] = {
        "defaultMode": 0o440,
        "items": [
            {"key": "password", "path": "password"},
            {"key": "username", "path": "username"},
        ],
        "secretName": "loom-postgres-cnpg-credentials",
    }
    objects = {kind: runner._stored(document) for kind, document in documents.items()}
    objects["Job"]["status"] = {
        "failed": 1,
        "conditions": [
            {"reason": "BackoffLimitExceeded", "status": "True", "type": "FailureTarget"},
            {"reason": "BackoffLimitExceeded", "status": "True", "type": "Failed"},
        ],
    }
    return objects


def _write_database_plan_ledger_entry(
    tmp_path: Path,
    plan: FinalGatePlan,
    *,
    request_id: str | None = None,
    attempt_number: int | None = None,
) -> None:
    ledger_request_id = plan.request_id if request_id is None else request_id
    ledger_attempt_number = plan.attempt_number if attempt_number is None else attempt_number
    request_root = tmp_path / "state" / "requests" / ledger_request_id
    attempt_root = request_root / "attempts" / str(ledger_attempt_number)
    for directory in (request_root.parent, request_root, attempt_root.parent, attempt_root):
        directory.mkdir(mode=0o700, exist_ok=True)
    path = attempt_root / "final-gate-plan.json"
    path.write_text(
        json.dumps(plan.to_dict(), sort_keys=True, separators=(",", ":")),
        encoding="utf-8",
    )
    path.chmod(0o600)


def test_database_component_bootstraps_with_candidate_image_then_removes_credentials(
    tmp_path: Path,
) -> None:
    plan, runner, component = _database_component(tmp_path, database_state="absent")

    assert component.classify(plan).state is ComponentState.READY

    component.apply(plan)

    assert component.classify(plan).state is ComponentState.EXACT
    assert runner.objects == {}
    peer_commands = [call for call, _payload in runner.checked_inputs]
    assert all(
        call
        == (
            "kubectl",
            "--namespace",
            "loom-staging",
            "exec",
            "-i",
            "service/loom-postgres-rw",
            "--",
            "sh",
            "-ceu",
            "exec psql -U postgres -d loom -qAtX -v ON_ERROR_STOP=1",
        )
        for call in peer_commands
    )
    password = str(runner.seed["migrator_database_password"])
    assert all(password not in " ".join(call) for call in runner.calls)
    arm_payload = next(
        payload
        for _command, payload in runner.checked_inputs
        if b"GRANT loom TO loom_cap_staging_migrator" in payload
    )
    assert password.encode() in arm_payload
    assert b" NOCREATEROLE " in arm_payload
    assert b" CREATEROLE " not in arm_payload
    assert b"WITH ADMIN TRUE" not in arm_payload
    assert b"SET FALSE" not in arm_payload
    assert str(runner.seed["agent_database_password"]).encode() in arm_payload
    assert str(runner.seed["observer_database_password"]).encode() in arm_payload
    assert str(runner.seed["runtime_database_password"]).encode() in arm_payload
    assert all(
        password.encode() not in payload
        for _command, payload in runner.checked_inputs
        if payload is not arm_payload
    )
    assert runner.events.index("cleanup") < runner.events.index("arm")
    mutations = [
        call
        for call in runner.calls
        if "create" in call
        or "delete" in call
        or ("wait" in call and "--for=condition=complete" in call)
    ]
    assert [
        next(item for item in ("create", "wait", "delete") if item in call) for call in mutations
    ] == [
        "create",
        "wait",
        "delete",
        "delete",
    ]
    secret = runner.created_objects["Secret"]
    job = runner.created_objects["Job"]
    assert secret["immutable"] is True
    assert set(secret["data"]) == {
        "admin-password",
        "admin-username",
        "reporter-configuration.json",
        "seed.json",
    }
    assert base64.b64decode(secret["data"]["admin-username"], validate=True) == (
        b"loom_cap_staging_migrator"
    )
    assert base64.b64decode(secret["data"]["admin-password"], validate=True) == (password.encode())
    configuration = json.loads(
        base64.b64decode(secret["data"]["reporter-configuration.json"], validate=True)
    )
    assert configuration["authority_mode"] == "disabled"
    assert configuration["allocation_epoch"] == 0
    assert configuration["candidate_digest"] == plan.artifact_bundle_digest
    assert configuration["candidate_identity_algorithm"] == "git-sha1"
    assert configuration["candidate_identity"] == plan.candidate_sha
    assert configuration["candidate_publication_sha256"] == plan.artifact_bundle_digest
    assert configuration["deployment_generation"] == plan.starting_mutation_epoch + 1
    assert configuration["configuration_generation"] == plan.starting_mutation_epoch + 1
    assert job["spec"]["template"]["metadata"]["labels"]["app"] == "loom-migration"
    pod_spec = job["spec"]["template"]["spec"]
    assert pod_spec["automountServiceAccountToken"] is False
    assert pod_spec["securityContext"] == {
        "fsGroup": 65532,
        "fsGroupChangePolicy": "OnRootMismatch",
        "runAsGroup": 65532,
        "runAsNonRoot": True,
        "runAsUser": 65532,
        "seccompProfile": {"type": "RuntimeDefault"},
    }
    container = pod_spec["containers"][0]
    assert container["image"] == (
        "registry.example.test/loom/loom-control-plane@" + plan.image_digests["loom-control-plane"]
    )
    assert container["command"] == [
        "python",
        "-I",
        "-B",
        "-m",
        "loom.staging_capacity_database_bootstrap",
    ]
    assert container["securityContext"] == {
        "allowPrivilegeEscalation": False,
        "capabilities": {"drop": ["ALL"]},
        "readOnlyRootFilesystem": True,
    }
    volumes = {volume["name"]: volume["secret"] for volume in pod_spec["volumes"]}
    assert {volume["secretName"] for volume in volumes.values()} == {
        "loom-staging-capacity-database-bootstrap",
        "loom-postgres-ca",
    }
    assert volumes["bootstrap"]["items"] == [
        {"key": "reporter-configuration.json", "path": "reporter-configuration.json"},
        {"key": "seed.json", "path": "seed.json"},
    ]
    assert volumes["postgres-admin"]["items"] == [
        {"key": "admin-password", "path": "password"},
        {"key": "admin-username", "path": "username"},
    ]
    assert volumes["postgres-admin"]["secretName"] == ("loom-staging-capacity-database-bootstrap")
    assert volumes["postgres-ca"]["items"] == [{"key": "ca.crt", "path": "ca.crt"}]


@pytest.mark.asyncio
async def test_database_manifest_runs_real_bootstrap_with_generation_reporter(
    tmp_path: Path,
) -> None:
    """Break caught: serializing the raw seed beside a generation-derived configuration."""

    plan, runner, _component = _database_component(tmp_path, database_state="absent")
    component = KubernetesProtectedStagingCapacityDatabaseComponent(
        runner=runner,  # type: ignore[arg-type]
        container_registry="registry.example.test/loom",
        seed_reader=lambda: runner.seed,
    )
    raw_reporter_incarnation = runner.seed["reporter_incarnation"]
    documents = {
        document["kind"]: document
        for document in yaml.safe_load_all(component._manifest(plan, runner.seed))
        if document is not None
    }
    secret_data = documents["Secret"]["data"]
    bootstrap_root = tmp_path / "bootstrap-inputs"
    bootstrap_root.mkdir()
    for secret_name, file_name in (
        ("seed.json", "seed.json"),
        ("reporter-configuration.json", "reporter-configuration.json"),
        ("admin-username", "username"),
        ("admin-password", "password"),
    ):
        (bootstrap_root / file_name).write_bytes(
            base64.b64decode(secret_data[secret_name], validate=True)
        )
    (bootstrap_root / "ca.crt").write_bytes(b"test-ca")
    observed: dict[str, object] = {}

    class Database:
        def __init__(self, admin_url: str, *, transient_role_admin: bool) -> None:
            observed["admin_url"] = admin_url
            observed["transient_role_admin"] = transient_role_admin

        async def converge_protected(
            self,
            *,
            identity: object,
            credentials: CapacityDatabaseCredentials,
            configuration: ReporterConfigurationV1,
        ) -> CapacityDatabaseInstallation:
            observed["credentials"] = credentials
            observed["configuration"] = configuration
            return CapacityDatabaseInstallation(
                protected_admission_sha256="4" * 64,
                agent_database_url="redacted-agent-url",
                runtime_database_url="redacted-runtime-url",
            )

    await bootstrap_staging_capacity_database(
        StagingCapacityDatabaseBootstrapSettings(
            credential_seed_path=bootstrap_root / "seed.json",
            reporter_configuration_path=bootstrap_root / "reporter-configuration.json",
            admin_username_path=bootstrap_root / "username",
            admin_password_path=bootstrap_root / "password",
            database_ca_path=bootstrap_root / "ca.crt",
        ),
        database_factory=Database,
    )

    credentials = observed["credentials"]
    configuration = observed["configuration"]
    assert isinstance(credentials, CapacityDatabaseCredentials)
    assert isinstance(configuration, ReporterConfigurationV1)
    assert credentials.reporter_incarnation == configuration.reporter_incarnation
    assert str(configuration.reporter_incarnation) != raw_reporter_incarnation
    assert runner.seed["reporter_incarnation"] == raw_reporter_incarnation


def test_database_component_accepts_api_defaulted_job_and_cleans_it_up(tmp_path: Path) -> None:
    """Break caught: comparing a live defaulted Job directly with its raw manifest."""

    plan, runner, component = _database_component(tmp_path, database_state="absent")
    runner.api_default_jobs = True

    component.apply(plan)

    assert runner.objects == {}
    assert component.classify(plan).state is ComponentState.EXACT


def test_database_component_rejects_unlabelled_resource_at_reserved_name(tmp_path: Path) -> None:
    """Break caught: selector-only inventory hiding an occupied reserved name."""

    plan, runner, component = _database_component(tmp_path, database_state="absent")
    runner.objects["Secret"] = runner._stored(
        {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {
                "name": "loom-staging-capacity-database-bootstrap",
                "namespace": "loom-staging",
                "labels": {"app.kubernetes.io/managed-by": "foreign-controller"},
            },
            "type": "Opaque",
            "data": {"foreign": "dmFsdWU="},
        }
    )

    assert component.classify(plan).state is ComponentState.DRIFTED
    with pytest.raises(RuntimeError, match="state changed"):
        component.apply(plan)


@pytest.mark.parametrize(
    ("kind", "section", "key", "value"),
    [
        ("Secret", "data", "foreign", "dmFsdWU="),
        ("Secret", "annotations", "foreign.example/owner", "other-manager"),
        ("Secret", "labels", "foreign.example/managed", "true"),
        ("Job", "spec", "ttlSecondsAfterFinished", 3600),
    ],
)
def test_database_component_rejects_additive_unmanaged_resource_fields(
    tmp_path: Path,
    kind: str,
    section: str,
    key: str,
    value: object,
) -> None:
    """Break caught: SSA retaining additive foreign fields while reporting no diff."""

    plan, runner, component = _database_component(tmp_path, database_state="absent")
    direct = KubernetesProtectedStagingCapacityDatabaseComponent(
        runner=runner,  # type: ignore[arg-type]
        container_registry="registry.example.test/loom",
        seed_reader=lambda: runner.seed,
    )
    manifest = direct._manifest(plan, runner.seed)
    runner.objects = {
        document["kind"]: runner._stored(document)
        for document in yaml.safe_load_all(manifest)
        if document is not None
    }
    resource = runner.objects[kind]
    if section == "spec":
        target = resource["spec"]
    else:
        metadata = resource["metadata"]
        assert isinstance(metadata, dict)
        target = metadata[section] if section != "data" else resource["data"]
    assert isinstance(target, dict)
    target[key] = value

    assert component.classify(plan).state is ComponentState.DRIFTED
    with pytest.raises(RuntimeError, match="state changed"):
        component.apply(plan)
    assert set(runner.objects) == {"Job", "Secret"}


def test_database_component_distinguishes_finite_from_durable_runtime_credentials(
    tmp_path: Path,
) -> None:
    """Break caught: treating a half-finished finite lease as final exact state."""

    plan, runner, _component = _database_component(tmp_path, database_state="exact")
    direct = KubernetesProtectedStagingCapacityDatabaseComponent(
        runner=runner,  # type: ignore[arg-type]
        container_registry="registry.example.test/loom",
        seed_reader=lambda: runner.seed,
    )
    runner.runtime_credentials_durable = False

    assert direct._database_state(plan, runner.seed).value == "needs-convergence"
    assert (
        direct._database_state(
            plan,
            runner.seed,
            durable_runtime_credentials=False,
        ).value
        == "exact"
    )

    runner.runtime_credentials_durable = True
    assert direct._database_state(plan, runner.seed).value == "exact"


def test_database_component_repairs_exact_unused_legacy_authority(
    tmp_path: Path,
) -> None:
    """Break caught: leaving the disabled guard database bound to the retired seed UUID."""
    plan, runner, component = _database_component(tmp_path, database_state="exact")
    runner.registration_overrides = {
        "authority_incarnation": "558afea6-2a37-55a1-9f7c-3399695da966",
    }

    assert component.classify(plan).state is ComponentState.READY
    component.apply(plan)

    assert component.classify(plan).state is ComponentState.EXACT
    assert runner.registration_overrides == {}
    assert runner.events.index("authority-rebind") < runner.events.index(
        "authority-runtime-restore"
    )
    assert "arm" not in runner.events
    assert "create" not in runner.events


def test_journal_recovers_legacy_database_terminal_by_authority_forward(
    tmp_path: Path,
) -> None:
    """Break caught: a historical database terminal prevents certified authority rebinding."""
    plan, runner, component = _database_component(tmp_path, database_state="exact")
    runner.registration_overrides = {
        "authority_incarnation": "558afea6-2a37-55a1-9f7c-3399695da966",
    }
    legacy_observation = component.classify(plan)
    assert legacy_observation.state is ComponentState.READY
    historical_component = replace(
        component,
        classify=lambda _plan: ComponentObservation(
            state=ComponentState.EXACT,
            evidence_digest=legacy_observation.evidence_digest,
            observed_epoch=legacy_observation.observed_epoch,
        ),
        apply=lambda _plan: pytest.fail("historical exact database must not mutate"),
    )
    attempt_root = tmp_path / f"state/requests/{plan.request_id}/attempts/{plan.attempt_number}"
    attempt_root.mkdir(parents=True, mode=0o700)
    journal = ProtectedApplyJournal(
        tmp_path / "state",
        request_id=plan.request_id,
        attempt_number=plan.attempt_number,
        service_uid=os.geteuid(),
    )
    journal.execute(plan, (historical_component,))
    component_root = journal.root / f"00-{component.component_id}"
    original_terminal = (component_root / "terminal.json").read_bytes()

    recovered = journal.execute(plan, (component,))[component.component_id]
    rebound_events = runner.events.count("authority-rebind")
    replayed = journal.execute(plan, (component,))[component.component_id]

    assert component.classify(plan).state is ComponentState.EXACT
    assert runner.registration_overrides == {}
    assert rebound_events == 1
    assert runner.events.count("authority-rebind") == rebound_events
    assert (component_root / "terminal.json").read_bytes() == original_terminal
    assert recovered == replayed


def test_journal_does_not_recover_an_ordinary_ready_database_terminal(
    tmp_path: Path,
) -> None:
    """Break caught: terminal recovery turns an ordinary database bootstrap into a replay repair."""
    plan, runner, component = _database_component(tmp_path, database_state="absent")
    ready_observation = component.classify(plan)
    assert ready_observation.state is ComponentState.READY
    historical_component = replace(
        component,
        classify=lambda _plan: ComponentObservation(
            state=ComponentState.EXACT,
            evidence_digest=ready_observation.evidence_digest,
            observed_epoch=ready_observation.observed_epoch,
        ),
        apply=lambda _plan: pytest.fail("historical exact database must not mutate"),
    )
    attempt_root = tmp_path / f"state/requests/{plan.request_id}/attempts/{plan.attempt_number}"
    attempt_root.mkdir(parents=True, mode=0o700)
    journal = ProtectedApplyJournal(
        tmp_path / "state",
        request_id=plan.request_id,
        attempt_number=plan.attempt_number,
        service_uid=os.geteuid(),
    )
    journal.execute(plan, (historical_component,))

    with pytest.raises(ProtectedApplyJournalError, match="terminal state drifted"):
        journal.execute(plan, (component,))

    component_root = journal.root / f"00-{component.component_id}"
    assert not (component_root / "terminal-recovery-intent.json").exists()
    assert "create" not in runner.events


def test_database_component_rejects_legacy_authority_after_any_guard_work(
    tmp_path: Path,
) -> None:
    """Break caught: rebinding an authority after the protected guard recorded workload state."""
    plan, runner, component = _database_component(tmp_path, database_state="exact")
    runner.registration_overrides = {
        "authority_incarnation": "558afea6-2a37-55a1-9f7c-3399695da966",
    }
    runner.authority_rebind_safe = False

    assert component.classify(plan).state is ComponentState.DRIFTED
    with pytest.raises(RuntimeError, match="state changed"):
        component.apply(plan)
    assert "authority-rebind" not in runner.events


def test_database_component_rejects_legacy_authority_with_corrupt_audit_history(
    tmp_path: Path,
) -> None:
    """Break caught: preserving legacy audit payloads whose canonical digest is false."""
    plan, runner, component = _database_component(tmp_path, database_state="exact")
    runner.registration_overrides = {
        "authority_incarnation": "558afea6-2a37-55a1-9f7c-3399695da966",
    }
    runner.authority_rebind_audit_integrity = False

    assert component.classify(plan).state is ComponentState.DRIFTED
    with pytest.raises(RuntimeError, match="state changed"):
        component.apply(plan)
    assert "authority-rebind" not in runner.events


def test_database_component_rejects_legacy_authority_with_trigger_drift(
    tmp_path: Path,
) -> None:
    """Break caught: silently adopting a disabled immutable-binding trigger."""
    plan, runner, component = _database_component(tmp_path, database_state="exact")
    runner.registration_overrides = {
        "authority_incarnation": "558afea6-2a37-55a1-9f7c-3399695da966",
    }
    runner.authority_rebind_trigger_integrity = False

    assert component.classify(plan).state is ComponentState.DRIFTED
    with pytest.raises(RuntimeError, match="state changed"):
        component.apply(plan)
    assert "authority-rebind" not in runner.events


def test_database_component_recovers_committed_authority_rebind_after_response_loss(
    tmp_path: Path,
) -> None:
    """Break caught: retrying the obsolete candidate bootstrap after a committed UUID repair."""
    plan, runner, component = _database_component(tmp_path, database_state="exact")
    runner.registration_overrides = {
        "authority_incarnation": "558afea6-2a37-55a1-9f7c-3399695da966",
    }
    runner.fail_peer_phase_after_mutation_counts["authority-rebind"] = 1

    with pytest.raises(RuntimeError, match="response loss"):
        component.apply(plan)

    assert component.classify(plan).state is ComponentState.READY
    component.apply(plan)
    assert component.classify(plan).state is ComponentState.EXACT
    assert runner.events.count("authority-rebind") == 1
    assert runner.events.count("authority-runtime-restore") == 1
    assert "arm" not in runner.events
    assert "create" not in runner.events


def test_database_component_rejects_target_authority_with_only_legacy_audits(
    tmp_path: Path,
) -> None:
    """Break caught: a partial UUID update falling through to the obsolete bootstrap."""
    plan, runner, component = _database_component(tmp_path, database_state="exact")
    runner.protected_roles_sealed = True
    runner.authority_rebind_incomplete = True

    assert component.classify(plan).state is ComponentState.DRIFTED
    with pytest.raises(RuntimeError, match="state changed"):
        component.apply(plan)
    assert "arm" not in runner.events
    assert "create" not in runner.events


def test_database_component_preserves_long_target_authority_sealed_retry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Break caught: treating normal target-authority reconfiguration history as a repair."""
    plan, runner, component = _database_component(tmp_path, database_state="exact")
    runner.protected_roles_sealed = True
    current_registration = AgentRegistrationV1.model_validate_json(
        json.dumps(runner._registration(), sort_keys=True)
    )
    middle_registration = AgentRegistrationV1.model_validate(
        {
            **current_registration.model_dump(mode="python", exclude_none=False),
            "candidate_digest": "e" * 64,
            "candidate_identity": "e" * 40,
            "candidate_publication_sha256": "e" * 64,
            "configuration_generation": current_registration.configuration_generation - 1,
            "deployment_generation": current_registration.deployment_generation - 1,
            "reporter_incarnation": UUID("00000000-0000-4000-8000-000000000201"),
        }
    )
    initial_registration = AgentRegistrationV1.model_validate(
        {
            **middle_registration.model_dump(mode="python", exclude_none=False),
            "candidate_digest": "d" * 64,
            "candidate_identity": "d" * 40,
            "candidate_publication_sha256": "d" * 64,
            "configuration_generation": middle_registration.configuration_generation - 1,
            "deployment_generation": middle_registration.deployment_generation - 1,
            "reporter_incarnation": UUID("00000000-0000-4000-8000-000000000202"),
        }
    )
    histories = (initial_registration, middle_registration, current_registration)
    audit_rows: list[dict[str, object]] = []
    for index, registration in enumerate(histories):
        event_base = index * 2 + 1
        audit_rows.extend(
            (
                runner._audit_row(
                    event_base,
                    "authority_initialized.v1" if index == 0 else "authority_reconfigured.v1",
                    KubernetesProtectedStagingCapacityDatabaseComponent._fence_for_registration(
                        registration
                    ),
                ),
                runner._audit_row(
                    event_base + 1,
                    "agent_registered.v1" if index == 0 else "agent_reconfigured.v1",
                    registration,
                ),
            )
        )
    monkeypatch.setattr(runner, "_audit_history", lambda: audit_rows)

    assert component.classify(plan).state is ComponentState.READY


def test_database_component_rejects_corrupt_target_authority_audits(
    tmp_path: Path,
) -> None:
    """Break caught: malformed target-authority history opening ordinary bootstrap recovery."""
    plan, runner, component = _database_component(tmp_path, database_state="exact")
    runner.protected_roles_sealed = True
    runner.authority_rebind_audit_integrity = False

    assert component.classify(plan).state is ComponentState.DRIFTED
    with pytest.raises(RuntimeError, match="state changed"):
        component.apply(plan)
    assert "arm" not in runner.events
    assert "create" not in runner.events


def test_database_component_rejects_unreadable_target_authority_audits(
    tmp_path: Path,
) -> None:
    """Break caught: audit observation failure opening ordinary bootstrap recovery."""
    plan, runner, component = _database_component(tmp_path, database_state="exact")
    runner.protected_roles_sealed = True
    runner.authority_rebind_audit_read_failure = True

    assert component.classify(plan).state is ComponentState.DRIFTED
    with pytest.raises(RuntimeError, match="state changed"):
        component.apply(plan)
    assert "arm" not in runner.events
    assert "create" not in runner.events


@pytest.mark.parametrize("corruption", ["audit", "extra-event"])
def test_database_component_rejects_uncertified_committed_authority_rebind(
    tmp_path: Path,
    corruption: str,
) -> None:
    """Break caught: restoring credentials over an unrecognized target-authority audit tail."""
    plan, runner, component = _database_component(tmp_path, database_state="exact")
    runner.registration_overrides = {
        "authority_incarnation": "558afea6-2a37-55a1-9f7c-3399695da966",
    }
    runner.fail_peer_phase_after_mutation_counts["authority-rebind"] = 1

    with pytest.raises(RuntimeError, match="response loss"):
        component.apply(plan)

    if corruption == "audit":
        runner.authority_rebind_audit_integrity = False
    else:
        runner.authority_rebind_extra_event = True
    assert component.classify(plan).state is ComponentState.DRIFTED
    with pytest.raises(RuntimeError, match="state changed"):
        component.apply(plan)
    assert runner.events.count("authority-rebind") == 1
    assert "create" not in runner.events


def test_database_component_retries_authority_rebind_after_precommit_failure(
    tmp_path: Path,
) -> None:
    """Break caught: sealing runtime roles before a failed repair made retry unclassifiable."""
    plan, runner, component = _database_component(tmp_path, database_state="exact")
    runner.registration_overrides = {
        "authority_incarnation": "558afea6-2a37-55a1-9f7c-3399695da966",
    }
    runner.fail_peer_phase_counts["authority-rebind"] = 1

    with pytest.raises(RuntimeError, match="compensation phase failure"):
        component.apply(plan)

    assert component.classify(plan).state is ComponentState.READY
    component.apply(plan)
    assert component.classify(plan).state is ComponentState.EXACT
    assert runner.events.count("authority-rebind") == 2
    assert runner.events.count("authority-runtime-restore") == 1
    assert "arm" not in runner.events
    assert "create" not in runner.events


def test_database_component_revalidates_committed_state_while_restoring_credentials(
    tmp_path: Path,
) -> None:
    """Break caught: guard activity racing between UUID repair and credential restoration."""
    plan, runner, component = _database_component(tmp_path, database_state="exact")
    runner.registration_overrides = {
        "authority_incarnation": "558afea6-2a37-55a1-9f7c-3399695da966",
    }
    runner.activity_before_authority_restore = True

    with pytest.raises(RuntimeError, match="restore revalidation failure"):
        component.apply(plan)

    assert runner.protected_roles_sealed is True
    assert "create" not in runner.events


def test_database_component_reseals_credentials_after_post_restore_failure(
    tmp_path: Path,
) -> None:
    """Break caught: leaving runtime logins active after exact readback failed."""
    plan, runner, component = _database_component(tmp_path, database_state="exact")
    runner.registration_overrides = {
        "authority_incarnation": "558afea6-2a37-55a1-9f7c-3399695da966",
    }
    runner.fail_verification_after_authority_restore = True

    with pytest.raises(RuntimeError, match="post-restore verification failure"):
        component.apply(plan)

    assert runner.protected_roles_sealed is True
    assert "create" not in runner.events


def test_database_component_rebind_changes_only_authority_columns_and_audit_tail(
    tmp_path: Path,
) -> None:
    """Break caught: overwriting the preserved authority update timestamp during repair."""
    plan, runner, component = _database_component(tmp_path, database_state="exact")
    runner.registration_overrides = {
        "authority_incarnation": "558afea6-2a37-55a1-9f7c-3399695da966",
    }

    component.apply(plan)

    rebind_payloads = [
        payload
        for _command, payload in runner.checked_inputs
        if runner._peer_phase(payload) == "authority-rebind"
    ]
    assert len(rebind_payloads) == 1
    assert b"updated_at = statement_timestamp()" not in rebind_payloads[0]


def test_database_component_retries_exact_sealed_compensation_state(
    tmp_path: Path,
) -> None:
    """Break caught: treating the verified compensation state as foreign drift."""

    plan, runner, component = _database_component(tmp_path, database_state="exact")
    runner.protected_roles_sealed = True

    assert component.classify(plan).state is ComponentState.READY
    component.apply(plan)

    assert component.classify(plan).state is ComponentState.EXACT


def test_database_component_retries_sealed_predecessor_candidate_state(
    tmp_path: Path,
) -> None:
    """Break caught: rejecting a safely sealed predecessor after a new rollout starts."""

    plan, runner, component = _database_component(tmp_path, database_state="exact")
    runner.registration_overrides = {
        "candidate_digest": "d" * 64,
        "candidate_identity": "f" * 40,
        "candidate_publication_sha256": "d" * 64,
        "configuration_generation": plan.starting_mutation_epoch,
        "deployment_generation": plan.starting_mutation_epoch,
    }
    runner.protected_roles_sealed = True

    assert component.classify(plan).state is ComponentState.READY


@pytest.mark.parametrize(
    "registration_updates",
    [
        {"reporter_incarnation": "00000000-0000-4000-8000-000000000004"},
        {
            "candidate_identity_algorithm": "source-sha256",
            "candidate_identity": "f" * 64,
        },
    ],
)
def test_database_component_retries_sealed_predecessor_reconfiguration_state(
    tmp_path: Path,
    registration_updates: dict[str, object],
) -> None:
    """Break caught: rejecting predecessor fields supported by guarded reconfiguration."""

    plan, runner, component = _database_component(tmp_path, database_state="exact")
    runner.registration_overrides = {
        "candidate_digest": "d" * 64,
        "candidate_identity": "f" * 40,
        "candidate_publication_sha256": "d" * 64,
        "configuration_generation": plan.starting_mutation_epoch,
        "deployment_generation": plan.starting_mutation_epoch,
        **registration_updates,
    }
    runner.protected_roles_sealed = True

    assert component.classify(plan).state is ComponentState.READY


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("schema_version", 2),
        ("environment_id", "other-environment"),
        ("subject_id", "00000000-0000-4000-8000-000000000000"),
        ("subject_incarnation", "00000000-0000-4000-8000-000000000001"),
        ("authority_incarnation", "00000000-0000-4000-8000-000000000002"),
        ("agent_incarnation", "00000000-0000-4000-8000-000000000003"),
        ("reporter_high_water", 1),
        ("authority_mode", "enabled"),
        ("allocation_epoch", 1),
    ],
)
def test_database_component_rejects_sealed_predecessor_with_changed_stable_identity(
    tmp_path: Path,
    field: str,
    value: object,
) -> None:
    """Break caught: accepting a sealed predecessor with another stable identity."""

    plan, runner, component = _database_component(tmp_path, database_state="exact")
    runner.registration_overrides = {
        "candidate_digest": "d" * 64,
        "candidate_identity": "f" * 40,
        "candidate_publication_sha256": "d" * 64,
        "configuration_generation": plan.starting_mutation_epoch,
        "deployment_generation": plan.starting_mutation_epoch,
        field: value,
    }
    runner.protected_roles_sealed = True

    assert component.classify(plan).state is ComponentState.DRIFTED


@pytest.mark.parametrize(
    "role",
    [
        "loom_cap_staging_agent",
        "loom_cap_staging_observer",
        "loom_cap_staging_runtime",
    ],
)
def test_database_component_allows_active_login_role_session_in_exact_state(
    tmp_path: Path,
    role: str,
) -> None:
    """Break caught: treating a normal protected login-role session as database drift."""

    plan, runner, _component = _database_component(tmp_path, database_state="exact")
    runner.active_protected_sessions[role] = 1
    direct = KubernetesProtectedStagingCapacityDatabaseComponent(
        runner=runner,  # type: ignore[arg-type]
        container_registry="registry.example.test/loom",
        seed_reader=lambda: runner.seed,
    )

    assert direct._database_state(plan, runner.seed).value == "exact"


def test_database_component_rejects_active_non_migrator_session_in_sealed_state(
    tmp_path: Path,
) -> None:
    """Break caught: sealed retry ignoring a live protected non-migrator session."""

    plan, runner, component = _database_component(tmp_path, database_state="exact")
    runner.protected_roles_sealed = True
    runner.allow_sealed_runtime_impersonation = True
    runner.active_protected_sessions["loom_cap_staging_agent"] = 1

    assert component.classify(plan).state is ComponentState.DRIFTED


def test_database_component_rejects_changed_database_grant_in_sealed_state(
    tmp_path: Path,
) -> None:
    """Break caught: sealed retry ignoring changed protected-role database authority."""

    plan, runner, component = _database_component(tmp_path, database_state="exact")
    runner.protected_roles_sealed = True
    runner.allow_sealed_runtime_impersonation = True
    runner.protected_database_privileges["loom_cap_staging_executor"] = {
        "acl": [{"grantable": True, "grantor": "loom_cap_other", "privilege": "CONNECT"}],
        "connect": True,
        "create": False,
        "temporary": False,
    }

    assert component.classify(plan).state is ComponentState.DRIFTED


def test_database_component_recovers_exact_completed_residue_by_cleanup_only(
    tmp_path: Path,
) -> None:
    plan, runner, component = _database_component(tmp_path, database_state="absent")
    component.apply(plan)
    runner.objects = deepcopy(runner.created_objects)
    runner.objects["Job"]["status"] = {"succeeded": 1}
    runner.database_state = "exact"
    runner.calls.clear()

    assert component.classify(plan).state is ComponentState.READY
    component.apply(plan)

    assert runner.objects == {}
    assert all(
        "create" not in call and "--for=condition=complete" not in call for call in runner.calls
    )
    assert component.classify(plan).state is ComponentState.EXACT


@pytest.mark.parametrize("kind", ["Secret", "Job"])
def test_database_component_recovers_exact_partial_bootstrap_resource_set(
    tmp_path: Path,
    kind: str,
) -> None:
    plan, runner, component = _database_component(tmp_path, database_state="absent")
    direct = KubernetesProtectedStagingCapacityDatabaseComponent(
        runner=runner,  # type: ignore[arg-type]
        container_registry="registry.example.test/loom",
        seed_reader=lambda: runner.seed,
    )
    manifest = direct._manifest(plan, runner.seed)
    expected = {
        document["kind"]: runner._stored(document)
        for document in yaml.safe_load_all(manifest)
        if document is not None
    }
    runner.objects = {kind: expected[kind]}

    assert component.classify(plan).state is ComponentState.READY

    component.apply(plan)

    assert runner.objects == {}
    assert component.classify(plan).state is ComponentState.EXACT


@pytest.mark.parametrize("epoch_gap", [1, 2])
@pytest.mark.parametrize("raw_reporter_payload", [False, True])
def test_database_component_recovers_certified_failed_older_auth_manifest(
    tmp_path: Path,
    epoch_gap: int,
    raw_reporter_payload: bool,
) -> None:
    """Break caught: a reviewed auth upgrade stranding the prior failed bootstrap."""

    plan, runner, component = _database_component(tmp_path, database_state="absent")
    prior_plan = _prior_database_plan(
        plan,
        starting_mutation_epoch=plan.starting_mutation_epoch - epoch_gap,
    )
    request_root = tmp_path / "state" / "requests" / prior_plan.request_id
    attempt_root = request_root / "attempts" / str(prior_plan.attempt_number)
    for directory in (request_root.parent, request_root, attempt_root.parent, attempt_root):
        directory.mkdir(mode=0o700)
    FinalGatePlanStore(
        tmp_path / "state",
        request_id=prior_plan.request_id,
        attempt_number=prior_plan.attempt_number,
        service_uid=os.geteuid(),
    ).publish(prior_plan)
    direct = KubernetesProtectedStagingCapacityDatabaseComponent(
        runner=runner,  # type: ignore[arg-type]
        container_registry="registry.example.test/loom",
        seed_reader=lambda: runner.seed,
    )
    runner.objects = _legacy_database_bootstrap_objects(direct, runner, prior_plan)
    if raw_reporter_payload:
        secret_data = runner.objects["Secret"]["data"]
        assert isinstance(secret_data, dict)
        raw_reporter_incarnation = runner.seed["reporter_incarnation"]
        reporter_configuration = json.loads(
            base64.b64decode(secret_data["reporter-configuration.json"], validate=True)
        )
        reporter_configuration["reporter_incarnation"] = raw_reporter_incarnation
        secret_data["reporter-configuration.json"] = base64.b64encode(
            json.dumps(
                reporter_configuration,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("ascii")
        ).decode("ascii")
        secret_data["seed.json"] = base64.b64encode(
            (json.dumps(runner.seed, sort_keys=True, separators=(",", ":")) + "\n").encode("ascii")
        ).decode("ascii")
    retained_uids = {
        kind: str(resource["metadata"]["uid"]) for kind, resource in runner.objects.items()
    }

    assert component.classify(plan).state is ComponentState.READY

    component.apply(plan)

    assert runner.objects == {}
    assert runner.events.index("delete-job") < runner.events.index("arm")
    assert runner.events.index("delete-secret") < runner.events.index("arm")
    deleted_identities = [
        (
            "Job" if any("/jobs/" in item for item in command) else "Secret",
            json.loads(payload)["preconditions"]["uid"],
        )
        for command, payload in runner.delete_inputs
    ]
    assert deleted_identities[:2] == [
        ("Job", retained_uids["Job"]),
        ("Secret", retained_uids["Secret"]),
    ]
    assert component.classify(plan).state is ComponentState.EXACT


def test_database_component_streams_cleanup_patch_through_supported_stdin_path(
    tmp_path: Path,
) -> None:
    """Break caught: a literal dash patch filename stranding a certified failed Job."""

    plan, runner, component = _database_component(tmp_path, database_state="absent")
    prior_plan = _prior_database_plan(
        plan,
        starting_mutation_epoch=plan.starting_mutation_epoch - 2,
    )
    _write_database_plan_ledger_entry(tmp_path, prior_plan)
    direct = KubernetesProtectedStagingCapacityDatabaseComponent(
        runner=runner,  # type: ignore[arg-type]
        container_registry="registry.example.test/loom",
        seed_reader=lambda: runner.seed,
    )
    runner.objects = _legacy_database_bootstrap_objects(direct, runner, prior_plan)
    runner.require_supported_patch_stdin = True

    component.apply(plan)

    assert runner.objects == {}
    assert component.classify(plan).state is ComponentState.EXACT


@pytest.mark.parametrize("epoch_offset", [0, 1])
def test_database_component_rejects_same_or_future_auth_manifest(
    tmp_path: Path,
    epoch_offset: int,
) -> None:
    """Break caught: cleanup authorized by a plan that is not strictly older."""

    plan, runner, component = _database_component(tmp_path, database_state="absent")
    prior_plan = _prior_database_plan(
        plan,
        starting_mutation_epoch=plan.starting_mutation_epoch + epoch_offset,
    )
    _write_database_plan_ledger_entry(tmp_path, prior_plan)
    direct = KubernetesProtectedStagingCapacityDatabaseComponent(
        runner=runner,  # type: ignore[arg-type]
        container_registry="registry.example.test/loom",
        seed_reader=lambda: runner.seed,
    )
    runner.objects = _legacy_database_bootstrap_objects(direct, runner, prior_plan)
    retained = deepcopy(runner.objects)

    assert component.classify(plan).state is ComponentState.DRIFTED
    with pytest.raises(RuntimeError, match="state changed before"):
        component.apply(plan)

    assert runner.objects == retained
    assert runner.delete_inputs == []


def test_database_component_uses_supported_diff_for_previous_auth_manifest(
    tmp_path: Path,
) -> None:
    """Break caught: passing the unsupported --validate flag to kubectl diff."""

    plan, runner, component = _database_component(tmp_path, database_state="absent")
    prior_plan = _prior_database_plan(plan)
    _write_database_plan_ledger_entry(tmp_path, prior_plan)
    direct = KubernetesProtectedStagingCapacityDatabaseComponent(
        runner=runner,  # type: ignore[arg-type]
        container_registry="registry.example.test/loom",
        seed_reader=lambda: runner.seed,
    )
    runner.objects = _legacy_database_bootstrap_objects(direct, runner, prior_plan)
    runner.reject_diff_validate_flag = True

    assert component.classify(plan).state is ComponentState.READY


@pytest.mark.parametrize(
    "job_status",
    [
        {"active": 1, "failed": 1},
        {
            "active": 1,
            "failed": 1,
            "conditions": [{"status": "True", "type": "FailureTarget"}],
        },
        {
            "active": 1,
            "failed": 1,
            "conditions": [{"status": "True", "type": "Failed"}],
        },
    ],
)
def test_database_component_rejects_nonterminal_previous_auth_job(
    tmp_path: Path,
    job_status: dict[str, object],
) -> None:
    """Break caught: deleting a prior Job that has failed pods but is still retrying."""

    plan, runner, component = _database_component(tmp_path, database_state="absent")
    prior_plan = _prior_database_plan(plan)
    _write_database_plan_ledger_entry(tmp_path, prior_plan)
    direct = KubernetesProtectedStagingCapacityDatabaseComponent(
        runner=runner,  # type: ignore[arg-type]
        container_registry="registry.example.test/loom",
        seed_reader=lambda: runner.seed,
    )
    runner.objects = _legacy_database_bootstrap_objects(direct, runner, prior_plan)
    runner.objects["Job"]["status"] = job_status
    retained = deepcopy(runner.objects)

    assert component.classify(plan).state is ComponentState.DRIFTED
    with pytest.raises(RuntimeError, match="state changed before"):
        component.apply(plan)

    assert runner.objects == retained
    assert runner.delete_inputs == []


def test_database_component_rejects_previous_job_activated_after_final_certification(
    tmp_path: Path,
) -> None:
    """Break caught: cleanup adopting a newly active Job after certification."""

    plan, runner, component = _database_component(tmp_path, database_state="exact")
    prior_plan = _prior_database_plan(plan)
    _write_database_plan_ledger_entry(tmp_path, prior_plan)
    direct = KubernetesProtectedStagingCapacityDatabaseComponent(
        runner=runner,  # type: ignore[arg-type]
        container_registry="registry.example.test/loom",
        seed_reader=lambda: runner.seed,
    )
    runner.objects = _legacy_database_bootstrap_objects(direct, runner, prior_plan)
    runner.activate_job_after_diff_count = 3

    with pytest.raises(RuntimeError, match="changed during cleanup"):
        component.apply(plan)

    assert runner.objects["Job"]["status"] == {
        "active": 1,
        "failed": 1,
        "conditions": [{"status": "True", "type": "Failed"}],
    }
    assert runner.patch_inputs == []
    assert runner.delete_inputs == []


@pytest.mark.parametrize("change", ["resource-version", "replacement"])
def test_database_component_rejects_previous_job_identity_changed_after_final_certification(
    tmp_path: Path,
    change: str,
) -> None:
    """Break caught: cleanup adopting a newer version or replacement after certification."""

    plan, runner, component = _database_component(tmp_path, database_state="exact")
    prior_plan = _prior_database_plan(plan)
    _write_database_plan_ledger_entry(tmp_path, prior_plan)
    direct = KubernetesProtectedStagingCapacityDatabaseComponent(
        runner=runner,  # type: ignore[arg-type]
        container_registry="registry.example.test/loom",
        seed_reader=lambda: runner.seed,
    )
    runner.objects = _legacy_database_bootstrap_objects(direct, runner, prior_plan)
    original_uid = runner.objects["Job"]["metadata"]["uid"]
    original_resource_version = runner.objects["Job"]["metadata"]["resourceVersion"]
    if change == "resource-version":
        runner.churn_job_after_diff_count = 3
    else:
        runner.replace_job_after_diff_count = 3

    with pytest.raises(RuntimeError, match="changed during cleanup"):
        component.apply(plan)

    job_metadata = runner.objects["Job"]["metadata"]
    assert isinstance(job_metadata, dict)
    if change == "resource-version":
        assert job_metadata["uid"] == original_uid
        assert job_metadata["resourceVersion"] != original_resource_version
    else:
        assert job_metadata["uid"] != original_uid
    assert runner.patch_inputs == []
    assert runner.delete_inputs == []


def test_database_component_requires_complete_pair_after_final_certification(
    tmp_path: Path,
) -> None:
    """Break caught: cleanup proceeding after one certified resource disappears."""

    plan, runner, component = _database_component(tmp_path, database_state="exact")
    prior_plan = _prior_database_plan(plan)
    _write_database_plan_ledger_entry(tmp_path, prior_plan)
    direct = KubernetesProtectedStagingCapacityDatabaseComponent(
        runner=runner,  # type: ignore[arg-type]
        container_registry="registry.example.test/loom",
        seed_reader=lambda: runner.seed,
    )
    runner.objects = _legacy_database_bootstrap_objects(direct, runner, prior_plan)
    runner.disappear_secret_after_diff_count = 3

    with pytest.raises(RuntimeError, match="changed during cleanup"):
        component.apply(plan)

    assert set(runner.objects) == {"Job"}
    assert runner.patch_inputs == []
    assert runner.delete_inputs == []


def test_database_component_retries_certified_job_after_its_cleanup_patch(
    tmp_path: Path,
) -> None:
    """Break caught: one-time certified versions blocking safe compensation retry."""

    plan, runner, component = _database_component(tmp_path, database_state="absent")
    prior_plan = _prior_database_plan(plan)
    _write_database_plan_ledger_entry(tmp_path, prior_plan)
    direct = KubernetesProtectedStagingCapacityDatabaseComponent(
        runner=runner,  # type: ignore[arg-type]
        container_registry="registry.example.test/loom",
        seed_reader=lambda: runner.seed,
    )
    runner.objects = _legacy_database_bootstrap_objects(direct, runner, prior_plan)
    runner.fail_delete_job_before_mutation = 3

    component.apply(plan)

    assert runner.fail_delete_job_before_mutation == 0
    assert runner.events.count("delete-job") >= 4
    assert runner.objects == {}
    assert component.classify(plan).state is ComponentState.EXACT


def test_database_component_rejects_previous_auth_manifest_without_plan(tmp_path: Path) -> None:
    """Break caught: treating annotations alone as cleanup authority."""

    plan, runner, component = _database_component(tmp_path, database_state="absent")
    prior_plan = _prior_database_plan(plan)
    direct = KubernetesProtectedStagingCapacityDatabaseComponent(
        runner=runner,  # type: ignore[arg-type]
        container_registry="registry.example.test/loom",
        seed_reader=lambda: runner.seed,
    )
    runner.objects = _legacy_database_bootstrap_objects(direct, runner, prior_plan)
    retained = deepcopy(runner.objects)

    assert component.classify(plan).state is ComponentState.DRIFTED
    with pytest.raises(RuntimeError, match="state changed before"):
        component.apply(plan)

    assert runner.objects == retained
    assert runner.delete_inputs == []


def test_database_component_rejects_misfiled_previous_plan(tmp_path: Path) -> None:
    """Break caught: accepting plan content that is not bound to its ledger identity."""

    plan, runner, component = _database_component(tmp_path, database_state="absent")
    prior_plan = _prior_database_plan(plan)
    _write_database_plan_ledger_entry(tmp_path, prior_plan, request_id="req-misfiled01")
    direct = KubernetesProtectedStagingCapacityDatabaseComponent(
        runner=runner,  # type: ignore[arg-type]
        container_registry="registry.example.test/loom",
        seed_reader=lambda: runner.seed,
    )
    runner.objects = _legacy_database_bootstrap_objects(direct, runner, prior_plan)
    retained = deepcopy(runner.objects)

    assert component.classify(plan).state is ComponentState.DRIFTED
    with pytest.raises(RuntimeError, match="state changed before"):
        component.apply(plan)

    assert runner.objects == retained
    assert runner.delete_inputs == []


def test_database_component_rejects_duplicated_previous_plan(tmp_path: Path) -> None:
    """Break caught: choosing cleanup authority from duplicate ledger entries."""

    plan, runner, component = _database_component(tmp_path, database_state="absent")
    prior_plan = _prior_database_plan(plan)
    _write_database_plan_ledger_entry(tmp_path, prior_plan)
    _write_database_plan_ledger_entry(tmp_path, prior_plan, request_id="req-duplicate01")
    direct = KubernetesProtectedStagingCapacityDatabaseComponent(
        runner=runner,  # type: ignore[arg-type]
        container_registry="registry.example.test/loom",
        seed_reader=lambda: runner.seed,
    )
    runner.objects = _legacy_database_bootstrap_objects(direct, runner, prior_plan)
    retained = deepcopy(runner.objects)

    assert component.classify(plan).state is ComponentState.DRIFTED
    with pytest.raises(RuntimeError, match="state changed before"):
        component.apply(plan)

    assert runner.objects == retained
    assert runner.delete_inputs == []


def test_database_component_rejects_noncanonical_previous_attempt_directory(
    tmp_path: Path,
) -> None:
    """Break caught: treating attempts/01 as the canonical attempt 1 ledger path."""

    plan, runner, component = _database_component(tmp_path, database_state="absent")
    prior_plan = _prior_database_plan(plan)
    _write_database_plan_ledger_entry(tmp_path, prior_plan)
    attempts_root = tmp_path / "state" / "requests" / prior_plan.request_id / "attempts"
    (attempts_root / "1").rename(attempts_root / "01")
    direct = KubernetesProtectedStagingCapacityDatabaseComponent(
        runner=runner,  # type: ignore[arg-type]
        container_registry="registry.example.test/loom",
        seed_reader=lambda: runner.seed,
    )
    runner.objects = _legacy_database_bootstrap_objects(direct, runner, prior_plan)
    retained = deepcopy(runner.objects)

    assert component.classify(plan).state is ComponentState.DRIFTED
    with pytest.raises(RuntimeError, match="state changed before"):
        component.apply(plan)

    assert runner.objects == retained
    assert runner.delete_inputs == []


def test_database_component_rejects_unexpected_previous_attempt_entry(tmp_path: Path) -> None:
    """Break caught: silently skipping an unsafe entry in the protected attempts ledger."""

    plan, runner, component = _database_component(tmp_path, database_state="absent")
    prior_plan = _prior_database_plan(plan)
    _write_database_plan_ledger_entry(tmp_path, prior_plan)
    unexpected = tmp_path / "state" / "requests" / prior_plan.request_id / "attempts" / "unexpected"
    unexpected.write_text("unsafe\n", encoding="utf-8")
    unexpected.chmod(0o600)
    direct = KubernetesProtectedStagingCapacityDatabaseComponent(
        runner=runner,  # type: ignore[arg-type]
        container_registry="registry.example.test/loom",
        seed_reader=lambda: runner.seed,
    )
    runner.objects = _legacy_database_bootstrap_objects(direct, runner, prior_plan)
    retained = deepcopy(runner.objects)

    assert component.classify(plan).state is ComponentState.DRIFTED
    with pytest.raises(RuntimeError, match="state changed before"):
        component.apply(plan)

    assert runner.objects == retained
    assert runner.delete_inputs == []


@pytest.mark.parametrize("entry_kind", ["file", "symlink"])
def test_database_component_rejects_unexpected_recovery_request_entry(
    tmp_path: Path,
    entry_kind: str,
) -> None:
    """Break caught: silently skipping an unsafe entry in the requests ledger."""

    plan, runner, component = _database_component(tmp_path, database_state="absent")
    prior_plan = _prior_database_plan(plan)
    _write_database_plan_ledger_entry(tmp_path, prior_plan)
    requests_root = tmp_path / "state" / "requests"
    unexpected = requests_root / "unexpected"
    if entry_kind == "file":
        unexpected.write_text("unsafe\n", encoding="utf-8")
        unexpected.chmod(0o600)
    else:
        unexpected.symlink_to(requests_root / prior_plan.request_id, target_is_directory=True)
    direct = KubernetesProtectedStagingCapacityDatabaseComponent(
        runner=runner,  # type: ignore[arg-type]
        container_registry="registry.example.test/loom",
        seed_reader=lambda: runner.seed,
    )
    runner.objects = _legacy_database_bootstrap_objects(direct, runner, prior_plan)
    retained = deepcopy(runner.objects)

    assert component.classify(plan).state is ComponentState.DRIFTED
    with pytest.raises(RuntimeError, match="state changed before"):
        component.apply(plan)

    assert runner.objects == retained
    assert runner.delete_inputs == []


def test_database_component_rejects_recovery_ledger_over_global_plan_file_budget(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Break caught: a bounded directory fanout still causing an excessive global scan."""

    monkeypatch.setattr(protected_runtime, "_MAX_RECOVERY_PLAN_FILES", 1, raising=False)
    plan, runner, component = _database_component(tmp_path, database_state="absent")
    prior_plan = _prior_database_plan(plan)
    unrelated_plan = _prior_database_plan(
        plan,
        request_id="req-unrelated01",
        rollout_id="20260905t181433z-staging-unrelated01",
    )
    _write_database_plan_ledger_entry(tmp_path, prior_plan)
    _write_database_plan_ledger_entry(tmp_path, unrelated_plan)
    direct = KubernetesProtectedStagingCapacityDatabaseComponent(
        runner=runner,  # type: ignore[arg-type]
        container_registry="registry.example.test/loom",
        seed_reader=lambda: runner.seed,
    )
    runner.objects = _legacy_database_bootstrap_objects(direct, runner, prior_plan)
    retained = deepcopy(runner.objects)

    assert component.classify(plan).state is ComponentState.DRIFTED
    with pytest.raises(RuntimeError, match="state changed before"):
        component.apply(plan)

    assert runner.objects == retained
    assert runner.delete_inputs == []


def test_database_component_rejects_recovery_ledger_over_global_attempt_probe_budget(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Break caught: missing plan files bypassing the global ledger scan bound."""

    monkeypatch.setattr(protected_runtime, "_MAX_RECOVERY_ATTEMPT_PROBES", 1, raising=False)
    plan, runner, component = _database_component(tmp_path, database_state="absent")
    prior_plan = _prior_database_plan(plan)
    _write_database_plan_ledger_entry(tmp_path, prior_plan)
    empty_attempt = tmp_path / "state" / "requests" / prior_plan.request_id / "attempts" / "2"
    empty_attempt.mkdir(mode=0o700)
    direct = KubernetesProtectedStagingCapacityDatabaseComponent(
        runner=runner,  # type: ignore[arg-type]
        container_registry="registry.example.test/loom",
        seed_reader=lambda: runner.seed,
    )
    runner.objects = _legacy_database_bootstrap_objects(direct, runner, prior_plan)
    retained = deepcopy(runner.objects)

    assert component.classify(plan).state is ComponentState.DRIFTED
    with pytest.raises(RuntimeError, match="state changed before"):
        component.apply(plan)

    assert runner.objects == retained
    assert runner.delete_inputs == []


@pytest.mark.parametrize("directory_kind", ["requests", "attempts"])
def test_database_component_stops_recovery_directory_enumeration_at_limit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    directory_kind: str,
) -> None:
    """Break caught: materializing an unbounded directory before enforcing its limit."""

    plan, runner, component = _database_component(tmp_path, database_state="absent")
    prior_plan = _prior_database_plan(plan)
    _write_database_plan_ledger_entry(tmp_path, prior_plan)
    requests_root = tmp_path / "state" / "requests"
    attempts_root = requests_root / prior_plan.request_id / "attempts"
    if directory_kind == "requests":
        for request_id in ("req-extra01", "req-extra02"):
            (requests_root / request_id).mkdir(mode=0o700)
        target = requests_root
        monkeypatch.setattr(protected_runtime, "_MAX_RECOVERY_REQUESTS", 1)
    else:
        for attempt_number in (2, 3):
            (attempts_root / str(attempt_number)).mkdir(mode=0o700)
        target = attempts_root
        monkeypatch.setattr(protected_runtime, "_MAX_RECOVERY_ATTEMPTS_PER_REQUEST", 1)
    direct = KubernetesProtectedStagingCapacityDatabaseComponent(
        runner=runner,  # type: ignore[arg-type]
        container_registry="registry.example.test/loom",
        seed_reader=lambda: runner.seed,
    )
    runner.objects = _legacy_database_bootstrap_objects(direct, runner, prior_plan)

    original_scandir = protected_runtime.os.scandir
    yielded = 0

    class CountingScandir:
        def __init__(self, path: Path) -> None:
            self.inner = original_scandir(path)

        def __iter__(self):
            return self

        def __next__(self):
            nonlocal yielded
            entry = next(self.inner)
            yielded += 1
            return entry

        def __enter__(self):
            return self

        def __exit__(self, *_args: object) -> None:
            self.inner.close()

    def counted_scandir(path: Path):
        if Path(path) == target:
            return CountingScandir(path)
        return original_scandir(path)

    monkeypatch.setattr(protected_runtime.os, "scandir", counted_scandir)

    assert component.classify(plan).state is ComponentState.DRIFTED
    assert yielded == 2


def test_database_component_rejects_recovery_ledger_over_global_byte_budget(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Break caught: aggregate recovery plan reads exceeding their byte budget."""

    monkeypatch.setattr(protected_runtime, "_MAX_RECOVERY_PLAN_BYTES", 1, raising=False)
    plan, runner, component = _database_component(tmp_path, database_state="absent")
    prior_plan = _prior_database_plan(plan)
    _write_database_plan_ledger_entry(tmp_path, prior_plan)
    direct = KubernetesProtectedStagingCapacityDatabaseComponent(
        runner=runner,  # type: ignore[arg-type]
        container_registry="registry.example.test/loom",
        seed_reader=lambda: runner.seed,
    )
    runner.objects = _legacy_database_bootstrap_objects(direct, runner, prior_plan)
    retained = deepcopy(runner.objects)

    assert component.classify(plan).state is ComponentState.DRIFTED
    with pytest.raises(RuntimeError, match="state changed before"):
        component.apply(plan)

    assert runner.objects == retained
    assert runner.delete_inputs == []


@pytest.mark.parametrize(
    "manager_field",
    [
        "manager_configuration_epoch",
        "manager_configuration_digest",
        "manager_writer_epoch",
    ],
)
def test_database_component_rejects_previous_plan_with_manager_authority_drift(
    tmp_path: Path,
    manager_field: str,
) -> None:
    """Break caught: cleanup authorized by a different manager authority tuple."""

    plan, runner, component = _database_component(tmp_path, database_state="absent")
    current_value = getattr(plan, manager_field)
    drifted_value: object
    if manager_field == "manager_configuration_digest":
        drifted_value = "f" * 64 if current_value != "f" * 64 else "e" * 64
    else:
        assert isinstance(current_value, int)
        drifted_value = current_value + 1
    prior_plan = _prior_database_plan(
        plan,
        starting_mutation_epoch=plan.starting_mutation_epoch - 2,
        **{manager_field: drifted_value},
    )
    _write_database_plan_ledger_entry(tmp_path, prior_plan)
    direct = KubernetesProtectedStagingCapacityDatabaseComponent(
        runner=runner,  # type: ignore[arg-type]
        container_registry="registry.example.test/loom",
        seed_reader=lambda: runner.seed,
    )
    runner.objects = _legacy_database_bootstrap_objects(direct, runner, prior_plan)
    retained = deepcopy(runner.objects)

    assert component.classify(plan).state is ComponentState.DRIFTED
    with pytest.raises(RuntimeError, match="state changed before"):
        component.apply(plan)

    assert runner.objects == retained
    assert runner.delete_inputs == []


def test_database_component_accepts_previous_public_schema_revision(
    tmp_path: Path,
) -> None:
    """Break caught: equating the full database digest with manager authority."""

    plan, runner, component = _database_component(tmp_path, database_state="exact")
    prior_plan = _prior_database_plan(
        plan,
        public_schema_revision="prior_schema_revision",
        schema_revision="prior_schema_revision",
    )
    assert prior_plan.database_authority_digest != plan.database_authority_digest
    _write_database_plan_ledger_entry(tmp_path, prior_plan)
    direct = KubernetesProtectedStagingCapacityDatabaseComponent(
        runner=runner,  # type: ignore[arg-type]
        container_registry="registry.example.test/loom",
        seed_reader=lambda: runner.seed,
    )
    runner.objects = _legacy_database_bootstrap_objects(direct, runner, prior_plan)

    assert component.classify(plan).state is ComponentState.READY
    component.apply(plan)

    assert runner.objects == {}


@pytest.mark.parametrize("drift", ["additive-secret-data", "job-plan-annotation"])
def test_database_component_rejects_drifted_previous_auth_manifest(
    tmp_path: Path,
    drift: str,
) -> None:
    """Break caught: cleanup of prior resources with additive or identity drift."""

    plan, runner, component = _database_component(tmp_path, database_state="absent")
    prior_plan = _prior_database_plan(plan)
    _write_database_plan_ledger_entry(tmp_path, prior_plan)
    direct = KubernetesProtectedStagingCapacityDatabaseComponent(
        runner=runner,  # type: ignore[arg-type]
        container_registry="registry.example.test/loom",
        seed_reader=lambda: runner.seed,
    )
    runner.objects = _legacy_database_bootstrap_objects(direct, runner, prior_plan)
    if drift == "additive-secret-data":
        secret_data = runner.objects["Secret"]["data"]
        assert isinstance(secret_data, dict)
        secret_data["foreign"] = "dmFsdWU="
    else:
        metadata = runner.objects["Job"]["metadata"]
        assert isinstance(metadata, dict)
        annotations = metadata["annotations"]
        assert isinstance(annotations, dict)
        annotations["loom.carin.dev/plan-digest"] = "0" * 64
    retained = deepcopy(runner.objects)

    assert component.classify(plan).state is ComponentState.DRIFTED
    with pytest.raises(RuntimeError, match="state changed before"):
        component.apply(plan)

    assert runner.objects == retained
    assert runner.delete_inputs == []


def test_database_component_treats_failure_target_condition_as_failed(tmp_path: Path) -> None:
    plan, runner, _component = _database_component(tmp_path, database_state="absent")
    direct = KubernetesProtectedStagingCapacityDatabaseComponent(
        runner=runner,  # type: ignore[arg-type]
        container_registry="registry.example.test/loom",
        seed_reader=lambda: runner.seed,
    )
    manifest = direct._manifest(plan, runner.seed)
    runner.objects = {
        document["kind"]: runner._stored(document)
        for document in yaml.safe_load_all(manifest)
        if document is not None
    }
    runner.objects["Job"]["status"] = {"conditions": [{"type": "FailureTarget", "status": "True"}]}

    state, _evidence = direct._resource_state(plan, manifest)

    assert state.value == "failed"


@pytest.mark.parametrize("fail_checked", ["exec", "create", "wait"])
def test_database_component_always_seals_transient_authority_after_failure(
    tmp_path: Path,
    fail_checked: str,
) -> None:
    plan, runner, component = _database_component(
        tmp_path,
        database_state="absent",
        fail_checked=fail_checked,
    )
    password = str(runner.seed["migrator_database_password"])

    failure_pattern = (
        "protected staging capacity database bootstrap job failed"
        if fail_checked == "wait"
        else "injected protected database mutation failure"
    )
    with pytest.raises(RuntimeError, match=failure_pattern) as failure:
        component.apply(plan)

    assert password not in str(failure.value)
    assert all(password not in " ".join(call) for call in runner.calls)
    arm_payload = next(
        payload
        for _command, payload in runner.checked_inputs
        if b"GRANT loom TO loom_cap_staging_migrator" in payload
    )
    disable_payload = next(
        payload
        for _command, payload in reversed(runner.checked_inputs)
        if b"loom_cap_staging_agent NOLOGIN" in payload
    )
    assert password.encode() in arm_payload
    assert password.encode() not in disable_payload
    assert b"NOLOGIN" in disable_payload
    assert b"NOCREATEROLE" in disable_payload
    assert b"loom_cap_staging_agent NOLOGIN" in disable_payload
    assert b"loom_cap_staging_observer NOLOGIN" in disable_payload
    assert b"loom_cap_staging_runtime NOLOGIN" in disable_payload
    if fail_checked == "wait":
        assert sum("--for=condition=complete" in call for call in runner.calls) == 1


@pytest.mark.parametrize("fail_checked", ["create", "wait"])
def test_database_component_failure_compensation_uses_safe_phase_order(
    tmp_path: Path,
    fail_checked: str,
) -> None:
    """Break caught: sealing authority before stopping the exact bootstrap Job."""

    _plan, runner, component = _database_component(
        tmp_path,
        database_state="absent",
        fail_checked=fail_checked,
    )
    runner.create_failure_leaves_all = fail_checked == "create"

    with pytest.raises(RuntimeError):
        component.apply(_plan)

    failure = max(index for index, event in enumerate(runner.events) if event == fail_checked)
    disable = next(
        index
        for index, event in enumerate(runner.events)
        if index > failure and event == "disable-all"
    )
    terminate = next(
        index
        for index, event in enumerate(runner.events)
        if index > disable and event == "terminate"
    )
    delete_job = next(
        index
        for index, event in enumerate(runner.events)
        if index > terminate and event == "delete-job"
    )
    cleanup = next(
        index
        for index, event in enumerate(runner.events)
        if index > delete_job and event == "cleanup"
    )
    verify = next(
        index for index, event in enumerate(runner.events) if index > cleanup and event == "verify"
    )
    delete_secret = next(
        index
        for index, event in enumerate(runner.events)
        if index > verify and event == "delete-secret"
    )
    assert disable < terminate < delete_job < cleanup < verify < delete_secret
    assert all("+" not in event for event in runner.events if event.startswith("disable"))


def test_database_component_verifies_database_before_finalizing_runtime_credentials(
    tmp_path: Path,
) -> None:
    """Break caught: making runtime credentials permanent before exact DB verification."""

    plan, runner, component = _database_component(tmp_path, database_state="absent")
    runner.fail_database_verification_after_wait = True

    with pytest.raises(RuntimeError, match="injected protected database verification failure"):
        component.apply(plan)

    assert not any(event == "finalize" for event in runner.events)
    assert any(event == "disable-all" for event in runner.events)
    assert not any(
        b"ALTER ROLE loom_cap_staging_agent LOGIN NOSUPERUSER NOCREATEDB "
        b"NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS VALID UNTIL 'infinity';" in payload
        for _command, payload in runner.checked_inputs
    )


def test_database_component_safely_seals_residue_before_retrying(tmp_path: Path) -> None:
    """Break caught: deleting retry residue while its old credentials remain armed."""

    plan, runner, component = _database_component(tmp_path, database_state="absent")
    direct = KubernetesProtectedStagingCapacityDatabaseComponent(
        runner=runner,  # type: ignore[arg-type]
        container_registry="registry.example.test/loom",
        seed_reader=lambda: runner.seed,
    )
    manifest = direct._manifest(plan, runner.seed)
    runner.objects = {
        document["kind"]: runner._stored(document)
        for document in yaml.safe_load_all(manifest)
        if document is not None
    }
    runner.objects["Job"]["status"] = {"failed": 1}

    component.apply(plan)

    first_arm = runner.events.index("arm")
    first_disable = runner.events.index("disable-all")
    first_terminate = runner.events.index("terminate")
    first_delete_job = runner.events.index("delete-job")
    first_cleanup = runner.events.index("cleanup")
    assert first_disable < first_terminate < first_delete_job < first_cleanup < first_arm


@pytest.mark.parametrize("phase", ["disable-all", "terminate", "delete-job"])
def test_database_component_retries_transient_compensation_shutdown_failures(
    tmp_path: Path,
    phase: str,
) -> None:
    """Break caught: a transient shutdown failure abandoning an authenticated Job."""

    plan, runner, component = _database_component(tmp_path, database_state="absent")
    direct = KubernetesProtectedStagingCapacityDatabaseComponent(
        runner=runner,  # type: ignore[arg-type]
        container_registry="registry.example.test/loom",
        seed_reader=lambda: runner.seed,
    )
    manifest = direct._manifest(plan, runner.seed)
    runner.objects = {
        document["kind"]: runner._stored(document)
        for document in yaml.safe_load_all(manifest)
        if document is not None
    }
    runner.objects["Job"]["status"] = {"failed": 1}
    if phase == "delete-job":
        runner.fail_delete_job_before_mutation = 1
    else:
        runner.fail_peer_phase_counts[phase] = 1

    component.apply(plan)

    assert runner.objects == {}
    assert runner.events.count(phase) >= 2
    assert runner.events.index("cleanup") < runner.events.index("arm")


def test_database_component_observes_shutdown_after_compensation_response_loss(
    tmp_path: Path,
) -> None:
    """Break caught: committed safety mutations reported as unresolved transport failures."""

    plan, runner, component = _database_component(tmp_path, database_state="absent")
    direct = KubernetesProtectedStagingCapacityDatabaseComponent(
        runner=runner,  # type: ignore[arg-type]
        container_registry="registry.example.test/loom",
        seed_reader=lambda: runner.seed,
    )
    manifest = direct._manifest(plan, runner.seed)
    runner.objects = {
        document["kind"]: runner._stored(document)
        for document in yaml.safe_load_all(manifest)
        if document is not None
    }
    runner.objects["Job"]["status"] = {"failed": 1}
    runner.fail_peer_phase_after_mutation_counts = {
        "disable-all": 2,
        "terminate": 2,
    }

    component.apply(plan)

    assert runner.objects == {}
    assert runner.events.count("disable-all") >= 2
    assert runner.events.count("terminate") >= 2
    assert component.classify(plan).state is ComponentState.EXACT


@pytest.mark.parametrize("phase", ["disable-all", "terminate"])
def test_database_component_stops_job_before_reporting_unresolved_compensation(
    tmp_path: Path,
    phase: str,
) -> None:
    """Break caught: a database outage preventing independent Kubernetes shutdown."""

    plan, runner, component = _database_component(tmp_path, database_state="absent")
    direct = KubernetesProtectedStagingCapacityDatabaseComponent(
        runner=runner,  # type: ignore[arg-type]
        container_registry="registry.example.test/loom",
        seed_reader=lambda: runner.seed,
    )
    manifest = direct._manifest(plan, runner.seed)
    runner.objects = {
        document["kind"]: runner._stored(document)
        for document in yaml.safe_load_all(manifest)
        if document is not None
    }
    runner.objects["Job"]["status"] = {"failed": 1}
    runner.fail_peer_phase_counts[phase] = 2

    with pytest.raises(RuntimeError, match="could not confirm safe shutdown"):
        component.apply(plan)

    assert "Job" not in runner.objects
    assert "Secret" in runner.objects
    assert runner.events.count(phase) == 2
    assert "cleanup" not in runner.events


def test_database_component_does_not_revoke_when_job_shutdown_is_unconfirmed(
    tmp_path: Path,
) -> None:
    """Break caught: revoking authority while the bootstrap Job may still be running."""

    plan, runner, component = _database_component(tmp_path, database_state="absent")
    direct = KubernetesProtectedStagingCapacityDatabaseComponent(
        runner=runner,  # type: ignore[arg-type]
        container_registry="registry.example.test/loom",
        seed_reader=lambda: runner.seed,
    )
    manifest = direct._manifest(plan, runner.seed)
    runner.objects = {
        document["kind"]: runner._stored(document)
        for document in yaml.safe_load_all(manifest)
        if document is not None
    }
    runner.objects["Job"]["status"] = {"failed": 1}
    runner.fail_delete_job_before_mutation = 100

    with pytest.raises(RuntimeError, match="could not confirm safe shutdown: job"):
        component.apply(plan)

    assert set(runner.objects) == {"Job", "Secret"}
    assert runner.events.count("disable-all") == 2
    assert runner.events.count("terminate") == 2
    assert runner.events.count("delete-job") == 6
    assert "cleanup" not in runner.events


def test_database_component_recovers_from_non_atomic_resource_create_failure(
    tmp_path: Path,
) -> None:
    plan, runner, component = _database_component(
        tmp_path,
        database_state="absent",
        fail_checked="create",
    )

    with pytest.raises(RuntimeError, match="injected protected database mutation failure"):
        component.apply(plan)

    assert component.classify(plan).state is ComponentState.READY
    runner.fail_checked = None
    runner.failed_checked = False
    runner.calls.clear()
    runner.checked_inputs.clear()
    runner.patch_inputs.clear()

    component.apply(plan)

    assert runner.objects == {}
    assert component.classify(plan).state is ComponentState.EXACT


def test_database_component_recovers_from_ambiguous_successful_resource_delete(
    tmp_path: Path,
) -> None:
    """Break caught: retrying a delete whose lost response hid confirmed disappearance."""

    plan, runner, component = _database_component(tmp_path, database_state="absent")
    component.apply(plan)
    runner.objects = deepcopy(runner.created_objects)
    runner.objects["Job"]["status"] = {"failed": 1}
    runner.database_state = "absent"
    runner.calls.clear()
    runner.checked_inputs.clear()
    runner.patch_inputs.clear()
    runner.events.clear()
    runner.fail_checked = "delete"
    runner.failed_checked = False

    component.apply(plan)

    assert runner.failed_checked is True
    assert runner.objects == {}
    assert runner.events.index("delete-job") < runner.events.index("cleanup")
    assert runner.events.index("cleanup") < runner.events.index("arm")
    assert component.classify(plan).state is ComponentState.EXACT


def test_database_component_cleanup_nonce_is_always_a_valid_kubernetes_label(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Break caught: an underscore-leading URL-safe nonce is not a valid label value."""

    raw_nonce = "_" + "a" * 42
    monkeypatch.setattr(
        "loom_cli.rollout.operator.protected_staging_capacity_database_component."
        "secrets.token_urlsafe",
        lambda _size: raw_nonce,
    )
    plan, runner, component = _database_component(tmp_path, database_state="absent")

    component.apply(plan)

    cleanup_values = [
        operation["value"]
        for _command, payload in runner.patch_inputs
        for operation in json.loads(payload)
        if operation["op"] == "add"
        and operation["path"] == "/metadata/labels/loom.carin.dev~1protected-cleanup"
    ]
    assert cleanup_values
    assert all(value.endswith(raw_nonce) for value in cleanup_values)
    assert all(
        len(value) <= 63
        and value[0].isalnum()
        and value[-1].isalnum()
        and set(value) <= set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_.")
        for value in cleanup_values
    )


def test_database_component_waits_after_ambiguous_accepted_job_delete(
    tmp_path: Path,
) -> None:
    """Break caught: a lost DELETE response hiding the same terminating Job UID."""

    plan, runner, component = _database_component(tmp_path, database_state="absent")
    component.apply(plan)
    runner.objects = deepcopy(runner.created_objects)
    runner.objects["Job"]["status"] = {"failed": 1}
    runner.database_state = "exact"
    runner.calls.clear()
    runner.checked_inputs.clear()
    runner.patch_inputs.clear()
    runner.events.clear()
    runner.fail_checked = "delete"
    runner.failed_checked = False
    runner.delete_wait_counts["Job"] = 2

    component.apply(plan)

    assert runner.failed_checked is True
    assert runner.objects == {}
    assert runner.events.count("delete-job") == 1
    assert runner.events.count("wait-delete-job") == 2
    assert component.classify(plan).state is ComponentState.EXACT


@pytest.mark.parametrize("kind", ["Secret", "Job"])
def test_database_component_does_not_delete_replacement_after_cleanup_label_race(
    tmp_path: Path,
    kind: str,
) -> None:
    plan, runner, component = _database_component(tmp_path, database_state="absent")
    component.apply(plan)
    runner.objects = deepcopy(runner.created_objects)
    runner.objects["Job"]["status"] = {"succeeded": 1}
    runner.database_state = "exact"
    runner.replace_after_patch_kind = kind
    runner.calls.clear()
    runner.checked_inputs.clear()
    runner.patch_inputs.clear()

    with pytest.raises(RuntimeError, match="identity changed during cleanup"):
        component.apply(plan)

    replacement = runner.objects[kind]
    replacement_metadata = replacement["metadata"]
    assert isinstance(replacement_metadata, dict)
    labels = replacement_metadata["labels"]
    assert isinstance(labels, dict)
    assert "loom.carin.dev/protected-cleanup" not in labels
    assert runner.patch_inputs


@pytest.mark.parametrize("kind", ["Secret", "Job"])
def test_database_component_does_not_delete_replacement_after_cleanup_selection_race(
    tmp_path: Path,
    kind: str,
) -> None:
    """Break caught: selector listing an original before deleting its name replacement."""

    plan, runner, component = _database_component(tmp_path, database_state="absent")
    component.apply(plan)
    runner.objects = deepcopy(runner.created_objects)
    runner.objects["Job"]["status"] = {"succeeded": 1}
    runner.database_state = "exact"
    original_uid = runner.objects[kind]["metadata"]["uid"]
    runner.replace_after_selector_match_kind = kind

    with pytest.raises(RuntimeError, match="identity changed during cleanup"):
        component.apply(plan)

    assert runner.objects[kind]["metadata"]["uid"] != original_uid
    assert "loom.carin.dev/protected-cleanup" not in runner.objects[kind]["metadata"]["labels"]


def test_database_component_recovers_from_ambiguous_successful_job_cleanup_patch(
    tmp_path: Path,
) -> None:
    """Break caught: a YAML labels alias making a confirmed patch appear drifted."""

    plan, runner, component = _database_component(tmp_path, database_state="absent")
    component.apply(plan)
    runner.objects = deepcopy(runner.created_objects)
    runner.objects["Job"]["status"] = {"succeeded": 1}
    runner.database_state = "exact"
    runner.fail_patch_after_mutation_kind = "Job"
    runner.events.clear()

    component.apply(plan)

    assert runner.objects == {}
    assert runner.events.count("patch-job") == 1
    assert component.classify(plan).state is ComponentState.EXACT


def test_database_component_waits_for_same_uid_foreground_job_deletion(
    tmp_path: Path,
) -> None:
    """Break caught: deletion metadata on the same terminating Job reported as drift."""

    plan, runner, component = _database_component(tmp_path, database_state="absent")
    component.apply(plan)
    runner.objects = deepcopy(runner.created_objects)
    runner.objects["Job"]["status"] = {"succeeded": 1}
    runner.database_state = "exact"
    runner.events.clear()
    runner.delete_inputs.clear()
    runner.delete_wait_counts["Job"] = 2

    component.apply(plan)

    assert runner.objects == {}
    assert runner.events.count("delete-job") == 1
    assert runner.events.count("wait-delete-job") == 2
    assert len(runner.delete_inputs) == 2
    assert component.classify(plan).state is ComponentState.EXACT


@pytest.mark.parametrize("kind", ["Secret", "Job"])
def test_database_component_retries_same_uid_cleanup_patch_churn(
    tmp_path: Path,
    kind: str,
) -> None:
    """Break caught: one status/resourceVersion update making exact cleanup unrecoverable."""

    plan, runner, component = _database_component(tmp_path, database_state="absent")
    component.apply(plan)
    runner.objects = deepcopy(runner.created_objects)
    runner.objects["Job"]["status"] = {"succeeded": 1}
    runner.database_state = "exact"
    runner.patch_churn_counts[kind] = 1

    component.apply(plan)

    assert runner.objects == {}
    assert runner.events.count(f"patch-{kind.lower()}") >= 2


@pytest.mark.parametrize("kind", ["Secret", "Job"])
def test_database_component_accepts_disappearance_during_cleanup_patch(
    tmp_path: Path,
    kind: str,
) -> None:
    """Break caught: a confirmed absent exact object being treated as unsafe drift."""

    plan, runner, component = _database_component(tmp_path, database_state="absent")
    component.apply(plan)
    runner.objects = deepcopy(runner.created_objects)
    runner.objects["Job"]["status"] = {"succeeded": 1}
    runner.database_state = "exact"
    runner.disappear_before_patch_kind = kind

    component.apply(plan)

    assert runner.objects == {}


@pytest.mark.parametrize("kind", ["Secret", "Job"])
def test_database_component_preserves_replacement_during_cleanup_patch(
    tmp_path: Path,
    kind: str,
) -> None:
    """Break caught: retrying a cleanup patch across a reserved-name UID replacement."""

    plan, runner, component = _database_component(tmp_path, database_state="absent")
    component.apply(plan)
    runner.objects = deepcopy(runner.created_objects)
    runner.objects["Job"]["status"] = {"succeeded": 1}
    runner.database_state = "exact"
    original_uid = runner.objects[kind]["metadata"]["uid"]
    runner.replace_before_patch_kind = kind

    with pytest.raises(RuntimeError, match="identity changed during cleanup"):
        component.apply(plan)

    assert runner.objects[kind]["metadata"]["uid"] != original_uid
    assert "loom.carin.dev/protected-cleanup" not in runner.objects[kind]["metadata"]["labels"]


def test_database_component_bounds_same_uid_cleanup_patch_churn(tmp_path: Path) -> None:
    """Break caught: unbounded retry while an active Job continually changes status."""

    plan, runner, component = _database_component(tmp_path, database_state="absent")
    component.apply(plan)
    runner.objects = deepcopy(runner.created_objects)
    runner.objects["Job"]["status"] = {"succeeded": 1}
    runner.database_state = "exact"
    runner.patch_churn_counts["Job"] = 100

    with pytest.raises(RuntimeError, match="cleanup patch did not stabilize"):
        component.apply(plan)

    assert runner.events.count("patch-job") < 100
    assert set(runner.objects) == {"Job", "Secret"}


def test_database_component_retries_after_failed_bootstrap_is_safely_compensated(
    tmp_path: Path,
) -> None:
    plan, runner, component = _database_component(
        tmp_path,
        database_state="absent",
        fail_checked="wait",
    )
    with pytest.raises(
        RuntimeError,
        match="protected staging capacity database bootstrap job failed",
    ):
        component.apply(plan)

    runner.fail_checked = None
    runner.calls.clear()
    runner.checked_inputs.clear()
    runner.events.clear()

    assert component.classify(plan).state is ComponentState.READY
    component.apply(plan)

    mutations = [
        next(item for item in ("delete", "create", "wait") if item in call)
        for call in runner.calls
        if "create" in call
        or "delete" in call
        or ("wait" in call and "--for=condition=complete" in call)
    ]
    assert mutations == ["create", "wait", "delete", "delete"]
    assert runner.events.index("cleanup") < runner.events.index("arm")
    assert runner.objects == {}
    assert component.classify(plan).state is ComponentState.EXACT


def test_database_component_rejects_mismatched_partial_bootstrap_resource_set(
    tmp_path: Path,
) -> None:
    plan, runner, component = _database_component(tmp_path, database_state="absent")
    runner.objects = {
        "Secret": {
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {
                "name": "loom-staging-capacity-database-bootstrap",
                "namespace": "loom-staging",
            },
            "data": {"unexpected": "dmFsdWU="},
        }
    }

    assert component.classify(plan).state is ComponentState.DRIFTED


def test_database_component_rejects_immutable_database_identity_drift(
    tmp_path: Path,
) -> None:
    plan, _runner, component = _database_component(tmp_path, database_state="drifted")

    assert component.classify(plan).state is ComponentState.DRIFTED
    with pytest.raises(RuntimeError, match="state changed"):
        component.apply(plan)
