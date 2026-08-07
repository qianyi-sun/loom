"""Exact immutable migration Job component for protected staging apply."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

import yaml  # type: ignore[import-untyped]

from loom_cli.rollout.credential_authority import read_trusted_file

from .final_gate_plan import FinalGatePlan
from .protected_apply_journal import (
    ComponentObservation,
    ComponentState,
    ProtectedApplyComponent,
)

_REVISION_RE = re.compile(r"^[0-9]{4}(?:_[a-z0-9_]+)?$")
_MAX_MANIFEST_BYTES = 1024 * 1024
_QUERY_TIMEOUT_SECONDS = 30.0
_APPLY_TIMEOUT_SECONDS = 60.0
_WAIT_TIMEOUT_SECONDS = 660.0
_READ_REVISION_SQL = "SELECT version_num FROM alembic_version;"
_IMPLEMENTATION_DIGEST = hashlib.sha256(
    json.dumps(
        {
            "apply": ["kubectl", "apply", "--validate=strict", "-f", "-"],
            "read_revision_sql": _READ_REVISION_SQL,
            "version": "v1",
            "wait": ["kubectl", "wait", "--for=condition=complete", "--timeout=600s"],
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
).hexdigest()


class ProtectedMigrationCommandRunner(Protocol):
    def capture_stdout(
        self,
        argv: Sequence[str],
        *,
        env: Mapping[str, str],
        timeout_seconds: float,
    ) -> bytes: ...

    def run_checked(
        self,
        argv: Sequence[str],
        *,
        env: Mapping[str, str],
        input_payload: bytes | None,
        timeout_seconds: float,
    ) -> None: ...


@dataclass(frozen=True, slots=True)
class KubernetesProtectedMigrationComponent:
    """Converge the attested schema with the preflight-published Job only."""

    runner: ProtectedMigrationCommandRunner
    environment: Mapping[str, str]
    service_uid: int

    def __post_init__(self) -> None:
        if self.service_uid < 0 or "KUBECONFIG" not in self.environment:
            raise ValueError("protected migration command authority is invalid")

    def component(self, plan: FinalGatePlan) -> ProtectedApplyComponent:
        return ProtectedApplyComponent(
            component_id="database-migration",
            implementation_digest=_IMPLEMENTATION_DIGEST,
            input_fingerprint=_hash_json(
                {
                    "candidate_sha": plan.candidate_sha,
                    "candidate_tree": plan.candidate_tree,
                    "manifest_artifact_sha256": plan.migration_manifest_artifact_sha256,
                    "manifest_sha256": plan.migration_manifest_sha256,
                    "migration_plan_digest": plan.migration_plan_digest,
                    "schema_revision": plan.schema_revision,
                    "target_revision": plan.migration_target_revision,
                }
            ),
            classify=self.classify,
            apply=self.apply,
        )

    def classify(self, plan: FinalGatePlan) -> ComponentObservation:
        revision = self._read_revision()
        if revision == plan.migration_target_revision:
            state = ComponentState.EXACT
        elif revision == plan.schema_revision:
            state = ComponentState.READY
        else:
            state = ComponentState.DRIFTED
        return ComponentObservation(
            state=state,
            evidence_digest=_hash_json(
                {
                    "manifest_sha256": plan.migration_manifest_sha256,
                    "migration_plan_digest": plan.migration_plan_digest,
                    "revision": revision,
                    "target_revision": plan.migration_target_revision,
                }
            ),
            observed_epoch=_stable_observed_epoch(plan),
        )

    def apply(self, plan: FinalGatePlan) -> None:
        payload = self._read_manifest(plan)
        if self._read_revision() != plan.schema_revision:
            raise RuntimeError("protected migration schema changed before apply")
        self.runner.run_checked(
            (
                "kubectl",
                "--namespace",
                plan.namespace,
                "apply",
                "--validate=strict",
                "--request-timeout=30s",
                "-f",
                "-",
            ),
            env=self.environment,
            input_payload=payload,
            timeout_seconds=_APPLY_TIMEOUT_SECONDS,
        )
        self.runner.run_checked(
            (
                "kubectl",
                "--namespace",
                plan.namespace,
                "wait",
                "--for=condition=complete",
                "--timeout=600s",
                f"job/{plan.migration_job_name}",
            ),
            env=self.environment,
            input_payload=None,
            timeout_seconds=_WAIT_TIMEOUT_SECONDS,
        )

    def _read_revision(self) -> str:
        payload = self.runner.capture_stdout(
            (
                "kubectl",
                "--namespace",
                "loom-staging",
                "exec",
                "service/loom-postgres-rw",
                "--",
                "sh",
                "-ceu",
                'exec psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -AtX '
                '-v ON_ERROR_STOP=1 -c "$1"',
                "sh",
                _READ_REVISION_SQL,
            ),
            env=self.environment,
            timeout_seconds=_QUERY_TIMEOUT_SECONDS,
        )
        try:
            revision = payload.decode("utf-8").strip()
        except UnicodeDecodeError as exc:
            raise ValueError("protected migration revision is not UTF-8") from exc
        if _REVISION_RE.fullmatch(revision) is None:
            raise ValueError("protected migration revision is invalid or ambiguous")
        return revision

    def _read_manifest(self, plan: FinalGatePlan) -> bytes:
        path = Path(plan.migration_manifest_path)
        trusted = read_trusted_file(
            path,
            service_uid=self.service_uid,
            private=True,
            max_bytes=_MAX_MANIFEST_BYTES,
            require_nonempty=True,
        )
        payload = trusted.payload
        if hashlib.sha256(payload).hexdigest() != plan.migration_manifest_sha256:
            raise ValueError("protected migration manifest content drifted")
        _verify_job(payload, plan)
        return payload


def _verify_job(payload: bytes, plan: FinalGatePlan) -> None:
    try:
        documents = [value for value in yaml.safe_load_all(payload) if value is not None]
    except (UnicodeDecodeError, yaml.YAMLError) as exc:
        raise ValueError("protected migration manifest is invalid") from exc
    if len(documents) != 1 or not isinstance(documents[0], dict):
        raise ValueError("protected migration manifest resource set drifted")
    job = documents[0]
    metadata = job.get("metadata")
    spec = job.get("spec")
    template = spec.get("template") if isinstance(spec, dict) else None
    pod_spec = template.get("spec") if isinstance(template, dict) else None
    containers = pod_spec.get("containers") if isinstance(pod_spec, dict) else None
    container = containers[0] if isinstance(containers, list) and len(containers) == 1 else None
    labels = metadata.get("labels") if isinstance(metadata, dict) else None
    image_tag = labels.get("loom.image-tag") if isinstance(labels, dict) else None
    if (
        job.get("apiVersion") != "batch/v1"
        or job.get("kind") != "Job"
        or not isinstance(metadata, dict)
        or metadata.get("name") != plan.migration_job_name
        or metadata.get("namespace") != plan.namespace
        or not isinstance(labels, dict)
        or labels.get("app") != "loom-migration"
        or not isinstance(image_tag, str)
        or not image_tag.startswith("staging-")
        or not isinstance(container, dict)
        or container.get("name") != "migrate"
        or container.get("command")
        != ["alembic", "-c", "migrations/alembic.ini", "upgrade", "head"]
        or container.get("image") != f"loom-control-plane:{image_tag}"
    ):
        raise ValueError("protected migration Job identity drifted")


def _stable_observed_epoch(plan: FinalGatePlan) -> int:
    current = int(plan.schema_revision[:4])
    target = int(plan.migration_target_revision[:4])
    if current < 68 <= target and plan.starting_mutation_epoch == 0:
        return 0
    return plan.starting_mutation_epoch + 1


def _hash_json(value: Mapping[str, object]) -> str:
    return hashlib.sha256(
        json.dumps(dict(value), sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


__all__ = [
    "KubernetesProtectedMigrationComponent",
    "ProtectedMigrationCommandRunner",
]
