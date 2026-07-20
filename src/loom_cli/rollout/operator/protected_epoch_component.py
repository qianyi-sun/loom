"""Exact PostgreSQL compare-and-swap component for protected rollout epochs."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Protocol

from .final_gate_plan import FinalGatePlan
from .postgres_sql import single_line_sql
from .protected_apply_journal import (
    ComponentObservation,
    ComponentState,
    ProtectedApplyComponent,
)

_TIMEOUT_SECONDS = 30.0
_REVISION_RE = re.compile(r"^(?P<number>[0-9]{4})(?:_[a-z0-9_]+)?$")
_READ_SQL = single_line_sql("""
SELECT jsonb_build_object(
  'environment', environment,
  'namespace', namespace,
  'epoch', epoch,
  'mutation_class', reason,
  'request_id', request_id,
  'evidence_sha256', evidence_sha256
)::text
FROM staging_mutation_epochs
WHERE environment = 'staging' AND namespace = 'loom-staging';
""")
_ADVANCE_SQL = single_line_sql("""
WITH bootstrapped AS (
  INSERT INTO staging_mutation_epochs
    (environment, namespace, epoch, reason)
  SELECT 'staging', 'loom-staging', 0, 'bootstrap'
  WHERE :'allow_bootstrap'::boolean
  ON CONFLICT (environment) DO NOTHING
  RETURNING epoch
), advanced AS (
  UPDATE staging_mutation_epochs
  SET epoch = epoch + 1,
      reason = 'rollout_apply',
      request_id = :'request_id',
      evidence_sha256 = :'evidence_sha256',
      updated_at = clock_timestamp()
  WHERE environment = 'staging'
    AND namespace = 'loom-staging'
    AND epoch = :'expected_epoch'::bigint
  RETURNING environment, namespace, epoch, reason, request_id,
            evidence_sha256, updated_at
), recorded AS (
  INSERT INTO staging_mutation_epoch_events
    (environment, namespace, epoch, mutation_class, request_id,
     evidence_sha256, occurred_at)
  SELECT environment, namespace, epoch, reason, request_id,
         evidence_sha256, updated_at
  FROM advanced
  RETURNING epoch
)
SELECT jsonb_build_object(
  'environment', advanced.environment,
  'namespace', advanced.namespace,
  'epoch', advanced.epoch,
  'mutation_class', advanced.reason,
  'request_id', advanced.request_id,
  'evidence_sha256', advanced.evidence_sha256
)::text
FROM advanced JOIN recorded USING (epoch);
""")
_IMPLEMENTATION_DIGEST = hashlib.sha256(
    json.dumps(
        {"advance_sql": _ADVANCE_SQL, "read_sql": _READ_SQL, "version": "v1"},
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
).hexdigest()


class ProtectedEpochCommandRunner(Protocol):
    def capture_stdout(
        self,
        argv: Sequence[str],
        *,
        env: Mapping[str, str],
        timeout_seconds: float,
    ) -> bytes: ...


@dataclass(frozen=True, slots=True)
class KubernetesProtectedEpochComponent:
    """Classify and claim the one epoch owned by an exact final plan."""

    runner: ProtectedEpochCommandRunner
    environment: Mapping[str, str]

    def component(self, plan: FinalGatePlan) -> ProtectedApplyComponent:
        return ProtectedApplyComponent(
            component_id="mutation-epoch-claim",
            implementation_digest=_IMPLEMENTATION_DIGEST,
            input_fingerprint=_hash_json(
                {
                    "environment": plan.environment,
                    "namespace": plan.namespace,
                    "plan_digest": plan.plan_digest,
                    "request_id": plan.request_id,
                    "starting_epoch": plan.starting_mutation_epoch,
                }
            ),
            classify=self.classify,
            apply=self.apply,
        )

    def classify(self, plan: FinalGatePlan) -> ComponentObservation:
        record = self._query((_READ_SQL,))
        if record is None:
            return ComponentObservation(
                state=(
                    ComponentState.READY
                    if requires_legacy_epoch_bootstrap(plan)
                    else ComponentState.DRIFTED
                ),
                evidence_digest=_hash_json({"status": "missing"}),
                observed_epoch=plan.starting_mutation_epoch,
            )
        epoch = record["epoch"]
        assert type(epoch) is int
        expected = _expected_identity(plan)
        if epoch == plan.starting_mutation_epoch:
            state = ComponentState.READY
        elif record == expected:
            state = ComponentState.EXACT
        else:
            state = ComponentState.DRIFTED
        return ComponentObservation(
            state=state,
            evidence_digest=_hash_json(record),
            observed_epoch=epoch,
        )

    def apply(self, plan: FinalGatePlan) -> None:
        record = self._query(
            (
                "-v",
                f"request_id={plan.request_id}",
                "-v",
                f"evidence_sha256={plan.plan_digest}",
                "-v",
                f"expected_epoch={plan.starting_mutation_epoch}",
                "-v",
                (
                    "allow_bootstrap=true"
                    if requires_legacy_epoch_bootstrap(plan)
                    else "allow_bootstrap=false"
                ),
                _ADVANCE_SQL,
            )
        )
        if record != _expected_identity(plan):
            raise RuntimeError("protected mutation epoch claim was stale or incomplete")

    def _query(self, arguments: Sequence[str]) -> dict[str, object] | None:
        sql = arguments[-1]
        variables = tuple(arguments[:-1])
        payload = self.runner.capture_stdout(
            [
                "kubectl",
                "-n",
                "loom-staging",
                "exec",
                "statefulset/loom-postgres",
                "--",
                "sh",
                "-ceu",
                'sql="$1"; shift; exec psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -AtX '
                '-v ON_ERROR_STOP=1 "$@" -c "$sql"',
                "sh",
                sql,
                *variables,
            ],
            env=self.environment,
            timeout_seconds=_TIMEOUT_SECONDS,
        )
        try:
            decoded = payload.decode("utf-8").strip()
            if not decoded:
                return None
            value = json.loads(decoded)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("protected mutation epoch query returned invalid JSON") from exc
        if (
            not isinstance(value, dict)
            or set(value)
            != {
                "environment",
                "namespace",
                "epoch",
                "mutation_class",
                "request_id",
                "evidence_sha256",
            }
            or value["environment"] != "staging"
            or value["namespace"] != "loom-staging"
            or type(value["epoch"]) is not int
            or int(value["epoch"]) < 0
            or not all(
                item is None or isinstance(item, str)
                for item in (
                    value["mutation_class"],
                    value["request_id"],
                    value["evidence_sha256"],
                )
            )
        ):
            raise ValueError("protected mutation epoch authority is incomplete")
        return value


def _expected_identity(plan: FinalGatePlan) -> dict[str, object]:
    return {
        "environment": plan.environment,
        "namespace": plan.namespace,
        "epoch": plan.starting_mutation_epoch + 1,
        "mutation_class": "rollout_apply",
        "request_id": plan.request_id,
        "evidence_sha256": plan.plan_digest,
    }


def requires_legacy_epoch_bootstrap(plan: FinalGatePlan) -> bool:
    """Return whether migration must create the first epoch authority row."""

    current = _REVISION_RE.fullmatch(plan.schema_revision)
    target = _REVISION_RE.fullmatch(plan.migration_target_revision)
    return bool(
        current is not None
        and target is not None
        and int(current.group("number")) < 66
        and int(target.group("number")) >= 66
        and plan.starting_mutation_epoch == 0
    )


def _hash_json(value: Mapping[str, object]) -> str:
    return hashlib.sha256(
        json.dumps(dict(value), sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


__all__ = [
    "KubernetesProtectedEpochComponent",
    "ProtectedEpochCommandRunner",
    "requires_legacy_epoch_bootstrap",
]
