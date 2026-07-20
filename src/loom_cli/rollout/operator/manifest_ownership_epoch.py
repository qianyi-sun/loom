"""Exact compare-and-swap epoch authority for manifest ownership maintenance."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from dataclasses import dataclass

from .postgres_sql import single_line_sql
from .protected_epoch_component import ProtectedEpochCommandRunner

_REQUEST_RE = re.compile(r"^req-manifest-ownership-[a-z0-9]{8,32}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_TIMEOUT_SECONDS = 30.0
_ADVANCE_SQL = single_line_sql("""
WITH advanced AS (
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


@dataclass(frozen=True, slots=True)
class ManifestOwnershipEpochClaimer:
    runner: ProtectedEpochCommandRunner
    environment: Mapping[str, str]

    def __call__(self, expected_epoch: int, request_id: str, evidence_sha256: str) -> int:
        if (
            expected_epoch < 0
            or _REQUEST_RE.fullmatch(request_id) is None
            or _SHA256_RE.fullmatch(evidence_sha256) is None
        ):
            raise ValueError("manifest ownership epoch authority is invalid")
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
                'sql="$1"; shift; exec psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" '
                '-AtX -v ON_ERROR_STOP=1 "$@" -c "$sql"',
                "sh",
                _ADVANCE_SQL,
                "-v",
                f"request_id={request_id}",
                "-v",
                f"evidence_sha256={evidence_sha256}",
                "-v",
                f"expected_epoch={expected_epoch}",
            ],
            env=self.environment,
            timeout_seconds=_TIMEOUT_SECONDS,
        )
        try:
            value = json.loads(payload.decode("utf-8").strip())
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise RuntimeError("manifest ownership epoch claim returned invalid evidence") from exc
        expected = {
            "environment": "staging",
            "namespace": "loom-staging",
            "epoch": expected_epoch + 1,
            "mutation_class": "rollout_apply",
            "request_id": request_id,
            "evidence_sha256": evidence_sha256,
        }
        if value != expected:
            raise RuntimeError("manifest ownership epoch claim was stale or incomplete")
        return expected_epoch + 1


__all__ = ["ManifestOwnershipEpochClaimer"]
