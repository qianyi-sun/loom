"""Single-source SQL authority for protected staging mutation epochs."""

from __future__ import annotations

import hashlib
import json
import re

from .postgres_sql import single_line_sql

_REQUEST_RE = re.compile(r"^[a-z0-9][a-z0-9-]{7,79}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_REQUEST_TOKEN = "__REQUEST_ID__"
_EVIDENCE_TOKEN = "__EVIDENCE_SHA256__"
_EPOCH_TOKEN = "__EXPECTED_EPOCH__"
_BOOTSTRAP_TOKEN = "__ALLOW_BOOTSTRAP__"

READ_STAGING_EPOCH_SQL = single_line_sql("""
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

_ADVANCE_TEMPLATE = single_line_sql("""
WITH bootstrapped AS (
  INSERT INTO staging_mutation_epochs
    (environment, namespace, epoch, reason)
  SELECT 'staging', 'loom-staging', 0, 'bootstrap'
  WHERE __ALLOW_BOOTSTRAP__
  ON CONFLICT (environment) DO NOTHING
  RETURNING epoch
), advanced AS (
  UPDATE staging_mutation_epochs
  SET epoch = epoch + 1,
      reason = 'rollout_apply',
      request_id = '__REQUEST_ID__',
      evidence_sha256 = '__EVIDENCE_SHA256__',
      updated_at = clock_timestamp()
  WHERE environment = 'staging'
    AND namespace = 'loom-staging'
    AND epoch = __EXPECTED_EPOCH__::bigint
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


def render_staging_epoch_advance_sql(
    *,
    request_id: str,
    evidence_sha256: str,
    expected_epoch: int,
    allow_bootstrap: bool,
) -> str:
    """Bind validated literals into the one protected epoch CAS statement."""

    if (
        _REQUEST_RE.fullmatch(request_id) is None
        or _SHA256_RE.fullmatch(evidence_sha256) is None
        or type(expected_epoch) is not int
        or expected_epoch < 0
        or type(allow_bootstrap) is not bool
    ):
        raise ValueError("protected mutation epoch SQL authority is invalid")
    replacements = {
        _REQUEST_TOKEN: request_id,
        _EVIDENCE_TOKEN: evidence_sha256,
        _EPOCH_TOKEN: str(expected_epoch),
        _BOOTSTRAP_TOKEN: "true" if allow_bootstrap else "false",
    }
    statement = _ADVANCE_TEMPLATE
    for token, value in replacements.items():
        if statement.count(token) != 1:
            raise RuntimeError("protected mutation epoch SQL template drifted")
        statement = statement.replace(token, value)
    if any(token in statement for token in replacements) or ":'" in statement:
        raise RuntimeError("protected mutation epoch SQL binding is incomplete")
    return statement


STAGING_EPOCH_SQL_CONTRACT_DIGEST = hashlib.sha256(
    json.dumps(
        {
            "advance_template": _ADVANCE_TEMPLATE,
            "read": READ_STAGING_EPOCH_SQL,
            "version": "v1",
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
).hexdigest()


__all__ = [
    "READ_STAGING_EPOCH_SQL",
    "STAGING_EPOCH_SQL_CONTRACT_DIGEST",
    "render_staging_epoch_advance_sql",
]
