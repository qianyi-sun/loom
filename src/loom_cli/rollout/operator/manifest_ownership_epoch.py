"""Exact compare-and-swap epoch authority for manifest ownership maintenance."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from dataclasses import dataclass

from .protected_epoch_component import ProtectedEpochCommandRunner
from .staging_epoch_sql import render_staging_epoch_advance_sql

_REQUEST_RE = re.compile(r"^req-manifest-ownership-[a-z0-9]{8,32}$")
_TIMEOUT_SECONDS = 30.0


@dataclass(frozen=True, slots=True)
class ManifestOwnershipEpochClaimer:
    runner: ProtectedEpochCommandRunner
    environment: Mapping[str, str]

    def __call__(self, expected_epoch: int, request_id: str, evidence_sha256: str) -> int:
        if _REQUEST_RE.fullmatch(request_id) is None:
            raise ValueError("manifest ownership epoch authority is invalid")
        statement = render_staging_epoch_advance_sql(
            request_id=request_id,
            evidence_sha256=evidence_sha256,
            expected_epoch=expected_epoch,
            allow_bootstrap=False,
        )
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
                statement,
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
