from __future__ import annotations

import json
from collections.abc import Mapping, Sequence

import pytest

from loom_cli.rollout.operator.manifest_ownership_epoch import (
    ManifestOwnershipEpochClaimer,
)


class _Runner:
    def __init__(self, *, stale: bool = False) -> None:
        self.stale = stale
        self.calls: list[tuple[tuple[str, ...], Mapping[str, str], float]] = []

    def capture_stdout(
        self,
        argv: Sequence[str],
        *,
        env: Mapping[str, str],
        timeout_seconds: float,
    ) -> bytes:
        command = tuple(argv)
        self.calls.append((command, dict(env), timeout_seconds))
        request_id = next(
            item.removeprefix("request_id=") for item in command if item.startswith("request_id=")
        )
        evidence = next(
            item.removeprefix("evidence_sha256=")
            for item in command
            if item.startswith("evidence_sha256=")
        )
        epoch = next(
            int(item.removeprefix("expected_epoch="))
            for item in command
            if item.startswith("expected_epoch=")
        )
        return json.dumps(
            {
                "environment": "staging",
                "namespace": "loom-staging",
                "epoch": epoch if self.stale else epoch + 1,
                "mutation_class": "rollout_apply",
                "request_id": request_id,
                "evidence_sha256": evidence,
            }
        ).encode()


def test_exact_epoch_claim_uses_cas_and_records_event() -> None:
    runner = _Runner()
    claimer = ManifestOwnershipEpochClaimer(runner=runner, environment={"KUBECONFIG": "/k"})
    assert claimer(2, "req-manifest-ownership-12345678", "a" * 64) == 3
    command, environment, timeout = runner.calls[0]
    sql = next(item for item in command if "UPDATE staging_mutation_epochs" in item)
    assert "INSERT INTO staging_mutation_epoch_events" in sql
    assert environment == {"KUBECONFIG": "/k"}
    assert timeout == 30.0


def test_epoch_claim_rejects_stale_or_unbounded_identity() -> None:
    claimer = ManifestOwnershipEpochClaimer(runner=_Runner(stale=True), environment={})
    with pytest.raises(RuntimeError, match="stale"):
        claimer(2, "req-manifest-ownership-12345678", "a" * 64)
    with pytest.raises(ValueError, match="authority"):
        claimer(-1, "req-manifest-ownership-12345678", "a" * 64)
