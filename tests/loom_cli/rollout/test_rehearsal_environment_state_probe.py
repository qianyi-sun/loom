from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from loom_cli.rollout.rehearsal_environment_state_probe import (
    RehearsalEnvironmentStateError,
    run_probe,
)

REPO_ROOT = Path(__file__).resolve().parents[3]


def _profile() -> bytes:
    return (REPO_ROOT / "deploy/environment-state/staging.toml").read_bytes()


def _arguments(profile: bytes) -> dict[str, object]:
    return {
        "profile_bytes": profile,
        "database_url": (
            "postgresql+psycopg://loom_rehearsal@127.0.0.1:5432/loom_rehearsal_" + "5" * 24
        ),
        "expected_database": "loom_rehearsal_" + "5" * 24,
        "plan_sha256": "c" * 64,
        "expected_profile_sha256": hashlib.sha256(profile).hexdigest(),
        "expected_candidate_sha": "a" * 40,
        "expected_candidate_tree": "b" * 40,
        "expected_image_tag": "staging-aaaaaaaa",
    }


def test_probe_converges_exact_candidate_policies_with_redacted_evidence() -> None:
    profile = _profile()
    calls: list[tuple[str, list[dict[str, object]]]] = []

    def apply(database_url: str, policies: list[dict[str, object]]):
        calls.append((database_url, policies))
        return [dict(policy) for policy in policies]

    result = run_probe(**_arguments(profile), apply_policies=apply)

    assert result["status"] == "ready"
    assert result["policy_count"] == 2
    assert result["profile_sha256"] == hashlib.sha256(profile).hexdigest()
    assert len(calls) == 1
    assert [policy["pool_name"] for policy in calls[0][1]] == ["gb10", "oldlab"]
    assert calls[0][1][0]["actuator_config"]["slurm_cluster_name"] == "trt-gb10"
    assert calls[0][1][1]["actuator_config"]["slurm_cluster_name"] == "trt-oldlab"
    serialized = json.dumps(result, sort_keys=True)
    assert "postgresql" not in serialized
    assert "/shared_work" not in serialized


@pytest.mark.parametrize(
    "replacement,error",
    [
        ({"expected_profile_sha256": "0" * 64}, "profile identity drifted"),
        (
            {
                "database_url": (
                    "postgresql+psycopg://loom_rehearsal@loom-postgres-rw:5432/"
                    "loom_rehearsal_" + "5" * 24
                )
            },
            "database authority is invalid",
        ),
    ],
)
def test_probe_rejects_identity_or_database_authority_before_mutation(
    replacement: dict[str, object],
    error: str,
) -> None:
    profile = _profile()
    arguments = _arguments(profile)
    arguments.update(replacement)
    calls: list[object] = []

    with pytest.raises(RehearsalEnvironmentStateError, match=error):
        run_probe(
            **arguments,
            apply_policies=lambda *_args: calls.append(_args),
        )

    assert calls == []


def test_probe_rejects_policy_readback_drift() -> None:
    profile = _profile()

    def apply(_database_url: str, policies: list[dict[str, object]]):
        observed = [dict(policy) for policy in policies]
        observed[1] = {**observed[1], "min_slots": 24}
        return observed

    with pytest.raises(RehearsalEnvironmentStateError, match="did not converge exactly"):
        run_probe(**_arguments(profile), apply_policies=apply)
