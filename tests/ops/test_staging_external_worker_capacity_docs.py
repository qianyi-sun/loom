from __future__ import annotations

import tomllib
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]


def _toml(path: str) -> dict[str, Any]:
    return tomllib.loads((ROOT / path).read_text(encoding="utf-8"))


def test_staging_profiles_use_external_worker_pools() -> None:
    for path in (
        "deploy/environments/staging.cluster.toml",
        "deploy/environments/staging.multinode.cluster.toml",
    ):
        profile = _toml(path)
        assert profile["k8s_worker"]["enabled"] is False


def test_staging_external_pool_ceilings_match_current_policy() -> None:
    profile = _toml("deploy/environment-state/staging.toml")
    policies = {
        item["pool_name"]: item for item in profile["worker_pool_autoscaler_policies"]
    }

    assert set(policies) == {"gb10", "oldlab"}
    assert policies["gb10"]["enabled"] is True
    assert policies["gb10"]["min_slots"] == 0
    assert policies["gb10"]["max_slots"] == 140
    assert policies["oldlab"]["enabled"] is True
    assert policies["oldlab"]["min_slots"] == 0
    assert policies["oldlab"]["max_slots"] == 18
    for policy in policies.values():
        assert policy["actuator_config"]["external_runner"] is True
        assert policy["actuator_config"]["exclusive"] is False


def test_staging_validation_uses_current_external_pool_evidence() -> None:
    runbook = (ROOT / "docs/runbooks/staging-launch.md").read_text(encoding="utf-8")

    assert "--required-worker-pool gb10" in runbook
    assert "external worker pools" in runbook
    assert "worker_capacity_manifest.py status" in runbook
