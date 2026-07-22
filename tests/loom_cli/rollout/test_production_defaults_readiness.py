from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

import pytest

from loom_cli.rollout.preflight_contract import CheckContext, CheckOperation
from loom_cli.rollout.preflight_registered_checks import (
    build_production_defaults_plan_check,
)
from loom_cli.rollout.production_defaults_readiness import (
    ProductionDefaultsArtifact,
    build_production_defaults_artifact,
)


def _profile(tmp_path: Path) -> Path:
    path = tmp_path / "staging.toml"
    path.write_text(
        """
environment = "staging"

[rate_card_sync.yibuapi]
enabled = true
group = "default"
source_url = "https://rates.example.invalid/catalog.json"

[[hosted_provider_pricing_defaults]]
name = "z-provider"
pricing_source = "tokens-only"
required = false

[[hosted_provider_pricing_defaults]]
name = "a-provider"
pricing_source = "rate-card"
rate_card_provider = "yibuapi"
required = true
""".strip()
        + "\n",
        encoding="utf-8",
    )
    return path


def test_production_defaults_artifact_is_canonical_and_round_trips(tmp_path: Path) -> None:
    artifact = build_production_defaults_artifact(
        _profile(tmp_path),
        candidate_sha="a" * 40,
        candidate_tree="b" * 40,
        image_tag="staging-aaaaaaa",
        environment="staging",
    )

    assert [provider.name for provider in artifact.providers] == ["a-provider", "z-provider"]
    assert artifact.yibuapi_sync == {
        "group": "default",
        "source_url": "https://rates.example.invalid/catalog.json",
    }
    assert ProductionDefaultsArtifact.from_bytes(artifact.to_bytes()) == artifact


def test_production_defaults_artifact_rejects_tamper_and_duplicate_names(tmp_path: Path) -> None:
    artifact = build_production_defaults_artifact(
        _profile(tmp_path),
        candidate_sha="a" * 40,
        candidate_tree="b" * 40,
        image_tag="staging-aaaaaaa",
        environment="staging",
    )
    payload = json.loads(artifact.to_bytes())
    payload["providers"][0]["pricing_source"] = "tokens-only"

    with pytest.raises(ValueError, match="digest drifted"):
        ProductionDefaultsArtifact.from_bytes(json.dumps(payload).encode())
    with pytest.raises(ValueError, match="identity is invalid"):
        replace(artifact, providers=(artifact.providers[0], artifact.providers[0]))


def test_production_defaults_materializes_default_sync_url(tmp_path: Path) -> None:
    profile = _profile(tmp_path)
    profile.write_text(
        profile.read_text().replace(
            'source_url = "https://rates.example.invalid/catalog.json"\n', ""
        )
    )

    artifact = build_production_defaults_artifact(
        profile,
        candidate_sha="a" * 40,
        candidate_tree="b" * 40,
        image_tag="staging-aaaaaaa",
        environment="staging",
    )

    assert artifact.yibuapi_sync == {
        "group": "default",
        "source_url": "https://yibuapi.com/api/pricing",
    }


def test_production_defaults_plan_check_binds_exact_candidate(tmp_path: Path) -> None:
    captured: list[ProductionDefaultsArtifact] = []
    check = build_production_defaults_plan_check(
        profile_path=_profile(tmp_path),
        candidate_sha="a" * 40,
        candidate_tree="b" * 40,
        image_tag="staging-aaaaaaa",
        environment="staging",
        artifact_sink=captured.append,
    )
    context = CheckContext(
        {
            "candidate.sha": "a" * 40,
            "candidate.tree": "b" * 40,
            "environment": "staging",
        }
    )

    outcome = check.operations[CheckOperation.PROBE](context)

    assert outcome.passed
    assert outcome.evidence["artifact-digest"] == captured[0].artifact_digest
    assert outcome.evidence["provider-count"] == 2
    assert check.spec.tier == 1
