from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path

import pytest

from loom_cli.rollout.production_defaults_readiness import (
    ProductionDefaultsArtifact,
    ProviderPricingDefault,
)
from loom_cli.rollout.rehearsal_production_defaults_probe import (
    RehearsalProductionDefaultsError,
    run_probe,
)


def _artifact() -> ProductionDefaultsArtifact:
    provider = ProviderPricingDefault(
        name="hosted-openai",
        pricing_source="tokens-only",
        rate_card_provider=None,
        required=True,
    )
    payload = {
        "schema_version": 1,
        "candidate_sha": "a" * 40,
        "candidate_tree": "b" * 40,
        "environment": "staging",
        "yibuapi_sync": None,
        "providers": [provider.to_dict()],
    }
    digest = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return ProductionDefaultsArtifact(
        schema_version=1,
        candidate_sha="a" * 40,
        candidate_tree="b" * 40,
        environment="staging",
        yibuapi_sync=None,
        providers=(provider,),
        artifact_digest=digest,
    )


def _secret(tmp_path: Path) -> Path:
    path = tmp_path / "secrets.toml"
    path.write_text('[admin]\ntoken = "loom_admin_' + "s" * 40 + '"\n', encoding="utf-8")
    path.chmod(0o440)
    return path


def test_probe_converges_cloned_state_with_shared_plan_and_redacted_evidence(
    tmp_path: Path,
) -> None:
    artifact = _artifact()
    providers = [
        {
            "id": "11111111-1111-4111-8111-111111111111",
            "name": "hosted-openai",
            "pricing_source": "rate-card",
            "rate_card_provider": "yibuapi",
        }
    ]
    calls: list[tuple[str, str, dict[str, object] | None]] = []

    def request(method, path, token, payload):
        assert token.startswith("loom_admin_")
        calls.append((method, path, None if payload is None else dict(payload)))
        if method == "GET":
            return {"items": [dict(item) for item in providers]}
        assert method == "PATCH"
        providers[0].update(dict(payload or {}))
        providers[0]["rate_card_provider"] = None
        return dict(providers[0])

    result = run_probe(
        artifact_bytes=artifact.to_bytes(),
        plan_sha256="c" * 64,
        expected_artifact_sha256=artifact.artifact_digest,
        expected_candidate_sha=artifact.candidate_sha,
        expected_candidate_tree=artifact.candidate_tree,
        request=request,
        read_rate_cards=lambda: [],
        admin_secret_path=_secret(tmp_path),
        expected_owner_uid=os.geteuid(),
        allowed_group_gid=os.getegid(),
    )

    assert result["status"] == "ready"
    assert result["mutation_count"] == 1
    assert calls == [
        ("GET", "/api/v1/provider-connections", None),
        (
            "PATCH",
            "/api/v1/provider-connections/11111111-1111-4111-8111-111111111111",
            {"pricing_source": "tokens-only"},
        ),
        ("GET", "/api/v1/provider-connections", None),
    ]
    serialized = json.dumps(result, sort_keys=True)
    assert "loom_admin_" not in serialized
    assert "hosted-openai" not in serialized


def test_probe_rejects_artifact_or_inventory_drift_before_mutation(tmp_path: Path) -> None:
    artifact = _artifact()
    secret = _secret(tmp_path)
    mutations: list[str] = []

    def request(method, path, _token, _payload):
        if method != "GET":
            mutations.append(path)
        return {"items": []}

    with pytest.raises(RehearsalProductionDefaultsError, match="inventory drifted"):
        run_probe(
            artifact_bytes=artifact.to_bytes(),
            plan_sha256="c" * 64,
            expected_artifact_sha256=artifact.artifact_digest,
            expected_candidate_sha=artifact.candidate_sha,
            expected_candidate_tree=artifact.candidate_tree,
            request=request,
            read_rate_cards=lambda: [],
            admin_secret_path=secret,
            expected_owner_uid=os.geteuid(),
            allowed_group_gid=os.getegid(),
        )
    assert not mutations

    with pytest.raises(RehearsalProductionDefaultsError, match="artifact is invalid"):
        run_probe(
            artifact_bytes=artifact.to_bytes().replace(b"hosted-openai", b"hosted-other"),
            plan_sha256="c" * 64,
            expected_artifact_sha256=artifact.artifact_digest,
            expected_candidate_sha=artifact.candidate_sha,
            expected_candidate_tree=artifact.candidate_tree,
            request=request,
            read_rate_cards=lambda: [],
            admin_secret_path=secret,
            expected_owner_uid=os.geteuid(),
            allowed_group_gid=os.getegid(),
        )
