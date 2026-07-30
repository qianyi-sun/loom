from __future__ import annotations

import json
import tomllib
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from scripts.ops import developer_sandbox_capacity_promotion as promotion
from scripts.ops import developer_sandbox_live_acceptance as live_acceptance
from scripts.ops import developer_sandbox_platform_health_authority as health_authority


def _profile_text() -> str:
    return promotion.STAGING_PROFILE.read_text(encoding="utf-8")


def _evidence() -> promotion.PromotionEvidence:
    values, policy_sha256 = promotion._policy_contract()
    return promotion.PromotionEvidence(
        session_id="1" * 32,
        evidence_path=(
            "/var/lib/loom-developer-sandbox-platform-health-authority/"
            f"sessions/{'1' * 32}/evidence.json"
        ),
        payload_sha256="2" * 64,
        candidates_sha256="3" * 64,
        expires_at="2026-07-29T12:00:00Z",
        policy_sha256=policy_sha256,
        recommendation_sha256="4" * 64,
        values=values,
    )


def _authority_payload(
    *,
    now: datetime,
    payload_sha256: str = "2" * 64,
) -> tuple[dict[str, Any], dict[str, Any]]:
    candidates = {
        "qianyi": {"sha": "a" * 40, "tree": "1" * 40},
        "hongjian": {"sha": "b" * 40, "tree": "2" * 40},
        "devansh": {"sha": "c" * 40, "tree": "3" * 40},
    }
    values, policy_sha256 = promotion._policy_contract()
    capacity = {
        **values,
        "minimum_node_cpu_cores": 32,
        "minimum_node_memory_bytes": 128 * 1024**3,
        "reserved_cpu_cores_per_node": 4,
        "reserved_memory_mib_per_node": 16384,
    }
    recommendation = {
        "schema_version": 1,
        "pool": "oldlab",
        "source": promotion.POLICY_SOURCE,
        "source_sha256": policy_sha256,
        "values": capacity,
        "derivation": {"all_nodes_passed": True},
    }
    session_id = "1" * 32
    evidence_path = promotion.AUTHORITY_ROOT / "sessions" / session_id / "evidence.json"
    evidence = {
        "session_id": session_id,
        "registry_snapshot": {"payload_sha256": "5" * 64},
        "candidates": candidates,
        "completed_at": (now - timedelta(minutes=1)).isoformat().replace("+00:00", "Z"),
        "expires_at": (now + timedelta(minutes=14)).isoformat().replace("+00:00", "Z"),
        "policy_capacity": {"oldlab": capacity},
        "oldlab_capacity_recommendation": recommendation,
        "payload_sha256": payload_sha256,
    }
    current = {
        "schema_version": 1,
        "session_id": session_id,
        "evidence_path": str(evidence_path),
        "payload_sha256": payload_sha256,
    }
    return current, evidence


def test_checked_in_profile_is_exact_disabled_fail_closed() -> None:
    profile = tomllib.loads(_profile_text())

    assert promotion.check_profile(profile) == "disabled_fail_closed"


def test_disabled_profile_rejects_nonempty_authority_binding() -> None:
    profile = tomllib.loads(_profile_text())
    profile["oldlab_capacity_promotion"]["evidence_session_id"] = "1" * 32

    with pytest.raises(promotion.PromotionError, match="fixed fail-closed"):
        promotion.check_profile(profile)


def test_render_rejects_a_noncanonical_disabled_prerequisite_base() -> None:
    drifted = _profile_text().replace(
        'pools = ["gb10"]',
        'pools = ["gb10", "oldlab"]',
        1,
    )

    with pytest.raises(promotion.PromotionError, match="disabled external-allocation"):
        promotion.render_profile_text(drifted, _evidence())


def test_render_is_deterministic_and_exact_evidence_bound() -> None:
    current = _profile_text()
    evidence = _evidence()

    first = promotion.render_profile_text(current, evidence)
    second = promotion.render_profile_text(current, evidence)
    parsed = tomllib.loads(first)

    assert first == second
    assert "PROVISIONAL placeholders" not in first
    assert "GATED enabled=false/active=false" not in first
    assert promotion.check_profile(parsed) == "enabled_evidence_bound"
    binding = parsed["oldlab_capacity_promotion"]
    assert binding["evidence_path"] == evidence.evidence_path
    assert binding["evidence_payload_sha256"] == evidence.payload_sha256
    assert binding["evidence_candidates_sha256"] == evidence.candidates_sha256
    assert binding["policy_source_sha256"] == evidence.policy_sha256
    assert binding["recommendation_sha256"] == evidence.recommendation_sha256
    prerequisites = parsed["external_slurm_runner_prerequisites"]
    assert prerequisites["pools"] == ["gb10", "oldlab"]
    assert prerequisites["env_template_glob"] == promotion.PROMOTED_ENV_TEMPLATE_GLOB
    oldlab = next(
        row for row in parsed["worker_pool_autoscaler_policies"] if row["pool_name"] == "oldlab"
    )
    assert oldlab["enabled"] is True
    assert "disabled_reason" not in oldlab
    assert oldlab["actuator_config"]["shared_capacity_managed"] is True
    assert oldlab["actuator_config"]["env_file"] == promotion.PROMOTED_OLDLAB_ENV_FILE
    supervisor = next(
        row
        for row in parsed["external_slurm_autoscaler_supervisors"]
        if row["pool_name"] == "oldlab"
    )
    assert supervisor["enabled"] is True
    assert supervisor["active"] is True


def test_enabled_profile_rejects_any_repository_binding_drift() -> None:
    evidence = _evidence()
    parsed = tomllib.loads(
        promotion.render_profile_text(_profile_text(), evidence),
    )
    parsed["oldlab_capacity_promotion"]["evidence_path"] = (
        "/var/lib/loom-developer-sandbox-platform-health-authority/"
        f"sessions/{'9' * 32}/evidence.json"
    )

    with pytest.raises(promotion.PromotionError, match="provenance binding"):
        promotion.check_profile(parsed)


def test_enabled_check_is_offline_and_accepts_historical_expiry(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    evidence = _evidence()
    parsed = tomllib.loads(
        promotion.render_profile_text(_profile_text(), evidence),
    )
    monkeypatch.setattr(
        promotion,
        "_load_promotion_evidence",
        lambda **_kwargs: pytest.fail("offline check must not read live root evidence"),
    )

    assert promotion.check_profile(parsed) == "enabled_evidence_bound"


def test_load_evidence_closes_pointer_candidate_policy_and_freshness(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 7, 29, 10, 0, tzinfo=UTC)
    current, evidence = _authority_payload(now=now)

    def secure_json(path: Path, *, label: str) -> tuple[dict[str, Any], bytes]:
        del label
        payload = current if path == promotion.AUTHORITY_CURRENT else evidence
        return payload, promotion._canonical(payload)

    monkeypatch.setattr(health_authority, "_secure_json", secure_json)
    monkeypatch.setattr(
        live_acceptance,
        "_validate_platform_health_authority",
        lambda *_args, **_kwargs: None,
    )

    result = promotion._load_promotion_evidence(now=now)

    assert result.session_id == current["session_id"]
    assert result.payload_sha256 == evidence["payload_sha256"]
    assert result.candidates_sha256 == promotion._digest(evidence["candidates"])
    assert result.recommendation_sha256 == promotion._digest(
        evidence["oldlab_capacity_recommendation"],
    )


def test_candidate_validation_is_dynamic_and_requires_an_isolation_cohort() -> None:
    candidates = {
        "denv-z": {"sha": "d" * 40, "tree": "4" * 40},
        "denv-a": {"sha": "a" * 40, "tree": "1" * 40},
        "denv-b": {"sha": "b" * 40, "tree": "2" * 40},
        "denv-c": {"sha": "c" * 40, "tree": "3" * 40},
    }

    assert promotion._validate_candidates(candidates) == promotion._digest(candidates)
    with pytest.raises(promotion.PromotionError, match="candidate set is invalid"):
        promotion._validate_candidates(
            {"denv-only": {"sha": "a" * 40, "tree": "1" * 40}},
        )


def test_load_evidence_rejects_stale_receipt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    then = datetime(2026, 7, 29, 10, 0, tzinfo=UTC)
    current, evidence = _authority_payload(now=then)

    def secure_json(path: Path, *, label: str) -> tuple[dict[str, Any], bytes]:
        del label
        payload = current if path == promotion.AUTHORITY_CURRENT else evidence
        return payload, promotion._canonical(payload)

    monkeypatch.setattr(health_authority, "_secure_json", secure_json)
    monkeypatch.setattr(
        live_acceptance,
        "_validate_platform_health_authority",
        lambda *_args, **_kwargs: None,
    )

    with pytest.raises(promotion.PromotionError, match="not currently fresh"):
        promotion._load_promotion_evidence(now=then + timedelta(hours=1))


def test_load_evidence_rejects_pointer_digest_drift(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime(2026, 7, 29, 10, 0, tzinfo=UTC)
    current, evidence = _authority_payload(now=now)
    current["payload_sha256"] = "9" * 64

    def secure_json(path: Path, *, label: str) -> tuple[dict[str, Any], bytes]:
        del label
        payload = current if path == promotion.AUTHORITY_CURRENT else evidence
        return payload, promotion._canonical(payload)

    monkeypatch.setattr(health_authority, "_secure_json", secure_json)
    monkeypatch.setattr(
        live_acceptance,
        "_validate_platform_health_authority",
        lambda *_args, **_kwargs: None,
    )

    with pytest.raises(promotion.PromotionError, match="digest drifted"):
        promotion._load_promotion_evidence(now=now)


def test_cli_has_no_path_or_value_override_flags() -> None:
    parser = promotion._parser()

    with pytest.raises(SystemExit):
        parser.parse_args(["render", "--evidence", "/tmp/untrusted.json"])
    with pytest.raises(SystemExit):
        parser.parse_args(["render", "--enabled", "true"])


def test_check_output_is_secret_free_and_read_only(capsys: pytest.CaptureFixture[str]) -> None:
    assert promotion.main(["check"]) == 0

    output = json.loads(capsys.readouterr().out)
    assert output["status"] == "disabled_fail_closed"
    assert output["live_mutations_supported"] is False


def test_render_main_only_prints_unified_diff(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    before = promotion.STAGING_PROFILE.read_bytes()
    text = before.decode("utf-8")
    monkeypatch.setattr(promotion, "_profile", lambda: (tomllib.loads(text), text))
    monkeypatch.setattr(promotion, "_load_promotion_evidence", lambda: _evidence())

    assert promotion.main(["render"]) == 0

    output = capsys.readouterr().out
    assert output.startswith("--- a/deploy/environment-state/staging.toml\n")
    assert "+++ b/deploy/environment-state/staging.toml\n" in output
    assert "+enabled = true\n" in output
    assert promotion.STAGING_PROFILE.read_bytes() == before
