from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import Any

from scripts.ops.operator_free_user_e2e_gate import validate_evidence

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "scripts/ops/operator_free_user_e2e_gate.py"


def _step(
    command: str,
    api_route: str,
    *,
    surface: str = "cli",
    artifact_sha256: str | None = None,
) -> dict[str, Any]:
    step: dict[str, Any] = {
        "status": "pass",
        "surface": surface,
        "actor_role": "normal_user",
        "command": command,
        "api_route": api_route,
        "evidence": "redacted response excerpt recorded",
    }
    if artifact_sha256 is not None:
        step["expected_sha256"] = artifact_sha256
        step["observed_sha256"] = artifact_sha256
    return step


def _passing_evidence() -> dict[str, Any]:
    digest = "a" * 64
    return {
        "schema_version": 1,
        "issue": 493,
        "environment": {
            "name": "production",
            "route": "https://yylx.world/prod",
            "api_base": "https://yylx.world/prod/api",
            "user_role": "normal_user",
            "token_source": "env:LOOM_PROD_USER_E2E_TOKEN",
        },
        "prod_dev_separation": {
            "production_route": "https://yylx.world/prod",
            "development_route": "https://yylx.world/dev",
            "production_api_base": "https://yylx.world/prod/api",
            "development_api_base": "https://yylx.world/dev/api",
        },
        "cli_api": {
            "submit": _step(
                "loom eval batch create --agent opencode --model glm-4.5 "
                "--benchmark humaneval --task HumanEval/0 --api-base "
                "https://yylx.world/prod/api",
                "POST /api/v1/batches",
            ),
            "monitor": _step(
                "loom eval batch show batch-493-user-gate --format json",
                "GET /api/v1/batches/{batch_id}",
            ),
            "batch_detail": _step(
                "loom eval batch show batch-493-user-gate --format json",
                "GET /api/v1/batches/{batch_id}",
            ),
            "batch_debug": _step(
                "loom eval batch debug batch-493-user-gate --format json",
                "GET /api/v1/batches/{batch_id}/debug",
            ),
            "trial_detail": _step(
                "loom eval trial show trial-493-user-gate --format json",
                "GET /api/v1/trials/{trial_id}",
            ),
            "trial_debug": _step(
                "loom eval trial debug trial-493-user-gate --format json",
                "GET /api/v1/trials/{trial_id}/debug",
            ),
            "download_atif": _step(
                "loom eval trial download trial-493-user-gate --kind atif --output atif.json",
                "GET /api/v1/trials/{trial_id}/atif",
                artifact_sha256=digest,
            ),
            "download_trajectory": _step(
                "loom eval trial download trial-493-user-gate --kind trajectory "
                "--output events.jsonl",
                "GET /api/v1/trials/{trial_id}/trajectory/download",
                artifact_sha256=digest,
            ),
            "download_artifact": _step(
                "loom eval trial download trial-493-user-gate --kind artifact "
                "--artifact-key main/report.json --output artifact.bin",
                "GET /api/v1/trials/{trial_id}/artifacts/download",
                artifact_sha256=digest,
            ),
            "delivery_bundle": _step(
                "loom eval batch delivery-bundle batch-493-user-gate "
                "--mode raw-harbor-tb2-v1 --output delivery.tar.gz",
                "GET /api/v1/batches/{batch_id}/delivery-export/{artifact_id}/download",
                artifact_sha256=digest,
            ),
            "integrity": _step(
                "shasum -a 256 delivery.tar.gz",
                "GET /api/v1/batches/{batch_id}/delivery-export",
                artifact_sha256=digest,
            ),
        },
        "frontend": {
            "route": "https://yylx.world/prod",
            "api_base": "https://yylx.world/prod/api",
            "environment_label": "Production",
            "navigation_checks": {
                "app_loaded": "pass",
                "runs_list": "pass",
                "batch_detail": "pass",
                "trial_detail": "pass",
                "run_library": "pass",
            },
            "button_checks": {
                "submit_batch": "pass",
                "refresh_status": "pass",
                "load_debug": "pass",
                "download_atif": "pass",
                "download_trajectory": "pass",
                "download_artifact": "pass",
                "download_delivery_bundle": "pass",
            },
            "download_routes": [
                "https://yylx.world/prod/api/v1/trials/trial-493-user-gate/atif",
                "https://yylx.world/prod/api/v1/trials/trial-493-user-gate/trajectory/download",
                "https://yylx.world/prod/api/v1/trials/trial-493-user-gate/"
                "artifacts/download?key=main%2Freport.json",
                "https://yylx.world/prod/api/v1/batches/batch-493-user-gate/"
                "delivery-export/export-493/download",
            ],
        },
        "forbidden_shortcuts": [],
    }


def _run_gate(
    tmp_path: Path, evidence: dict[str, Any], *args: str
) -> subprocess.CompletedProcess[str]:
    evidence_path = tmp_path / "evidence.json"
    evidence_path.write_text(json.dumps(evidence), encoding="utf-8")
    return subprocess.run(
        [sys.executable, str(SCRIPT), "validate", "--evidence", str(evidence_path), *args],
        check=False,
        text=True,
        capture_output=True,
        cwd=REPO_ROOT,
    )


def test_validate_accepts_operator_free_redacted_evidence_package() -> None:
    assert validate_evidence(_passing_evidence()) == []


def test_validate_rejects_missing_required_cli_api_step() -> None:
    evidence = _passing_evidence()
    evidence["cli_api"].pop("trial_debug")

    errors = validate_evidence(evidence)

    assert "missing required cli_api step 'trial_debug'" in errors


def test_validate_rejects_missing_frontend_button_check() -> None:
    evidence = _passing_evidence()
    evidence["frontend"]["button_checks"].pop("download_artifact")

    errors = validate_evidence(evidence)

    assert "missing required frontend button check 'download_artifact'" in errors


def test_validate_rejects_forbidden_shortcuts_and_internal_downloads() -> None:
    evidence = _passing_evidence()
    evidence["cli_api"]["monitor"]["command"] = "kubectl logs deploy/loom-worker"
    evidence["cli_api"]["download_artifact"]["evidence"] = (
        "downloaded from https://loom-minio.loom.svc.cluster.local/bucket/object"
    )
    evidence["forbidden_shortcuts"] = [{"kind": "direct_db", "status": "used"}]

    errors = validate_evidence(evidence)

    assert "forbidden shortcut declared at forbidden_shortcuts[0]" in errors
    assert any("cli_api.monitor.command" in error for error in errors)
    assert any("cli_api.download_artifact.evidence" in error for error in errors)


def test_cli_rejects_secret_leaks_without_echoing_secret(tmp_path: Path) -> None:
    evidence = _passing_evidence()
    raw_token = "sk-thisrawsecretmustnotappear1234567890"
    evidence["environment"]["token_source"] = raw_token
    evidence["cli_api"]["submit"]["evidence"] = f"Authorization: Bearer {raw_token}"

    result = _run_gate(tmp_path, evidence)

    assert result.returncode == 1
    assert "forbidden evidence value" in result.stderr
    assert "environment.token_source must start with env: or file:" in result.stderr
    assert raw_token not in result.stderr
    assert raw_token not in result.stdout


def test_cli_writes_redacted_pass_report(tmp_path: Path) -> None:
    report_path = tmp_path / "report.json"

    result = _run_gate(tmp_path, _passing_evidence(), "--output-json", str(report_path))

    assert result.returncode == 0, result.stderr
    assert "Operator-free user E2E gate: PASS" in result.stdout
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["status"] == "pass"
    assert report["issue"] == 493
    assert report["environment"]["token_source"] == "env:LOOM_PROD_USER_E2E_TOKEN"
    assert report["validated_cli_api_steps"] == [
        "submit",
        "monitor",
        "batch_detail",
        "batch_debug",
        "trial_detail",
        "trial_debug",
        "download_atif",
        "download_trajectory",
        "download_artifact",
        "delivery_bundle",
        "integrity",
    ]
