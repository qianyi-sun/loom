from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "public_beta_smoke_gate.py"


def _load_gate_module():
    spec = importlib.util.spec_from_file_location("public_beta_smoke_gate", SCRIPT)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_gate_declares_required_public_beta_checks() -> None:
    gate = _load_gate_module()

    required = {
        "http.health",
        "spa.logged_out",
        "auth.team_a_whoami",
        "auth.team_b_whoami",
        "providers.list",
        "providers.models",
        "benchmarks.runnable_catalog",
        "benchmarks.ready_bundle_objects",
        "runs.batch_detail",
        "runs.trial_detail",
        "artifacts.owner_atif_download",
        "artifacts.owner_trajectory_download",
        "library.my_team_contains_run",
        "library.all_teams_contains_run",
        "library.owner_team_label",
        "library.cross_team_safe_download",
        "library.direct_cross_team_download_denied",
        "library.clone_config",
        "library.reuse_artifact",
        "library.reuse_provenance",
        "library.blocked_artifact_denied",
        "library.private_artifact_denied",
        "runs.cross_team_mutation_denied",
        "security.no_secret_or_internal_url_leaks",
    }

    assert required <= set(gate.REQUIRED_CHECK_IDS)


def test_scan_evidence_flags_secrets_and_internal_urls_without_echoing_secret() -> None:
    gate = _load_gate_module()

    text = (
        "public payload contains seeded-token-123 and "
        "http://loom-minio.loom.svc.cluster.local:9000/object"
    )
    result = gate.scan_evidence_text(
        text,
        secret_needles=["seeded-token-123"],
        internal_url_needles=["loom-minio.loom.svc.cluster.local"],
    )

    assert result.status == "fail"
    assert result.subsystem == "security"
    assert "seeded-token-123" not in result.detail
    assert "loom-minio.loom.svc.cluster.local" not in result.detail
    assert "seeded..." in result.detail
    assert "loom-..." in result.detail


def test_markdown_report_redacts_tokens_and_lists_remediation() -> None:
    gate = _load_gate_module()
    report = gate.SmokeReport(
        server_url="https://loom.example.com",
        results=[
            gate.CheckResult(
                check_id="library.blocked_artifact_denied",
                subsystem="run-library",
                status="fail",
                detail="blocked artifact returned 200 for loom_api_should_not_print",
                remediation="Check Run Library artifact share_status enforcement.",
            ),
        ],
        response_bytes_scanned=256,
    )

    rendered = gate.render_markdown(
        report,
        secret_values=["loom_api_should_not_print"],
    )

    assert "loom_api_should_not_print" not in rendered
    assert "loom_..." in rendered
    assert "Run Library artifact share_status enforcement" in rendered
    assert "library.blocked_artifact_denied" in rendered


def test_console_summary_omits_check_details_and_secret_values() -> None:
    gate = _load_gate_module()
    report = gate.SmokeReport(
        server_url="https://loom.example.com",
        results=[
            gate.CheckResult(
                check_id="security.no_secret_or_internal_url_leaks",
                subsystem="security",
                status="fail",
                detail="raw token loom_api_should_not_print appeared in API response",
                remediation="Do not echo the token.",
            ),
        ],
        response_bytes_scanned=128,
    )

    rendered = gate.render_console_summary(
        report,
        markdown_output="public-beta-smoke.md",
        json_output="public-beta-smoke.json",
    )

    assert "loom_api_should_not_print" not in rendered
    assert "raw token" not in rendered
    assert "1 fail" in rendered
    assert "public-beta-smoke.md" in rendered
    assert "public-beta-smoke.json" in rendered


def test_run_smoke_uses_parser_max_response_scan_bytes(monkeypatch) -> None:
    gate = _load_gate_module()
    observed: dict[str, int] = {}

    class FakeClient:
        def __init__(self, server_url: str, *, max_scan_bytes: int) -> None:
            observed["max_scan_bytes"] = max_scan_bytes
            self.server_url = server_url
            self.evidence_chunks: list[str] = []
            self.response_bytes_scanned = 0

        def request(self, method: str, path: str, **kwargs):
            if path == "/api/v1/models":
                body = b'{"items":[{"provider":"openai","name":"gpt-4o-mini"}]}'
            else:
                body = b'{"items":[]}'
            return gate.HttpResponse(status_code=200, headers={}, body=body)

    monkeypatch.setattr(gate, "SmokeClient", FakeClient)
    args = gate._build_parser().parse_args([
        "--server-url", "https://loom.example.com",
        "--team-a-token", "loom_api_team_a",
        "--team-b-token", "loom_api_team_b",
        "--max-response-scan-bytes", "12345",
    ])

    gate.run_smoke(args)

    assert observed["max_scan_bytes"] == 12345


def test_run_smoke_fails_when_runnable_benchmark_catalog_is_empty(monkeypatch) -> None:
    gate = _load_gate_module()

    class FakeClient:
        def __init__(self, server_url: str, *, max_scan_bytes: int) -> None:
            self.server_url = server_url
            self.evidence_chunks: list[str] = []
            self.response_bytes_scanned = 0

        def request(self, method: str, path: str, **kwargs):
            if path == "/api/v1/models":
                body = b'{"items":[{"provider":"openai","name":"gpt-4o-mini"}]}'
            elif path == "/api/v1/benchmarks":
                body = b'{"items":[]}'
            else:
                body = b'{"items":[]}'
            return gate.HttpResponse(status_code=200, headers={}, body=body)

    monkeypatch.setattr(gate, "SmokeClient", FakeClient)
    args = gate._build_parser().parse_args([
        "--server-url", "https://loom.example.com",
        "--team-a-token", "loom_api_team_a",
        "--team-b-token", "loom_api_team_b",
    ])

    report = gate.run_smoke(args)
    result = next(r for r in report.results if r.check_id == "benchmarks.runnable_catalog")

    assert result.status == "fail"
    assert "no runnable benchmarks" in result.detail.lower()
    assert "provision" in result.remediation.lower()


def test_run_smoke_fails_when_ready_task_bundle_prefix_is_missing(monkeypatch) -> None:
    gate = _load_gate_module()

    class FakeClient:
        def __init__(self, server_url: str, *, max_scan_bytes: int) -> None:
            self.server_url = server_url
            self.evidence_chunks: list[str] = []
            self.response_bytes_scanned = 0

        def request(self, method: str, path: str, **kwargs):
            if path == "/api/v1/models":
                body = b'{"items":[{"provider":"openai","name":"gpt-4o-mini"}]}'
            elif path == "/api/v1/benchmarks":
                body = (
                    b'{"items":[{"id":"humaneval","task_count":1,'
                    b'"readiness_state":"runnable"}]}'
                )
            elif path == "/api/v1/tasks":
                body = (
                    b'{"items":[{"id":"humaneval/HumanEval/0",'
                    b'"source":"s3://loom-benchmarks/humaneval/HumanEval/0/"}]}'
                )
            else:
                body = b'{"items":[]}'
            return gate.HttpResponse(status_code=200, headers={}, body=body)

    monkeypatch.setattr(gate, "SmokeClient", FakeClient)
    monkeypatch.setattr(gate, "_s3_prefix_has_objects", lambda **_kwargs: False)
    args = gate._build_parser().parse_args([
        "--server-url", "https://loom.example.com",
        "--team-a-token", "loom_api_team_a",
        "--team-b-token", "loom_api_team_b",
        "--catalog-minio-endpoint",
        "http://minio:9000",
        "--catalog-minio-access-key",
        "access",
        "--catalog-minio-secret-key",
        "secret",
    ])

    report = gate.run_smoke(args)
    result = next(r for r in report.results if r.check_id == "benchmarks.ready_bundle_objects")

    assert result.status == "fail"
    assert "humaneval/HumanEval/0" in result.detail
    assert "missing" in result.detail.lower()
