from __future__ import annotations

import importlib.util
import io
import sys
from pathlib import Path
from urllib.parse import urlparse

import pytest
from botocore.exceptions import ClientError

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "staging_smoke_gate.py"


def _load_gate_module():
    spec = importlib.util.spec_from_file_location("staging_smoke_gate", SCRIPT)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _empty_catalog_client(gate):
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

    return FakeClient


def test_gate_declares_required_staging_checks() -> None:
    gate = _load_gate_module()

    required = {
        "http.health",
        "spa.logged_out",
        "auth.team_a_whoami",
        "auth.team_b_whoami",
        "providers.list",
        "providers.models",
        "agents.ready_catalog",
        "benchmarks.runnable_catalog",
        "benchmarks.ready_bundle_objects",
        "object_store.minio_write_probe",
        "service.no_oom_restarts",
        "runs.batch_detail",
        "runs.claimed_without_started",
        "runs.worker_pool_coverage",
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
        markdown_output="staging-smoke.md",
        json_output="staging-smoke.json",
    )

    assert "loom_api_should_not_print" not in rendered
    assert "raw token" not in rendered
    assert "1 fail" in rendered
    assert "staging-smoke.md" in rendered
    assert "staging-smoke.json" in rendered


def test_resolve_smoke_secret_args_supports_env_file_and_stdin(
    monkeypatch,
    tmp_path: Path,
) -> None:
    gate = _load_gate_module()
    secret_file = tmp_path / "minio-secret.txt"
    secret_file.write_text("resolved-minio-secret\n", encoding="utf-8")
    monkeypatch.setenv("SMOKE_TEAM_A_TOKEN", "resolved-team-a-token")
    monkeypatch.setenv("SMOKE_TEAM_B_TOKEN", "resolved-team-b-token")
    monkeypatch.setenv("SMOKE_MINIO_ACCESS", "resolved-minio-access")
    monkeypatch.setattr(sys, "stdin", io.StringIO("stdin-secret-needle\n"))

    args = gate._build_parser().parse_args([
        "--server-url", "https://loom.example.com",
        "--team-a-token", "env:SMOKE_TEAM_A_TOKEN",
        "--team-b-token", "env:SMOKE_TEAM_B_TOKEN",
        "--catalog-minio-access-key", "env:SMOKE_MINIO_ACCESS",
        "--catalog-minio-secret-key", f"file:{secret_file}",
        "--secret-needle", "-",
    ])

    resolved = gate.resolve_smoke_secret_args(args)

    assert resolved.team_a_token == "resolved-team-a-token"
    assert resolved.team_b_token == "resolved-team-b-token"
    assert resolved.catalog_minio_access_key == "resolved-minio-access"
    assert resolved.catalog_minio_secret_key == "resolved-minio-secret"
    assert resolved.secret_needle == ["stdin-secret-needle"]
    assert args.team_a_token == "env:SMOKE_TEAM_A_TOKEN"


def test_main_resolves_token_sources_before_requests_and_redacts_evidence(
    monkeypatch,
    tmp_path: Path,
    capsys,
) -> None:
    gate = _load_gate_module()
    markdown_path = tmp_path / "smoke.md"
    json_path = tmp_path / "smoke.json"
    observed_tokens: list[str | None] = []
    monkeypatch.setenv("SMOKE_TEAM_A_TOKEN", "resolved-team-a-token")
    monkeypatch.setenv("SMOKE_TEAM_B_TOKEN", "resolved-team-b-token")

    class FakeClient:
        def __init__(self, server_url: str, *, max_scan_bytes: int) -> None:
            self.server_url = server_url
            self.evidence_chunks: list[str] = []
            self.response_bytes_scanned = 0

        def request(self, method: str, path: str, **kwargs):
            token = kwargs.get("token")
            if token is not None:
                observed_tokens.append(token)
            if path in {"/api/v1/health", "/"}:
                body = b'{"status":"ok"}'
            elif path == "/api/v1/auth/whoami":
                body = (
                    b'{"credential_type":"user_owned_api_token",'
                    b'"principal_type":"team","username":"smoke-user",'
                    b'"team_name":"Smoke Team","role":"member",'
                    b'"is_platform_admin":false}'
                )
            elif path == "/api/v1/provider-connections":
                body = b'{"items":[{"name":"mz_tn_canada_qianyi"}]}'
            elif path == "/api/v1/models":
                body = b'{"items":[{"provider":"openai","name":"gpt-4o-mini"}]}'
            elif path == "/api/v1/agents":
                body = b'{"items":[{"name":"oracle","service_mode_ready":true}]}'
            elif path == "/api/v1/benchmarks":
                body = (
                    b'{"items":[{"id":"skilllearnbench","task_count":100,'
                    b'"readiness_state":"runnable"}]}'
                )
            else:
                body = b'{"items":[]}'
            return gate.HttpResponse(status_code=200, headers={}, body=body)

    monkeypatch.setattr(gate, "SmokeClient", FakeClient)

    rc = gate.main([
        "--server-url", "https://loom.example.com",
        "--team-a-token", "env:SMOKE_TEAM_A_TOKEN",
        "--team-b-token", "env:SMOKE_TEAM_B_TOKEN",
        "--markdown-output", str(markdown_path),
        "--json-output", str(json_path),
    ])

    assert rc == 0
    assert "resolved-team-a-token" in observed_tokens
    assert "resolved-team-b-token" in observed_tokens
    assert "env:SMOKE_TEAM_A_TOKEN" not in observed_tokens
    captured = capsys.readouterr()
    rendered = markdown_path.read_text(encoding="utf-8")
    payload = json_path.read_text(encoding="utf-8")
    assert "resolved-team-a-token" not in captured.out
    assert "resolved-team-b-token" not in captured.out
    assert "resolved-team-a-token" not in rendered
    assert "resolved-team-b-token" not in rendered
    assert "resolved-team-a-token" not in payload
    assert "resolved-team-b-token" not in payload


def test_main_rejects_literal_minio_secret_before_s3_use(
    monkeypatch,
    capsys,
) -> None:
    gate = _load_gate_module()
    raw_secret = "plain-minio-secret-value"
    monkeypatch.setenv("SMOKE_TEAM_A_TOKEN", "resolved-team-a-token")
    monkeypatch.setenv("SMOKE_TEAM_B_TOKEN", "resolved-team-b-token")
    monkeypatch.setenv("SMOKE_MINIO_ACCESS", "resolved-minio-access")
    monkeypatch.setattr(gate, "SmokeClient", _empty_catalog_client(gate))
    monkeypatch.setattr(
        gate.boto3,
        "client",
        lambda *_args, **_kwargs: pytest.fail("literal secret reached S3 client"),
    )

    with pytest.raises(SystemExit) as exc_info:
        gate.main([
            "--server-url", "https://loom.example.com",
            "--team-a-token", "env:SMOKE_TEAM_A_TOKEN",
            "--team-b-token", "env:SMOKE_TEAM_B_TOKEN",
            "--catalog-minio-endpoint", "http://minio:9000",
            "--catalog-minio-access-key", "env:SMOKE_MINIO_ACCESS",
            "--catalog-minio-secret-key", raw_secret,
            "--object-store-write-check-only",
        ])

    assert exc_info.value.code == 2
    err = capsys.readouterr().err
    assert "--catalog-minio-secret-key" in err
    assert "literal values are rejected" in err
    assert raw_secret not in err


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


def test_run_smoke_fails_when_team_b_uses_legacy_team_token(monkeypatch) -> None:
    gate = _load_gate_module()

    class FakeClient:
        def __init__(self, server_url: str, *, max_scan_bytes: int) -> None:
            self.server_url = server_url
            self.evidence_chunks: list[str] = []
            self.response_bytes_scanned = 0

        def request(self, method: str, path: str, **kwargs):
            token = kwargs.get("token")
            if path in {"/api/v1/health", "/"}:
                body = b'{"status":"ok"}'
            elif path == "/api/v1/auth/whoami" and token == "loom_api_team_a":
                body = (
                    b'{"credential_type":"user_owned_api_token",'
                    b'"principal_type":"user","username":"team-a-smoke",'
                    b'"team_name":"Team A","role":"owner","is_platform_admin":false}'
                )
            elif path == "/api/v1/auth/whoami" and token == "loom_api_team_b":
                body = (
                    b'{"credential_type":"legacy_team_token",'
                    b'"principal_type":"team","team_name":"Team B",'
                    b'"is_platform_admin":false}'
                )
            elif path == "/api/v1/provider-connections":
                body = b'{"items":[{"name":"mz_tn_canada_qianyi"}]}'
            elif path == "/api/v1/models":
                body = b'{"items":[{"provider":"openai","name":"gpt-4o-mini"}]}'
            elif path == "/api/v1/agents":
                body = b'{"items":[{"name":"oracle","service_mode_ready":true}]}'
            elif path == "/api/v1/benchmarks":
                body = (
                    b'{"items":[{"id":"skilllearnbench","task_count":100,'
                    b'"readiness_state":"runnable"}]}'
                )
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
    result = next(r for r in report.results if r.check_id == "auth.team_b_whoami")

    assert result.status == "fail"
    assert "legacy_team_token" in result.detail
    assert "non-admin user-owned API token" in result.remediation


def test_run_smoke_fails_when_team_b_token_restores_platform_admin(monkeypatch) -> None:
    gate = _load_gate_module()

    class FakeClient:
        def __init__(self, server_url: str, *, max_scan_bytes: int) -> None:
            self.server_url = server_url
            self.evidence_chunks: list[str] = []
            self.response_bytes_scanned = 0

        def request(self, method: str, path: str, **kwargs):
            token = kwargs.get("token")
            if path in {"/api/v1/health", "/"}:
                body = b'{"status":"ok"}'
            elif path == "/api/v1/auth/whoami" and token == "loom_api_team_a":
                body = (
                    b'{"credential_type":"user_owned_api_token",'
                    b'"principal_type":"user","username":"team-a-smoke",'
                    b'"team_name":"Team A","role":"owner","is_platform_admin":false}'
                )
            elif path == "/api/v1/auth/whoami" and token == "loom_api_team_b":
                body = (
                    b'{"credential_type":"user_owned_api_token",'
                    b'"principal_type":"user","username":"qianyi",'
                    b'"team_name":"Team B","role":"platform_admin",'
                    b'"is_platform_admin":true}'
                )
            elif path == "/api/v1/provider-connections":
                body = b'{"items":[{"name":"mz_tn_canada_qianyi"}]}'
            elif path == "/api/v1/models":
                body = b'{"items":[{"provider":"openai","name":"gpt-4o-mini"}]}'
            elif path == "/api/v1/agents":
                body = b'{"items":[{"name":"oracle","service_mode_ready":true}]}'
            elif path == "/api/v1/benchmarks":
                body = (
                    b'{"items":[{"id":"skilllearnbench","task_count":100,'
                    b'"readiness_state":"runnable"}]}'
                )
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
    result = next(r for r in report.results if r.check_id == "auth.team_b_whoami")

    assert result.status == "fail"
    assert "platform_admin" in result.detail
    assert "cross-team negative checks" in result.remediation


def test_auth_whoami_accepts_user_owned_team_api_token() -> None:
    gate = _load_gate_module()

    result = gate._auth_whoami_result(
        "team_a",
        gate.HttpResponse(
            status_code=200,
            headers={},
            body=(
                b'{"credential_type":"user_owned_api_token",'
                b'"principal_type":"team",'
                b'"user_id":"03698dad-e6c3-42ed-a4f6-3781a6f25704",'
                b'"username":"pb-smoke-team-a",'
                b'"team_name":"Agentic RL",'
                b'"role":null,'
                b'"scopes":["read:own","submit","tokens:manage"],'
                b'"is_platform_admin":false}'
            ),
        ),
    )

    assert result.status == "pass"
    assert "user_owned_api_token" in result.detail
    assert "pb-smoke-team-a" in result.detail


def test_main_writes_evidence_when_run_library_list_times_out(
    monkeypatch,
    tmp_path: Path,
) -> None:
    gate = _load_gate_module()
    markdown_path = tmp_path / "smoke.md"
    json_path = tmp_path / "smoke.json"
    monkeypatch.setenv("SMOKE_TEAM_A_TOKEN", "loom_api_team_a")
    monkeypatch.setenv("SMOKE_TEAM_B_TOKEN", "loom_api_team_b")

    class FakeResponse:
        def __init__(self, body: bytes) -> None:
            self.status = 200
            self.headers: dict[str, str] = {}
            self._body = body

        def __enter__(self):
            return self

        def __exit__(self, *_args) -> None:
            return None

        def read(self, _limit: int) -> bytes:
            return self._body

    def fake_urlopen(request, timeout: float):
        url = request.full_url
        path = urlparse(url).path
        query = urlparse(url).query
        if path == "/api/v1/run-library/batches" and query == "":
            raise TimeoutError("The read operation timed out")
        if path in {"/api/v1/health", "/"}:
            return FakeResponse(b'{"status":"ok"}')
        if path == "/api/v1/auth/whoami":
            return FakeResponse(
                b'{"credential_type":"user_owned_api_token",'
                b'"principal_type":"user","username":"smoke-user",'
                b'"team_name":"Smoke Team","role":"member",'
                b'"is_platform_admin":false}'
            )
        if path == "/api/v1/provider-connections":
            return FakeResponse(b'{"items":[{"name":"mz_tn_canada_qianyi"}]}')
        if path == "/api/v1/models":
            return FakeResponse(b'{"items":[{"provider":"openai","name":"gpt-4o-mini"}]}')
        if path == "/api/v1/agents":
            return FakeResponse(b'{"items":[{"name":"oracle","service_mode_ready":true}]}')
        if path == "/api/v1/benchmarks":
            return FakeResponse(
                b'{"items":[{"id":"skilllearnbench","task_count":100,'
                b'"readiness_state":"runnable"}]}'
            )
        if path == "/api/v1/batches/batch-1":
            return FakeResponse(
                b'{"id":"batch-1","debug_evidence":{"trials":{'
                b'"summary":{"claimed_without_started":0},'
                b'"worker_pools":{"terminal":{"oldlab":1},'
                b'"unknown_terminal":0}}}}'
            )
        if path == "/api/v1/run-library/batches/batch-1":
            return FakeResponse(
                b'{"id":"batch-1","owner_team":{"id":"team-a","name":"Team A"}}'
            )
        return FakeResponse(b'{"items":[{"id":"batch-1"}]}')

    monkeypatch.setattr(gate, "urlopen", fake_urlopen)
    rc = gate.main([
        "--server-url", "https://loom.example.com",
        "--team-a-token", "env:SMOKE_TEAM_A_TOKEN",
        "--team-b-token", "env:SMOKE_TEAM_B_TOKEN",
        "--batch-id", "batch-1",
        "--markdown-output", str(markdown_path),
        "--json-output", str(json_path),
    ])

    assert rc == 1
    assert markdown_path.exists()
    assert json_path.exists()
    markdown = markdown_path.read_text(encoding="utf-8")
    payload = json_path.read_text(encoding="utf-8")
    assert "library.my_team_contains_run" in markdown
    assert "GET /api/v1/run-library/batches" in markdown
    assert "timed out after 30s" in markdown
    assert "library.my_team_contains_run" in payload


def test_owner_team_label_check_accepts_truncated_large_detail_prefix() -> None:
    gate = _load_gate_module()
    body = (
        b'{"id":"batch-1","team_id":"team-1",'
        b'"owner_team":{"id":"team-1","name":"Alpha Research"},'
        b'"name":"large run","artifact_inventory":{"reports":['
        + (b'{"id":"artifact","key":"object"},' * 5000)
    )
    response = gate.HttpResponse(status_code=200, headers={}, body=body)

    assert gate._json_has_owner_team(response)


def test_owner_team_label_check_rejects_nested_artifact_owner_when_truncated() -> None:
    gate = _load_gate_module()
    body = (
        b'{"id":"batch-1","team_id":"team-1",'
        b'"artifact_inventory":{"reports":[{"id":"artifact",'
        b'"owner_team":{"id":"team-1","name":"Alpha Research"}}'
    )
    response = gate.HttpResponse(status_code=200, headers={}, body=body)

    assert not gate._json_has_owner_team(response)


def test_run_smoke_fails_when_batch_debug_reports_claimed_without_started(
    monkeypatch,
) -> None:
    gate = _load_gate_module()

    class FakeClient:
        def __init__(self, server_url: str, *, max_scan_bytes: int) -> None:
            self.server_url = server_url
            self.evidence_chunks: list[str] = []
            self.response_bytes_scanned = 0

        def request(self, method: str, path: str, **kwargs):
            if path in {"/api/v1/health", "/", "/api/v1/auth/whoami"}:
                body = b'{"status":"ok"}'
            elif path == "/api/v1/provider-connections":
                body = b'{"items":[{"name":"mz_tn_canada_qianyi"}]}'
            elif path == "/api/v1/models":
                body = b'{"items":[{"provider":"openai","name":"gpt-4o-mini"}]}'
            elif path == "/api/v1/agents":
                body = b'{"items":[{"name":"oracle","service_mode_ready":true}]}'
            elif path == "/api/v1/benchmarks":
                body = (
                    b'{"items":[{"id":"skilllearnbench","task_count":100,'
                    b'"readiness_state":"runnable"}]}'
                )
            elif path == "/api/v1/batches/batch-1":
                body = (
                    b'{"id":"batch-1","debug_evidence":{"trials":{"summary":{'
                    b'"claimed_without_started":2}}}}'
                )
            elif path == "/api/v1/run-library/batches":
                body = b'{"items":[{"id":"batch-1"}]}'
            elif path == "/api/v1/run-library/batches/batch-1":
                body = (
                    b'{"id":"batch-1","owner_team":'
                    b'{"id":"team-1","name":"Agentic RL"}}'
                )
            else:
                body = b'{"items":[]}'
            return gate.HttpResponse(status_code=200, headers={}, body=body)

    monkeypatch.setattr(gate, "SmokeClient", FakeClient)
    args = gate._build_parser().parse_args([
        "--server-url", "https://loom.example.com",
        "--team-a-token", "loom_api_team_a",
        "--team-b-token", "loom_api_team_b",
        "--batch-id", "batch-1",
    ])

    report = gate.run_smoke(args)
    result = next(r for r in report.results if r.check_id == "runs.claimed_without_started")

    assert result.status == "fail"
    assert "2" in result.detail
    assert "claimed_without_started" in result.detail
    assert "reclaim" in result.remediation.lower()


def test_run_smoke_fails_when_required_worker_pool_has_no_terminal_trials(
    monkeypatch,
) -> None:
    gate = _load_gate_module()

    class FakeClient:
        def __init__(self, server_url: str, *, max_scan_bytes: int) -> None:
            self.server_url = server_url
            self.evidence_chunks: list[str] = []
            self.response_bytes_scanned = 0

        def request(self, method: str, path: str, **kwargs):
            if path in {"/api/v1/health", "/", "/api/v1/auth/whoami"}:
                body = b'{"status":"ok"}'
            elif path == "/api/v1/provider-connections":
                body = b'{"items":[{"name":"mz_tn_canada_qianyi"}]}'
            elif path == "/api/v1/models":
                body = b'{"items":[{"provider":"openai","name":"gpt-4o-mini"}]}'
            elif path == "/api/v1/agents":
                body = b'{"items":[{"name":"oracle","service_mode_ready":true}]}'
            elif path == "/api/v1/benchmarks":
                body = (
                    b'{"items":[{"id":"skilllearnbench","task_count":100,'
                    b'"readiness_state":"runnable"}]}'
                )
            elif path == "/api/v1/batches/batch-1":
                body = (
                    b'{"id":"batch-1","debug_evidence":{"trials":{'
                    b'"summary":{"claimed_without_started":0},'
                    b'"worker_pools":{"terminal":{"gb10-arm64":12},'
                    b'"unknown_terminal":0}}}}'
                )
            elif path == "/api/v1/run-library/batches":
                body = b'{"items":[{"id":"batch-1"}]}'
            elif path == "/api/v1/run-library/batches/batch-1":
                body = (
                    b'{"id":"batch-1","owner_team":'
                    b'{"id":"team-1","name":"Agentic RL"}}'
                )
            else:
                body = b'{"items":[]}'
            return gate.HttpResponse(status_code=200, headers={}, body=body)

    monkeypatch.setattr(gate, "SmokeClient", FakeClient)
    args = gate._build_parser().parse_args([
        "--server-url", "https://loom.example.com",
        "--team-a-token", "loom_api_team_a",
        "--team-b-token", "loom_api_team_b",
        "--batch-id", "batch-1",
        "--required-worker-pool", "oldlab",
    ])

    report = gate.run_smoke(args)
    result = next(r for r in report.results if r.check_id == "runs.worker_pool_coverage")

    assert result.status == "fail"
    assert "oldlab" in result.detail
    assert "gb10-arm64=12" in result.detail
    assert "required worker pools" in result.remediation.lower()


def test_worker_pool_coverage_passes_when_required_pools_have_terminal_trials() -> None:
    gate = _load_gate_module()
    response = gate.HttpResponse(
        status_code=200,
        headers={},
        body=(
            b'{"debug_evidence":{"trials":{"worker_pools":{"terminal":'
            b'{"gb10-arm64":158,"k8s-worker":12,"oldlab":2},'
            b'"unknown_terminal":0}}}}'
        ),
    )

    result = gate._worker_pool_coverage_result(
        response,
        ["oldlab", "gb10-arm64"],
    )

    assert result.status == "pass"
    assert "oldlab=2" in result.detail
    assert "gb10-arm64=158" in result.detail


def test_run_smoke_fails_when_service_pod_reports_oom_restart(monkeypatch) -> None:
    gate = _load_gate_module()

    class FakeClient:
        def __init__(self, server_url: str, *, max_scan_bytes: int) -> None:
            self.server_url = server_url
            self.evidence_chunks: list[str] = []
            self.response_bytes_scanned = 0

        def request(self, method: str, path: str, **kwargs):
            if path in {"/api/v1/health", "/", "/api/v1/auth/whoami"}:
                body = b'{"status":"ok"}'
            elif path == "/api/v1/provider-connections":
                body = b'{"items":[{"name":"mz_tn_canada_qianyi"}]}'
            elif path == "/api/v1/models":
                body = b'{"items":[{"provider":"openai","name":"gpt-4o-mini"}]}'
            elif path == "/api/v1/agents":
                body = b'{"items":[{"name":"oracle","service_mode_ready":true}]}'
            elif path == "/api/v1/benchmarks":
                body = (
                    b'{"items":[{"id":"skilllearnbench","task_count":100,'
                    b'"readiness_state":"runnable"}]}'
                )
            else:
                body = b'{"items":[]}'
            return gate.HttpResponse(status_code=200, headers={}, body=body)

    class FakeCompleted:
        returncode = 0
        stderr = ""
        stdout = (
            '{"items":[{"metadata":{"name":"loom-service-abc"},'
            '"status":{"containerStatuses":[{"name":"loom-service",'
            '"restartCount":1,"lastState":{"terminated":{'
            '"reason":"OOMKilled","exitCode":137}}}]}}]}'
        )

    class FakeSubprocess:
        @staticmethod
        def run(*_args, **_kwargs):
            return FakeCompleted()

    monkeypatch.setattr(gate, "SmokeClient", FakeClient)
    monkeypatch.setattr(gate, "subprocess", FakeSubprocess, raising=False)
    args = gate._build_parser().parse_args([
        "--server-url", "https://loom.example.com",
        "--team-a-token", "loom_api_team_a",
        "--team-b-token", "loom_api_team_b",
        "--k8s-namespace", "loom-staging",
    ])

    report = gate.run_smoke(args)
    result = next(r for r in report.results if r.check_id == "service.no_oom_restarts")

    assert result.status == "fail"
    assert "OOMKilled" in result.detail
    assert "loom-service-abc" in result.detail


def test_run_smoke_fails_when_provider_connection_catalog_is_empty(monkeypatch) -> None:
    gate = _load_gate_module()

    class FakeClient:
        def __init__(self, server_url: str, *, max_scan_bytes: int) -> None:
            self.server_url = server_url
            self.evidence_chunks: list[str] = []
            self.response_bytes_scanned = 0

        def request(self, method: str, path: str, **kwargs):
            if path == "/api/v1/models":
                body = b'{"items":[{"provider":"yibuapi","name":"qwen3.6-35b-a3b"}]}'
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
    result = next(r for r in report.results if r.check_id == "providers.list")

    assert result.status == "fail"
    assert "no provider connections" in result.detail.lower()
    assert "provider connection" in result.remediation.lower()


def test_run_smoke_requires_exact_named_provider_connection(monkeypatch) -> None:
    gate = _load_gate_module()

    class FakeClient:
        def __init__(self, server_url: str, *, max_scan_bytes: int) -> None:
            self.server_url = server_url
            self.evidence_chunks: list[str] = []
            self.response_bytes_scanned = 0

        def request(self, method: str, path: str, **kwargs):
            if path == "/api/v1/provider-connections":
                body = (
                    b'{"items":[{"name":"different-provider",'
                    b'"description":"mentions mz_tn_canada_qianyi"}]}'
                )
            elif path == "/api/v1/models":
                body = b'{"items":[{"provider":"yibuapi","name":"qwen3.6-35b-a3b"}]}'
            else:
                body = b'{"items":[]}'
            return gate.HttpResponse(status_code=200, headers={}, body=body)

    monkeypatch.setattr(gate, "SmokeClient", FakeClient)
    args = gate._build_parser().parse_args([
        "--server-url", "https://loom.example.com",
        "--team-a-token", "loom_api_team_a",
        "--team-b-token", "loom_api_team_b",
        "--provider-connection-name", "mz_tn_canada_qianyi",
    ])

    report = gate.run_smoke(args)
    result = next(r for r in report.results if r.check_id == "providers.list")

    assert result.status == "fail"
    assert "mz_tn_canada_qianyi" in result.detail
    assert "different-provider" in result.detail


def test_run_smoke_requires_expected_provider_model_when_configured(monkeypatch) -> None:
    gate = _load_gate_module()

    class FakeClient:
        def __init__(self, server_url: str, *, max_scan_bytes: int) -> None:
            self.server_url = server_url
            self.evidence_chunks: list[str] = []
            self.response_bytes_scanned = 0

        def request(self, method: str, path: str, **kwargs):
            if path == "/api/v1/provider-connections":
                body = b'{"items":[{"name":"mz_tn_canada_qianyi"}]}'
            elif path == "/api/v1/models":
                body = b'{"items":[{"provider":"openai","name":"gpt-4o-mini"}]}'
            else:
                body = b'{"items":[]}'
            return gate.HttpResponse(status_code=200, headers={}, body=body)

    monkeypatch.setattr(gate, "SmokeClient", FakeClient)
    args = gate._build_parser().parse_args([
        "--server-url", "https://loom.example.com",
        "--team-a-token", "loom_api_team_a",
        "--team-b-token", "loom_api_team_b",
        "--provider-connection-name", "mz_tn_canada_qianyi",
        "--provider-model-provider", "yibuapi",
        "--provider-model-name", "qwen3.6-35b-a3b",
    ])

    report = gate.run_smoke(args)
    result = next(r for r in report.results if r.check_id == "providers.models")

    assert result.status == "fail"
    assert "qwen3.6-35b-a3b" in result.detail
    assert "gpt-4o-mini" in result.detail


def test_run_smoke_reports_expected_provider_model_match(monkeypatch) -> None:
    gate = _load_gate_module()

    class FakeClient:
        def __init__(self, server_url: str, *, max_scan_bytes: int) -> None:
            self.server_url = server_url
            self.evidence_chunks: list[str] = []
            self.response_bytes_scanned = 0

        def request(self, method: str, path: str, **kwargs):
            if path == "/api/v1/provider-connections":
                body = b'{"items":[{"name":"mz_tn_canada_qianyi"}]}'
            elif path == "/api/v1/models":
                body = (
                    b'{"items":['
                    b'{"provider":"openai","name":"gpt-4o-mini"},'
                    b'{"provider":"yibuapi","name":"gpt-4o-mini"}'
                    b']}'
                )
            else:
                body = b'{"items":[]}'
            return gate.HttpResponse(status_code=200, headers={}, body=body)

    monkeypatch.setattr(gate, "SmokeClient", FakeClient)
    args = gate._build_parser().parse_args([
        "--server-url", "https://loom.example.com",
        "--team-a-token", "loom_api_team_a",
        "--team-b-token", "loom_api_team_b",
        "--provider-connection-name", "mz_tn_canada_qianyi",
        "--provider-model-provider", "yibuapi",
        "--provider-model-name", "gpt-4o-mini",
    ])

    report = gate.run_smoke(args)
    result = next(r for r in report.results if r.check_id == "providers.models")

    assert result.status == "pass"
    assert "expected yibuapi/gpt-4o-mini" in result.detail


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
            elif path == "/api/v1/agents":
                body = b'{"items":[{"name":"oracle","service_mode_ready":true}]}'
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


def test_run_smoke_fails_when_ready_agent_catalog_is_empty(monkeypatch) -> None:
    gate = _load_gate_module()

    class FakeClient:
        def __init__(self, server_url: str, *, max_scan_bytes: int) -> None:
            self.server_url = server_url
            self.evidence_chunks: list[str] = []
            self.response_bytes_scanned = 0

        def request(self, method: str, path: str, **kwargs):
            if path == "/api/v1/models":
                body = b'{"items":[{"provider":"openai","name":"gpt-4o-mini"}]}'
            elif path == "/api/v1/agents":
                body = b'{"items":[]}'
            elif path == "/api/v1/benchmarks":
                body = (
                    b'{"items":[{"id":"humaneval","task_count":1,'
                    b'"readiness_state":"runnable"}]}'
                )
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
    result = next(r for r in report.results if r.check_id == "agents.ready_catalog")

    assert result.status == "fail"
    assert "no ready agents" in result.detail.lower()
    assert "agent catalog" in result.remediation.lower()


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
            elif path == "/api/v1/agents":
                body = b'{"items":[{"name":"oracle","service_mode_ready":true}]}'
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


def test_run_smoke_fails_when_minio_write_probe_hits_storage_full(monkeypatch) -> None:
    gate = _load_gate_module()

    class StorageFullS3:
        def put_object(self, **_kwargs: object) -> None:
            raise ClientError(
                {
                    "Error": {
                        "Code": "XMinioStorageFull",
                        "Message": "Storage backend reached minimum free drive threshold",
                    },
                },
                "PutObject",
            )

    monkeypatch.setattr(gate, "SmokeClient", _empty_catalog_client(gate))
    monkeypatch.setattr(gate.boto3, "client", lambda *_args, **_kwargs: StorageFullS3())
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
        "--object-store-write-check",
    ])

    report = gate.run_smoke(args)
    result = next(r for r in report.results if r.check_id == "object_store.minio_write_probe")

    assert result.status == "fail"
    assert "XMinioStorageFull" in result.detail
    assert "free" in result.remediation.lower()


def test_run_smoke_minio_write_probe_deletes_probe_object(monkeypatch) -> None:
    gate = _load_gate_module()
    s3 = _RecordingS3()

    monkeypatch.setattr(gate, "SmokeClient", _empty_catalog_client(gate))
    monkeypatch.setattr(gate.boto3, "client", lambda *_args, **_kwargs: s3)
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
        "--object-store-write-check",
    ])

    report = gate.run_smoke(args)
    result = next(r for r in report.results if r.check_id == "object_store.minio_write_probe")

    assert result.status == "pass"
    assert "trajectories" in result.detail
    assert s3.objects == {}


def test_run_smoke_skips_minio_write_probe_without_explicit_opt_in(monkeypatch) -> None:
    gate = _load_gate_module()

    class ExplodingS3:
        def put_object(self, **_kwargs: object) -> None:
            raise AssertionError("write probe should not run without opt-in")

    monkeypatch.setattr(gate, "SmokeClient", _empty_catalog_client(gate))
    monkeypatch.setattr(gate.boto3, "client", lambda *_args, **_kwargs: ExplodingS3())
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
    result = next(r for r in report.results if r.check_id == "object_store.minio_write_probe")

    assert result.status == "skip"
    assert "--object-store-write-check" in result.detail


def test_run_smoke_can_run_only_minio_write_probe(monkeypatch) -> None:
    gate = _load_gate_module()
    s3 = _RecordingS3()

    monkeypatch.setattr(gate, "SmokeClient", _empty_catalog_client(gate))
    monkeypatch.setattr(gate.boto3, "client", lambda *_args, **_kwargs: s3)
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
        "--object-store-write-check-only",
    ])

    report = gate.run_smoke(args)

    assert [r.check_id for r in report.results] == ["object_store.minio_write_probe"]
    assert report.results[0].status == "pass"
    assert "trajectories/_ops/staging-smoke/probe-" in report.results[0].detail
    assert s3.objects == {}


def test_run_smoke_minio_write_probe_can_exercise_concurrent_objects(
    monkeypatch,
) -> None:
    gate = _load_gate_module()
    s3 = _RecordingS3()

    monkeypatch.setattr(gate, "SmokeClient", _empty_catalog_client(gate))
    monkeypatch.setattr(gate.boto3, "client", lambda *_args, **_kwargs: s3)
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
        "--object-store-write-check-only",
        "--object-store-write-check-count",
        "5",
        "--object-store-write-check-concurrency",
        "3",
    ])

    report = gate.run_smoke(args)

    assert [r.check_id for r in report.results] == ["object_store.minio_write_probe"]
    assert report.results[0].status == "pass"
    assert "5 probe object" in report.results[0].detail
    assert "concurrency=3" in report.results[0].detail
    assert len(s3.put_keys) == 5
    assert sorted(s3.delete_keys) == sorted(s3.put_keys)
    assert s3.objects == {}


class _RecordingS3:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}
        self.put_keys: list[tuple[str, str]] = []
        self.delete_keys: list[tuple[str, str]] = []

    def put_object(self, *, Bucket: str, Key: str, Body: bytes) -> None:  # noqa: N803
        self.objects[(Bucket, Key)] = Body
        self.put_keys.append((Bucket, Key))

    def delete_object(self, *, Bucket: str, Key: str) -> None:  # noqa: N803
        self.delete_keys.append((Bucket, Key))
        del self.objects[(Bucket, Key)]
