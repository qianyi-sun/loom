"""CLI contract tests for the fixed official-Recipe Pipeline surface."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import UUID

import httpx
import pytest

from loom.pipeline.keys import canonical_digest
from loom.pipeline.stage1_smoke import Stage1SmokeAuthorizationV1
from loom_cli.__main__ import main
from loom_cli.pipeline_cmd import _build_bundle
from tests.unit.test_pipeline_stage1_smoke import _candidate

RUN_ID = "00000000-0000-0000-0000-000000000011"
STAGE_ID = "00000000-0000-0000-0000-000000000022"
ARTIFACT_ID = "00000000-0000-0000-0000-000000000033"
TASK_SET_ID = "00000000-0000-0000-0000-000000000044"
IMPORT_ID = "00000000-0000-0000-0000-000000000055"
SESSION_ID = "00000000-0000-0000-0000-000000000066"


@pytest.fixture(autouse=True)
def _logged_in(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "config"))
    monkeypatch.setenv("PIPELINE_TEST_TOKEN", "loom_admin_pipeline_test123")
    assert (
        main(
            [
                "auth",
                "login",
                "--server",
                "https://loom.test",
                "--token",
                "env:PIPELINE_TEST_TOKEN",
            ]
        )
        == 0
    )


class MockServer:
    def __init__(self) -> None:
        self.requests: list[httpx.Request] = []
        self.responses: dict[tuple[str, str], list[httpx.Response]] = {}
        self.interrupts: set[tuple[str, str]] = set()

    def add(self, method: str, path: str, response: httpx.Response) -> None:
        self.responses.setdefault((method, path), []).append(response)


@pytest.fixture
def server(monkeypatch: pytest.MonkeyPatch) -> MockServer:
    mock = MockServer()

    def handler(request: httpx.Request) -> httpx.Response:
        request.read()
        mock.requests.append(request)
        if (request.method, request.url.path) in mock.interrupts:
            mock.interrupts.remove((request.method, request.url.path))
            raise KeyboardInterrupt
        responses = mock.responses.get((request.method, request.url.path), [])
        if responses:
            return responses.pop(0)
        return httpx.Response(
            404,
            json={
                "detail": {
                    "reason_code": "test_route_missing",
                    "message": "mock route missing",
                }
            },
        )

    transport = httpx.MockTransport(handler)

    def patched_client(cfg: Any, *, timeout: float = 30.0) -> httpx.Client:
        return httpx.Client(
            base_url=cfg.server_url,
            headers={"Authorization": f"Bearer {cfg.auth_token}"},
            timeout=timeout,
            transport=transport,
        )

    monkeypatch.setattr("loom_cli.pipeline_cmd.authed_client", patched_client)
    return mock


def _write_json(tmp_path: Path, name: str, value: Any) -> str:
    path = tmp_path / name
    path.write_text(json.dumps(value), encoding="utf-8")
    return f"@{path}"


def _budget() -> dict[str, Any]:
    return {
        "max_artifact_bytes": 1,
        "max_attempts_total": 1,
        "max_gpu_seconds": 0,
        "max_provider_cost_usd": "0.000000",
        "max_stage_runs": 1,
        "max_wall_seconds": 1,
    }


def test_stage1_render_candidate_is_local_and_mutation_free(
    server: MockServer, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    candidate = _candidate()
    rc = main(
        [
            "pipeline",
            "stage1-smoke",
            "render-candidate",
            "--candidate",
            _write_json(tmp_path, "stage1-candidate.json", candidate.model_dump(mode="json")),
            "--json",
        ]
    )
    assert rc == 0
    rendered = json.loads(capsys.readouterr().out)
    assert rendered["candidate_sha256"] == candidate.candidate_sha256
    assert rendered["recipe"] == "behavior-stage1-smoke@1"
    assert rendered["stage_count"] == 1
    assert rendered["network_profile"] == "none"
    assert server.requests == []


def test_stage1_two_phase_live_commands_use_exact_hidden_routes(
    server: MockServer, tmp_path: Path
) -> None:
    candidate = _candidate()
    now = datetime(2026, 8, 13, 16, tzinfo=UTC)
    authorization = Stage1SmokeAuthorizationV1(
        schema_version="loom.behavior-stage1-smoke-authorization.v1",
        action="stage1",
        authorization_id=UUID("00000000-0000-4000-8000-000000000099"),
        candidate_sha256=candidate.candidate_sha256,
        operator_user_id=candidate.operator_user_id,
        team_id=candidate.team_id,
        environment=candidate.environment,
        loom_commit_sha=candidate.loom_commit_sha,
        recipe_digest=candidate.recipe_digest,
        image_index_digest=candidate.image_index_digest,
        platform=candidate.platform,
        platform_child_digest=candidate.platform_child_digest,
        backend_variant_id=candidate.backend_variant_id,
        policy_id=candidate.policy_id,
        policy_config_sha256=candidate.policy_config_sha256,
        policy_activation_epoch=candidate.policy_activation_epoch,
        input_descriptor_set_sha256=canonical_digest(candidate.inputs),
        run_budget_sha256=canonical_digest(candidate.run_budget),
        start_by=candidate.start_by,
        cleanup_deadline=candidate.cleanup_deadline,
        live_mutation_authorized=True,
        authorized_at=now,
        expires_at=now + timedelta(minutes=5),
        nonce_sha256="sha256:" + "9" * 64,
    )
    candidate_path = _write_json(
        tmp_path, "stage1-candidate.json", candidate.model_dump(mode="json")
    )
    authorization_path = _write_json(
        tmp_path, "stage1-authorization.json", authorization.model_dump(mode="json")
    )
    signature = tmp_path / "signature.hex"
    signature.write_text("ab" * 64, encoding="ascii")
    server.add(
        "POST",
        "/api/v1/internal/pipeline-stage1-smoke/capacity-preflight",
        httpx.Response(201, json={"state": "capacity_pending"}),
    )

    assert main(
        [
            "pipeline",
            "stage1-smoke",
            "capacity-preflight",
            "--candidate",
            candidate_path,
            "--authorization",
            authorization_path,
            "--confirm-candidate-sha",
            candidate.candidate_sha256,
            "--idempotency-key",
            "stage1-capacity-1",
            "--signature-key-id",
            "pipeline-stage1-operator-v1",
            "--signature",
            f"@{signature}",
            "--json",
        ]
    ) == 0

    request = server.requests[-1]
    assert request.url.path == "/api/v1/internal/pipeline-stage1-smoke/capacity-preflight"
    assert request.headers["Idempotency-Key"] == "stage1-capacity-1"
    assert json.loads(request.content) == {
        "candidate": candidate.model_dump(mode="json"),
        "authorization": authorization.model_dump(mode="json"),
    }


def test_run_posts_exact_body_and_idempotency_header(
    server: MockServer, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    server.add(
        "POST",
        "/api/v1/pipeline-runs",
        httpx.Response(201, json={"pipeline_run_id": RUN_ID, "state": "submitted"}),
    )
    rc = main(
        [
            "pipeline",
            "run",
            "--recipe",
            "behavior-recovery@1",
            "--input",
            f"task_set={ARTIFACT_ID}",
            "--params",
            _write_json(tmp_path, "params.json", {"seed": 7}),
            "--budget",
            _write_json(tmp_path, "budget.json", _budget()),
            "--idempotency-key",
            "pipeline-run-1",
            "--json",
        ]
    )
    assert rc == 0
    assert json.loads(capsys.readouterr().out)["pipeline_run_id"] == RUN_ID
    request = server.requests[-1]
    assert request.headers["Idempotency-Key"] == "pipeline-run-1"
    assert json.loads(request.content) == {
        "budget": _budget(),
        "display_name": None,
        "inputs": {"task_set": ARTIFACT_ID},
        "judge_profile_id": None,
        "parameters": {"seed": 7},
        "recipe": "behavior-recovery@1",
    }


def test_run_rejects_non_strict_budget_before_api(server: MockServer, tmp_path: Path) -> None:
    budget = {**_budget(), "raw_graph": {}}
    rc = main(
        [
            "pipeline",
            "run",
            "--recipe",
            "behavior-recovery@1",
            "--input",
            f"task_set={ARTIFACT_ID}",
            "--params",
            _write_json(tmp_path, "params.json", {}),
            "--budget",
            _write_json(tmp_path, "budget.json", budget),
            "--idempotency-key",
            "strict-budget",
            "--json",
        ]
    )
    assert rc == 1
    assert not server.requests


def test_run_sends_selected_judge_profile(server: MockServer, tmp_path: Path) -> None:
    server.add(
        "POST",
        "/api/v1/pipeline-runs",
        httpx.Response(201, json={"pipeline_run_id": RUN_ID, "state": "submitted"}),
    )
    assert (
        main(
            [
                "pipeline",
                "run",
                "--recipe",
                "behavior-recovery@1",
                "--input",
                f"task_set={ARTIFACT_ID}",
                "--params",
                _write_json(tmp_path, "params.json", {}),
                "--budget",
                _write_json(tmp_path, "budget.json", _budget()),
                "--idempotency-key",
                "profile-run",
                "--judge-profile",
                TASK_SET_ID,
            ]
        )
        == 0
    )
    assert json.loads(server.requests[-1].content)["judge_profile_id"] == TASK_SET_ID


def test_materialize_expands_defaults_and_rejects_extra_parameters(
    server: MockServer, tmp_path: Path
) -> None:
    path = "/api/v1/pipeline-recipes/behavior-recovery/1/materialize-inputs"
    server.add(
        "POST", path, httpx.Response(201, json={"materialization_id": RUN_ID, "state": "committed"})
    )
    arguments = [
        "pipeline",
        "materialize-inputs",
        "--recipe",
        "behavior-recovery@1",
        "--task-set",
        TASK_SET_ID,
        "--input",
        f"dataset={RUN_ID}",
        "--input",
        f"policy={STAGE_ID}",
        "--input",
        f"mop_bank={ARTIFACT_ID}",
        "--params",
        _write_json(tmp_path, "params.json", {}),
        "--idempotency-key",
        "materialize-1",
    ]
    assert main(arguments) == 0
    assert json.loads(server.requests[-1].content) == {
        "inputs": {"dataset": RUN_ID, "policy": STAGE_ID, "mop_bank": ARTIFACT_ID},
        "parameters": {"episodes_per_instance": 1, "seed_base": 0},
        "task_set_id": TASK_SET_ID,
    }
    bad = arguments.copy()
    bad[bad.index(_write_json(tmp_path, "params.json", {}))] = _write_json(
        tmp_path, "bad.json", {"extra": 1}
    )
    assert main(bad) == 1
    assert len(server.requests) == 1


def test_watch_uses_last_sequence_and_suppresses_duplicates(
    server: MockServer, capsys: pytest.CaptureFixture[str]
) -> None:
    path = f"/api/v1/pipeline-runs/{RUN_ID}/events"
    server.add(
        "GET",
        path,
        httpx.Response(
            200,
            json={
                "items": [{"seq": 1, "state": "submitted"}],
                "next_after_seq": 1,
                "terminal": False,
                "retry_after_ms": 0,
            },
        ),
    )
    server.add(
        "GET",
        path,
        httpx.Response(
            200,
            json={
                "items": [{"seq": 1, "state": "submitted"}, {"seq": 2, "state": "finished"}],
                "next_after_seq": 2,
                "terminal": True,
            },
        ),
    )
    assert main(["pipeline", "watch", RUN_ID, "--json", "--poll-interval", "0"]) == 0
    rows = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert [row["seq"] for row in rows] == [1, 2]
    assert [request.url.params["after_seq"] for request in server.requests] == ["0", "1"]
    assert all(request.method == "GET" for request in server.requests)


def test_watch_ctrl_c_never_cancels(server: MockServer, monkeypatch: pytest.MonkeyPatch) -> None:
    path = f"/api/v1/pipeline-runs/{RUN_ID}/events"
    server.add(
        "GET",
        path,
        httpx.Response(
            200, json={"items": [], "next_after_seq": 0, "terminal": False, "retry_after_ms": 0}
        ),
    )

    def interrupt(_seconds: float) -> None:
        raise KeyboardInterrupt

    monkeypatch.setattr("loom_cli.pipeline_cmd.time.sleep", interrupt)
    assert main(["pipeline", "watch", RUN_ID, "--poll-interval", "0"]) == 130
    assert [(request.method, request.url.path) for request in server.requests] == [("GET", path)]


def test_download_streams_direct_route_and_reports_digest(
    server: MockServer, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    payload = b"artifact-stream-content"
    path = f"/api/v1/pipeline-artifacts/{ARTIFACT_ID}/download"
    server.add(
        "GET",
        path,
        httpx.Response(200, content=payload, headers={"content-type": "application/octet-stream"}),
    )
    output = tmp_path / "artifact.bin"
    assert main(["pipeline", "download", ARTIFACT_ID, "--output", str(output), "--json"]) == 0
    assert output.read_bytes() == payload
    report = json.loads(capsys.readouterr().out)
    assert report["sha256"] == f"sha256:{hashlib.sha256(payload).hexdigest()}"
    assert server.requests[-1].url.path == path


def test_download_rejects_redirect_without_exposing_internal_url(
    server: MockServer, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    path = f"/api/v1/pipeline-artifacts/{ARTIFACT_ID}/download"
    server.add(
        "GET",
        path,
        httpx.Response(
            307, headers={"location": "http://minio:9000/private?X-Amz-Signature=secret"}
        ),
    )
    output = tmp_path / "no.bin"
    assert main(["pipeline", "download", ARTIFACT_ID, "--output", str(output), "--json"]) == 1
    error = capsys.readouterr().err
    assert "unsafe_download_redirect" in error
    assert "minio" not in error.lower()
    assert not output.exists()


def test_structured_api_error_preserves_safe_fields_and_redacts_message(
    server: MockServer, capsys: pytest.CaptureFixture[str]
) -> None:
    server.add(
        "POST",
        f"/api/v1/pipeline-runs/{RUN_ID}/cancel",
        httpx.Response(
            409,
            json={
                "detail": {
                    "reason_code": "controller_owned_run",
                    "message": "hidden http://minio:9000/a?X-Amz-Signature=secret loom_admin_secret123",
                }
            },
        ),
    )
    assert main(["pipeline", "cancel", RUN_ID, "--reason", "stop", "--json"]) == 1
    error = json.loads(capsys.readouterr().err)
    assert error["status"] == 409
    assert error["reason_code"] == "controller_owned_run"
    assert "secret123" not in error["message"]
    assert "minio" not in error["message"]


def test_recipe_and_profile_paths_are_fixed(server: MockServer) -> None:
    server.add(
        "GET",
        "/api/v1/pipeline-recipes/behavior-recovery/1",
        httpx.Response(200, json={"name": "behavior-recovery", "version": "1"}),
    )
    server.add(
        "GET",
        "/api/v1/pipeline-recipes/behavior-recovery/1/judge-profiles",
        httpx.Response(200, json={"items": []}),
    )
    assert main(["pipeline", "recipes", "behavior-recovery@1"]) == 0
    assert main(["pipeline", "judge-profiles", "--recipe", "behavior-recovery@1", "--json"]) == 0
    assert [request.url.path for request in server.requests] == [
        "/api/v1/pipeline-recipes/behavior-recovery/1",
        "/api/v1/pipeline-recipes/behavior-recovery/1/judge-profiles",
    ]


def test_retry_stage_help_states_full_replay(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as raised:
        main(["pipeline", "retry-stage", "--help"])
    assert raised.value.code == 0
    help_text = capsys.readouterr().out
    assert "new full replay PipelineRun" in help_text
    assert "never reopen" in help_text


def test_deterministic_import_bundle_and_multipart_flow(server: MockServer, tmp_path: Path) -> None:
    root = tmp_path / "payload"
    root.mkdir()
    content = b"declared payload\n"
    (root / "data.bin").write_bytes(content)
    manifest = {
        "schema_version": "behavior.input-import.v1",
        "kind": "policy",
        "name": "fixture",
        "version": "1",
        "upstream": {"type": "test", "locator": "fixture", "revision": "1"},
        "compatibility": {},
        "files": [
            {
                "path": "data.bin",
                "sha256": hashlib.sha256(content).hexdigest(),
                "size_bytes": len(content),
                "media_type": "application/octet-stream",
            }
        ],
    }
    first = tmp_path / "first.tar.zst"
    second = tmp_path / "second.tar.zst"
    _build_bundle(root, manifest, first)
    _build_bundle(root, manifest, second)
    assert first.read_bytes() == second.read_bytes()

    create_path = "/api/v1/pipeline-input-imports"
    renew_path = f"/api/v1/pipeline-input-imports/{IMPORT_ID}/renew-upload-token"
    part_path = f"/api/v1/pipeline-input-imports/{IMPORT_ID}/parts/1"
    complete_path = f"/api/v1/pipeline-input-imports/{IMPORT_ID}/complete"
    server.add(
        "POST",
        create_path,
        httpx.Response(
            201,
            json={
                "import_id": IMPORT_ID,
                "session_id": SESSION_ID,
                "part_size_bytes": 1024 * 1024,
                "upload_grant": {"token": "create-token", "expires_at": "2026-08-12T00:00:00Z"},
            },
        ),
    )
    server.add(
        "POST",
        renew_path,
        httpx.Response(
            200,
            json={
                "part_size_bytes": 1024 * 1024,
                "upload_grant": {"token": "renewed-token", "expires_at": "2036-08-12T00:15:00Z"},
            },
        ),
    )
    server.add(
        "PUT",
        part_path,
        httpx.Response(
            200,
            json={
                "file_index": 0,
                "part_number": 1,
                "sha256": "sha256:" + "a" * 64,
                "size_bytes": first.stat().st_size,
            },
        ),
    )
    server.add(
        "POST",
        complete_path,
        httpx.Response(
            201, json={"import_id": IMPORT_ID, "artifact_id": ARTIFACT_ID, "state": "committed"}
        ),
    )
    manifest_path = _write_json(tmp_path, "manifest.json", manifest)
    assert (
        main(
            [
                "pipeline",
                "import-input",
                "--recipe",
                "behavior-recovery@1",
                "--kind",
                "policy",
                "--manifest",
                manifest_path,
                "--root",
                str(root),
                "--idempotency-key",
                "import-1",
            ]
        )
        == 0
    )
    assert [(request.method, request.url.path) for request in server.requests] == [
        ("POST", create_path),
        ("POST", renew_path),
        ("PUT", part_path),
        ("POST", complete_path),
    ]
    assert server.requests[2].headers["X-Loom-Upload-Token"] == "renewed-token"
    completion = json.loads(server.requests[3].content)
    assert completion["upload_session_id"] == SESSION_ID
    assert completion["parts"][0]["part_number"] == 1


def test_import_ctrl_c_aborts_with_fresh_idempotency_key(
    server: MockServer, tmp_path: Path
) -> None:
    root = tmp_path / "payload"
    root.mkdir()
    content = b"payload"
    (root / "data.bin").write_bytes(content)
    manifest = {
        "schema_version": "behavior.input-import.v1",
        "kind": "policy",
        "name": "fixture",
        "version": "1",
        "upstream": {"type": "test", "locator": "fixture", "revision": "1"},
        "compatibility": {},
        "files": [
            {
                "path": "data.bin",
                "sha256": hashlib.sha256(content).hexdigest(),
                "size_bytes": len(content),
                "media_type": "application/octet-stream",
            }
        ],
    }
    create_path = "/api/v1/pipeline-input-imports"
    renew_path = f"/api/v1/pipeline-input-imports/{IMPORT_ID}/renew-upload-token"
    part_path = f"/api/v1/pipeline-input-imports/{IMPORT_ID}/parts/1"
    abort_path = f"/api/v1/pipeline-input-imports/{IMPORT_ID}/abort"
    server.add(
        "POST",
        create_path,
        httpx.Response(
            201,
            json={
                "import_id": IMPORT_ID,
                "session_id": SESSION_ID,
                "part_size_bytes": 1024 * 1024,
                "upload_grant": {"token": "old", "expires_at": "soon"},
            },
        ),
    )
    server.add(
        "POST",
        renew_path,
        httpx.Response(
            200, json={"upload_grant": {"token": "fresh", "expires_at": "2036-08-12T00:15:00Z"}}
        ),
    )
    server.interrupts.add(("PUT", part_path))
    server.add(
        "POST", abort_path, httpx.Response(200, json={"import_id": IMPORT_ID, "state": "aborted"})
    )
    assert (
        main(
            [
                "pipeline",
                "import-input",
                "--recipe",
                "behavior-recovery@1",
                "--kind",
                "policy",
                "--manifest",
                _write_json(tmp_path, "manifest.json", manifest),
                "--root",
                str(root),
                "--idempotency-key",
                "import-interrupt",
            ]
        )
        == 130
    )
    abort = server.requests[-1]
    assert abort.url.path == abort_path
    assert abort.headers["Idempotency-Key"].startswith("cli-abort-")
    assert json.loads(abort.content) == {
        "reason": "client interrupted",
        "upload_session_id": SESSION_ID,
    }
