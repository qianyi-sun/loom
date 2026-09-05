from __future__ import annotations

import asyncio
import json
import socket
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import urlsplit
from uuid import uuid4

import httpx
import pytest
import uvicorn
from scripts.ops import issue_1748_deadline_canary as canary
from scripts.ops.issue_1748_deadline_canary import (
    FaultProviderConfig,
    FaultProviderState,
    assert_local_candidate_binding,
    build_local_transport_evidence,
    create_fault_provider_app,
    main,
    validate_local_transport_evidence,
)

_FIXTURE_CANDIDATE_SHA = "a" * 40
_FIXTURE_CANDIDATE_TREE = "b" * 40


@asynccontextmanager
async def _serve(
    app: object,
    *,
    graceful_shutdown_sec: int = 2,
) -> AsyncIterator[str]:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("127.0.0.1", 0))
    sock.listen(128)
    host, port = sock.getsockname()
    server = uvicorn.Server(
        uvicorn.Config(
            app,
            log_level="error",
            access_log=False,
            lifespan="on",
            timeout_graceful_shutdown=graceful_shutdown_sec,
        )
    )
    task = asyncio.create_task(server.serve(sockets=[sock]))
    for _ in range(200):
        if server.started:
            break
        if task.done():
            await task
        await asyncio.sleep(0.005)
    else:
        raise AssertionError("test HTTP server did not start")
    try:
        yield f"http://{host}:{port}"
    finally:
        server.should_exit = True
        await asyncio.wait_for(task, timeout=5)


def _config(*, case: str, hold_sec: float = 0.08) -> FaultProviderConfig:
    return FaultProviderConfig(
        case=case,
        # Synthetic bindings exercise the evidence schema only. The manual CLI
        # separately verifies the exact clean checkout before serving.
        candidate_sha=_FIXTURE_CANDIDATE_SHA,
        candidate_tree=_FIXTURE_CANDIDATE_TREE,
        trial_id=uuid4(),
        step_id="main",
        nonce="local-capability-value",
        deadline_budget_sec=0.05,
        hold_sec=hold_sec,
    )


def _case_a_gateway_outcomes() -> list[dict[str, object]]:
    return [
        {
            "phase": "initial_deadline",
            "case_attempt_ordinal": 1,
            "http_status": 504,
            "detail_code": "agent_timeout",
            "detail_reason": "attempt_deadline_reached",
            "request_started_at": "2026-09-04T12:00:00+00:00",
            "response_received_at": "2026-09-04T12:00:00.060000+00:00",
            "signed_deadline_wall_clock": "2026-09-04T12:00:00.050000+00:00",
            "grant_expires_at": "2026-09-04T12:06:00+00:00",
            "provider_request_count_before": 0,
            "provider_request_count_after": 1,
        },
        {
            "phase": "expired_deadline_replay",
            "case_attempt_ordinal": 1,
            "http_status": 504,
            "detail_code": "agent_timeout",
            "detail_reason": "attempt_deadline_reached",
            "request_started_at": "2026-09-04T12:00:00.070000+00:00",
            "response_received_at": "2026-09-04T12:00:00.080000+00:00",
            "signed_deadline_wall_clock": "2026-09-04T12:00:00.050000+00:00",
            "grant_expires_at": "2026-09-04T12:06:00+00:00",
            "provider_request_count_before": 1,
            "provider_request_count_after": 1,
        },
    ]


@pytest.mark.asyncio
async def test_case_b_real_http_holds_first_and_allows_exactly_one_fresh_attempt() -> None:
    config = _config(case="B")
    state = FaultProviderState(config)
    app = create_fault_provider_app(state)

    async with _serve(app) as base_url:
        url = f"{base_url}/{config.nonce}/v1/responses"
        async with httpx.AsyncClient() as client:
            with pytest.raises(httpx.ReadTimeout):
                await client.post(
                    url,
                    json={"model": "must-not-be-recorded", "input": "secret-canary-body"},
                    headers={"Authorization": "Bearer must-not-be-recorded"},
                    timeout=0.02,
                )
            await asyncio.sleep(config.hold_sec + 0.03)
            completed = await client.post(url, json={"model": "canary-model"})
            replay = await client.post(url, json={"model": "canary-model"})

    assert completed.status_code == 200
    assert completed.json()["id"] == "issue-1748-canary"
    assert replay.status_code == 409
    assert replay.json()["detail"]["code"] == "fault_provider_request_limit_reached"

    snapshot = state.snapshot()
    assert [item["outcome"] for item in snapshot["requests"]] == ["held", "completed"]
    serialized = json.dumps(snapshot, sort_keys=True)
    assert config.nonce not in serialized
    assert "must-not-be-recorded" not in serialized
    assert "secret-canary-body" not in serialized
    assert "authorization" not in serialized.lower()
    assert snapshot["rejected_request_count"] == 1


@pytest.mark.asyncio
async def test_real_http_slow_body_times_out_without_consuming_request_slot() -> None:
    config = _config(case="A")
    state = FaultProviderState(config)
    app = create_fault_provider_app(state, request_body_timeout_sec=0.05)

    async with _serve(app) as base_url:
        parsed = urlsplit(base_url)
        reader, writer = await asyncio.open_connection(parsed.hostname, parsed.port)
        writer.write(
            (
                f"POST /{config.nonce}/v1/responses HTTP/1.1\r\n"
                f"Host: {parsed.hostname}\r\n"
                "Content-Length: 100\r\n"
                "Content-Type: application/json\r\n\r\n"
                "{"
            ).encode()
        )
        await writer.drain()
        status_line = await asyncio.wait_for(reader.readline(), timeout=1)
        writer.close()
        await writer.wait_closed()

    assert status_line.startswith(b"HTTP/1.1 408")
    assert state.snapshot()["requests"] == []


@pytest.mark.asyncio
async def test_real_server_shutdown_is_bounded_with_incomplete_body() -> None:
    config = _config(case="A")
    state = FaultProviderState(config)
    app = create_fault_provider_app(state, request_body_timeout_sec=30)
    writer: asyncio.StreamWriter | None = None

    started = asyncio.get_running_loop().time()
    async with _serve(app, graceful_shutdown_sec=1) as base_url:
        parsed = urlsplit(base_url)
        _, writer = await asyncio.open_connection(parsed.hostname, parsed.port)
        writer.write(
            (
                f"POST /{config.nonce}/v1/responses HTTP/1.1\r\n"
                f"Host: {parsed.hostname}\r\n"
                "Content-Length: 100\r\n\r\n"
                "{"
            ).encode()
        )
        await writer.drain()
        await asyncio.sleep(0.02)
    elapsed = asyncio.get_running_loop().time() - started
    if writer is not None:
        writer.close()
        await writer.wait_closed()

    assert elapsed < 2
    assert state.snapshot()["requests"] == []


def test_candidate_binding_rejects_mismatched_or_dirty_checkout(tmp_path: Path) -> None:
    import subprocess

    subprocess.run(["git", "init", "-q"], cwd=tmp_path, check=True)
    subprocess.run(
        ["git", "config", "user.email", "canary@example.invalid"], cwd=tmp_path, check=True
    )
    subprocess.run(["git", "config", "user.name", "Canary"], cwd=tmp_path, check=True)
    tracked = tmp_path / "tracked.txt"
    tracked.write_text("clean\n", encoding="utf-8")
    subprocess.run(["git", "add", "tracked.txt"], cwd=tmp_path, check=True)
    subprocess.run(["git", "commit", "-qm", "fixture"], cwd=tmp_path, check=True)
    sha = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=tmp_path, text=True).strip()
    tree = subprocess.check_output(
        ["git", "rev-parse", "HEAD^{tree}"], cwd=tmp_path, text=True
    ).strip()

    assert_local_candidate_binding(repo_root=tmp_path, candidate_sha=sha, candidate_tree=tree)
    with pytest.raises(ValueError, match="does not match"):
        assert_local_candidate_binding(
            repo_root=tmp_path,
            candidate_sha="c" * 40,
            candidate_tree=tree,
        )

    tracked.write_text("dirty\n", encoding="utf-8")
    with pytest.raises(ValueError, match="clean checkout"):
        assert_local_candidate_binding(repo_root=tmp_path, candidate_sha=sha, candidate_tree=tree)


def test_local_transport_evidence_is_candidate_bound_and_never_claims_full_acceptance(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    config = _config(case="A", hold_sec=0.1)
    state = FaultProviderState(config)
    state.record_for_test(outcome="held")

    evidence = build_local_transport_evidence(
        config=config,
        provider_snapshot=state.snapshot(),
        gateway_outcomes=_case_a_gateway_outcomes(),
        harness_path=Path("scripts/ops/issue_1748_deadline_canary.py"),
        gateway_route="/v1/responses",
    )

    validate_local_transport_evidence(evidence)
    assert evidence["scope"] == "local_real_http_transport_only"
    assert evidence["full_canary_passed"] is False
    assert evidence["candidate_sha"] == config.candidate_sha
    assert evidence["candidate_tree"] == config.candidate_tree
    assert evidence["missing_acceptance_layers"] == [
        "control_plane_persistence",
        "worker_supervision_and_retry",
        "canonical_trajectory",
        "atif_artifact",
        "deployed_image_and_route_readback",
        "post_run_worker_pool_readback",
    ]

    output = tmp_path / "evidence.json"
    output.write_text(json.dumps(evidence), encoding="utf-8")
    assert config.nonce not in output.read_text(encoding="utf-8")
    assert main(["validate", "--input", str(output)]) == 0
    assert capsys.readouterr().out == (
        "issue-1748 local transport evidence valid (not full canary acceptance)\n"
    )


def test_local_transport_evidence_rejects_secret_bearing_or_mismatched_input() -> None:
    config = _config(case="A")
    state = FaultProviderState(config)
    state.record_for_test(outcome="held")
    evidence = build_local_transport_evidence(
        config=config,
        provider_snapshot=state.snapshot(),
        gateway_outcomes=_case_a_gateway_outcomes(),
        harness_path=Path("scripts/ops/issue_1748_deadline_canary.py"),
        gateway_route="/v1/responses",
    )

    evidence["candidate_sha"] = "c" * 40
    with pytest.raises(ValueError, match="candidate binding"):
        validate_local_transport_evidence(evidence)

    evidence["candidate_sha"] = config.candidate_sha
    evidence["raw_header"] = "Bearer forbidden"
    with pytest.raises(ValueError, match="missing or unexpected"):
        validate_local_transport_evidence(evidence)


def test_builder_rejects_capability_in_freeform_route() -> None:
    config = _config(case="A")
    state = FaultProviderState(config)
    state.record_for_test(outcome="held")
    with pytest.raises(ValueError, match="fixed Gateway route"):
        build_local_transport_evidence(
            config=config,
            provider_snapshot=state.snapshot(),
            gateway_outcomes=_case_a_gateway_outcomes(),
            harness_path=Path("scripts/ops/issue_1748_deadline_canary.py"),
            gateway_route=f"/{config.nonce}/v1/responses",
        )


def test_validator_rejects_bound_capability_in_fixed_string_field() -> None:
    config = _config(case="A")
    state = FaultProviderState(config)
    state.record_for_test(outcome="held")
    evidence = build_local_transport_evidence(
        config=config,
        provider_snapshot=state.snapshot(),
        gateway_outcomes=_case_a_gateway_outcomes(),
        harness_path=Path("scripts/ops/issue_1748_deadline_canary.py"),
        gateway_route="/v1/responses",
    )
    evidence["step_id"] = f"prefix-{config.nonce}"
    evidence["provider_observation"]["step_id"] = f"prefix-{config.nonce}"

    with pytest.raises(ValueError, match="raw capability"):
        validate_local_transport_evidence(evidence)


def test_validator_rejects_success_relabelled_as_same_attempt() -> None:
    config = _config(case="B")
    state = FaultProviderState(config)
    state.record_for_test(outcome="held")
    state.record_for_test(outcome="completed")
    outcomes = _case_a_gateway_outcomes()
    outcomes.append(
        {
            **outcomes[-1],
            "phase": "fresh_deadline_grant",
            "http_status": 200,
            "detail_code": None,
            "detail_reason": None,
            "request_started_at": "2026-09-04T12:00:00.090000+00:00",
            "response_received_at": "2026-09-04T12:00:00.100000+00:00",
            "signed_deadline_wall_clock": "2026-09-04T12:00:02+00:00",
            "case_attempt_ordinal": 1,
            "provider_request_count_before": 1,
            "provider_request_count_after": 2,
        }
    )

    with pytest.raises(ValueError, match="fresh deadline grant outcome"):
        build_local_transport_evidence(
            config=config,
            provider_snapshot=state.snapshot(),
            gateway_outcomes=outcomes,
            harness_path=Path("scripts/ops/issue_1748_deadline_canary.py"),
            gateway_route="/v1/responses",
        )


def test_public_cli_sanitizes_invalid_capability_environment(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    sensitive_nonce = "s3cr3t"
    monkeypatch.setenv("LOOM_1748_CANARY_NONCE", sensitive_nonce)
    monkeypatch.setattr(canary, "assert_local_candidate_binding", lambda **_kwargs: None)

    result = canary.main(
        [
            "serve",
            "--case",
            "A",
            "--candidate-sha",
            _FIXTURE_CANDIDATE_SHA,
            "--candidate-tree",
            _FIXTURE_CANDIDATE_TREE,
            "--trial-id",
            str(uuid4()),
            "--step-id",
            "main",
            "--deadline-budget-sec",
            "10",
            "--hold-sec",
            "15",
            "--output",
            str(tmp_path / "unused.json"),
        ]
    )

    assert result == 2
    captured = capsys.readouterr()
    assert sensitive_nonce not in captured.out
    assert sensitive_nonce not in captured.err
    assert captured.err == "issue-1748 local canary failed safely; inspect configuration\n"


def test_manual_server_configures_bounded_graceful_shutdown(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    nonce = "n" * 32
    observed: dict[str, object] = {}
    output = tmp_path / "provider-observation.json"
    monkeypatch.setenv("LOOM_1748_CANARY_NONCE", nonce)
    monkeypatch.setattr(canary, "assert_local_candidate_binding", lambda **_kwargs: None)

    def fake_run(_app: object, **kwargs: object) -> None:
        observed.update(kwargs)

    monkeypatch.setattr(canary.uvicorn, "run", fake_run)

    result = canary.main(
        [
            "serve",
            "--case",
            "A",
            "--candidate-sha",
            _FIXTURE_CANDIDATE_SHA,
            "--candidate-tree",
            _FIXTURE_CANDIDATE_TREE,
            "--trial-id",
            str(uuid4()),
            "--step-id",
            "main",
            "--deadline-budget-sec",
            "10",
            "--hold-sec",
            "15",
            "--output",
            str(output),
        ]
    )

    assert result == 0
    assert observed["timeout_graceful_shutdown"] == 2
    serialized = output.read_text(encoding="utf-8")
    assert nonce not in serialized
    assert "harness_sha256" not in serialized


@pytest.mark.parametrize(
    "raw_document",
    [
        '{"full_canary_passed": true, "full_canary_passed": false}',
        '{"full_canary_passed": NaN}',
        '{"full_canary_passed": Infinity}',
    ],
)
def test_validator_cli_rejects_ambiguous_or_nonfinite_json(
    raw_document: str,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    input_path = tmp_path / "invalid-evidence.json"
    input_path.write_text(raw_document, encoding="utf-8")

    assert main(["validate", "--input", str(input_path)]) == 2
    captured = capsys.readouterr()
    assert "valid" not in captured.out
    assert captured.err == "issue-1748 local canary failed safely; inspect configuration\n"
