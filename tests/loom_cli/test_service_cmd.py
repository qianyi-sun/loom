"""`loom service {up,down,status}` argparse + dispatch.

The actual docker-compose / alembic / seed_test_data calls are
subprocess invocations against a real docker daemon — exercised by
manual smoke + tests/system/ (gated). Here we only verify the CLI
surface, missing-compose-file handling, and that the right argv
shapes flow into our subprocess wrappers.
"""

from __future__ import annotations

import io
import json
import stat
import subprocess
import tomllib
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import httpx
import pytest

from loom_cli.__main__ import main
from loom_cli.service_cmd import _compose_args, _mutable_dev_images

_GENERIC_EXPECTED_DENIAL_ERROR = (
    "error: expected hidden-resource denial was not observed\n"
)
_UPDATE_DENIAL_RECEIPT = (
    '{"error_code":"resource_hidden","http_method":"PUT",'
    '"schema":"loom-personal-dev-expected-hidden-denial-v1","status":404,'
    '"target_phase":"target_update"}\n'
)


def _read_admin_token(secret_file: Path) -> str:
    data = tomllib.loads(secret_file.read_text(encoding="utf-8"))
    return data["admin"]["token"]


def test_help_lists_subcommands(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit) as exc:
        main(["service", "--help"])
    assert exc.value.code == 0
    out = capsys.readouterr().out
    assert "up" in out and "down" in out and "status" in out


def test_service_up_requires_explicit_environment(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit) as exc:
        main(["service", "up"])

    assert exc.value.code == 2
    assert "--environment" in capsys.readouterr().err


@pytest.mark.parametrize("environment", ["staging", "production"])
def test_protected_service_up_fails_before_mutation_without_candidate(
    environment: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    rc = main(["service", "up", "--environment", environment])

    assert rc == 2
    assert "--candidate is required" in capsys.readouterr().err


def test_personal_service_up_requires_a_content_digest_candidate(
    capsys: pytest.CaptureFixture[str],
) -> None:
    rc = main(
        [
            "service",
            "up",
            "--environment",
            "dev-alice",
            "--candidate",
            "a" * 40,
        ]
    )

    assert rc == 2
    assert "personal candidate must be a lowercase SHA-256 digest" in capsys.readouterr().err


def test_protected_service_up_requires_a_full_git_commit_candidate(
    capsys: pytest.CaptureFixture[str],
) -> None:
    rc = main(
        [
            "service",
            "up",
            "--environment",
            "staging",
            "--candidate",
            "a" * 64,
        ]
    )

    assert rc == 2
    assert "protected candidate must be a full lowercase Git commit" in capsys.readouterr().err


def test_local_service_up_rejects_personal_only_options_before_docker(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with patch(
        "loom_cli.service_cmd._up_local",
        side_effect=AssertionError("invalid target options must fail before Docker"),
    ):
        rc = main(
            [
                "service",
                "up",
                "--environment",
                "local",
                "--min-slots",
                "1",
            ]
        )

    assert rc == 2
    assert "personal-dev options are not valid" in capsys.readouterr().err


def test_personal_service_up_rejects_local_only_options_before_auth(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    with patch(
        "loom_cli.server_client.require_logged_in",
        side_effect=AssertionError("invalid target options must fail before auth"),
    ):
        rc = main(
            [
                "service",
                "up",
                "--environment",
                "dev-alice",
                "--compose-file",
                str(tmp_path / "compose.yml"),
            ]
        )

    assert rc == 2
    assert "local Compose options are not valid" in capsys.readouterr().err


def test_personal_service_up_routes_to_authenticated_lifecycle_without_docker(
    capsys: pytest.CaptureFixture[str],
) -> None:
    candidate_sha = "c" * 64
    candidate_id = "00000000-0000-0000-0000-000000000001"
    operation_id = "00000000-0000-0000-0000-000000000002"
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path == "/api/v1/personal-dev-candidates":
            return httpx.Response(
                200,
                json={
                    "items": [
                        {
                            "id": candidate_id,
                            "candidate_sha": candidate_sha,
                            "attestation_scope": "personal-dev-only",
                            "promotable": False,
                            "status": "ready",
                        }
                    ]
                },
            )
        if request.url.path == "/api/v1/dev-instances/alice" and request.method == "GET":
            return httpx.Response(404, json={"detail": "not found"})
        if request.url.path == "/api/v1/dev-instances/alice" and request.method == "PUT":
            payload = json.loads(request.content)
            assert payload["candidate_id"] == candidate_id
            assert payload["candidate_sha"] == candidate_sha
            assert payload["min_slots"] == 0
            assert payload["max_slots"] == 2
            assert payload["expected_operation_epoch"] == 0
            return httpx.Response(
                202,
                json={
                    "environment": {
                        "name": "alice",
                        "status": "provisioning",
                        "operation_epoch": 1,
                        "candidate_sha": candidate_sha,
                        "min_slots": 0,
                        "max_slots": 2,
                        "identity": {"route_host": "alice.dev.example"},
                    },
                    "operation": {
                        "id": operation_id,
                        "environment_name": "alice",
                        "candidate_sha": candidate_sha,
                        "min_slots": 0,
                        "max_slots": 2,
                        "expected_operation_epoch": 0,
                        "operation_epoch": 1,
                        "state": "running",
                    },
                },
            )
        raise AssertionError(f"unexpected request: {request.method} {request.url.path}")

    http_client = httpx.Client(
        base_url="https://loom.example",
        transport=httpx.MockTransport(handler),
    )
    try:
        with (
            patch(
                "loom_cli.server_client.require_logged_in",
                return_value=SimpleNamespace(server_url="https://loom.example"),
            ),
            patch("loom_cli.server_client.authed_client", return_value=http_client),
            patch(
                "loom_cli.service_cmd._up_local",
                side_effect=AssertionError("personal deployment must not touch Docker"),
            ),
        ):
            rc = main(
                [
                    "service",
                    "up",
                    "--environment",
                    "dev-alice",
                    "--candidate",
                    candidate_sha,
                    "--no-wait",
                ]
            )
    finally:
        http_client.close()

    assert rc == 0
    output = capsys.readouterr().out
    assert output == (
        f"→ resolving owned ready candidate {candidate_sha}\n"
        "→ applying dev-alice at expected operation epoch 0\n"
        "Development environment: dev-alice\n"
        "Status: provisioning\n"
        f"Candidate: {candidate_sha}\n"
        "Capacity: min 0 · max 2 shared slots\n"
        "URL: https://alice.dev.example\n"
    )
    assert [(request.method, request.url.path) for request in requests] == [
        ("GET", "/api/v1/dev-instances/alice"),
        ("GET", "/api/v1/personal-dev-candidates"),
        ("PUT", "/api/v1/dev-instances/alice"),
    ]


def test_personal_service_up_retries_a_failed_environment_at_its_current_epoch(
    capsys: pytest.CaptureFixture[str],
) -> None:
    candidate_sha = "c" * 64
    candidate_id = "00000000-0000-0000-0000-000000000001"
    operation_id = "00000000-0000-0000-0000-000000000002"
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.method == "GET" and request.url.path == "/api/v1/dev-instances/alice":
            return httpx.Response(
                200,
                json={
                    "name": "alice",
                    "status": "failed",
                    "operation_epoch": 3,
                    "candidate_sha": candidate_sha,
                    "min_slots": 0,
                    "max_slots": 2,
                    "identity": {},
                },
            )
        if request.url.path == "/api/v1/personal-dev-candidates":
            return httpx.Response(
                200,
                json={
                    "items": [
                        {
                            "id": candidate_id,
                            "candidate_sha": candidate_sha,
                            "attestation_scope": "personal-dev-only",
                            "promotable": False,
                            "status": "ready",
                        }
                    ]
                },
            )
        if request.method == "PUT" and request.url.path == "/api/v1/dev-instances/alice":
            payload = json.loads(request.content)
            assert payload["expected_operation_epoch"] == 3
            return httpx.Response(
                202,
                json={
                    "environment": {
                        "name": "alice",
                        "status": "provisioning",
                        "operation_epoch": 3,
                        "candidate_sha": candidate_sha,
                        "min_slots": 0,
                        "max_slots": 2,
                        "identity": {},
                    },
                    "operation": {
                        "id": operation_id,
                        "environment_name": "alice",
                        "candidate_sha": candidate_sha,
                        "min_slots": 0,
                        "max_slots": 2,
                        "expected_operation_epoch": 3,
                        "operation_epoch": 3,
                        "state": "running",
                    },
                },
            )
        raise AssertionError(f"unexpected request: {request.method} {request.url.path}")

    http_client = httpx.Client(
        base_url="https://loom.example",
        transport=httpx.MockTransport(handler),
    )
    try:
        with (
            patch(
                "loom_cli.server_client.require_logged_in",
                return_value=SimpleNamespace(server_url="https://loom.example"),
            ),
            patch("loom_cli.server_client.authed_client", return_value=http_client),
        ):
            rc = main(
                [
                    "service",
                    "up",
                    "--environment",
                    "dev-alice",
                    "--candidate",
                    candidate_sha,
                    "--no-wait",
                ]
            )
    finally:
        http_client.close()

    assert rc == 0
    assert "expected operation epoch 3" in capsys.readouterr().out
    assert [(request.method, request.url.path) for request in requests] == [
        ("GET", "/api/v1/dev-instances/alice"),
        ("GET", "/api/v1/personal-dev-candidates"),
        ("PUT", "/api/v1/dev-instances/alice"),
    ]


def test_personal_service_up_redeploys_a_deleted_retained_name_with_explicit_epoch(
    capsys: pytest.CaptureFixture[str],
) -> None:
    candidate_sha = "c" * 64
    candidate_id = "00000000-0000-0000-0000-000000000001"
    operation_id = "00000000-0000-0000-0000-000000000002"
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path == "/api/v1/personal-dev-candidates":
            return httpx.Response(
                200,
                json={
                    "items": [
                        {
                            "id": candidate_id,
                            "candidate_sha": candidate_sha,
                            "attestation_scope": "personal-dev-only",
                            "promotable": False,
                            "status": "ready",
                        }
                    ]
                },
            )
        if request.method == "PUT" and request.url.path == "/api/v1/dev-instances/alice":
            payload = json.loads(request.content)
            assert payload["candidate_id"] == candidate_id
            assert payload["candidate_sha"] == candidate_sha
            assert payload["expected_operation_epoch"] == 4
            return httpx.Response(
                202,
                json={
                    "environment": {
                        "name": "alice",
                        "status": "provisioning",
                        "operation_epoch": 5,
                        "candidate_sha": candidate_sha,
                        "min_slots": 0,
                        "max_slots": 2,
                        "identity": {},
                    },
                    "operation": {
                        "id": operation_id,
                        "environment_name": "alice",
                        "candidate_sha": candidate_sha,
                        "min_slots": 0,
                        "max_slots": 2,
                        "expected_operation_epoch": 4,
                        "operation_epoch": 5,
                        "state": "running",
                    },
                },
            )
        raise AssertionError(f"unexpected request: {request.method} {request.url.path}")

    http_client = httpx.Client(
        base_url="https://loom.example",
        transport=httpx.MockTransport(handler),
    )
    try:
        with (
            patch(
                "loom_cli.server_client.require_logged_in",
                return_value=SimpleNamespace(server_url="https://loom.example"),
            ),
            patch("loom_cli.server_client.authed_client", return_value=http_client),
        ):
            rc = main(
                [
                    "service",
                    "up",
                    "--environment",
                    "dev-alice",
                    "--candidate",
                    candidate_sha,
                    "--expected-operation-epoch",
                    "4",
                    "--no-wait",
                ]
            )
    finally:
        http_client.close()

    assert rc == 0
    assert capsys.readouterr().err == ""
    assert [(request.method, request.url.path) for request in requests] == [
        ("GET", "/api/v1/personal-dev-candidates"),
        ("PUT", "/api/v1/dev-instances/alice"),
    ]


def test_personal_update_expected_hidden_denial_binds_candidate_then_target_put(
    capsys: pytest.CaptureFixture[str],
) -> None:
    candidate_sha = "c" * 64
    candidate_id = "00000000-0000-0000-0000-000000000001"
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path == "/api/v1/personal-dev-candidates":
            return httpx.Response(
                200,
                json={
                    "items": [
                        {
                            "id": candidate_id,
                            "candidate_sha": candidate_sha,
                            "attestation_scope": "personal-dev-only",
                            "promotable": False,
                            "status": "ready",
                        }
                    ]
                },
            )
        if request.method == "PUT" and request.url.path == "/api/v1/dev-instances/alice":
            payload = json.loads(request.content)
            assert payload["candidate_id"] == candidate_id
            assert payload["candidate_sha"] == candidate_sha
            assert payload["expected_operation_epoch"] == 1
            assert payload["min_slots"] == 0
            return httpx.Response(
                404,
                json={"detail": "dev-private-target must not escape"},
                headers={
                    "X-Loom-Personal-Dev-Hidden-Denial-Phase": "target_update",
                },
            )
        raise AssertionError(f"unexpected request: {request.method} {request.url.path}")

    http_client = httpx.Client(
        base_url="https://loom.example",
        transport=httpx.MockTransport(handler),
    )
    try:
        with (
            patch(
                "loom_cli.server_client.require_logged_in",
                return_value=SimpleNamespace(server_url="https://loom.example"),
            ),
            patch("loom_cli.server_client.authed_client", return_value=http_client),
        ):
            rc = main(
                [
                    "service",
                    "up",
                    "--environment",
                    "dev-alice",
                    "--candidate",
                    candidate_sha,
                    "--expected-operation-epoch",
                    "1",
                    "--min-slots",
                    "0",
                    "--quiet",
                    "--expected-hidden-denial",
                ]
            )
    finally:
        http_client.close()

    captured = capsys.readouterr()
    assert rc == 1
    assert captured.out == ""
    assert captured.err == _UPDATE_DENIAL_RECEIPT
    assert [(request.method, request.url.path) for request in requests] == [
        ("GET", "/api/v1/personal-dev-candidates"),
        ("PUT", "/api/v1/dev-instances/alice"),
    ]


@pytest.mark.parametrize("phase", [None, "target_read", "target_update "])
def test_personal_update_expected_hidden_denial_requires_exact_put_404_marker(
    capsys: pytest.CaptureFixture[str],
    phase: str | None,
) -> None:
    candidate_sha = "c" * 64
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path == "/api/v1/personal-dev-candidates":
            return httpx.Response(
                200,
                json={
                    "items": [
                        {
                            "id": "00000000-0000-0000-0000-000000000001",
                            "candidate_sha": candidate_sha,
                            "attestation_scope": "personal-dev-only",
                            "promotable": False,
                            "status": "ready",
                        }
                    ]
                },
            )
        if request.method == "PUT" and request.url.path == "/api/v1/dev-instances/alice":
            return httpx.Response(
                404,
                json={"detail": "candidate disappeared before apply"},
                headers=(
                    {}
                    if phase is None
                    else {
                        "X-Loom-Personal-Dev-Hidden-Denial-Phase": phase,
                    }
                ),
            )
        raise AssertionError(f"unexpected request: {request.method} {request.url.path}")

    http_client = httpx.Client(
        base_url="https://loom.example",
        transport=httpx.MockTransport(handler),
    )
    try:
        with (
            patch(
                "loom_cli.server_client.require_logged_in",
                return_value=SimpleNamespace(server_url="https://loom.example"),
            ),
            patch("loom_cli.server_client.authed_client", return_value=http_client),
        ):
            rc = main(
                [
                    "service",
                    "up",
                    "--environment",
                    "dev-alice",
                    "--candidate",
                    candidate_sha,
                    "--expected-operation-epoch",
                    "1",
                    "--min-slots",
                    "0",
                    "--quiet",
                    "--expected-hidden-denial",
                ]
            )
    finally:
        http_client.close()

    captured = capsys.readouterr()
    assert rc == 2
    assert captured.out == ""
    assert captured.err == _GENERIC_EXPECTED_DENIAL_ERROR
    assert "candidate disappeared" not in captured.err
    assert [(request.method, request.url.path) for request in requests] == [
        ("GET", "/api/v1/personal-dev-candidates"),
        ("PUT", "/api/v1/dev-instances/alice"),
    ]


def test_personal_update_expected_hidden_denial_cannot_certify_candidate_failure(
    capsys: pytest.CaptureFixture[str],
) -> None:
    candidate_sha = "c" * 64
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(
            404,
            json={"detail": "dev-private-target must not escape"},
        )

    http_client = httpx.Client(
        base_url="https://loom.example",
        transport=httpx.MockTransport(handler),
    )
    try:
        with (
            patch(
                "loom_cli.server_client.require_logged_in",
                return_value=SimpleNamespace(server_url="https://loom.example"),
            ),
            patch("loom_cli.server_client.authed_client", return_value=http_client),
        ):
            rc = main(
                [
                    "service",
                    "up",
                    "--environment",
                    "dev-alice",
                    "--candidate",
                    candidate_sha,
                    "--expected-operation-epoch",
                    "1",
                    "--quiet",
                    "--expected-hidden-denial",
                ]
            )
    finally:
        http_client.close()

    captured = capsys.readouterr()
    assert rc == 2
    assert captured.out == ""
    assert captured.err == _GENERIC_EXPECTED_DENIAL_ERROR
    assert [(request.method, request.url.path) for request in requests] == [
        ("GET", "/api/v1/personal-dev-candidates")
    ]


def test_personal_update_expected_hidden_denial_hides_malformed_candidate_output(
    capsys: pytest.CaptureFixture[str],
) -> None:
    candidate_sha = "c" * 64
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, content=b"dev-private-target is not JSON")

    http_client = httpx.Client(
        base_url="https://loom.example",
        transport=httpx.MockTransport(handler),
    )
    try:
        with (
            patch(
                "loom_cli.server_client.require_logged_in",
                return_value=SimpleNamespace(server_url="https://loom.example"),
            ),
            patch("loom_cli.server_client.authed_client", return_value=http_client),
        ):
            rc = main(
                [
                    "service",
                    "up",
                    "--environment",
                    "dev-alice",
                    "--candidate",
                    candidate_sha,
                    "--expected-operation-epoch",
                    "1",
                    "--quiet",
                    "--expected-hidden-denial",
                ]
            )
    finally:
        http_client.close()

    captured = capsys.readouterr()
    assert rc == 2
    assert captured.out == ""
    assert captured.err == _GENERIC_EXPECTED_DENIAL_ERROR
    assert "dev-private-target" not in captured.err
    assert [(request.method, request.url.path) for request in requests] == [
        ("GET", "/api/v1/personal-dev-candidates")
    ]


@pytest.mark.parametrize("status", [200, 401, 403, 409, 500])
def test_personal_update_expected_hidden_denial_rejects_non_404_target_response(
    capsys: pytest.CaptureFixture[str],
    status: int,
) -> None:
    candidate_sha = "c" * 64
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path == "/api/v1/personal-dev-candidates":
            return httpx.Response(
                200,
                json={
                    "items": [
                        {
                            "id": "00000000-0000-0000-0000-000000000001",
                            "candidate_sha": candidate_sha,
                            "attestation_scope": "personal-dev-only",
                            "promotable": False,
                            "status": "ready",
                        }
                    ]
                },
            )
        return httpx.Response(
            status,
            json={"detail": "dev-private-target must not escape"},
        )

    http_client = httpx.Client(
        base_url="https://loom.example",
        transport=httpx.MockTransport(handler),
    )
    try:
        with (
            patch(
                "loom_cli.server_client.require_logged_in",
                return_value=SimpleNamespace(server_url="https://loom.example"),
            ),
            patch("loom_cli.server_client.authed_client", return_value=http_client),
        ):
            rc = main(
                [
                    "service",
                    "up",
                    "--environment",
                    "dev-alice",
                    "--candidate",
                    candidate_sha,
                    "--expected-operation-epoch",
                    "1",
                    "--quiet",
                    "--expected-hidden-denial",
                ]
            )
    finally:
        http_client.close()

    captured = capsys.readouterr()
    assert rc == 2
    assert captured.out == ""
    assert captured.err == _GENERIC_EXPECTED_DENIAL_ERROR
    assert "dev-private-target" not in captured.err
    assert [(request.method, request.url.path) for request in requests] == [
        ("GET", "/api/v1/personal-dev-candidates"),
        ("PUT", "/api/v1/dev-instances/alice"),
    ]


@pytest.mark.parametrize("missing", ["candidate", "epoch", "quiet"])
def test_personal_update_expected_hidden_denial_requires_probe_boundaries_before_auth(
    capsys: pytest.CaptureFixture[str],
    missing: str,
) -> None:
    command = [
        "service",
        "up",
        "--environment",
        "dev-alice",
        "--candidate",
        "c" * 64,
        "--expected-operation-epoch",
        "1",
        "--quiet",
        "--expected-hidden-denial",
    ]
    removals = {
        "candidate": ("--candidate", "c" * 64),
        "epoch": ("--expected-operation-epoch", "1"),
        "quiet": ("--quiet",),
    }
    for value in removals[missing]:
        command.remove(value)

    with patch(
        "loom_cli.server_client.require_logged_in",
        side_effect=AssertionError("invalid expected-denial mode must fail before auth"),
    ):
        rc = main(command)

    captured = capsys.readouterr()
    assert rc == 2
    assert captured.out == ""
    assert captured.err == _GENERIC_EXPECTED_DENIAL_ERROR


def test_personal_update_expected_hidden_denial_rejects_zero_epoch_before_auth(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with patch(
        "loom_cli.server_client.require_logged_in",
        side_effect=AssertionError("zero-epoch denial probe must fail before auth"),
    ):
        rc = main(
            [
                "service",
                "up",
                "--environment",
                "dev-alice",
                "--candidate",
                "c" * 64,
                "--expected-operation-epoch",
                "0",
                "--quiet",
                "--expected-hidden-denial",
            ]
        )

    captured = capsys.readouterr()
    assert rc == 2
    assert captured.out == ""
    assert captured.err == _GENERIC_EXPECTED_DENIAL_ERROR


def test_personal_service_up_quiet_suppresses_stdout_but_not_lifecycle_requests(
    capsys: pytest.CaptureFixture[str],
) -> None:
    candidate_sha = "c" * 64
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.method == "GET" and request.url.path == "/api/v1/dev-instances/alice":
            return httpx.Response(404, json={})
        if request.url.path == "/api/v1/personal-dev-candidates":
            return httpx.Response(200, json={"items": [{
                "id": "00000000-0000-0000-0000-000000000001",
                "candidate_sha": candidate_sha,
                "attestation_scope": "personal-dev-only", "promotable": False,
                "status": "ready",
            }]})
        if request.method == "PUT" and request.url.path == "/api/v1/dev-instances/alice":
            return httpx.Response(202, json={
                "environment": {
                    "name": "alice", "status": "provisioning", "operation_epoch": 1,
                    "candidate_sha": candidate_sha, "min_slots": 0, "max_slots": 2,
                    "identity": {},
                },
                "operation": {
                    "id": "00000000-0000-0000-0000-000000000002",
                    "environment_name": "alice", "candidate_sha": candidate_sha,
                    "min_slots": 0, "max_slots": 2, "expected_operation_epoch": 0,
                    "operation_epoch": 1, "state": "running",
                },
            })
        raise AssertionError(f"unexpected request: {request.method} {request.url.path}")

    http_client = httpx.Client(base_url="https://loom.example", transport=httpx.MockTransport(handler))
    try:
        with (
            patch("loom_cli.server_client.require_logged_in", return_value=SimpleNamespace(server_url="https://loom.example")),
            patch("loom_cli.server_client.authed_client", return_value=http_client),
        ):
            rc = main(["service", "up", "--environment", "dev-alice", "--candidate", candidate_sha,
                       "--no-wait", "--quiet"])
    finally:
        http_client.close()

    assert rc == 0
    assert capsys.readouterr().out == ""
    assert [(request.method, request.url.path) for request in requests] == [
        ("GET", "/api/v1/dev-instances/alice"),
        ("GET", "/api/v1/personal-dev-candidates"),
        ("PUT", "/api/v1/dev-instances/alice"),
    ]


def test_personal_service_up_quiet_keeps_denial_on_stderr(
    capsys: pytest.CaptureFixture[str],
) -> None:
    candidate_sha = "c" * 64

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET" and request.url.path == "/api/v1/dev-instances/alice":
            return httpx.Response(404, json={})
        if request.url.path == "/api/v1/personal-dev-candidates":
            return httpx.Response(403, json={"detail": "denied"})
        raise AssertionError(f"unexpected request: {request.method} {request.url.path}")

    http_client = httpx.Client(base_url="https://loom.example", transport=httpx.MockTransport(handler))
    try:
        with (
            patch("loom_cli.server_client.require_logged_in", return_value=SimpleNamespace(server_url="https://loom.example")),
            patch("loom_cli.server_client.authed_client", return_value=http_client),
        ):
            rc = main(["service", "up", "--environment", "dev-alice", "--candidate", candidate_sha,
                       "--quiet"])
    finally:
        http_client.close()

    captured = capsys.readouterr()
    assert rc == 1
    assert captured.out == ""
    assert captured.err.startswith("error:")


@pytest.mark.parametrize("environment", ["local", "staging", "production"])
def test_service_up_quiet_rejected_before_nonpersonal_action(
    environment: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    with (
        patch("loom_cli.service_cmd._up_local", side_effect=AssertionError("must not start Compose")),
        patch("loom_cli.service_cmd._up_protected", side_effect=AssertionError("must not deploy protected")),
    ):
        rc = main(["service", "up", "--environment", environment, "--quiet"])

    captured = capsys.readouterr()
    assert rc == 2
    assert captured.out == ""
    assert "--quiet is only valid" in captured.err


def test_up_errors_when_compose_file_missing(
    tmp_path: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    rc = main([
        "service", "up", "--environment", "local",
        "--compose-file", str(tmp_path / "nonexistent.yml"),
    ])
    assert rc == 1
    assert "not found" in capsys.readouterr().err.lower()


def test_down_errors_when_compose_file_missing(
    tmp_path: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    rc = main([
        "service", "down",
        "--compose-file", str(tmp_path / "nonexistent.yml"),
    ])
    assert rc == 1
    assert "not found" in capsys.readouterr().err.lower()


def test_status_errors_when_compose_file_missing(
    tmp_path: Path, capsys: pytest.CaptureFixture[str],
) -> None:
    rc = main([
        "service", "status",
        "--compose-file", str(tmp_path / "nonexistent.yml"),
    ])
    assert rc == 1
    assert "not found" in capsys.readouterr().err.lower()


@pytest.mark.parametrize("service_cmd", ["up", "down", "status"])
def test_service_commands_error_without_docker_cli(
    service_cmd: str,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Missing Docker should produce an actionable CLI error, not a traceback."""
    compose = tmp_path / "compose.yml"
    compose.write_text("services: {}\n")
    monkeypatch.setattr(
        "shutil.which",
        lambda binary: None if binary == "docker" else "/bin/other",
    )

    with patch("loom_cli.service_cmd._run", side_effect=AssertionError("compose should not run")):
        argv = [
            "service", service_cmd,
            "--compose-file", str(compose),
            "--env-file", str(tmp_path / "absent.env"),
        ]
        if service_cmd == "up":
            argv.extend(["--environment", "local"])
        rc = main(argv)

    captured = capsys.readouterr()
    assert rc == 2
    assert "Docker CLI was not found" in captured.err
    assert "Docker Desktop for Mac" in captured.err
    assert "docker compose version" in captured.err
    assert "Traceback" not in captured.err


def test_service_status_errors_when_docker_compose_unavailable(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A missing Compose plugin should be diagnosed before compose ps runs."""
    compose = tmp_path / "compose.yml"
    compose.write_text("services: {}\n")
    monkeypatch.setattr(
        "shutil.which",
        lambda binary: "/usr/local/bin/docker" if binary == "docker" else None,
    )

    def _fake_run(argv, **kwargs):  # type: ignore[no-untyped-def]
        assert argv == ["docker", "compose", "version"]
        return subprocess.CompletedProcess(
            args=argv,
            returncode=1,
            stdout="",
            stderr="docker: compose is not a docker command\n",
        )

    monkeypatch.setattr("loom_cli.service_cmd.subprocess.run", _fake_run)
    rc = main([
        "service", "status",
        "--compose-file", str(compose),
        "--env-file", str(tmp_path / "absent.env"),
    ])

    captured = capsys.readouterr()
    assert rc == 2
    assert "Docker Compose is not available" in captured.err
    assert "docker compose version" in captured.err
    assert "docker: compose is not a docker command" in captured.err


def test_compose_args_includes_env_file_when_present(
    tmp_path: Path,
) -> None:
    compose = tmp_path / "compose.yml"
    compose.write_text("services: {}\n")
    env = tmp_path / ".env"
    env.write_text("FOO=bar\n")
    args = _compose_args(compose, env)
    assert "--env-file" in args
    assert str(env) in args
    assert "-f" in args
    assert str(compose) in args


def test_compose_args_omits_env_file_when_missing(
    tmp_path: Path,
) -> None:
    compose = tmp_path / "compose.yml"
    compose.write_text("services: {}\n")
    missing_env = tmp_path / "does-not-exist.env"
    args = _compose_args(compose, missing_env)
    assert "--env-file" not in args
    assert "-f" in args
    assert str(compose) in args


def test_mutable_dev_image_detection_is_literal_and_local(tmp_path: Path) -> None:
    compose = tmp_path / "compose.yml"
    compose.write_text(
        """services:
  service:
    image: loom-service:dev
  worker:
    image: 'loom-worker:dev'
  release:
    image: loom-control-plane:v1.0.0
  external:
    image: postgres:16
  parameterized:
    image: loom-gateway:${LOOM_IMAGE_TAG:-dev}
  sandboxed:
    image: loom-control-plane:${LOOM_DEV_IMAGE_TAG:-dev}
""",
    )

    assert _mutable_dev_images(compose) == (
        "loom-control-plane:${LOOM_DEV_IMAGE_TAG:-dev}",
        "loom-service:dev",
        "loom-worker:dev",
    )


def test_dev_step_jwt_has_one_local_source() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    compose_text = (repo_root / "deploy" / "docker-compose.dev.yml").read_text()
    env_text = (repo_root / ".env.example").read_text()
    source = "${LOOM_CP_STEP_JWT_SIGNING_KEY:-dev-only-shared-jwt-key-do-not-use-in-prod}"

    assert f'LOOM_CP_STEP_JWT_SIGNING_KEY: "{source}"' in compose_text
    assert f'LOOM_GW_STEP_JWT_SIGNING_KEY: "{source}"' in compose_text
    assert "LOOM_CP_STEP_JWT_SIGNING_KEY=" in env_text
    assert "LOOM_TASK_IMAGE_BUILDER_TOKEN=placeholder" in env_text


def test_up_invokes_docker_compose_up(
    tmp_path: Path,
) -> None:
    """Verify the happy-path invocation chain — `docker compose up -d`
    runs first; on its failure we bail before alembic + seed."""
    compose = tmp_path / "compose.yml"
    compose.write_text("services: {}\n")
    with patch("loom_cli.service_cmd._ensure_docker_compose_available",
               return_value=0), \
         patch("loom_cli.service_cmd._run") as mock_run, \
         patch("loom_cli.service_cmd._wait_for_postgres",
               return_value=False) as mock_wait:
        # _run returns CompletedProcess-like; we need .returncode = 0
        from subprocess import CompletedProcess
        mock_run.return_value = CompletedProcess([], 0, "", "")
        rc = main([
            "service", "up", "--environment", "local",
            "--compose-file", str(compose),
            "--env-file", str(tmp_path / "absent.env"),
        ])
        # postgres didn't go healthy → exit 1, no alembic call
        assert rc == 1
        # First call should be `docker compose ... up -d`
        first_args = mock_run.call_args_list[0].args[0]
        assert "up" in first_args and "-d" in first_args
        assert mock_wait.called


def test_up_builds_mutable_dev_images_before_start_and_migrations(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Fresh, cached, and stale ``:dev`` images share one safe contract:
    Compose checks/builds them before any DB-facing container starts.
    """
    from subprocess import CompletedProcess

    compose = tmp_path / "compose.yml"
    compose.write_text(
        "services:\n  service:\n    image: loom-service:dev\n    build: .\n",
    )
    env_file = tmp_path / ".env"
    secret = "test-step-jwt-secret-that-must-not-be-printed"
    env_file.write_text(f"LOOM_CP_STEP_JWT_SIGNING_KEY={secret}\n")
    events: list[str] = []

    def _capture_run(argv, *_args, **_kwargs):
        events.append("up")
        assert argv[-3:] == ["up", "-d", "--build"]
        return CompletedProcess(argv, 0, "", "")

    def _alembic(_db_url: str) -> int:
        events.append("alembic")
        return 0

    with (
        patch("loom_cli.service_cmd._ensure_docker_compose_available", return_value=0),
        patch("loom_cli.service_cmd._run", side_effect=_capture_run),
        patch("loom_cli.service_cmd._wait_for_postgres", return_value=True),
        patch("loom_cli.service_cmd._alembic_upgrade", side_effect=_alembic),
        patch("loom_cli.service_cmd._seed_test_data", return_value=(1, {})),
        patch(
            "loom_cli.service_cmd._ensure_dev_admin_secret",
            return_value="loom_admin_" + "A" * 43,
        ),
    ):
        rc = main(
            [
                "service",
                "up",
                "--environment",
                "local",
                "--compose-file",
                str(compose),
                "--env-file",
                str(env_file),
            ]
        )

    assert rc == 1
    assert events == ["up", "alembic"]
    captured = capsys.readouterr()
    assert secret not in captured.out
    assert secret not in captured.err


def test_up_does_not_force_build_for_immutable_images(tmp_path: Path) -> None:
    from subprocess import CompletedProcess

    compose = tmp_path / "compose.yml"
    compose.write_text("services:\n  service:\n    image: loom-service:v1.0.0\n")

    with (
        patch("loom_cli.service_cmd._ensure_docker_compose_available", return_value=0),
        patch("loom_cli.service_cmd._run") as mock_run,
        patch("loom_cli.service_cmd._wait_for_postgres", return_value=False),
    ):
        mock_run.return_value = CompletedProcess([], 0, "", "")
        rc = main(
            [
                "service",
                "up",
                "--environment",
                "local",
                "--compose-file",
                str(compose),
                "--env-file",
                str(tmp_path / "absent.env"),
            ]
        )

    assert rc == 1
    first_args = mock_run.call_args_list[0].args[0]
    assert first_args[-2:] == ["up", "-d"]
    assert "--build" not in first_args


def test_seed_test_data_parses_all_tokens(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`--print all` emits `<label>: <token>` per line; the wrapper
    parses team/worker/builder tokens while admin comes from a secret file."""
    from subprocess import CompletedProcess

    from loom_cli.service_cmd import _seed_test_data

    fake_stdout = (
        "team: loom_team_aaaaaa\n"
        "worker: loom_w_bbbbbb\n"
        "builder: loom_tib_cccccc\n"
    )

    def _fake_run(*_args, **_kwargs):
        return CompletedProcess([], 0, fake_stdout, "")

    monkeypatch.setattr("loom_cli.service_cmd.subprocess.run", _fake_run)
    rc, tokens = _seed_test_data("postgresql://x/y")
    assert rc == 0
    assert tokens == {
        "team": "loom_team_aaaaaa",
        "worker": "loom_w_bbbbbb",
        "builder": "loom_tib_cccccc",
    }


def test_seed_test_data_invokes_dev_mode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`loom service up` must call the seed script with `--mode dev`
    so the Benchmarks table is populated and no placeholder
    (hello-world Task, card-e2e RateCard) is seeded into the dev
    stack."""
    from subprocess import CompletedProcess

    from loom_cli.service_cmd import _seed_test_data

    captured_argv: list[list[str]] = []

    def _fake_run(argv, *_args, **_kwargs):
        captured_argv.append(list(argv))
        return CompletedProcess([], 0, "team: t\nworker: w\n", "")

    monkeypatch.setattr("loom_cli.service_cmd.subprocess.run", _fake_run)
    rc, _tokens = _seed_test_data("postgresql://x/y")
    assert rc == 0
    assert captured_argv, "expected at least one subprocess.run call"
    argv = captured_argv[0]
    assert "--mode" in argv
    assert argv[argv.index("--mode") + 1] == "dev"
    assert "--print" in argv
    assert argv[argv.index("--print") + 1] == "all"


def test_print_summary_labels_admin_as_file_backed_dev_singleton(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The summary must not print the raw admin secret."""
    from loom_cli.service_cmd import _print_summary

    _print_summary({"team": "loom_team_x", "admin": "loom_admin_y"})
    out = capsys.readouterr().out
    assert "loom_team_x" in out
    assert "loom_admin_y" not in out
    assert "DEV-ONLY" in out
    assert "file-backed" in out
    assert "reveal-admin" in out


def test_ensure_dev_admin_secret_creates_0600_and_returns_token(
    tmp_path: Path,
) -> None:
    from loom_cli.service_cmd import _ensure_dev_admin_secret

    secret_file = tmp_path / ".loom" / "admin" / "secrets.toml"

    token = _ensure_dev_admin_secret(secret_file)

    assert token == _read_admin_token(secret_file)
    assert token.startswith("loom_admin_")
    assert stat.S_IMODE(secret_file.stat().st_mode) == 0o600


def test_ensure_dev_admin_secret_preserves_existing_token(
    tmp_path: Path,
) -> None:
    from loom_cli.service_cmd import _ensure_dev_admin_secret

    secret_file = tmp_path / ".loom" / "admin" / "secrets.toml"
    existing = "loom_admin_" + "E" * 43
    secret_file.parent.mkdir(parents=True)
    secret_file.write_text(
        "[admin]\n"
        f"token = \"{existing}\"\n",
        encoding="utf-8",
    )
    secret_file.chmod(0o600)

    token = _ensure_dev_admin_secret(secret_file)

    assert token == existing


def test_write_env_tokens_creates_file_when_absent(tmp_path) -> None:
    from loom_cli.service_cmd import _write_env_tokens

    env_file = tmp_path / ".env"
    _write_env_tokens(env_file, {
        "team": "loom_team_aaa",
        "worker": "loom_w_bbb",
        "admin": "loom_admin_ccc",
    })
    content = env_file.read_text()
    assert "LOOM_TEAM_TOKEN=loom_team_aaa" in content
    assert "LOOM_WORKER_TOKEN=loom_w_bbb" in content
    assert "LOOM_ADMIN_TOKEN=loom_admin_ccc" in content


def test_write_env_tokens_writes_builder_token(tmp_path: Path) -> None:
    from loom_cli.service_cmd import _write_env_tokens

    env_file = tmp_path / ".env"
    _write_env_tokens(env_file, {
        "team": "loom_team_t",
        "worker": "loom_w_w",
        "builder": "loom_tib_b",
        "admin": "loom_admin_a",
    })
    content = env_file.read_text()
    assert "LOOM_TASK_IMAGE_BUILDER_TOKEN=loom_tib_b" in content


def test_write_env_tokens_replaces_existing_keys_preserving_others(
    tmp_path,
) -> None:
    """Idempotent overwrite: existing token lines get the new value,
    unrelated lines (comments, custom env vars) survive verbatim."""
    from loom_cli.service_cmd import _write_env_tokens

    env_file = tmp_path / ".env"
    env_file.write_text(
        "# Local dev tokens\n"
        "LOOM_WORKER_TOKEN=loom_w_old\n"
        "LOOM_TEAM_TOKEN=loom_team_old\n"
        "LOOM_ADMIN_TOKEN=loom_admin_old\n"
        "MY_CUSTOM_VAR=please-keep-me\n",
    )
    _write_env_tokens(env_file, {
        "team": "loom_team_new",
        "worker": "loom_w_new",
        "admin": "loom_admin_new",
    })
    lines = env_file.read_text().splitlines()
    assert "# Local dev tokens" in lines
    assert "LOOM_TEAM_TOKEN=loom_team_new" in lines
    assert "LOOM_WORKER_TOKEN=loom_w_new" in lines
    assert "LOOM_ADMIN_TOKEN=loom_admin_new" in lines
    assert "MY_CUSTOM_VAR=please-keep-me" in lines
    # Old values are gone; no duplicates of any key.
    assert not any(line.endswith("=loom_team_old") for line in lines)
    keys = [
        line.split("=", 1)[0]
        for line in lines
        if "=" in line and not line.lstrip().startswith("#")
    ]
    assert len(keys) == len(set(keys))


def test_write_env_tokens_appends_missing_keys(tmp_path) -> None:
    """If only one key exists in .env, the other two must be appended
    rather than silently dropped."""
    from loom_cli.service_cmd import _write_env_tokens

    env_file = tmp_path / ".env"
    env_file.write_text("LOOM_TEAM_TOKEN=loom_team_only\n")
    _write_env_tokens(env_file, {
        "team": "loom_team_new",
        "worker": "loom_w_new",
        "admin": "loom_admin_new",
    })
    content = env_file.read_text()
    assert "LOOM_TEAM_TOKEN=loom_team_new" in content
    assert "LOOM_WORKER_TOKEN=loom_w_new" in content
    assert "LOOM_ADMIN_TOKEN=loom_admin_new" in content


def test_up_recreates_worker_after_seeding_fresh_tokens(
    tmp_path: Path,
) -> None:
    """After `_seed_test_data` mints fresh tokens and `_write_env_tokens`
    persists them to .env, the worker container is still running with
    the STALE token it booted with — `docker restart` reuses the old env,
    only `up --force-recreate` re-reads .env. `loom service up` MUST issue
    that recreate, otherwise the worker keeps rejecting control-plane
    requests with 401 until the operator notices.
    """
    from subprocess import CompletedProcess

    compose = tmp_path / "compose.yml"
    compose.write_text("services: {}\n")
    env_file = tmp_path / ".env"

    captured_run_calls: list[list[str]] = []

    def _capture_run(argv, *_args, **_kwargs):
        captured_run_calls.append(list(argv))
        return CompletedProcess(argv, 0, "", "")

    fake_tokens = {
        "team": "loom_team_fresh",
        "worker": "loom_w_fresh",
        "admin": "loom_admin_db_ignored",
    }
    admin_secret_token = "loom_admin_" + "U" * 43

    with patch("loom_cli.service_cmd._ensure_docker_compose_available",
               return_value=0), \
         patch("loom_cli.service_cmd._run", side_effect=_capture_run), \
         patch("loom_cli.service_cmd._wait_for_postgres",
               return_value=True), \
         patch("loom_cli.service_cmd._alembic_upgrade",
               return_value=0), \
         patch("loom_cli.service_cmd._seed_test_data",
               return_value=(0, fake_tokens)), \
         patch("loom_cli.service_cmd._ensure_dev_admin_secret",
               return_value=admin_secret_token), \
         patch("loom_cli.service_cmd._mint_batch_runner_cp_token",
               return_value=None):
        rc = main([
            "service", "up", "--environment", "local",
            "--compose-file", str(compose),
            "--env-file", str(env_file),
        ])

    assert rc == 0
    assert env_file.exists(), "env_file should have been written"
    assert f"LOOM_ADMIN_TOKEN={admin_secret_token}" in env_file.read_text()
    assert "loom_admin_db_ignored" not in env_file.read_text()

    # Find the recreate call — must come after the initial `up -d` and
    # carry --force-recreate + --no-deps + worker target.
    recreate_calls = [
        argv for argv in captured_run_calls
        if "--force-recreate" in argv and "worker" in argv
    ]
    assert len(recreate_calls) == 1, (
        f"expected exactly one --force-recreate worker call; "
        f"got {len(recreate_calls)} of {len(captured_run_calls)} _run calls. "
        f"All argvs: {captured_run_calls!r}"
    )
    recreate_argv = recreate_calls[0]
    assert "up" in recreate_argv
    assert "-d" in recreate_argv
    assert "--no-deps" in recreate_argv

    # Order check: the worker recreate must come AFTER _write_env_tokens
    # has run. When mint returns None (no loom-service recreate), the
    # worker recreate is the last _run call.
    assert captured_run_calls[-1] == recreate_argv, (
        "worker recreate must be the last subprocess call, AFTER "
        "_write_env_tokens has persisted fresh tokens"
    )


def test_up_recreates_task_image_builder_after_seeding_builder_token(
    tmp_path: Path,
) -> None:
    from subprocess import CompletedProcess

    compose = tmp_path / "compose.yml"
    compose.write_text("services: {}\n")
    env_file = tmp_path / ".env"
    captured_run_calls: list[list[str]] = []

    def _capture_run(argv, *_args, **_kwargs):
        captured_run_calls.append(list(argv))
        return CompletedProcess(argv, 0, "", "")

    fake_tokens = {
        "team": "loom_team_fresh",
        "worker": "loom_w_fresh",
        "builder": "loom_tib_fresh",
        "admin": "loom_admin_db_ignored",
    }

    with patch("loom_cli.service_cmd._ensure_docker_compose_available",
               return_value=0), \
         patch("loom_cli.service_cmd._run", side_effect=_capture_run), \
         patch("loom_cli.service_cmd._wait_for_postgres",
               return_value=True), \
         patch("loom_cli.service_cmd._alembic_upgrade",
               return_value=0), \
         patch("loom_cli.service_cmd._seed_test_data",
               return_value=(0, fake_tokens)), \
         patch("loom_cli.service_cmd._ensure_dev_admin_secret",
               return_value="loom_admin_" + "U" * 43), \
         patch("loom_cli.service_cmd._mint_batch_runner_cp_token",
               return_value=None):
        rc = main([
            "service", "up", "--environment", "local",
            "--compose-file", str(compose),
            "--env-file", str(env_file),
        ])

    assert rc == 0
    assert "LOOM_TASK_IMAGE_BUILDER_TOKEN=loom_tib_fresh" in env_file.read_text()
    builder_recreates = [
        argv for argv in captured_run_calls
        if "--force-recreate" in argv and "task-image-builder" in argv
    ]
    assert len(builder_recreates) == 1
    assert "--no-deps" in builder_recreates[0]



def test_up_skips_worker_recreate_when_no_env_file(
    tmp_path: Path,
) -> None:
    """When operator runs without --env-file, _write_env_tokens is
    skipped — there's no .env to update, so there's no stale token to
    flush. Recreating the worker would be pointless work.
    """
    from subprocess import CompletedProcess

    compose = tmp_path / "compose.yml"
    compose.write_text("services: {}\n")

    captured_run_calls: list[list[str]] = []

    def _capture_run(argv, *_args, **_kwargs):
        captured_run_calls.append(list(argv))
        return CompletedProcess(argv, 0, "", "")

    fake_tokens = {
        "team": "loom_team_fresh",
        "worker": "loom_w_fresh",
    }

    with patch("loom_cli.service_cmd._ensure_docker_compose_available",
               return_value=0), \
         patch("loom_cli.service_cmd._run", side_effect=_capture_run), \
         patch("loom_cli.service_cmd._wait_for_postgres",
               return_value=True), \
         patch("loom_cli.service_cmd._alembic_upgrade",
               return_value=0), \
         patch("loom_cli.service_cmd._seed_test_data",
               return_value=(0, fake_tokens)), \
         patch("loom_cli.service_cmd._ensure_dev_admin_secret",
               return_value="loom_admin_" + "N" * 43), \
         patch("loom_cli.service_cmd._mint_batch_runner_cp_token",
               return_value="loom_br_unused"):
        # argparse defaults --env-file to <compose_dir>/.env if not
        # explicitly None, so we have to pass --env-file pointing
        # somewhere AND ensure the loader treats it as "absent".
        # The CLI doesn't actually have a "no env file" mode — it
        # defaults to .env next to the compose file. So this test
        # documents that "env_file is None" is actually an internal
        # branch reachable only via the API, not the CLI flag set.
        import argparse

        from loom_cli.service_cmd import _DEFAULT_CP_URL, _up_local
        args = argparse.Namespace(
            compose_file=compose,
            env_file=None,
            db_url="postgresql://x/y",
            admin_secret_file=tmp_path / ".loom" / "admin" / "secrets.toml",
            cp_url=_DEFAULT_CP_URL,
        )
        rc = _up_local(args)

    assert rc == 0
    # No recreate should have happened (env_file is None → nothing to write).
    recreate_calls = [
        argv for argv in captured_run_calls
        if "--force-recreate" in argv
    ]
    assert recreate_calls == [], (
        f"unexpected --force-recreate when env_file is None: "
        f"{recreate_calls!r}"
    )


def test_init_admin_secret_writes_0600_without_printing_token(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    secret_file = tmp_path / "secrets.toml"

    rc = main([
        "service", "init-admin", "--secret-file", str(secret_file),
    ])

    assert rc == 0
    token = _read_admin_token(secret_file)
    assert token.startswith("loom_admin_")
    assert len(token.removeprefix("loom_admin_")) >= 32
    assert stat.S_IMODE(secret_file.stat().st_mode) == 0o600
    captured = capsys.readouterr()
    assert token not in captured.out
    assert token not in captured.err
    assert str(secret_file) in captured.out


def test_reveal_admin_requires_confirmation_unless_yes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    secret_file = tmp_path / "secrets.toml"
    token = "loom_admin_" + "R" * 43
    secret_file.write_text(
        "[admin]\n"
        f"token = \"{token}\"\n",
        encoding="utf-8",
    )
    secret_file.chmod(0o600)

    monkeypatch.setattr("sys.stdin", io.StringIO("no\n"))
    denied = main([
        "service", "reveal-admin", "--secret-file", str(secret_file),
    ])
    denied_output = capsys.readouterr()

    approved = main([
        "service", "reveal-admin", "--secret-file", str(secret_file), "--yes",
    ])
    approved_output = capsys.readouterr()

    assert denied == 2
    assert token not in denied_output.out
    assert token not in denied_output.err
    assert approved == 0
    assert token in approved_output.out


def test_rotate_admin_replaces_secret_without_printing_new_token(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    secret_file = tmp_path / "secrets.toml"
    old_token = "loom_admin_" + "O" * 43
    secret_file.write_text(
        "[admin]\n"
        f"token = \"{old_token}\"\n",
        encoding="utf-8",
    )
    secret_file.chmod(0o600)

    rc = main([
        "service", "rotate-admin", "--secret-file", str(secret_file),
    ])

    assert rc == 0
    new_token = _read_admin_token(secret_file)
    assert new_token.startswith("loom_admin_")
    assert new_token != old_token
    assert stat.S_IMODE(secret_file.stat().st_mode) == 0o600
    out = capsys.readouterr().out
    assert new_token not in out
    assert old_token not in out
    assert "restart" in out.lower()


# ---------------------------------------------------------------------------
# _mint_batch_runner_cp_token
# ---------------------------------------------------------------------------


def test_mint_batch_runner_cp_token_returns_token_on_201(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A 201 response with a token field yields the raw token string."""
    import httpx

    from loom_cli.service_cmd import _mint_batch_runner_cp_token

    def _fake_post(url, *, json, headers, timeout):  # type: ignore[no-untyped-def]
        assert "/admin/batch-runner-tokens" in url
        assert "Authorization" in headers
        return httpx.Response(201, json={"token": "loom_br_testtoken", "token_hash_prefix": "ab12cd34"})

    monkeypatch.setattr("loom_cli.service_cmd.httpx.post", _fake_post)
    result = _mint_batch_runner_cp_token("loom_admin_" + "A" * 43)
    assert result == "loom_br_testtoken"


def test_mint_batch_runner_cp_token_returns_none_on_non_201(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A non-201 response yields None and prints a warning."""
    import httpx

    from loom_cli.service_cmd import _mint_batch_runner_cp_token

    def _fake_post(url, *, json, headers, timeout):  # type: ignore[no-untyped-def]
        return httpx.Response(403, text="missing scope admin:tokens")

    monkeypatch.setattr("loom_cli.service_cmd.httpx.post", _fake_post)
    result = _mint_batch_runner_cp_token("loom_admin_" + "B" * 43)
    assert result is None
    err = capsys.readouterr().err
    assert "warning" in err.lower()
    assert "403" in err


def test_mint_batch_runner_cp_token_returns_none_on_network_error(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A network error yields None and prints a recoverable warning."""
    import httpx

    from loom_cli.service_cmd import _mint_batch_runner_cp_token

    def _fake_post(url, *, json, headers, timeout):  # type: ignore[no-untyped-def]
        raise httpx.ConnectError("connection refused")

    monkeypatch.setattr("loom_cli.service_cmd.httpx.post", _fake_post)
    result = _mint_batch_runner_cp_token("loom_admin_" + "C" * 43)
    assert result is None
    err = capsys.readouterr().err
    assert "warning" in err.lower()
    assert "connection refused" in err


# ---------------------------------------------------------------------------
# _up: batch-runner token integration
# ---------------------------------------------------------------------------


def test_up_mints_batch_runner_token_and_writes_to_env(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """After a successful seed, _up mints a batch-runner CP token, writes
    it to .env as LOOM_SVC_BATCH_RUNNER_CP_TOKEN, and prints a confirm line."""
    from subprocess import CompletedProcess

    compose = tmp_path / "compose.yml"
    compose.write_text("services: {}\n")
    env_file = tmp_path / ".env"

    fake_tokens = {
        "team": "loom_team_fresh",
        "worker": "loom_w_fresh",
    }
    admin_secret_token = "loom_admin_" + "U" * 43
    batch_runner_token = "loom_br_fakebatchtoken"

    def _capture_run(argv, *_args, **_kwargs):
        return CompletedProcess(argv, 0, "", "")

    with patch("loom_cli.service_cmd._ensure_docker_compose_available",
               return_value=0), \
         patch("loom_cli.service_cmd._run", side_effect=_capture_run), \
         patch("loom_cli.service_cmd._wait_for_postgres",
               return_value=True), \
         patch("loom_cli.service_cmd._alembic_upgrade",
               return_value=0), \
         patch("loom_cli.service_cmd._seed_test_data",
               return_value=(0, fake_tokens)), \
         patch("loom_cli.service_cmd._ensure_dev_admin_secret",
               return_value=admin_secret_token), \
         patch("loom_cli.service_cmd._mint_batch_runner_cp_token",
               return_value=batch_runner_token) as mock_mint:
        rc = main([
            "service", "up", "--environment", "local",
            "--compose-file", str(compose),
            "--env-file", str(env_file),
        ])

    assert rc == 0
    # mint was called with the admin token
    mock_mint.assert_called_once()
    call_args = mock_mint.call_args
    assert call_args.args[0] == admin_secret_token

    # token written to .env
    assert env_file.exists()
    env_content = env_file.read_text()
    assert f"LOOM_SVC_BATCH_RUNNER_CP_TOKEN={batch_runner_token}" in env_content

    # confirmation line printed
    out = capsys.readouterr().out
    assert "batch-runner CP token written to .env" in out


def test_up_recreates_loom_service_after_writing_batch_runner_token(
    tmp_path: Path,
) -> None:
    """loom-service must be force-recreated AFTER .env is updated so it
    reads the freshly-minted LOOM_SVC_BATCH_RUNNER_CP_TOKEN."""
    from subprocess import CompletedProcess

    compose = tmp_path / "compose.yml"
    compose.write_text("services: {}\n")
    env_file = tmp_path / ".env"

    captured_run_calls: list[list[str]] = []

    def _capture_run(argv, *_args, **_kwargs):
        captured_run_calls.append(list(argv))
        return CompletedProcess(argv, 0, "", "")

    fake_tokens = {"team": "loom_team_x", "worker": "loom_w_x"}
    admin_secret_token = "loom_admin_" + "V" * 43
    batch_runner_token = "loom_br_recreatetest"

    with patch("loom_cli.service_cmd._ensure_docker_compose_available",
               return_value=0), \
         patch("loom_cli.service_cmd._run", side_effect=_capture_run), \
         patch("loom_cli.service_cmd._wait_for_postgres",
               return_value=True), \
         patch("loom_cli.service_cmd._alembic_upgrade",
               return_value=0), \
         patch("loom_cli.service_cmd._seed_test_data",
               return_value=(0, fake_tokens)), \
         patch("loom_cli.service_cmd._ensure_dev_admin_secret",
               return_value=admin_secret_token), \
         patch("loom_cli.service_cmd._mint_batch_runner_cp_token",
               return_value=batch_runner_token):
        rc = main([
            "service", "up", "--environment", "local",
            "--compose-file", str(compose),
            "--env-file", str(env_file),
        ])

    assert rc == 0

    # loom-service recreate call must exist
    svc_recreate_calls = [
        argv for argv in captured_run_calls
        if "--force-recreate" in argv and "loom-service" in argv
    ]
    assert len(svc_recreate_calls) == 1, (
        f"expected exactly one --force-recreate loom-service call; "
        f"got {len(svc_recreate_calls)} of {len(captured_run_calls)} _run calls. "
        f"All argvs: {captured_run_calls!r}"
    )
    svc_recreate_argv = svc_recreate_calls[0]
    assert "up" in svc_recreate_argv
    assert "-d" in svc_recreate_argv
    assert "--no-deps" in svc_recreate_argv

    # The loom-service recreate must come AFTER the env file is written
    # (i.e., it must be a later _run call than the initial `up -d`).
    initial_up_idx = next(
        i for i, argv in enumerate(captured_run_calls)
        if "up" in argv and "-d" in argv and "--force-recreate" not in argv
    )
    svc_recreate_idx = captured_run_calls.index(svc_recreate_argv)
    assert svc_recreate_idx > initial_up_idx, (
        "loom-service recreate must come after initial compose up"
    )

    # env file must have the token before the recreate ran (we wrote it)
    assert f"LOOM_SVC_BATCH_RUNNER_CP_TOKEN={batch_runner_token}" in env_file.read_text()


def test_up_skips_loom_service_recreate_when_mint_fails(
    tmp_path: Path,
) -> None:
    """If minting the batch-runner token fails (returns None), _up must
    not attempt to recreate loom-service — that would be pointless."""
    from subprocess import CompletedProcess

    compose = tmp_path / "compose.yml"
    compose.write_text("services: {}\n")
    env_file = tmp_path / ".env"

    captured_run_calls: list[list[str]] = []

    def _capture_run(argv, *_args, **_kwargs):
        captured_run_calls.append(list(argv))
        return CompletedProcess(argv, 0, "", "")

    fake_tokens = {"team": "loom_team_x", "worker": "loom_w_x"}

    with patch("loom_cli.service_cmd._ensure_docker_compose_available",
               return_value=0), \
         patch("loom_cli.service_cmd._run", side_effect=_capture_run), \
         patch("loom_cli.service_cmd._wait_for_postgres",
               return_value=True), \
         patch("loom_cli.service_cmd._alembic_upgrade",
               return_value=0), \
         patch("loom_cli.service_cmd._seed_test_data",
               return_value=(0, fake_tokens)), \
         patch("loom_cli.service_cmd._ensure_dev_admin_secret",
               return_value="loom_admin_" + "W" * 43), \
         patch("loom_cli.service_cmd._mint_batch_runner_cp_token",
               return_value=None):
        rc = main([
            "service", "up", "--environment", "local",
            "--compose-file", str(compose),
            "--env-file", str(env_file),
        ])

    assert rc == 0
    # No loom-service recreate when mint failed
    svc_recreate_calls = [
        argv for argv in captured_run_calls
        if "--force-recreate" in argv and "loom-service" in argv
    ]
    assert svc_recreate_calls == [], (
        f"unexpected loom-service recreate when mint failed: {svc_recreate_calls!r}"
    )


# ---------------------------------------------------------------------------
# _write_env_tokens: batch_runner_cp key
# ---------------------------------------------------------------------------


def test_write_env_tokens_writes_batch_runner_cp_token(tmp_path: Path) -> None:
    """LOOM_SVC_BATCH_RUNNER_CP_TOKEN must be persisted alongside the
    other token keys when the batch_runner_cp label is present."""
    from loom_cli.service_cmd import _write_env_tokens

    env_file = tmp_path / ".env"
    _write_env_tokens(env_file, {
        "team": "loom_team_t",
        "worker": "loom_w_w",
        "admin": "loom_admin_a",
        "batch_runner_cp": "loom_br_b",
    })
    content = env_file.read_text()
    assert "LOOM_SVC_BATCH_RUNNER_CP_TOKEN=loom_br_b" in content


def test_write_env_tokens_replaces_existing_batch_runner_cp_token(
    tmp_path: Path,
) -> None:
    """Idempotent overwrite: an existing LOOM_SVC_BATCH_RUNNER_CP_TOKEN
    line is replaced with the new value on re-seed."""
    from loom_cli.service_cmd import _write_env_tokens

    env_file = tmp_path / ".env"
    env_file.write_text(
        "LOOM_TEAM_TOKEN=loom_team_old\n"
        "LOOM_SVC_BATCH_RUNNER_CP_TOKEN=loom_br_old\n",
    )
    _write_env_tokens(env_file, {
        "team": "loom_team_new",
        "batch_runner_cp": "loom_br_new",
    })
    lines = env_file.read_text().splitlines()
    assert "LOOM_TEAM_TOKEN=loom_team_new" in lines
    assert "LOOM_SVC_BATCH_RUNNER_CP_TOKEN=loom_br_new" in lines
    assert not any("loom_br_old" in line for line in lines)


# ---------------------------------------------------------------------------
# _mint_secret_store_master_key + _ensure_secret_store_master_key
# ---------------------------------------------------------------------------


def test_secret_store_master_key_is_32_bytes_base64() -> None:
    """The helper must produce a base64-encoded 32-byte (256-bit) key."""
    import base64

    from loom_cli.service_cmd import _mint_secret_store_master_key

    key = _mint_secret_store_master_key()
    raw = base64.b64decode(key)
    assert len(raw) == 32, f"expected 32 bytes, got {len(raw)}"


def test_up_generates_secret_store_master_key_if_absent(
    tmp_path: Path,
) -> None:
    """On a fresh .env (no pre-existing key), `_up` must write
    LOOM_SECRET_STORE_MASTER_KEY before docker compose starts."""
    from subprocess import CompletedProcess

    compose = tmp_path / "compose.yml"
    compose.write_text("services: {}\n")
    env_file = tmp_path / ".env"
    # env_file does NOT yet exist (fresh dev setup)

    def _capture_run(argv, *_args, **_kwargs):
        return CompletedProcess(argv, 0, "", "")

    fake_tokens = {"team": "loom_team_t", "worker": "loom_w_w"}
    admin_secret_token = "loom_admin_" + "K" * 43

    with patch("loom_cli.service_cmd._ensure_docker_compose_available",
               return_value=0), \
         patch("loom_cli.service_cmd._run", side_effect=_capture_run), \
         patch("loom_cli.service_cmd._wait_for_postgres",
               return_value=True), \
         patch("loom_cli.service_cmd._alembic_upgrade",
               return_value=0), \
         patch("loom_cli.service_cmd._seed_test_data",
               return_value=(0, fake_tokens)), \
         patch("loom_cli.service_cmd._ensure_dev_admin_secret",
               return_value=admin_secret_token), \
         patch("loom_cli.service_cmd._mint_batch_runner_cp_token",
               return_value=None):
        rc = main([
            "service", "up", "--environment", "local",
            "--compose-file", str(compose),
            "--env-file", str(env_file),
        ])

    assert rc == 0
    assert env_file.exists(), ".env should have been created"
    content = env_file.read_text()
    assert "LOOM_SECRET_STORE_MASTER_KEY=" in content
    # The value must be non-empty
    for line in content.splitlines():
        if line.startswith("LOOM_SECRET_STORE_MASTER_KEY="):
            value = line.split("=", 1)[1]
            assert value, "LOOM_SECRET_STORE_MASTER_KEY must have a non-empty value"
            break


def test_up_preserves_existing_secret_store_master_key(
    tmp_path: Path,
) -> None:
    """When LOOM_SECRET_STORE_MASTER_KEY already exists in .env, `_up` must
    NOT regenerate it — rotating the master key would invalidate all stored
    provider-connection secrets."""
    import base64
    from subprocess import CompletedProcess

    compose = tmp_path / "compose.yml"
    compose.write_text("services: {}\n")
    env_file = tmp_path / ".env"
    existing_key = base64.b64encode(b"existing-32-byte-key-padded-here").decode()
    env_file.write_text(f"LOOM_SECRET_STORE_MASTER_KEY={existing_key}\n")

    def _capture_run(argv, *_args, **_kwargs):
        return CompletedProcess(argv, 0, "", "")

    fake_tokens = {"team": "loom_team_t", "worker": "loom_w_w"}
    admin_secret_token = "loom_admin_" + "P" * 43

    with patch("loom_cli.service_cmd._ensure_docker_compose_available",
               return_value=0), \
         patch("loom_cli.service_cmd._run", side_effect=_capture_run), \
         patch("loom_cli.service_cmd._wait_for_postgres",
               return_value=True), \
         patch("loom_cli.service_cmd._alembic_upgrade",
               return_value=0), \
         patch("loom_cli.service_cmd._seed_test_data",
               return_value=(0, fake_tokens)), \
         patch("loom_cli.service_cmd._ensure_dev_admin_secret",
               return_value=admin_secret_token), \
         patch("loom_cli.service_cmd._mint_batch_runner_cp_token",
               return_value=None):
        rc = main([
            "service", "up", "--environment", "local",
            "--compose-file", str(compose),
            "--env-file", str(env_file),
        ])

    assert rc == 0
    content = env_file.read_text()
    # The original key must be unchanged.
    assert f"LOOM_SECRET_STORE_MASTER_KEY={existing_key}" in content
    # No duplicate lines for the key.
    key_lines = [
        line for line in content.splitlines()
        if line.startswith("LOOM_SECRET_STORE_MASTER_KEY=")
    ]
    assert len(key_lines) == 1, (
        f"expected exactly one LOOM_SECRET_STORE_MASTER_KEY line; "
        f"got {len(key_lines)}: {key_lines!r}"
    )
