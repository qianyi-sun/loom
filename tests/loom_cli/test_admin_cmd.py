"""`loom admin tokens worker {mint, revoke, rotate}` tests (#80)."""

from __future__ import annotations

import hashlib
import io
import json
from typing import Any

import httpx
import pytest

import loom_cli.environment_state as environment_state
from loom_cli.__main__ import main
from loom_cli.environment_state import StateDrift


class _StubResponse:
    def __init__(self, status_code: int, json_data: Any = None, text: str = "") -> None:
        self.status_code = status_code
        self._json = json_data
        self.text = text

    def json(self) -> Any:
        return self._json


# ──────────────────────────────────────────────────────────────────────
# loom admin tokens worker mint
# ──────────────────────────────────────────────────────────────────────


def test_mint_posts_to_cp_with_bearer(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    captured: dict[str, Any] = {}

    def _fake_post(url, *, json, headers, timeout):  # type: ignore[no-untyped-def]
        captured["url"] = url
        captured["json"] = json
        captured["headers"] = headers
        return _StubResponse(
            201,
            json_data={
                "token": "loom_w_deadbeef",
                "token_hash_prefix": "ab12cd34",
            },
        )

    monkeypatch.setattr(httpx, "post", _fake_post)
    monkeypatch.setenv("LOOM_ADMIN_TOKEN", "admin-secret")

    rc = main(
        [
            "admin",
            "tokens",
            "worker",
            "mint",
            "--cp-url",
            "http://cp.example:8080",
            "--expires-in-days",
            "30",
        ]
    )
    assert rc == 0
    assert captured["url"] == "http://cp.example:8080/admin/worker-tokens"
    assert captured["json"] == {"expires_in_days": 30}
    assert captured["headers"]["Authorization"] == "Bearer admin-secret"

    out = capsys.readouterr().out
    assert "ab12cd34" in out
    # Default text mode must NOT print the raw token — terminal
    # scrollback risk. Operator opts in via --show-secret or pipes
    # --format json into a secret store.
    assert "loom_w_deadbeef" not in out
    assert "--show-secret" in out  # capture hint
    assert "--from-file=worker-token=/dev/stdin" in out  # safe-install hint


def test_mint_strips_trailing_slash_on_cp_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, str] = {}

    def _fake_post(url, **kwargs):  # type: ignore[no-untyped-def]
        captured["url"] = url
        return _StubResponse(
            201,
            json_data={"token": "x", "token_hash_prefix": "y"},
        )

    monkeypatch.setattr(httpx, "post", _fake_post)
    monkeypatch.setenv("LOOM_ADMIN_TOKEN", "t")
    main(
        [
            "admin",
            "tokens",
            "worker",
            "mint",
            "--cp-url",
            "http://cp:8080/",
        ]
    )
    assert captured["url"] == "http://cp:8080/admin/worker-tokens"


def test_mint_json_format_emits_raw_json(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def _fake_post(url, **kwargs):  # type: ignore[no-untyped-def]
        return _StubResponse(
            201,
            json_data={"token": "t", "token_hash_prefix": "p"},
        )

    monkeypatch.setattr(httpx, "post", _fake_post)
    monkeypatch.setenv("LOOM_ADMIN_TOKEN", "t")
    rc = main(
        [
            "admin",
            "tokens",
            "worker",
            "mint",
            "--format",
            "json",
        ]
    )
    assert rc == 0
    out = capsys.readouterr().out
    parsed = json.loads(out)
    assert parsed == {"token": "t", "token_hash_prefix": "p"}
    # Rollout-hint text MUST NOT be present in JSON mode.
    assert "loom-secrets" not in out


def test_mint_show_secret_prints_raw_token(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """--show-secret is the explicit opt-in to print the raw token in
    text mode. Operator takes responsibility for terminal scrollback."""

    def _fake_post(url, **kwargs):  # type: ignore[no-untyped-def]
        return _StubResponse(
            201,
            json_data={
                "token": "loom_w_explicitlyshown",
                "token_hash_prefix": "shown",
            },
        )

    monkeypatch.setattr(httpx, "post", _fake_post)
    monkeypatch.setenv("LOOM_ADMIN_TOKEN", "t")
    rc = main(
        ["admin", "tokens", "worker", "mint", "--show-secret"],
    )
    assert rc == 0
    out = capsys.readouterr().out
    assert "loom_w_explicitlyshown" in out
    assert "shown" in out
    assert "loom-secrets" in out  # rollout hint


def test_mint_admin_token_env_missing_returns_2(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """When env:VAR resolves to an unset variable, exit 2. Test uses a
    deliberately unique var name to dodge the dev `.env` file (loaded
    by `_load_dotenv_from_cwd` at the start of every `main()` call)."""
    monkeypatch.delenv("LOOM_ADMIN_TOKEN_FOR_TEST", raising=False)
    rc = main(
        [
            "admin",
            "tokens",
            "worker",
            "mint",
            "--admin-token",
            "env:LOOM_ADMIN_TOKEN_FOR_TEST",
        ]
    )
    assert rc == 2
    assert "LOOM_ADMIN_TOKEN_FOR_TEST" in capsys.readouterr().err


def test_mint_cp_unreachable_returns_2(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def _raise(url, **kwargs):  # type: ignore[no-untyped-def]
        raise httpx.ConnectError("Connection refused")

    monkeypatch.setattr(httpx, "post", _raise)
    monkeypatch.setenv("LOOM_ADMIN_TOKEN", "t")
    rc = main(["admin", "tokens", "worker", "mint"])
    assert rc == 2
    err = capsys.readouterr().err
    assert "could not reach CP" in err
    assert "port-forward" in err


def test_mint_cp_non_201_returns_1(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def _fake_post(url, **kwargs):  # type: ignore[no-untyped-def]
        return _StubResponse(403, json_data=None, text="missing scope")

    monkeypatch.setattr(httpx, "post", _fake_post)
    monkeypatch.setenv("LOOM_ADMIN_TOKEN", "wrong-token")
    rc = main(["admin", "tokens", "worker", "mint"])
    assert rc == 1
    assert "403" in capsys.readouterr().err


def test_mint_admin_token_via_stdin(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    def _fake_post(url, *, json, headers, timeout):  # type: ignore[no-untyped-def]
        captured["headers"] = headers
        return _StubResponse(
            201,
            json_data={"token": "x", "token_hash_prefix": "y"},
        )

    monkeypatch.setattr(httpx, "post", _fake_post)
    monkeypatch.setattr("sys.stdin", io.StringIO("from-stdin\n"))
    rc = main(
        [
            "admin",
            "tokens",
            "worker",
            "mint",
            "--admin-token",
            "-",
        ]
    )
    assert rc == 0
    assert captured["headers"]["Authorization"] == "Bearer from-stdin"


def test_mint_admin_token_via_file(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Any,
) -> None:
    captured: dict[str, Any] = {}
    token_file = tmp_path / "admin.txt"
    token_file.write_text("from-file\n")

    def _fake_post(url, *, json, headers, timeout):  # type: ignore[no-untyped-def]
        captured["headers"] = headers
        return _StubResponse(
            201,
            json_data={"token": "x", "token_hash_prefix": "y"},
        )

    monkeypatch.setattr(httpx, "post", _fake_post)
    rc = main(
        [
            "admin",
            "tokens",
            "worker",
            "mint",
            "--admin-token",
            f"file:{token_file}",
        ]
    )
    assert rc == 0
    assert captured["headers"]["Authorization"] == "Bearer from-file"


# ──────────────────────────────────────────────────────────────────────
# loom admin tokens worker revoke
# ──────────────────────────────────────────────────────────────────────


def test_revoke_deletes_at_correct_url(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    captured: dict[str, Any] = {}

    def _fake_delete(url, *, headers, timeout):  # type: ignore[no-untyped-def]
        captured["url"] = url
        captured["headers"] = headers
        return _StubResponse(200, json_data={"status": "revoked"})

    monkeypatch.setattr(httpx, "delete", _fake_delete)
    monkeypatch.setenv("LOOM_ADMIN_TOKEN", "t")
    rc = main(
        [
            "admin",
            "tokens",
            "worker",
            "revoke",
            "ab12cd34",
            "--cp-url",
            "http://cp:8080",
        ]
    )
    assert rc == 0
    assert captured["url"] == ("http://cp:8080/admin/worker-tokens/ab12cd34")
    assert "ab12cd34" in capsys.readouterr().out


def test_revoke_rejects_non_hex_prefix(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Server-side regex also rejects non-hex, but we catch client-side
    to avoid a round-trip."""
    monkeypatch.setenv("LOOM_ADMIN_TOKEN", "t")
    rc = main(
        [
            "admin",
            "tokens",
            "worker",
            "revoke",
            "not-hex!",
        ]
    )
    assert rc == 2
    assert "hex" in capsys.readouterr().err.lower()


def test_revoke_rejects_too_short_prefix(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """3 chars < the 4-char floor that prevents wildcard-revoke
    disasters."""
    monkeypatch.setenv("LOOM_ADMIN_TOKEN", "t")
    rc = main(["admin", "tokens", "worker", "revoke", "abc"])
    assert rc == 2
    assert "4-64" in capsys.readouterr().err


def test_revoke_cp_unreachable_returns_2(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def _raise(url, **kwargs):  # type: ignore[no-untyped-def]
        raise httpx.ConnectError("nope")

    monkeypatch.setattr(httpx, "delete", _raise)
    monkeypatch.setenv("LOOM_ADMIN_TOKEN", "t")
    rc = main(
        [
            "admin",
            "tokens",
            "worker",
            "revoke",
            "ab12cd34",
        ]
    )
    assert rc == 2
    assert "could not reach CP" in capsys.readouterr().err


def test_revoke_cp_non_200_returns_1(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def _fake_delete(url, **kwargs):  # type: ignore[no-untyped-def]
        return _StubResponse(400, text="prefix must be 4-64 hex characters")

    monkeypatch.setattr(httpx, "delete", _fake_delete)
    monkeypatch.setenv("LOOM_ADMIN_TOKEN", "t")
    rc = main(
        [
            "admin",
            "tokens",
            "worker",
            "revoke",
            "abcd",
        ]
    )
    assert rc == 1


# ──────────────────────────────────────────────────────────────────────
# loom admin tokens worker rotate
# ──────────────────────────────────────────────────────────────────────


def test_rotate_mints_then_prints_checklist(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Rotate is `mint` + a checklist. Must NOT call delete."""
    delete_called = False

    def _fake_post(url, **kwargs):  # type: ignore[no-untyped-def]
        return _StubResponse(
            201,
            json_data={"token": "loom_w_new", "token_hash_prefix": "newpref"},
        )

    def _fake_delete(url, **kwargs):  # type: ignore[no-untyped-def]
        nonlocal delete_called
        delete_called = True
        return _StubResponse(200, json_data={"status": "revoked"})

    monkeypatch.setattr(httpx, "post", _fake_post)
    monkeypatch.setattr(httpx, "delete", _fake_delete)
    monkeypatch.setenv("LOOM_ADMIN_TOKEN", "t")

    rc = main(["admin", "tokens", "worker", "rotate"])
    assert rc == 0
    assert not delete_called  # rotate doesn't auto-revoke
    out = capsys.readouterr().out
    assert "newpref" in out
    # Default text mode must NOT print the raw token (same as mint).
    assert "loom_w_new" not in out
    assert "Rotation checklist" in out
    assert "rollout restart" in out
    assert "loom admin tokens worker revoke" in out
    # Without --show-secret the checklist must point at the safe
    # capture path (pipe-stdin), not at `kubectl patch` with the
    # raw token in argv.
    assert "--from-file=worker-token=/dev/stdin" in out


def test_rotate_show_secret_prints_token_and_kubectl_install(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """--show-secret on rotate prints the raw token and the install
    step uses the literal-from-stdin form, not a raw `kubectl patch -p`
    that would put the token on argv."""

    def _fake_post(url, **kwargs):  # type: ignore[no-untyped-def]
        return _StubResponse(
            201,
            json_data={
                "token": "loom_w_rotated",
                "token_hash_prefix": "rotpref",
            },
        )

    monkeypatch.setattr(httpx, "post", _fake_post)
    monkeypatch.setenv("LOOM_ADMIN_TOKEN", "t")
    rc = main(["admin", "tokens", "worker", "rotate", "--show-secret"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "loom_w_rotated" in out
    assert "rotpref" in out
    # The install step in the checklist must NOT use `kubectl patch -p`
    # with the raw token in argv. The safe pattern uses
    # --from-literal or --from-file via apply.
    assert "kubectl create secret generic loom-secrets" in out
    assert "kubectl rollout restart" in out


def test_rotate_mint_failure_propagates_exit_code(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def _fake_post(url, **kwargs):  # type: ignore[no-untyped-def]
        return _StubResponse(403, text="missing scope")

    monkeypatch.setattr(httpx, "post", _fake_post)
    monkeypatch.setenv("LOOM_ADMIN_TOKEN", "wrong")
    rc = main(["admin", "tokens", "worker", "rotate"])
    assert rc == 1
    # Checklist must NOT have been printed when mint failed.
    assert "Rotation checklist" not in capsys.readouterr().out


def test_rotate_json_mode_skips_checklist(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """--format json → output is just the mint JSON, no human-readable
    rollout text (would break parsers)."""

    def _fake_post(url, **kwargs):  # type: ignore[no-untyped-def]
        return _StubResponse(
            201,
            json_data={"token": "t", "token_hash_prefix": "p"},
        )

    monkeypatch.setattr(httpx, "post", _fake_post)
    monkeypatch.setenv("LOOM_ADMIN_TOKEN", "t")
    rc = main(
        [
            "admin",
            "tokens",
            "worker",
            "rotate",
            "--format",
            "json",
        ]
    )
    assert rc == 0
    out = capsys.readouterr().out
    assert "Rotation checklist" not in out
    assert "rollout restart" not in out


# ──────────────────────────────────────────────────────────────────────
# loom admin slurm-workers status
# ──────────────────────────────────────────────────────────────────────


def test_slurm_workers_status_gets_cp_capacity_without_printing_secrets(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    captured: dict[str, Any] = {}

    def _fake_get(url, *, headers, timeout):  # type: ignore[no-untyped-def]
        captured["url"] = url
        captured["headers"] = headers
        return _StubResponse(
            200,
            json_data={
                "summary": [
                    {
                        "environment": "production",
                        "pool_name": "oldlab",
                        "desired_slots": 18,
                        "active_slots": 12,
                        "pending_slots": 6,
                        "running_jobs": 2,
                        "pending_jobs": 1,
                        "stale_slots": 6,
                        "stale_jobs": 1,
                        "failed_submissions": 0,
                        "cancelled_pending_jobs": 0,
                        "idle_exits": 1,
                    },
                ],
                "jobs": [
                    {
                        "job_id": "13441",
                        "environment": "production",
                        "pool_name": "oldlab",
                        "state": "running",
                        "nodelist": "oldlab-1",
                        "requested_concurrency": 6,
                        "redacted_env": {"LOOM_WORKER_TOKEN": "<redacted>"},
                    },
                ],
            },
        )

    monkeypatch.setattr(httpx, "get", _fake_get)
    monkeypatch.setenv("LOOM_ADMIN_TOKEN", "admin-secret")

    rc = main(
        [
            "admin",
            "slurm-workers",
            "status",
            "--cp-url",
            "http://cp:8080/",
        ]
    )
    assert rc == 0
    assert captured["url"] == "http://cp:8080/admin/slurm-worker-jobs/status"
    assert captured["headers"]["Authorization"] == "Bearer admin-secret"

    out = capsys.readouterr().out
    assert "production/oldlab" in out
    assert "desired=18 active=12 pending=6" in out
    assert "stale=6" in out
    assert "stale_jobs=1" in out
    assert "13441" in out
    assert "loom_w_secret" not in out
    assert "<redacted>" in out


def test_slurm_workers_status_json_format_emits_raw_json(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    payload = {"summary": [], "jobs": []}

    def _fake_get(url, **kwargs):  # type: ignore[no-untyped-def]
        return _StubResponse(200, json_data=payload)

    monkeypatch.setattr(httpx, "get", _fake_get)
    monkeypatch.setenv("LOOM_ADMIN_TOKEN", "admin-secret")

    rc = main(["admin", "slurm-workers", "status", "--format", "json"])
    assert rc == 0
    assert json.loads(capsys.readouterr().out) == payload


# ──────────────────────────────────────────────────────────────────────
# loom admin gb10-workers status
# ──────────────────────────────────────────────────────────────────────


def test_gb10_workers_status_gets_cp_rollout_state_without_secrets(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    captured: dict[str, Any] = {}

    def _fake_get(url, *, headers, timeout):  # type: ignore[no-untyped-def]
        captured["url"] = url
        captured["headers"] = headers
        return _StubResponse(
            200,
            json_data={
                "desired_states": [
                    {
                        "environment": "production",
                        "pool_name": "gb10-arm64",
                        "image_tag": "2026-06-26-gb10",
                        "max_concurrent": 10,
                        "env_config_version": "gb10-env-v2",
                        "previous_image_tag": "2026-06-25-gb10",
                    },
                ],
                "nodes": [
                    {
                        "environment": "production",
                        "pool_name": "gb10-arm64",
                        "hostname": "trt-gb10-1",
                        "apply_state": "applied",
                        "current_image_tag": "2026-06-26-gb10",
                        "desired_image_tag": "2026-06-26-gb10",
                        "current_max_concurrent": 10,
                        "desired_max_concurrent": 10,
                        "current_env_config_version": "gb10-env-v2",
                        "desired_env_config_version": "gb10-env-v2",
                        "last_apply_result": "ok",
                        "error_message": None,
                    },
                ],
            },
        )

    monkeypatch.setattr(httpx, "get", _fake_get)
    monkeypatch.setenv("LOOM_ADMIN_TOKEN", "admin-secret")

    rc = main(
        [
            "admin",
            "gb10-workers",
            "status",
            "--cp-url",
            "http://cp:8080/",
        ]
    )
    assert rc == 0
    assert captured["url"] == "http://cp:8080/admin/gb10-worker-pools/status"
    assert captured["headers"]["Authorization"] == "Bearer admin-secret"

    out = capsys.readouterr().out
    assert "production/gb10-arm64" in out
    assert "2026-06-26-gb10" in out
    assert "trt-gb10-1" in out
    assert "applied" in out
    assert "gb10-env-v2" in out
    assert "loom_w_secret" not in out


def test_gb10_workers_status_json_format_emits_raw_json(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    payload = {"desired_states": [], "nodes": []}

    def _fake_get(url, **kwargs):  # type: ignore[no-untyped-def]
        return _StubResponse(200, json_data=payload)

    monkeypatch.setattr(httpx, "get", _fake_get)
    monkeypatch.setenv("LOOM_ADMIN_TOKEN", "admin-secret")

    rc = main(["admin", "gb10-workers", "status", "--format", "json"])
    assert rc == 0
    assert json.loads(capsys.readouterr().out) == payload


def test_gb10_workers_status_release_target_gate_fails_on_stale_nodes(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def _fake_get(url, **kwargs):  # type: ignore[no-untyped-def]
        return _StubResponse(
            200,
            json_data={
                "desired_states": [
                    {
                        "environment": "production",
                        "pool_name": "gb10-arm64",
                        "image_tag": "public-beta-new",
                        "max_concurrent": 10,
                        "env_config_version": "env-new",
                        "previous_image_tag": "public-beta-old",
                    },
                ],
                "nodes": [
                    {
                        "environment": "production",
                        "pool_name": "gb10-arm64",
                        "hostname": "trt-gb10-1",
                        "apply_state": "applied",
                        "current_image_tag": "public-beta-old",
                        "desired_image_tag": "public-beta-new",
                        "current_max_concurrent": 10,
                        "desired_max_concurrent": 10,
                        "current_env_config_version": "env-old",
                        "desired_env_config_version": "env-new",
                        "current_intent": "active",
                        "desired_intent": "active",
                        "last_apply_result": "already current",
                        "error_message": None,
                    },
                ],
            },
        )

    monkeypatch.setattr(httpx, "get", _fake_get)
    monkeypatch.setenv("LOOM_ADMIN_TOKEN", "admin-secret")

    rc = main(
        [
            "admin",
            "gb10-workers",
            "status",
            "--environment",
            "production",
            "--pool-name",
            "gb10-arm64",
            "--release-image-tag",
            "public-beta-new",
            "--release-env-config-version",
            "env-new",
        ]
    )

    assert rc == 1
    err = capsys.readouterr().err
    assert "GB10 rollout target mismatch" in err
    assert "trt-gb10-1" in err
    assert "public-beta-old" in err
    assert "env-old" in err


# ──────────────────────────────────────────────────────────────────────
# loom admin worker-pools autoscaler status
# ──────────────────────────────────────────────────────────────────────


def test_worker_pool_autoscaler_status_gets_cp_decisions(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    captured: dict[str, Any] = {}

    def _fake_get(url, *, headers, timeout):  # type: ignore[no-untyped-def]
        captured["url"] = url
        captured["headers"] = headers
        return _StubResponse(
            200,
            json_data={
                "policies": [
                    {
                        "environment": "production",
                        "pool_name": "oldlab",
                        "actuator": "slurm",
                        "enabled": True,
                        "min_slots": 6,
                        "max_slots": 30,
                        "last_desired_slots": 12,
                        "last_actual_slots": 6,
                        "last_pending_slots": 6,
                        "last_draining_slots": 0,
                        "last_occupied_slots": 6,
                        "last_queued_slots": 7,
                        "last_decision": "scale_up",
                        "last_decision_reason": "queued_deficit",
                        "last_blocked_reason": None,
                        "last_error": None,
                    },
                ],
            },
        )

    monkeypatch.setattr(httpx, "get", _fake_get)
    monkeypatch.setenv("LOOM_ADMIN_TOKEN", "admin-secret")

    rc = main([
        "admin", "worker-pools", "autoscaler", "status",
        "--cp-url", "http://cp:8080/",
    ])

    assert rc == 0
    assert captured["url"] == "http://cp:8080/admin/worker-pool-autoscalers/status"
    assert captured["headers"]["Authorization"] == "Bearer admin-secret"
    out = capsys.readouterr().out
    assert "production/oldlab" in out
    assert "desired=12 actual=6 pending=6 draining=0" in out
    assert "decision=scale_up" in out


def test_worker_pool_autoscaler_status_json_format_emits_raw_json(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    payload = {"policies": []}

    def _fake_get(url, **kwargs):  # type: ignore[no-untyped-def]
        return _StubResponse(200, json_data=payload)

    monkeypatch.setattr(httpx, "get", _fake_get)
    monkeypatch.setenv("LOOM_ADMIN_TOKEN", "admin-secret")

    rc = main([
        "admin", "worker-pools", "autoscaler", "status", "--format", "json",
    ])
    assert rc == 0
    assert json.loads(capsys.readouterr().out) == payload


# ──────────────────────────────────────────────────────────────────────
# loom admin environment-state apply/check
# ──────────────────────────────────────────────────────────────────────


def _write_environment_state_profile(path: Path) -> None:
    path.write_text(
        """
environment = "public-beta"

[[worker_pool_autoscaler_policies]]
pool_name = "gb10-arm64"
actuator = "slurm"
enabled = true
min_slots = 0
max_slots = 150
scale_up_threshold_slots = 1
scale_down_idle_seconds = 600
scale_up_cooldown_seconds = 60
scale_down_cooldown_seconds = 300
drain_timeout_seconds = 600
force = false

[worker_pool_autoscaler_policies.actuator_config]
backend = "docker"
cpu_arch = "arm64"
partition = "gb10"
allowed_nodes = ["trt-gb10-1"]
requested_concurrency = 10
max_jobs = 15
pending_job_cap = 2

[[gb10_worker_pool_desired_states]]
pool_name = "gb10-arm64"
image_tag = "${IMAGE_TAG}"
max_concurrent = 10
env_config_version = "${ENV_CONFIG_VERSION}"
target_slots = 150

[gb10_worker_pool_desired_states.rollout_policy]
mode = "all"
""".strip()
        + "\n",
        encoding="utf-8",
    )


def test_environment_state_apply_puts_profile_resources(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    profile_path = tmp_path / "public-beta.state.toml"
    _write_environment_state_profile(profile_path)
    captured: list[dict[str, Any]] = []

    def _fake_put(url, *, json, headers, timeout):  # type: ignore[no-untyped-def]
        captured.append({"url": url, "json": json, "headers": headers})
        return _StubResponse(200, json_data={"ok": True})

    monkeypatch.setattr(httpx, "put", _fake_put)
    monkeypatch.setenv("LOOM_ADMIN_TOKEN", "admin-secret")

    rc = main(
        [
            "admin",
            "environment-state",
            "apply",
            "--cp-url",
            "http://cp:8080/",
            "--file",
            str(profile_path),
            "--environment",
            "public-beta",
            "--var",
            "IMAGE_TAG=public-beta-57a7509",
            "--var",
            "ENV_CONFIG_VERSION=public-beta-57a7509",
        ]
    )

    assert rc == 0
    assert [item["url"] for item in captured] == [
        "http://cp:8080/admin/worker-pool-autoscaler-policies/public-beta/gb10-arm64",
        "http://cp:8080/admin/gb10-worker-pools/public-beta/gb10-arm64/desired-state",
    ]
    assert captured[0]["headers"]["Authorization"] == "Bearer admin-secret"
    assert captured[0]["json"]["actuator"] == "slurm"
    assert captured[0]["json"]["actuator_config"]["partition"] == "gb10"
    assert captured[1]["json"]["image_tag"] == "public-beta-57a7509"
    assert "Applied environment state public-beta" in capsys.readouterr().out


def test_environment_state_check_fails_with_actionable_drift(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    profile_path = tmp_path / "public-beta.state.toml"
    _write_environment_state_profile(profile_path)

    def _fake_get(url, *, headers, timeout):  # type: ignore[no-untyped-def]
        if url.endswith("/admin/worker-pool-autoscalers/status"):
            return _StubResponse(
                200,
                json_data={
                    "policies": [
                        {
                            "environment": "public-beta",
                            "pool_name": "gb10-arm64",
                            "actuator": "gb10",
                            "enabled": True,
                            "min_slots": 0,
                            "max_slots": 150,
                            "scale_up_threshold_slots": 1,
                            "scale_down_idle_seconds": 600,
                            "scale_up_cooldown_seconds": 60,
                            "scale_down_cooldown_seconds": 300,
                            "drain_timeout_seconds": 600,
                            "force": False,
                            "actuator_config": {"backend": "docker", "cpu_arch": "arm64"},
                        },
                    ],
                },
            )
        if url.endswith("/admin/gb10-worker-pools/status"):
            return _StubResponse(
                200,
                json_data={
                    "desired_states": [
                        {
                            "environment": "public-beta",
                            "pool_name": "gb10-arm64",
                            "image_tag": "public-beta-old",
                            "max_concurrent": 10,
                            "env_config_version": "public-beta-old",
                            "target_slots": 150,
                            "host_intents": {},
                            "rollout_policy": {"mode": "all"},
                            "env": {},
                        },
                    ],
                },
            )
        if url.endswith("/admin/slurm-worker-jobs/status"):
            return _StubResponse(200, json_data={"jobs": []})
        raise AssertionError(url)

    monkeypatch.setattr(httpx, "get", _fake_get)
    monkeypatch.setenv("LOOM_ADMIN_TOKEN", "admin-secret")

    rc = main(
        [
            "admin",
            "environment-state",
            "check",
            "--file",
            str(profile_path),
            "--environment",
            "public-beta",
            "--var",
            "IMAGE_TAG=public-beta-57a7509",
            "--var",
            "ENV_CONFIG_VERSION=public-beta-57a7509",
        ]
    )

    assert rc == 1
    err = capsys.readouterr().err
    assert "Environment state drift for public-beta" in err
    assert "worker_pool_autoscaler_policies[public-beta/gb10-arm64].actuator" in err
    assert "desired='slurm' live='gb10'" in err
    assert "gb10_worker_pool_desired_states[public-beta/gb10-arm64].image_tag" in err


def test_environment_state_check_fetches_slurm_jobs_and_reports_external_prereq_drift(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    profile_path = tmp_path / "public-beta.state.toml"
    _write_environment_state_profile(profile_path)
    called_urls: list[str] = []

    def _fake_get(url, *, headers, timeout):  # type: ignore[no-untyped-def]
        called_urls.append(url)
        if url.endswith("/admin/worker-pool-autoscalers/status"):
            return _StubResponse(
                200,
                json_data={
                    "policies": [
                        {
                            "environment": "public-beta",
                            "pool_name": "gb10-arm64",
                            "actuator": "slurm",
                            "enabled": True,
                            "min_slots": 0,
                            "max_slots": 150,
                            "scale_up_threshold_slots": 1,
                            "scale_down_idle_seconds": 600,
                            "scale_up_cooldown_seconds": 60,
                            "scale_down_cooldown_seconds": 300,
                            "drain_timeout_seconds": 600,
                            "force": False,
                            "actuator_config": {
                                "backend": "docker",
                                "cpu_arch": "arm64",
                                "partition": "gb10",
                                "allowed_nodes": ["trt-gb10-1"],
                                "requested_concurrency": 10,
                                "max_jobs": 15,
                                "pending_job_cap": 2,
                            },
                        },
                    ],
                },
            )
        if url.endswith("/admin/gb10-worker-pools/status"):
            return _StubResponse(
                200,
                json_data={
                    "desired_states": [
                        {
                            "environment": "public-beta",
                            "pool_name": "gb10-arm64",
                            "image_tag": "public-beta-57a7509",
                            "max_concurrent": 10,
                            "env_config_version": "public-beta-57a7509",
                            "target_slots": 150,
                            "host_intents": {},
                            "rollout_policy": {"mode": "all"},
                            "env": {},
                        },
                    ],
                },
            )
        if url.endswith("/admin/slurm-worker-jobs/status"):
            return _StubResponse(200, json_data={"jobs": []})
        raise AssertionError(url)

    monkeypatch.setattr(httpx, "get", _fake_get)
    monkeypatch.setattr(
        environment_state,
        "diff_external_slurm_runner_prerequisites",
        lambda profile, **kwargs: [
            StateDrift(
                path="external_slurm_runner_prerequisites[production/oldlab].env_file",
                desired="/shared_work/qianyi/loom-worker-capacity/public-beta-oldlab-worker.env",
                live="missing",
            ),
        ],
    )
    monkeypatch.setenv("LOOM_ADMIN_TOKEN", "admin-secret")

    rc = main(
        [
            "admin",
            "environment-state",
            "check",
            "--file",
            str(profile_path),
            "--environment",
            "public-beta",
            "--var",
            "IMAGE_TAG=public-beta-57a7509",
            "--var",
            "ENV_CONFIG_VERSION=public-beta-57a7509",
        ]
    )

    assert rc == 1
    assert "http://localhost:8080/admin/slurm-worker-jobs/status" in called_urls
    err = capsys.readouterr().err
    assert "external_slurm_runner_prerequisites[production/oldlab].env_file" in err


def test_environment_state_check_fails_before_cp_when_admin_token_fingerprint_drifts(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    profile_path = tmp_path / "public-beta.state.toml"
    _write_environment_state_profile(profile_path)
    stale_admin_token = "loom_admin_operator_stale_secret"
    stale_fingerprint = (
        "sha256:"
        f"{hashlib.sha256(stale_admin_token.encode('utf-8')).hexdigest()[:12]} "
        f"len={len(stale_admin_token)}"
    )

    def _fake_get(url, *, headers, timeout):  # type: ignore[no-untyped-def]
        raise AssertionError("environment-state check contacted CP before token drift preflight")

    monkeypatch.setattr(httpx, "get", _fake_get)
    monkeypatch.setenv("LOOM_ADMIN_TOKEN", stale_admin_token)

    rc = main(
        [
            "admin",
            "environment-state",
            "check",
            "--file",
            str(profile_path),
            "--environment",
            "public-beta",
            "--var",
            "IMAGE_TAG=public-beta-57a7509",
            "--var",
            "ENV_CONFIG_VERSION=public-beta-57a7509",
            "--expect-admin-token-fingerprint",
            "sha256:liveexpected len=36",
        ]
    )

    assert rc == 1
    err = capsys.readouterr().err
    assert "admin_token_fingerprint" in err
    assert "sha256:liveexpected len=36" in err
    assert stale_fingerprint in err
    assert stale_admin_token not in err


def test_environment_state_check_passes_worker_token_without_leaking_secret(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    profile_path = tmp_path / "public-beta.state.toml"
    _write_environment_state_profile(profile_path)
    active_token = "loom_w_current_environment_secret"

    def _fake_get(url, *, headers, timeout):  # type: ignore[no-untyped-def]
        if url.endswith("/admin/worker-pool-autoscalers/status"):
            return _StubResponse(
                200,
                json_data={
                    "policies": [
                        {
                            "environment": "public-beta",
                            "pool_name": "gb10-arm64",
                            "actuator": "slurm",
                            "enabled": True,
                            "min_slots": 0,
                            "max_slots": 150,
                            "scale_up_threshold_slots": 1,
                            "scale_down_idle_seconds": 600,
                            "scale_up_cooldown_seconds": 60,
                            "scale_down_cooldown_seconds": 300,
                            "drain_timeout_seconds": 600,
                            "force": False,
                            "actuator_config": {
                                "backend": "docker",
                                "cpu_arch": "arm64",
                                "partition": "gb10",
                                "allowed_nodes": ["trt-gb10-1"],
                                "requested_concurrency": 10,
                                "max_jobs": 15,
                                "pending_job_cap": 2,
                            },
                        },
                    ],
                },
            )
        if url.endswith("/admin/gb10-worker-pools/status"):
            return _StubResponse(
                200,
                json_data={
                    "desired_states": [
                        {
                            "environment": "public-beta",
                            "pool_name": "gb10-arm64",
                            "image_tag": "public-beta-57a7509",
                            "max_concurrent": 10,
                            "env_config_version": "public-beta-57a7509",
                            "target_slots": 150,
                            "host_intents": {},
                            "rollout_policy": {"mode": "all"},
                            "env": {},
                        },
                    ],
                },
            )
        if url.endswith("/admin/slurm-worker-jobs/status"):
            return _StubResponse(200, json_data={"jobs": []})
        raise AssertionError(url)

    def _fake_external_prereqs(profile, *, expected_worker_token=None):  # type: ignore[no-untyped-def]
        assert expected_worker_token == active_token
        return [
            StateDrift(
                path=(
                    "external_slurm_runner_prerequisites"
                    "[public-beta/gb10-arm64].worker_token_fingerprint"
                ),
                desired="sha256:active123456 len=33",
                live="sha256:stale1234567 len=28",
            ),
        ]

    monkeypatch.setattr(httpx, "get", _fake_get)
    monkeypatch.setattr(
        environment_state,
        "diff_external_slurm_runner_prerequisites",
        _fake_external_prereqs,
    )
    monkeypatch.setenv("LOOM_ADMIN_TOKEN", "admin-secret")
    monkeypatch.setenv("LOOM_WORKER_TOKEN", active_token)

    rc = main(
        [
            "admin",
            "environment-state",
            "check",
            "--file",
            str(profile_path),
            "--environment",
            "public-beta",
            "--var",
            "IMAGE_TAG=public-beta-57a7509",
            "--var",
            "ENV_CONFIG_VERSION=public-beta-57a7509",
            "--worker-token",
            "env:LOOM_WORKER_TOKEN",
        ]
    )

    assert rc == 1
    err = capsys.readouterr().err
    assert "worker_token_fingerprint" in err
    assert "sha256:active123456 len=33" in err
    assert active_token not in err


# ──────────────────────────────────────────────────────────────────────
# loom admin tokens team — uses loom_service /api/v1/tokens
# ──────────────────────────────────────────────────────────────────────


from pathlib import Path  # noqa: E402

_TEAM_ID = "00000000-0000-0000-0000-000000000aaa"


@pytest.fixture
def _team_logged_in(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Each team-token test starts logged in with an isolated config
    dir so we don't read the dev `~/.config/loom/config.toml`."""
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path))
    monkeypatch.setenv("MY_TOK", "loom_admin_abcdefgh")
    main(
        [
            "auth",
            "login",
            "--server",
            "https://loom.test",
            "--token",
            "env:MY_TOK",
        ]
    )


class _MockServer:
    def __init__(self) -> None:
        self.requests: list[httpx.Request] = []
        self.canned: dict[tuple[str, str], httpx.Response] = {}


@pytest.fixture
def mock_server(
    monkeypatch: pytest.MonkeyPatch,
    _team_logged_in: None,
) -> _MockServer:
    server = _MockServer()

    def _handler(request: httpx.Request) -> httpx.Response:
        server.requests.append(request)
        key = (request.method, request.url.path)
        if key in server.canned:
            return server.canned[key]
        return httpx.Response(404, json={"detail": f"no mock for {key}"})

    transport = httpx.MockTransport(_handler)

    def _patched(cfg: Any, *, timeout: float = 30.0) -> httpx.Client:
        return httpx.Client(
            base_url=cfg.server_url,
            headers={"Authorization": f"Bearer {cfg.auth_token}"},
            transport=transport,
            timeout=timeout,
        )

    monkeypatch.setattr("loom_cli.admin_cmd.authed_client", _patched)
    return server


# ── team mint ────────────────────────────────────────────────────────


def test_team_mint_posts_payload(
    mock_server: _MockServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    mock_server.canned[("POST", "/api/v1/tokens")] = httpx.Response(
        201,
        json={
            "token": "loom_team_xyz",
            "token_hash_prefix": "01234567",
            "expires_at": "2026-09-01T00:00:00+00:00",
        },
    )
    rc = main(
        [
            "admin",
            "tokens",
            "team",
            "mint",
            "--name",
            "ci-submit-token",
            "--team-id",
            _TEAM_ID,
            "--scopes",
            "read:own,submit,providers:manage",
            "--expires-in-days",
            "30",
            "--admin-actor",
            "qianyi",
        ]
    )
    assert rc == 0
    req = mock_server.requests[0]
    body = json.loads(req.content)
    assert body == {
        "name": "ci-submit-token",
        "type": "team",
        "team_id": _TEAM_ID,
        "scopes": ["read:own", "submit", "providers:manage"],
        "expires_in_days": 30,
    }
    assert req.headers["X-Loom-Admin-Actor"] == "qianyi"
    out = capsys.readouterr().out
    assert "01234567" in out
    assert "loom_team_xyz" in out


def test_team_mint_rejects_admin_type_before_request(
    _team_logged_in: None,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Admin credentials are singleton secret-file credentials now.

    The compatibility CLI surface must reject DB-backed admin token creation
    before issuing an HTTP request.
    """
    with pytest.raises(SystemExit) as exc:
        main(
            [
                "admin",
                "tokens",
                "team",
                "mint",
                "--type",
                "admin",
                "--admin-actor",
                "qianyi",
            ]
        )
    assert exc.value.code == 2
    assert "invalid choice" in capsys.readouterr().err


def test_team_mint_rejects_unknown_scope_before_request(
    _team_logged_in: None,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Unknown scopes fail at argparse-time so a typo doesn't burn an
    HTTP round-trip + audit log entry."""
    with pytest.raises(SystemExit):
        main(
            [
                "admin",
                "tokens",
                "team",
                "mint",
                "--scopes",
                "submit,bogus",
                "--team-id",
                _TEAM_ID,
            ]
        )
    err = capsys.readouterr().err
    assert "bogus" in err


def test_team_mint_default_scopes(
    mock_server: _MockServer,
) -> None:
    mock_server.canned[("POST", "/api/v1/tokens")] = httpx.Response(
        201,
        json={
            "token": "loom_team_xyz",
            "token_hash_prefix": "abc12345",
            "expires_at": "2026-09-01T00:00:00+00:00",
        },
    )
    rc = main(
        [
            "admin",
            "tokens",
            "team",
            "mint",
            "--name",
            "default-submit-token",
            "--team-id",
            _TEAM_ID,
        ]
    )
    assert rc == 0
    body = json.loads(mock_server.requests[0].content)
    assert body["scopes"] == ["read:own", "submit"]
    assert body["expires_in_days"] == 90  # default


def test_team_mint_no_admin_actor_omits_header(
    mock_server: _MockServer,
) -> None:
    mock_server.canned[("POST", "/api/v1/tokens")] = httpx.Response(
        201,
        json={
            "token": "t",
            "token_hash_prefix": "ab123456",
            "expires_at": "2026-09-01T00:00:00+00:00",
        },
    )
    main(
        [
            "admin",
            "tokens",
            "team",
            "mint",
            "--name",
            "no-admin-actor-token",
            "--team-id",
            _TEAM_ID,
        ]
    )
    assert "X-Loom-Admin-Actor" not in mock_server.requests[0].headers


def test_team_mint_not_logged_in_returns_2(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path))
    # No `auth login` — config is empty.
    rc = main(
        [
            "admin",
            "tokens",
            "team",
            "mint",
            "--name",
            "not-logged-in-token",
            "--team-id",
            _TEAM_ID,
        ]
    )
    assert rc == 2
    assert "not logged in" in capsys.readouterr().err.lower()


def test_team_mint_server_403_returns_1(
    mock_server: _MockServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    mock_server.canned[("POST", "/api/v1/tokens")] = httpx.Response(
        403,
        json={"detail": "X-Loom-Admin-Actor required"},
    )
    rc = main(
        [
            "admin",
            "tokens",
            "team",
            "mint",
            "--name",
            "forbidden-token",
            "--team-id",
            _TEAM_ID,
        ]
    )
    assert rc == 1
    err = capsys.readouterr().err
    assert "token rejected by server" in err
    assert "X-Loom-Admin-Actor required" in err


def test_team_mint_json_format(
    mock_server: _MockServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    mock_server.canned[("POST", "/api/v1/tokens")] = httpx.Response(
        201,
        json={
            "token": "t",
            "token_hash_prefix": "01234567",
            "expires_at": "2026-09-01T00:00:00+00:00",
        },
    )
    main(
        [
            "admin",
            "tokens",
            "team",
            "mint",
            "--name",
            "json-token",
            "--team-id",
            _TEAM_ID,
            "--format",
            "json",
        ]
    )
    out = capsys.readouterr().out
    parsed = json.loads(out)
    assert parsed["token_hash_prefix"] == "01234567"


# ── team revoke ──────────────────────────────────────────────────────


def test_team_revoke_deletes_at_correct_url(
    mock_server: _MockServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    mock_server.canned[("DELETE", "/api/v1/tokens/01234567")] = httpx.Response(204)
    rc = main(
        [
            "admin",
            "tokens",
            "team",
            "revoke",
            "01234567",
            "--admin-actor",
            "qianyi",
        ]
    )
    assert rc == 0
    req = mock_server.requests[0]
    assert req.method == "DELETE"
    assert req.url.path == "/api/v1/tokens/01234567"
    assert req.headers["X-Loom-Admin-Actor"] == "qianyi"
    assert "01234567" in capsys.readouterr().out


def test_team_revoke_rejects_non_hex_prefix(
    _team_logged_in: None,
    capsys: pytest.CaptureFixture[str],
) -> None:
    rc = main(
        [
            "admin",
            "tokens",
            "team",
            "revoke",
            "deadbeefX",
        ]
    )
    assert rc == 2
    assert "hex" in capsys.readouterr().err.lower()


def test_team_revoke_rejects_wrong_length_prefix(
    _team_logged_in: None,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Server requires exactly 8 hex chars; reject 7 + 9 client-side."""
    for bad in ("abcdef0", "abcdef012"):
        rc = main(
            [
                "admin",
                "tokens",
                "team",
                "revoke",
                bad,
            ]
        )
        assert rc == 2


def test_team_revoke_uppercase_hex_rejected(
    _team_logged_in: None,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Server's check is lowercase hex; client matches."""
    rc = main(
        [
            "admin",
            "tokens",
            "team",
            "revoke",
            "ABCDEF01",
        ]
    )
    assert rc == 2


def test_team_revoke_404_returns_1(
    mock_server: _MockServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    mock_server.canned[("DELETE", "/api/v1/tokens/01234567")] = httpx.Response(
        404, json={"detail": "token not found"}
    )
    rc = main(
        [
            "admin",
            "tokens",
            "team",
            "revoke",
            "01234567",
        ]
    )
    assert rc == 1


# ── team rotate ──────────────────────────────────────────────────────


def test_team_rotate_mints_then_prints_checklist(
    mock_server: _MockServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    mock_server.canned[("POST", "/api/v1/tokens")] = httpx.Response(
        201,
        json={
            "token": "loom_team_new",
            "token_hash_prefix": "newpref0",
            "expires_at": "2026-09-01T00:00:00+00:00",
        },
    )
    rc = main(
        [
            "admin",
            "tokens",
            "team",
            "rotate",
            "--name",
            "rotated-token",
            "--team-id",
            _TEAM_ID,
        ]
    )
    assert rc == 0
    # Only ONE request — rotate does not auto-DELETE.
    assert len(mock_server.requests) == 1
    out = capsys.readouterr().out
    assert "newpref0" in out
    assert "Rotation checklist" in out
    assert "secure channel" in out
    assert "loom admin tokens team revoke" in out


def test_team_rotate_mint_failure_propagates(
    mock_server: _MockServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    mock_server.canned[("POST", "/api/v1/tokens")] = httpx.Response(
        403,
        json={"detail": "X-Loom-Admin-Actor required"},
    )
    rc = main(
        [
            "admin",
            "tokens",
            "team",
            "rotate",
            "--name",
            "blocked-rotate-token",
            "--team-id",
            _TEAM_ID,
        ]
    )
    assert rc == 1
    out = capsys.readouterr().out
    assert "Rotation checklist" not in out


def test_team_rotate_json_skips_checklist(
    mock_server: _MockServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    mock_server.canned[("POST", "/api/v1/tokens")] = httpx.Response(
        201,
        json={
            "token": "t",
            "token_hash_prefix": "ab123456",
            "expires_at": "2026-09-01T00:00:00+00:00",
        },
    )
    rc = main(
        [
            "admin",
            "tokens",
            "team",
            "rotate",
            "--name",
            "json-rotate-token",
            "--team-id",
            _TEAM_ID,
            "--format",
            "json",
        ]
    )
    assert rc == 0
    out = capsys.readouterr().out
    assert "Rotation checklist" not in out


# ── rate-cards sync-yibuapi ──────────────────────────────────────────


def test_rate_cards_sync_yibuapi_posts_public_api(
    mock_server: _MockServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    mock_server.canned[("POST", "/api/v1/rate-cards/sync/yibuapi")] = httpx.Response(
        201,
        json={
            "id": "yibuapi-pricing-v1",
            "source_url": "https://yibuapi.test/api/pricing",
            "pricing_version": "pricing-v1",
            "entry_count": 42,
            "skipped_model_count": 3,
        },
    )

    rc = main(
        [
            "admin",
            "rate-cards",
            "sync-yibuapi",
            "--source-url",
            "https://yibuapi.test/api/pricing",
            "--group",
            "codex",
        ]
    )

    assert rc == 0
    req = mock_server.requests[0]
    assert req.method == "POST"
    assert req.url.path == "/api/v1/rate-cards/sync/yibuapi"
    assert json.loads(req.content) == {
        "source_url": "https://yibuapi.test/api/pricing",
        "group": "codex",
    }
    out = capsys.readouterr().out
    assert "yibuapi-pricing-v1" in out
    assert "42" in out
    assert "pricing-v1" in out


def test_rate_cards_sync_yibuapi_json_output(
    mock_server: _MockServer,
    capsys: pytest.CaptureFixture[str],
) -> None:
    body = {
        "id": "yibuapi-pricing-v1",
        "source_url": "https://yibuapi.test/api/pricing",
        "pricing_version": "pricing-v1",
        "entry_count": 42,
        "skipped_model_count": 3,
    }
    mock_server.canned[("POST", "/api/v1/rate-cards/sync/yibuapi")] = httpx.Response(201, json=body)

    rc = main(
        [
            "admin",
            "rate-cards",
            "sync-yibuapi",
            "--format",
            "json",
        ]
    )

    assert rc == 0
    assert json.loads(capsys.readouterr().out) == body
