from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

from loom_cli.rollout.operator.redaction import (
    known_secrets_from_sources,
    redact_rollout_mapping,
    redact_rollout_text,
    rollout_redaction_scope,
)
from loom_cli.rollout.steps.subprocess_util import format_command, run_captured


def test_redaction_replaces_exact_secrets_before_pattern_redaction() -> None:
    raw_token = "opaque-catalog-password-that-has-no-known-prefix"
    source = "file:/var/lib/loom-staging-rollout/credentials/admin-token"
    text = f"token={raw_token}\nsource={source}\nAuthorization: Bearer loom_admin_abcdef123456\n"

    redacted = redact_rollout_text(text, known_secrets=(raw_token, source))

    assert raw_token not in redacted
    assert source not in redacted
    assert "loom_admin_abcdef123456" not in redacted
    assert "[REDACTED:known-secret]" in redacted
    assert "[REDACTED:bearer]" in redacted


def test_redaction_covers_pem_and_credential_bearing_urls() -> None:
    text = (
        "-----BEGIN OPENSSH PRIVATE KEY-----\n"
        "b3BlbnNzaC1rZXktdjEAAAAA\n"
        "-----END OPENSSH PRIVATE KEY-----\n"
        "postgresql://loom:super-secret@postgres.internal/loom\n"
    )

    redacted = redact_rollout_text(text)

    assert "OPENSSH PRIVATE KEY" not in redacted
    assert "super-secret" not in redacted
    assert "[REDACTED:pem]" in redacted
    assert "[REDACTED:credential-url]" in redacted


def test_mapping_redaction_recurses_without_persisting_exact_values() -> None:
    secret = "catalog-secret-without-prefix"
    payload = {
        "error": f"multiline\n{secret}",
        "nested": [f"https://user:{secret}@example.invalid/path"],
    }

    redacted = redact_rollout_mapping(payload, known_secrets=(secret,))

    assert secret not in str(redacted)
    assert redacted["error"] == "multiline\n[REDACTED:known-secret]"


def test_mapping_redaction_masks_plain_values_under_sensitive_keys() -> None:
    redacted = redact_rollout_mapping(
        {"password": "plain-value", "nested": {"API_KEY": "also-plain"}},
    )

    assert redacted == {
        "password": "[REDACTED:password]",
        "nested": {"API_KEY": "[REDACTED:API_KEY]"},
    }


def test_exact_redaction_covers_json_escaped_multiline_secret() -> None:
    secret = "line-one\nline-two"
    rendered = '{"error":"line-one\\nline-two"}'

    assert secret not in redact_rollout_text(rendered, known_secrets=(secret,))
    assert "line-one\\nline-two" not in redact_rollout_text(
        rendered,
        known_secrets=(secret,),
    )


def test_pattern_redaction_runs_before_output_limit() -> None:
    rendered = redact_rollout_text(
        "prefix loom_admin_abcdef123456 suffix",
        limit=20,
    )

    assert "loom_admin" not in rendered
    assert len(rendered) <= 20


def test_known_secret_loader_does_not_follow_symlink(tmp_path: Path) -> None:
    secret = "opaque-value-behind-symlink"
    target = tmp_path / "target"
    target.write_text(secret, encoding="utf-8")
    target.chmod(0o600)
    link = tmp_path / "link"
    link.symlink_to(target)

    known = known_secrets_from_sources((f"file:{link}",))

    assert secret not in known


def test_redaction_scope_sanitizes_captured_output_before_return_and_write(
    tmp_path: Path,
    monkeypatch,
) -> None:
    secret = "opaque-subprocess-secret"

    class Completed:
        returncode = 1
        stdout = f"stdout {secret}\n"
        stderr = f"stderr {secret}\n"

    monkeypatch.setattr("subprocess.run", lambda *args, **kwargs: Completed())
    stdout = tmp_path / "stdout.log"
    stderr = tmp_path / "stderr.log"

    with rollout_redaction_scope((secret,)):
        result = run_captured(
            ["example-command"],
            stdout_log=stdout,
            stderr_log=stderr,
            sanitize_return=True,
        )

    combined = result.stdout + result.stderr + stdout.read_text() + stderr.read_text()
    assert secret not in combined
    assert combined.count("[REDACTED:known-secret]") == 4


def test_run_captured_preserves_functional_return_but_redacts_diagnostic_log(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    internal_url = "http://loom-control-plane:8080/api"

    class Completed:
        returncode = 0
        stdout = f"endpoint: {internal_url}\n"
        stderr = ""

    monkeypatch.setattr("subprocess.run", lambda *args, **kwargs: Completed())
    stdout = tmp_path / "stdout.log"

    result = run_captured(["render"], stdout_log=stdout)

    assert result.stdout == f"endpoint: {internal_url}\n"
    assert internal_url not in stdout.read_text(encoding="utf-8")
    assert "[REDACTED:internal-url]" in stdout.read_text(encoding="utf-8")


def test_exact_redaction_is_longest_first_and_idempotent() -> None:
    short = "known-secret"
    long = f"prefix-{short}"
    raw = f"{long} then {short}"

    once = redact_rollout_text(raw, known_secrets=(short, long))
    twice = redact_rollout_text(once, known_secrets=(short, long))

    assert short not in once.replace("[REDACTED:known-secret]", "")
    assert long not in once
    assert twice == once


@pytest.mark.parametrize(
    ("raw", "secret"),
    [
        ("TOKEN=opaque-token-value", "opaque-token-value"),
        ("password: plain-password-value", "plain-password-value"),
        ('{"api_key": "opaque-json-value"}', "opaque-json-value"),
        ("AUTH_TOKEN='opaque-auth-value'", "opaque-auth-value"),
    ],
)
def test_generic_key_value_redaction_masks_unprefixed_secrets(
    raw: str,
    secret: str,
) -> None:
    redacted = redact_rollout_text(raw)

    assert secret not in redacted
    assert "[REDACTED:" in redacted


@pytest.mark.parametrize(
    "pem",
    [
        "-----BEGIN RSA PRIVATE KEY-----\ncnNhLWJvZHk=\n-----END RSA PRIVATE KEY-----",
        "-----BEGIN EC PRIVATE KEY-----\nZWMtYm9keQ==\n-----END EC PRIVATE KEY-----",
        "-----BEGIN OPENSSH PRIVATE KEY-----\nb3BlbnNzaC1ib2R5\n-----END OPENSSH PRIVATE KEY-----",
        "-----BEGIN PRIVATE KEY-----\ndW50ZXJtaW5hdGVkLWJvZHk=",
    ],
)
def test_redaction_covers_complete_and_unterminated_private_pem(pem: str) -> None:
    redacted = redact_rollout_text(f"before\n{pem}\nafter")

    assert "PRIVATE KEY" not in redacted
    assert "cnNhLWJvZHk" not in redacted
    assert "ZWMtYm9keQ" not in redacted
    assert "b3BlbnNzaC1ib2R5" not in redacted
    assert "dW50ZXJtaW5hdGVkLWJvZHk" not in redacted


@pytest.mark.parametrize(
    "url",
    [
        "postgresql://loom:pg-secret@db.example/loom",
        "redis://:redis-secret@cache.example/0",
        "amqp://loom:amqp-secret@mq.example/vhost",
        "https://user:http-secret@example.invalid/path",
    ],
)
def test_redaction_covers_credential_urls_across_supported_schemes(url: str) -> None:
    redacted = redact_rollout_text(url)

    assert url not in redacted
    assert "secret" not in redacted
    assert redacted == "[REDACTED:credential-url]"


def test_mapping_redaction_masks_generic_sensitive_keys() -> None:
    redacted = redact_rollout_mapping(
        {
            "authorization": "plain-authorization-value",
            "cookie": "plain-cookie-value",
            "nested": {"credentials": "plain-credentials-value"},
        }
    )

    assert redacted == {
        "authorization": "[REDACTED:authorization]",
        "cookie": "[REDACTED:cookie]",
        "nested": {"credentials": "[REDACTED:credentials]"},
    }


def test_nested_redaction_scope_keeps_outer_known_secret_active() -> None:
    outer = "opaque-outer-secret"
    inner = "opaque-inner-secret"

    with rollout_redaction_scope((outer,)):
        with rollout_redaction_scope((inner,)):
            redacted = redact_rollout_text(f"{outer} {inner}")

    assert outer not in redacted
    assert inner not in redacted


def test_known_secret_loader_rejects_writable_or_oversized_source(
    tmp_path: Path,
) -> None:
    writable = tmp_path / "writable-token"
    writable.write_text("opaque-writable-secret", encoding="utf-8")
    writable.chmod(0o660)
    oversized = tmp_path / "oversized-token"
    oversized.write_bytes(b"x" * (1024 * 1024 + 1))
    oversized.chmod(0o600)

    known = known_secrets_from_sources(
        (f"file:{writable}", f"file:{oversized}"),
    )

    assert "opaque-writable-secret" not in known
    assert "x" * 1024 not in known


def test_known_secret_loader_accepts_intentionally_group_readable_source(
    tmp_path: Path,
) -> None:
    secret = "opaque-group-readable-secret"
    source = tmp_path / "group-readable-token"
    source.write_text(secret, encoding="utf-8")
    source.chmod(0o640)

    known = known_secrets_from_sources((f"file:{source}",))

    assert secret in known


def test_known_secret_loader_rejects_source_changed_during_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    secret = "opaque-raced-secret"
    source = tmp_path / "raced-token"
    source.write_text(secret, encoding="utf-8")
    source.chmod(0o600)
    real_fstat = os.fstat
    calls = 0

    def changed_fstat(fd: int) -> os.stat_result | SimpleNamespace:
        nonlocal calls
        calls += 1
        metadata = real_fstat(fd)
        if calls == 1:
            return metadata
        return SimpleNamespace(
            st_mode=metadata.st_mode,
            st_size=metadata.st_size + 1,
            st_nlink=metadata.st_nlink,
            st_dev=metadata.st_dev,
            st_ino=metadata.st_ino,
            st_mtime_ns=metadata.st_mtime_ns + 1,
        )

    monkeypatch.setattr(
        "loom_cli.rollout.operator.redaction.os.fstat",
        changed_fstat,
    )

    known = known_secrets_from_sources((f"file:{source}",))

    assert secret not in known


def test_format_command_redacts_protected_source_reference() -> None:
    source = "file:/var/lib/loom-staging-rollout/private/admin-token"

    with rollout_redaction_scope((source,)):
        rendered = format_command(["loom", "--token-source", source])

    assert source not in rendered
    assert "[REDACTED:known-secret]" in rendered


def test_run_captured_sanitizes_oserror_before_raise_and_log(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    secret = "opaque-launch-secret"
    stderr = tmp_path / "stderr.log"

    def fail(*_args: object, **_kwargs: object) -> None:
        raise OSError(f"launch failed with {secret}")

    monkeypatch.setattr("subprocess.run", fail)

    with rollout_redaction_scope((secret,)), pytest.raises(Exception) as caught:
        run_captured(["example-command"], stderr_log=stderr)

    assert secret not in str(caught.value)
    assert stderr.is_file()
    assert secret not in stderr.read_text(encoding="utf-8")


def test_run_captured_sanitizes_timeout_streams_before_raise_and_write(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    secret = "opaque-timeout-secret"
    stdout = tmp_path / "stdout.log"
    stderr = tmp_path / "stderr.log"

    def timeout(*_args: object, **_kwargs: object) -> None:
        raise subprocess.TimeoutExpired(
            "example-command",
            3,
            output=f"stdout {secret}\n",
            stderr=f"stderr {secret}\n",
        )

    monkeypatch.setattr("subprocess.run", timeout)

    with rollout_redaction_scope((secret,)), pytest.raises(Exception) as caught:
        run_captured(
            ["example-command"],
            stdout_log=stdout,
            stderr_log=stderr,
            timeout_sec=3,
        )

    persisted = stdout.read_text(encoding="utf-8") + stderr.read_text(encoding="utf-8")
    assert secret not in persisted
    assert secret not in str(caught.value)


def test_json_escaped_multiline_secret_remains_redacted_after_limit() -> None:
    secret = "opaque-line-one\nopaque-line-two"
    rendered = json.dumps({"error": secret})

    redacted = redact_rollout_text(
        rendered,
        known_secrets=(secret,),
        limit=48,
    )

    assert "opaque-line" not in redacted
    assert len(redacted) <= 48
