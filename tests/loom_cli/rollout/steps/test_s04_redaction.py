"""Thread-boundary redaction tests for GB10 prep evidence."""

from __future__ import annotations

from pathlib import Path

import pytest

from loom_cli.rollout.base_context_fixture import make_ctx
from loom_cli.rollout.context import RolloutContext
from loom_cli.rollout.evidence import EvidenceDirectory, StepDir
from loom_cli.rollout.steps.s04_gb10_prep import GB10Host, GB10PrepStep
from loom_cli.rollout.steps.subprocess_util import SubprocessResult


def _configured_step(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[GB10PrepStep, RolloutContext, StepDir]:
    token_file = tmp_path / "admin-token"
    token_file.write_text("opaque-thread-secret", encoding="utf-8")
    token_file.chmod(0o600)
    ctx = make_ctx(tmp_path, admin_token_source=f"file:{token_file}")
    host = GB10Host(
        ssh_target="trt-gb10-1",
        repo_path="/srv/loom/staging",
        env_file_path="/srv/loom/staging/.env",
    )
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s04_gb10_prep.gb10_hosts_for",
        lambda _ctx, **_kwargs: [host],
    )
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s04_gb10_prep._gb10_prep_config_paths",
        lambda _ctx, _step_dir: (_ctx.cluster_config_path, _ctx.cluster_config_path),
    )
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s04_gb10_prep._gb10_ssh_auth_preflight",
        lambda _hosts: None,
    )
    step = GB10PrepStep()
    step.max_retries = 1
    step.backoff_sec = 0
    evidence = EvidenceDirectory(tmp_path, "rid")
    evidence.ensure()
    return step, ctx, evidence.step_dir(12, "gb10-prep")


def _rendered_evidence(step_dir: StepDir) -> str:
    path = step_dir.path
    return "".join(item.read_text(encoding="utf-8") for item in path.rglob("*") if item.is_file())


def test_threaded_prep_log_redacts_explicit_known_values(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    step, ctx, step_dir = _configured_step(tmp_path, monkeypatch)
    payload = (
        "opaque-thread-secret\n"
        "redis://:thread-url-password@cache.example/0\n"
        "-----BEGIN OPENSSH PRIVATE KEY-----\n"
        "dGhyZWFkLXByaXZhdGUta2V5\n"
        "-----END OPENSSH PRIVATE KEY-----\n"
    )
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s04_gb10_prep._ssh",
        lambda _host, _cmd: SubprocessResult(
            argv=["ssh"],
            returncode=1,
            stdout=payload,
            stderr=payload,
        ),
    )

    result = step.run(ctx, step_dir)

    assert result.exit_code == 1
    rendered = _rendered_evidence(step_dir)
    for forbidden in (
        "opaque-thread-secret",
        "thread-url-password",
        "dGhyZWFkLXByaXZhdGUta2V5",
    ):
        assert forbidden not in rendered


def test_threaded_prep_exception_log_redacts_explicit_known_values(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    step, ctx, step_dir = _configured_step(tmp_path, monkeypatch)

    def raise_secret(_host: GB10Host, _cmd: str) -> SubprocessResult:
        raise RuntimeError("crashed with opaque-thread-secret")

    monkeypatch.setattr(
        "loom_cli.rollout.steps.s04_gb10_prep._ssh",
        raise_secret,
    )

    result = step.run(ctx, step_dir)

    assert result.exit_code == 1
    assert "opaque-thread-secret" not in _rendered_evidence(step_dir)
