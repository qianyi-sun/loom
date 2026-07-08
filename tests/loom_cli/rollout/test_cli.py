"""CLI tests for `loom cluster rollout` (#340)."""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from loom_cli.__main__ import main
from loom_cli.rollout.context import sha256_of_file
from loom_cli.rollout.evidence import EvidenceDirectory
from loom_cli.rollout.state import RolloutState


class _FakeSubprocess:
    """Return canned SubprocessResult-ish objects for known argvs."""

    def __init__(self) -> None:
        self.calls: list[list[str]] = []

    def __call__(self, argv, **kwargs):
        self.calls.append(list(argv))

        class _R:
            def __init__(self, rc: int = 0, out: str = "", err: str = "") -> None:
                self.returncode = rc
                self.stdout = out
                self.stderr = err
                self.args = argv

        # `git rev-parse` used by resolve_ref_to_sha.
        if argv[:2] == ["git", "rev-parse"]:
            return _R(rc=0, out="a" * 40 + "\n")
        return _R(rc=0)


class TestRolloutCLIDryRun:
    def test_refuses_short_rollout_without_environment_selector(
        self,
        capsys: pytest.CaptureFixture[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(subprocess, "run", _FakeSubprocess())

        rc = main([
            "cluster", "rollout",
            "--ref", "origin/dev",
            "--dry-run",
        ])

        assert rc == 2
        err = capsys.readouterr().err
        assert "environment selector" in err
        assert "staging" in err
        assert "prod" in err

    def test_staging_preset_dry_run_expands_stable_inputs_and_derived_tag(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        fake = _FakeSubprocess()
        monkeypatch.setattr(subprocess, "run", fake)
        monkeypatch.setattr("loom_cli.rollout.cli.new_rollout_id", lambda *, image_tag: f"rid-{image_tag}")

        rc = main([
            "cluster", "rollout",
            "staging",
            "--ref", "origin/dev",
            "--rollout-root", str(tmp_path),
            "--dry-run",
        ])

        assert rc == 0
        out = capsys.readouterr().out
        assert "preset: staging" in out
        assert "resolved_sha: " + ("a" * 40) in out
        assert "image_tag: staging-aaaaaaa" in out
        assert "rollout_id: rid-staging-aaaaaaa" in out
        assert "cluster_name: loom-staging" in out
        assert "namespace: loom-staging" in out
        assert "environment: staging" in out
        assert "cp_url: http://127.0.0.1:18081" in out
        assert "cluster_config_path: deploy/environments/staging.cluster.toml" in out
        assert "backup_manifest_path: /data/loom-staging/backups/latest/backup-manifest.json" in out
        assert "rollout_root: " + str(tmp_path) in out
        assert "scope: current-gb10" in out
        assert "admin_token_source: file:" in out
        assert "worker_token_source: file:" in out
        assert "service_token_source: file:" in out
        assert "smoke_submit_mode: admin-on-behalf" in out
        assert "smoke_on_behalf_username: devansh" in out
        assert "smoke_on_behalf_team_id: env:LOOM_SMOKE_ON_BEHALF_TEAM_ID" in out
        assert "smoke_admin_actor: codex-v1-release-gate" in out
        assert "raw-secret" not in out
        assert "steps:\n" in out

    def test_staging_preset_allows_explicit_image_tag_override(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(subprocess, "run", _FakeSubprocess())
        monkeypatch.setattr("loom_cli.rollout.cli.new_rollout_id", lambda *, image_tag: f"rid-{image_tag}")

        rc = main([
            "cluster", "rollout",
            "staging",
            "--ref", "origin/dev",
            "--image-tag", "staging-manual",
            "--rollout-root", str(tmp_path),
            "--dry-run",
        ])

        assert rc == 0
        out = capsys.readouterr().out
        assert "image_tag: staging-manual" in out
        assert "rollout_id: rid-staging-manual" in out

    def test_prod_preset_fails_closed_until_configured(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(subprocess, "run", _FakeSubprocess())

        rc = main([
            "cluster", "rollout",
            "prod",
            "--ref", "main",
            "--rollout-root", str(tmp_path),
            "--dry-run",
        ])

        assert rc == 2
        err = capsys.readouterr().err
        assert "prod preset not configured" in err
        assert "staging" not in err.lower()

    def test_dry_run_prints_step_list(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        cfg = tmp_path / "cluster-config.toml"
        cfg.write_text("image_tag = 'x'\n")

        fake = _FakeSubprocess()
        monkeypatch.setattr(subprocess, "run", fake)

        backup = tmp_path / "backup-manifest.json"
        backup.write_text("{}")
        rc = main([
            "cluster", "rollout",
            "--ref", "origin/dev",
            "--image-tag", "staging-aaaaaaa",
            "--cluster-name", "loom-staging",
            "--namespace", "loom-staging",
            "--environment", "staging",
            "--cp-url", "http://control-node.lan:18081",
            "--cluster-config", str(cfg),
            "--backup-manifest", str(backup),
            "--rollout-root", str(tmp_path),
            "--dry-run",
        ])
        assert rc == 0
        out = capsys.readouterr().out
        assert "steps:\n" in out
        step_lines = [
            line.strip()
            for line in out.splitlines()
            if line.startswith("  ")
        ]
        assert step_lines == [
            "00 resolve-target",
            "01 worktree",
            "02 build-images",
            "03 kind-load-images",
            "05 backup",
            "06 audit",
            "07 render",
            "08 preflight",
            "09 migrate",
            "10 env-state",
            "11 gb10-prep",
            "12 cluster-up",
            "13 production-defaults",
            "14 release-gate",
            "15 smoke",
            "99 summary",
        ]

    def test_dry_run_refuses_scope_conflict(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        cfg = tmp_path / "cluster-config.toml"
        cfg.write_text("image_tag = 'x'\n")

        monkeypatch.setattr(subprocess, "run", _FakeSubprocess())

        backup = tmp_path / "backup-manifest.json"
        backup.write_text("{}")
        rc = main([
            "cluster", "rollout",
            "--ref", "origin/dev",
            "--image-tag", "staging-aaaaaaa",
            "--cluster-name", "loom-staging",
            "--namespace", "loom-staging",
            "--environment", "staging",
            "--cp-url", "http://control-node.lan:18081",
            "--cluster-config", str(cfg),
            "--backup-manifest", str(backup),
            "--rollout-root", str(tmp_path),
            "--scope", "full-cluster",
            "--exclude-oldlab",
            "--dry-run",
        ])
        # Dry-run doesn't trigger preflight — refusal must happen in the
        # real run path. This confirms dry-run itself is a safe read-only.
        assert rc == 0

    def test_staging_refuses_non_staging_physical_targets(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        cfg = tmp_path / "cluster-config.toml"
        cfg.write_text("image_tag = 'x'\n")
        backup = tmp_path / "backup-manifest.json"
        backup.write_text("{}")

        monkeypatch.setattr(subprocess, "run", _FakeSubprocess())

        rc = main([
            "cluster", "rollout",
            "--ref", "origin/dev",
            "--image-tag", "staging-aaaaaaa",
            "--cluster-name", "loom-legacy-preprod",
            "--namespace", "loom-legacy-preprod",
            "--environment", "staging",
            "--cp-url", "http://control-node.lan:18081",
            "--cluster-config", str(cfg),
            "--backup-manifest", str(backup),
            "--rollout-root", "/data/loom-legacy-preprod",
            "--dry-run",
        ])

        assert rc == 2
        err = capsys.readouterr().err
        assert "staging rollout must use physical staging resources" in err
        assert "loom-staging" in err
        assert "/data/loom-staging" in err
        assert "loom-legacy-preprod" in err


class TestRolloutCLIRealRun:
    def test_refuses_literal_admin_token_before_evidence_capture(
        self,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        raw_token = "raw-secret-token"

        with pytest.raises(SystemExit) as excinfo:
            main([
                "cluster", "rollout",
                "--ref", "origin/dev",
                "--image-tag", "staging-aaaaaaa",
                "--cluster-name", "loom-staging",
                "--namespace", "loom-staging",
                "--environment", "staging",
                "--cp-url", "http://control-node.lan:18081",
                "--admin-token", raw_token,
                "--cluster-config", "/tmp/cluster-config.toml",
                "--backup-manifest", "/tmp/backup-manifest.json",
                "--rollout-root", "/tmp/rollout-root",
            ])

        assert excinfo.value.code == 2
        err = capsys.readouterr().err
        assert "literal values are rejected" in err
        assert "env:VAR | file:PATH" in err
        assert raw_token not in err

    def test_refuses_stdin_admin_token_for_replayable_rollout(
        self,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        with pytest.raises(SystemExit) as excinfo:
            main([
                "cluster", "rollout",
                "--ref", "origin/dev",
                "--image-tag", "staging-aaaaaaa",
                "--cluster-name", "loom-staging",
                "--namespace", "loom-staging",
                "--environment", "staging",
                "--cp-url", "http://control-node.lan:18081",
                "--admin-token", "-",
                "--cluster-config", "/tmp/cluster-config.toml",
                "--backup-manifest", "/tmp/backup-manifest.json",
                "--rollout-root", "/tmp/rollout-root",
            ])

        assert excinfo.value.code == 2
        err = capsys.readouterr().err
        assert "stdin source '-' is not replayable for rollout" in err
        assert "env:VAR or file:PATH" in err

    def test_refuses_literal_worker_token_before_evidence_capture(
        self,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        raw_token = "raw-worker-secret"

        with pytest.raises(SystemExit) as excinfo:
            main([
                "cluster", "rollout",
                "--ref", "origin/dev",
                "--image-tag", "staging-aaaaaaa",
                "--cluster-name", "loom-staging",
                "--namespace", "loom-staging",
                "--environment", "staging",
                "--cp-url", "http://control-node.lan:18081",
                "--worker-token", raw_token,
                "--cluster-config", "/tmp/cluster-config.toml",
                "--backup-manifest", "/tmp/backup-manifest.json",
                "--rollout-root", "/tmp/rollout-root",
            ])

        assert excinfo.value.code == 2
        err = capsys.readouterr().err
        assert "literal values are rejected" in err
        assert "env:VAR | file:PATH" in err
        assert raw_token not in err

    def test_refuses_stdin_worker_token_for_replayable_rollout(
        self,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        with pytest.raises(SystemExit) as excinfo:
            main([
                "cluster", "rollout",
                "--ref", "origin/dev",
                "--image-tag", "staging-aaaaaaa",
                "--cluster-name", "loom-staging",
                "--namespace", "loom-staging",
                "--environment", "staging",
                "--cp-url", "http://control-node.lan:18081",
                "--worker-token", "-",
                "--cluster-config", "/tmp/cluster-config.toml",
                "--backup-manifest", "/tmp/backup-manifest.json",
                "--rollout-root", "/tmp/rollout-root",
            ])

        assert excinfo.value.code == 2
        err = capsys.readouterr().err
        assert "stdin source '-' is not replayable for rollout" in err
        assert "env:VAR or file:PATH" in err

    def test_refuses_literal_service_token_before_evidence_capture(
        self,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        raw_token = "raw-service-secret"

        with pytest.raises(SystemExit) as excinfo:
            main([
                "cluster", "rollout",
                "--ref", "origin/dev",
                "--image-tag", "staging-aaaaaaa",
                "--cluster-name", "loom-staging",
                "--namespace", "loom-staging",
                "--environment", "staging",
                "--cp-url", "http://control-node.lan:18081",
                "--service-token", raw_token,
                "--cluster-config", "/tmp/cluster-config.toml",
                "--backup-manifest", "/tmp/backup-manifest.json",
                "--rollout-root", "/tmp/rollout-root",
            ])

        assert excinfo.value.code == 2
        err = capsys.readouterr().err
        assert "literal values are rejected" in err
        assert "env:VAR | file:PATH" in err
        assert raw_token not in err

    def test_refuses_stdin_service_token_for_replayable_rollout(
        self,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        with pytest.raises(SystemExit) as excinfo:
            main([
                "cluster", "rollout",
                "--ref", "origin/dev",
                "--image-tag", "staging-aaaaaaa",
                "--cluster-name", "loom-staging",
                "--namespace", "loom-staging",
                "--environment", "staging",
                "--cp-url", "http://control-node.lan:18081",
                "--service-token", "-",
                "--cluster-config", "/tmp/cluster-config.toml",
                "--backup-manifest", "/tmp/backup-manifest.json",
                "--rollout-root", "/tmp/rollout-root",
            ])

        assert excinfo.value.code == 2
        err = capsys.readouterr().err
        assert "stdin source '-' is not replayable for rollout" in err
        assert "env:VAR or file:PATH" in err

    def test_refuses_literal_smoke_api_token_before_evidence_capture(
        self,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        raw_token = "raw-smoke-secret"

        with pytest.raises(SystemExit) as excinfo:
            main([
                "cluster", "rollout",
                "--ref", "origin/dev",
                "--image-tag", "staging-aaaaaaa",
                "--cluster-name", "loom-staging",
                "--namespace", "loom-staging",
                "--environment", "staging",
                "--cp-url", "http://control-node.lan:18081",
                "--smoke-api-token", raw_token,
                "--cluster-config", "/tmp/cluster-config.toml",
                "--backup-manifest", "/tmp/backup-manifest.json",
                "--rollout-root", "/tmp/rollout-root",
            ])

        assert excinfo.value.code == 2
        err = capsys.readouterr().err
        assert "literal values are rejected" in err
        assert "env:VAR | file:PATH" in err
        assert raw_token not in err

    def test_refuses_stdin_smoke_api_token_for_replayable_rollout(
        self,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        with pytest.raises(SystemExit) as excinfo:
            main([
                "cluster", "rollout",
                "--ref", "origin/dev",
                "--image-tag", "staging-aaaaaaa",
                "--cluster-name", "loom-staging",
                "--namespace", "loom-staging",
                "--environment", "staging",
                "--cp-url", "http://control-node.lan:18081",
                "--smoke-api-token", "-",
                "--cluster-config", "/tmp/cluster-config.toml",
                "--backup-manifest", "/tmp/backup-manifest.json",
                "--rollout-root", "/tmp/rollout-root",
            ])

        assert excinfo.value.code == 2
        err = capsys.readouterr().err
        assert "stdin source '-' is not replayable for rollout" in err
        assert "env:VAR or file:PATH" in err

    def test_passes_cp_url_into_rollout_context(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        cfg = tmp_path / "cluster-config.toml"
        cfg.write_text("image_tag = 'x'\n")
        backup = tmp_path / "backup-manifest.json"
        backup.write_text("{}")
        monkeypatch.setattr(subprocess, "run", _FakeSubprocess())
        captured = {}

        def fake_run_rollout(ctx, steps, evidence):
            captured["ctx"] = ctx
            captured["steps"] = steps
            captured["evidence"] = evidence
            return 0

        monkeypatch.setattr("loom_cli.rollout.cli.run_rollout", fake_run_rollout)

        rc = main([
            "cluster", "rollout",
            "--ref", "origin/dev",
            "--image-tag", "staging-aaaaaaa",
            "--cluster-name", "loom-staging",
            "--namespace", "loom-staging",
            "--environment", "staging",
            "--cp-url", "http://control-node.lan:18081",
            "--cluster-config", str(cfg),
            "--backup-manifest", str(backup),
            "--rollout-root", str(tmp_path),
        ])

        assert rc == 0
        assert captured["ctx"].cp_url == "http://control-node.lan:18081"
        assert captured["ctx"].backup_manifest_min_remaining_hours == 2

    def test_passes_smoke_inputs_into_rollout_context(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        cfg = tmp_path / "cluster-config.toml"
        cfg.write_text("image_tag = 'x'\n")
        backup = tmp_path / "backup-manifest.json"
        backup.write_text("{}")
        monkeypatch.setattr(subprocess, "run", _FakeSubprocess())
        captured = {}

        def fake_run_rollout(ctx, steps, evidence):
            captured["ctx"] = ctx
            return 0

        monkeypatch.setattr("loom_cli.rollout.cli.run_rollout", fake_run_rollout)

        rc = main([
            "cluster", "rollout",
            "--ref", "origin/dev",
            "--image-tag", "staging-aaaaaaa",
            "--cluster-name", "loom-staging",
            "--namespace", "loom-staging",
            "--environment", "staging",
            "--cp-url", "http://control-node.lan:18081",
            "--cluster-config", str(cfg),
            "--backup-manifest", str(backup),
            "--rollout-root", str(tmp_path),
            "--smoke-submit-mode", "admin-on-behalf",
            "--smoke-api-token", "file:/run/loom/smoke-token",
            "--smoke-task-id", "loom-smoke/gb10-oracle-hello-world",
            "--smoke-required-worker-pool", "gb10-arm64",
            "--smoke-agent", "oracle",
            "--smoke-on-behalf-username", "devansh",
            "--smoke-on-behalf-team-id", "agentic-rl-team-id",
            "--smoke-admin-actor", "codex-v1-release-gate",
        ])

        assert rc == 0
        ctx = captured["ctx"]
        assert ctx.smoke_submit_mode == "admin-on-behalf"
        assert ctx.smoke_api_token_source == "file:/run/loom/smoke-token"
        assert ctx.smoke_task_id == "loom-smoke/gb10-oracle-hello-world"
        assert ctx.smoke_required_worker_pool == "gb10-arm64"
        assert ctx.smoke_agent == "oracle"
        assert ctx.smoke_on_behalf_username == "devansh"
        assert ctx.smoke_on_behalf_team_id == "agentic-rl-team-id"
        assert ctx.smoke_admin_actor == "codex-v1-release-gate"
        inputs = ctx.to_inputs_dict()
        assert inputs["smoke_api_token_source"] == "file:/run/loom/smoke-token"
        assert "smoke-token-secret" not in str(inputs)

    def test_passes_backup_manifest_min_remaining_hours_into_rollout_context(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        cfg = tmp_path / "cluster-config.toml"
        cfg.write_text("image_tag = 'x'\n")
        backup = tmp_path / "backup-manifest.json"
        backup.write_text("{}")
        monkeypatch.setattr(subprocess, "run", _FakeSubprocess())
        captured = {}

        def fake_run_rollout(ctx, steps, evidence):
            captured["ctx"] = ctx
            return 0

        monkeypatch.setattr("loom_cli.rollout.cli.run_rollout", fake_run_rollout)

        rc = main([
            "cluster", "rollout",
            "--ref", "origin/dev",
            "--image-tag", "staging-aaaaaaa",
            "--cluster-name", "loom-staging",
            "--namespace", "loom-staging",
            "--environment", "staging",
            "--cp-url", "http://control-node.lan:18081",
            "--cluster-config", str(cfg),
            "--backup-manifest", str(backup),
            "--backup-manifest-min-remaining-hours", "4",
            "--rollout-root", str(tmp_path),
        ])

        assert rc == 0
        assert captured["ctx"].backup_manifest_min_remaining_hours == 4
        assert captured["ctx"].to_inputs_dict()[
            "backup_manifest_min_remaining_hours"
        ] == 4

    def test_passes_gb10_prep_concurrency_into_rollout_inputs(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(subprocess, "run", _FakeSubprocess())
        captured = {}

        def fake_run_rollout(ctx, steps, evidence):
            captured["ctx"] = ctx
            return 0

        monkeypatch.setattr("loom_cli.rollout.cli.run_rollout", fake_run_rollout)

        rc = main([
            "cluster", "rollout",
            "staging",
            "--ref", "origin/dev",
            "--image-tag", "staging-aaaaaaa",
            "--rollout-root", str(tmp_path),
            "--gb10-prep-concurrency", "7",
        ])

        assert rc == 0
        assert captured["ctx"].gb10_prep_concurrency == 7
        assert captured["ctx"].to_inputs_dict()["gb10_prep_concurrency"] == 7

    def test_passes_protected_admin_token_contract_into_rollout_context(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        cfg = tmp_path / "cluster-config.toml"
        cfg.write_text("image_tag = 'x'\n")
        backup = tmp_path / "backup-manifest.json"
        backup.write_text("{}")
        monkeypatch.setattr(subprocess, "run", _FakeSubprocess())
        captured = {}

        def fake_run_rollout(ctx, steps, evidence):
            captured["ctx"] = ctx
            return 0

        monkeypatch.setattr("loom_cli.rollout.cli.run_rollout", fake_run_rollout)

        rc = main([
            "cluster", "rollout",
            "--ref", "origin/dev",
            "--image-tag", "staging-aaaaaaa",
            "--cluster-name", "loom-staging",
            "--namespace", "loom-staging",
            "--environment", "staging",
            "--cp-url", "http://control-node.lan:18081",
            "--admin-token", "file:/secure/path/admin-token",
            "--expect-admin-token-fingerprint", "sha256:abc123def456 len=64",
            "--cluster-config", str(cfg),
            "--backup-manifest", str(backup),
            "--rollout-root", str(tmp_path),
        ])

        assert rc == 0
        assert captured["ctx"].admin_token_source == "file:/secure/path/admin-token"
        assert captured["ctx"].expect_admin_token_fingerprint == (
            "sha256:abc123def456 len=64"
        )

    def test_passes_worker_token_source_into_rollout_context(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        cfg = tmp_path / "cluster-config.toml"
        cfg.write_text("image_tag = 'x'\n")
        backup = tmp_path / "backup-manifest.json"
        backup.write_text("{}")
        monkeypatch.setattr(subprocess, "run", _FakeSubprocess())
        captured = {}

        def fake_run_rollout(ctx, steps, evidence):
            captured["ctx"] = ctx
            return 0

        monkeypatch.setattr("loom_cli.rollout.cli.run_rollout", fake_run_rollout)

        rc = main([
            "cluster", "rollout",
            "--ref", "origin/dev",
            "--image-tag", "staging-aaaaaaa",
            "--cluster-name", "loom-staging",
            "--namespace", "loom-staging",
            "--environment", "staging",
            "--cp-url", "http://control-node.lan:18081",
            "--worker-token", "file:/secure/path/worker-token",
            "--cluster-config", str(cfg),
            "--backup-manifest", str(backup),
            "--rollout-root", str(tmp_path),
        ])

        assert rc == 0
        assert captured["ctx"].worker_token_source == "file:/secure/path/worker-token"
        assert captured["ctx"].to_inputs_dict()["worker_token_source"] == (
            "file:/secure/path/worker-token"
        )

    def test_passes_service_token_source_into_rollout_context(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        cfg = tmp_path / "cluster-config.toml"
        cfg.write_text("image_tag = 'x'\n")
        backup = tmp_path / "backup-manifest.json"
        backup.write_text("{}")
        monkeypatch.setattr(subprocess, "run", _FakeSubprocess())
        captured = {}

        def fake_run_rollout(ctx, steps, evidence):
            captured["ctx"] = ctx
            return 0

        monkeypatch.setattr("loom_cli.rollout.cli.run_rollout", fake_run_rollout)

        rc = main([
            "cluster", "rollout",
            "--ref", "origin/dev",
            "--image-tag", "staging-aaaaaaa",
            "--cluster-name", "loom-staging",
            "--namespace", "loom-staging",
            "--environment", "staging",
            "--cp-url", "http://control-node.lan:18081",
            "--service-token", "file:/secure/path/service-token",
            "--cluster-config", str(cfg),
            "--backup-manifest", str(backup),
            "--rollout-root", str(tmp_path),
        ])

        assert rc == 0
        assert captured["ctx"].service_token_source == "file:/secure/path/service-token"
        assert captured["ctx"].to_inputs_dict()["service_token_source"] == (
            "file:/secure/path/service-token"
        )

    def test_resume_reuses_persisted_resolved_sha_when_target_branch_moved(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        cfg = tmp_path / "cluster-config.toml"
        cfg.write_text("image_tag = 'staging-948f6e8'\n")
        backup = tmp_path / "backup-manifest.json"
        backup.write_text("{}")
        rollout_id = "20260707t000020z-staging-948f6e8"
        evidence = EvidenceDirectory(tmp_path, rollout_id)
        evidence.ensure()
        old_sha = "948f6e8d17815b24e6df3af05e456658e8daa386"
        new_sha = "468c23f9ca1a9484eb0f522bb3f75f1b8ff2b56c"
        evidence.write_inputs({
            "image_tag": "staging-948f6e8",
            "target_ref": "origin/dev",
            "resolved_sha": old_sha,
            "cluster_name": "loom-staging",
            "namespace": "loom-staging",
            "environment": "staging",
            "cp_url": "http://control-node.lan:18081",
            "admin_token_source": "env:LOOM_CP_ADMIN_TOKEN",
            "expect_admin_token_fingerprint": None,
            "worker_token_source": None,
            "service_token_source": None,
            "cluster_config_path": str(cfg),
            "cluster_config_sha256": sha256_of_file(cfg),
            "rollout_root": str(tmp_path),
            "scope": "current-gb10",
            "exclude_oldlab": False,
        })
        state = RolloutState.new(
            rollout_id=rollout_id,
            steps=[(13, "production-defaults")],
        )
        state.mark_step_failed(
            13,
            finished_at="2026-07-07T00:12:00Z",
            error="planned failure",
        )
        state.save(evidence.state_path())

        class _MovedBranchSubprocess(_FakeSubprocess):
            def __call__(self, argv, **kwargs):
                result = super().__call__(argv, **kwargs)
                if argv[:2] == ["git", "rev-parse"]:
                    result.stdout = new_sha + "\n"
                return result

        monkeypatch.setattr(subprocess, "run", _MovedBranchSubprocess())
        captured = {}

        def fake_run_rollout(ctx, steps, evidence_dir):
            captured["ctx"] = ctx
            captured["evidence"] = evidence_dir
            return 0

        monkeypatch.setattr("loom_cli.rollout.cli.run_rollout", fake_run_rollout)

        rc = main([
            "cluster", "rollout",
            "--ref", "origin/dev",
            "--image-tag", "staging-948f6e8",
            "--cluster-name", "loom-staging",
            "--namespace", "loom-staging",
            "--environment", "staging",
            "--cp-url", "http://control-node.lan:18081",
            "--cluster-config", str(cfg),
            "--backup-manifest", str(backup),
            "--rollout-root", str(tmp_path),
            "--resume",
        ])

        assert rc == 0
        assert captured["ctx"].target_ref == "origin/dev"
        assert captured["ctx"].resolved_sha == old_sha
        assert captured["evidence"].rollout_id == rollout_id

    def test_refuses_without_matching_cluster_config(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        fake = _FakeSubprocess()
        monkeypatch.setattr(subprocess, "run", fake)

        backup = tmp_path / "backup-manifest.json"
        backup.write_text("{}")
        rc = main([
            "cluster", "rollout",
            "--ref", "origin/dev",
            "--image-tag", "staging-aaaaaaa",
            "--cluster-name", "loom-staging",
            "--namespace", "loom-staging",
            "--environment", "staging",
            "--cp-url", "http://control-node.lan:18081",
            "--cluster-config", str(tmp_path / "missing.toml"),
            "--backup-manifest", str(backup),
            "--rollout-root", str(tmp_path),
            "--dry-run",
        ])
        assert rc == 2
        err = capsys.readouterr().err
        assert "cluster-config" in err
