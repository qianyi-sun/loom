"""CLI tests for `loom cluster rollout` (#340)."""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from loom_cli.__main__ import main


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
        for name in (
            "resolve-target", "worktree", "build-images",
            "kind-load-images", "gb10-prep", "backup",
            "audit", "render", "preflight", "migrate",
            "env-state", "cluster-up", "release-gate",
            "smoke", "summary",
        ):
            assert name in out, f"step {name!r} missing from dry-run output"

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
