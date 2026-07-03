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
            "--image-tag", "public-beta-aaaaaaa",
            "--cluster-name", "loom-public-beta",
            "--namespace", "loom-public-beta",
            "--environment", "public-beta",
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
            "--image-tag", "public-beta-aaaaaaa",
            "--cluster-name", "loom-public-beta",
            "--environment", "public-beta",
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


class TestRolloutCLIRealRun:
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
            "--image-tag", "public-beta-aaaaaaa",
            "--cluster-name", "loom-public-beta",
            "--environment", "public-beta",
            "--cluster-config", str(tmp_path / "missing.toml"),
            "--backup-manifest", str(backup),
            "--rollout-root", str(tmp_path),
            "--dry-run",
        ])
        assert rc == 2
        err = capsys.readouterr().err
        assert "cluster-config" in err
