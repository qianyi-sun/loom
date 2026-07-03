"""`loom cluster bootstrap-evidence-paths` unit tests (#174).

Emits a sudo-install script that creates operator-writable rollout evidence
directories under a protected /data root. Idempotent by construction
(`install -d`), refuses to touch service-locked-down siblings.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from loom_cli.__main__ import main
from loom_cli.cluster_bootstrap_evidence_paths import (
    RESERVED_SERVICE_DIRS,
    ServiceDirCollisionError,
    render_bootstrap_script,
)


class TestRenderBootstrapScript:
    """Tests on the pure render function (no filesystem side effects)."""

    def test_emits_install_command_for_each_default_path(self) -> None:
        script = render_bootstrap_script(
            rollout_root=Path("/data/loom-staging"),
            operator_user="qianyi",
            evidence_paths=["rollouts", "evidence", "logs"],
        )
        assert "install -d" in script
        for name in ("rollouts", "evidence", "logs"):
            expected = (
                f"sudo install -d -o qianyi -g qianyi -m 755 "
                f"/data/loom-staging/{name}"
            )
            assert expected in script, (
                f"missing install line for {name}; script was:\n{script}"
            )

    def test_emits_header_documenting_intent(self) -> None:
        script = render_bootstrap_script(
            rollout_root=Path("/data/loom-staging"),
            operator_user="qianyi",
            evidence_paths=["rollouts"],
        )
        assert "#!/bin/bash" in script.splitlines()[0]
        assert "operator: qianyi" in script
        assert "rollout_root: /data/loom-staging" in script
        assert "#174" in script  # cite the issue for future readers

    def test_emits_idempotence_note(self) -> None:
        script = render_bootstrap_script(
            rollout_root=Path("/data/loom-staging"),
            operator_user="qianyi",
            evidence_paths=["rollouts"],
        )
        assert "install -d" in script  # `install -d` is idempotent by design
        assert "idempotent" in script.lower()

    def test_refuses_service_dir_names(self) -> None:
        for name in sorted(RESERVED_SERVICE_DIRS):
            with pytest.raises(ServiceDirCollisionError) as exc:
                render_bootstrap_script(
                    rollout_root=Path("/data/loom-staging"),
                    operator_user="qianyi",
                    evidence_paths=["rollouts", name],
                )
            assert name in str(exc.value)

    def test_reserved_service_dirs_include_expected_set(self) -> None:
        """Guardrail: any change here needs to be intentional."""
        assert RESERVED_SERVICE_DIRS == frozenset({
            "backups",
            "migrations",
            "minio",
            "postgres",
            "trajectories",
        })

    def test_absolute_rollout_root_required(self) -> None:
        with pytest.raises(ValueError, match="absolute"):
            render_bootstrap_script(
                rollout_root=Path("loom-staging"),
                operator_user="qianyi",
                evidence_paths=["rollouts"],
            )

    def test_operator_user_must_not_be_empty(self) -> None:
        with pytest.raises(ValueError, match="operator_user"):
            render_bootstrap_script(
                rollout_root=Path("/data/loom-staging"),
                operator_user="",
                evidence_paths=["rollouts"],
            )

    def test_operator_user_must_be_valid_username(self) -> None:
        """POSIX username rules: [a-z_][a-z0-9_-]*  (approximate)."""
        with pytest.raises(ValueError, match="invalid"):
            render_bootstrap_script(
                rollout_root=Path("/data/loom-staging"),
                operator_user="qianyi;rm -rf /",  # injection attempt
                evidence_paths=["rollouts"],
            )

    def test_evidence_paths_must_not_be_empty(self) -> None:
        with pytest.raises(ValueError, match="evidence_paths"):
            render_bootstrap_script(
                rollout_root=Path("/data/loom-staging"),
                operator_user="qianyi",
                evidence_paths=[],
            )

    def test_evidence_paths_reject_slashes(self) -> None:
        """Only leaf names allowed; no traversal / nesting."""
        with pytest.raises(ValueError, match="leaf"):
            render_bootstrap_script(
                rollout_root=Path("/data/loom-staging"),
                operator_user="qianyi",
                evidence_paths=["rollouts", "../rogue"],
            )


class TestCLIDispatch:
    """End-to-end CLI test: `loom cluster bootstrap-evidence-paths ...`."""

    def test_prints_script_by_default(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        rc = main([
            "cluster", "bootstrap-evidence-paths",
            "--rollout-root", "/data/loom-staging",
            "--operator-user", "qianyi",
        ])
        assert rc == 0
        out = capsys.readouterr().out
        assert "install -d -o qianyi -g qianyi -m 755" in out
        # Default evidence paths cover the three the issue names.
        for name in ("rollouts", "evidence", "logs"):
            assert f"/data/loom-staging/{name}" in out

    def test_custom_evidence_paths(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        rc = main([
            "cluster", "bootstrap-evidence-paths",
            "--rollout-root", "/data/loom-staging",
            "--operator-user", "qianyi",
            "--evidence-paths", "rollouts,extra",
        ])
        assert rc == 0
        out = capsys.readouterr().out
        assert "/data/loom-staging/rollouts" in out
        assert "/data/loom-staging/extra" in out
        assert "/data/loom-staging/logs" not in out  # not in override

    def test_refuses_service_dir_collision_at_cli(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        rc = main([
            "cluster", "bootstrap-evidence-paths",
            "--rollout-root", "/data/loom-staging",
            "--operator-user", "qianyi",
            "--evidence-paths", "rollouts,postgres",
        ])
        assert rc == 2
        err = capsys.readouterr().err
        assert "postgres" in err
        assert "service" in err.lower() or "reserved" in err.lower()

    def test_refuses_relative_root_at_cli(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        rc = main([
            "cluster", "bootstrap-evidence-paths",
            "--rollout-root", "loom-staging",
            "--operator-user", "qianyi",
        ])
        assert rc == 2
        err = capsys.readouterr().err
        assert "absolute" in err.lower()

    def test_defaults_operator_user_to_env(
        self,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        monkeypatch.setenv("USER", "carbon")
        rc = main([
            "cluster", "bootstrap-evidence-paths",
            "--rollout-root", "/data/loom-staging",
        ])
        assert rc == 0
        out = capsys.readouterr().out
        assert "install -d -o carbon -g carbon -m 755" in out
