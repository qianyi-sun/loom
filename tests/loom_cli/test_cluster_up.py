"""`loom cluster up` — orchestration tests (#76 Phase 3).

The unit under test composes `collect_preflight`, `render_manifests`,
`apply_manifests` (kubectl subprocess), and `wait_for_ready` (status
polling loop). We mock the subprocess + the k8s clients to drive
each branch of the orchestration without needing a real cluster.
"""

from __future__ import annotations

import subprocess
from typing import Any

import pytest

from loom_cli.__main__ import main
from loom_cli.cluster_cmd import (
    ApplyResult,
    ClusterStatus,
    ComponentStatus,
    apply_manifests,
    wait_for_ready,
)

# ──────────────────────────────────────────────────────────────────────
# apply_manifests — subprocess wrapper
# ──────────────────────────────────────────────────────────────────────


def test_apply_manifests_invokes_kubectl_with_correct_args(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The kubectl invocation MUST include `apply -f - -n NS` plus
    `--context X` only when context is non-None. Pipes the YAML
    text via stdin."""
    captured: dict[str, Any] = {}

    def _fake_run(cmd, **kwargs):  # type: ignore[no-untyped-def]
        captured["cmd"] = list(cmd)
        captured["input"] = kwargs.get("input")
        # Simulate kubectl reporting one configured object.
        return subprocess.CompletedProcess(
            args=cmd, returncode=0,
            stdout="deployment.apps/loom-service configured\n",
            stderr="",
        )

    monkeypatch.setattr(subprocess, "run", _fake_run)
    monkeypatch.setattr(
        "shutil.which", lambda _bin: "/usr/local/bin/kubectl",
    )

    result = apply_manifests(
        "apiVersion: v1\nkind: ConfigMap\n", "loom", context="prod",
    )
    assert result.returncode == 0
    assert result.summary_lines == [
        "deployment.apps/loom-service configured",
    ]
    assert captured["cmd"][:5] == [
        "kubectl", "apply", "-n", "loom", "-f",
    ]
    assert "--context" in captured["cmd"]
    assert "prod" in captured["cmd"]
    assert "ConfigMap" in captured["input"]


def test_apply_manifests_omits_context_when_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, list[str]] = {}

    def _fake_run(cmd, **kwargs):  # type: ignore[no-untyped-def]
        captured["cmd"] = list(cmd)
        return subprocess.CompletedProcess(
            args=cmd, returncode=0, stdout="", stderr="",
        )

    monkeypatch.setattr(subprocess, "run", _fake_run)
    monkeypatch.setattr("shutil.which", lambda _bin: "/x/kubectl")

    apply_manifests("---", "loom", context=None)
    assert "--context" not in captured["cmd"]


def test_apply_manifests_propagates_nonzero_returncode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _fake_run(cmd, **kwargs):  # type: ignore[no-untyped-def]
        return subprocess.CompletedProcess(
            args=cmd, returncode=1, stdout="",
            stderr='error: secret "loom-secrets" not found\n',
        )

    monkeypatch.setattr(subprocess, "run", _fake_run)
    monkeypatch.setattr("shutil.which", lambda _bin: "/x/kubectl")

    result = apply_manifests("---", "loom", context=None)
    assert result.returncode == 1
    assert "loom-secrets" in result.stderr


def test_apply_manifests_raises_when_kubectl_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If kubectl isn't on PATH, surface a friendly install hint
    instead of a cryptic FileNotFoundError from subprocess."""
    monkeypatch.setattr("shutil.which", lambda _bin: None)
    with pytest.raises(RuntimeError, match="kubectl is required"):
        apply_manifests("---", "loom", context=None)


# ──────────────────────────────────────────────────────────────────────
# wait_for_ready — polling loop
# ──────────────────────────────────────────────────────────────────────


class _FakeApi:
    """Stand-in for the apps/networking/core clients. The polling
    loop only calls `collect_status`, which only touches the clients
    via the workload methods. We patch `collect_status` directly so
    the fake API just needs to exist."""


def test_wait_for_ready_returns_on_first_pass_when_already_ready(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Happy path: collect_status reports all_ready=True on the first
    iteration; no sleeps."""
    sleeps: list[float] = []
    nows: list[float] = [0.0]  # never advances

    def _collect(apps, net, core, ns, *, context):  # type: ignore[no-untyped-def]
        return ClusterStatus(
            namespace=ns, context=context,
            components=[ComponentStatus(
                name="loom-service", kind="Deployment",
                ready=2, desired=2, available=True,
            )],
            ingresses=[], warnings=[],
        )

    monkeypatch.setattr("loom_cli.cluster_cmd.collect_status", _collect)

    status = wait_for_ready(
        _FakeApi(), _FakeApi(), _FakeApi(),
        namespace="loom", context=None,
        timeout_sec=60,
        _sleep=lambda s: sleeps.append(s),
        _now=lambda: nows[0],
    )
    assert status.all_ready
    assert sleeps == []  # didn't sleep


def test_wait_for_ready_polls_until_ready(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """First two polls return not-ready, third returns ready. Verify
    we sleep between polls and stop on first ready result."""
    sleeps: list[float] = []
    call_count = {"n": 0}

    def _collect(apps, net, core, ns, *, context):  # type: ignore[no-untyped-def]
        call_count["n"] += 1
        ready_replicas = 2 if call_count["n"] >= 3 else 0
        return ClusterStatus(
            namespace=ns, context=context,
            components=[ComponentStatus(
                name="loom-service", kind="Deployment",
                ready=ready_replicas, desired=2,
                available=ready_replicas > 0,
            )],
            ingresses=[], warnings=[],
        )

    monkeypatch.setattr("loom_cli.cluster_cmd.collect_status", _collect)

    nows = iter([0.0, 1.0, 2.0, 3.0, 4.0, 5.0])
    status = wait_for_ready(
        _FakeApi(), _FakeApi(), _FakeApi(),
        namespace="loom", context=None,
        timeout_sec=60, poll_interval_sec=1.0,
        _sleep=lambda s: sleeps.append(s),
        _now=lambda: next(nows),
    )
    assert status.all_ready
    assert call_count["n"] == 3
    # Two sleeps between three polls.
    assert sleeps == [1.0, 1.0]


def test_wait_for_ready_returns_unready_status_on_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Components never become ready; the wait returns the last
    observed status at deadline rather than raising."""
    sleeps: list[float] = []

    def _collect(apps, net, core, ns, *, context):  # type: ignore[no-untyped-def]
        return ClusterStatus(
            namespace=ns, context=context,
            components=[ComponentStatus(
                name="loom-service", kind="Deployment",
                ready=0, desired=2, available=False,
            )],
            ingresses=[], warnings=[],
        )

    monkeypatch.setattr("loom_cli.cluster_cmd.collect_status", _collect)

    # Clock advances past the 5s deadline on the second `_now` call.
    nows = iter([0.0, 0.0, 6.0, 6.0, 6.0])
    status = wait_for_ready(
        _FakeApi(), _FakeApi(), _FakeApi(),
        namespace="loom", context=None,
        timeout_sec=5, poll_interval_sec=1.0,
        _sleep=lambda s: sleeps.append(s),
        _now=lambda: next(nows),
    )
    assert not status.all_ready


# ──────────────────────────────────────────────────────────────────────
# CLI dispatch — orchestration
# ──────────────────────────────────────────────────────────────────────


def _all_ready_status(ns: str = "loom") -> ClusterStatus:
    """Helper: a fully-ready snapshot with one Deployment."""
    return ClusterStatus(
        namespace=ns, context=None,
        components=[ComponentStatus(
            name="loom-service", kind="Deployment",
            ready=2, desired=2, available=True,
        )],
        ingresses=[], warnings=[],
    )


def _patch_full_up_path(
    monkeypatch: pytest.MonkeyPatch, *,
    preflight_any_fail: bool = False,
    target_doctor_fail: bool = False,
    apply_returncode: int = 0,
    final_ready: bool = True,
) -> dict[str, Any]:
    """Stub every external dependency: k8s clients (4-tuple),
    collect_preflight, apply_manifests, collect_status. Returns a
    `captures` dict tests can inspect for what got called with what."""
    captures: dict[str, Any] = {}

    # k8s clients — opaque sentinels; preflight/status are patched.
    monkeypatch.setattr(
        "loom_cli.cluster_cmd._load_clients",
        lambda _ctx: (object(), object(), object(), object()),
    )

    from loom_cli.cluster_cmd import PreflightCheck, PreflightReport
    preflight_report = PreflightReport(
        namespace="loom", context=None,
        checks=[PreflightCheck(
            name="namespace-exists",
            outcome="fail" if preflight_any_fail else "pass",
            detail="ok",
            remediation="kubectl create namespace loom" if preflight_any_fail else None,
        )],
    )

    def _collect_preflight(*args, **kwargs):  # type: ignore[no-untyped-def]
        captures["preflight_called"] = True
        captures["preflight_kwargs"] = kwargs
        return preflight_report

    monkeypatch.setattr(
        "loom_cli.cluster_cmd.collect_preflight", _collect_preflight,
    )

    def _append_target_schema_doctor_check(report, **kwargs):  # type: ignore[no-untyped-def]
        captures["target_doctor_called"] = True
        from loom_cli.cluster_cmd import PreflightCheck

        report.checks.append(PreflightCheck(
            name="schema-doctor",
            outcome="fail" if target_doctor_fail else "pass",
            detail=(
                "1 schema violation(s)"
                if target_doctor_fail else "schema reconciliation clean"
            ),
            remediation=(
                "  - missing_env: LOOM_CP_EXAMPLE: Rendered Deployment missing env"
                if target_doctor_fail else None
            ),
        ))

    monkeypatch.setattr(
        "loom_cli.cluster_cmd._append_target_schema_doctor_check",
        _append_target_schema_doctor_check,
    )

    def _apply(yaml_text, ns, *, context, extra_args=()):  # type: ignore[no-untyped-def]
        captures["apply_yaml_len"] = len(yaml_text)
        captures["apply_yaml_text"] = yaml_text
        captures["apply_ns"] = ns
        return ApplyResult(
            returncode=apply_returncode,
            summary_lines=(
                ["deployment.apps/loom-service configured"]
                if apply_returncode == 0 else []
            ),
            stderr="error: x" if apply_returncode != 0 else "",
        )

    monkeypatch.setattr("loom_cli.cluster_cmd.apply_manifests", _apply)

    final_status = (
        _all_ready_status() if final_ready
        else ClusterStatus(
            namespace="loom", context=None,
            components=[ComponentStatus(
                name="loom-service", kind="Deployment",
                ready=0, desired=2, available=False,
            )],
            ingresses=[], warnings=[],
        )
    )

    def _wait(*args, **kwargs):  # type: ignore[no-untyped-def]
        captures["waited"] = True
        return final_status

    monkeypatch.setattr("loom_cli.cluster_cmd.wait_for_ready", _wait)
    return captures


def test_cli_up_happy_path(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Preflight pass → render → apply → wait → all_ready → exit 0."""
    captures = _patch_full_up_path(monkeypatch)
    rc = main(["cluster", "up"])
    assert rc == 0
    assert captures.get("preflight_called") is True
    assert captures.get("target_doctor_called") is True
    assert captures.get("waited") is True
    out = capsys.readouterr().out
    assert "Preflight" in out
    assert "loom-service configured" in out


def test_cli_up_preflight_fail_blocks_apply(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Preflight any_fail=True → refuse to apply; exit 1."""
    captures = _patch_full_up_path(
        monkeypatch, preflight_any_fail=True,
    )
    rc = main(["cluster", "up"])
    assert rc == 1
    err = capsys.readouterr().err
    assert "preflight checks failed" in err
    # Apply MUST NOT have been called.
    assert "apply_ns" not in captures


def test_cli_up_target_schema_doctor_fail_blocks_apply(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    captures = _patch_full_up_path(monkeypatch, target_doctor_fail=True)

    rc = main(["cluster", "up"])

    assert rc == 1
    err = capsys.readouterr().err
    assert "preflight checks failed" in err
    assert "schema-doctor" in err
    assert captures.get("target_doctor_called") is True
    assert "apply_ns" not in captures


def test_cli_up_skip_preflight_bypasses_check(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """--skip-preflight is the escape hatch for known-transient
    preflight failures (e.g. operator just created a Secret that
    hasn't propagated)."""
    captures = _patch_full_up_path(
        monkeypatch, preflight_any_fail=True,  # would fail
    )
    rc = main(["cluster", "up", "--skip-preflight"])
    assert rc == 0
    assert captures.get("preflight_called") is None
    assert captures.get("apply_ns") == "loom"


def test_cli_up_apply_failure_returns_1(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """kubectl apply non-zero → exit 1 with stderr surfaced."""
    _patch_full_up_path(monkeypatch, apply_returncode=1)
    rc = main(["cluster", "up"])
    assert rc == 1
    err = capsys.readouterr().err
    assert "kubectl apply failed" in err


def test_cli_up_no_wait_skips_readiness_check(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    captures = _patch_full_up_path(monkeypatch)
    rc = main(["cluster", "up", "--no-wait"])
    assert rc == 0
    assert "Skipping readiness wait" in capsys.readouterr().out
    assert captures.get("waited") is None


def test_cli_up_timeout_returns_1_when_not_ready(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _patch_full_up_path(monkeypatch, final_ready=False)
    rc = main(["cluster", "up", "--timeout", "30"])
    assert rc == 1
    err = capsys.readouterr().err
    assert "did not reach ready state" in err


def test_cli_up_cluster_unreachable_returns_2(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    def _raise(_: str | None) -> object:
        raise OSError("kubeconfig not found")

    monkeypatch.setattr("loom_cli.cluster_cmd._load_clients", _raise)
    rc = main(["cluster", "up"])
    assert rc == 2
    assert "cannot connect to cluster" in capsys.readouterr().err


def test_cli_up_kubectl_missing_returns_2(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _patch_full_up_path(monkeypatch)

    def _apply_raise(*args, **kwargs):  # type: ignore[no-untyped-def]
        raise RuntimeError(
            "kubectl is required for `loom cluster up`. install from ...",
        )

    monkeypatch.setattr(
        "loom_cli.cluster_cmd.apply_manifests", _apply_raise,
    )
    rc = main(["cluster", "up"])
    assert rc == 2
    assert "kubectl is required" in capsys.readouterr().err


def test_cli_up_namespace_flag_threads_through(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captures = _patch_full_up_path(monkeypatch)
    main(["cluster", "up", "--namespace", "loom-stage"])
    assert captures["apply_ns"] == "loom-stage"


def test_cli_up_backup_guard_flags_thread_to_preflight(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    manifest = tmp_path / "backup-manifest.json"
    manifest.write_text("{}", encoding="utf-8")
    captures = _patch_full_up_path(monkeypatch)

    rc = main([
        "cluster", "up",
        "--namespace", "loom-public-beta",
        "--environment", "public-beta",
        "--backup-manifest", str(manifest),
        "--backup-max-age-hours", "12",
    ])

    assert rc == 0
    assert captures["preflight_kwargs"]["environment"] == "public-beta"
    assert captures["preflight_kwargs"]["backup_manifest"] == manifest.resolve()
    assert captures["preflight_kwargs"]["backup_max_age_hours"] == 12


def test_cli_up_config_file_threads_static_host_path_to_preflight_and_render(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    cfg = tmp_path / "cluster.toml"
    cfg.write_text(
        'namespace = "loom-public-beta"\n'
        'persistent_storage_backend = "static-host-path"\n'
        'persistent_storage_host_path_root = "/data/loom-public-beta"\n',
        encoding="utf-8",
    )
    captures = _patch_full_up_path(monkeypatch)

    rc = main(["cluster", "up", "--config", str(cfg)])

    assert rc == 0
    assert (
        captures["preflight_kwargs"]["cluster_config"].persistent_storage_backend
        == "static-host-path"
    )
    assert "kind: PersistentVolume" in captures["apply_yaml_text"]
    assert "path: \"/data/loom-public-beta/postgres\"" in captures["apply_yaml_text"]


def test_cli_up_config_file_invalid_returns_2(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A typo in cluster-config.toml shouldn't reach apply."""
    cfg = tmp_path / "bad.toml"
    cfg.write_text('imag_tag = "1.2.3"\n', encoding="utf-8")
    _patch_full_up_path(monkeypatch)
    rc = main(["cluster", "up", "--config", str(cfg)])
    assert rc == 2
    err = capsys.readouterr().err
    assert "render failed" in err
