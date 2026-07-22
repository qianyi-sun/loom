from __future__ import annotations

import io
import json
from pathlib import Path
from typing import Any

import pytest

from loom_cli.rollout.base_context_fixture import make_ctx
from loom_cli.rollout.evidence import StepDir
from loom_cli.rollout.operator.redaction import rollout_redaction_scope
from loom_cli.rollout.steps.s10_env_state import (
    CatalogPortForwardHandle,
    CatalogProvisioningError,
    CatalogProvisioningPlan,
    EnvStateStep,
    _catalog_env_file,
    _redact_catalog_output,
    _run_catalog_provisioning,
    _start_catalog_port_forward,
    _stop_catalog_port_forward,
    _wait_for_local_tcp,
)
from loom_cli.rollout.steps.subprocess_util import SubprocessResult


def _step_dir(tmp_path: Path) -> StepDir:
    path = tmp_path / "11-env-state"
    path.mkdir()
    return StepDir(number=11, name="env-state", path=path)


def test_catalog_redacts_optional_env_command_and_source_paths(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    optional_secret = "optional-catalog-secret-sentinel"
    command_source = "/private/catalog-command-source-sentinel"
    env_file_path = "/private/catalog-env-file-sentinel"
    source_ref = "file:/private/catalog-source-ref-sentinel"
    plan = CatalogProvisioningPlan(
        command=f"loom datasets audit --source {command_source}",
        env={"OPTIONAL_CATALOG_VALUE": optional_secret},
        required_env=[],
        env_file={"path": env_file_path, "key_count": 1, "keys": ["OPTIONAL_CATALOG_VALUE"]},
        env_sources={"OPTIONAL_CATALOG_VALUE": source_ref},
        kubernetes_port_forward=None,
    )

    monkeypatch.setattr(
        "loom_cli.rollout.steps.s10_env_state.run_captured",
        lambda argv, **kwargs: SubprocessResult(
            argv=list(argv),
            returncode=1,
            stdout=f"optional={optional_secret}\n",
            stderr=f"failed reading {command_source} via {source_ref}\n",
        ),
    )

    step_dir = _step_dir(tmp_path)
    result = _run_catalog_provisioning(
        plan,
        cwd=tmp_path,
        step_dir=step_dir,
    )

    assert result is not None
    persisted = "\n".join(
        path.read_text(encoding="utf-8", errors="replace")
        for path in step_dir.path.rglob("*")
        if path.is_file()
    )
    for sentinel in (optional_secret, command_source, env_file_path, source_ref):
        assert sentinel not in persisted
        assert sentinel not in (result.error or "")
    evidence = json.loads(
        step_dir.artifact_path("catalog-provisioning.json").read_text(encoding="utf-8")
    )
    assert "command_sha256" in evidence
    assert "command" not in evidence


def test_catalog_output_redacts_values_not_listed_as_required() -> None:
    optional_secret = "optional-unrequired-catalog-sentinel"

    rendered = _redact_catalog_output(
        f"catalog returned {optional_secret}",
        env={"OPTIONAL_VALUE": optional_secret},
        required_env=[],
    )

    assert optional_secret not in rendered
    assert "[REDACTED:OPTIONAL_VALUE]" in rendered


def test_catalog_cache_rejects_symlink_authority_without_running_command(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    step_dir = _step_dir(tmp_path)
    outside = tmp_path / "outside-cache"
    outside.mkdir(mode=0o700)
    step_dir.artifact_path("catalog-cache").symlink_to(outside, target_is_directory=True)
    called = False

    def unexpected_run(argv: list[str], **kwargs: Any) -> SubprocessResult:
        nonlocal called
        called = True
        return SubprocessResult(argv=argv, returncode=0, stdout="", stderr="")

    monkeypatch.setattr(
        "loom_cli.rollout.steps.s10_env_state.run_captured",
        unexpected_run,
    )
    plan = CatalogProvisioningPlan(
        command="loom datasets audit --all",
        env={"PATH": "/usr/bin"},
        required_env=[],
        env_file=None,
        env_sources={},
        kubernetes_port_forward=None,
    )

    result = _run_catalog_provisioning(plan, cwd=tmp_path, step_dir=step_dir)

    assert result is not None
    assert result.exit_code == 1
    assert "cache authority is unsafe" in (result.error or "")
    assert called is False
    assert outside.is_dir()
    assert list(outside.iterdir()) == []


def test_catalog_env_file_is_private_bounded_and_never_exposes_source_path(
    tmp_path: Path,
) -> None:
    secret = "private-catalog-env-secret-sentinel"
    source = tmp_path / "catalog-secret.env"
    source.write_text(f"OPTIONAL_VALUE={secret}\n", encoding="utf-8")
    source.chmod(0o600)

    values, evidence, protected = _catalog_env_file({"env_file": str(source)})

    assert values == {"OPTIONAL_VALUE": secret}
    assert evidence is not None
    assert evidence["source_identity"].startswith("sha256:")
    assert str(source) not in json.dumps(evidence)
    assert str(source) in protected

    # The deployment contract uses root/owner write plus service-group read.
    source.chmod(0o640)
    group_values, _, _ = _catalog_env_file({"env_file": str(source)})
    assert group_values == values

    symlink = tmp_path / "catalog-link.env"
    symlink.symlink_to(source)
    with pytest.raises(CatalogProvisioningError, match="unavailable or unsafe"):
        _catalog_env_file({"env_file": str(symlink)})

    source.chmod(0o660)
    with pytest.raises(CatalogProvisioningError, match="group-writable"):
        _catalog_env_file({"env_file": str(source)})


def test_port_forward_uses_bounded_env_and_never_persists_raw_partial_output(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    secret = "port-forward-secret-sentinel"
    source_path = "/private/port-forward-source-sentinel"
    captured: dict[str, Any] = {}

    class _ExitedPopen:
        returncode = 19

        def __init__(self, argv: list[str], **kwargs: Any) -> None:
            captured["argv"] = argv
            captured["kwargs"] = kwargs
            self.stdout = io.StringIO(f"partial {secret}\n")
            self.stderr = io.StringIO(f"source={source_path}\n")

        def poll(self) -> int:
            return self.returncode

        def communicate(self, timeout: float | None = None) -> tuple[str, str]:
            del timeout
            return (f"partial {secret}\n", f"source={source_path}\n")

        def terminate(self) -> None:
            return None

        def kill(self) -> None:
            return None

        def wait(self, timeout: float | None = None) -> int:
            del timeout
            return self.returncode

    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "ambient-cloud-secret")
    monkeypatch.setenv("HTTP_PROXY", "http://ambient-proxy")
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s10_env_state.subprocess.Popen",
        _ExitedPopen,
    )

    step_dir = _step_dir(tmp_path)
    with pytest.raises(CatalogProvisioningError) as caught:
        _start_catalog_port_forward(
            namespace="loom-staging",
            resource="service/loom-postgres",
            remote_port=5432,
            local_port=15432,
            step_dir=step_dir,
            name="postgres",
            child_env={"PATH": "/usr/bin", "KUBECONFIG": "/fixed/kubeconfig"},
            known_values=(secret, source_path),
        )

    child_env = captured["kwargs"]["env"]
    assert child_env == {"PATH": "/usr/bin", "KUBECONFIG": "/fixed/kubeconfig"}
    assert captured["kwargs"]["stdout"] == -1
    assert captured["kwargs"]["stderr"] == -1
    combined = (
        result := "\n".join(
            path.read_text(encoding="utf-8", errors="replace")
            for path in step_dir.path.rglob("*")
            if path.is_file()
        )
    ) + str(caught.value)
    assert result
    assert secret not in combined
    assert source_path not in combined
    assert "ambient-cloud-secret" not in combined
    assert "ambient-proxy" not in combined


def test_chatty_port_forward_is_drained_and_persisted_output_stays_bounded(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    secret = "chatty-port-forward-secret-sentinel"
    chatter = "progress-line\n" * 50_000

    class _ChattyPopen:
        returncode: int | None = None

        def __init__(self, argv: list[str], **kwargs: Any) -> None:
            del argv, kwargs
            self.stdout = io.StringIO(chatter + secret)
            self.stderr = io.StringIO(chatter + secret)

        def poll(self) -> int | None:
            return self.returncode

        def communicate(self, timeout: float | None = None) -> tuple[str, str]:
            del timeout
            raise AssertionError("background drainers must own child pipes")

        def terminate(self) -> None:
            self.returncode = 0

        def kill(self) -> None:
            self.returncode = -9

        def wait(self, timeout: float | None = None) -> int:
            del timeout
            self.returncode = 0 if self.returncode is None else self.returncode
            return self.returncode

    monkeypatch.setattr(
        "loom_cli.rollout.steps.s10_env_state.subprocess.Popen",
        _ChattyPopen,
    )
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s10_env_state._wait_for_local_tcp",
        lambda **_kwargs: None,
    )

    step_dir = _step_dir(tmp_path)
    handle = _start_catalog_port_forward(
        namespace="loom-staging",
        resource="service/loom-postgres",
        remote_port=5432,
        local_port=15432,
        step_dir=step_dir,
        name="postgres",
        child_env={"PATH": "/usr/bin"},
        known_values=(secret,),
    )
    _stop_catalog_port_forward(handle)

    for path in (handle.stdout_log, handle.stderr_log):
        rendered = path.read_text(encoding="utf-8")
        assert len(rendered) <= 70_000
        assert secret not in rendered


def test_oversized_pem_port_forward_output_is_discarded_fail_closed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    oversized_pem = (
        "-----BEGIN PRIVATE KEY-----\n"
        + ("long-private-key-body-sentinel" * 4_000)
        + "\n-----END PRIVATE KEY-----\n"
    )

    class _LongPemPopen:
        returncode: int | None = None

        def __init__(self, argv: list[str], **kwargs: Any) -> None:
            del argv, kwargs
            self.stdout = io.StringIO(oversized_pem)
            self.stderr = io.StringIO(oversized_pem)

        def poll(self) -> int | None:
            return self.returncode

        def communicate(self, timeout: float | None = None) -> tuple[str, str]:
            del timeout
            raise AssertionError("background drainers must own child pipes")

        def terminate(self) -> None:
            self.returncode = 0

        def kill(self) -> None:
            self.returncode = -9

        def wait(self, timeout: float | None = None) -> int:
            del timeout
            self.returncode = 0 if self.returncode is None else self.returncode
            return self.returncode

    monkeypatch.setattr(
        "loom_cli.rollout.steps.s10_env_state.subprocess.Popen",
        _LongPemPopen,
    )
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s10_env_state._wait_for_local_tcp",
        lambda **_kwargs: None,
    )

    step_dir = _step_dir(tmp_path)
    handle = _start_catalog_port_forward(
        namespace="loom-staging",
        resource="service/loom-postgres",
        remote_port=5432,
        local_port=15432,
        step_dir=step_dir,
        name="postgres",
        child_env={"PATH": "/usr/bin"},
        known_values=(),
    )
    _stop_catalog_port_forward(handle)

    for path in (handle.stdout_log, handle.stderr_log):
        assert path.read_text(encoding="utf-8") == ("[REDACTED:oversized-port-forward-output]\n")


def test_early_port_forward_exit_redacts_full_diagnostic_before_tail_slice(
    tmp_path: Path,
) -> None:
    secret = "boundary-tail-leak-sentinel-" * 40

    class _RawCapture:
        def rendered(self) -> str:
            return "kubectl failed: " + secret

    class _ExitedProcess:
        returncode = 19

        def poll(self) -> int:
            return self.returncode

        def wait(self, timeout: float | None = None) -> int:
            del timeout
            return self.returncode

    handle = CatalogPortForwardHandle(
        name="postgres",
        namespace="loom-staging",
        resource="service/loom-postgres",
        remote_port=5432,
        local_port=15432,
        stdout_log=tmp_path / "stdout.log",
        stderr_log=tmp_path / "stderr.log",
        process=_ExitedProcess(),  # type: ignore[arg-type]
        known_values=(secret,),
        stdout_capture=_RawCapture(),  # type: ignore[arg-type]
    )

    with pytest.raises(CatalogProvisioningError) as caught:
        _wait_for_local_tcp(handle=handle, local_port=15432)

    rendered = str(caught.value)
    assert "boundary-tail-leak-sentinel" not in rendered
    assert "[REDACTED" in rendered


def test_env_state_resolves_profile_from_validated_candidate_config(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = make_ctx(tmp_path)
    candidate_root = tmp_path / "candidate"
    candidate_config = candidate_root / "deploy" / "environments" / "staging.cluster.toml"
    candidate_profile = candidate_root / "deploy" / "environment-state" / "staging.toml"
    candidate_config.parent.mkdir(parents=True)
    candidate_profile.parent.mkdir(parents=True)
    candidate_config.write_text(
        'env_state_profile = "../environment-state/staging.toml"\n',
        encoding="utf-8",
    )
    candidate_profile.write_text('environment = "staging"\n', encoding="utf-8")
    calls: list[list[str]] = []
    validation_order: list[str] = []

    def fake_candidate_cwd(_step_dir: StepDir) -> Path:
        validation_order.append("validated")
        return candidate_root

    def fake_candidate_env(_step_dir: StepDir) -> dict[str, str]:
        assert validation_order == ["validated"]
        return {"PATH": "/usr/bin"}

    def fake_candidate_relative_path(path: Path, _step_dir: StepDir) -> Path:
        assert validation_order == ["validated"]
        if path == ctx.cluster_config_path:
            return candidate_config
        return path

    outputs = iter(
        [
            SubprocessResult(
                argv=["loom", "admin", "environment-state", "apply"],
                returncode=0,
                stdout="applied\n",
                stderr="",
            ),
            SubprocessResult(
                argv=["loom", "admin", "environment-state", "check"],
                returncode=0,
                stdout='{"ok": true}\n',
                stderr="",
            ),
        ]
    )

    monkeypatch.setattr(
        "loom_cli.rollout.steps.s10_env_state.candidate_loom_cwd",
        fake_candidate_cwd,
    )
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s10_env_state.candidate_loom_env",
        fake_candidate_env,
    )
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s10_env_state.candidate_worktree",
        lambda _step_dir: candidate_root,
    )
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s10_env_state.candidate_relative_path",
        fake_candidate_relative_path,
    )
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s10_env_state.candidate_loom_argv",
        lambda *args: list(args),
    )
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s10_env_state._materialize_external_slurm_runner_prerequisites",
        lambda *_args, **_kwargs: [],
    )
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s10_env_state._wait_for_control_plane",
        lambda *_args, **_kwargs: None,
    )

    def fake_run(argv: list[str], **_kwargs: Any) -> SubprocessResult:
        calls.append(list(argv))
        return next(outputs)

    monkeypatch.setattr(
        "loom_cli.rollout.steps.s10_env_state.run_captured",
        fake_run,
    )

    result = EnvStateStep().run(ctx, _step_dir(tmp_path))

    assert result.exit_code == 0
    assert validation_order == ["validated"]
    assert len(calls) == 2
    assert all(str(candidate_profile) in call for call in calls)


def test_env_state_candidate_without_profile_is_noop_despite_stale_runner_config(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = make_ctx(tmp_path)
    stale_profile = tmp_path / "stale-runner-profile.toml"
    stale_profile.write_text('environment = "staging"\n', encoding="utf-8")
    ctx.cluster_config_path.write_text(
        f'env_state_profile = "{stale_profile}"\n',
        encoding="utf-8",
    )
    candidate_root = tmp_path / "candidate"
    candidate_config = candidate_root / "deploy" / "environments" / "staging.cluster.toml"
    candidate_config.parent.mkdir(parents=True)
    candidate_config.write_text("image_tag = 'candidate'\n", encoding="utf-8")
    validation_order: list[str] = []

    monkeypatch.setattr(
        "loom_cli.rollout.steps.s10_env_state.candidate_loom_cwd",
        lambda _step_dir: validation_order.append("validated") or candidate_root,
    )
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s10_env_state.candidate_loom_env",
        lambda _step_dir: {"PATH": "/usr/bin"},
    )
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s10_env_state.candidate_worktree",
        lambda _step_dir: candidate_root,
    )
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s10_env_state.candidate_relative_path",
        lambda path, _step_dir: candidate_config if path == ctx.cluster_config_path else path,
    )
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s10_env_state.candidate_loom_argv",
        lambda *args: list(args),
    )
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s10_env_state._materialize_external_slurm_runner_prerequisites",
        lambda *_args, **_kwargs: [],
    )
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s10_env_state._wait_for_control_plane",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s10_env_state.run_captured",
        lambda *_args, **_kwargs: pytest.fail("candidate no-op must not mutate staging"),
    )

    result = EnvStateStep().run(ctx, _step_dir(tmp_path))

    assert result.exit_code == 0
    assert result.summary == "no env-state profile; step is a no-op"
    assert validation_order == ["validated"]


def test_env_state_sanitizes_all_direct_step_evidence_writes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    secret = "direct-step-output-secret-sentinel"
    source_path = "/private/direct-step-source-sentinel"
    credential_url = "postgresql://loom:direct-password@db.internal/loom"
    private_key = (
        "-----BEGIN PRIVATE KEY-----\ndirect-private-key-body-sentinel\n-----END PRIVATE KEY-----"
    )
    ctx = make_ctx(tmp_path)
    profile = tmp_path / "staging.toml"
    profile.write_text('environment = "staging"\n', encoding="utf-8")
    step_dir = _step_dir(tmp_path)
    outputs = iter(
        [
            SubprocessResult(
                argv=["loom", "admin", "environment-state", "apply"],
                returncode=0,
                stdout=f"applied {secret} from {source_path}\n",
                stderr=f"diagnostic {credential_url}\n",
            ),
            SubprocessResult(
                argv=["loom", "admin", "environment-state", "check"],
                returncode=1,
                stdout=('{"ok": false, "drift": ["' + secret + '"], "autoscaler_blockers": []}\n'),
                stderr=private_key + "\n",
            ),
        ]
    )
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s10_env_state._candidate_profile_path",
        lambda _ctx, _step_dir: Path(profile),
    )
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s10_env_state.candidate_loom_cwd",
        lambda _step_dir: tmp_path,
    )
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s10_env_state.candidate_loom_env",
        lambda _step_dir: {"PATH": "/usr/bin"},
    )
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s10_env_state.candidate_relative_path",
        lambda path, _step_dir: path,
    )
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s10_env_state.candidate_loom_argv",
        lambda *args: list(args),
    )
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s10_env_state._materialize_external_slurm_runner_prerequisites",
        lambda *_args, **_kwargs: [],
    )
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s10_env_state._wait_for_control_plane",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        "loom_cli.rollout.steps.s10_env_state.run_captured",
        lambda *_args, **_kwargs: next(outputs),
    )

    with rollout_redaction_scope((secret, source_path)):
        result = EnvStateStep().run(ctx, step_dir)

    assert result.exit_code == 1
    persisted = "\n".join(
        path.read_text(encoding="utf-8", errors="replace")
        for path in step_dir.path.rglob("*")
        if path.is_file()
    )
    for sentinel in (secret, source_path, credential_url, private_key):
        assert sentinel not in persisted
        assert sentinel not in (result.error or "")
