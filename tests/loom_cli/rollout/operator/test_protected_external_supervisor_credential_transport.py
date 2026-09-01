from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

import loom_cli.rollout.operator.protected_external_supervisor_credential_transport as credential


class _Run:
    def __init__(self, output_path: Path) -> None:
        self.output_path = output_path
        self.calls: list[tuple[tuple[str, ...], dict[str, str], float]] = []

    def __call__(self, argv, environment, timeout_seconds):
        command = tuple(argv)
        self.calls.append((command, dict(environment), timeout_seconds))
        if "--check" not in command:
            self.output_path.write_text(
                "apiVersion: v1\nkind: Config\nusers: []\n",
                encoding="utf-8",
            )
            self.output_path.chmod(0o600)
        return SimpleNamespace(returncode=0, stdout=b"", stderr=b"")


def _transport(tmp_path: Path, run: _Run):
    candidate = tmp_path / "candidate"
    publisher = candidate / "deploy/slurm/publish-external-slurm-autoscaler-kubeconfig.sh"
    publisher.parent.mkdir(parents=True)
    publisher.write_text("#!/usr/bin/env bash\n", encoding="utf-8")
    publisher.chmod(0o500)
    source = tmp_path / "rollout.kubeconfig"
    source.write_text("protected-source", encoding="utf-8")
    source.chmod(0o600)
    return credential.FixedLocalExternalSupervisorCredentialTransport(
        candidate_root=candidate,
        execution_host="TRT-EAI-OLDLAB-1",
        source_kubeconfig=source,
        output_kubeconfig=run.output_path,
        service_uid=os.geteuid(),
        service_gid=os.getegid(),
        run=run,
    )


def test_absent_credential_is_repairable_and_publish_returns_safe_evidence(
    tmp_path: Path,
) -> None:
    output = tmp_path / "external-supervisor.kubeconfig"
    run = _Run(output)
    transport = _transport(tmp_path, run)

    assert transport.observe() is None
    evidence = transport.publish()

    assert evidence.execution_host == "TRT-EAI-OLDLAB-1"
    assert evidence.uid == os.geteuid()
    assert evidence.gid == os.getegid()
    assert evidence.mode == 0o600
    assert evidence.size == output.stat().st_size
    assert len(evidence.kubeconfig_sha256) == 64
    assert evidence.database_secret_readable is True
    assert evidence.witness_config_map_readable is True
    assert evidence.pods_exec_denied is True
    wire = evidence.to_dict()
    encoded = json.dumps(wire, sort_keys=True)
    assert credential.ExternalSupervisorCredentialEvidence.from_dict(wire) == evidence
    for forbidden in ("token", "certificate", "kubeconfig_bytes", "command_output"):
        assert forbidden not in encoded
    assert [call[0][1:] for call in run.calls] == [
        (str(output),),
        ("--check", str(output)),
    ]
    assert run.calls[0][1]["KUBECONFIG"].endswith("rollout.kubeconfig")
    assert "KUBECONFIG" not in run.calls[1][1]


def test_present_unsafe_credential_is_drift_not_overwritten(tmp_path: Path) -> None:
    output = tmp_path / "external-supervisor.kubeconfig"
    output.write_text("unsafe", encoding="utf-8")
    output.chmod(0o644)
    run = _Run(output)
    transport = _transport(tmp_path, run)

    with pytest.raises(ValueError, match="metadata is unsafe"):
        transport.observe()
    with pytest.raises(ValueError, match="metadata is unsafe"):
        transport.publish()

    assert run.calls == []


def test_check_failure_is_secret_free_and_fails_closed(tmp_path: Path) -> None:
    output = tmp_path / "external-supervisor.kubeconfig"
    output.write_text("apiVersion: v1\n", encoding="utf-8")
    output.chmod(0o600)

    class _FailingRun(_Run):
        def __call__(self, argv, environment, timeout_seconds):
            super().__call__(argv, environment, timeout_seconds)
            return SimpleNamespace(
                returncode=1,
                stdout=b"unexpected-sensitive-output",
                stderr=b"unexpected-sensitive-error",
            )

    run = _FailingRun(output)
    transport = _transport(tmp_path, run)

    with pytest.raises(RuntimeError, match="credential check failed safely") as caught:
        transport.observe()

    message = str(caught.value)
    assert "sensitive" not in message
    assert output.read_text(encoding="utf-8") == "apiVersion: v1\n"


def test_default_command_runner_discards_unbounded_child_output(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    def run(*args, **kwargs):
        captured.update(kwargs)
        return subprocess.CompletedProcess(
            args=args[0],
            returncode=0,
            stdout=b"unexpected child output",
            stderr=b"unexpected child error",
        )

    monkeypatch.setattr(credential.subprocess, "run", run)

    result = credential._subprocess_run(("publisher",), {}, 1.0)

    assert captured["stdout"] is subprocess.DEVNULL
    assert captured["stderr"] is subprocess.DEVNULL
    assert result.stdout == b""
    assert result.stderr == b""
