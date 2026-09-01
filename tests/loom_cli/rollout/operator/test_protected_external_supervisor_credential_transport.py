from __future__ import annotations

import json
import os
import subprocess
import time
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


def _transport(
    tmp_path: Path,
    run: _Run,
    *,
    execution_host: str = "TRT-EAI-OLDLAB-1",
    promote_existing_source: bool = False,
):
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
        execution_host=execution_host,
        source_kubeconfig=source,
        output_kubeconfig=run.output_path,
        service_uid=os.geteuid(),
        service_gid=os.getegid(),
        promote_existing_source=promote_existing_source,
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


def test_gb10_promotes_the_existing_narrow_source_without_broad_authority(
    tmp_path: Path,
) -> None:
    output = tmp_path / "external-supervisor.kubeconfig"
    run = _Run(output)
    transport = _transport(
        tmp_path,
        run,
        execution_host="gx10-01c7",
        promote_existing_source=True,
    )

    evidence = transport.publish()
    source = tmp_path / "rollout.kubeconfig"

    assert output.read_bytes() == source.read_bytes() == b"protected-source"
    assert output.stat().st_ino != source.stat().st_ino
    assert output.stat().st_nlink == source.stat().st_nlink == 1
    assert evidence.execution_host == "gx10-01c7"
    assert evidence.kubeconfig_sha256 == credential.hashlib.sha256(b"protected-source").hexdigest()
    assert [call[0][1:] for call in run.calls] == [
        ("--check", str(source)),
        ("--check", str(output)),
    ]
    assert all("KUBECONFIG" not in environment for _, environment, _ in run.calls)


def test_gb10_promotion_is_visible_only_with_final_single_link_authority(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output = tmp_path / "external-supervisor.kubeconfig"
    run = _Run(output)
    transport = _transport(
        tmp_path,
        run,
        execution_host="gx10-01c7",
        promote_existing_source=True,
    )
    concurrent_observations = []

    def rename_noreplace(directory_fd: int, source: str, destination: str) -> None:
        os.rename(source, destination, src_dir_fd=directory_fd, dst_dir_fd=directory_fd)
        assert output.stat().st_nlink == 1
        concurrent_observations.append(transport.observe())

    monkeypatch.setattr(
        credential,
        "_rename_noreplace",
        rename_noreplace,
        raising=False,
    )

    published = transport.publish()

    assert concurrent_observations == [published]
    assert output.stat().st_nlink == 1


def test_gb10_promotion_recovers_after_publication_crash_without_link_count_drift(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output = tmp_path / "external-supervisor.kubeconfig"
    run = _Run(output)
    transport = _transport(
        tmp_path,
        run,
        execution_host="gx10-01c7",
        promote_existing_source=True,
    )

    def publish_then_crash(directory_fd: int, source: str, destination: str) -> None:
        os.rename(source, destination, src_dir_fd=directory_fd, dst_dir_fd=directory_fd)
        raise OSError("simulated crash after atomic publication")

    monkeypatch.setattr(
        credential,
        "_rename_noreplace",
        publish_then_crash,
        raising=False,
    )

    with pytest.raises(RuntimeError, match="publication failed safely"):
        transport.publish()

    assert output.read_bytes() == b"protected-source"
    assert output.stat().st_nlink == 1
    assert transport.publish() == transport.observe()


def test_gb10_rejects_a_legacy_source_that_changes_during_permission_check(
    tmp_path: Path,
) -> None:
    output = tmp_path / "external-supervisor.kubeconfig"

    class _SourceMutatingRun(_Run):
        def __call__(self, argv, environment, timeout_seconds):
            result = super().__call__(argv, environment, timeout_seconds)
            if tuple(argv)[1:] == ("--check", str(tmp_path / "rollout.kubeconfig")):
                source = tmp_path / "rollout.kubeconfig"
                source.write_text("changed-source", encoding="utf-8")
                source.chmod(0o600)
            return result

    run = _SourceMutatingRun(output)
    transport = _transport(
        tmp_path,
        run,
        execution_host="gx10-01c7",
        promote_existing_source=True,
    )

    with pytest.raises(RuntimeError, match="publication failed safely"):
        transport.publish()

    assert not output.exists()


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


def test_observe_ignores_an_aged_atime_update(tmp_path: Path) -> None:
    output = tmp_path / "external-supervisor.kubeconfig"
    output.write_text("apiVersion: v1\nkind: Config\n", encoding="utf-8")
    output.chmod(0o600)
    now_ns = time.time_ns()
    os.utime(output, ns=(now_ns - 3 * 24 * 60 * 60 * 1_000_000_000, now_ns))
    run = _Run(output)

    evidence = _transport(tmp_path, run).observe()

    assert evidence is not None
    assert evidence.size == len("apiVersion: v1\nkind: Config\n")


@pytest.mark.parametrize(
    "mutation",
    ["replace", "write", "chmod", "truncate", "grow"],
)
def test_observe_rejects_stable_identity_or_content_mutation(
    tmp_path: Path,
    mutation: str,
) -> None:
    output = tmp_path / "external-supervisor.kubeconfig"
    original = "apiVersion: v1\nkind: Config\n"
    output.write_text(original, encoding="utf-8")
    output.chmod(0o600)

    class _MutatingRun(_Run):
        def __call__(self, argv, environment, timeout_seconds):
            result = super().__call__(argv, environment, timeout_seconds)
            if mutation == "replace":
                replacement = self.output_path.with_suffix(".replacement")
                replacement.write_text(original, encoding="utf-8")
                replacement.chmod(0o600)
                replacement.replace(self.output_path)
            elif mutation == "write":
                self.output_path.write_text(original.replace("v1", "v2"), encoding="utf-8")
            elif mutation == "chmod":
                self.output_path.chmod(0o640)
            elif mutation == "truncate":
                self.output_path.write_text("apiVersion: v1\n", encoding="utf-8")
            elif mutation == "grow":
                with self.output_path.open("a", encoding="utf-8") as stream:
                    stream.write("users: []\n")
            return result

    with pytest.raises((RuntimeError, ValueError), match=r"changed|unsafe"):
        _transport(tmp_path, _MutatingRun(output)).observe()
