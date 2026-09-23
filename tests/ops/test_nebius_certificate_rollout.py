"""Certificate workflow transports only approved tooling and cannot select app rollout."""
from __future__ import annotations

import importlib
import io
import json
import os
import subprocess
import zipfile
from pathlib import Path

import pytest
import yaml


def module():
    return importlib.import_module("scripts.ops.nebius_certificate_rollout")


def test_bundle_is_repeatable_and_does_not_include_dns_credentials(tmp_path):
    uv = tmp_path / "uv"
    uv.write_bytes(b"qualified uv")
    requirements = tmp_path / "requirements.txt"
    requirements.write_text("certbot==5.8.0 --hash=sha256:fixture\n")
    config = {"credential_file": "/gateway/private/godaddy.json", "state_dir": "/gateway/nebius-certificates/state"}
    first = module().build_bundle(config, uv=uv, requirements=requirements)
    assert first == module().build_bundle(config, uv=uv, requirements=requirements)
    with zipfile.ZipFile(io.BytesIO(first)) as archive:
        assert set(archive.namelist()) == {"uv", "requirements.txt", "installation.json", "manifest.json",
                                           "scripts/ops/nebius_certificates.py", "scripts/ops/nebius_dns_challenge.py"}
        assert json.loads(archive.read("installation.json")) == config


def test_transfer_preserves_host_verification_and_passes_only_data_stdin(monkeypatch):
    report = {"status": "qualified", "installation_id": "024cfbfb-a7e8-4d85-9c60-c1d838730f9a",
              "fingerprint_sha256": "a" * 64, "generation": "b" * 64,
              "expires_at": "2026-12-02T12:00:00+00:00",
              "sans": ["*.dev.example.test", "management.example.test"]}

    def run(args, **kwargs):
        assert args[0] == "ssh"
        assert "StrictHostKeyChecking=yes" in args and "IdentitiesOnly=yes" in args
        assert "UserKnownHostsFile=/private/known_hosts" in args
        assert args[-2] == "codex@192.0.2.1"
        assert kwargs["input"] == b"tooling-only"
        assert b"tooling-only" not in args[-1].encode()
        return subprocess.CompletedProcess(args, 0, json.dumps(report).encode(), b"")

    monkeypatch.setattr(subprocess, "run", run)
    result = module().transfer(b"tooling-only", target="codex@192.0.2.1", key=Path("/private/key"),
                               known_hosts=Path("/private/known_hosts"))
    assert result == report


@pytest.mark.parametrize("target", ["-oProxyCommand=evil", "codex@host;echo injected", "host", "codex@host\nother"])
def test_invalid_ssh_target_is_rejected_before_process_creation(target, monkeypatch):
    monkeypatch.setattr(subprocess, "run", lambda *a, **k: pytest.fail("unsafe SSH target executed"))
    with pytest.raises(module().RolloutError):
        module().transfer(b"data", target=target, key=Path("/private/key"), known_hosts=Path("/private/known_hosts"))


def test_remote_failure_is_not_retried_or_forwarded(monkeypatch):
    calls = []

    def failure(args, **kwargs):
        calls.append(args)
        return subprocess.CompletedProcess(args, 1, b"private-response", b"private-error")

    monkeypatch.setattr(subprocess, "run", failure)
    with pytest.raises(module().RolloutError) as error:
        module().transfer(b"data", target="codex@192.0.2.1", key=Path("/private/key"), known_hosts=Path("/private/known_hosts"))
    assert len(calls) == 1 and "private-" not in str(error.value)


def test_certificate_operation_is_protected_and_not_a_route_to_application_rollout():
    workflow = yaml.load((Path(__file__).parents[2] / ".github/workflows/nebius-rollout.yml").read_text(), Loader=yaml.BaseLoader)
    assert "certificate" in workflow["on"]["workflow_dispatch"]["inputs"]["operation"]["options"]
    job = workflow["jobs"]["certificate"]
    assert job["environment"]["name"] == "nebius-integration"
    assert job["permissions"] == {"contents": "read"}
    for condition in ("github.repository == 'qianyi-sun/loom'", "github.ref == 'refs/heads/dev'",
                      "github.event_name == 'workflow_dispatch'", "inputs.operation == 'certificate'"):
        assert condition in job["if"]
    # No schedule is enabled until protected Secret delivery/reload is integrated.
    assert "schedule" not in workflow["on"]
    assert "inputs.operation == 'rollout'" in workflow["jobs"]["rollout"]["if"]
    commands = "\n".join(step.get("run", "") for step in job["steps"])
    assert "nebius_certificate_rollout.py" in commands
    assert "nebius_idle_rollout.py" not in commands
    assert "--only-group nebius-certificates" in commands
    artifact = next(step for step in job["steps"] if step.get("name") == "Preserve sanitized certificate evidence")
    assert artifact["with"]["path"].endswith("/certificate-result.json")


@pytest.mark.skipif(not os.environ.get("LOOM_TEST_CERTIFICATE_REQUIREMENTS"), reason="isolated locked gateway qualification")
def test_actual_locked_bundle_bootstraps_without_using_existing_operator_environment(tmp_path):
    from scripts.ops.nebius_certificate_gateway import prepare_release, run_private

    config = {"state_dir": str(tmp_path / "nebius-certificates" / "state")}
    content = module().build_bundle(config, uv=Path(os.environ["LOOM_TEST_CERTIFICATE_UV"]),
                                    requirements=Path(os.environ["LOOM_TEST_CERTIFICATE_REQUIREMENTS"]))
    release, _ = prepare_release(content)
    python = str(release / "venv" / "bin" / "python")
    result = run_private([python, "-c", "from certbot.main import main; raise SystemExit(main())", "--version"], timeout=30)
    assert result.strip() == b"certbot 5.8.0"
    result = run_private([python, str(release / "scripts" / "ops" / "nebius_certificates.py"), "--help"], timeout=30)
    assert b"issue" in result and b"hook" in result
    assert not (tmp_path / "nebius-certificates" / "state").exists(), "tooling qualification started live issuance"
