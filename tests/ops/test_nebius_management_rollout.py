"""Actions builds only integrated tooling and sends no private installation inputs."""
from __future__ import annotations

import importlib
import io
import json
import subprocess
import zipfile
from pathlib import Path

import pytest
import yaml
from tests.ops.test_nebius_ingress_bootstrap import archive
from tests.ops.test_nebius_management_gateway import bundle, operation


def module():
    return importlib.import_module("scripts.ops.nebius_management_rollout")


def test_bundle_is_reproducible_complete_and_excludes_private_inputs(tmp_path):
    uv, requirements, wheels = tmp_path / "uv", tmp_path / "requirements.txt", tmp_path / "wheels"
    uv.write_bytes(b"approved uv")
    requirements.write_bytes(b"approved dependencies")
    wheels.mkdir()
    for name in ("loom-0.0.0-py3-none-any.whl", "loom_bundle_checksum-0.1.0-py3-none-any.whl"):
        (wheels / name).write_bytes(b"wheel fixture")
    metadata = operation(tmp_path)
    content = module().build_bundle(metadata, uv=uv, requirements=requirements, wheels=wheels)
    assert content == module().build_bundle(metadata, uv=uv, requirements=requirements, wheels=wheels)
    with zipfile.ZipFile(io.BytesIO(content)) as result:
        assert "inputs.json" not in result.namelist()
        assert json.loads(result.read("operation.json")) == metadata
        assert result.read("scripts/ops/nebius_management_entry.py") == (
            Path(__file__).resolve().parents[2] / "scripts/ops/nebius_management_entry.py").read_bytes()
        assert len([name for name in result.namelist() if name.endswith(".whl")]) == 2


@pytest.mark.parametrize("action,status", [("preflight", "preflight_qualified"), ("install", "pending"), ("install", "management_installed")])
def test_exact_operation_transports_only_bundle_and_strips_private_reports(tmp_path, monkeypatch, action, status):
    content, metadata = archive(bundle(tmp_path)), operation(tmp_path)
    report = {key: metadata[key] for key in ("source_sha", "candidate", "installation_id", "namespace")}
    report.update(status=status, private="never-transfer", phase="database", revision="sha256:" + "d" * 64,
                  namespace_uid="52f5b18c-7dd3-4095-bd7e-49f6a6330391")
    calls = []
    def run(args, **kwargs):
        calls.append(args)
        assert args[-1] == "loom-nebius-management-" + action + "-v1"
        assert args[-2] == "codex@192.0.2.1"
        assert "StrictHostKeyChecking=yes" in args and "IdentitiesOnly=yes" in args
        assert kwargs["input"] == content
        return subprocess.CompletedProcess(args, 0, json.dumps(report).encode(), b"private logs")
    monkeypatch.setattr(subprocess, "run", run)
    result = module().transfer(content, action=action, target="codex@192.0.2.1", key=Path("/private/key"),
                               known_hosts=Path("/private/hosts"))
    assert result["status"] == status and "never-transfer" not in json.dumps(result) and len(calls) == 1


@pytest.mark.parametrize("case", ["failure", "timeout", "wrong_action", "other_candidate"])
def test_unknown_or_misbound_result_never_retries_or_leaks(tmp_path, monkeypatch, case):
    content, metadata = archive(bundle(tmp_path)), operation(tmp_path)
    calls = []
    def run(args, **kwargs):
        calls.append(args)
        if case == "timeout":
            raise subprocess.TimeoutExpired(args, 10, output=b"private data")
        report = {key: metadata[key] for key in ("source_sha", "candidate", "installation_id", "namespace")}
        report["status"] = "preflight_qualified"
        if case == "other_candidate":
            report["candidate"] = "0" * 40
        return subprocess.CompletedProcess(args, 1 if case == "failure" else 0, json.dumps(report).encode(), b"private data")
    monkeypatch.setattr(subprocess, "run", run)
    with pytest.raises(module().RolloutError) as error:
        module().transfer(content, action="install", target="codex@host", key=Path("/private/key"), known_hosts=Path("/private/hosts"))
    assert len(calls) == 1 and "private data" not in str(error.value)


@pytest.mark.parametrize("case", ["clean", "dirty", "different_head", "not_integrated"])
def test_source_must_be_exact_clean_and_integrated(tmp_path, monkeypatch, case):
    metadata = operation(tmp_path)
    def run(args, **kwargs):
        if args[1] == "rev-parse":
            result = b"0" * 40 if case == "different_head" else b"a" * 40
        elif args[1] == "status":
            result = b" M source.py" if case == "dirty" else b""
        else:
            assert args == ["git", "merge-base", "--is-ancestor", "a" * 40, "refs/remotes/origin/dev"]
            return subprocess.CompletedProcess(args, int(case == "not_integrated"), b"", b"")
        return subprocess.CompletedProcess(args, 0, result, b"")
    monkeypatch.setattr(subprocess, "run", run)
    if case == "clean":
        module().verify_source(metadata)
    else:
        with pytest.raises(module().RolloutError):
            module().verify_source(metadata)


def test_management_workflow_uses_protected_environment_and_separate_fixed_authority():
    path = Path(__file__).resolve().parents[2] / ".github/workflows/nebius-rollout.yml"
    workflow = yaml.safe_load(path.read_text())
    dispatch = workflow.get("on", workflow.get(True))["workflow_dispatch"]["inputs"]["operation"]["options"]
    assert {"management-preflight", "management-install"} <= set(dispatch)
    job = workflow["jobs"]["management"]
    assert job["environment"]["name"] == "nebius-integration" and job["permissions"] == {"contents": "read"}
    assert "workflow_dispatch" in job["if"] and "refs/heads/dev" in job["if"]
    assert workflow["concurrency"]["cancel-in-progress"] is False
    run = next(step for step in job["steps"] if step.get("name") == "Run fixed management operation")
    assert run["env"]["DEPLOY_SSH_KEY"] == "${{ secrets.NEBIUS_MANAGEMENT_SSH_KEY }}"
    assert not any("SERVICE_ACCOUNT" in name for name in run["env"])
