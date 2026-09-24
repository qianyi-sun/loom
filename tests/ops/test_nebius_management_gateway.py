"""Protected management transport accepts one exact source/input authority only."""
from __future__ import annotations

import hashlib
import importlib
import io
import json
from types import SimpleNamespace

import pytest
from tests.ops.test_nebius_ingress_bootstrap import archive


def module():
    return importlib.import_module("scripts.ops.nebius_management_gateway")


def operation(tmp_path):
    root = tmp_path / "nebius-management"
    return {"schema": "loom.nebius-management-operation.v1", "source_sha": "a" * 40,
        "candidate": "b" * 40, "installation_id": "18718d96-d389-40b3-a79b-11489924d0d4",
        "namespace": "loom-nebius-management", "state_dir": str(root / "state"),
        "anchor_dir": str(root / "anchor"), "inputs_path": str(root / "inputs.json"),
        "inputs_sha256": "c" * 64}


def bundle(tmp_path):
    files = {name: b"fixture source" for name in module().SOURCES}
    files.update({"uv": b"fixture binary", "requirements.txt": b"fixture hashed dependencies",
        "operation.json": json.dumps(operation(tmp_path)).encode(),
        "wheels/loom-0.0.0-py3-none-any.whl": b"loom wheel",
        "wheels/loom_bundle_checksum-0.1.0-py3-none-any.whl": b"checksum wheel"})
    return files


@pytest.mark.parametrize("change", ["extra", "missing_template", "hash", "missing_wheel", "private_input"])
def test_unqualified_bundle_cannot_create_tooling(tmp_path, change):
    files = bundle(tmp_path)
    if change == "extra":
        files["../outside"] = b"bad"
    elif change == "missing_template":
        files.pop("deploy/k8s/nebius-execution-actuator.yaml")
    elif change == "missing_wheel":
        files.pop("wheels/loom_bundle_checksum-0.1.0-py3-none-any.whl")
    elif change == "private_input":
        files["inputs.json"] = b"must-never-transfer"
    with pytest.raises(module().GatewayError):
        module().prepare_release(archive(files, bad_hash=change == "hash"))
    assert not (tmp_path / "nebius-management").exists()


@pytest.mark.parametrize("field,value", [("state_dir", "/tmp/foreign/state"), ("anchor_dir", "/tmp/elsewhere"),
    ("source_sha", "dev"), ("candidate", "HEAD"), ("inputs_sha256", ""), ("private_key", "never-transfer")])
def test_operation_metadata_cannot_broaden_authority(tmp_path, field, value):
    metadata = operation(tmp_path)
    metadata[field] = value
    with pytest.raises(module().GatewayError):
        module().validate_operation(metadata)


def test_tooling_is_private_pinned_and_replay_does_not_reinstall(tmp_path, monkeypatch):
    gateway = module()
    calls = []
    monkeypatch.setattr(gateway, "run_private", lambda args, **kwargs: calls.append(args) or b"")
    content = archive(bundle(tmp_path))
    release = gateway.prepare_release(content)
    assert (release / "operation.json").read_bytes() == bundle(tmp_path)["operation.json"]
    assert (release / "deploy/k8s/nebius-capacity-collector.yaml").is_file()
    assert "--require-hashes" in calls[1] and "--only-binary" in calls[1]
    assert "--offline" in calls[2] and "--no-deps" in calls[2]
    assert calls[3][-1] == "qualify" and calls[3][1:3] == ["-I", "-c"]
    assert gateway.prepare_release(content) == release and len(calls) == 4
    assert not (release / "inputs.json").exists()
    assert not (release.parent.parent / "state").exists()
    assert all(path.stat().st_mode & 0o077 == 0 for path in release.rglob("*"))


def test_incomplete_tooling_never_retries_and_modified_tooling_never_replays(tmp_path, monkeypatch):
    gateway = module()
    calls = []
    def fail(args, **kwargs):
        calls.append(args)
        raise RuntimeError("private failure")
    monkeypatch.setattr(gateway, "run_private", fail)
    content = archive(bundle(tmp_path))
    for _ in range(2):
        with pytest.raises(gateway.GatewayError):
            gateway.prepare_release(content)
    assert len(calls) == 1


@pytest.mark.parametrize("command", ["", "loom-nebius-management-install-v1 extra", "kubectl apply", "loom-nebius-ingress-v1"])
def test_forced_command_rejects_arbitrary_or_other_authority_without_reading_input(command, monkeypatch):
    import sys
    monkeypatch.setenv("SSH_ORIGINAL_COMMAND", command)
    monkeypatch.setattr(sys, "stdin", object())
    assert module().authorized_main("a" * 64) == 126


def test_forced_command_requires_exact_bundle_digest(tmp_path, monkeypatch):
    import sys
    content = archive(bundle(tmp_path))
    monkeypatch.setenv("SSH_ORIGINAL_COMMAND", "loom-nebius-management-install-v1")
    monkeypatch.setattr(sys, "stdin", SimpleNamespace(buffer=io.BytesIO(content)))
    assert module().authorized_main("a" * 64) == 126
    assert not (tmp_path / "nebius-management").exists()


@pytest.mark.parametrize("status", ["preflight_qualified", "pending", "management_installed"])
def test_public_report_strips_all_private_material_and_binds_operation(tmp_path, status):
    metadata = operation(tmp_path)
    report = {key: metadata[key] for key in ("source_sha", "candidate", "installation_id", "namespace")}
    report.update(status=status, phase="database", namespace_uid="52f5b18c-7dd3-4095-bd7e-49f6a6330391",
                  revision="sha256:" + "d" * 64, material="must-never-transfer")
    safe = module().safe_report(json.dumps(report).encode(), metadata)
    assert safe["status"] == status and "must-never-transfer" not in json.dumps(safe)
    report["candidate"] = "e" * 40
    with pytest.raises(module().GatewayError):
        module().safe_report(json.dumps(report).encode(), metadata)


def test_forced_command_runs_only_selected_action_and_exports_sanitized_report(tmp_path, monkeypatch, capsys):
    import sys
    gateway = module()
    content = archive(bundle(tmp_path))
    report = {key: operation(tmp_path)[key] for key in ("source_sha", "candidate", "installation_id", "namespace")}
    report.update(status="preflight_qualified", private="must-never-transfer")
    calls = []
    monkeypatch.setenv("SSH_ORIGINAL_COMMAND", "loom-nebius-management-preflight-v1")
    monkeypatch.setattr(sys, "stdin", SimpleNamespace(buffer=io.BytesIO(content)))
    monkeypatch.setattr(gateway, "prepare_release", lambda content: tmp_path / "release")
    monkeypatch.setattr(gateway, "run_private", lambda args, **kwargs: calls.append(args) or json.dumps(report).encode())
    assert gateway.authorized_main(hashlib.sha256(content).hexdigest()) == 0
    assert calls[0][-1] == "preflight"
    assert "must-never-transfer" not in capsys.readouterr().out
