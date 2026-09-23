"""Installed entry only connects exact local authority to the owned operation."""
from __future__ import annotations

import importlib
import json

import pytest
from tests.ops.test_nebius_ingress_bootstrap import configuration


def module():
    return importlib.import_module("scripts.ops.nebius_ingress_entry")


def installed(tmp_path):
    config = configuration(tmp_path)
    config["image"] = "cr.eu-north1.nebius.cloud/registry/loom-shared-ingress@sha256:3429c14149401de2ac82fc72ddc6a92642332b90deb3012301ff211b9d2d0f18"
    path = tmp_path / "installation.json"
    path.write_text(json.dumps(config))
    path.chmod(0o600)
    return path, config


def test_qualification_imports_without_contacting_live_cluster(tmp_path, monkeypatch, capsys):
    path, _ = installed(tmp_path)
    monkeypatch.setattr(module(), "LiveIngressAPI", lambda *a, **kw: pytest.fail("qualification touched cluster"))
    assert module().main(str(path), "qualify") == 0
    assert json.loads(capsys.readouterr().out) == {"status": "tooling_qualified"}


def test_image_copy_intent_survives_new_entry_invocations_without_cluster_or_certificate(tmp_path, monkeypatch, capsys):
    path, config = installed(tmp_path)
    (path.parent / "nebius-ingress").mkdir(mode=0o700)
    for name in ("LiveIngressAPI", "load_installation"):
        monkeypatch.setattr(module(), name, lambda *a, **kw: pytest.fail("image intent touched unrelated authority"))
    assert module().main(str(path), "image-intent") == 0
    first = json.loads(capsys.readouterr().out)
    assert first == {"status": "image_copy_once", "image": config["image"],
                     "installation_id": config["binding"]["installation_id"],
                     "candidate": config["candidate"], "namespace": config["binding"]["namespace"]}
    assert module().main(str(path), "image-intent") == 0
    assert json.loads(capsys.readouterr().out) == {**first, "status": "image_readback_only"}
    # Even an otherwise-valid different registry cannot overwrite retained intent.
    config["image"] = config["image"].replace("/registry/", "/foreign/")
    path.write_text(json.dumps(config))
    assert module().main(str(path), "image-intent") == 1


def test_lost_intent_write_reply_cannot_grant_second_copy(tmp_path, monkeypatch, capsys):
    from scripts.ops import nebius_certificates as state

    path, _ = installed(tmp_path)
    (path.parent / "nebius-ingress").mkdir(mode=0o700)
    write = state._atomic_json
    def lost_reply(*args, **kwargs):
        write(*args, **kwargs)
        raise RuntimeError("lost reply after durable intent")
    monkeypatch.setattr(state, "_atomic_json", lost_reply)
    assert module().main(str(path), "image-intent") == 1
    capsys.readouterr()
    monkeypatch.setattr(state, "_atomic_json", write)
    assert module().main(str(path), "image-intent") == 0
    assert json.loads(capsys.readouterr().out)["status"] == "image_readback_only"


def test_install_connects_bound_certificate_and_candidate_without_authority_overrides(tmp_path, monkeypatch, capsys):
    path, config = installed(tmp_path)
    calls = []
    sentinel = object()

    def api(kubeconfig, **kwargs):
        assert str(kubeconfig) == config["kubeconfig"]
        assert kwargs["candidate"] == "b" * 40
        assert kwargs["binding"].installation_id == config["binding"]["installation_id"]
        assert kwargs["image"] == config["image"]
        calls.append("bound")
        return sentinel

    certificate = {"installation_id": config["binding"]["certificate_installation_id"],
                   "child_domain": "dev.example.test", "management_host": "management.example.test"}
    monkeypatch.setattr(module(), "LiveIngressAPI", api)
    monkeypatch.setattr(module(), "load_installation", lambda p: certificate)

    def install(**kwargs):
        assert kwargs == {"api": sentinel, "state_dir": path.parent / "nebius-ingress/state", "certificate_config": certificate}
        calls.append("installed")
        return {"status": "complete"}

    monkeypatch.setattr(module(), "install_ingress", install)
    assert module().main(str(path), "install") == 0
    assert calls == ["bound", "installed"]
    assert json.loads(capsys.readouterr().out) == {"status": "complete"}


def test_rollback_does_not_require_certificate_availability(tmp_path, monkeypatch):
    path, _ = installed(tmp_path)
    sentinel = object()
    monkeypatch.setattr(module(), "LiveIngressAPI", lambda *a, **kw: sentinel)
    monkeypatch.setattr(module(), "load_installation", lambda p: pytest.fail("recovery consulted certificate"))
    calls = []

    def rollback(**kwargs):
        assert kwargs == {"api": sentinel, "state_dir": path.parent / "nebius-ingress/state"}
        calls.append("restored")
        return {"status": "rolled_back"}

    monkeypatch.setattr(module(), "rollback_ingress", rollback)
    assert module().main(str(path), "rollback") == 0
    assert calls == ["restored"]


@pytest.mark.parametrize("change", ["image", "namespace", "api_server", "unknown_action", "public_file"])
def test_unqualified_authority_rejected_before_constructing_cluster_client(tmp_path, monkeypatch, capsys, change):
    path, config = installed(tmp_path)
    action = "install"
    if change == "image":
        config["image"] = "traefik:latest"
    elif change == "namespace":
        config["binding"]["namespace"] = "default; id"
    elif change == "api_server":
        config["api_server"] = "http://192.0.2.1"
    elif change == "unknown_action":
        action = "delete"
    elif change == "public_file":
        path.chmod(0o644)
    path.write_text(json.dumps(config))
    monkeypatch.setattr(module(), "LiveIngressAPI", lambda *a, **kw: pytest.fail("unqualified client constructed"))
    assert module().main(str(path), action) == 1
    assert "192.0.2.1" not in capsys.readouterr().out


def test_wrong_certificate_binding_fails_without_kubernetes_writes(tmp_path, monkeypatch):
    path, config = installed(tmp_path)
    monkeypatch.setattr(module(), "load_installation", lambda p: {"installation_id": config["binding"]["installation_id"]})
    monkeypatch.setattr(module(), "LiveIngressAPI", lambda *a, **kw: pytest.fail("mismatched certificate admitted"))
    assert module().main(str(path), "install") == 1
