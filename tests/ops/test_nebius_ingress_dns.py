"""DNS may target only an already completed, freshly observed ingress cutover."""
from __future__ import annotations

import copy
import json
from pathlib import Path
from uuid import uuid4

import pytest
from scripts.ops import nebius_ingress_operation as operation
from scripts.ops.nebius_ingress_cutover import MARKER
from tests.ops.test_nebius_ingress_gateway import inputs as inputs
from tests.ops.test_nebius_ingress_install import installation as installation
from tests.ops.test_nebius_ingress_operation import inventory as inventory
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


@pytest.fixture
def installed(installation, monkeypatch):
    arguments, tls, _ = installation
    api = arguments["api"]
    api.service["status"]["loadBalancer"]["ingress"] = [{"ip": "8.8.8.8"}]
    operation.install_ingress(**arguments)

    def denied(*args, **kwargs):
        pytest.fail("DNS qualification attempted a Kubernetes/guard write")

    for name in ("patch", "restore", "guard", "create_secret"):
        monkeypatch.setattr(api, name, denied)
    monkeypatch.setattr(api.stage, "create_resource", denied)
    return {key: arguments[key] for key in ("api", "certificate_config", "state_dir")}, tls


def test_read_only_dns_qualification_returns_exact_retained_target(installed):
    arguments, tls = installed
    api = arguments["api"]
    before = copy.deepcopy(api.read())
    target = operation.qualify_dns_target(**arguments)
    assert target == {
        "installation_id": api.binding.installation_id,
        "service_uid": before[0]["metadata"]["uid"], "candidate": api.candidate,
        "address": "8.8.8.8", "zone": arguments["certificate_config"]["zone"],
        "child_domain": api.binding.child_domain, "management_host": api.binding.management_host,
        "fingerprint_sha256": json.loads((Path(arguments["certificate_config"]["state_dir"]) / "selected.json").read_text())["fingerprint_sha256"],
    }
    assert api.read() == before and tls.creates == 1


def test_dns_qualification_accepts_new_protected_application_candidate(installed):
    arguments, _ = installed
    api = arguments["api"]
    api.candidate = "b" * 40
    profile = json.loads(api.config["data"]["profile.json"])
    api.config["data"]["profile.json"] = json.dumps({**profile, "candidate_sha": api.candidate})
    assert operation.qualify_dns_target(**arguments)["candidate"] == "b" * 40


def test_unrelated_service_annotation_does_not_require_dns_ownership(installed):
    arguments, _ = installed
    api = arguments["api"]
    api.service["metadata"]["annotations"]["foreign"] = "retained"
    operation.qualify_dns_target(**arguments)
    assert api.service["metadata"]["annotations"]["foreign"] == "retained"


@pytest.mark.parametrize("drift", ["service_uid", "ip", "selector", "port", "config_uid", "owner", "mode", "candidate", "host"])
def test_dns_qualification_refuses_live_routing_identity_drift(installed, drift):
    arguments, _ = installed
    api = arguments["api"]
    if drift == "service_uid":
        api.service["metadata"]["uid"] = str(uuid4())
    elif drift == "ip":
        api.service["status"]["loadBalancer"]["ingress"] = [{"ip": "1.1.1.1"}]
    elif drift == "selector":
        api.service["spec"]["selector"] = {"app": "foreign"}
    elif drift == "port":
        api.service["spec"]["ports"][0]["targetPort"] = 9443
    elif drift == "config_uid":
        api.config["metadata"]["uid"] = str(uuid4())
    elif drift == "owner":
        api.config["metadata"]["annotations"][MARKER] = str(uuid4())
    elif drift == "candidate":
        api.candidate = "b" * 40
    else:
        environment = json.loads(api.config["data"]["environment.json"])
        environment["shared_ingress_enabled" if drift == "mode" else "public_host"] = False if drift == "mode" else "other.example.test"
        api.config["data"]["environment.json"] = json.dumps(environment)
    with pytest.raises(operation.OperationError):
        operation.qualify_dns_target(**arguments)


@pytest.mark.parametrize("phase", ["release_intent", "configuration_switched", "rolled_back"])
def test_dns_qualification_requires_completed_cutover(installed, phase):
    arguments, _ = installed
    path = arguments["state_dir"] / "cutover/cutover.json"
    journal = json.loads(path.read_text())
    journal["phase"] = phase
    path.write_text(json.dumps(journal))
    with pytest.raises(operation.OperationError):
        operation.qualify_dns_target(**arguments)


def test_dns_qualification_does_not_create_missing_tls_delivery(installed):
    arguments, _ = installed
    root = arguments["certificate_config"]["state_dir"]
    receipt = next((Path(root) / "deliveries").glob("*.json"))
    receipt.unlink()
    with pytest.raises(operation.OperationError):
        operation.qualify_dns_target(**arguments)
    assert not receipt.exists()


@pytest.mark.parametrize("drift", ["cluster", "controller", "tls", "legacy", "public", "during_probe"])
def test_dns_qualification_requires_current_end_to_end_route(installed, monkeypatch, drift):
    arguments, tls = installed
    api = arguments["api"]
    if drift == "cluster":
        monkeypatch.setattr(api, "verify_identity", lambda binding: (_ for _ in ()).throw(RuntimeError("replaced namespace")))
    elif drift == "controller":
        deployment = next(row for row in api.stage.resources.values() if row["kind"] == "Deployment")
        deployment["metadata"]["uid"] = str(uuid4())
    elif drift == "tls":
        monkeypatch.setattr(api, "probe_tls", lambda *args: "0" * 64)
    elif drift == "legacy":
        api.legacy_ok = False
    elif drift == "public":
        api.probe_ok = False
    else:
        def changed(receipt):
            api.service["metadata"]["uid"] = str(uuid4())
        monkeypatch.setattr(api, "probe_public", changed)
    with pytest.raises(operation.OperationError):
        operation.qualify_dns_target(**arguments)
    assert tls.creates == 1


@pytest.mark.parametrize("evidence", ["cutover", "stage", "selection"])
def test_dns_qualification_does_not_recreate_missing_evidence(installed, evidence):
    arguments, _ = installed
    paths = {"cutover": arguments["state_dir"] / "cutover/cutover.json",
             "stage": arguments["state_dir"] / "stage" / (arguments["api"].binding.installation_id + ".json"),
             "selection": Path(arguments["certificate_config"]["state_dir"]) / "selected.json"}
    paths[evidence].unlink()
    with pytest.raises(operation.OperationError):
        operation.qualify_dns_target(**arguments)
    assert not paths[evidence].exists()
