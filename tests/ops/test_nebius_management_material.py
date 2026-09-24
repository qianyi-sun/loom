"""Management bootstrap credentials survive interrupted create-only delivery."""
from __future__ import annotations

import copy
import json
import os
import ssl
import stat
import subprocess
import sys
import threading
from dataclasses import replace
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from uuid import uuid4

import pytest
from cryptography.hazmat.primitives import serialization


class SecretAPI:
    """In-memory external API: generated material and journal IO remain real."""

    def __init__(self, binding):
        self.binding = binding
        self.secrets = {}
        self.created = []
        self.failure = None

    def verify_identity(self, binding):
        if binding != self.binding:
            raise RuntimeError("private-cluster-identity-diagnostic")

    def get_secret(self, namespace, name):
        assert namespace == self.binding.namespace
        return copy.deepcopy(self.secrets.get(name))

    def create_secret(self, document):
        name = document["metadata"]["name"]
        assert document["metadata"]["namespace"] == self.binding.namespace
        assert name not in self.secrets
        self.created.append(name)
        if self.failure == "before":
            raise OSError("private-request-diagnostic")
        actual = copy.deepcopy(document)
        actual["metadata"].update(uid=str(uuid4()), resourceVersion="1", creationTimestamp="2026-09-24T00:00:00Z")
        self.secrets[name] = actual
        if self.failure == "after":
            raise OSError("private-response-diagnostic")


def delivery():
    from scripts.ops.nebius_management_material import ManagementBinding

    binding = ManagementBinding(str(uuid4()), "loom-nebius-management", str(uuid4()), str(uuid4()))
    return binding, SecretAPI(binding)


def test_replay_retains_original_credentials_and_uids_without_recreation(tmp_path):
    # A regression that regenerates keys or issues another create fails here.
    from scripts.ops.nebius_management_material import deliver_material

    binding, api = delivery()
    state = tmp_path / "material"
    first = deliver_material(binding=binding, api=api, state_dir=state)
    original = copy.deepcopy(api.secrets)
    persisted = (state / "material.json").read_bytes()
    second = deliver_material(binding=binding, api=api, state_dir=state)
    assert first == second
    assert first["status"] == "management_material_delivered"
    assert set(first) == {"status", "installation_id", "namespace", "namespace_uid", "secret_uids"}
    assert first["namespace_uid"] == binding.namespace_uid
    assert set(api.created) == {"loom-platform-db", "loom-management-db-tls", "loom-platform-auth", "loom-admin-secret"}
    assert len(api.created) == 4 and api.secrets == original
    assert (state / "material.json").read_bytes() == persisted
    assert stat.S_IMODE((state / "material.json").stat().st_mode) == 0o600
    assert stat.S_IMODE(state.stat().st_mode) == 0o700
    for name, secret in api.secrets.items():
        assert secret["immutable"] is True
        assert first["secret_uids"][name] == secret["metadata"]["uid"]
        for value in secret["data"].values():
            assert value not in json.dumps(first)


@pytest.mark.parametrize("failure", ["before", "after"])
def test_unknown_create_is_read_back_never_repeated(tmp_path, failure):
    from scripts.ops.nebius_management_material import MaterialError, deliver_material

    binding, api = delivery()
    api.failure = failure
    state = tmp_path / "material"
    if failure == "after":
        deliver_material(binding=binding, api=api, state_dir=state)
        assert len(api.created) == 4
    else:
        with pytest.raises(MaterialError, match="unresolved"):
            deliver_material(binding=binding, api=api, state_dir=state)
        api.failure = None
        with pytest.raises(MaterialError, match="unresolved"):
            deliver_material(binding=binding, api=api, state_dir=state)
        assert len(api.created) == 1 and not api.secrets


def test_lost_journal_after_unknown_create_never_regenerates(tmp_path):
    from scripts.ops.nebius_management_material import MaterialError, deliver_material

    binding, api = delivery()
    api.failure = "before"
    state = tmp_path / "material"
    with pytest.raises(MaterialError, match="unresolved"):
        deliver_material(binding=binding, api=api, state_dir=state)
    # Simulate loss of only the retained material. API absence cannot prove that
    # the first create will not complete later, so this must not reopen creation.
    (state / "material.json").unlink()
    api.failure = None
    with pytest.raises(MaterialError, match="journal"):
        deliver_material(binding=binding, api=api, state_dir=state)
    assert api.created == ["loom-platform-db"] and not api.secrets
    assert not (state / "material.json").exists()


@pytest.mark.parametrize("drift", ["missing", "operation", "digest", "binding"])
def test_missing_or_changed_initialization_evidence_blocks_replay(tmp_path, drift):
    from scripts.ops.nebius_management_material import MaterialError, deliver_material

    binding, api = delivery()
    state = tmp_path / "material"
    deliver_material(binding=binding, api=api, state_dir=state)
    marker = state / "initialized.json"
    assert marker.exists()
    journal = (state / "material.json").read_bytes()
    evidence = json.loads(marker.read_bytes())
    if drift == "missing":
        marker.unlink()
    else:
        if drift == "operation":
            evidence["operation_id"] = str(uuid4())
        elif drift == "digest":
            evidence["material_sha256"] = "f" * 64
        else:
            evidence["binding"]["namespace_uid"] = str(uuid4())
        marker.write_text(json.dumps(evidence))
    with pytest.raises(MaterialError, match="journal"):
        deliver_material(binding=binding, api=api, state_dir=state)
    assert len(api.created) == 4
    assert (state / "material.json").read_bytes() == journal


@pytest.mark.parametrize("name", ["loom-platform-db", "loom-management-db-tls", "loom-platform-auth", "loom-admin-secret"])
def test_any_existing_secret_blocks_all_creation(tmp_path, name):
    from scripts.ops.nebius_management_material import MaterialError, deliver_material

    binding, api = delivery()
    api.secrets[name] = {"foreign": "retained"}
    with pytest.raises(MaterialError, match="refusing adoption"):
        deliver_material(binding=binding, api=api, state_dir=tmp_path / "material")
    assert not api.created and api.secrets == {name: {"foreign": "retained"}}
    assert not (tmp_path / "material/material.json").exists()


@pytest.mark.parametrize("drift", ["uid", "data", "owner", "label", "immutable", "type", "deleting", "missing"])
def test_completed_delivery_rejects_secret_drift_without_replacement(tmp_path, drift):
    from scripts.ops.nebius_management_material import MaterialError, deliver_material

    binding, api = delivery()
    state = tmp_path / "material"
    deliver_material(binding=binding, api=api, state_dir=state)
    original_journal = (state / "material.json").read_bytes()
    secret = api.secrets["loom-platform-auth"]
    if drift == "uid":
        secret["metadata"]["uid"] = str(uuid4())
    elif drift == "data":
        secret["data"]["secret-store-master-key"] = "Zm9yZWlnbg=="
    elif drift == "owner":
        secret["metadata"]["ownerReferences"] = [{"uid": str(uuid4())}]
    elif drift == "label":
        secret["metadata"]["labels"]["loom.nebius/management-installation"] = str(uuid4())
    elif drift == "deleting":
        secret["metadata"]["deletionTimestamp"] = "2026-09-24T00:00:01Z"
    elif drift == "immutable":
        secret["immutable"] = False
    elif drift == "type":
        secret["type"] = "kubernetes.io/service-account-token"
    else:
        del api.secrets["loom-platform-auth"]
    with pytest.raises(MaterialError):
        deliver_material(binding=binding, api=api, state_dir=state)
    assert len(api.created) == 4
    assert (state / "material.json").read_bytes() == original_journal


@pytest.mark.parametrize("drift", ["material", "hash", "status", "resource", "binding", "invalid_json", "mode", "symlink"])
def test_bad_private_state_never_regenerates_credentials(tmp_path, drift):
    from scripts.ops.nebius_management_material import MaterialError, deliver_material

    binding, api = delivery()
    state = tmp_path / "material"
    deliver_material(binding=binding, api=api, state_dir=state)
    before = copy.deepcopy(api.secrets)
    journal = state / "material.json"
    record = json.loads(journal.read_bytes())
    if drift == "material":
        del record["material"]["loom-platform-auth"]
    elif drift == "hash":
        record["material_sha256"] = "f" * 64
    elif drift == "status":
        record["status"] = "unknown"
    elif drift == "resource":
        record["resources"]["loom-platform-db"]["uid"] = None
    elif drift == "binding":
        record["binding"]["namespace_uid"] = str(uuid4())
    journal.write_text(json.dumps(record) if drift != "invalid_json" else "{invalid-private-value")
    if drift == "mode":
        journal.chmod(0o644)
    if drift == "symlink":
        retained = state / "retained.json"
        journal.rename(retained)
        journal.symlink_to(retained)
    with pytest.raises(MaterialError) as error:
        deliver_material(binding=binding, api=api, state_dir=state)
    assert "invalid-private-value" not in str(error.value)
    assert len(api.created) == 4 and api.secrets == before


def test_changed_cluster_or_namespace_binding_does_not_deliver(tmp_path):
    from scripts.ops.nebius_management_material import MaterialError, deliver_material

    binding, api = delivery()
    for field in ("namespace_uid", "kube_system_uid"):
        with pytest.raises(MaterialError) as error:
            deliver_material(binding=replace(binding, **{field: str(uuid4())}), api=api, state_dir=tmp_path / "material")
        assert "private-cluster-identity-diagnostic" not in str(error.value)
    assert not api.created


def test_final_readback_catches_earlier_secret_changed_during_later_create(tmp_path):
    from scripts.ops.nebius_management_material import MaterialError, deliver_material

    binding, api = delivery()
    create = api.create_secret

    def race(document):
        create(document)
        if len(api.created) == 4:
            api.secrets["loom-platform-db"]["metadata"]["uid"] = str(uuid4())

    api.create_secret = race
    with pytest.raises(MaterialError, match="differs"):
        deliver_material(binding=binding, api=api, state_dir=tmp_path / "material")
    assert json.loads((tmp_path / "material/material.json").read_bytes())["status"] == "prepared"


def test_process_death_after_create_intent_cannot_reopen_create(tmp_path):
    from scripts.ops.nebius_management_material import MaterialError, deliver_material

    binding, api = delivery()
    # Real process death, not a catchable exception: the fsynced intent survives.
    program = r'''
import json, os, sys
from pathlib import Path
from scripts.ops.nebius_management_material import ManagementBinding, deliver_material
from tests.ops.test_nebius_management_material import SecretAPI
binding = ManagementBinding(**json.loads(sys.argv[1]))
api = SecretAPI(binding)
api.create_secret = lambda document: os._exit(73)
deliver_material(binding=binding, api=api, state_dir=Path(sys.argv[2]))
'''
    state = tmp_path / "material"
    outcome = subprocess.run([sys.executable, "-c", program, json.dumps(binding.__dict__), str(state)],
                             env=dict(os.environ), capture_output=True, timeout=30)
    assert outcome.returncode == 73
    journal = json.loads((state / "material.json").read_bytes())
    assert sum(row["status"] == "create_intent" for row in journal["resources"].values()) == 1
    with pytest.raises(MaterialError, match="unresolved"):
        deliver_material(binding=binding, api=api, state_dir=state)
    assert not api.created


@pytest.fixture
def https_api(tmp_path, monkeypatch):
    from tests.ops import test_nebius_certificates as certificates

    monkeypatch.setattr(certificates, "NOW", datetime.now(UTC))
    chain, key, roots = certificates.material(names=("localhost",))
    cert, private = tmp_path / "server.crt", tmp_path / "server.key"
    cert.write_bytes(chain)
    private.write_bytes(key)
    private.chmod(0o600)
    trust = ssl.create_default_context(cadata=roots[0].public_bytes(serialization.Encoding.PEM).decode())
    binding, backend = delivery()
    state = {"response": 201, "store": True, "requests": []}

    class Handler(BaseHTTPRequestHandler):
        def respond(self, status, body):
            payload = json.dumps(body).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.send_header("Retry-After", "1")
            self.send_header("Location", "/retry-target")
            self.end_headers()
            self.wfile.write(payload)

        def do_GET(self):
            state["requests"].append(("GET", self.path, self.headers.get("Authorization")))
            ns = self.path.rsplit("/", 1)[-1]
            if ns in ("kube-system", binding.namespace):
                self.respond(200, {"apiVersion": "v1", "kind": "Namespace", "metadata": {
                    "name": ns, "uid": binding.kube_system_uid if ns == "kube-system" else binding.namespace_uid,
                    "labels": {"loom.nebius/management-installation": binding.installation_id}}})
            else:
                secret = backend.secrets.get(ns)
                self.respond(404 if secret is None else 200, secret or {"kind": "Status"})

        def do_POST(self):
            state["requests"].append(("POST", self.path, self.headers.get("Authorization")))
            document = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            if state["store"]:
                backend.create_secret(document)
            self.respond(state["response"], {"private-error": "must-not-leak"})

        def log_message(self, *args):
            pass

    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(cert, private)
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.socket = context.wrap_socket(server.socket, server_side=True)
    worker = threading.Thread(target=server.serve_forever, daemon=True)
    worker.start()
    try:
        yield binding, backend, state, f"https://localhost:{server.server_port}", trust
    finally:
        server.shutdown()
        server.server_close()
        worker.join(timeout=5)


@pytest.mark.parametrize("status", [429, 503, 307])
def test_http_unknown_write_never_retries_or_redirects(https_api, tmp_path, status):
    from scripts.ops.nebius_management_material import HTTPSMaterialAPI, MaterialError, deliver_material

    binding, backend, wire, endpoint, trust = https_api
    wire.update(response=status, store=False)
    with HTTPSMaterialAPI(binding=binding, api_server=endpoint, ssl_context=trust, token="private-token") as api:
        for _ in range(2):
            with pytest.raises(MaterialError, match="unresolved") as error:
                deliver_material(binding=binding, api=api, state_dir=tmp_path / "material")
            assert "must-not-leak" not in str(error.value)
    posts = [row for row in wire["requests"] if row[0] == "POST"]
    assert posts == [("POST", "/api/v1/namespaces/loom-nebius-management/secrets", "Bearer private-token")]
    assert not backend.secrets
    assert not any(row[1] == "/retry-target" for row in wire["requests"])


def test_http_lost_success_is_resolved_only_by_secret_readback(https_api, tmp_path, monkeypatch):
    from scripts.ops.nebius_management_material import HTTPSMaterialAPI, deliver_material

    binding, backend, wire, endpoint, trust = https_api
    wire["response"] = 503
    monkeypatch.setenv("HTTPS_PROXY", "http://127.0.0.1:1")
    with HTTPSMaterialAPI(binding=binding, api_server=endpoint, ssl_context=trust, token="private-token") as api:
        first = deliver_material(binding=binding, api=api, state_dir=tmp_path / "material")
        assert deliver_material(binding=binding, api=api, state_dir=tmp_path / "material") == first
    assert len([row for row in wire["requests"] if row[0] == "POST"]) == 4
    assert {name: row["metadata"]["uid"] for name, row in backend.secrets.items()} == first["secret_uids"]


@pytest.mark.parametrize("change", ["namespace", "name", "owner", "operation", "immutable", "extra", "data", "type"])
def test_transport_cannot_write_outside_fixed_secret_shape(tmp_path, monkeypatch, change):
    from scripts.ops.nebius_management_material import (
        KubectlMaterialAPI,
        MaterialError,
        deliver_material,
    )

    binding, fake = delivery()
    deliver_material(binding=binding, api=fake, state_dir=tmp_path / "material")
    doc = copy.deepcopy(fake.secrets["loom-platform-auth"])
    for field in ("uid", "resourceVersion", "creationTimestamp"):
        del doc["metadata"][field]
    if change == "namespace":
        doc["metadata"]["namespace"] = "foreign"
    elif change == "name":
        doc["metadata"]["name"] = "foreign"
    elif change == "owner":
        doc["metadata"]["labels"]["loom.nebius/management-installation"] = str(uuid4())
    elif change == "operation":
        doc["metadata"]["annotations"]["loom.nebius/management-material-operation"] = "not-a-uuid"
    elif change == "immutable":
        doc["immutable"] = False
    elif change == "extra":
        doc["stringData"] = {"foreign": "private-payload"}
    elif change == "data":
        doc["data"]["secret-store-master-key"] = "not-base64-private-payload"
    else:
        doc["type"] = "kubernetes.io/service-account-token"
    kubeconfig = tmp_path / "config"
    kubeconfig.write_text("private-kubeconfig")
    kubeconfig.chmod(0o600)
    adapter = KubectlMaterialAPI(kubeconfig, binding=binding, executable=Path("/usr/bin/kubectl"),
                                 api_server="https://cluster.example.test")

    def no_command(*args, **kwargs):
        raise AssertionError("Rejected documents must never reach a subprocess")

    monkeypatch.setattr(subprocess, "run", no_command)
    with pytest.raises(MaterialError, match="outside management material scope") as error:
        adapter.create_secret(doc)
    assert "private-payload" not in str(error.value)
