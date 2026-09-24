"""Management Namespace creation and credential delivery preserve recovery identity."""
from __future__ import annotations

import copy
import json
import shutil
import ssl
from contextlib import contextmanager
from dataclasses import replace
from uuid import uuid4

import pytest
import httpx

from tests.ops.test_nebius_management_material import SecretAPI


class BootstrapAPI:
    """Only the external Kubernetes store is doubled; journal and keys are real."""

    def __init__(self, binding):
        self.binding = binding
        self.namespace = None
        self.creates = []
        self.secrets = None
        self.failure = None

    def verify_cluster(self, binding):
        if binding != self.binding:
            raise RuntimeError("private-cluster-diagnostic")

    def get_namespace(self):
        return copy.deepcopy(self.namespace)

    def create_namespace(self, document):
        assert self.namespace is None
        self.creates.append(copy.deepcopy(document))
        if self.failure == "before":
            raise OSError("private-create-diagnostic")
        self.namespace = copy.deepcopy(document)
        self.namespace["metadata"].update(uid=str(uuid4()), resourceVersion="1")
        self.namespace["metadata"]["labels"]["kubernetes.io/metadata.name"] = self.binding.namespace
        self.namespace["spec"] = {"finalizers": ["kubernetes"]}
        self.namespace["status"] = {"phase": "Active"}
        if self.failure == "after":
            raise OSError("private-reply-diagnostic")

    @contextmanager
    def material_api(self, binding):
        assert self.namespace["metadata"]["uid"] == binding.namespace_uid
        if self.secrets is None:
            self.secrets = SecretAPI(binding)
        yield self.secrets


def setup():
    from scripts.ops.nebius_management_bootstrap import BootstrapBinding

    binding = BootstrapBinding(str(uuid4()), "loom-nebius-management", str(uuid4()))
    return binding, BootstrapAPI(binding)


def test_bootstrap_replay_preserves_namespace_and_generated_credentials(tmp_path):
    from scripts.ops.nebius_management_bootstrap import bootstrap_management

    binding, api = setup()
    state = tmp_path / "bootstrap"
    first = bootstrap_management(binding=binding, api=api, state_dir=state)
    material = (state / "material/material.json").read_bytes()
    original = copy.deepcopy(api.secrets.secrets)
    assert bootstrap_management(binding=binding, api=api, state_dir=state) == first
    assert first["status"] == "management_bootstrapped"
    assert first["namespace"] == "loom-nebius-management"
    assert first["namespace_uid"] == api.namespace["metadata"]["uid"]
    assert first["installation_id"] == binding.installation_id
    assert set(first) == {"status", "installation_id", "namespace", "namespace_uid", "secret_uids"}
    assert len(api.creates) == 1 and len(api.secrets.created) == 4
    assert original == api.secrets.secrets
    assert (state / "material/material.json").read_bytes() == material
    assert api.namespace["metadata"]["labels"]["pod-security.kubernetes.io/enforce"] == "restricted"
    for secret in original.values():
        for value in secret["data"].values():
            assert value not in json.dumps(first)


@pytest.mark.parametrize("failure", ["before", "after"])
def test_namespace_unknown_create_is_read_back_never_repeated(tmp_path, failure):
    from scripts.ops.nebius_management_bootstrap import BootstrapError, bootstrap_management

    binding, api = setup()
    api.failure = failure
    if failure == "after":
        first = bootstrap_management(binding=binding, api=api, state_dir=tmp_path / "bootstrap")
        assert bootstrap_management(binding=binding, api=api, state_dir=tmp_path / "bootstrap") == first
    else:
        for _ in range(2):
            with pytest.raises(BootstrapError, match="unresolved"):
                bootstrap_management(binding=binding, api=api, state_dir=tmp_path / "bootstrap")
        assert api.secrets is None
    assert len(api.creates) == 1


def test_untracked_namespace_is_never_adopted(tmp_path):
    from scripts.ops.nebius_management_bootstrap import BootstrapError, bootstrap_management

    binding, api = setup()
    api.namespace = {"metadata": {"name": binding.namespace, "uid": str(uuid4())}}
    with pytest.raises(BootstrapError, match="untracked"):
        bootstrap_management(binding=binding, api=api, state_dir=tmp_path / "bootstrap")
    assert not api.creates and api.secrets is None


@pytest.mark.parametrize("change", ["missing", "uid", "owner", "pss", "deleting", "cluster"])
def test_namespace_or_cluster_drift_prevents_more_credential_writes(tmp_path, change):
    from scripts.ops.nebius_management_bootstrap import BootstrapError, bootstrap_management

    binding, api = setup()
    state = tmp_path / "bootstrap"
    bootstrap_management(binding=binding, api=api, state_dir=state)
    before = (state / "bootstrap.json").read_bytes()
    if change == "missing":
        api.namespace = None
    elif change == "uid":
        api.namespace["metadata"]["uid"] = str(uuid4())
    elif change == "owner":
        api.namespace["metadata"]["labels"]["loom.nebius/management-installation"] = str(uuid4())
    elif change == "pss":
        api.namespace["metadata"]["labels"]["pod-security.kubernetes.io/enforce"] = "privileged"
    elif change == "deleting":
        api.namespace["metadata"]["deletionTimestamp"] = "2026-09-24T10:00:00Z"
    else:
        api.binding = replace(binding, kube_system_uid=str(uuid4()))
    with pytest.raises(BootstrapError):
        bootstrap_management(binding=binding, api=api, state_dir=state)
    assert len(api.creates) == 1 and len(api.secrets.created) == 4
    assert (state / "bootstrap.json").read_bytes() == before


def test_entire_material_directory_loss_cannot_regenerate_keys(tmp_path):
    from scripts.ops.nebius_management_bootstrap import BootstrapError, bootstrap_management

    binding, api = setup()
    state = tmp_path / "bootstrap"
    bootstrap_management(binding=binding, api=api, state_dir=state)
    shutil.rmtree(state / "material")  # Disposable fixture only.
    api.secrets.secrets.clear()  # API absence must not reopen ambiguous creation.
    with pytest.raises(BootstrapError, match="material.*missing"):
        bootstrap_management(binding=binding, api=api, state_dir=state)
    assert len(api.secrets.created) == 4
    assert not (state / "material").exists()


def test_crash_after_outer_material_intent_cannot_restart_generation(tmp_path, monkeypatch):
    from scripts.ops import nebius_management_bootstrap as bootstrap

    binding, api = setup()
    state = tmp_path / "bootstrap"
    deliver = bootstrap.deliver_material

    def crash(**kwargs):
        raise SystemExit(73)

    monkeypatch.setattr(bootstrap, "deliver_material", crash)
    with pytest.raises(SystemExit):
        bootstrap.bootstrap_management(binding=binding, api=api, state_dir=state)
    monkeypatch.setattr(bootstrap, "deliver_material", deliver)
    with pytest.raises(bootstrap.BootstrapError, match="material.*missing"):
        bootstrap.bootstrap_management(binding=binding, api=api, state_dir=state)
    assert api.secrets is not None and not api.secrets.created


@pytest.mark.parametrize("change", ["namespace_uid", "stage", "binding", "invalid_json", "mode", "symlink"])
def test_invalid_outer_journal_preserves_all_resources(tmp_path, change):
    from scripts.ops.nebius_management_bootstrap import BootstrapError, bootstrap_management

    binding, api = setup()
    state = tmp_path / "bootstrap"
    bootstrap_management(binding=binding, api=api, state_dir=state)
    path = state / "bootstrap.json"
    record = json.loads(path.read_bytes())
    if change == "namespace_uid":
        record["namespace_uid"] = str(uuid4())
    elif change == "stage":
        record["stage"] = "namespace_prepared"
    elif change == "binding":
        record["binding"]["installation_id"] = str(uuid4())
    path.write_text(json.dumps(record) if change != "invalid_json" else "{private-secret-diagnostic")
    if change == "mode":
        path.chmod(0o644)
    elif change == "symlink":
        path.rename(state / "retained.json")
        path.symlink_to(state / "retained.json")
    with pytest.raises(BootstrapError) as error:
        bootstrap_management(binding=binding, api=api, state_dir=state)
    assert "private-secret-diagnostic" not in str(error.value)
    assert len(api.creates) == 1 and len(api.secrets.created) == 4


def test_namespace_policy_change_between_secrets_stops_delivery(tmp_path):
    from scripts.ops.nebius_management_bootstrap import BootstrapError, bootstrap_management

    binding, api = setup()
    original_factory = api.material_api

    @contextmanager
    def changing_namespace(material_binding):
        with original_factory(material_binding) as secrets:
            create = secrets.create_secret

            def mutate(document):
                create(document)
                api.namespace["metadata"]["labels"]["pod-security.kubernetes.io/enforce"] = "privileged"

            secrets.create_secret = mutate
            yield secrets

    api.material_api = changing_namespace
    with pytest.raises(BootstrapError):
        bootstrap_management(binding=binding, api=api, state_dir=tmp_path / "bootstrap")
    assert len(api.secrets.created) == 1


@pytest.mark.parametrize("change", ["name", "owner", "pss", "missing_operation", "extra"])
def test_transport_cannot_create_arbitrary_namespace(monkeypatch, change):
    from scripts.ops.nebius_management_bootstrap import BootstrapError, HTTPSBootstrapAPI

    binding, _ = setup()
    document = {
        "apiVersion": "v1", "kind": "Namespace", "metadata": {
            "name": binding.namespace,
            "labels": {"loom.nebius/platform": "true", "pod-security.kubernetes.io/enforce": "restricted",
                       "loom.nebius/management-installation": binding.installation_id},
            "annotations": {"loom.nebius/management-bootstrap-operation": str(uuid4())},
        },
    }
    if change == "name":
        document["metadata"]["name"] = "production"
    elif change == "owner":
        document["metadata"]["labels"]["loom.nebius/management-installation"] = str(uuid4())
    elif change == "pss":
        document["metadata"]["labels"]["pod-security.kubernetes.io/enforce"] = "privileged"
    elif change == "missing_operation":
        document["metadata"]["annotations"] = {}
    else:
        document["metadata"]["finalizers"] = ["foreign"]

    def no_network(*args, **kwargs):
        raise AssertionError("Rejected Namespace must not reach network")

    monkeypatch.setattr(httpx.Client, "send", no_network)
    with HTTPSBootstrapAPI(binding=binding, api_server="https://cluster.example.test",
                           ssl_context=ssl.create_default_context()) as api:
        with pytest.raises(BootstrapError, match="outside"):
            api.create_namespace(document)
