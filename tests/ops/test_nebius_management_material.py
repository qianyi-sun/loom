"""Management bootstrap credentials survive interrupted create-only delivery."""
from __future__ import annotations

import copy
import json
import stat
from uuid import uuid4

import pytest


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
