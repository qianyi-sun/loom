"""Fixed rendered management phases keep create outcomes and identities durable."""
from __future__ import annotations

import copy
import json
from dataclasses import replace
from uuid import uuid4

import pytest
from tests.unit.test_nebius_management_render import management_inputs as management_inputs
from tests.unit.test_nebius_management_render import render
from tests.unit.test_nebius_platform_render import platform_inputs as platform_inputs


class PhaseAPI:
    """External API double; renderer, state, comparisons and writes remain real."""

    def __init__(self, binding):
        self.binding = binding
        self.resources = {}
        self.creates = []
        self.failure = None
        self.default_change = None

    @staticmethod
    def key(doc):
        return doc["kind"] + ":" + doc["metadata"]["name"]

    def verify_identity(self, binding):
        if binding != self.binding:
            raise RuntimeError("private-identity-detail")

    def get_resource(self, doc):
        return copy.deepcopy(self.resources.get(self.key(doc)))

    def default_resource(self, doc):
        value = copy.deepcopy(doc)
        if self.default_change:
            self.default_change(value)
        return value

    def create_resource(self, doc):
        self.creates.append(self.key(doc))
        if self.failure == "before":
            raise OSError("private-request-detail")
        value = self.default_resource(doc)
        value["metadata"].update(uid=str(uuid4()), resourceVersion="1")
        self.resources[self.key(doc)] = value
        if self.failure == "after":
            raise OSError("private-response-detail")
        if self.failure == "identity":
            self.binding = replace(self.binding, namespace_uid=str(uuid4()))


@pytest.fixture
def inputs(management_inputs):
    from scripts.ops.nebius_management_material import ManagementBinding

    result = render(management_inputs)
    binding = ManagementBinding(management_inputs[0]["installation_id"],
                                "loom-nebius-management", str(uuid4()), str(uuid4()))
    return result, binding, PhaseAPI(binding)


def run(inputs, state):
    from scripts.ops.nebius_management_stage import stage_management_resources

    rendered, binding, api = inputs
    return stage_management_resources(rendered=rendered, phase="10-config-network.yaml",
                                      binding=binding, api=api, state_dir=state)


def test_phase_creates_once_and_replay_keeps_exact_resource_uids(inputs, tmp_path):
    first = run(inputs, tmp_path / "stage")
    saved = copy.deepcopy(inputs[2].resources)
    assert run(inputs, tmp_path / "stage") == first
    assert inputs[2].resources == saved
    assert inputs[2].creates == [
        "ConfigMap:loom-platform-config", "ServiceAccount:loom-platform",
        "NetworkPolicy:default-deny-ingress", "NetworkPolicy:management-api", "NetworkPolicy:postgres-private",
    ]
    assert first["status"] == "management_phase_staged"
    assert first["phase"] == "10-config-network.yaml"
    assert len(first["resource_uids"]) == 5
    assert "installation.json" not in json.dumps(first)
    assert (tmp_path / "stage/stage.json").stat().st_mode & 0o777 == 0o600


@pytest.mark.parametrize("failure", ["before", "after"])
def test_lost_create_response_never_repeats_write(inputs, tmp_path, failure):
    from scripts.ops.nebius_management_stage import ManagementStageError

    inputs[2].failure = failure
    if failure == "after":
        receipt = run(inputs, tmp_path / "stage")
        assert run(inputs, tmp_path / "stage") == receipt
        assert len(inputs[2].creates) == 5
    else:
        for _ in range(2):
            with pytest.raises(ManagementStageError, match="unresolved"):
                run(inputs, tmp_path / "stage")
        assert len(inputs[2].creates) == 1


def test_late_name_collision_blocks_all_phase_writes(inputs, tmp_path):
    from scripts.ops.nebius_management_stage import ManagementStageError

    inputs[2].resources["NetworkPolicy:postgres-private"] = {"metadata": {"uid": str(uuid4())}}
    with pytest.raises(ManagementStageError, match="untracked"):
        run(inputs, tmp_path / "stage")
    assert not inputs[2].creates


@pytest.mark.parametrize("change", ["missing", "uid", "policy", "deleting", "owner"])
def test_recorded_resource_drift_stops_replay_without_replacement(inputs, tmp_path, change):
    from scripts.ops.nebius_management_stage import ManagementStageError

    state = tmp_path / "stage"
    run(inputs, state)
    before = (state / "stage.json").read_bytes()
    item = inputs[2].resources["NetworkPolicy:management-api"]
    if change == "missing":
        del inputs[2].resources["NetworkPolicy:management-api"]
    elif change == "uid":
        item["metadata"]["uid"] = str(uuid4())
    elif change == "policy":
        item["spec"]["ingress"].append({})
    elif change == "deleting":
        item["metadata"]["deletionTimestamp"] = "2026-09-24T16:00:00Z"
    else:
        item["metadata"]["labels"]["loom.nebius/management-installation"] = str(uuid4())
    with pytest.raises(ManagementStageError):
        run(inputs, state)
    assert len(inputs[2].creates) == 5
    assert (state / "stage.json").read_bytes() == before


def test_namespace_replacement_between_writes_blocks_remaining_resources(inputs, tmp_path):
    from scripts.ops.nebius_management_stage import ManagementStageError

    inputs[2].failure = "identity"
    with pytest.raises(ManagementStageError):
        run(inputs, tmp_path / "stage")
    assert len(inputs[2].creates) == 1


def test_defaulting_cannot_broaden_default_deny_policy(inputs, tmp_path):
    from scripts.ops.nebius_management_stage import ManagementStageError

    def mutate(doc):
        if doc["metadata"]["name"] == "default-deny-ingress":
            doc["spec"]["ingress"] = [{}]

    inputs[2].default_change = mutate
    with pytest.raises(ManagementStageError, match="defaulting"):
        run(inputs, tmp_path / "stage")
    assert not inputs[2].creates


@pytest.mark.parametrize("change", ["revision", "phase", "corrupt", "mode", "symlink", "record"])
def test_invalid_journal_never_authorizes_more_writes(inputs, tmp_path, change):
    from scripts.ops.nebius_management_stage import ManagementStageError

    state = tmp_path / "stage"
    run(inputs, state)
    path = state / "stage.json"
    data = json.loads(path.read_bytes())
    if change in {"revision", "phase"}:
        data[change] = "private-corrupt-value"
    elif change == "record":
        next(iter(data["resources"].values()))["uid"] = None
    path.write_text("private-corrupt-value" if change == "corrupt" else json.dumps(data))
    if change == "mode":
        path.chmod(0o644)
    elif change == "symlink":
        path.rename(state / "original.json")
        path.symlink_to(state / "original.json")
    with pytest.raises(ManagementStageError) as error:
        run(inputs, state)
    assert "private-corrupt-value" not in str(error.value)
    assert len(inputs[2].creates) == 5


@pytest.mark.parametrize("phase", ["00-namespaces.yaml", "untrusted.yaml"])
def test_namespace_or_arbitrary_phase_is_not_a_manifest_apply_interface(inputs, tmp_path, phase):
    from scripts.ops.nebius_management_stage import ManagementStageError, stage_management_resources

    rendered, binding, api = inputs
    with pytest.raises(ManagementStageError):
        stage_management_resources(rendered=rendered, phase=phase, binding=binding, api=api, state_dir=tmp_path / "stage")
    assert not api.creates


def test_final_readback_catches_earlier_resource_changed_by_last_create(inputs, tmp_path):
    from scripts.ops.nebius_management_stage import ManagementStageError

    api = inputs[2]
    create = api.create_resource

    def changed(doc):
        create(doc)
        if len(api.creates) == 5:
            api.resources["ConfigMap:loom-platform-config"]["data"]["installation.json"] = "foreign"

    api.create_resource = changed
    with pytest.raises(ManagementStageError):
        run(inputs, tmp_path / "stage")
    assert len(api.creates) == 5
