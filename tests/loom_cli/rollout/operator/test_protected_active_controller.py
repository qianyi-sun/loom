"""Bind active-controller files to actual prepared contracts without target I/O."""

import hashlib
import json
from dataclasses import replace
from pathlib import Path
from uuid import UUID, uuid4

import pytest

from loom_capacity_executor.launch_renderer import canonical_launch_policy_digest
from loom_capacity_executor.runtime import (
    ActivationRuntimeDocumentV2,
    ApprovedLaunchProfileSetV2,
    canonical_approved_profiles_digest,
)
from loom_cli.capacity_control_plane import (
    render_capacity_pool_executor_active_manifest_sha256,
    render_capacity_pool_executor_configs,
    render_capacity_pool_executor_service_environment,
    render_capacity_pool_inventory_policies,
)
from loom_cli.rollout.operator.protected_active_controller import ActiveControllerRequest
from loom_cli.rollout.operator.protected_capacity_execution_preparation_component import (
    PreparedControllerRequest,
    prepared_executor_profile_sha256,
)
from tests.loom_cli.test_capacity_control_plane import _active_render_fixture
from tests.ops.test_install_capacity_executor import _controller_request


def _request(tmp_path, *, prerequisite=None):
    if prerequisite is None:
        prerequisite = _controller_request(tmp_path)
    fixture_root = tmp_path / "runtime-fixture"
    fixture_root.mkdir()
    profile, _, artifact = _active_render_fixture(fixture_root)
    pool = prerequisite.binding
    operator = artifact.profiles[0].model_copy(
        update={
            "pool_id": pool.pool_id,
            "pool_generation": pool.pool_generation,
            "profile_id": pool.profile_id,
            "profile_generation": pool.profile_generation,
            "profile_digest": pool.profile_digest,
            "slurm_cluster": pool.slurm_cluster,
            "controller_host": pool.controller_host,
            "partition": pool.partition,
            "association": pool.association,
            "submitter": pool.submitter,
            "qos": pool.qos,
        }
    )
    operator = operator.model_copy(
        update={"controller_authority_sha256": canonical_launch_policy_digest(operator)}
    )
    pool = pool.model_copy(
        update={"controller_authority_sha256": operator.controller_authority_sha256}
    )
    prerequisite = replace(prerequisite, binding=pool)
    profile = profile.model_copy(
        update={
            "executor_image": prerequisite.image,
            "pools": tuple(
                pool if value.pool_id == pool.pool_id else value for value in profile.pools
            ),
        }
    )
    profiles = ApprovedLaunchProfileSetV2(profiles=(operator,))
    payload = artifact.model_dump(mode="json")
    for name in (
        "pool_id",
        "pool_generation",
        "executor_id",
        "executor_incarnation",
        "controller_authority_sha256",
        "local_authority_sha256",
        "signing_key_id",
        "signing_key_sha256",
        "state_directory",
        "journal_file",
    ):
        payload[name] = getattr(pool, name)
    payload.update(
        {
            "profiles": [operator.model_dump(mode="json")],
            "approved_profiles_sha256": canonical_approved_profiles_digest(profiles.profiles),
            "immutable_manifest_sha256": render_capacity_pool_executor_active_manifest_sha256(
                profile, pool.pool_id, profiles
            ),
            "handoff_directory": str(Path(pool.state_directory) / "handoff"),
            "admission_directory": str(Path(pool.state_directory) / "admission"),
        }
    )
    payload["slurm_authority"].update(
        {
            "cluster": pool.slurm_cluster,
            "controller_host": pool.controller_host,
            "partition": pool.partition,
            "account": pool.association,
            "submitter": pool.submitter,
            "qos": pool.qos,
            "local_uid": pool.local_uid,
        }
    )
    for name, path in pool.slurm_executables.model_dump().items():
        payload["slurm_authority"]["executables"][name]["path"] = path
    document = ActivationRuntimeDocumentV2.model_validate_json(json.dumps(payload))
    config_path = Path(pool.config_file)
    prepared = PreparedControllerRequest(
        schema_version=1,
        pool_id=pool.pool_id,
        prerequisite=prerequisite,
        transport_authority_sha256=prerequisite.transport_authority_sha256,
        execution=document.execution.model_copy(
            update={
                "execution_state": "prepared",
                "executable_new_capacity_ceiling": 0,
                "executable_new_capacity_rate_per_minute": 0,
            }
        ),
        profile_sha256=prepared_executor_profile_sha256(profile),
        files={
            str(config_path): render_capacity_pool_executor_configs(profile)[pool.pool_id].encode(),
            str(
                config_path.with_name(f"{pool.pool_id}-inventory-policy.json")
            ): render_capacity_pool_inventory_policies(profile)[pool.pool_id].encode(),
            "/etc/loom-capacity-executor/service.env": render_capacity_pool_executor_service_environment(
                profile, pool.pool_id
            ).encode(),
        },
    )
    return ActiveControllerRequest(uuid4(), prepared, profile, document)


def test_active_request_round_trips_and_derives_only_fixed_files(tmp_path):
    request = _request(tmp_path)
    assert ActiveControllerRequest.from_bytes(request.to_bytes()) == request
    assert request.request_sha256 == hashlib.sha256(request.to_bytes()).hexdigest()
    assert set(request.files) == {
        "/etc/loom-capacity-executor/oldlab-active.json",
        "/etc/loom-capacity-executor/oldlab-activation-runtime.json",
        "/etc/loom-capacity-executor/active-service.env",
    }
    assert not set(request.files) & set(request.prepared.files)


@pytest.mark.parametrize(
    "mutation", ["operation", "epoch", "profile", "pool", "handoff", "admission", "prepared-files"]
)
def test_active_request_refuses_changed_preparation_or_runtime_scope(tmp_path, mutation):
    request = _request(tmp_path)
    changes = {}
    if mutation == "operation":
        changes["operation_id"] = UUID(int=0)
    elif mutation == "epoch":
        changes["document"] = request.document.model_copy(
            update={
                "execution": request.document.execution.model_copy(update={"writer_epoch": 999})
            }
        )
    elif mutation == "profile":
        changes["profile"] = request.profile.model_copy(update={"configuration_epoch": 999})
    elif mutation == "pool":
        changes["document"] = request.document.model_copy(update={"pool_id": "gb10"})
    elif mutation in {"handoff", "admission"}:
        changes["document"] = request.document.model_copy(
            update={mutation + "_directory": "/unrelated/private"}
        )
    else:
        files = dict(request.prepared.files)
        files["/etc/loom-capacity-executor/service.env"] = b"CHANGED=1\n"
        changes["prepared"] = replace(request.prepared, files=files)
    with pytest.raises(ValueError):
        replace(request, **changes)


@pytest.mark.parametrize(
    "mutation", ["whitespace", "extra", "schema-bool", "operation-type", "duplicate"]
)
def test_active_request_refuses_noncanonical_wire(tmp_path, mutation):
    request = _request(tmp_path)
    payload = json.loads(request.to_bytes())
    if mutation == "extra":
        payload["files"] = {"/unrelated": "value"}
    elif mutation == "schema-bool":
        payload["schema_version"] = True
    elif mutation == "operation-type":
        payload["operation_id"] = 1
    wire = (json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode()
    if mutation == "whitespace":
        wire += b"\n"
    elif mutation == "duplicate":
        wire = wire.replace(b'{"document":', b'{"schema_version":1,"document":', 1)
    with pytest.raises(ValueError):
        ActiveControllerRequest.from_bytes(wire)
