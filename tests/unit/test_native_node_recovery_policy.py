"""Node recovery narrows authenticated history to retained protected local scope."""

from importlib import import_module
from uuid import uuid4

import pytest

from loom_capacity_agent.native_recovery_publication import NativeRecoveryHostIdentityV1
from tests.unit.test_native_recovery_contracts import observation


def scope_values():
    _, final = observation()
    host = NativeRecoveryHostIdentityV1(node_id=final.preparation.node_id,
        boot_id=final.preparation.boot_id, original_uid=24850, original_gid=24851,
        cgroup_namespace_device=4, cgroup_namespace_inode=100)
    return dict(installation_id=uuid4(), pool_id="oldlab", profile_sha256="a" * 64,
        host=host, scratch_root="/var/lib/loom-scratch/owner", quarantine_root="/var/lib/loom-scratch/recovery",
        scratch_device=8, scratch_mount_id=30, filesystem="ext4", uid_map=final.uid_map, gid_map=final.gid_map)


@pytest.mark.parametrize("fault", ["exact", "root", "nested", "relative", "remote", "root-map", "overlap", "inside-overlap"])
def test_node_scope_requires_local_disjoint_roots_and_exact_retained_maps(fault):
    module = import_module("loom_capacity_executor.native_node_recovery_policy")
    values = scope_values()
    if fault == "root":
        values["scratch_root"] = "/"
    elif fault == "nested":
        values["quarantine_root"] = values["scratch_root"] + "/cleanup"
    elif fault == "relative":
        values["scratch_root"] = "relative"
    elif fault == "remote":
        values["filesystem"] = "nfs4"
    elif fault in {"root-map", "overlap", "inside-overlap"}:
        ranges = list(values["uid_map"])
        index = 0 if fault == "root-map" else 1
        changes = {"outside": 0 if fault == "root-map" else 24850} if fault != "inside-overlap" else {"inside": 0}
        ranges[index] = ranges[index].model_copy(update=changes)
        values["uid_map"] = tuple(ranges)
    if fault == "exact":
        scope = module.NativeNodeRecoveryScopeV1(**values)
        assert scope.scratch_root == values["scratch_root"]
    else:
        with pytest.raises(ValueError):
            module.NativeNodeRecoveryScopeV1(**values)


@pytest.mark.parametrize("fault", ["exact", "duplicate", "principal-original", "principal-mapped"])
def test_node_policy_reserves_a_separate_management_identity(fault):
    module = import_module("loom_capacity_executor.native_node_recovery_policy")
    scope = module.NativeNodeRecoveryScopeV1(**scope_values())
    values = dict(management_uid=25000, scopes=(scope,))
    if fault == "duplicate":
        values["scopes"] = (scope, scope)
    elif fault == "principal-original":
        values["management_uid"] = 24850
    elif fault == "principal-mapped":
        values["management_uid"] = 100001
    if fault == "exact":
        assert module.NativeNodeRecoveryPolicyV1(**values).scopes == (scope,)
    else:
        with pytest.raises(ValueError):
            module.NativeNodeRecoveryPolicyV1(**values)
