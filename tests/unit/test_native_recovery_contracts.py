"""Historical recovery facts need an exact externally retained binding."""

import hashlib
import json
from importlib import import_module
from uuid import uuid4

import pytest

from loom_capacity_manager.contracts import canonical_bytes
from loom_capacity_manager.executable_contracts import canonical_executable_bytes
from tests.unit.test_capacity_executor_bootstrap_handoff import _physical
from tests.unit.test_native_execution_permit import execution_request


def observation():
    module = import_module("loom_capacity_agent.native_recovery")
    physical = _physical(execution_request("oldlab").claim.binding)
    locator = module.NativeInstalledAttemptV1(physical=physical, worker_id=uuid4(),
        worker_incarnation=uuid4(), config_sha256="a" * 64, release_manifest_sha256="b" * 64,
        directory="/scratch/attempt-123", device=8, inode=100)
    prepared = module.NativeRecoveryPreparationV1(locator=locator,
        launch_profile_sha256="c" * 64, node_configuration_sha256="d" * 64,
        node_id=physical.binding.node_ids[0], boot_id=uuid4(), original_uid=24850,
        original_gid=24851, cgroup_path="/system.slice/slurmstepd.scope/job_101",
        cgroup_device=29, cgroup_inode=300, cgroup_mount_id=40)
    record = module.NativeInstalledAttemptV2(preparation=prepared, runtime_spec_sha256="e" * 64,
        uid_map=[{"inside": 0, "outside": 24850, "count": 1}, {"inside": 1, "outside": 100000, "count": 65536}],
        gid_map=[{"inside": 0, "outside": 24851, "count": 1}, {"inside": 1, "outside": 200000, "count": 65536}])
    return module, record


def test_final_record_requires_expected_digest_and_preparation():
    module, record = observation()
    wire = canonical_executable_bytes(record)
    digest = hashlib.sha256(wire).hexdigest()
    assert module.read_final_native_recovery(wire, expected_sha256=digest,
        expected_preparation=record.preparation) == record
    with pytest.raises(ValueError, match="digest"):
        module.read_final_native_recovery(wire, expected_sha256="f" * 64,
            expected_preparation=record.preparation)
    changed = record.preparation.model_copy(update={"boot_id": uuid4()})
    with pytest.raises(ValueError, match="preparation"):
        module.read_final_native_recovery(wire, expected_sha256=digest, expected_preparation=changed)


def test_legacy_locator_remains_readable_but_is_not_final_recovery_evidence():
    module, record = observation()
    wire = canonical_bytes(record.preparation.locator)
    assert module.parse_native_recovery_locator(wire) == record.preparation.locator
    with pytest.raises(ValueError, match="finalized"):
        module.read_final_native_recovery(wire, expected_sha256=hashlib.sha256(wire).hexdigest(),
            expected_preparation=record.preparation)


@pytest.mark.parametrize("fault", ["node", "job", "no-slurm", "nested-job", "path", "uid", "gid", "overlap", "root", "overflow", "boolean", "empty", "extra"])
def test_final_record_rejects_mismatched_or_unsafe_facts(fault):
    module, record = observation()
    document = json.loads(record.model_dump_json())
    if fault == "node":
        document["preparation"]["node_id"] = "foreign-node"
    elif fault == "job":
        document["preparation"]["cgroup_path"] = "/slurm/job_999"
    elif fault == "no-slurm":
        document["preparation"]["cgroup_path"] = "/foreign/job_101"
    elif fault == "nested-job":
        document["preparation"]["cgroup_path"] = "/slurm/job_999/job_101"
    elif fault == "path":
        document["preparation"]["locator"]["directory"] = "/scratch/../foreign"
    elif fault in {"uid", "gid"}:
        document[f"{fault}_map"][0]["outside"] += 1
    elif fault == "overlap":
        document["uid_map"][1]["outside"] = 24850
    elif fault == "root":
        document["gid_map"][1]["outside"] = 0
    elif fault == "overflow":
        document["uid_map"][1]["outside"] = 4294967295
    elif fault == "boolean":
        document["uid_map"][0]["count"] = True
    elif fault == "empty":
        document["gid_map"] = []
    else:
        document["terminal"] = True
    with pytest.raises(ValueError):
        module.parse_native_recovery_locator(json.dumps(document, sort_keys=True, separators=(",", ":")).encode())


def test_matching_hash_does_not_accept_noncanonical_or_oversized_record():
    module, record = observation()
    for wire in (record.model_dump_json(indent=2).encode(), b" " * (128 * 1024 + 1)):
        with pytest.raises(ValueError):
            module.read_final_native_recovery(wire, expected_sha256=hashlib.sha256(wire).hexdigest(),
                expected_preparation=record.preparation)


@pytest.mark.parametrize("field,value", [("launch_profile_sha256", "f" * 64),
    ("node_configuration_sha256", "f" * 64), ("cgroup_inode", 999), ("cgroup_mount_id", 99)])
def test_protected_preparation_drift_is_not_overridden_by_local_digest(field, value):
    module, record = observation()
    wire = canonical_executable_bytes(record)
    expected = record.preparation.model_copy(update={field: value})
    with pytest.raises(ValueError, match="preparation"):
        module.read_final_native_recovery(wire, expected_sha256=hashlib.sha256(wire).hexdigest(),
            expected_preparation=expected)
