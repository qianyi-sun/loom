"""Native allocation identity comes from versioned live scheduler facts."""

import json
from copy import deepcopy
from datetime import UTC, datetime
from importlib import import_module

import pytest

from tests.support.fake_slurm import FakeSlurm
from tests.unit.test_capacity_executor_slurm_backend import slurm_launch_request_fixture

_NOW = datetime(2026, 9, 11, 12, tzinfo=UTC)


def _number(value, *, set=True):
    return {"set": set, "infinite": False, "number": value}


def _document(request, uid):
    return {
        "meta": {"plugin": {"data_parser": "v0.0.40"}}, "errors": [], "warnings": [],
        "jobs": [{
            "job_id": 101, "cluster": request.cluster, "job_state": ["RUNNING"], "batch_flag": True,
            "user_name": request.submitter, "user_id": uid, "account": request.account,
            "partition": request.partition, "qos": request.qos, "comment": request.ownership_token,
            "nodes": request.nodes[0], "node_count": _number(1), "cpus": _number(request.cpus),
            "tres_alloc_str": f"cpu={request.cpus},mem=64G,node=1,billing={request.cpus}",
            "submit_time": _number(int(_NOW.timestamp()) - 60),
            "start_time": _number(int(_NOW.timestamp()) - 30),
            "array_job_id": _number(0), "array_task_id": _number(0, set=False),
            "array_task_string": "", "het_job_id": _number(0), "het_job_offset": _number(0),
            "het_job_id_set": "", "requeue": False, "restart_cnt": 0,
        }],
    }


def _fixture(tmp_path):
    fake = FakeSlurm(tmp_path / "slurm")
    request = slurm_launch_request_fixture(fake).model_copy(update={"gpus": 0})
    return fake, request, _document(request, fake.backend().authority.local_uid)


def test_native_observation_binds_live_incarnation_and_exact_allocation(tmp_path):
    module = import_module("loom_capacity_executor.native_slurm_allocation")
    fake, request, raw = _fixture(tmp_path)
    observed = module.parse_native_allocation(json.dumps(raw), request=request, job_id="101",
        expected_uid=fake.backend().authority.local_uid, observed_at=_NOW)
    assert observed.job_id == "101"
    assert observed.submitted_at == datetime.fromtimestamp(int(_NOW.timestamp()) - 60, UTC)
    assert observed.started_at == datetime.fromtimestamp(int(_NOW.timestamp()) - 30, UTC)
    assert observed.observed_at == _NOW
    assert observed.cpus == request.cpus
    assert observed.memory_bytes == request.memory_bytes
    assert observed.hostname == request.nodes[0]
    assert observed.ownership_token == request.ownership_token
    assert observed.requeue is False and observed.restart_count == 0


@pytest.mark.parametrize("boundary", [
    "job", "uid", "user", "account", "cluster", "partition", "qos", "ownership", "node",
    "state", "state-flags", "cpus", "memory", "tres-cpus", "tres-nodes", "gpu", "duplicate-tres",
    "array", "array-task", "array-string", "heterogeneous", "requeue", "restart", "boolean-restart",
    "unset-time", "infinite-time", "future-start", "reversed-times", "boolean-time", "parser",
    "duplicate-job", "error", "warning", "duplicate-json", "oversize", "non-batch",
])
def test_native_observation_rejects_ambiguous_or_foreign_facts(tmp_path, boundary):
    module = import_module("loom_capacity_executor.native_slurm_allocation")
    fake, request, raw = _fixture(tmp_path)
    job = raw["jobs"][0]
    changes = {
        "job": ("job_id", 102), "uid": ("user_id", 99999), "user": ("user_name", "foreign"),
        "account": ("account", "foreign"), "cluster": ("cluster", "foreign"),
        "partition": ("partition", "foreign"), "qos": ("qos", "foreign"),
        "ownership": ("comment", "B" * 43), "node": ("nodes", "foreign"),
        "state": ("job_state", ["PENDING"]), "state-flags": ("job_state", ["RUNNING", "COMPLETING"]),
        "cpus": ("cpus", _number(1)), "memory": ("tres_alloc_str", "cpu=16,mem=128G,node=1"),
        "tres-cpus": ("tres_alloc_str", "cpu=32,mem=64G,node=1"),
        "tres-nodes": ("tres_alloc_str", "cpu=16,mem=64G,node=2"),
        "gpu": ("tres_alloc_str", "cpu=16,mem=64G,node=1,gres/gpu=1"),
        "duplicate-tres": ("tres_alloc_str", "cpu=16,cpu=16,mem=64G,node=1"),
        "array": ("array_job_id", _number(101)), "array-task": ("array_task_id", _number(0)),
        "array-string": ("array_task_string", "1-2"), "heterogeneous": ("het_job_id", _number(101)),
        "requeue": ("requeue", True), "restart": ("restart_cnt", 1), "boolean-restart": ("restart_cnt", False),
        "unset-time": ("start_time", _number(0, set=False)),
        "infinite-time": ("start_time", {"set": True, "infinite": True, "number": 0}),
        "future-start": ("start_time", _number(int(_NOW.timestamp()) + 60)),
        "reversed-times": ("submit_time", _number(int(_NOW.timestamp()) - 1)),
        "boolean-time": ("start_time", _number(True)),
        "non-batch": ("batch_flag", False),
    }
    if boundary in changes:
        key, value = changes[boundary]
        job[key] = value
    elif boundary == "parser":
        raw["meta"]["plugin"]["data_parser"] = "v0.0.999"
    elif boundary == "duplicate-job":
        raw["jobs"].append(deepcopy(job))
    elif boundary in {"error", "warning"}:
        raw[boundary + "s"] = [{"message": "partial readback"}]
    wire = json.dumps(raw)
    if boundary == "duplicate-json":
        wire = wire.replace('"restart_cnt": 0', '"restart_cnt": 1, "restart_cnt": 0')
    elif boundary == "oversize":
        wire += " " * (1024 * 1024)
    with pytest.raises(module.NativeSlurmObservationError):
        module.parse_native_allocation(wire, request=request, job_id="101",
            expected_uid=fake.backend().authority.local_uid, observed_at=_NOW)


async def test_backend_reads_only_exact_versioned_job_through_pinned_commands(tmp_path):
    fake, request, raw = _fixture(tmp_path)
    epoch = int(datetime.now(UTC).timestamp())
    raw["jobs"][0]["submit_time"] = _number(epoch - 60)
    raw["jobs"][0]["start_time"] = _number(epoch - 30)
    # The fake process still exercises real fd-bound execution and authority
    # probes; only its exact scheduler response is a disposable fixture.
    fake._state["native_job_output"] = json.dumps(raw)
    fake._write_state()
    before = datetime.now(UTC)
    observed = await fake.backend().observe_native_allocation(request, job_id="101")
    assert observed.job_id == "101"
    assert before <= observed.observed_at <= datetime.now(UTC)
    assert fake.calls[-1].argv == ("--json=v0.0.40", "--clusters=oldlab", "show", "job", "101")
    assert not fake.sbatch_calls and not fake.scancel_calls


@pytest.mark.parametrize("job_id", ["0", "1_2", "1.batch", "--all", "01"])
async def test_backend_rejects_nonphysical_job_identity_before_reading(tmp_path, job_id):
    fake, request, _ = _fixture(tmp_path)
    with pytest.raises(ValueError):
        await fake.backend().observe_native_allocation(request, job_id=job_id)
    assert not fake.calls


@pytest.mark.parametrize("field,value", [("schema_version", True), ("restart_count", False), ("requeue", 0)])
def test_projected_native_observation_has_no_boolean_integer_aliases(tmp_path, field, value):
    from pydantic import ValidationError

    module = import_module("loom_capacity_executor.native_slurm_allocation")
    fake, request, raw = _fixture(tmp_path)
    observed = module.parse_native_allocation(json.dumps(raw), request=request, job_id="101",
        expected_uid=fake.backend().authority.local_uid, observed_at=_NOW)
    wire = json.dumps(observed.model_dump(mode="json") | {field: value})
    with pytest.raises(ValidationError):
        module.NativeSlurmAllocationV1.model_validate_json(wire)
