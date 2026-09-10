"""Typed inventory retains purpose/provenance; legacy consumers cannot parse it."""

import json
from importlib import import_module

import pytest

from loom_capacity_executor.typed_launch_renderer import render_typed_signed_launch
from loom_capacity_manager.executable_contracts import (
    ExecutableExecutorInventoryV2,
    ExecutionContextV2,
    canonical_executable_bytes,
)
from tests.unit.test_capacity_executor_typed_launch_renderer import typed_context


def typed_inventory(*, purpose="personal-build-worker", pool="oldlab"):
    module = import_module("loom_capacity_manager.typed_inventory_contracts")
    context = typed_context(purpose=purpose, pool=pool)
    proof = render_typed_signed_launch(context).ownership_proof
    binding = proof.metadata.binding
    record = module.ExecutableInventoryRecordV3(
        physical_identity="job-1",
        physical_kind="slurm-job",
        authority_scope="dedicated-loom-association",
        state="active",
        resources=binding.resources,
        node_ids=binding.node_ids,
        controller_evidence_sha256="a" * 64,
        ownership_proof=proof,
    )
    return module.ExecutableExecutorInventoryV3(
        execution=ExecutionContextV2.model_validate(
            binding.execution.model_dump(exclude={"allocation_epoch", "executable"})
        ),
        executor_id=binding.executor_id,
        executor_incarnation=binding.executor_incarnation,
        pool_id=binding.pool_id,
        pool_generation=binding.pool_generation,
        inventory_sequence=1,
        journal_sequence=0,
        journal_digest="0" * 64,
        records=(record,),
    )


@pytest.mark.parametrize("purpose", ("application-worker", "personal-build-worker"))
@pytest.mark.parametrize("pool", ("oldlab", "gb10"))
def test_typed_inventory_round_trip_preserves_exact_provenance(purpose, pool):
    module = import_module("loom_capacity_manager.typed_inventory_contracts")
    value = typed_inventory(purpose=purpose, pool=pool)
    payload = canonical_executable_bytes(value)
    assert module.parse_executor_inventory(payload) == value
    assert value.records[0].ownership_proof.metadata.subject_authority.purpose == purpose
    with pytest.raises(ValueError):
        ExecutableExecutorInventoryV2.model_validate_json(payload)


@pytest.mark.parametrize(
    "tamper",
    (
        "executor",
        "pool",
        "epoch",
        "record-version",
        "proof-version",
        "duplicate",
        "foreign",
        "terminal",
        "checkpoint",
    ),
)
def test_typed_inventory_rejects_mixed_or_inconsistent_evidence(tamper):
    module = import_module("loom_capacity_manager.typed_inventory_contracts")
    value = typed_inventory().model_dump(mode="json")
    if tamper == "executor":
        value["executor_id"] = "other-executor"
    elif tamper == "pool":
        value["pool_generation"] += 1
    elif tamper == "epoch":
        value["execution"]["execution_epoch"] += 1
    elif tamper == "record-version":
        value["records"][0]["schema_version"] = 2
    elif tamper == "proof-version":
        value["records"][0]["ownership_proof"]["schema_version"] = 2
    elif tamper == "duplicate":
        value["records"].append(value["records"][0])
    elif tamper == "foreign":
        value["records"][0]["authority_scope"] = "foreign"
    elif tamper == "terminal":
        value["records"][0]["state"] = "terminal"
    else:
        value["journal_checkpoint_sequence"] = 1
        value["journal_checkpoint_digest"] = "a" * 64
    with pytest.raises(ValueError):
        module.parse_executor_inventory(json.dumps(value).encode())


def test_inventory_parser_keeps_legacy_version_and_rejects_duplicate_version():
    module = import_module("loom_capacity_manager.typed_inventory_contracts")
    typed = typed_inventory()
    legacy = ExecutableExecutorInventoryV2.model_validate_json(
        typed.model_copy(update={"schema_version": 2, "records": ()}).model_dump_json()
    )
    assert module.parse_executor_inventory(canonical_executable_bytes(legacy)) == legacy
    payload = canonical_executable_bytes(typed)
    with pytest.raises(ValueError):
        module.parse_executor_inventory(b'{"schema_version":2,' + payload[1:])


def test_typed_inventory_confirmation_matches_actual_journal(tmp_path):
    from hashlib import sha256

    from loom_capacity_executor.journal import ExecutorJournal

    module = import_module("loom_capacity_manager.typed_inventory_contracts")
    value = typed_inventory()
    payload = canonical_executable_bytes(value)
    with ExecutorJournal(tmp_path / "journal") as journal:
        for event in ("inventory-publish-requested", "inventory-publish-confirmed"):
            journal.append(
                event,
                sha256(payload).hexdigest(),
                object_kind="inventory",
                object_id=str(value.executor_incarnation),
                payload=payload,
            )
        assert module.typed_inventory_confirmation_journal_head(value) == (
            journal.head.sequence,
            journal.head.digest,
        )


async def test_legacy_inventory_store_rejects_typed_contract_before_database_access():
    from loom_capacity_manager.execution_store import CapacityExecutionStore
    from loom_capacity_manager.store import ExecutionConflictError

    with pytest.raises(ExecutionConflictError, match="inventory contract"):
        await CapacityExecutionStore().ingest_executor_inventory(None, typed_inventory())
