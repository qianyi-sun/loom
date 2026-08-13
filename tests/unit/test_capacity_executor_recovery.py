from __future__ import annotations

import hashlib
import json
from pathlib import Path
from uuid import UUID

import pytest

from loom_capacity_executor.executable import ExecutablePoolExecutor
from loom_capacity_executor.journal import ExecutorJournal, JournalRegressionError
from loom_capacity_executor.launch_renderer import TrustedLaunchContextV2
from tests.unit.test_capacity_executor_executable import (
    _NOW,
    FakeAdmission,
    FakeManager,
    FakeSlurm,
    SimulatedCrash,
    executor_fixture,
    permit_fixture,
)
from tests.unit.test_capacity_executor_launch_renderer import launch_context_fixture


def _restart(
    path: Path,
    manager: FakeManager,
    admission: FakeAdmission,
    slurm: FakeSlurm,
    launch: TrustedLaunchContextV2,
) -> tuple[ExecutablePoolExecutor, ExecutorJournal]:
    journal = ExecutorJournal(path)
    journal.__enter__()
    return (
        ExecutablePoolExecutor(
            manager.registration,
            journal,
            manager,
            admission,
            slurm,
            profile=launch.profile,
            controller_authority=launch.controller_authority,
            ownership_key=launch.ownership_key,
            now=lambda: _NOW,
            bootstrap_digest=lambda _binding: "b" * 64,
        ),
        journal,
    )


def _write_changed_launch_journal(
    path: Path,
    *,
    intent_id: UUID,
    payload: bytes,
    change: str,
) -> None:
    value = json.loads(payload.decode("ascii"))
    if change == "signature":
        signature = value["ownership_proof"]["signature_base64"]
        value["ownership_proof"]["signature_base64"] = (
            "A" if signature[0] != "A" else "B"
        ) + signature[1:]
    elif change == "operation-id":
        value["request"]["operation_id"] = str(UUID(int=999))
    else:
        raise AssertionError(f"unexpected launch change: {change}")
    changed = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("ascii")
    with ExecutorJournal(path) as replacement:
        replacement.append(
            "slurm-submit-unknown",
            hashlib.sha256(changed).hexdigest(),
            object_kind="job",
            object_id=str(intent_id),
            payload=changed,
        )


# Production break caught: treating an interrupted sbatch as safely absent would
# submit the same stable operation twice after process restart.
async def test_crash_after_submit_never_resubmits(tmp_path: Path) -> None:
    launch = launch_context_fixture()
    executor, journal, manager, admission, slurm, launch = executor_fixture(
        tmp_path, work=permit_fixture(launch.binding)
    )
    slurm.crash_after_submit = True
    with pytest.raises(SimulatedCrash):
        await executor.tick()
    journal.close()

    slurm.crash_after_submit = False
    recovered, reopened = _restart(journal.path, manager, admission, slurm, launch)
    result = await recovered.recover()

    assert result.status == "adopted"
    assert slurm.submit_count == 1
    assert admission.bound[launch.binding.intent_id].slurm_job_id == "101"
    assert manager.inventories[-1].records[0].ownership_proof is not None
    reopened.close()


# Production break caught: a confirmed scheduler submission was omitted from
# recovery if the process stopped before protected physical binding completed.
async def test_crash_after_submit_confirmation_replays_physical_binding(
    tmp_path: Path,
) -> None:
    launch = launch_context_fixture()
    executor, journal, manager, admission, slurm, launch = executor_fixture(
        tmp_path,
        work=permit_fixture(launch.binding),
    )
    admission.bind_failure = SimulatedCrash("stopped before physical bind committed")

    with pytest.raises(SimulatedCrash):
        await executor.tick()

    submitted = journal.latest("job", str(launch.binding.intent_id))
    binding = journal.latest("intent", str(launch.binding.intent_id))
    assert submitted is not None and submitted.event_kind == "slurm-submit-confirmed"
    assert binding is not None and binding.event_kind == "physical-bind-requested"
    assert admission.bound == {}
    journal.close()

    admission.bind_failure = None
    recovered, reopened = _restart(journal.path, manager, admission, slurm, launch)
    result = await recovered.recover()

    assert result.status == "adopted"
    assert slurm.submit_count == 1
    assert admission.bind_requests[1] == admission.bind_requests[0]
    assert admission.bound[launch.binding.intent_id].slurm_job_id == "101"
    reopened.close()


# Production break caught: a committed protected physical bind with a lost
# response remained ambiguous forever instead of replaying the idempotent request.
async def test_ambiguous_committed_physical_binding_replays_exact_request(
    tmp_path: Path,
) -> None:
    launch = launch_context_fixture()
    executor, journal, manager, admission, slurm, launch = executor_fixture(
        tmp_path,
        work=permit_fixture(launch.binding),
    )
    admission.bind_commits_before_failure = True
    admission.bind_failure = SimulatedCrash("physical bind committed before response loss")

    with pytest.raises(SimulatedCrash):
        await executor.tick()

    first_receipt = admission.bound[launch.binding.intent_id]
    binding = journal.latest("intent", str(launch.binding.intent_id))
    assert binding is not None and binding.event_kind == "physical-bind-requested"
    journal.close()

    admission.bind_failure = None
    recovered, reopened = _restart(journal.path, manager, admission, slurm, launch)
    result = await recovered.recover()

    assert result.status == "adopted"
    assert slurm.submit_count == 1
    assert admission.bind_requests[1] == admission.bind_requests[0]
    assert admission.bound[launch.binding.intent_id] == first_receipt
    reopened.close()


# Production break caught: journal-chain integrity does not authenticate the
# stored Ed25519 proof, so a well-formed but invalid signature could be adopted.
async def test_recovery_rejects_tampered_ownership_signature(tmp_path: Path) -> None:
    launch = launch_context_fixture()
    executor, journal, manager, admission, slurm, launch = executor_fixture(
        tmp_path,
        work=permit_fixture(launch.binding),
    )
    slurm.crash_after_submit = True
    with pytest.raises(SimulatedCrash):
        await executor.tick()
    stored = journal.latest("job", str(launch.binding.intent_id))
    assert stored is not None and stored.durable_payload() is not None
    payload = stored.durable_payload()
    assert payload is not None
    journal.close()
    changed_path = tmp_path / "tampered-signature.journal"
    _write_changed_launch_journal(
        changed_path,
        intent_id=launch.binding.intent_id,
        payload=payload,
        change="signature",
    )

    slurm.crash_after_submit = False
    recovered, reopened = _restart(changed_path, manager, admission, slurm, launch)
    with pytest.raises(JournalRegressionError, match="ownership"):
        await recovered.recover()

    assert slurm.submit_count == 1
    assert admission.bound == {}
    assert manager.inventories == []
    reopened.close()


# Production break caught: a valid signed proof could be paired with a different
# scheduler request operation identity and still exact-match a pending job.
async def test_recovery_rejects_request_and_proof_identity_mismatch(tmp_path: Path) -> None:
    launch = launch_context_fixture()
    executor, journal, manager, admission, slurm, launch = executor_fixture(
        tmp_path,
        work=permit_fixture(launch.binding),
    )
    slurm.crash_after_submit = True
    with pytest.raises(SimulatedCrash):
        await executor.tick()
    stored = journal.latest("job", str(launch.binding.intent_id))
    assert stored is not None and stored.durable_payload() is not None
    payload = stored.durable_payload()
    assert payload is not None
    journal.close()
    changed_path = tmp_path / "mismatched-request.journal"
    _write_changed_launch_journal(
        changed_path,
        intent_id=launch.binding.intent_id,
        payload=payload,
        change="operation-id",
    )

    slurm.crash_after_submit = False
    recovered, reopened = _restart(changed_path, manager, admission, slurm, launch)
    with pytest.raises(JournalRegressionError, match="ownership"):
        await recovered.recover()

    assert slurm.submit_count == 1
    assert admission.bound == {}
    assert manager.inventories == []
    reopened.close()


# Production break caught: deleting a nonempty local history could make the same
# incarnation appear fresh and allow scheduler mutation below the central high-water.
async def test_empty_replacement_journal_fences(tmp_path: Path) -> None:
    launch = launch_context_fixture()
    executor, journal, manager, admission, slurm, launch = executor_fixture(
        tmp_path, work=permit_fixture(launch.binding)
    )
    await executor.tick()
    manager.work = None
    await executor.tick()
    assert manager.journal_sequence > 0
    journal_path = journal.path
    journal.close()
    journal_path.write_bytes(b"")
    journal_path.chmod(0o600)

    restarted, replacement = _restart(journal_path, manager, admission, slurm, launch)
    before = (slurm.submit_count, tuple(slurm.jobs))
    with pytest.raises(JournalRegressionError):
        await restarted.tick()

    assert (slurm.submit_count, tuple(slurm.jobs)) == before
    replacement.close()


# Production break caught: recovery could adopt an absent, duplicate, foreign, or
# resource-drifted job and incorrectly free or mutate conservatively charged work.
@pytest.mark.parametrize("case", ("absent", "duplicate", "foreign", "resource-mismatch"))
async def test_ambiguous_recovery_stays_quarantined_and_charged(tmp_path: Path, case: str) -> None:
    launch = launch_context_fixture()
    executor, journal, manager, admission, slurm, launch = executor_fixture(
        tmp_path, work=permit_fixture(launch.binding)
    )
    slurm.crash_after_submit = True
    with pytest.raises(SimulatedCrash):
        await executor.tick()
    journal.close()
    exact = slurm.jobs[0]
    if case == "absent":
        slurm.jobs = []
    elif case == "duplicate":
        slurm.jobs.append(exact.model_copy(update={"job_id": "102"}))
    elif case == "foreign":
        slurm.jobs[0] = exact.model_copy(update={"ownership_token": "F" * 43})
    else:
        slurm.jobs[0] = exact.model_copy(update={"cpus": exact.cpus + 1})

    slurm.crash_after_submit = False
    recovered, reopened = _restart(journal.path, manager, admission, slurm, launch)
    result = await recovered.recover()

    assert result.status == "quarantined"
    assert slurm.submit_count == 1
    assert admission.bound == {}
    inventory = manager.inventories[-1]
    assert len(inventory.records) == len(slurm.jobs)
    assert all(
        record.authority_scope == "dedicated-loom-association" for record in inventory.records
    )
    assert all(record.ownership_proof is None for record in inventory.records)
    reopened.close()
