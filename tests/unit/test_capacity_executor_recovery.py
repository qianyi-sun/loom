from __future__ import annotations

import hashlib
import json
from pathlib import Path
from uuid import UUID

import pytest

from loom_capacity_agent.admission import ExecutableWorkerWithdrawalRequestV2
from loom_capacity_executor.executable import ExecutablePoolExecutor, ProtectedIntentObservationV2
from loom_capacity_executor.journal import ExecutorJournal, JournalRegressionError
from loom_capacity_executor.launch_renderer import TrustedLaunchContextV2
from loom_capacity_executor.slurm_contracts import (
    SlurmCancelRequestV2,
    SlurmJobObservationV2,
    SlurmTerminalEvidenceV2,
)
from loom_capacity_manager.executable_contracts import (
    ExecutableIntentCloseV2,
    canonical_executable_bytes,
    canonical_executable_digest,
)
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


def _terminal_from_job(
    job: SlurmJobObservationV2,
    *,
    partition: str | None = None,
) -> SlurmTerminalEvidenceV2:
    return SlurmTerminalEvidenceV2(
        cluster=job.cluster,
        job_id=job.job_id,
        state="COMPLETED",
        submitter=job.submitter,
        account=job.account,
        partition=partition or job.partition,
        submitted_at=_NOW,
        started_at=_NOW,
        ended_at=_NOW,
        elapsed_seconds=0,
        exit_code="0:0",
        cpus=job.cpus,
        memory_bytes=job.memory_bytes,
        gpus=job.gpus,
        generic_tres=job.generic_tres,
        nodes=job.nodes,
        ownership_token=job.ownership_token,
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


# Production break caught: a protected drain request that committed locally but
# crashed before journal confirmation could remain unresolved forever.
async def test_crash_after_protected_drain_request_replays_before_work_fetch(
    tmp_path: Path,
) -> None:
    launch = launch_context_fixture()
    executor, journal, manager, admission, slurm, launch = executor_fixture(
        tmp_path,
        work=permit_fixture(launch.binding),
    )
    await executor.tick()
    slurm.jobs[0] = slurm.jobs[0].model_copy(
        update={"state": "RUNNING", "nodes": launch.binding.node_ids, "pending_reason": None}
    )
    manager.work = ExecutableIntentCloseV2(binding=launch.binding, command_sequence=2)
    admission.observations[launch.binding.intent_id] = ProtectedIntentObservationV2(
        binding=launch.binding,
        bootstrap_registration_epoch=1,
        worker_id=UUID(int=401),
        worker_incarnation=UUID(int=402),
        protected_registration_epoch=2,
        claim_high_water=0,
    )
    admission.crash_after_drain = True

    with pytest.raises(SimulatedCrash):
        await executor.tick()

    requested = journal.latest("intent", str(launch.binding.intent_id))
    assert requested is not None and requested.event_kind == "protected-drain-requested"
    journal.close()

    admission.crash_after_drain = False
    manager.reject_work_fetch = True
    recovered, reopened = _restart(journal.path, manager, admission, slurm, launch)
    result = await recovered.tick()

    assert result.status == "draining"
    assert admission.drain_requests[1] == admission.drain_requests[0]
    confirmed = reopened.latest("intent", str(launch.binding.intent_id))
    assert confirmed is not None and confirmed.event_kind == "protected-drain-confirmed"
    reopened.close()


# Production break caught: a durable pending-cancel request could block the
# journal forever after a crash before scancel; recovery must retry safely from
# the exact scheduler observation and resolve the local request.
async def test_crash_after_pending_cancel_request_retries_exact_cancel_before_work_fetch(
    tmp_path: Path,
) -> None:
    launch = launch_context_fixture()
    executor, journal, manager, admission, slurm, launch = executor_fixture(
        tmp_path,
        work=permit_fixture(launch.binding),
    )
    await executor.tick()
    manager.work = ExecutableIntentCloseV2(binding=launch.binding, command_sequence=2)
    admission.observations[launch.binding.intent_id] = ProtectedIntentObservationV2(
        binding=launch.binding,
        bootstrap_registration_epoch=1,
        worker_id=UUID(int=411),
        worker_incarnation=UUID(int=412),
        protected_registration_epoch=2,
        claim_high_water=0,
    )
    slurm.cancel_failure = SimulatedCrash("process stopped before scancel mutation")

    with pytest.raises(SimulatedCrash):
        await executor.tick()

    job_id = slurm.jobs[0].job_id
    requested = journal.latest("job", job_id)
    assert requested is not None and requested.event_kind == "pending-cancel-requested"
    journal.close()

    slurm.cancel_failure = None
    manager.reject_work_fetch = True
    recovered, reopened = _restart(journal.path, manager, admission, slurm, launch)
    result = await recovered.tick()

    assert result.status == "pending-cancelled"
    assert slurm.cancel_requests[1] == slurm.cancel_requests[0]
    assert slurm.jobs == []
    confirmed = reopened.latest("job", job_id)
    assert confirmed is not None and confirmed.event_kind == "pending-cancel-confirmed-cancelled"
    reopened.close()


# Production break caught: after protected withdrawal confirmed and the process
# stopped before the pending-cancel request, the latest intent record no longer
# contained the physical bind, so repeated close quarantined an owned pending job.
async def test_repeated_close_after_withdraw_confirmed_recovers_physical_binding(
    tmp_path: Path,
) -> None:
    launch = launch_context_fixture()
    executor, journal, manager, admission, slurm, launch = executor_fixture(
        tmp_path,
        work=permit_fixture(launch.binding),
    )
    await executor.tick()
    pending = slurm.jobs[0]
    withdrawal = ExecutableWorkerWithdrawalRequestV2(
        operation_id=UUID(int=901),
        binding=launch.binding,
        bootstrap_registration_epoch=1,
        protected_registration_epoch=2,
        slurm_job_id=pending.job_id,
        ownership_evidence_sha256=admission.bind_requests[0].ownership_evidence_sha256,
    )
    payload = canonical_executable_bytes(withdrawal)
    digest = canonical_executable_digest(withdrawal)
    journal.append(
        "protected-withdraw-requested",
        digest,
        object_kind="intent",
        object_id=str(launch.binding.intent_id),
        payload=payload,
    )
    await admission.withdraw_unregistered_worker(withdrawal)
    journal.append(
        "protected-withdraw-confirmed",
        digest,
        object_kind="intent",
        object_id=str(launch.binding.intent_id),
        payload=payload,
    )
    journal.close()

    close = ExecutableIntentCloseV2(binding=launch.binding, command_sequence=2)
    manager.work = close
    recovered, reopened = _restart(journal.path, manager, admission, slurm, launch)
    result = await recovered.tick()

    assert result.status == "pending-cancelled"
    assert slurm.jobs == []
    assert len(slurm.cancel_requests) == 1
    assert slurm.cancel_requests[0].job_id == pending.job_id
    assert manager.work is None
    confirmed = reopened.latest("job", pending.job_id)
    assert confirmed is not None
    assert confirmed.event_kind == "pending-cancel-confirmed-cancelled"
    reopened.close()


# Production break caught: after drain confirmation, the latest intent record no
# longer exposed the protected physical binding, so repeated close reported
# quarantine instead of the stable drain-only state for a running worker.
async def test_repeated_close_after_drain_confirmed_recovers_running_physical_binding(
    tmp_path: Path,
) -> None:
    launch = launch_context_fixture()
    executor, journal, manager, admission, slurm, launch = executor_fixture(
        tmp_path,
        work=permit_fixture(launch.binding),
    )
    await executor.tick()
    slurm.jobs[0] = slurm.jobs[0].model_copy(
        update={"state": "RUNNING", "nodes": launch.binding.node_ids, "pending_reason": None}
    )
    close = ExecutableIntentCloseV2(binding=launch.binding, command_sequence=2)
    manager.work = close
    admission.observations[launch.binding.intent_id] = ProtectedIntentObservationV2(
        binding=launch.binding,
        bootstrap_registration_epoch=1,
        worker_id=UUID(int=421),
        worker_incarnation=UUID(int=422),
        protected_registration_epoch=2,
        claim_high_water=0,
    )

    first = await executor.tick()
    assert first.status == "draining"
    journal.close()

    recovered, reopened = _restart(journal.path, manager, admission, slurm, launch)
    result = await recovered.tick()

    assert result.status == "draining"
    assert slurm.cancel_requests == []
    assert len(admission.drain_requests) == 1
    assert manager.work == close
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


# Production break caught: recovery of an unknown submission ignored exact
# terminal accounting evidence and quarantined work that had conclusively ended.
async def test_unknown_submission_recovery_adopts_exact_terminal_accounting_match(
    tmp_path: Path,
) -> None:
    launch = launch_context_fixture()
    executor, journal, manager, admission, slurm, launch = executor_fixture(
        tmp_path, work=permit_fixture(launch.binding)
    )
    slurm.crash_after_submit = True
    with pytest.raises(SimulatedCrash):
        await executor.tick()
    submitted = slurm.jobs.pop()
    slurm.terminal_jobs = (
        SlurmTerminalEvidenceV2(
            cluster=submitted.cluster,
            job_id=submitted.job_id,
            state="COMPLETED",
            submitter=submitted.submitter,
            account=submitted.account,
            partition=submitted.partition,
            submitted_at=_NOW,
            started_at=_NOW,
            ended_at=_NOW,
            elapsed_seconds=0,
            exit_code="0:0",
            cpus=submitted.cpus,
            memory_bytes=submitted.memory_bytes,
            gpus=submitted.gpus,
            generic_tres=submitted.generic_tres,
            nodes=launch.binding.node_ids,
            ownership_token=submitted.ownership_token,
        ),
    )
    journal.close()

    slurm.crash_after_submit = False
    recovered, reopened = _restart(journal.path, manager, admission, slurm, launch)
    result = await recovered.recover()

    assert result.status == "adopted"
    assert result.detail == submitted.job_id
    assert slurm.submit_count == 1
    assert admission.bound[launch.binding.intent_id].slurm_job_id == submitted.job_id
    record = manager.inventories[-1].records[0]
    assert record.physical_identity == submitted.job_id
    assert record.state == "terminal"
    assert record.ownership_proof is not None
    reopened.close()


# Production break caught: recovery adopted a live match before checking
# accounting, so contradictory live and terminal evidence was inferred released.
async def test_unknown_submission_recovery_quarantines_exact_live_and_terminal_conflict(
    tmp_path: Path,
) -> None:
    launch = launch_context_fixture()
    executor, journal, manager, admission, slurm, launch = executor_fixture(
        tmp_path, work=permit_fixture(launch.binding)
    )
    slurm.crash_after_submit = True
    with pytest.raises(SimulatedCrash):
        await executor.tick()
    submitted = slurm.jobs[0]
    slurm.terminal_jobs = (_terminal_from_job(submitted),)
    journal.close()

    slurm.crash_after_submit = False
    recovered, reopened = _restart(journal.path, manager, admission, slurm, launch)
    result = await recovered.recover()

    assert result.status == "quarantined"
    assert slurm.submit_count == 1
    assert admission.bound == {}
    reopened.close()


# Production break caught: a changed live record with the same signed ownership
# token was ignored when one exact terminal row existed.
async def test_unknown_submission_recovery_quarantines_changed_live_with_exact_terminal(
    tmp_path: Path,
) -> None:
    launch = launch_context_fixture()
    executor, journal, manager, admission, slurm, launch = executor_fixture(
        tmp_path, work=permit_fixture(launch.binding)
    )
    slurm.crash_after_submit = True
    with pytest.raises(SimulatedCrash):
        await executor.tick()
    submitted = slurm.jobs[0]
    slurm.jobs[0] = submitted.model_copy(update={"cpus": submitted.cpus + 1})
    slurm.terminal_jobs = (_terminal_from_job(submitted),)
    journal.close()

    slurm.crash_after_submit = False
    recovered, reopened = _restart(journal.path, manager, admission, slurm, launch)
    result = await recovered.recover()

    assert result.status == "quarantined"
    assert slurm.submit_count == 1
    assert admission.bound == {}
    reopened.close()


# Production break caught: terminal evidence lacked partition matching, allowing
# a terminal row from a changed partition to be adopted as exact.
async def test_unknown_submission_recovery_rejects_terminal_partition_mismatch(
    tmp_path: Path,
) -> None:
    launch = launch_context_fixture()
    executor, journal, manager, admission, slurm, launch = executor_fixture(
        tmp_path, work=permit_fixture(launch.binding)
    )
    slurm.crash_after_submit = True
    with pytest.raises(SimulatedCrash):
        await executor.tick()
    submitted = slurm.jobs.pop()
    slurm.terminal_jobs = (_terminal_from_job(submitted, partition="loom-debug"),)
    journal.close()

    slurm.crash_after_submit = False
    recovered, reopened = _restart(journal.path, manager, admission, slurm, launch)
    result = await recovered.recover()

    assert result.status == "quarantined"
    assert slurm.submit_count == 1
    assert admission.bound == {}
    reopened.close()


# Production break caught: cancellation replay filtered away same-job live
# conflicts and classified an older accounting row as already terminal.
async def test_pending_cancel_recovery_quarantines_same_job_live_terminal_conflict(
    tmp_path: Path,
) -> None:
    launch = launch_context_fixture()
    executor, journal, manager, admission, slurm, launch = executor_fixture(
        tmp_path, work=permit_fixture(launch.binding)
    )
    await executor.tick()
    submitted = slurm.jobs[0]
    cancel = SlurmCancelRequestV2(
        cluster=submitted.cluster,
        job_id=submitted.job_id,
        submitter=submitted.submitter,
        account=submitted.account,
        partition=submitted.partition,
        cpus=submitted.cpus,
        memory_bytes=submitted.memory_bytes,
        gpus=submitted.gpus,
        generic_tres=submitted.generic_tres,
        nodes=submitted.nodes,
        ownership_token=submitted.ownership_token,
        ownership_evidence_sha256=admission.bind_requests[0].ownership_evidence_sha256,
    )
    payload = json.dumps(
        cancel.model_dump(mode="json"),
        sort_keys=True,
        separators=(",", ":"),
    ).encode("ascii")
    journal.append(
        "pending-cancel-requested",
        hashlib.sha256(payload).hexdigest(),
        object_kind="job",
        object_id=submitted.job_id,
        payload=payload,
    )
    slurm.jobs[0] = submitted.model_copy(update={"cpus": submitted.cpus + 1})
    slurm.terminal_jobs = (_terminal_from_job(submitted),)
    manager.reject_work_fetch = True

    result = await executor.tick()

    assert result.status == "quarantined"
    assert slurm.cancel_requests == []
    retained = journal.latest("job", submitted.job_id)
    assert retained is not None
    assert retained.event_kind == "pending-cancel-ambiguous-quarantined"
    journal.close()
