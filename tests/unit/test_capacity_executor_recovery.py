from __future__ import annotations

from pathlib import Path

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
