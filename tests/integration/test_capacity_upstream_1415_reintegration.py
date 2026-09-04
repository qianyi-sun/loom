from pathlib import Path

from alembic.config import Config
from alembic.script import ScriptDirectory

REPO_ROOT = Path(__file__).resolve().parents[2]


def _script(package: str) -> ScriptDirectory:
    root = REPO_ROOT / package
    config = Config(str(root / "alembic.ini"))
    config.set_main_option("script_location", str(root))
    return ScriptDirectory.from_config(config)


def test_upstream_1415_union_has_one_exact_capacity_history() -> None:
    script = _script("capacity_migrations")
    assert tuple(script.get_heads()) == ("capacity_0014",)
    assert tuple(
        revision.revision for revision in script.walk_revisions("capacity_0004", "capacity_0014")
    ) == (
        "capacity_0014",
        "capacity_0013",
        "capacity_0012",
        "capacity_0011",
        "capacity_0010",
        "capacity_0009",
        "capacity_0008",
        "capacity_0007",
        "capacity_0006",
        "capacity_0005",
        "capacity_0004",
    )
    official = script.get_revision("capacity_0007")
    prepared_abort = script.get_revision("capacity_0013")
    protected_admission = script.get_revision("capacity_0014")
    assert official is not None
    assert prepared_abort is not None
    assert protected_admission is not None
    assert official.path.endswith("capacity_0007_protected_bootstrap_handshake.py")
    assert prepared_abort.path.endswith("capacity_0013_prepared_abort_evidence.py")
    assert protected_admission.path.endswith("capacity_0014_protected_admission_plan.py")


def test_upstream_1415_union_has_one_exact_guard_history() -> None:
    script = _script("capacity_guard_migrations")
    assert tuple(script.get_heads()) == ("guard_0026",)
    assert tuple(
        revision.revision for revision in script.walk_revisions("guard_0012", "guard_0026")
    ) == (
        "guard_0026",
        "guard_0025",
        "guard_0024",
        "guard_0023",
        "guard_0022",
        "guard_0021",
        "guard_0020",
        "guard_0019",
        "guard_0018",
        "guard_0017",
        "guard_0016",
        "guard_0015",
        "guard_0014",
        "guard_0013",
        "guard_0012",
    )
    handshake = script.get_revision("guard_0012")
    admission = script.get_revision("guard_0013")
    exact_assignment = script.get_revision("guard_0020")
    current_assignment = script.get_revision("guard_0021")
    staging_atomic_submission = script.get_revision("guard_0022")
    staging_worker_session = script.get_revision("guard_0023")
    protected_trial_terminal_closure = script.get_revision("guard_0024")
    protected_trial_retry = script.get_revision("guard_0025")
    protected_trial_requeue = script.get_revision("guard_0026")
    assert handshake is not None
    assert admission is not None
    assert exact_assignment is not None
    assert current_assignment is not None
    assert staging_atomic_submission is not None
    assert staging_worker_session is not None
    assert protected_trial_terminal_closure is not None
    assert protected_trial_retry is not None
    assert protected_trial_requeue is not None
    assert handshake.path.endswith("guard_0012_protected_bootstrap_handshake.py")
    assert admission.path.endswith("guard_0013_executable_admission.py")
    assert exact_assignment.path.endswith("guard_0020_exact_claim_assignment.py")
    assert current_assignment.path.endswith("guard_0021_current_assignment_assertion.py")
    assert staging_atomic_submission.path.endswith("guard_0022_staging_atomic_submission.py")
    assert staging_worker_session.path.endswith("guard_0023_staging_worker_session.py")
    assert protected_trial_terminal_closure.path.endswith(
        "guard_0024_protected_trial_terminal_closure.py"
    )
    assert protected_trial_retry.path.endswith("guard_0025_protected_trial_retry.py")
    assert protected_trial_requeue.path.endswith(
        "guard_0026_protected_trial_requeue.py"
    )
