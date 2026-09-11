from pathlib import Path

from alembic.config import Config
from alembic.script import ScriptDirectory

REPO_ROOT = Path(__file__).resolve().parents[2]


def _capacity_config_without_database() -> Config:
    config = Config(str(REPO_ROOT / "capacity_migrations/alembic.ini"))
    config.set_main_option("script_location", str(REPO_ROOT / "capacity_migrations"))
    return config


def test_upstream_1412_union_has_one_exact_capacity_history() -> None:
    script = ScriptDirectory.from_config(_capacity_config_without_database())
    assert tuple(script.get_heads()) == ("capacity_0023",)
    assert tuple(
        revision.revision for revision in script.walk_revisions("capacity_0004", "capacity_0023")
    ) == (
        "capacity_0023",
        "capacity_0022",
        "capacity_0021",
        "capacity_0020",
        "capacity_0019",
        "capacity_0018",
        "capacity_0017",
        "capacity_0016",
        "capacity_0015",
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
    revision = script.get_revision("capacity_0006")
    assert revision is not None
    assert revision.path.endswith("capacity_0006_executable_work_queue.py")
    protected_admission = script.get_revision("capacity_0014")
    terminal_inventory = script.get_revision("capacity_0015")
    personal_membership = script.get_revision("capacity_0016")
    membership_execution = script.get_revision("capacity_0017")
    assert protected_admission is not None
    assert terminal_inventory is not None
    assert personal_membership is not None
    assert membership_execution is not None
    assert protected_admission.path.endswith("capacity_0014_protected_admission_plan.py")
    assert terminal_inventory.path.endswith("capacity_0015_terminal_inventory_evidence.py")
    assert personal_membership.path.endswith("capacity_0016_personal_membership_events.py")
    assert membership_execution.path.endswith("capacity_0017_personal_membership_execution.py")
