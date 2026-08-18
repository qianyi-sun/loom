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
    revision = script.get_revision("capacity_0006")
    assert revision is not None
    assert revision.path.endswith("capacity_0006_executable_work_queue.py")
    protected_admission = script.get_revision("capacity_0014")
    assert protected_admission is not None
    assert protected_admission.path.endswith("capacity_0014_protected_admission_plan.py")
