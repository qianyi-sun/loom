"""Build guard migrations ship with the management runtime, independently."""

from importlib import import_module

from alembic.config import Config
from alembic.script import ScriptDirectory


def test_installed_build_guard_resources_have_one_explicit_head():
    resources = import_module("loom_capacity_build_guard.migration_resources").resolve_build_guard_migration_resources()
    config = Config(str(resources.config))
    config.set_main_option("script_location", str(resources.scripts))
    assert ScriptDirectory.from_config(config).get_heads() == ["build_guard_0012"]
    assert (resources.scripts / "env.py").is_file()
    assert (resources.scripts / "script.py.mako").is_file()
