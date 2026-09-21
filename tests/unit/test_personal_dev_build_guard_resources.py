"""Historical build guard migrations remain independently packaged."""

from importlib.resources import files

from alembic.config import Config
from alembic.script import ScriptDirectory


def test_installed_build_guard_resources_have_one_explicit_head():
    scripts = files("capacity_build_guard_migrations")
    config = Config(str(scripts / "alembic.ini"))
    config.set_main_option("script_location", str(scripts))
    assert ScriptDirectory.from_config(config).get_heads() == ["build_guard_0032"]
    assert (scripts / "env.py").is_file()
    assert (scripts / "script.py.mako").is_file()
