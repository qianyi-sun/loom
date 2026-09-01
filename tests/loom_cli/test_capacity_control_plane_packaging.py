"""Installed capacity-control-plane migration resource coverage."""

from __future__ import annotations

import ast
import json
import os
import shutil
import subprocess
import sysconfig
import venv
import zipfile
from pathlib import Path
from uuid import UUID

import pytest
import yaml  # type: ignore[import-untyped]

_REPO_ROOT = Path(__file__).resolve().parents[2]
_CAPACITY_MANAGER_DOCKERFILE = _REPO_ROOT / "deploy/Dockerfile.capacity-manager"
_CAPACITY_MANAGER_SOURCES = _REPO_ROOT / "src/loom_capacity_manager"
_MIGRATION_RESOURCES = {
    "capacity_migrations/__init__.py",
    "capacity_migrations/alembic.ini",
    "capacity_migrations/env.py",
    "capacity_migrations/script.py.mako",
    "capacity_migrations/versions/__init__.py",
    "capacity_migrations/versions/capacity_0001_shadow_management_schema.py",
    "capacity_migrations/versions/capacity_0002_dynamic_development_projection.py",
    "capacity_migrations/versions/capacity_0003_fenced_grant_protocol.py",
    "capacity_migrations/versions/capacity_0004_executable_bridge.py",
    "capacity_migrations/versions/capacity_0005_executable_allocation.py",
    "capacity_migrations/versions/capacity_0006_executable_work_queue.py",
    "capacity_migrations/versions/capacity_0007_protected_bootstrap_handshake.py",
    "capacity_migrations/versions/capacity_0008_executable_bridge_completion.py",
    "capacity_migrations/versions/capacity_0009_inventory_confirmation.py",
    "capacity_migrations/versions/capacity_0010_prepared_retirement_evidence.py",
    "capacity_migrations/versions/capacity_0011_retirement_heartbeat_freshness.py",
    "capacity_migrations/versions/capacity_0012_executable_intent_observed_state_check.py",
    "capacity_migrations/versions/capacity_0013_prepared_abort_evidence.py",
    "capacity_migrations/versions/capacity_0014_protected_admission_plan.py",
}
_PROFILE = _REPO_ROOT / "deploy/dev-fleet/capacity-control-plane.toml"
_MANAGER_IMAGE = "ghcr.io/qianyi-sun/loom-capacity-manager@sha256:" + "a" * 64
_AUTHORITY = UUID("00000000-0000-4000-8000-000000000901")


@pytest.fixture(scope="module")
def built_loom_wheel(tmp_path_factory: pytest.TempPathFactory) -> Path:
    output_directory = tmp_path_factory.mktemp("capacity-wheel")
    source_directory = tmp_path_factory.mktemp("capacity-source") / "loom"
    shutil.copytree(
        _REPO_ROOT,
        source_directory,
        ignore=shutil.ignore_patterns(
            ".git",
            ".hypothesis",
            ".mypy_cache",
            ".pytest_cache",
            ".ruff_cache",
            ".superpowers",
            ".venv",
            ".env",
            "*.egg-info",
            "__pycache__",
            "build",
            "dist",
        ),
    )
    completed = subprocess.run(
        [
            "uv",
            "build",
            "--wheel",
            "--out-dir",
            str(output_directory),
            str(source_directory),
        ],
        cwd=source_directory,
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stderr
    wheels = list(output_directory.glob("loom-*.whl"))
    assert len(wheels) == 1
    return wheels[0]


def test_wheel_contains_complete_capacity_migration_package(
    built_loom_wheel: Path,
) -> None:
    with zipfile.ZipFile(built_loom_wheel) as wheel:
        members = set(wheel.namelist())

    assert _MIGRATION_RESOURCES <= members
    assert not any(member.endswith((".pyc", ".pyo")) for member in members)


def test_capacity_manager_image_installs_the_packaged_migration_tree() -> None:
    dockerfile = _CAPACITY_MANAGER_DOCKERFILE.read_text(encoding="utf-8")

    copy = "COPY capacity_migrations ./capacity_migrations"
    root_install = "pip install --no-cache-dir -e ."
    install_commands = [
        line.strip().removesuffix(" && \\")
        for line in dockerfile.splitlines()
        if line.strip().startswith("pip install ")
    ]
    assert dockerfile.count(copy) == 1
    assert install_commands.count(root_install) == 1
    assert dockerfile.index(copy) < dockerfile.index(root_install)


def test_capacity_manager_sources_do_not_import_unpackaged_loom_modules() -> None:
    unpackaged: list[str] = []
    for source in sorted(_CAPACITY_MANAGER_SOURCES.glob("*.py")):
        for node in ast.walk(ast.parse(source.read_text(encoding="utf-8"))):
            modules: tuple[str, ...]
            if isinstance(node, ast.Import):
                modules = tuple(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module is not None:
                modules = (node.module,)
            else:
                continue
            unpackaged.extend(
                f"{source.relative_to(_REPO_ROOT)}:{module}"
                for module in modules
                if module == "loom"
                or module.startswith("loom.")
                or (module.startswith("loom_") and not module.startswith("loom_capacity_manager"))
            )

    assert unpackaged == []


def test_installed_wheel_renders_capacity_manifests_outside_checkout(
    built_loom_wheel: Path,
    tmp_path: Path,
) -> None:
    environment = tmp_path / "wheel-environment"
    venv.EnvBuilder(with_pip=False).create(environment)
    python = environment / "bin/python"
    purelib_result = subprocess.run(
        [
            str(python),
            "-c",
            "import sysconfig; print(sysconfig.get_paths()['purelib'])",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    installed_purelib = Path(purelib_result.stdout.strip())
    dependency_site = tmp_path / "non-loom-dependencies"
    dependency_site.mkdir()
    for dependency in Path(sysconfig.get_paths()["purelib"]).iterdir():
        if (
            dependency.name.endswith(".pth")
            or "loom" in dependency.name.casefold()
            or dependency.name == "__pycache__"
        ):
            continue
        dependency_site.joinpath(dependency.name).symlink_to(
            dependency,
            target_is_directory=dependency.is_dir(),
        )
    installed_purelib.joinpath("loom-test-dependencies.pth").write_text(
        str(dependency_site) + "\n",
        encoding="utf-8",
    )
    install = subprocess.run(
        [
            "uv",
            "pip",
            "install",
            "--python",
            str(python),
            "--no-deps",
            str(built_loom_wheel),
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert install.returncode == 0, install.stderr

    outside_checkout = tmp_path / "outside-checkout"
    outside_checkout.mkdir()
    profile = outside_checkout / "capacity-control-plane.toml"
    shutil.copyfile(_PROFILE, profile)
    process_environment = os.environ.copy()
    process_environment.pop("PYTHONPATH", None)
    process_environment["PATH"] = f"{environment / 'bin'}:{process_environment['PATH']}"
    process_environment["VIRTUAL_ENV"] = str(environment)
    probe = subprocess.run(
        [
            str(python),
            "-c",
            (
                "import json, capacity_migrations, loom_cli; "
                "from loom_capacity_manager.migration_resources import "
                "resolve_capacity_migration_resources; "
                "print(json.dumps({"
                "'capacity_package': capacity_migrations.__file__, "
                "'loom_cli': loom_cli.__file__, "
                "'migration_config': str("
                "resolve_capacity_migration_resources().config)}))"
            ),
        ],
        cwd=outside_checkout,
        env=process_environment,
        check=False,
        capture_output=True,
        text=True,
    )
    assert probe.returncode == 0, probe.stderr
    loaded_paths = [Path(value).resolve() for value in json.loads(probe.stdout).values()]
    assert all(path.is_relative_to(installed_purelib) for path in loaded_paths)
    assert not any(path.is_relative_to(_REPO_ROOT) for path in loaded_paths)
    completed = subprocess.run(
        [
            str(environment / "bin/loom"),
            "admin",
            "capacity-control-plane",
            "render",
            "--file",
            str(profile),
            "--manager-image",
            _MANAGER_IMAGE,
            "--authority-incarnation",
            str(_AUTHORITY),
        ],
        cwd=outside_checkout,
        env=process_environment,
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr
    documents = [document for document in yaml.safe_load_all(completed.stdout) if document]
    namespace = next(
        document
        for document in documents
        if document["kind"] == "Namespace" and document["metadata"]["name"] == "loom-dev"
    )
    postgres_service = next(
        document
        for document in documents
        if document["kind"] == "Service"
        and document["metadata"]["name"] == "loom-capacity-postgres"
    )
    postgres_statefulset = next(
        document
        for document in documents
        if document["kind"] == "StatefulSet"
        and document["metadata"]["name"] == "loom-capacity-postgres"
    )
    migration_jobs = [document for document in documents if document["kind"] == "Job"]
    assert namespace["metadata"]["name"] == "loom-dev"
    assert postgres_service["metadata"]["name"] == "loom-capacity-postgres"
    assert postgres_statefulset["metadata"]["name"] == "loom-capacity-postgres"
    assert len(migration_jobs) == 1
    assert migration_jobs[0]["metadata"]["name"].startswith("loom-capacity-migrate-capacity-0014-")
