from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import textwrap
from pathlib import Path
from typing import Any

import pytest
import yaml  # type: ignore[import-untyped]

from loom.personal_dev_control_plane_config import (
    load_personal_dev_control_plane_profile,
    load_personal_dev_trusted_release,
)
from loom.personal_dev_control_plane_render import render_shadow_personal_dev_control_plane
from loom.personal_dev_schema_transition import (
    PersonalDevSchemaTransitionError,
    prepare_personal_dev_schema_transition,
)
from tests.unit.test_personal_dev_control_plane_render import _release_value

_ROOT = Path(__file__).resolve().parents[2]
_PROFILE = _ROOT / "deploy/dev-fleet/personal-dev-control-plane.toml"


def _write_json(path: Path, value: object) -> str:
    payload = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")
    path.write_bytes(payload)
    path.chmod(0o600)
    return hashlib.sha256(payload).hexdigest()


def _predecessor_profile(path: Path, *, includes_web: bool) -> Path:
    profile = _PROFILE.read_text(encoding="utf-8").replace(
        "schema_version = 3\n",
        f"schema_version = {2 if includes_web else 1}\n",
        1,
    )
    profile = profile.replace("personal_dev_native_builder_enabled = false\n", "", 1)
    profile = profile.replace(
        'native_builder_public_secret = "loom-personal-dev-native-builder-public"\n',
        "",
        1,
    )
    profile = re.sub(
        r"\n\[native_builder\]\n.*?(?=\n\[network\]\n)",
        "\n",
        profile,
        count=1,
        flags=re.DOTALL,
    )
    if not includes_web:
        profile = re.sub(
            r"\n\[resources\.web\]\n(?:[^\n]*\n){4}",
            "\n",
            profile,
            count=1,
        )
    path.write_text(profile, encoding="utf-8")
    return path


def _write_source_validation_checkout(path: Path) -> tuple[str, str]:
    path.mkdir()
    shutil.copytree(_ROOT / "src", path / "src")
    shutil.copytree(_ROOT / "migrations", path / "migrations")
    (path / ".gitignore").write_text("__pycache__/\n*.pyc\n", encoding="ascii")
    subprocess.run(["/usr/bin/git", "init", "-q", str(path)], check=True)
    subprocess.run(
        [
            "/usr/bin/git",
            "-C",
            str(path),
            "add",
            ".gitignore",
            "src",
            "migrations",
        ],
        check=True,
    )
    subprocess.run(
        [
            "/usr/bin/git",
            "-C",
            str(path),
            "-c",
            "user.name=Loom tests",
            "-c",
            "user.email=loom-tests@example.invalid",
            "commit",
            "-qm",
            "source validation fixture",
        ],
        check=True,
    )
    commit = subprocess.run(
        ["/usr/bin/git", "-C", str(path), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    tree = subprocess.run(
        ["/usr/bin/git", "-C", str(path), "rev-parse", "HEAD^{tree}"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    return commit, tree


def _run_source_validation(
    checkout: Path,
    *,
    commit: str,
    tree: str,
    environment: dict[str, str] | None = None,
    expect_rejection: bool,
) -> subprocess.CompletedProcess[str]:
    program = textwrap.dedent(
        f"""
        from pathlib import Path
        from types import SimpleNamespace

        from loom.personal_dev_schema_transition import (
            PersonalDevSchemaTransitionError,
            validate_personal_dev_schema_transition_source_root,
        )

        root = Path({str(checkout)!r})
        release = SimpleNamespace(source_sha={commit!r}, source_tree={tree!r})
        rejected = False
        try:
            validate_personal_dev_schema_transition_source_root(
                root,
                release=release,
                alembic_ini_path=root / "migrations" / "alembic.ini",
            )
        except PersonalDevSchemaTransitionError:
            rejected = True
        if rejected is not {expect_rejection!r}:
            raise SystemExit(f"unexpected rejection result: {{rejected}}")
        """
    )
    child_environment = os.environ.copy()
    child_environment["PYTHONPATH"] = str(checkout / "src")
    if environment is not None:
        child_environment.update(environment)
    return subprocess.run(
        [sys.executable, "-c", program],
        check=False,
        capture_output=True,
        text=True,
        env=child_environment,
    )


def _transition_inputs(
    tmp_path: Path,
    *,
    predecessor_includes_web: bool = False,
) -> dict[str, Any]:
    current_value = _release_value()
    current_release_path = tmp_path / "current-release.json"
    current_release_sha256 = _write_json(current_release_path, current_value)
    current_release = load_personal_dev_trusted_release(
        current_release_path,
        current_release_sha256,
    )
    profile = load_personal_dev_control_plane_profile(_PROFILE)

    predecessor_value = _release_value()
    predecessor_value["source_sha"] = "9" * 40
    predecessor_value["source_tree"] = "8" * 40
    predecessor_value["images"]["loom_service"] = (
        "ghcr.io/qianyi-sun/loom-service@sha256:" + "d" * 64
    )
    predecessor_value["schema_version"] = 3 if predecessor_includes_web else 2
    del predecessor_value["images"]["personal_dev_native_builder_agent"]
    if not predecessor_includes_web:
        del predecessor_value["images"]["loom_web"]
    predecessor_release_path = tmp_path / "predecessor-release.json"
    predecessor_release_sha256 = _write_json(
        predecessor_release_path,
        predecessor_value,
    )
    predecessor_release = load_personal_dev_trusted_release(
        predecessor_release_path,
        predecessor_release_sha256,
    )
    predecessor_profile = load_personal_dev_control_plane_profile(
        _predecessor_profile(
            tmp_path / "predecessor-profile.toml",
            includes_web=predecessor_includes_web,
        )
    )
    predecessor_shadow = render_shadow_personal_dev_control_plane(
        predecessor_profile,
        predecessor_release,
    )
    predecessor_shadow_path = tmp_path / "predecessor-shadow.yaml"
    predecessor_shadow_path.write_text(predecessor_shadow.yaml_text, encoding="utf-8")
    predecessor_shadow_path.chmod(0o600)
    predecessor_shadow_sha256 = hashlib.sha256(
        predecessor_shadow.yaml_text.encode("utf-8")
    ).hexdigest()

    postgres_dump_path = tmp_path / "postgres.dump"
    postgres_dump_path.write_bytes(b"opaque-postgres-dump")
    postgres_dump_path.chmod(0o600)
    postgres_source_state_path = tmp_path / "postgres.source-state.tsv"
    postgres_source_state_path.write_bytes(b"table\tpublic.example\t0\t" + b"a" * 64 + b"\n")
    postgres_source_state_path.chmod(0o600)
    backup_value = {
        "cleanup": {
            "isolated_minio_absent": True,
            "isolated_network_absent": True,
            "isolated_postgres_absent": True,
        },
        "completed_at": "2026-08-28T04:31:25Z",
        "manager": {
            "executable_new_capacity_ceiling": 0,
            "personal_worker_count": 0,
        },
        "minio": {
            "backup_manifest_sha256": "b" * 64,
            "image": predecessor_release.images.minio,
            "restored_manifest_sha256": "b" * 64,
            "restored_object_count": 0,
            "source_object_count": 0,
        },
        "namespace": "loom-dev",
        "postgres": {
            "dump_sha256": hashlib.sha256(postgres_dump_path.read_bytes()).hexdigest(),
            "image": predecessor_release.images.postgres,
            "restored_schema_head": "0112",
            "restored_state_sha256": hashlib.sha256(
                postgres_source_state_path.read_bytes()
            ).hexdigest(),
            "source_schema_head": "0112",
            "source_state_sha256": hashlib.sha256(
                postgres_source_state_path.read_bytes()
            ).hexdigest(),
        },
        "release_sha256": predecessor_release_sha256,
        "schema": "loom-personal-dev-backup-restore-evidence-v1",
        "secrets": {"key_inventory_sha256": "c" * 64, "values_included": False},
        "source": {
            "commit": predecessor_release.source_sha,
            "tree": predecessor_release.source_tree,
        },
        "started_at": "2026-08-28T04:30:18Z",
        "storage": {
            "minio_pvc": "data-loom-dev-minio-0",
            "postgres_pvc": "data-loom-dev-postgres-0",
            "storage_class": "longhorn",
        },
    }
    backup_evidence_path = tmp_path / "backup-evidence.json"
    backup_evidence_sha256 = _write_json(backup_evidence_path, backup_value)
    return {
        "profile": profile,
        "current_release": current_release,
        "current_release_sha256": current_release_sha256,
        "predecessor_release": predecessor_release,
        "predecessor_release_sha256": predecessor_release_sha256,
        "backup_evidence_path": backup_evidence_path,
        "backup_evidence_sha256": backup_evidence_sha256,
        "postgres_dump_path": postgres_dump_path,
        "postgres_source_state_path": postgres_source_state_path,
        "predecessor_shadow_path": predecessor_shadow_path,
        "predecessor_shadow_sha256": predecessor_shadow_sha256,
        "alembic_ini_path": _ROOT / "migrations/alembic.ini",
        "expected_predecessor_head": "0112",
        "expected_target_head": "0129",
    }


def _write_alembic_graph(
    root: Path,
    revisions: dict[str, str | tuple[str, ...] | None],
    *,
    dependencies: dict[str, str | tuple[str, ...] | None] | None = None,
    branch_labels: dict[str, str | tuple[str, ...] | None] | None = None,
) -> Path:
    versions = root / "versions"
    versions.mkdir(parents=True)
    config = root / "alembic.ini"
    config.write_text(
        "[alembic]\nscript_location = %(here)s\n",
        encoding="utf-8",
    )
    for revision, down_revision in revisions.items():
        (versions / f"{revision}.py").write_text(
            textwrap.dedent(
                f"""
                revision = {revision!r}
                down_revision = {down_revision!r}
                branch_labels = {(branch_labels or {}).get(revision)!r}
                depends_on = {(dependencies or {}).get(revision)!r}
                """
            ),
            encoding="utf-8",
        )
    return config


def test_transition_preparation_binds_backup_graph_and_exact_migration_job(
    tmp_path: Path,
) -> None:
    inputs = _transition_inputs(tmp_path)

    prepared = prepare_personal_dev_schema_transition(**inputs)

    plan = prepared.plan
    assert plan["schema"] == "loom-personal-dev-schema-transition-plan-v1"
    assert plan["namespace"] == "loom-dev"
    assert plan["capacity"]["executable_new_capacity_ceiling"] == 0
    assert plan["predecessor"]["schema_head"] == "0112"
    assert plan["target"]["schema_head"] == "0129"
    predecessor_documents = list(
        yaml.safe_load_all(inputs["predecessor_shadow_path"].read_text(encoding="utf-8"))
    )
    predecessor_migration_name = next(
        item["metadata"]["name"]
        for item in predecessor_documents
        if item.get("kind") == "Job"
        and item.get("metadata", {}).get("labels", {}).get("app") == "loom-personal-dev-migration"
    )
    assert plan["predecessor"]["migration_job_name"] == predecessor_migration_name
    assert plan["migration"]["job_name"] != predecessor_migration_name
    assert plan["rollback"]["delete_after_predecessor_apply"] == [
        "deployment.apps/loom-personal-dev-web",
        "networkpolicy.networking.k8s.io/loom-personal-dev-web-ingress",
        "service/loom-personal-dev-web",
    ]
    assert plan["migration"]["revisions"] == [
        "0113",
        "0114",
        "0115",
        "0116",
        "0117",
        "0118",
        "0119",
        "0120",
        "0121",
        "0122",
        "0123",
        "0124",
        "0125",
        "0126",
        "0127",
        "0128",
        "0129",
    ]
    assert (
        hashlib.sha256(prepared.migration_job_json).hexdigest() == plan["migration"]["job_sha256"]
    )
    migration_job = json.loads(prepared.migration_job_json)
    assert migration_job["kind"] == "Job"
    assert migration_job["metadata"]["namespace"] == "loom-dev"
    assert migration_job["spec"]["template"]["spec"]["containers"][0]["image"] == (
        inputs["current_release"].images.loom_service
    )


def test_transition_preparation_preserves_web_owned_by_predecessor(
    tmp_path: Path,
) -> None:
    inputs = _transition_inputs(tmp_path, predecessor_includes_web=True)

    prepared = prepare_personal_dev_schema_transition(**inputs)

    assert prepared.plan["rollback"]["delete_after_predecessor_apply"] == []


@pytest.mark.parametrize(
    "mutation",
    (
        "dump-drift",
        "state-drift",
        "backup-drift",
        "predecessor-shadow-drift",
        "predecessor-release-drift",
        "dump-mode",
        "dump-hardlink",
        "state-mode",
        "shadow-mode",
        "alembic-symlink",
        "same-head",
    ),
)
def test_transition_preparation_fails_closed_on_unbound_input(
    tmp_path: Path,
    mutation: str,
) -> None:
    inputs = _transition_inputs(tmp_path)
    if mutation == "dump-drift":
        inputs["postgres_dump_path"].write_bytes(b"changed")
    elif mutation == "state-drift":
        inputs["postgres_source_state_path"].write_bytes(b"changed")
    elif mutation == "backup-drift":
        inputs["backup_evidence_sha256"] = "f" * 64
    elif mutation == "predecessor-shadow-drift":
        inputs["predecessor_shadow_path"].write_text("changed", encoding="utf-8")
    elif mutation == "predecessor-release-drift":
        inputs["predecessor_release_sha256"] = "f" * 64
    elif mutation == "dump-mode":
        inputs["postgres_dump_path"].chmod(0o640)
    elif mutation == "dump-hardlink":
        os.link(inputs["postgres_dump_path"], tmp_path / "dump-hardlink")
    elif mutation == "state-mode":
        inputs["postgres_source_state_path"].chmod(0o640)
    elif mutation == "shadow-mode":
        inputs["predecessor_shadow_path"].chmod(0o640)
    elif mutation == "alembic-symlink":
        linked_config = tmp_path / "linked-alembic.ini"
        linked_config.symlink_to(inputs["alembic_ini_path"])
        inputs["alembic_ini_path"] = linked_config
    elif mutation == "same-head":
        backup = json.loads(inputs["backup_evidence_path"].read_text(encoding="ascii"))
        backup["postgres"]["source_schema_head"] = "0121"
        backup["postgres"]["restored_schema_head"] = "0121"
        inputs["backup_evidence_sha256"] = _write_json(
            inputs["backup_evidence_path"],
            backup,
        )
    else:
        raise AssertionError(mutation)

    with pytest.raises(PersonalDevSchemaTransitionError):
        prepare_personal_dev_schema_transition(**inputs)


def test_transition_preparation_rejects_branched_path_to_sole_merge_head(
    tmp_path: Path,
) -> None:
    inputs = _transition_inputs(tmp_path)
    inputs["alembic_ini_path"] = _write_alembic_graph(
        tmp_path / "branched-migrations",
        {
            "0112": None,
            "0113": "0112",
            "0114a": "0113",
            "0114b": "0113",
            "0121": ("0114a", "0114b"),
        },
    )

    with pytest.raises(PersonalDevSchemaTransitionError):
        prepare_personal_dev_schema_transition(**inputs)


@pytest.mark.parametrize("edge_kind", ("dependency", "branch-label"))
def test_transition_preparation_rejects_non_linear_alembic_edges(
    tmp_path: Path,
    edge_kind: str,
) -> None:
    inputs = _transition_inputs(tmp_path)
    arguments: dict[str, object] = {}
    if edge_kind == "dependency":
        arguments["dependencies"] = {"0121": "0112"}
    elif edge_kind == "branch-label":
        arguments["branch_labels"] = {"0113": "forward-branch"}
    else:
        raise AssertionError(edge_kind)
    inputs["alembic_ini_path"] = _write_alembic_graph(
        tmp_path / "non-linear-migrations",
        {"0112": None, "0113": "0112", "0121": "0113"},
        **arguments,
    )

    with pytest.raises(PersonalDevSchemaTransitionError):
        prepare_personal_dev_schema_transition(**inputs)


@pytest.mark.parametrize(
    ("field", "value"),
    (("expected_predecessor_head", "0111"), ("expected_target_head", "0121")),
)
def test_transition_preparation_requires_the_reviewed_schema_boundary(
    tmp_path: Path,
    field: str,
    value: str,
) -> None:
    inputs = _transition_inputs(tmp_path)
    inputs[field] = value

    with pytest.raises(PersonalDevSchemaTransitionError):
        prepare_personal_dev_schema_transition(**inputs)


def test_transition_preparation_normalizes_invalid_alembic_graph(
    tmp_path: Path,
) -> None:
    inputs = _transition_inputs(tmp_path)
    inputs["alembic_ini_path"] = _write_alembic_graph(
        tmp_path / "invalid-migrations",
        {"0121": "missing_parent"},
    )

    with pytest.raises(PersonalDevSchemaTransitionError):
        prepare_personal_dev_schema_transition(**inputs)


@pytest.mark.parametrize("index_option", ("--skip-worktree", "--assume-unchanged"))
def test_source_validation_rejects_hidden_index_state(
    tmp_path: Path,
    index_option: str,
) -> None:
    checkout = tmp_path / "checkout"
    commit, tree = _write_source_validation_checkout(checkout)
    transition_module = "src/loom/personal_dev_schema_transition.py"
    subprocess.run(
        [
            "/usr/bin/git",
            "-C",
            str(checkout),
            "update-index",
            index_option,
            transition_module,
        ],
        check=True,
    )
    with (checkout / transition_module).open("a", encoding="utf-8") as stream:
        stream.write("\n# hidden source drift\n")

    result = _run_source_validation(
        checkout,
        commit=commit,
        tree=tree,
        expect_rejection=True,
    )

    assert result.returncode == 0, result.stderr


def test_source_validation_ignores_caller_git_environment(tmp_path: Path) -> None:
    checkout = tmp_path / "checkout"
    commit, tree = _write_source_validation_checkout(checkout)
    hostile_config = tmp_path / "hostile.gitconfig"
    hostile_config.write_text("[core]\n\tbare = true\n", encoding="ascii")

    result = _run_source_validation(
        checkout,
        commit=commit,
        tree=tree,
        environment={
            "PATH": str(tmp_path / "missing-bin"),
            "GIT_CONFIG_GLOBAL": str(hostile_config),
            "GIT_DIR": str(tmp_path / "wrong-git-dir"),
            "GIT_INDEX_FILE": str(tmp_path / "wrong-index"),
            "GIT_WORK_TREE": str(tmp_path / "wrong-work-tree"),
        },
        expect_rejection=False,
    )

    assert result.returncode == 0, result.stderr


def test_source_validation_rejects_mode_drift_hidden_by_git_config(tmp_path: Path) -> None:
    checkout = tmp_path / "checkout"
    commit, tree = _write_source_validation_checkout(checkout)
    transition_module = checkout / "src/loom/personal_dev_schema_transition.py"
    subprocess.run(
        ["/usr/bin/git", "-C", str(checkout), "config", "core.filemode", "false"],
        check=True,
    )
    transition_module.chmod(0o744)
    status = subprocess.run(
        [
            "/usr/bin/git",
            "-C",
            str(checkout),
            "status",
            "--porcelain=v1",
            "--untracked-files=all",
        ],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    assert status == ""

    result = _run_source_validation(
        checkout,
        commit=commit,
        tree=tree,
        expect_rejection=True,
    )

    assert result.returncode == 0, result.stderr


def test_source_validation_rejects_ignored_untracked_module(tmp_path: Path) -> None:
    checkout = tmp_path / "checkout"
    commit, tree = _write_source_validation_checkout(checkout)
    relative_module = "src/loom/ignored_injected_module.py"
    with (checkout / ".git" / "info" / "exclude").open("a", encoding="utf-8") as stream:
        stream.write(f"{relative_module}\n")
    (checkout / relative_module).write_text("INJECTED = True\n", encoding="ascii")
    status = subprocess.run(
        [
            "/usr/bin/git",
            "-C",
            str(checkout),
            "status",
            "--porcelain=v1",
            "--untracked-files=all",
        ],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    assert status == ""

    result = _run_source_validation(
        checkout,
        commit=commit,
        tree=tree,
        expect_rejection=True,
    )

    assert result.returncode == 0, result.stderr


def test_source_validation_does_not_execute_repository_clean_filter(
    tmp_path: Path,
) -> None:
    checkout = tmp_path / "checkout"
    _write_source_validation_checkout(checkout)
    marker = tmp_path / "filter-executed"
    (checkout / ".gitattributes").write_text(
        "src/loom/personal_dev_schema_transition.py filter=side-effect\n",
        encoding="ascii",
    )
    subprocess.run(
        [
            "/usr/bin/git",
            "-C",
            str(checkout),
            "config",
            "filter.side-effect.clean",
            f"/bin/sh -c '/usr/bin/touch {marker}; /bin/cat'",
        ],
        check=True,
    )
    subprocess.run(
        ["/usr/bin/git", "-C", str(checkout), "add", ".gitattributes"],
        check=True,
    )
    subprocess.run(
        [
            "/usr/bin/git",
            "-C",
            str(checkout),
            "-c",
            "user.name=Loom tests",
            "-c",
            "user.email=loom-tests@example.invalid",
            "commit",
            "-qm",
            "configure clean filter",
        ],
        check=True,
    )
    marker.unlink(missing_ok=True)
    transition_module = checkout / "src/loom/personal_dev_schema_transition.py"
    metadata = transition_module.stat()
    os.utime(
        transition_module,
        ns=(metadata.st_atime_ns, metadata.st_mtime_ns + 1_000_000_000),
    )
    commit = subprocess.run(
        ["/usr/bin/git", "-C", str(checkout), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    tree = subprocess.run(
        ["/usr/bin/git", "-C", str(checkout), "rev-parse", "HEAD^{tree}"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()

    result = _run_source_validation(
        checkout,
        commit=commit,
        tree=tree,
        expect_rejection=False,
    )

    assert result.returncode == 0, result.stderr
    assert not marker.exists()
