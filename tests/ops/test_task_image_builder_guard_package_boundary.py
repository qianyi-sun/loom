"""Closed-package tests for the isolated node-guard zipapp."""

from __future__ import annotations

import ast
import json
import os
import struct
import subprocess
import sys
import tomllib
import zipfile
from pathlib import Path

import pytest
from scripts.ops.task_image_builder_guard_release import build_release

from loom_task_image_builder_guard.bpf import NetworkPolicy
from loom_task_image_builder_guard.config import GuardConfig

ROOT = Path(__file__).resolve().parents[2]
PACKAGE = ROOT / "src/loom_task_image_builder_guard"


def _imports(tree: ast.AST) -> set[str]:
    result: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            result.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            if node.level:
                raise AssertionError("guard sources must use explicit absolute imports")
            if node.module is not None:
                result.add(node.module)
    return result


def _bpftool(path: Path) -> Path:
    ident = b"\x7fELF" + bytes((2, 1, 1, 0)) + bytes(8)
    path.write_bytes(
        struct.pack(
            "<16sHHIQQQIHHHHHH",
            ident,
            3,
            62,
            1,
            0,
            0,
            0,
            0,
            64,
            0,
            0,
            64,
            0,
            0,
        )
        + b"test-bpftool"
    )
    path.chmod(0o755)
    return path


def test_guard_ast_closes_imports_and_dynamic_code_loading() -> None:
    sources = sorted(PACKAGE.glob("*.py"))
    assert sources
    for source in sources:
        tree = ast.parse(source.read_bytes(), filename=str(source))
        for module in _imports(tree):
            root = module.partition(".")[0]
            assert root == "loom_task_image_builder_guard" or root in sys.stdlib_module_names
        forbidden_calls = {
            node.func.id
            for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id in {"__import__", "compile", "eval", "exec"}
        }
        assert not forbidden_calls
        assert not any(
            isinstance(node, ast.Attribute)
            and isinstance(node.value, ast.Name)
            and node.value.id == "sys"
            and node.attr == "path"
            for node in ast.walk(tree)
        )


def test_release_spec_covers_the_exact_guard_source_set() -> None:
    spec = json.loads((ROOT / "deploy/task-image-builder/guard-release-v1.json").read_bytes())
    assert [item["path"] for item in spec["sources"]] == [
        path.relative_to(ROOT).as_posix() for path in sorted(PACKAGE.glob("*.py"))
    ]


def test_built_zipapp_isolated_self_check_cannot_import_from_ambient_paths(
    tmp_path: Path,
) -> None:
    malicious = tmp_path / "ambient"
    malicious.mkdir()
    (malicious / "sitecustomize.py").write_text(
        "raise RuntimeError('ambient import succeeded')\n", encoding="ascii"
    )
    result = build_release(ROOT, _bpftool(tmp_path / "bpftool"), tmp_path / "out", "x86_64")
    archive = result.directory / "loom-task-image-builder-guard.pyz"

    completed = subprocess.run(
        ("/usr/bin/python3", "-I", "-B", str(archive), "--self-check"),
        cwd=malicious,
        env={
            "HOME": str(malicious),
            "LANG": "C",
            "LC_ALL": "C",
            "PATH": "/usr/bin:/bin",
        },
        stdin=subprocess.DEVNULL,
        capture_output=True,
        check=False,
        timeout=10,
    )

    assert completed.returncode == 0
    assert completed.stdout == (
        b'{"schema":"loom.task-image-builder-node-guard-self-check/v1",'
        b'"status":"ok"}\n'
    )
    assert completed.stderr == b""
    with zipfile.ZipFile(archive) as bundle:
        assert not any(
            name.endswith((".pyc", ".pyo"))
            or "/__pycache__/" in name
            or name.startswith(("src/", "tests/", "scripts/"))
            or name.endswith(("METADATA", "entry_points.txt"))
            for name in bundle.namelist()
        )


def test_guard_package_is_in_default_strict_mypy_scope() -> None:
    project = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))

    assert "src/loom_task_image_builder_guard" in project["tool"]["mypy"]["files"]


@pytest.mark.parametrize(
    ("cluster", "architecture"),
    (("oldlab", "x86_64"), ("gb10", "arm64")),
)
def test_native_example_config_and_policy_are_strict_public_inert_inputs(
    tmp_path: Path,
    cluster: str,
    architecture: str,
) -> None:
    config_source = ROOT / (
        f"deploy/task-image-builder/guard-config-{cluster}-v1.example.json"
    )
    policy_source = ROOT / (
        f"deploy/task-image-builder/guard-network-policy-{cluster}-v1.example.json"
    )
    config_payload = config_source.read_bytes()
    policy_payload = policy_source.read_bytes()
    assert b"loom_tibp_" not in config_payload + policy_payload
    assert b"loom_tibs_" not in config_payload + policy_payload
    assert b"activation" not in config_payload + policy_payload
    assert b'/current/' not in config_payload + policy_payload

    config_path = tmp_path / f"{cluster}-config.json"
    config_path.write_bytes(config_payload)
    config_path.chmod(0o600)
    policy_path = tmp_path / f"{cluster}-policy.json"
    policy_path.write_bytes(policy_payload)
    policy_path.chmod(0o444)

    config = GuardConfig.from_file(config_path)
    policy = NetworkPolicy.from_file(
        policy_path,
        uid=os.geteuid(),
        gid=os.getegid(),
        containment_policy_sha256=config.containment.containment_policy_sha256,
        resource_profile_sha256=config.containment.resource_profile_sha256,
        bpf_program_sha256=config.containment.bpf_program_sha256,
        bpf_map_schema_sha256=config.containment.bpf_map_schema_sha256,
    )

    assert (config.cluster_id, config.cpu_arch) == (cluster, architecture)
    assert policy.containment_policy_sha256 == config.containment.containment_policy_sha256
    release_component = config.commands.bpftool.path.parts[-2]
    assert len(release_component) == 64
    assert set(release_component) <= set("0123456789abcdef")
