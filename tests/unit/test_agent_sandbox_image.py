from __future__ import annotations

from pathlib import Path

from loom_service.agent_catalog import list_agents

ROOT = Path(__file__).resolve().parents[2]
DOCKERFILE = ROOT / "deploy" / "Dockerfile.agent-sandbox"
NPM_PACKAGES = ROOT / "deploy" / "agent-sandbox" / "npm-packages.txt"
PYTHON_REQUIREMENTS = ROOT / "deploy" / "agent-sandbox" / "python-requirements.txt"
PYTHON_CLI_REQUIREMENTS = ROOT / "deploy" / "agent-sandbox" / "python-cli-requirements.txt"


def _manifest_entries(path: Path) -> set[str]:
    return {
        line.strip()
        for line in path.read_text().splitlines()
        if line.strip() and not line.strip().startswith("#")
    }


def _matches_declared_package(entry: str, package: str) -> bool:
    normalized = entry.removeprefix("-e ").strip()
    return (
        normalized == package
        or normalized.startswith(f"{package}@")
        or normalized.startswith(f"{package}==")
    )


def test_agent_sandbox_image_files_exist_and_are_used_by_dockerfile() -> None:
    dockerfile = DOCKERFILE.read_text()

    assert NPM_PACKAGES.is_file()
    assert PYTHON_REQUIREMENTS.is_file()
    assert PYTHON_CLI_REQUIREMENTS.is_file()
    assert "FROM node:22-bookworm-slim AS node-runtime" in dockerfile
    assert "python:3.12-bookworm" in dockerfile
    assert "COPY --from=node-runtime /usr/local/ /usr/local/" in dockerfile
    assert "deb.nodesource.com" not in dockerfile
    assert "deploy/agent-sandbox/npm-packages.txt" in dockerfile
    assert "deploy/agent-sandbox/python-requirements.txt" in dockerfile
    assert "deploy/agent-sandbox/python-cli-requirements.txt" in dockerfile
    assert "packages/loom-launcher" in dockerfile
    assert "/opt/agent-runtimes/python-cli" in dockerfile
    assert "npm install -g" in dockerfile
    assert "pip install" in dockerfile
    assert "node --version" in dockerfile
    assert "python --version" in dockerfile
    assert 'importlib.import_module("openhands.sdk")' in dockerfile
    assert 'importlib.import_module("loom_launcher.openhands_sdk_runner")' in dockerfile
    assert "openhands_sdk.run" not in dockerfile


def test_agent_sandbox_package_manifests_cover_catalog_runtime_contracts() -> None:
    npm_entries = _manifest_entries(NPM_PACKAGES)
    python_entries = _manifest_entries(PYTHON_REQUIREMENTS)
    python_cli_entries = _manifest_entries(PYTHON_CLI_REQUIREMENTS)
    installed_entries = npm_entries | python_entries | python_cli_entries

    missing: list[tuple[str, str]] = []
    for agent in list_agents():
        if agent.name in {"oracle", "litellm", "hello"}:
            continue
        for package in agent.runtime_contract.required_packages:
            if not any(_matches_declared_package(entry, package) for entry in installed_entries):
                missing.append((agent.name, package))

    assert missing == []


def test_swe_agent_uses_editable_source_install_for_config_layout() -> None:
    python_entries = _manifest_entries(PYTHON_REQUIREMENTS)

    assert any(
        entry.startswith("-e git+https://github.com/SWE-agent/SWE-agent")
        and "#egg=sweagent" in entry
        for entry in python_entries
    )
