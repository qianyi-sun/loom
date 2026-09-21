"""Behavioral boundaries for the bounded Harbor image preparation adapter."""

from pathlib import Path

import pytest

from loom.nebius_terminus_image import (
    OFFLINE_SCRIPT,
    adapt_harbor_test_script,
    prepare_nebius_terminus_image,
)

SCRIPT = """#!/bin/bash
apt-get update
apt-get install -y curl primer3
curl -LsSf https://astral.sh/uv/0.9.5/install.sh | sh
source "$HOME/.local/bin/env"
# Keep this task-specific preparation and reward logic.
rm *.csv
python3 /tests/gen_large_csv.py input
cp /tests/test.py /app/test.py
uvx \\
  -p 3.13 \\
  -w pytest==8.4.1 \\
  -w pandas==2.3.3 \\
  -w pytest-json-ctrf==0.3.5 \\
  pytest --ctrf /logs/verifier/ctrf.json /tests/test_outputs.py -rA
if [ $? -eq 0 ]; then
  echo 1 > /logs/verifier/reward.txt
else
  echo 0 > /logs/verifier/reward.txt
fi
"""


def bundle(tmp_path: Path) -> dict:
    (tmp_path / "environment").mkdir()
    (tmp_path / "environment/Dockerfile").write_text(
        "FROM python:3.13-slim-bookworm\nWORKDIR /app\nCOPY data /data\n",
    )
    (tmp_path / "tests").mkdir()
    (tmp_path / "tests/test.sh").write_text(SCRIPT)
    return {
        "dockerfile": "environment/Dockerfile",
        "docker_build_context": "environment",
        "workdir": "/app",
    }


def test_retains_setup_and_reward_semantics_and_extracts_pins() -> None:
    result = adapt_harbor_test_script(SCRIPT)
    assert result.python_version == "3.13"
    assert result.apt_packages == ("curl", "primer3")
    assert result.requirements == ("pytest==8.4.1", "pandas==2.3.3", "pytest-json-ctrf==0.3.5")
    assert (
        "rm *.csv\npython3 /tests/gen_large_csv.py input\ncp /tests/test.py /app/test.py\n"
        in result.script
    )
    assert (
        "/opt/verifier/bin/pytest --ctrf /logs/verifier/ctrf.json /tests/test_outputs.py -rA\n"
        in result.script
    )
    assert result.script.endswith(SCRIPT[SCRIPT.index("if [ $? -eq 0 ]") :])
    assert "apt-get" not in result.script
    assert "uvx" not in result.script


def test_preserves_other_shell_continuations() -> None:
    statement = 'printf "%s" \\\n  "hello"\n'
    result = adapt_harbor_test_script(SCRIPT.replace("rm *.csv\n", statement))
    assert statement in result.script


@pytest.mark.parametrize(
    "old,new",
    [
        ("pytest==8.4.1", "pytest>=8"),
        ("-p 3.13", "--python 3.13"),
        ("-w pandas==2.3.3", "--with pandas==2.3.3"),
        ("uv/0.9.5", "uv/0.10.0"),
        ("apt-get install -y curl primer3", "apt-get install -y curl && echo danger"),
        ("rm *.csv", "python3 -m pip install pandas"),
        ("-rA", '-rA "$EXTRA"'),
    ],
)
def test_refuses_unknown_bootstrap_without_silent_fallback(old: str, new: str) -> None:
    with pytest.raises(ValueError, match="nebius-terminus"):
        adapt_harbor_test_script(SCRIPT.replace(old, new))


def test_requires_original_recognized_bootstrap() -> None:
    with pytest.raises(ValueError, match="recognized Harbor"):
        adapt_harbor_test_script("#!/bin/sh\n/opt/verifier/bin/pytest /tests/test_outputs.py\n")


def test_derivation_preserves_sources_and_base_interpreter_and_is_repeatable(
    tmp_path: Path,
) -> None:
    environment = bundle(tmp_path)
    original = (tmp_path / "environment/Dockerfile").read_bytes()
    assert prepare_nebius_terminus_image(tmp_path, environment)
    assert environment["dockerfile"] == "environment/Dockerfile.loom-nebius"
    assert (tmp_path / "environment/Dockerfile").read_bytes() == original
    assert (tmp_path / "tests/test.sh").read_text() == SCRIPT
    derived = (tmp_path / environment["dockerfile"]).read_text()
    assert derived.startswith(original.decode())
    assert "ghcr.io/astral-sh/uv:0.9.5" in derived
    assert "pandas==2.3.3" in derived
    assert "primer3" in derived
    assert "chown -R 65532:65532 /app /home/agent /tests /logs/verifier /loom/verifier" in derived
    assert "USER 65532:65532\nWORKDIR /app" in derived
    assert "ENV PATH=" not in derived
    assert "COPY tests" not in derived
    assert (tmp_path / OFFLINE_SCRIPT).stat().st_mode & 0o111
    assert not prepare_nebius_terminus_image(tmp_path, environment)
    assert (tmp_path / environment["dockerfile"]).read_text() == derived


@pytest.mark.parametrize("value", ["../outside", "/tmp/outside"])
def test_rejects_path_escape(tmp_path: Path, value: str) -> None:
    environment = bundle(tmp_path)
    environment["dockerfile"] = value
    with pytest.raises(ValueError, match="inside the bundle"):
        prepare_nebius_terminus_image(tmp_path, environment)


@pytest.mark.parametrize("link", ["tests/test.sh", "environment/Dockerfile", "verifier"])
def test_rejects_source_and_output_symlinks(tmp_path: Path, link: str) -> None:
    environment = bundle(tmp_path)
    target = tmp_path / link
    if target.exists():
        target.unlink()
    target.symlink_to(tmp_path / "elsewhere")
    with pytest.raises(ValueError, match="symlink"):
        prepare_nebius_terminus_image(tmp_path, environment)


@pytest.mark.parametrize(
    "changes,match",
    [
        ({"docker_image": "ubuntu:24.04"}, "original Dockerfile"),
        ({"docker_build_target": "base"}, "build targets"),
        ({"workdir": "/root"}, "workdir"),
    ],
)
def test_rejects_unreviewed_image_modes(tmp_path: Path, changes: dict, match: str) -> None:
    environment = bundle(tmp_path)
    environment.update(changes)
    with pytest.raises(ValueError, match=match):
        prepare_nebius_terminus_image(tmp_path, environment)
    assert not (tmp_path / OFFLINE_SCRIPT).exists()


def test_rejects_unknown_base_before_writing_outputs(tmp_path: Path) -> None:
    environment = bundle(tmp_path)
    (tmp_path / "environment/Dockerfile").write_text("FROM alpine:3.20\n")
    with pytest.raises(ValueError, match="Debian/Ubuntu"):
        prepare_nebius_terminus_image(tmp_path, environment)
    assert not (tmp_path / OFFLINE_SCRIPT).exists()
