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
        ("-p 3.13", "--python latest"),
        ("-w pandas==2.3.3", "--with pandas"),
        ("uv/0.9.5", "uv/latest"),
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


def test_plain_pip_keeps_base_dependencies_and_both_pytest_results(tmp_path: Path) -> None:
    environment = bundle(tmp_path)
    script = """pip install pytest==8.4.1 pytest-json-ctrf==0.3.5 --break-system-packages
python -m pytest --ctrf /logs/verifier/original.json -rA
ORIGINAL_EXIT_CODE=$?
python -m pytest --ctrf /logs/verifier/ctrf.json /tests/test_outputs.py -rA
ADDITIONAL_EXIT_CODE=$?
if [ $ORIGINAL_EXIT_CODE -eq 0 ] && [ $ADDITIONAL_EXIT_CODE -eq 0 ]; then
    echo 1 > /logs/verifier/reward.txt
else
    echo 0 > /logs/verifier/reward.txt
fi
"""
    (tmp_path / "tests/test.sh").write_text(script)
    prepare_nebius_terminus_image(tmp_path, environment)
    derived = (tmp_path / environment["dockerfile"]).read_text()
    offline = (tmp_path / OFFLINE_SCRIPT).read_text()
    assert '--python "$(command -v python3)" --system-site-packages /opt/verifier' in derived
    assert offline.count("/opt/verifier/bin/python -m pytest") == 2
    assert offline.endswith(script[script.index("ADDITIONAL_EXIT_CODE") :])
    assert "pip install" not in offline


def test_combined_apt_and_preinstalled_uv_keep_task_commands() -> None:
    script = SCRIPT.replace(
        "apt-get update\napt-get install -y curl primer3",
        "apt-get update -qq && DEBIAN_FRONTEND=noninteractive apt-get install -y curl primer3",
    )
    script = script.replace("curl -LsSf https://astral.sh/uv/0.9.5/install.sh | sh\n", "")
    script = script.replace('source "$HOME/.local/bin/env"\n', "")
    result = adapt_harbor_test_script(script)
    assert result.apt_packages == ("curl", "primer3")
    assert "python3 /tests/gen_large_csv.py input" in result.script
    assert "apt-get" not in result.script


def test_explicit_uv_venv_relocates_activation_and_preserves_python() -> None:
    result = adapt_harbor_test_script("""uv venv -p 3.12 .tb
source .tb/bin/activate
uv pip install pytest==8.4.1 mailman==3.3.8 pytest-json-ctrf==0.3.5
uv run pytest --ctrf /logs/verifier/ctrf.json /tests/test_outputs.py -rA
""")
    assert result.python_version == "3.12"
    assert result.script.startswith("source /opt/verifier/bin/activate\n")
    assert "mailman==3.3.8" in result.requirements
    assert "uv " not in result.script


def test_cpu_index_pinned_git_and_download_are_prepared_offline(tmp_path: Path) -> None:
    environment = bundle(tmp_path)
    (tmp_path / "environment/Dockerfile").write_text("FROM python:3.11\nWORKDIR /app\n")
    script = SCRIPT.replace(
        "uvx \\\n",
        """COMMIT_HASH='34bbbfdface3c18e5221aa7de6032d7220c6c6a1'
curl -L -o mobile_sam.pt https://github.com/ChaoningZhang/MobileSAM/raw/master/weights/mobile_sam.pt
uvx --index https://download.pytorch.org/whl/cpu --index-strategy unsafe-best-match \\
""",
    ).replace(
        "-w pandas==2.3.3", "-w git+https://github.com/ChaoningZhang/MobileSAM.git@${COMMIT_HASH}"
    )
    (tmp_path / "tests/test.sh").write_text(script)
    prepare_nebius_terminus_image(tmp_path, environment)
    derived = (tmp_path / environment["dockerfile"]).read_text()
    offline = (tmp_path / OFFLINE_SCRIPT).read_text()
    assert (
        "--index https://download.pytorch.org/whl/cpu --index-strategy unsafe-best-match" in derived
    )
    assert ".git@34bbbfdface3c18e5221aa7de6032d7220c6c6a1" in derived
    assert "curl --fail --location" in derived
    assert "cp /opt/verifier-assets/mobile_sam.pt mobile_sam.pt" in offline
    assert "curl " not in offline
    assert "${COMMIT_HASH}" not in derived


@pytest.mark.parametrize(
    "command",
    [
        "pip install pytest>=8",
        "pip install pytest==8.4.1 && touch /tmp/unreviewed",
        "uvx -p 3.13 -w pytest==8.4.1 -w git+https://example.com/repo.git@main pytest /tests/test.py",
        "curl -L -o ../weights https://example.com/weights",
    ],
)
def test_unknown_dependency_sources_or_shell_remain_rejected(command: str) -> None:
    with pytest.raises(ValueError, match="nebius-terminus"):
        adapt_harbor_test_script(command + "\npytest /tests/test.py\n")


@pytest.mark.parametrize("version", ["0.7.13", "0.8.22", "0.10.0"])
def test_pinned_uv_bootstraps_preserve_requirements_and_pytest_arguments(version: str) -> None:
    source = SCRIPT.replace("uv/0.9.5", "uv/" + version).replace(
        "-p 3.13", "--python 3.13"
    ).replace("-w ", "--with ")
    assert adapt_harbor_test_script(source) == adapt_harbor_test_script(SCRIPT)


@pytest.mark.parametrize(
    "dockerfile",
    [
        "FROM python:3.9-slim\nWORKDIR /app\nRUN python3 <<'PY'\nfrom datetime import datetime\nPY\n",
        'FROM python:3.9-slim\nCOPY <<-"FIRST" <<SECOND /tmp/\n\tFROM alpine:3.20\n\tFIRST\nSHELL []\nSECOND\n',
        "FROM --platform=linux/amd64 \\\n python:3.9-slim AS base\nFROM base AS task\n",
    ],
)
def test_preparation_identifies_final_stage_without_changing_task_python(
    tmp_path: Path, dockerfile: str,
) -> None:
    environment = bundle(tmp_path)
    (tmp_path / "environment/Dockerfile").write_text(dockerfile)
    prepare_nebius_terminus_image(tmp_path, environment)
    derived = (tmp_path / environment["dockerfile"]).read_text()
    assert derived.startswith(dockerfile)
    assert "loom-nebius-uv python install 3.13" in derived
    assert "ENV PATH=" not in derived
    assert (tmp_path / "environment/Dockerfile").read_text() == dockerfile


@pytest.mark.parametrize(
    "dockerfile,match",
    [
        ("FROM python:3.13-slim\nRUN cat <<EOF\nFROM ubuntu:24.04\n", "unterminated heredoc"),
        ("FROM ubuntu:24.04 AS base\nFROM alpine:3.20\n", "Debian/Ubuntu"),
        ("FROM python:3.13-slim\nSHELL [\"bash\", \"-c\"]\n", "SHELL"),
        ("ARG BASE=ubuntu:24.04\nFROM ${BASE}\n", "Debian/Ubuntu"),
    ],
)
def test_ambiguous_or_unsupported_image_preparation_does_not_write_outputs(
    tmp_path: Path, dockerfile: str, match: str,
) -> None:
    environment = bundle(tmp_path)
    (tmp_path / "environment/Dockerfile").write_text(dockerfile)
    with pytest.raises(ValueError, match=match):
        prepare_nebius_terminus_image(tmp_path, environment)
    assert not (tmp_path / OFFLINE_SCRIPT).exists()
    assert not (tmp_path / "environment/Dockerfile.loom-nebius").exists()


@pytest.mark.parametrize("user,home,expected", [
    ("root", "/root", "0:0"), ("1001:1002", "/home/miles", "1001:1002"),
])
def test_preparation_preserves_declared_user_home_and_missing_task_dependencies(tmp_path, user, home, expected):
    environment = bundle(tmp_path)
    environment.update(user=user, environment={"HOME": home})
    prepare_nebius_terminus_image(tmp_path, environment)
    derived = (tmp_path / environment["dockerfile"]).read_text()
    assert f"ENV HOME={home}\n" in derived
    assert f"USER {expected}\n" in derived
    assert "php" not in derived and "composer" not in derived and "wget" not in derived
    assert "chown -R 65532:65532 /app" not in derived
    assert "ENV PATH=" not in derived


def test_preparation_rejects_unresolved_named_user_before_writing(tmp_path):
    environment = bundle(tmp_path)
    environment["user"] = "miles"
    with pytest.raises(ValueError, match="unsupported task identity"):
        prepare_nebius_terminus_image(tmp_path, environment)
    assert not (tmp_path / OFFLINE_SCRIPT).exists()


def test_apt_metadata_cleanup_is_relocated_with_explicit_packages():
    source = SCRIPT.replace("apt-get update\napt-get install -y curl primer3", 
                            "apt-get update && apt-get install -y curl primer3 && rm -rf /var/lib/apt/lists/*")
    assert adapt_harbor_test_script(source) == adapt_harbor_test_script(SCRIPT)


@pytest.mark.parametrize("suffix", [" && rm -rf /app/*", " && touch /app/setup", " || true"])
def test_apt_cleanup_does_not_hide_arbitrary_task_commands(suffix):
    with pytest.raises(ValueError, match="unsupported apt bootstrap"):
        adapt_harbor_test_script(SCRIPT.replace("apt-get install -y curl primer3", 
                                               "apt-get install -y curl primer3" + suffix))
