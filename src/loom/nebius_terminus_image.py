"""Build-time preparation of the supported Harbor shell bootstrap on Nebius.

Only installer plumbing is relocated. Task setup, assertions, reward handling,
base-image programs, and original source files remain unchanged.
"""

from __future__ import annotations

import re
import shlex
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any

OFFLINE_SCRIPT = "verifier/harbor-offline.sh"
_DOCKERFILE_SUFFIX = ".loom-nebius"
_PACKAGE = re.compile(r"[a-z0-9][a-z0-9+.-]*(?:=[A-Za-z0-9.+:~_-]+)?\Z")
_REQUIREMENT = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]*==[A-Za-z0-9][A-Za-z0-9_.+!-]*\Z")
_UV_INSTALL = re.compile(
    r"curl -LsSf https://astral\.sh/uv/0\.9\.5/install\.sh\s*\|\s*sh\s*\Z",
)
_UV_SOURCE = re.compile(r'source\s+(?:"\$HOME/\.local/bin/env"|\$HOME/\.local/bin/env)\s*\Z')


@dataclass(frozen=True)
class HarborOfflineBootstrap:
    script: str
    apt_packages: tuple[str, ...]
    python_version: str
    requirements: tuple[str, ...]


def adapt_harbor_test_script(script: str) -> HarborOfflineBootstrap:
    """Relocate the known apt/uvx bootstrap; refuse unrecognized installers."""
    logical_lines: list[str] = []
    pending = ""
    for physical_line in script.splitlines(keepends=True):
        pending += physical_line
        if not physical_line.rstrip("\r\n").endswith("\\"):
            logical_lines.append(pending)
            pending = ""
    if pending:
        raise ValueError("nebius-terminus: incomplete shell continuation")
    output: list[str] = []
    packages: list[str] = []
    requirements: list[str] = []
    python_version: str | None = None
    installer_count = source_count = uvx_count = 0
    for line in logical_lines:
        command = re.sub(r"\\\r?\n", " ", line).strip()
        if not command or command.startswith("#"):
            output.append(line)
            continue
        if re.fullmatch(r"apt-get update(?: -qq)?", command):
            continue
        if command.startswith("apt-get install "):
            words = shlex.split(command)[2:]
            if "-y" not in words:
                raise ValueError("nebius-terminus: apt bootstrap requires noninteractive -y")
            names = [word for word in words if word not in {"-y", "--no-install-recommends"}]
            if not names or any(not _PACKAGE.fullmatch(name) for name in names):
                raise ValueError("nebius-terminus: unsupported apt bootstrap")
            packages.extend(names)
            continue
        if _UV_INSTALL.fullmatch(command):
            installer_count += 1
            continue
        if _UV_SOURCE.fullmatch(command):
            source_count += 1
            continue
        if command.startswith("uvx "):
            uvx_count += 1
            words = shlex.split(command)
            if len(words) < 6 or words[1] != "-p" or not re.fullmatch(r"\d+\.\d+", words[2]):
                raise ValueError("nebius-terminus: unsupported uvx Python declaration")
            python_version = words[2]
            position = 3
            while position + 1 < len(words) and words[position] == "-w":
                requirement = words[position + 1]
                if not _REQUIREMENT.fullmatch(requirement):
                    raise ValueError(
                        "nebius-terminus: verifier requirements must use exact version pins"
                    )
                requirements.append(requirement)
                position += 2
            if position >= len(words) or words[position] != "pytest":
                raise ValueError(
                    "nebius-terminus: only the pinned uvx pytest bootstrap is supported"
                )
            arguments = words[position + 1 :]
            if not arguments or any(
                not re.fullmatch(r"[A-Za-z0-9/_.,=:+-]+", arg) for arg in arguments
            ):
                raise ValueError("nebius-terminus: unsupported shell syntax in pytest invocation")
            output.append("/opt/verifier/bin/pytest " + shlex.join(arguments) + "\n")
            continue
        # Do not silently convert a script that still needs online installers.
        if re.search(r"\b(?:apt-get|apt|pip|pip3|uv|uvx|curl|wget)\b", command):
            raise ValueError(
                "nebius-terminus: unsupported online bootstrap command in tests/test.sh"
            )
        output.append(line)
    if (installer_count, source_count, uvx_count) != (1, 1, 1) or python_version is None:
        raise ValueError(
            "nebius-terminus: tests/test.sh requires the recognized Harbor uv 0.9.5 bootstrap"
        )
    if not any(item.startswith("pytest==") for item in requirements):
        raise ValueError("nebius-terminus: verifier pytest must have an exact version pin")
    return HarborOfflineBootstrap(
        script="".join(output),
        apt_packages=tuple(sorted(set(packages))),
        python_version=python_version,
        requirements=tuple(dict.fromkeys(requirements)),
    )


def _bundle_path(staged: Path, value: str, *, directory: bool = False) -> Path:
    relative = PurePosixPath(value)
    if relative.is_absolute() or ".." in relative.parts or not relative.parts:
        raise ValueError("nebius-terminus: preparation path must remain inside the bundle")
    path = staged
    if path.is_symlink():
        raise ValueError("nebius-terminus: symlink bundle paths are unsupported")
    for part in relative.parts:
        path /= part
        if path.is_symlink():
            raise ValueError("nebius-terminus: symlink bundle paths are unsupported")
    if path.exists() and (not path.is_dir() if directory else not path.is_file()):
        raise ValueError("nebius-terminus: invalid preparation path type")
    return path


def _preparation_dockerfile(original: str, bootstrap: HarborOfflineBootstrap, workdir: str) -> str:
    from_lines = re.findall(r"(?im)^FROM\s+(\S+)", original)
    if not from_lines or not re.fullmatch(
        r"(?:ubuntu:[A-Za-z0-9_.-]+|debian:[A-Za-z0-9_.-]+|python:[A-Za-z0-9_.-]*slim(?:-(?:bookworm|bullseye|trixie))?)",
        from_lines[-1],
    ):
        raise ValueError(
            "nebius-terminus: image preparation supports Debian/Ubuntu final base images only"
        )
    if re.search(r"(?im)^SHELL\s", original):
        raise ValueError("nebius-terminus: custom Dockerfile SHELL requires explicit adaptation")
    packages = sorted(
        {
            "ca-certificates",
            "curl",
            "bash",
            "tmux",
            "asciinema",
            "passwd",
            "python3",
            "python-is-python3",
            *bootstrap.apt_packages,
        }
    )
    requirements = " ".join(shlex.quote(item) for item in bootstrap.requirements)
    return (
        original.rstrip()
        + f"""\n\n# Loom Nebius: build-only nonroot/offline preparation; original task above.
USER root
COPY --from=ghcr.io/astral-sh/uv:0.9.5 /uv /usr/local/bin/loom-nebius-uv
RUN apt-get update -qq && apt-get install -y --no-install-recommends {" ".join(packages)} && \\
    UV_PYTHON_INSTALL_DIR=/opt/verifier-python loom-nebius-uv python install {bootstrap.python_version} && \\
    UV_PYTHON_INSTALL_DIR=/opt/verifier-python loom-nebius-uv venv --python {bootstrap.python_version} /opt/verifier && \\
    loom-nebius-uv pip install --python /opt/verifier/bin/python {requirements} && \\
    (getent group 65532 >/dev/null || groupadd --gid 65532 agent) && \\
    (getent passwd 65532 >/dev/null || useradd --uid 65532 --gid 65532 --home-dir /home/agent agent) && \\
    mkdir -p {workdir} /home/agent /tests /logs/verifier /loom/verifier && \\
    chown -R 65532:65532 {workdir} /home/agent /tests /logs/verifier /loom/verifier && \\
    rm -rf /var/lib/apt/lists/* /root/.cache
ENV HOME=/home/agent
# Preserve the base image PATH and agent interpreter; verifier uses its own venv.
USER 65532:65532
WORKDIR {workdir}
"""
    )


def prepare_nebius_terminus_image(staged: Path, environment: dict[str, Any]) -> bool:
    """Write deterministic derived inputs and point the config at their Dockerfile.

    The supplied environment must already use the supported workspace identity.
    This bounded adapter needs the original Dockerfile and Harbor tests/test.sh;
    prebuilt/custom offline images require an explicit reviewed preparation path.
    """
    if environment.get("docker_image") or not environment.get("dockerfile"):
        raise ValueError("nebius-terminus: image preparation requires original Dockerfile sources")
    if environment.get("docker_build_target"):
        raise ValueError(
            "nebius-terminus: selected Dockerfile build targets require explicit adaptation"
        )
    workdir = str(environment.get("workdir", "/app"))
    if workdir not in {"/app", "/workspace"}:
        raise ValueError("nebius-terminus: unsupported preparation workdir")
    source_name = str(environment["dockerfile"])
    if source_name.endswith(_DOCKERFILE_SUFFIX):
        source_name = source_name.removesuffix(_DOCKERFILE_SUFFIX)
    source = _bundle_path(staged, source_name)
    test_script = _bundle_path(staged, "tests/test.sh")
    if not source.is_file() or not test_script.is_file():
        raise ValueError("nebius-terminus: original Dockerfile and tests/test.sh are required")
    bootstrap = adapt_harbor_test_script(test_script.read_text())
    dockerfile = _preparation_dockerfile(source.read_text(), bootstrap, workdir)
    target_name = source_name + _DOCKERFILE_SUFFIX
    target = _bundle_path(staged, target_name)
    offline = _bundle_path(staged, OFFLINE_SCRIPT)
    # Validate both output paths before writing either one.
    _bundle_path(staged, "verifier", directory=True)
    changed = (
        not target.is_file()
        or target.read_text() != dockerfile
        or not offline.is_file()
        or offline.read_text() != bootstrap.script
        or not offline.stat().st_mode & 0o111
    )
    target.write_text(dockerfile)
    offline.parent.mkdir(parents=True, exist_ok=True)
    offline.write_text(bootstrap.script)
    offline.chmod(0o755)
    environment["dockerfile"] = target_name
    return changed
