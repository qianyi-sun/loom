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

from loom.dockerfile_instructions import DockerfileParseError, dockerfile_instructions
from loom.sandbox_identity import SandboxIdentityV1, resolve_sandbox_identity

OFFLINE_SCRIPT = "verifier/harbor-offline.sh"
_DOCKERFILE_SUFFIX = ".loom-nebius"
_PACKAGE = re.compile(r"[a-z0-9][a-z0-9+.-]*(?:=[A-Za-z0-9.+:~_-]+)?\Z")
_REQUIREMENT = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]*==[A-Za-z0-9][A-Za-z0-9_.+!-]*\Z")
_UV_INSTALL = re.compile(
    r"curl -LsSf https://astral\.sh/uv/\d+\.\d+\.\d+/install\.sh\s*\|\s*sh\s*\Z",
)
_UV_SOURCE = re.compile(
    r'(?:source\s+(?:"\$HOME/\.local/bin/env"|\$HOME/\.local/bin/env)'
    r'|export PATH="\$HOME/\.local/bin:\$PATH")\s*\Z'
)
_BOOTSTRAP_GUARD = re.compile(
    r"if ! command -v (curl|uv) (?:>/dev/null 2>&1|&> /dev/null); then\Z"
)


def _apt_bootstrap_packages(command: str) -> tuple[str, ...] | None:
    apt = re.fullmatch(
        r"(?:apt-get update(?: -qq)?\s*&&\s*)?"
        r"(?:DEBIAN_FRONTEND=noninteractive\s+)?apt-get install (.+)",
        command,
    )
    if apt is None:
        return None
    # Only conventional apt metadata cleanup is relocated with the installer.
    package_args = re.sub(r"\s*&&\s*rm -rf /var/lib/apt/lists/\*\s*$", "", apt[1])
    words = shlex.split(package_args)
    names = tuple(
        word for word in words if word not in {"-y", "-qq", "--no-install-recommends"}
    )
    if "-y" not in words or not names or any(not _PACKAGE.fullmatch(name) for name in names):
        raise ValueError("nebius-terminus: unsupported apt bootstrap")
    return names


def _unwrap_installer_guards(lines: list[str]) -> list[str]:
    """Unwrap only complete installer-only missing-curl/uv guards.

    All body commands must match before removing either control-flow boundary.
    Nested branches, task setup, and conditional installation of other packages
    cannot be made unconditional by image preparation.
    """
    output: list[str] = []
    position = 0
    while position < len(lines):
        line = lines[position]
        position += 1
        guard = _BOOTSTRAP_GUARD.fullmatch(line.strip())
        if guard is None:
            output.append(line)
            continue
        body: list[str] = []
        while position < len(lines) and lines[position].strip() != "fi":
            body.append(lines[position])
            position += 1
        if position == len(lines):
            raise ValueError("nebius-terminus: unterminated bootstrap guard")
        position += 1
        commands = [
            re.sub(r"\\\r?\n", " ", item).strip()
            for item in body if item.strip() and not item.lstrip().startswith("#")
        ]
        if commands and _apt_bootstrap_packages(commands[0]) == ("curl",):
            commands.pop(0)
            has_curl = True
        else:
            has_curl = False
        valid = (
            has_curl and not commands if guard[1] == "curl" else
            len(commands) == 2
            and _UV_INSTALL.fullmatch(commands[0]) is not None
            and _UV_SOURCE.fullmatch(commands[1]) is not None
        )
        if not valid:
            raise ValueError("nebius-terminus: unsupported bootstrap guard body")
        output.extend(body)
    return output


@dataclass(frozen=True)
class HarborOfflineBootstrap:
    script: str
    apt_packages: tuple[str, ...]
    python_version: str | None
    requirements: tuple[str, ...]
    index_args: tuple[str, ...] = ()
    downloads: tuple[tuple[str, str], ...] = ()
    system_site_packages: bool = False


def adapt_harbor_test_script(script: str) -> HarborOfflineBootstrap:
    """Relocate explicit Harbor installer forms, retaining test/setup semantics.

    This is a bounded command recognizer, not a shell evaluator. Unknown shell
    syntax, dependency sources and installers require an explicit adaptation.
    """
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
    indexes: list[str] = []
    downloads: list[tuple[str, str]] = []
    constants: dict[str, str] = {}
    python_version: str | None = None
    installer_count = source_count = uvx_count = pip_count = pytest_count = 0
    venv: str | None = None
    venv_activated = False

    def requirement(value: str) -> str:
        if _REQUIREMENT.fullmatch(value):
            return value
        # Expand only a literal commit variable, never arbitrary shell expressions.
        for name, commit in constants.items():
            value = value.replace("${" + name + "}", commit)
        if re.fullmatch(r"git\+https://[A-Za-z0-9.-]+/[A-Za-z0-9_./-]+\.git@[0-9a-f]{40}", value):
            return value
        raise ValueError("nebius-terminus: verifier requirements must use exact version pins")

    def pytest_command(arguments: list[str], *, module: bool = False) -> str:
        if not arguments or any(
            not re.fullmatch(r"[A-Za-z0-9/_.,=:+-]+", arg) for arg in arguments
        ):
            raise ValueError("nebius-terminus: unsupported shell syntax in pytest invocation")
        executable = "/opt/verifier/bin/python -m pytest" if module else "/opt/verifier/bin/pytest"
        return executable + " " + shlex.join(arguments) + "\n"

    for line in _unwrap_installer_guards(logical_lines):
        command = re.sub(r"\\\r?\n", " ", line).strip()
        if not command or command.startswith("#"):
            output.append(line)
            continue
        constant = re.fullmatch(r"([A-Z][A-Z0-9_]*)=['\"]([0-9a-f]{40})['\"]", command)
        if constant:
            constants[constant[1]] = constant[2]
            output.append(line)
            continue
        if re.fullmatch(r"apt-get update(?: -qq)?", command):
            continue
        apt_packages = _apt_bootstrap_packages(command)
        if apt_packages is not None:
            packages.extend(apt_packages)
            continue
        if _UV_INSTALL.fullmatch(command):
            installer_count += 1
            continue
        if _UV_SOURCE.fullmatch(command):
            source_count += 1
            continue
        words = shlex.split(command)
        if words[:2] == ["uv", "venv"]:
            if (
                len(words) != 5
                or words[2] not in {"-p", "--python"}
                or not re.fullmatch(r"\d+\.\d+", words[3])
                or not re.fullmatch(r"[A-Za-z0-9_.-]+", words[4])
                or venv is not None
            ):
                raise ValueError("nebius-terminus: unsupported verifier venv bootstrap")
            python_version, venv = words[3], words[4]
            continue
        if venv is not None and words == ["source", venv + "/bin/activate"]:
            output.append("source /opt/verifier/bin/activate\n")
            venv_activated = True
            continue
        pip_prefix = next(
            (
                prefix
                for prefix in (
                    ("pip", "install"),
                    ("pip3", "install"),
                    ("python", "-m", "pip", "install"),
                    ("python3", "-m", "pip", "install"),
                    ("uv", "pip", "install"),
                )
                if words[: len(prefix)] == list(prefix)
            ),
            None,
        )
        if pip_prefix:
            if pip_prefix[0] == "uv" and not venv_activated:
                raise ValueError("nebius-terminus: uv pip requires the declared verifier venv")
            values = [
                word for word in words[len(pip_prefix) :]
                if word not in {"--break-system-packages", "--no-cache-dir"}
            ]
            if not values:
                raise ValueError("nebius-terminus: empty verifier requirements")
            requirements.extend(requirement(value) for value in values)
            pip_count += 1
            continue
        if words[0] == "uvx":
            uvx_count += 1
            position = 1
            while position + 1 < len(words) and words[position] in {"--index", "--index-strategy"}:
                option, value = words[position : position + 2]
                if (
                    option == "--index"
                    and not re.fullmatch(r"https://[A-Za-z0-9.-]+/[A-Za-z0-9_./-]+", value)
                ) or (
                    option == "--index-strategy"
                    and value not in {"first-index", "unsafe-best-match"}
                ):
                    raise ValueError("nebius-terminus: unsupported verifier package index")
                indexes.extend((option, value))
                position += 2
            if words[position : position + 1] in (["-p"], ["--python"]):
                if position + 1 >= len(words) or not re.fullmatch(r"\d+\.\d+", words[position + 1]):
                    raise ValueError("nebius-terminus: unsupported uvx Python declaration")
                python_version = words[position + 1]
                position += 2
            while position + 1 < len(words) and words[position] in {"-w", "--with"}:
                requirements.append(requirement(words[position + 1]))
                position += 2
            if words[position : position + 1] != ["pytest"]:
                raise ValueError(
                    "nebius-terminus: only the pinned uvx pytest bootstrap is supported"
                )
            output.append(pytest_command(words[position + 1 :]))
            pytest_count += 1
            continue
        pytest_prefix = next(
            (
                prefix
                for prefix in (
                    ("pytest",),
                    ("python", "-m", "pytest"),
                    ("python3", "-m", "pytest"),
                    ("uv", "run", "pytest"),
                )
                if words[: len(prefix)] == list(prefix)
            ),
            None,
        )
        if pytest_prefix:
            if pytest_prefix[0] == "uv" and not venv_activated:
                raise ValueError("nebius-terminus: uv run requires the declared verifier venv")
            output.append(pytest_command(words[len(pytest_prefix) :], module="-m" in pytest_prefix))
            pytest_count += 1
            continue
        if words[:3] == ["curl", "-L", "-o"] and len(words) == 5:
            destination, url = words[3:]
            if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", destination) or not re.fullmatch(
                r"https://[A-Za-z0-9.-]+/[A-Za-z0-9_./-]+", url
            ):
                raise ValueError("nebius-terminus: unsupported verifier asset download")
            downloads.append((url, destination))
            output.append(f"cp /opt/verifier-assets/{destination} {destination}\n")
            continue
        if re.search(r"\b(?:apt-get|apt|pip|pip3|uv|uvx|curl|wget)\b", command):
            raise ValueError(
                "nebius-terminus: unsupported online bootstrap command in tests/test.sh"
            )
        output.append(line)
    uvx_mode = uvx_count == 1 and pip_count == 0 and venv is None
    pip_mode = pip_count == 1 and uvx_count == 0
    if (
        not (uvx_mode or pip_mode)
        or pytest_count == 0
        or installer_count > 1
        or source_count > 1
        or installer_count != source_count
        or (venv is not None and not venv_activated)
    ):
        raise ValueError(
            "nebius-terminus: tests/test.sh requires a recognized Harbor verifier bootstrap"
        )
    if not any(item.startswith("pytest==") for item in requirements):
        raise ValueError("nebius-terminus: verifier pytest must have an exact version pin")
    return HarborOfflineBootstrap(
        script="".join(output),
        apt_packages=tuple(sorted(set(packages))),
        python_version=python_version,
        requirements=tuple(dict.fromkeys(requirements)),
        index_args=tuple(indexes),
        downloads=tuple(downloads),
        system_site_packages=pip_mode and venv is None,
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


def _preparation_dockerfile(
    original: str, bootstrap: HarborOfflineBootstrap, workdir: str, identity: SandboxIdentityV1,
) -> str:
    try:
        instructions = dockerfile_instructions(original)
    except DockerfileParseError as exc:
        raise ValueError(f"nebius-terminus: {exc}") from exc
    stages: dict[str, str] = {}
    final_base = ""
    for instruction in instructions:
        if instruction.keyword != "FROM":
            continue
        words = instruction.arguments.split()
        if words and words[0].startswith("--platform="):
            words.pop(0)
        if not (len(words) == 1 or (len(words) == 3 and words[1].upper() == "AS")):
            raise ValueError(
                f"nebius-terminus: unsupported FROM instruction at line {instruction.line}"
            )
        final_base = stages.get(words[0].lower(), words[0])
        if len(words) == 3:
            alias = words[2].lower()
            if alias in stages:
                raise ValueError("nebius-terminus: duplicate Dockerfile stage alias")
            stages[alias] = final_base
    if not re.fullmatch(
        r"(?:ubuntu:[A-Za-z0-9_.-]+|debian:[A-Za-z0-9_.-]+|python:(?:[A-Za-z0-9_.-]*slim(?:-(?:bookworm|bullseye|trixie))?|[0-9]+\.[0-9]+(?:\.[0-9]+)?(?:-(?:bookworm|bullseye|trixie))?))",
        final_base,
    ):
        raise ValueError(
            "nebius-terminus: image preparation supports Debian/Ubuntu final base images only"
        )
    if any(instruction.keyword == "SHELL" for instruction in instructions):
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
    requirements = shlex.join((*bootstrap.index_args, *bootstrap.requirements))
    if bootstrap.python_version is None:
        # No Python override uses the image interpreter. uvx still isolates its
        # tool environment; plain pip scripts see the base task dependencies.
        inherit = " --system-site-packages" if bootstrap.system_site_packages else ""
        python_setup = f'loom-nebius-uv venv --python "$(command -v python3)"{inherit} /opt/verifier'
    else:
        python_setup = (
            f"UV_PYTHON_INSTALL_DIR=/opt/verifier-python loom-nebius-uv python install {bootstrap.python_version} && "
            f"UV_PYTHON_INSTALL_DIR=/opt/verifier-python loom-nebius-uv venv --python {bootstrap.python_version} /opt/verifier"
        )
    assets = "".join(
        f"RUN mkdir -p /opt/verifier-assets && curl --fail --location {shlex.quote(url)} -o /opt/verifier-assets/{name} && chmod 644 /opt/verifier-assets/{name}\n"
        for url, name in bootstrap.downloads
    )
    uid, gid, home = identity.run_as_user, identity.run_as_group, identity.home
    name = "agent" if uid == 65532 else f"loom-task-{uid}"
    identity_setup = (
        f"(getent group {gid} >/dev/null || groupadd --gid {gid} {name}) && "
        f"(getent passwd {uid} >/dev/null || useradd --uid {uid} --gid {gid} --home-dir {home} {name})"
    )
    workspace_setup = (
        f"mkdir -p {workdir} {home} /tests /logs/verifier /loom/verifier && "
        f"chown -R {uid}:{gid} {workdir} {home} /tests /logs/verifier /loom/verifier"
    )
    if uid != 65532 or gid != 65532 or home != "/home/agent":
        # Preserve authored ownership within the task image for explicit users.
        # Only newly created HOME/workspace roots need initial ownership; the
        # private verifier/harness directories are controlled by preparation.
        workspace_setup = (
            f'for directory in {workdir} {home}; do '
            f'if [ ! -e "$directory" ]; then mkdir -p "$directory" && '
            f'chown {uid}:{gid} "$directory"; fi; done && '
            f"mkdir -p /tests /logs/verifier /loom/verifier && "
            f"chown -R {uid}:{gid} /tests /logs/verifier /loom/verifier"
        )
    return (
        original.rstrip()
        + f"""\n\n# Loom Nebius: build-only harness/verifier preparation; original task above.
USER root
COPY --from=ghcr.io/astral-sh/uv:0.9.5 /uv /usr/local/bin/loom-nebius-uv
RUN apt-get update -qq && apt-get install -y --no-install-recommends {" ".join(packages)} && \\
    {python_setup} && \\
    loom-nebius-uv pip install --python /opt/verifier/bin/python {requirements} && \\
    {identity_setup} && \\
    {workspace_setup} && \\
    rm -rf /var/lib/apt/lists/* /root/.cache
{assets}ENV HOME={home}
# Preserve the base image PATH and agent interpreter; verifier uses its own venv.
USER {uid}:{gid}
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
    identity = resolve_sandbox_identity(
        environment.get("user", "agent"), (environment.get("environment") or {}).get("HOME"),
    ) or SandboxIdentityV1(run_as_user=65532, run_as_group=65532, home="/home/agent")
    dockerfile = _preparation_dockerfile(source.read_text(), bootstrap, workdir, identity)
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
