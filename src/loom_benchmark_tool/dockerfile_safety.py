"""Static safety checks for imported task Dockerfiles."""

from __future__ import annotations

import re
import shlex
from collections.abc import Iterator
from pathlib import Path

_PYTORCH_INDEX_RE = re.compile(r"https?://download\.pytorch\.org/whl(?:/|\b)")
_PYTORCH_INDEX_PACKAGES = frozenset({
    "torch",
    "torchaudio",
    "torchvision",
})
_FROM_NODE_RE = re.compile(
    r"^\s*FROM\s+(?:--platform=\S+\s+)?(?:\S+/)?node:(\d+)(?:\D|$)",
    re.IGNORECASE,
)
_NODESOURCE_MAJOR_RE = re.compile(r"\b(?:setup|node)_(\d+)\.x\b")
_NPM_LATEST_RE = re.compile(r"\bnpm\s+(?:install|i)\b[^\n]*\bnpm@latest\b")
_TRAILING_TRUE_RE = re.compile(r"(?:^|\s)\|\|\s*true\s*$")
_WORKDIR_APP_RE = re.compile(r"^\s*WORKDIR\s+/app(?:/|\s|$)", re.IGNORECASE)
_APP_WRITE_RE = re.compile(
    r"(?P<cmd>\bcp\b[^&;|]*\s/app(?:/|\s|$)"
    r"|\btar\b[^&;|]*(?:\s-?[A-Za-z]*f[A-Za-z]*\s+|\s--file(?:=|\s+))/app/"
    r"|>\s*/app/)"
)


def validate_task_dir_dockerfiles(task_dir: Path) -> None:
    """Reject converted task bundles with known-fragile Dockerfile patterns."""

    for dockerfile in sorted(task_dir.rglob("Dockerfile*")):
        if not dockerfile.is_file():
            continue
        try:
            text = dockerfile.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            text = dockerfile.read_text()
        validate_dockerfile_text(text, path=dockerfile.relative_to(task_dir))


def validate_dockerfile_text(text: str, *, path: Path | None = None) -> None:
    label = path.as_posix() if path is not None else "Dockerfile"
    fixed_node_major: int | None = None
    app_dir_created = False

    for line_no, instruction in _logical_instructions(text):
        stripped = instruction.strip()
        upper = stripped.upper()
        if upper.startswith("FROM "):
            app_dir_created = False
            fixed_node_major = _node_major_from_from_instruction(stripped)
            continue

        if _WORKDIR_APP_RE.search(stripped):
            app_dir_created = True
            continue

        if not upper.startswith("RUN "):
            continue

        body = stripped[4:].strip()
        if _broad_trailing_true(body):
            raise ValueError(
                f"{label}:{line_no} has trailing || true after a multi-command "
                "setup chain; scope tolerated failures to the optional command",
            )

        for segment in _shell_segments(body):
            _validate_pip_indexes(segment, label=label, line_no=line_no)
            if match := _NODESOURCE_MAJOR_RE.search(segment):
                fixed_node_major = int(match.group(1))
            if fixed_node_major is not None and _NPM_LATEST_RE.search(segment):
                raise ValueError(
                    f"{label}:{line_no} uses moving npm@latest under fixed "
                    f"Node {fixed_node_major}; pin an npm major compatible with "
                    "that Node major",
                )
            for match in _APP_WRITE_RE.finditer(segment):
                before = segment[:match.start()]
                if app_dir_created or _mkdir_creates_app_root(
                    before,
                    app_dir_created=app_dir_created,
                ):
                    continue
                command = " ".join(match.group("cmd").split())
                raise ValueError(
                    f"{label}:{line_no} writes to /app before creating the /app "
                    f"parent directory: {command}",
                )
            if _mkdir_creates_app_root(
                segment,
                app_dir_created=app_dir_created,
            ):
                app_dir_created = True


def _logical_instructions(text: str) -> Iterator[tuple[int, str]]:
    current: list[str] = []
    start_line = 1
    for line_no, raw_line in enumerate(text.splitlines(), start=1):
        line = raw_line.rstrip()
        if not current:
            start_line = line_no
        if line.endswith("\\"):
            current.append(line[:-1].rstrip())
            continue
        current.append(line)
        yield start_line, " ".join(part for part in current if part).strip()
        current = []
    if current:
        yield start_line, " ".join(part for part in current if part).strip()


def _node_major_from_from_instruction(instruction: str) -> int | None:
    if match := _FROM_NODE_RE.search(instruction):
        return int(match.group(1))
    return None


def _validate_pip_indexes(body: str, *, label: str, line_no: int) -> None:
    for segment in _shell_segments(body):
        try:
            tokens = shlex.split(segment)
        except ValueError:
            continue
        install_index = _pip_install_index(tokens)
        if install_index is None:
            continue
        args = tokens[install_index + 1:]
        if not _uses_pytorch_as_sole_index(args):
            continue
        packages = _install_package_names(args)
        non_index_packages = [
            package for package in packages
            if package not in _PYTORCH_INDEX_PACKAGES
        ]
        if non_index_packages:
            formatted = ", ".join(non_index_packages)
            raise ValueError(
                f"{label}:{line_no} uses a package-specific pip index as the "
                f"sole index while installing non-index package(s): {formatted}",
            )


def _shell_segments(body: str) -> list[str]:
    return [
        segment.strip()
        for segment in re.split(r"\s*(?:&&|\|\||;)\s*", body)
        if segment.strip()
    ]


def _pip_install_index(tokens: list[str]) -> int | None:
    for index, token in enumerate(tokens[:-1]):
        command = token.rsplit("/", 1)[-1]
        if _is_pip_command(command) and tokens[index + 1] == "install":
            return index + 1
    for index in range(len(tokens) - 3):
        if (
            _is_python_command(tokens[index].rsplit("/", 1)[-1])
            and tokens[index + 1] == "-m"
            and tokens[index + 2] == "pip"
            and tokens[index + 3] == "install"
        ):
            return index + 3
    return None


def _is_pip_command(command: str) -> bool:
    return bool(re.fullmatch(r"pip(?:\d+(?:\.\d+)?)?", command))


def _is_python_command(command: str) -> bool:
    return bool(re.fullmatch(r"python(?:\d+(?:\.\d+)?)?", command))


def _uses_pytorch_as_sole_index(args: list[str]) -> bool:
    has_pytorch_index = False
    has_extra_index = False
    for index, arg in enumerate(args):
        if arg in {"--extra-index-url", "--extra-index"}:
            has_extra_index = True
        elif arg.startswith("--extra-index-url="):
            has_extra_index = True
        elif arg in {"--index-url", "-i"} and index + 1 < len(args):
            has_pytorch_index = bool(_PYTORCH_INDEX_RE.search(args[index + 1]))
        elif arg.startswith("--index-url="):
            has_pytorch_index = bool(_PYTORCH_INDEX_RE.search(arg))
    return has_pytorch_index and not has_extra_index


def _install_package_names(args: list[str]) -> list[str]:
    names: list[str] = []
    skip_next = False
    options_with_values = {
        "--index-url",
        "--extra-index",
        "--extra-index-url",
        "-i",
        "-r",
        "--requirement",
        "-c",
        "--constraint",
        "-f",
        "--find-links",
    }
    for arg in args:
        if skip_next:
            skip_next = False
            continue
        if arg in options_with_values:
            skip_next = True
            continue
        if arg.startswith("-"):
            continue
        if "://" in arg or arg.startswith((".", "/")):
            continue
        if match := re.match(r"([A-Za-z0-9_.-]+)", arg):
            names.append(match.group(1).lower().replace("_", "-"))
    return names


def _mkdir_creates_app_root(segment: str, *, app_dir_created: bool) -> bool:
    try:
        tokens = shlex.split(segment)
    except ValueError:
        return False

    for index, token in enumerate(tokens):
        if token.rsplit("/", 1)[-1] != "mkdir":
            continue
        has_parents = False
        targets: list[str] = []
        remaining = tokens[index + 1:]
        while remaining:
            arg = remaining.pop(0)
            if arg == "--":
                targets.extend(remaining)
                break
            if arg == "--parents":
                has_parents = True
                continue
            if arg.startswith("--"):
                continue
            if arg.startswith("-") and arg != "-":
                if "p" in arg[1:]:
                    has_parents = True
                continue
            targets.append(arg)

        for target in targets:
            normalized = target.rstrip("/") or target
            if normalized == "/app":
                return True
            if normalized.startswith("/app/") and (has_parents or app_dir_created):
                return True
    return False


def _broad_trailing_true(body: str) -> bool:
    match = _TRAILING_TRUE_RE.search(body)
    if not match:
        return False
    before = body[:match.start()].strip()
    if _is_single_optional_git_remote_remove(before):
        return False
    return "&&" in before or ";" in before or _APP_WRITE_RE.search(before) is not None


def _is_single_optional_git_remote_remove(command: str) -> bool:
    command = command.strip()
    if command.startswith("(") and command.endswith(")"):
        command = command[1:-1].strip()
    return bool(re.fullmatch(r"git\s+remote\s+remove\s+origin(?:\s+2>/dev/null)?", command))
