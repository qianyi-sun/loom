"""Dockerfile instruction boundaries for preflight, without evaluating builds.

Heredoc bodies are opaque input, never Dockerfile directives. This scanner only
identifies instruction headers; Docker/BuildKit remains the syntax authority.
An incomplete boundary raises instead of producing a misleading final stage.
"""

from __future__ import annotations

import re
import shlex
from dataclasses import dataclass


class DockerfileParseError(ValueError):
    def __init__(self, line: int, message: str) -> None:
        self.line = line
        super().__init__(f"Dockerfile:{line}: {message}")


@dataclass(frozen=True)
class DockerfileInstruction:
    line: int
    keyword: str
    arguments: str


def _heredoc_markers(arguments: str, line: int) -> list[tuple[str, bool]]:
    # JSON exec forms contain string literals, not Dockerfile heredocs.
    if re.match(r"(?:--\S+\s+)*\[", arguments):
        return []
    markers: list[tuple[str, bool]] = []
    position = 0
    quote = ""
    while position < len(arguments):
        char = arguments[position]
        if char == "\\" and quote != "'":
            position += 2
            continue
        if quote:
            if char == quote:
                quote = ""
            position += 1
            continue
        if char in "\"'":
            quote = char
            position += 1
            continue
        if not arguments.startswith("<<", position):
            position += 1
            continue
        position += 2
        if position < len(arguments) and arguments[position] == "<":
            # Bash here-strings have no following body.
            position += 1
            continue
        strip_tabs = arguments[position:position + 1] == "-"
        position += int(strip_tabs)
        while position < len(arguments) and arguments[position].isspace():
            position += 1
        start = position
        marker_quote = ""
        while position < len(arguments):
            char = arguments[position]
            if char == "\\" and marker_quote != "'":
                position += 2
                continue
            if marker_quote:
                if char == marker_quote:
                    marker_quote = ""
            elif char in "\"'":
                marker_quote = char
            elif char.isspace() or char in ";|&<>()":
                break
            position += 1
        try:
            words = shlex.split(arguments[start:position])
        except ValueError as exc:
            raise DockerfileParseError(line, "invalid heredoc delimiter") from exc
        if len(words) != 1:
            raise DockerfileParseError(line, "missing heredoc delimiter")
        markers.append((words[0], strip_tabs))
    return markers


def dockerfile_instructions(text: str) -> tuple[DockerfileInstruction, ...]:
    """Read continued headers and consume each RUN/COPY/ADD heredoc body.

    Line numbers refer to the original source. Preserve exact heredoc delimiter
    matching, including quotes and the tab-only stripping rule for ``<<-``.
    """
    lines = text.splitlines()
    instructions: list[DockerfileInstruction] = []
    position = 0
    escape = "\\"
    directives = True
    while position < len(lines):
        raw = lines[position]
        position += 1
        stripped = raw.strip()
        if directives and (match := re.fullmatch(r"#\s*escape\s*=\s*([\\`])", stripped, re.I)):
            escape = match[1]
            continue
        if not stripped or stripped.startswith("#"):
            # Only consecutive parser directives at the top can set escape.
            if not re.match(r"#\s*(?:syntax|check)\s*=", stripped, re.I):
                directives = False
            continue
        directives = False
        start = position
        parts: list[str] = []
        while True:
            line = raw.rstrip()
            trailing_escapes = len(line) - len(line.rstrip(escape))
            continued = trailing_escapes % 2 == 1
            parts.append(line[:-1] if continued else line)
            if not continued:
                break
            while position < len(lines) and (
                not lines[position].strip() or lines[position].lstrip().startswith("#")
            ):
                position += 1
            if position == len(lines):
                raise DockerfileParseError(start, "incomplete instruction continuation")
            raw = lines[position]
            position += 1
        header = "".join(parts).strip()
        words = header.split(maxsplit=1)
        keyword = words[0].upper()
        arguments = words[1] if len(words) > 1 else ""
        if keyword in {"RUN", "COPY", "ADD"}:
            for delimiter, strip_tabs in _heredoc_markers(arguments, start):
                while position < len(lines):
                    body_line = lines[position]
                    position += 1
                    if (body_line.lstrip("\t") if strip_tabs else body_line) == delimiter:
                        break
                else:
                    raise DockerfileParseError(start, f"unterminated heredoc {delimiter!r}")
        instructions.append(DockerfileInstruction(start, keyword, arguments))
    return tuple(instructions)
