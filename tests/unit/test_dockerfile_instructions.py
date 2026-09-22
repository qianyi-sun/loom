"""Dockerfile boundary regressions independent of the diagnostic consumers."""

import pytest

from loom.dockerfile_instructions import DockerfileParseError, dockerfile_instructions


@pytest.mark.parametrize(
    "header",
    [
        'RUN echo "<<EOF"',
        "RUN echo '<<EOF'",
        r"RUN echo \<\<EOF",
        r'RUN ["python3", "-c", "print(\"<<EOF\")"]',
        'COPY ["<<EOF", "/tmp/file"]',
        "RUN bash -c 'cat <<< data'",
    ],
)
def test_quoted_or_json_strings_do_not_consume_following_instructions(header: str) -> None:
    instructions = dockerfile_instructions(header + "\nFROM alpine:3.20\n")
    assert [(item.line, item.keyword) for item in instructions] == [(1, header.split()[0]), (2, "FROM")]
    assert instructions[-1].arguments == "alpine:3.20"


def test_escape_directive_comments_and_continuations_preserve_instruction_boundary() -> None:
    instructions = dockerfile_instructions(
        "# syntax=docker/dockerfile:1\n"
        "# escape=`\n"
        "FROM --platform=linux/amd64 `\n"
        "# a comment does not end the continued instruction\n"
        "  python:3.9-slim AS task\n"
        "RUN cat <<E'OF'\nFROM fake\nEOF\n"
        "WORKDIR /app\n"
    )
    assert [(item.line, item.keyword) for item in instructions] == [(3, "FROM"), (6, "RUN"), (9, "WORKDIR")]
    assert instructions[0].arguments.split() == ["--platform=linux/amd64", "python:3.9-slim", "AS", "task"]


@pytest.mark.parametrize("ending", [" EOF", "EOF ", "\tEOF"])
def test_normal_heredoc_requires_exact_terminator(ending: str) -> None:
    with pytest.raises(DockerfileParseError, match="unterminated heredoc"):
        dockerfile_instructions("RUN cat <<EOF\nFROM fake\n" + ending + "\n")


def test_tab_stripped_heredoc_does_not_strip_spaces() -> None:
    with pytest.raises(DockerfileParseError, match="unterminated heredoc"):
        dockerfile_instructions("RUN cat <<-EOF\nFROM fake\n EOF\n")


def test_crlf_and_body_continuations_are_opaque() -> None:
    instructions = dockerfile_instructions(
        "FROM ubuntu:24.04\r\nRUN cat <<EOF\r\nFROM ignored\\\r\nEOF\r\nWORKDIR /app\r\n"
    )
    assert [(item.line, item.keyword) for item in instructions] == [(1, "FROM"), (2, "RUN"), (5, "WORKDIR")]


@pytest.mark.parametrize("source", ["RUN cat <<\n", "RUN cat <<'EOF\n", "FROM ubuntu:24.04 \\\n"])
def test_incomplete_boundaries_are_rejected(source: str) -> None:
    with pytest.raises(DockerfileParseError):
        dockerfile_instructions(source)
