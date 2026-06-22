"""Fail CI if any AgentAdapter.install_script uses unpinned versions (#317).

Forbidden:
  - `npm install -g <pkg>` without `@<version>`
  - `pip install <pkg>` without `==<version>` or `@git+<sha>`
  - `uv tool install <pkg>` without `==<version>`

Allowed:
  - `apt-get install -y <pkg>` (OS packages; system manages versions)
  - `apk add` / `yum install` likewise
  - `curl <url>` (assume reviewer judgment on the URL)

Why AST-parse rather than import: importing the adapter modules
drags in heavy deps (litellm, openhands, huggingface_hub) into the
lint CI job. AST parsing reads only the source files and extracts
the `install_script` string literal directly — fast + no irrelevant
imports. Exit code 0 = all scripts pinned; 1 = violations printed.
"""
from __future__ import annotations

import ast
import re
import sys
from pathlib import Path

ADAPTERS_DIR = (
    Path(__file__).resolve().parents[1]
    / "packages" / "loom-launcher" / "loom_launcher" / "adapters"
)

# Regex patterns for the forbidden shapes.
_RE_NPM_INSTALL_G = re.compile(
    r"\bnpm\s+install\s+-g\s+([^\s@][^\s]*)(?!@[^\s])",
)
_RE_PIP_INSTALL = re.compile(
    r"\bpip\s+install\s+(?:[^|&]*?\s)?([a-zA-Z0-9_.\-\[\]]+)",
)
_RE_UV_TOOL_INSTALL = re.compile(
    r"\buv\s+tool\s+install\s+(?:[^|&]*?\s)?([a-zA-Z0-9_.\-]+)",
)

# pip-arg flags we should skip when scanning tokens for an "is this
# a package name?" — these aren't packages.
_PIP_FLAGS_TO_SKIP = frozenset({
    "install", "-r", "--no-cache-dir", "--break-system-packages",
    "--upgrade", "-U", "--user", "--no-deps", "--prefer-binary",
    "--no-build-isolation",
})


def _looks_pinned_pip(token: str) -> bool:
    """A pip package spec is acceptable if it contains `==` (pinned
    version), `@` (URL/git spec), or `-r` (requirements file)."""
    return "==" in token or "@" in token or token.startswith("-")


def _looks_pinned_npm(token: str) -> bool:
    """npm: package@version or @scope/package@version. Just verify
    there's an `@` after the package-name portion."""
    # @scope/name needs an `@` AFTER the slash too: @anthropic-ai/[email protected]
    if token.startswith("@"):
        # Drop the leading @ before scope name, then look for next @
        rest = token[1:]
        return "@" in rest
    return "@" in token


def _looks_pinned_uv(token: str) -> bool:
    return "==" in token or "@" in token


def _join_continuations(script: str) -> str:
    """Collapse shell line continuations (`\\` at end of line) into
    single logical lines so the tokenizer sees the full command."""
    out: list[str] = []
    buf: list[str] = []
    for line in script.splitlines():
        if line.rstrip().endswith("\\"):
            buf.append(line.rstrip()[:-1])
        else:
            buf.append(line)
            out.append(" ".join(buf))
            buf = []
    if buf:
        out.append(" ".join(buf))
    return "\n".join(out)


def _scan_install_script(script: str) -> list[str]:
    """Return a list of human-readable violation messages, empty if all clean."""
    violations: list[str] = []
    script = _join_continuations(script)

    # Tokenize lines naively. Real shell parsing isn't needed — we
    # just want to spot `npm install -g <token>` etc.
    for line in script.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue

        # npm install -g
        for word_index, _word in enumerate(stripped.split()):
            if word_index < 3:
                continue
            tokens = stripped.split()
            # find pattern `npm install -g <pkg>...`
            for i in range(len(tokens) - 3):
                if (tokens[i] == "npm" and tokens[i + 1] == "install"
                        and tokens[i + 2] == "-g"):
                    for pkg in tokens[i + 3:]:
                        if pkg.startswith("-") or pkg in {"&&", "||", ";"}:
                            break
                        if not _looks_pinned_npm(pkg):
                            violations.append(
                                f"npm package {pkg!r} not pinned "
                                "(use `<pkg>@<version>`): " + stripped,
                            )
                    break  # one npm install per line

        # pip install
        tokens = stripped.split()
        if "pip" in tokens:
            try:
                idx = tokens.index("pip")
            except ValueError:
                idx = -1
            if 0 <= idx < len(tokens) - 1 and tokens[idx + 1] == "install":
                pip_tail = tokens[idx + 2:]
                skip_next = False
                for _j, pkg in enumerate(pip_tail):
                    if skip_next:
                        skip_next = False
                        continue
                    if pkg in {"&&", "||", ";"}:
                        break
                    # `-r requirements.txt` / `--requirement file.txt`:
                    # the next token is a file path, not a package.
                    # Either way, treat as acceptable (the file pins
                    # versions).
                    if pkg in {"-r", "--requirement", "-c", "--constraint"}:
                        skip_next = True
                        continue
                    if pkg in _PIP_FLAGS_TO_SKIP:
                        continue
                    if pkg.startswith("-"):
                        continue
                    if not _looks_pinned_pip(pkg):
                        violations.append(
                            f"pip package {pkg!r} not pinned "
                            "(use `<pkg>==<version>` or `<pkg>@git+<sha>`): "
                            + stripped,
                        )

        # uv tool install
        if "uv" in tokens:
            for i in range(len(tokens) - 3):
                if (tokens[i] == "uv" and tokens[i + 1] == "tool"
                        and tokens[i + 2] == "install"):
                    for pkg in tokens[i + 3:]:
                        if pkg.startswith("-") or pkg in {"&&", "||", ";"}:
                            break
                        if not _looks_pinned_uv(pkg):
                            violations.append(
                                f"uv tool package {pkg!r} not pinned "
                                "(use `<pkg>==<version>`): " + stripped,
                            )
                    break

    return violations


def _extract_install_script_from_module(source: str) -> list[str]:
    """Walk the AST and return all string-literal install_script values
    assigned at module or class scope. Handles both plain string
    constants and f-strings (JoinedStr — we render the JoinedStr by
    substituting any FormattedValue whose source is a Name pointing
    at a module-level string constant, which is how we sidestep
    `[email protected]` patterns getting mangled by display layers)."""
    scripts: list[str] = []
    tree = ast.parse(source)

    # Pre-collect module-level string constants for f-string rendering.
    module_constants: dict[str, str] = {}
    for node in tree.body:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if (isinstance(target, ast.Name)
                        and isinstance(node.value, ast.Constant)
                        and isinstance(node.value.value, str)):
                    module_constants[target.id] = node.value.value

    def _render(value: ast.expr) -> str | None:
        if isinstance(value, ast.Constant) and isinstance(value.value, str):
            return value.value
        if isinstance(value, ast.JoinedStr):
            parts: list[str] = []
            for piece in value.values:
                if (isinstance(piece, ast.Constant)
                        and isinstance(piece.value, str)):
                    parts.append(piece.value)
                elif isinstance(piece, ast.FormattedValue):
                    expr = piece.value
                    if (isinstance(expr, ast.Name)
                            and expr.id in module_constants):
                        parts.append(module_constants[expr.id])
                    else:
                        # Unknown FormattedValue source — keep as a
                        # placeholder so the regex doesn't match
                        # accidentally. Tests for unknown shapes can
                        # extend this.
                        parts.append(f"<{ast.dump(expr)}>")
                else:
                    parts.append(f"<{type(piece).__name__}>")
            return "".join(parts)
        return None

    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if (isinstance(target, ast.Name)
                        and "INSTALL_SCRIPT" in target.id):
                    rendered = _render(node.value)
                    if rendered is not None:
                        scripts.append(rendered)
        if isinstance(node, ast.AnnAssign):
            target = node.target
            if (isinstance(target, ast.Name)
                    and target.id == "install_script"
                    and node.value is not None):
                rendered = _render(node.value)
                if rendered is not None:
                    scripts.append(rendered)
    return scripts


def main() -> int:
    if not ADAPTERS_DIR.is_dir():
        print(
            f"error: adapters dir not found at {ADAPTERS_DIR}",
            file=sys.stderr,
        )
        return 2

    total_violations: list[tuple[Path, str]] = []
    files_scanned = 0

    for src_file in sorted(ADAPTERS_DIR.glob("*.py")):
        if src_file.name == "__init__.py":
            continue
        files_scanned += 1
        scripts = _extract_install_script_from_module(src_file.read_text())
        for script in scripts:
            for violation in _scan_install_script(script):
                total_violations.append((src_file, violation))

    if total_violations:
        print(
            f"FAIL: {len(total_violations)} unpinned package(s) in "
            f"adapter install_script values (#317):",
            file=sys.stderr,
        )
        for path, msg in total_violations:
            print(f"  {path.relative_to(ADAPTERS_DIR.parents[3])}: {msg}",
                  file=sys.stderr)
        print(
            "\nPin all npm/pip/uv installs to a specific version. "
            "Example: `npm install -g @anthropic-ai/[email protected]` "
            "instead of `npm install -g @anthropic-ai/claude-code`.",
            file=sys.stderr,
        )
        return 1

    print(f"OK: scanned {files_scanned} adapter file(s); all pinned.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
