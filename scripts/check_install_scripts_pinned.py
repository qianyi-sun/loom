"""Fail CI if any AgentAdapter.install_script uses unpinned versions
OR drifts from src/loom_service/agent_catalog._ADAPTER_REQUIRED_PACKAGES
(#317 Phase 1 + Phase 3a).

Two independent checks:

1. **Pinned versions** (Phase 1) — forbidden shapes:
     - `npm install -g <pkg>` without `@<version>`
     - `pip install <pkg>` without `==<version>` or `@git+<tag>`
     - `uv tool install <pkg>` without `==<version>`
   Allowed:
     - `apt-get install -y <pkg>` (OS pkgs — system manages versions)
     - `apk add` / `yum install` likewise
     - `curl <url>` (assume reviewer judgment on the URL)

2. **Catalog cross-check** (Phase 3a) — for every adapter listed in
   `_ADAPTER_REQUIRED_PACKAGES`, the corresponding adapter file's
   `install_script` text must contain each declared package name as
   a substring. Catches the easy drift case where someone bumps
   one source-of-truth but not the other (real Phase 1 bug: the
   `openhands` adapter declared `openhands-sdk` in the catalog but
   the install_script installed `openhands-ai`).

Why AST-parse rather than import: importing the adapter modules
drags in heavy deps (litellm, openhands, huggingface_hub) into the
lint CI job. AST parsing reads only the source files and extracts
the `install_script` string literal directly — fast + no irrelevant
imports. Exit code 0 = all checks pass; 1 = violations printed.
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
AGENT_CATALOG = (
    Path(__file__).resolve().parents[1]
    / "src" / "loom_service" / "agent_catalog.py"
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
                    if pkg in {
                        "-r",
                        "--requirement",
                        "-c",
                        "--constraint",
                        "--python",
                    }:
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

        # gpg --dearmor -o /etc/apt/keyrings/... must be noninteractive
        # and safe to rerun against task images that already have the
        # same keyring file.
        if (
            "gpg" in tokens
            and "--dearmor" in tokens
            and "-o" in tokens
            and any("/etc/apt/keyrings/" in token for token in tokens)
            and ("--batch" not in tokens or "--yes" not in tokens)
        ):
            violations.append(
                "gpg keyring writes must include `--batch --yes` so "
                "adapter install_script values are noninteractive and "
                "idempotent: " + stripped,
            )

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
    for top_node in tree.body:
        if isinstance(top_node, ast.Assign):
            for target in top_node.targets:
                if (isinstance(target, ast.Name)
                        and isinstance(top_node.value, ast.Constant)
                        and isinstance(top_node.value.value, str)):
                    module_constants[target.id] = top_node.value.value

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


def _iter_imported_install_script_modules(
    source: str,
    *,
    src_file: Path,
) -> list[Path]:
    """Return sibling adapter module files that export imported
    install_script constants.

    Adapter modules sometimes share install script text through a private
    `loom_launcher.adapters.*` module. CI still needs to scan the real script
    and catalog-drift checks still need to see the package names.
    """
    tree = ast.parse(source)
    out: list[Path] = []
    for node in tree.body:
        if not isinstance(node, ast.ImportFrom):
            continue
        imports_install_script = any(
            "INSTALL_SCRIPT" in (alias.asname or alias.name)
            for alias in node.names
        )
        if not imports_install_script:
            continue

        module = node.module or ""
        candidate: Path | None = None
        if node.level:
            rel_parts = [part for part in module.split(".") if part]
            if rel_parts:
                candidate = src_file.parent.joinpath(*rel_parts).with_suffix(".py")
        elif module.startswith("loom_launcher.adapters."):
            rel = module.removeprefix("loom_launcher.adapters.")
            rel_parts = [part for part in rel.split(".") if part]
            if rel_parts:
                candidate = ADAPTERS_DIR.joinpath(*rel_parts).with_suffix(".py")

        if candidate is not None and candidate.is_file():
            out.append(candidate)
    return out


def _extract_install_script_from_file(
    src_file: Path,
    *,
    _seen: set[Path] | None = None,
) -> list[str]:
    """Extract install scripts from `src_file` and imported sibling
    adapter modules that provide shared install script constants."""
    seen = set() if _seen is None else _seen
    resolved = src_file.resolve()
    if resolved in seen:
        return []
    seen.add(resolved)

    source = src_file.read_text()
    scripts = _extract_install_script_from_module(source)
    for imported in _iter_imported_install_script_modules(
        source,
        src_file=src_file,
    ):
        scripts.extend(_extract_install_script_from_file(imported, _seen=seen))
    return scripts


def _extract_required_packages_from_catalog(
    source: str,
) -> dict[str, tuple[str, ...]]:
    """AST-parse src/loom_service/agent_catalog.py and return the
    `_ADAPTER_REQUIRED_PACKAGES: dict[str, tuple[str, ...]]` mapping.
    Returns {} if the constant isn't found (lint then silently passes
    the cross-check, since there's nothing to compare against)."""
    tree = ast.parse(source)
    for node in tree.body:
        if not isinstance(node, ast.AnnAssign | ast.Assign):
            continue
        targets = (
            [node.target] if isinstance(node, ast.AnnAssign) else node.targets
        )
        for target in targets:
            if not (isinstance(target, ast.Name)
                    and target.id == "_ADAPTER_REQUIRED_PACKAGES"):
                continue
            if node.value is None or not isinstance(node.value, ast.Dict):
                return {}
            result: dict[str, tuple[str, ...]] = {}
            for k_node, v_node in zip(
                node.value.keys, node.value.values, strict=True,
            ):
                if not (isinstance(k_node, ast.Constant)
                        and isinstance(k_node.value, str)):
                    continue
                pkgs: list[str] = []
                if isinstance(v_node, ast.Tuple):
                    for elt in v_node.elts:
                        if (isinstance(elt, ast.Constant)
                                and isinstance(elt.value, str)):
                            pkgs.append(elt.value)
                result[k_node.value] = tuple(pkgs)
            return result
    return {}


# Map adapter slug → source filename (slugs are kebab-case, filenames
# snake_case). Generated by reading adapter files and matching their
# class-level `name = "..."` literal so we don't hand-maintain a table.
def _build_slug_to_filename_map() -> dict[str, str]:
    out: dict[str, str] = {}
    for src_file in sorted(ADAPTERS_DIR.glob("*.py")):
        if src_file.name == "__init__.py":
            continue
        try:
            tree = ast.parse(src_file.read_text())
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            if not isinstance(node, ast.ClassDef):
                continue
            for stmt in node.body:
                if not isinstance(stmt, ast.AnnAssign | ast.Assign):
                    continue
                targets = (
                    [stmt.target] if isinstance(stmt, ast.AnnAssign)
                    else stmt.targets
                )
                for target in targets:
                    if not (isinstance(target, ast.Name)
                            and target.id == "name"
                            and stmt.value is not None
                            and isinstance(stmt.value, ast.Constant)
                            and isinstance(stmt.value.value, str)):
                        continue
                    out[stmt.value.value] = src_file.name
    return out


def _check_catalog_drift(
    required_packages: dict[str, tuple[str, ...]],
    slug_to_filename: dict[str, str],
) -> list[tuple[Path, str]]:
    """For each adapter in the catalog, verify the corresponding
    adapter file's install_script contains every declared package
    name as a substring. Returns a list of (file, violation_msg)."""
    violations: list[tuple[Path, str]] = []
    for slug, pkgs in sorted(required_packages.items()):
        filename = slug_to_filename.get(slug)
        if filename is None:
            # Catalog claims this adapter but we can't find a matching
            # source file — surface as a violation so the catalog doesn't
            # silently keep stale entries after an adapter is removed.
            violations.append((
                AGENT_CATALOG,
                f"adapter slug {slug!r} in _ADAPTER_REQUIRED_PACKAGES "
                "but no matching adapter source file",
            ))
            continue
        src_file = ADAPTERS_DIR / filename
        scripts = _extract_install_script_from_file(src_file)
        # `hello` and other no-install adapters have no install_script:
        # skip the cross-check — they wouldn't appear in the catalog
        # dict if they had no real packages, but we tolerate either way.
        if not scripts:
            continue
        combined = "\n".join(scripts)
        for pkg in pkgs:
            if pkg not in combined:
                violations.append((
                    src_file,
                    f"adapter {slug!r}: install_script does not install "
                    f"declared package {pkg!r} "
                    f"(per _ADAPTER_REQUIRED_PACKAGES in agent_catalog.py)",
                ))
    return violations


def main() -> int:
    if not ADAPTERS_DIR.is_dir():
        print(
            f"error: adapters dir not found at {ADAPTERS_DIR}",
            file=sys.stderr,
        )
        return 2

    pin_violations: list[tuple[Path, str]] = []
    files_scanned = 0

    for src_file in sorted(ADAPTERS_DIR.glob("*.py")):
        if src_file.name == "__init__.py":
            continue
        files_scanned += 1
        scripts = _extract_install_script_from_file(src_file)
        for script in scripts:
            for violation in _scan_install_script(script):
                pin_violations.append((src_file, violation))

    # Phase 3a: cross-check install_script ↔ agent_catalog catalog.
    drift_violations: list[tuple[Path, str]] = []
    if AGENT_CATALOG.is_file():
        required_packages = _extract_required_packages_from_catalog(
            AGENT_CATALOG.read_text(),
        )
        slug_to_filename = _build_slug_to_filename_map()
        drift_violations = _check_catalog_drift(
            required_packages, slug_to_filename,
        )

    repo_root = ADAPTERS_DIR.parents[3]
    if pin_violations:
        print(
            f"FAIL: {len(pin_violations)} unpinned package(s) in "
            "adapter install_script values (#317):",
            file=sys.stderr,
        )
        for path, msg in pin_violations:
            print(f"  {path.relative_to(repo_root)}: {msg}", file=sys.stderr)
        print(
            "\nPin all npm/pip/uv installs to a specific version. "
            "Example: `npm install -g @anthropic-ai/[email protected]` "
            "instead of `npm install -g @anthropic-ai/claude-code`.",
            file=sys.stderr,
        )

    if drift_violations:
        print(
            f"FAIL: {len(drift_violations)} adapter/catalog drift "
            "violation(s) (#317 Phase 3a):",
            file=sys.stderr,
        )
        for path, msg in drift_violations:
            print(f"  {path.relative_to(repo_root)}: {msg}", file=sys.stderr)
        print(
            "\n_ADAPTER_REQUIRED_PACKAGES in agent_catalog.py is the "
            "declarative source of truth. Either update the adapter's "
            "install_script to install the declared package, or update "
            "the catalog dict to match what the script installs.",
            file=sys.stderr,
        )

    if pin_violations or drift_violations:
        return 1

    print(
        f"OK: scanned {files_scanned} adapter file(s); all pinned and "
        "consistent with agent_catalog.",
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
