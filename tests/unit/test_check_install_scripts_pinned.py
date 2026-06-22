"""Unit tests for scripts/check_install_scripts_pinned.py (#317)."""
from __future__ import annotations

import importlib.util
from pathlib import Path

_SCRIPT_PATH = (
    Path(__file__).resolve().parents[2]
    / "scripts" / "check_install_scripts_pinned.py"
)
_spec = importlib.util.spec_from_file_location("_lint_mod", _SCRIPT_PATH)
assert _spec is not None and _spec.loader is not None
_lint_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_lint_mod)

_scan = _lint_mod._scan_install_script
_extract = _lint_mod._extract_install_script_from_module


def test_pinned_npm_install_passes() -> None:
    assert _scan('npm install -g "@anthropic-ai/claude-code@2.1.183"') == []


def test_unpinned_npm_install_fails() -> None:
    violations = _scan("npm install -g @anthropic-ai/claude-code")
    assert any("@anthropic-ai/claude-code" in v for v in violations)


def test_pinned_pip_install_passes() -> None:
    assert _scan("pip install --no-cache-dir openhands-ai==1.8.0") == []


def test_unpinned_pip_install_fails() -> None:
    violations = _scan("pip install --no-cache-dir openhands-ai")
    assert any("openhands-ai" in v for v in violations)


def test_pip_install_with_git_spec_passes() -> None:
    assert _scan(
        "pip install foo@git+https://example.com/foo.git@abc123",
    ) == []


def test_pinned_uv_tool_install_passes() -> None:
    assert _scan("uv tool install kimi-cli==0.18.0") == []


def test_unpinned_uv_tool_install_fails() -> None:
    violations = _scan("uv tool install kimi-cli")
    assert any("kimi-cli" in v for v in violations)


def test_apt_get_ignored() -> None:
    """OS package managers manage their own versions."""
    assert _scan("apt-get install -y curl bash nodejs") == []


def test_apk_ignored() -> None:
    assert _scan("apk add --no-cache python3 py3-pip") == []


def test_pip_install_with_requirements_file_passes() -> None:
    """`-r requirements.txt` is acceptable — the file pins versions."""
    assert _scan("pip install -r requirements.txt") == []


def test_line_continuations_collapsed() -> None:
    """Multi-line install with backslash continuation should tokenize
    correctly. Previously the lint flagged `\\` as a package."""
    script = (
        'pip install --no-cache-dir \\\n'
        '  "openhands-ai==1.8.0" \\\n'
        '  "loom-launcher==0.1.0"'
    )
    assert _scan(script) == []


def test_extract_from_module_handles_fstring_with_constants(tmp_path) -> None:
    """f-string install_scripts that interpolate module-level string
    constants should be rendered before scanning. Avoids the
    `[email protected]` display-mangling issue."""
    src = tmp_path / "fake_adapter.py"
    src.write_text("""
_PKG = "@foo/bar"
_VER = "1.2.3"
_INSTALL_SCRIPT = f'''
npm install -g "{_PKG}@{_VER}"
'''
""")
    scripts = _extract(src.read_text())
    assert len(scripts) == 1
    rendered = scripts[0]
    assert "@foo/bar@1.2.3" in rendered
    # And scanning the rendered script should find no violations
    assert _scan(rendered) == []
