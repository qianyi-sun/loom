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
_extract_file = _lint_mod._extract_install_script_from_file
_extract_catalog = _lint_mod._extract_required_packages_from_catalog
_check_drift = _lint_mod._check_catalog_drift


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


def test_gpg_keyring_write_requires_noninteractive_overwrite_flags() -> None:
    violations = _scan(
        "curl -fsSL https://deb.nodesource.com/gpgkey/nodesource-repo.gpg.key "
        "| gpg --dearmor -o /etc/apt/keyrings/nodesource.gpg",
    )
    assert any("--batch --yes" in violation for violation in violations)


def test_gpg_keyring_write_with_noninteractive_overwrite_flags_passes() -> None:
    assert _scan(
        "curl -fsSL https://deb.nodesource.com/gpgkey/nodesource-repo.gpg.key "
        "| gpg --batch --yes --dearmor "
        "-o /etc/apt/keyrings/nodesource.gpg",
    ) == []


def test_pip_install_with_requirements_file_passes() -> None:
    """`-r requirements.txt` is acceptable — the file pins versions."""
    assert _scan("pip install -r requirements.txt") == []


def test_uv_pip_install_with_python_interpreter_passes() -> None:
    assert _scan(
        "uv pip install --python /opt/venv/bin/python --no-cache-dir pkg==1.0.0",
    ) == []


def test_line_continuations_collapsed() -> None:
    """Multi-line install with backslash continuation should tokenize
    correctly. Previously the lint flagged `\\` as a package."""
    script = (
        'pip install --no-cache-dir \\\n'
        '  "openhands-ai==1.8.0" \\\n'
        '  "loom-launcher==0.1.0"'
    )
    assert _scan(script) == []


def test_extract_required_packages_from_catalog() -> None:
    """The catalog parser should pick up `_ADAPTER_REQUIRED_PACKAGES`
    as a `dict[str, tuple[str, ...]]` mapping."""
    src = """
_ADAPTER_REQUIRED_PACKAGES: dict[str, tuple[str, ...]] = {
    "alpha": ("alpha-pkg",),
    "beta": ("beta-pkg", "beta-cli"),
}
"""
    out = _extract_catalog(src)
    assert out == {
        "alpha": ("alpha-pkg",),
        "beta": ("beta-pkg", "beta-cli"),
    }


def test_extract_required_packages_returns_empty_when_missing() -> None:
    """If the catalog file doesn't declare the constant, return {}
    so cross-check is a no-op rather than a false-positive fail."""
    out = _extract_catalog("# empty module")
    assert out == {}


def test_check_drift_passes_when_install_script_matches_catalog(
    tmp_path,
) -> None:
    """The drift check should pass when every catalog package is a
    substring of the corresponding adapter's install_script."""
    pkg = "fakepkg"
    adapters = tmp_path / "adapters"
    adapters.mkdir()
    (adapters / "alpha.py").write_text(
        f'_INSTALL_SCRIPT = "npm install -g {pkg}@1.0.0"\n',
    )
    required = {"alpha": (pkg,)}
    slug_to_filename = {"alpha": "alpha.py"}
    # Patch the module's ADAPTERS_DIR for this call
    orig = _lint_mod.ADAPTERS_DIR
    _lint_mod.ADAPTERS_DIR = adapters
    try:
        violations = _check_drift(required, slug_to_filename)
    finally:
        _lint_mod.ADAPTERS_DIR = orig
    assert violations == []


def test_check_drift_fails_when_install_script_diverges(tmp_path) -> None:
    """Catalog says pkgA but install_script installs pkgB → violation.
    Real Phase 1 openhands bug the check is designed to catch."""
    installed_pkg = "actually-installed"
    declared_pkg = "declared-but-missing"
    adapters = tmp_path / "adapters"
    adapters.mkdir()
    (adapters / "alpha.py").write_text(
        f'_INSTALL_SCRIPT = "pip install {installed_pkg}==1.0.0"\n',
    )
    required = {"alpha": (declared_pkg,)}
    slug_to_filename = {"alpha": "alpha.py"}
    orig = _lint_mod.ADAPTERS_DIR
    _lint_mod.ADAPTERS_DIR = adapters
    try:
        violations = _check_drift(required, slug_to_filename)
    finally:
        _lint_mod.ADAPTERS_DIR = orig
    assert len(violations) == 1
    assert declared_pkg in violations[0][1]


def test_check_drift_follows_imported_install_script_constant(tmp_path) -> None:
    """Adapters may share install_script constants from sibling modules;
    drift checks still need to inspect the referenced script."""
    adapters = tmp_path / "adapters"
    adapters.mkdir()
    (adapters / "_shared.py").write_text(
        '_INSTALL_SCRIPT = "pip install declared-pkg==1.0.0"\n',
    )
    (adapters / "alpha.py").write_text(
        """
from loom_launcher.adapters._shared import _INSTALL_SCRIPT


class Alpha:
    name = "alpha"
    install_script: str | None = _INSTALL_SCRIPT
""",
    )
    required = {"alpha": ("declared-pkg",)}
    slug_to_filename = {"alpha": "alpha.py"}
    orig = _lint_mod.ADAPTERS_DIR
    _lint_mod.ADAPTERS_DIR = adapters
    try:
        assert _extract_file(adapters / "alpha.py") == [
            "pip install declared-pkg==1.0.0",
        ]
        violations = _check_drift(required, slug_to_filename)
    finally:
        _lint_mod.ADAPTERS_DIR = orig
    assert violations == []


def test_check_drift_flags_orphan_catalog_entries(tmp_path) -> None:
    """Catalog mentions an adapter slug that has no matching source
    file → violation (probably a stale entry after adapter removal)."""
    adapters = tmp_path / "adapters"
    adapters.mkdir()
    required = {"ghost": ("ghost-pkg",)}
    slug_to_filename: dict[str, str] = {}  # no ghost.py
    orig = _lint_mod.ADAPTERS_DIR
    _lint_mod.ADAPTERS_DIR = adapters
    try:
        violations = _check_drift(required, slug_to_filename)
    finally:
        _lint_mod.ADAPTERS_DIR = orig
    assert len(violations) == 1
    assert "ghost" in violations[0][1]


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
