import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
WEB_SOURCE = ROOT / "web" / "src"
TABS_PRIMITIVE = WEB_SOURCE / "components" / "Tabs.tsx"

TAB_ROLE = re.compile(
    r"\brole\s*=\s*(?:[\"'](?:tab|tablist|tabpanel)[\"']|"
    r"\{\s*[\"'](?:tab|tablist|tabpanel)[\"']\s*\})"
)


def test_production_tab_semantics_are_owned_by_shared_tabs_primitive() -> None:
    offenders: list[str] = []
    for path in sorted(WEB_SOURCE.rglob("*.tsx")):
        if path == TABS_PRIMITIVE or "__tests__" in path.parts:
            continue
        if TAB_ROLE.search(path.read_text()):
            offenders.append(str(path.relative_to(ROOT)))

    assert offenders == [], (
        "Production ARIA tab roles must use web/src/components/Tabs.tsx; "
        f"found page-local semantics in {offenders}"
    )


def test_tabs_primitive_owns_the_complete_tab_relationship() -> None:
    source = TABS_PRIMITIVE.read_text()

    assert 'role="tablist"' in source
    assert 'role="tab"' in source
    assert '"tabpanel"' in source
    assert "aria-selected={selected}" in source
    assert "aria-controls={panelId}" in source
    assert re.search(r"aria-labelledby=\{[^}]*tabId", source)


def test_tabs_focus_recovery_does_not_snapshot_active_element_during_render() -> None:
    source = TABS_PRIMITIVE.read_text()

    assert ".activeElement" not in source
