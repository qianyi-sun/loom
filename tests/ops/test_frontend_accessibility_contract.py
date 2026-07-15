from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
WEB_SOURCE = REPO_ROOT / "web" / "src"

DIALOG_SEMANTIC_PATTERNS = (
    re.compile(r"<dialog(?:\s|>)", re.IGNORECASE),
    re.compile(r"\baria-modal\s*=", re.IGNORECASE),
    re.compile(
        r"""\brole\s*=\s*(?:
            ['\"](?:alert)?dialog['\"]
            |
            \{\s*[^}\n]*dialog[^}\n]*\}
        )""",
        re.IGNORECASE | re.VERBOSE,
    ),
)


def _has_dialog_semantics(source: str) -> bool:
    return any(pattern.search(source) for pattern in DIALOG_SEMANTIC_PATTERNS)


def test_dialog_semantics_stay_behind_the_shared_modal_primitive() -> None:
    offenders = {
        path.relative_to(REPO_ROOT)
        for path in WEB_SOURCE.rglob("*.tsx")
        if "__tests__" not in path.parts and _has_dialog_semantics(path.read_text(encoding="utf-8"))
    }

    assert offenders == {Path("web/src/components/Modal.tsx")}


def test_dialog_contract_recognizes_common_jsx_bypasses() -> None:
    for source in (
        '<div role="dialog">',
        "<div role={'alertdialog'}>",
        "<div role={DIALOG_ROLE}>",
        '<section aria-modal="true">',
        "<dialog open>",
    ):
        assert _has_dialog_semantics(source), source
