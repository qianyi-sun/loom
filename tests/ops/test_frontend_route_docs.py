from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_contributing_uses_canonical_frontend_routes() -> None:
    text = (ROOT / "CONTRIBUTING.md").read_text(encoding="utf-8")

    assert "dev.yylx.world" not in text
    assert "staging.yylx.world" not in text
    assert "prod.yylx.world" not in text
    assert "https://yylx.world/dev" in text
    assert "https://yylx.world/staging" in text
    assert "https://yylx.world/prod" in text


def test_current_environment_docs_use_only_path_prefix_routes() -> None:
    naming = (ROOT / "docs/architecture/env-naming-convention.md").read_text(
        encoding="utf-8",
    )
    runbook = (ROOT / "docs/runbooks/operator-runbook.md").read_text(
        encoding="utf-8",
    )

    assert "https://yylx.world/<short_name>" in naming
    for route in (
        "https://yylx.world/dev",
        "https://yylx.world/staging",
        "https://yylx.world/prod",
    ):
        assert route in runbook
    for stale_route in ("dev.yylx.world", "staging.yylx.world", "prod.yylx.world"):
        assert stale_route not in naming
        assert stale_route not in runbook
