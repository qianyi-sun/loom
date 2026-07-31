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


def test_domain_topology_adr_uses_only_path_prefix_routes() -> None:
    text = (ROOT / "docs/architecture/adr/env-domain-topology.md").read_text(
        encoding="utf-8"
    )

    assert "https://yylx.world/dev" in text
    assert "https://yylx.world/staging" in text
    assert "https://yylx.world/prod" in text
    assert "must not appear" in text
