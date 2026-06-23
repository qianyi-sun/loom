from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_db_facing_images_include_migrations_for_schema_startup() -> None:
    dockerfiles = [
        ROOT / "deploy" / "Dockerfile.control-plane",
        ROOT / "deploy" / "Dockerfile.gateway",
        ROOT / "deploy" / "Dockerfile.service",
    ]

    for dockerfile in dockerfiles:
        text = dockerfile.read_text()
        assert "COPY migrations ./migrations" in text, dockerfile
