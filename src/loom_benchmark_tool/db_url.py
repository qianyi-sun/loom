from __future__ import annotations


def normalize_db_url(url: str) -> str:
    """Ensure the URL is the async psycopg variant SQLAlchemy expects."""
    if url.startswith("postgresql+"):
        return url
    return url.replace("postgresql://", "postgresql+psycopg://", 1)
