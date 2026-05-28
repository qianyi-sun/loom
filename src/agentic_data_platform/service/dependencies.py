from __future__ import annotations

from collections.abc import Iterator

from fastapi import HTTPException
from sqlalchemy import Engine
from sqlalchemy.orm import Session

from agentic_data_platform.persistence import session_scope


def build_session_dependency(database_engine: Engine | None):
    def get_session() -> Iterator[Session]:
        if database_engine is None:
            raise HTTPException(status_code=503, detail="Database is not configured")

        with session_scope(database_engine) as session:
            yield session

    return get_session
