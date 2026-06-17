from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from sqlalchemy import create_engine, delete, select
from sqlalchemy.orm import sessionmaker

from loom.db.schema import Token


def test_seed_test_data_does_not_create_db_admin_tokens(postgres_url: str) -> None:
    engine = create_engine(postgres_url)
    session_factory = sessionmaker(engine)
    with session_factory() as s:
        s.execute(delete(Token))
        s.commit()

    repo_root = Path(__file__).resolve().parents[2]
    result = subprocess.run(
        [
            sys.executable,
            "scripts/seed_test_data.py",
            "--db-url",
            postgres_url,
            "--mode",
            "test",
            "--print",
            "all",
        ],
        cwd=repo_root,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    printed = {
        line.split(":", 1)[0].strip()
        for line in result.stdout.splitlines()
        if ":" in line
    }
    assert printed == {"team", "worker"}

    with session_factory() as s:
        rows = s.execute(select(Token.type, Token.scopes)).all()

    assert rows
    assert all(row.type != "admin" for row in rows)
    assert all(
        not any(scope.startswith("admin:") for scope in row.scopes)
        for row in rows
    )

    engine.dispose()
