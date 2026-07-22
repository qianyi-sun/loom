from __future__ import annotations

import subprocess
import sys
from pathlib import Path

from sqlalchemy import create_engine, delete, select
from sqlalchemy.orm import sessionmaker

from loom.db.schema import TeamMembership, Token, User


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
    printed_tokens = dict(
        line.split(":", 1)
        for line in result.stdout.splitlines()
        if ":" in line
    )
    assert printed_tokens["team"].strip().startswith("loom_api_")

    with session_factory() as s:
        rows = s.execute(
            select(Token.type, Token.scopes, Token.created_by_user_id),
        ).all()
        submitting_token = next(row for row in rows if "submit" in row.scopes)
        assert submitting_token.created_by_user_id is not None
        user = s.get(User, submitting_token.created_by_user_id)
        assert user is not None
        assert user.status == "active"
        membership = s.execute(
            select(TeamMembership).where(
                TeamMembership.user_id == submitting_token.created_by_user_id,
            ),
        ).scalar_one()
        assert membership.role == "owner"

    assert rows
    assert all(row.type != "admin" for row in rows)
    assert all(
        not any(scope.startswith("admin:") for scope in row.scopes)
        for row in rows
    )

    engine.dispose()
