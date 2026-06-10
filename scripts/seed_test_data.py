"""Bootstrap a team + task + tokens for tests.

Invoked by `tests/system/*` to seed the docker-compose stack with the
canonical fixture data. Prints tokens to stdout (system tests capture
them as the bearer for their submit calls; the SPA login screen
accepts them as paste-ins).

In production, an admin uses /admin/* + the rate-card admin endpoint;
this script side-channels straight into Postgres for speed.

NOTE: this script also seeds a `loom_admin_<…>` token in the DB so the
SPA admin views work in dev. This is a DEVELOPMENT-ONLY crutch — the
production model (singleton admin from secret-store, not multi-row DB
tokens) is tracked in https://github.com/carinrc/loom/issues/295. The
script refuses to run when `LOOM_ENV=production` so it can't be
fired against a prod cluster by accident.
"""

from __future__ import annotations

import argparse
import hashlib
import os
import secrets
import sys
import tomllib
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from sqlalchemy import create_engine, insert, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import sessionmaker

from loom.db.schema import RateCard, Task, Team, TeamQuota, Token

DB_URL = "postgresql+psycopg://loom:loom@localhost:55432/loom"
REPO_ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--db-url", default=DB_URL,
        help="Postgres URL (default: docker-compose.test.yml exposes 55432).",
    )
    parser.add_argument(
        "--task-id", default="hello-world",
        help="Which fixture under tests/fixtures/tasks/ to register.",
    )
    parser.add_argument(
        "--print", choices=("team", "worker", "admin", "all"), default="team",
        help=(
            "Which token to print to stdout (default: team). "
            "`admin` is dev-only — see issue #295."
        ),
    )
    args = parser.parse_args()

    # Hard guard: this script side-channels into the DB and ships a
    # dev-only admin token. Refuse to run against a production env
    # so a stray invocation can't manufacture an admin token.
    if os.environ.get("LOOM_ENV", "").lower() == "production":
        sys.stderr.write(
            "seed_test_data.py refuses to run with LOOM_ENV=production. "
            "It is a development crutch; production admin bootstrap is "
            "tracked in https://github.com/carinrc/loom/issues/295.\n",
        )
        raise SystemExit(2)

    engine = create_engine(args.db_url)
    session_local = sessionmaker(engine)
    team_id = uuid4()
    raw_team = "loom_team_" + secrets.token_hex(8)
    raw_worker = "loom_w_" + secrets.token_hex(8)
    raw_admin = "loom_admin_" + secrets.token_urlsafe(32)
    now = datetime.now(UTC)

    fixture_dir = REPO_ROOT / "tests" / "fixtures" / "tasks" / args.task_id
    if not (fixture_dir / "task.toml").is_file():
        raise SystemExit(f"unknown fixture {args.task_id} at {fixture_dir}")
    config = tomllib.loads((fixture_dir / "task.toml").read_text())
    checksum = hashlib.sha256(
        (fixture_dir / "task.toml").read_bytes(),
    ).hexdigest()

    with session_local() as s:
        s.execute(insert(Team).values(id=team_id, name=f"e2e-{team_id}"))
        s.execute(insert(TeamQuota).values(team_id=team_id))
        s.execute(insert(Token).values(
            token_hash=hashlib.sha256(raw_team.encode()).digest(),
            type="team", scopes=["submit", "read:own"], team_id=team_id,
            issued_at=now, expires_at=None,
        ))
        s.execute(insert(Token).values(
            token_hash=hashlib.sha256(raw_worker.encode()).digest(),
            type="worker",
            scopes=["worker:claim", "worker:report", "worker:index"],
            team_id=None,
            issued_at=now, expires_at=None,
        ))
        # Dev-only admin token so the SPA admin views work. See the
        # module docstring + issue #295 for the production model.
        s.execute(insert(Token).values(
            token_hash=hashlib.sha256(raw_admin.encode()).digest(),
            type="admin",
            scopes=["admin:tokens", "admin:rate_cards"],
            team_id=None,
            issued_at=now, expires_at=None,
        ))
        # Task may already exist if called twice for the same fixture
        # (e.g., stack_up + a second test re-seeding). Skip in that case
        # so repeated calls don't crash the test session.
        task_exists = s.execute(
            select(Task.id).where(Task.id == args.task_id),
        ).scalar_one_or_none()
        if not task_exists:
            s.execute(insert(Task).values(
                id=args.task_id, checksum=checksum,
                config=config, source=f"fixture://{args.task_id}",
            ))
        # Rate card has a fixed id across calls — upsert.
        s.execute(pg_insert(RateCard).values(
            id="card-e2e", captured_at=now,
            table={
                "id": "card-e2e",
                "entries": [{
                    "provider": "anthropic",
                    "model": "claude-opus-4-7",
                    "input_per_mtok": 1, "output_per_mtok": 1,
                    "cache_read_per_mtok": 0, "cache_write_per_mtok": 0,
                }],
            },
        ).on_conflict_do_nothing(index_elements=["id"]))
        s.commit()
    engine.dispose()

    if args.print == "team":
        print(raw_team)
    elif args.print == "worker":
        print(raw_worker)
    elif args.print == "admin":
        print(raw_admin)
    else:  # "all"
        print(f"team: {raw_team}")
        print(f"worker: {raw_worker}")
        print(f"admin: {raw_admin}")


if __name__ == "__main__":
    main()
