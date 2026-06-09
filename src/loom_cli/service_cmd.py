"""`loom service` — one-liner wrapper around docker-compose + migrations
+ test-data seeding for the dev stack.

Replaces the manual sequence:

    docker compose --env-file .env -f deploy/docker-compose.dev.yml up -d
    alembic -c migrations/alembic.ini upgrade head
    python scripts/seed_test_data.py --db-url 'postgresql+psycopg://loom:loom@localhost:5432/loom'

with:

    loom service up      # all of the above + endpoint summary + token
    loom service down    # stop containers (preserves volumes)
    loom service status  # container state + endpoint URLs + Swagger UI link

Assumes invocation from the loom repo root (where `deploy/` lives).
Use `--compose-file PATH` to override.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path

# Dev-stack defaults — match `deploy/docker-compose.dev.yml`. Keep in sync.
_DEFAULT_COMPOSE_FILE = Path("deploy/docker-compose.dev.yml")
_DEFAULT_DB_URL = "postgresql+psycopg://loom:loom@localhost:5432/loom"
_HEALTHCHECK_RETRIES = 30
_HEALTHCHECK_INTERVAL_SEC = 2.0

# Endpoint map (matches compose YAML port bindings + k8s ingress).
_ENDPOINTS = {
    "loom_service": ("http://localhost:8090", "user-facing REST + Swagger UI at /docs"),
    "control_plane": ("http://localhost:8080", "worker-facing; normally internal in k8s"),
    "llm_gateway": ("http://localhost:9100", "agent-facing"),
    "minio_console": ("http://localhost:9001", "browse trajectory / artifact buckets"),
    "minio_s3": ("http://localhost:9000", "S3 API"),
    "postgres": ("postgresql://loom:loom@localhost:5432/loom", "DB"),
}


def _compose_args(compose_file: Path, env_file: Path | None) -> list[str]:
    """Common `docker compose` argv prefix."""
    args = ["docker", "compose"]
    if env_file is not None and env_file.is_file():
        args += ["--env-file", str(env_file)]
    args += ["-f", str(compose_file)]
    return args


def _run(argv: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
    """Subprocess wrapper that streams output to our stdout/stderr."""
    return subprocess.run(argv, check=check, text=True)


def _wait_for_postgres(compose_file: Path, env_file: Path | None) -> bool:
    """Poll `docker compose ps` until postgres is healthy. Returns True
    on success, False on timeout."""
    cmd = [*_compose_args(compose_file, env_file), "ps", "--format", "json", "postgres"]
    for _ in range(_HEALTHCHECK_RETRIES):
        try:
            r = subprocess.run(cmd, capture_output=True, text=True, check=True)
            # Compose ps --format json prints one JSON object per line;
            # we only care whether "healthy" appears in the status.
            if '"Health":"healthy"' in r.stdout or '"health":"healthy"' in r.stdout:
                return True
        except subprocess.CalledProcessError:
            pass
        time.sleep(_HEALTHCHECK_INTERVAL_SEC)
    return False


def _alembic_upgrade(db_url: str) -> int:
    """Run alembic migrations against the dev postgres. Alembic reads
    `LOOM_DB_URL` per `migrations/env.py` convention."""
    env = os.environ.copy()
    env["LOOM_DB_URL"] = db_url
    return subprocess.run(
        [sys.executable, "-m", "alembic",
         "-c", "migrations/alembic.ini", "upgrade", "head"],
        env=env, check=False,
    ).returncode


def _seed_test_data(db_url: str) -> tuple[int, str | None]:
    """Run seed_test_data.py; return (exit_code, team_token_or_None).
    The script prints the team token as its last stdout line."""
    r = subprocess.run(
        [sys.executable, "scripts/seed_test_data.py",
         "--db-url", db_url, "--print", "team"],
        capture_output=True, text=True, check=False,
    )
    if r.returncode != 0:
        sys.stderr.write(r.stderr)
        return r.returncode, None
    token = r.stdout.strip().splitlines()[-1] if r.stdout.strip() else None
    return 0, token


def _print_summary(team_token: str | None) -> None:
    print()
    print("=" * 60)
    print("Loom dev stack is up.")
    print("=" * 60)
    print()
    print("Endpoints:")
    for name, (url, desc) in _ENDPOINTS.items():
        print(f"  {name:18} {url:48} {desc}")
    print()
    if team_token:
        print("Team token (use as Bearer):")
        print(f"  {team_token}")
        print()
        print("Try it:")
        print("  curl http://localhost:8090/api/v1/health")
        print(f"  curl -H 'Authorization: Bearer {team_token}' \\")
        print("       http://localhost:8090/api/v1/trials")
        print()
    print("Shut down:")
    print("  loom service down")
    print()


def _up(args: argparse.Namespace) -> int:
    compose_file = args.compose_file
    env_file = args.env_file
    if not compose_file.is_file():
        sys.stderr.write(
            f"error: compose file not found at {compose_file}.\n"
            "Run `loom service up` from the loom repo root, or pass "
            "--compose-file PATH.\n",
        )
        return 1

    print(f"→ docker compose up -d ({compose_file})")
    r = _run([*_compose_args(compose_file, env_file), "up", "-d"], check=False)
    if r.returncode != 0:
        return r.returncode

    print("→ waiting for postgres to be healthy...")
    if not _wait_for_postgres(compose_file, env_file):
        sys.stderr.write(
            "error: postgres did not become healthy within "
            f"{_HEALTHCHECK_RETRIES * _HEALTHCHECK_INTERVAL_SEC:.0f}s.\n",
        )
        return 1

    print("→ alembic upgrade head")
    rc = _alembic_upgrade(args.db_url)
    if rc != 0:
        sys.stderr.write("error: alembic upgrade failed.\n")
        return rc

    print("→ seeding team token + hello-world task fixture")
    rc, token = _seed_test_data(args.db_url)
    if rc != 0:
        sys.stderr.write("error: seed_test_data.py failed.\n")
        return rc

    _print_summary(token)
    return 0


def _down(args: argparse.Namespace) -> int:
    compose_file = args.compose_file
    if not compose_file.is_file():
        sys.stderr.write(f"error: compose file not found at {compose_file}.\n")
        return 1
    cmd = [*_compose_args(compose_file, args.env_file), "down"]
    if args.volumes:
        cmd.append("-v")
    print(f"→ docker compose down ({compose_file}{' -v' if args.volumes else ''})")
    return _run(cmd, check=False).returncode


def _status(args: argparse.Namespace) -> int:
    compose_file = args.compose_file
    if not compose_file.is_file():
        sys.stderr.write(f"error: compose file not found at {compose_file}.\n")
        return 1
    print(f"→ docker compose ps ({compose_file})")
    rc = _run(
        [*_compose_args(compose_file, args.env_file), "ps"],
        check=False,
    ).returncode
    print()
    print("Endpoints:")
    for name, (url, desc) in _ENDPOINTS.items():
        print(f"  {name:18} {url:48} {desc}")
    print()
    return rc


def add_service_subparser(sub: argparse._SubParsersAction) -> None:  # type: ignore[type-arg]
    """Register `loom service {up,down,status}` on the top-level argparse."""
    p_service = sub.add_parser(
        "service",
        help="Start / stop / inspect the dev compose stack",
        description=(
            "One-liner wrapper around docker-compose + alembic + "
            "seed_test_data.py for the local dev stack."
        ),
    )
    service_sub = p_service.add_subparsers(dest="service_cmd", required=True)

    common = argparse.ArgumentParser(add_help=False)
    common.add_argument(
        "--compose-file", type=Path, default=_DEFAULT_COMPOSE_FILE,
        help=f"Path to docker-compose YAML (default: {_DEFAULT_COMPOSE_FILE})",
    )
    common.add_argument(
        "--env-file", type=Path, default=Path(".env"),
        help="Path to .env file (default: ./.env; ignored if missing)",
    )

    p_up = service_sub.add_parser(
        "up", parents=[common],
        help="Start stack, run migrations, seed test data, print summary",
    )
    p_up.add_argument(
        "--db-url", default=_DEFAULT_DB_URL,
        help=f"Postgres URL for migrations + seeding (default: {_DEFAULT_DB_URL})",
    )
    p_up.set_defaults(handler=_up)

    p_down = service_sub.add_parser(
        "down", parents=[common],
        help="Stop stack (preserves named volumes by default)",
    )
    p_down.add_argument(
        "-v", "--volumes", action="store_true",
        help="Also remove named volumes (wipes postgres + minio state)",
    )
    p_down.set_defaults(handler=_down)

    p_status = service_sub.add_parser(
        "status", parents=[common],
        help="Show container state + endpoint URLs",
    )
    p_status.set_defaults(handler=_status)
