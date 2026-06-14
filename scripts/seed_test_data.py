"""Bootstrap a team + tokens (+ optional task/benchmark/rate-card fixtures).

Two modes:

- `--mode test` (default) — system-test fixture: hello-world Task,
  card-e2e RateCard, + tokens. Existing tests rely on these rows.
- `--mode dev` — what `loom service up` calls: tokens + every shipped
  benchmark adapter registered from HF Hub so the SPA dropdown is
  populated and submittable out of the box. Skips the hello-world Task
  + card-e2e RateCard.

Default register path (`hf://`): dev mode walks every adapter in
`loom_benchmarks.REGISTRY` and runs
`loom_benchmark_tool.register_cmd.run_register` for each. Manifests
(~tens of KB each) come from `{HF_ORG}/loom-benchmark-{slug}` on HF
Hub; task rows land with `source = "hf://..."`. Workers pull bundle
bytes on trial claim — no MinIO upload, no upstream conversion. This
is "registered = instantly available". Per-benchmark errors (benchmark
not yet published) are logged + skipped.

Air-gapped / local-import path: pass `--local-import` to run the older
`run_import` flow instead. That fetches each benchmark's upstream,
converts in-process, uploads to local MinIO, and writes `s3://` source
URLs. Slow on first boot (minutes), network-heavy, but works without
HF Hub access.

Prints tokens to stdout (system tests capture them as the bearer for
their submit calls; the SPA login screen accepts them as paste-ins).

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
from sqlalchemy.orm import Session, sessionmaker

from loom.db.schema import Benchmark, RateCard, Task, Team, TeamQuota, Token

DB_URL = "postgresql+psycopg://loom:loom@localhost:55432/loom"
REPO_ROOT = Path(__file__).resolve().parents[1]


def _seed_benchmarks_from_entrypoints(s: Session) -> int:
    """Register every entry-point-discovered BenchmarkAdapter as a
    Benchmark row. Idempotent — re-seeding only inserts missing rows.

    Returns the count of rows actually inserted (0 if the table was
    already populated).
    """
    # Imported lazily so test-mode runs without `loom-benchmarks`
    # installed don't crash. Real callers (loom service up, dev stack)
    # already have it via the editable install.
    from loom_benchmarks.registry import REGISTRY

    inserted = 0
    updated = 0
    for slug, adapter in sorted(REGISTRY.items()):
        upstream = adapter.upstream_source
        # PR-1 series/tags: pull `series` off the adapter so freshly
        # seeded rows group correctly in the SPA dropdown. Adapters
        # that don't declare a series leave the column NULL ("Other").
        adapter_series = getattr(adapter, "series", None)
        existing = s.execute(
            select(Benchmark.id, Benchmark.series)
            .where(Benchmark.id == slug),
        ).first()
        if existing is not None:
            # Backfill `series` on rows seeded before PR-1 — otherwise
            # every benchmark from a pre-PR-1 `loom service up` lands
            # in the SPA's "Other" bucket forever.
            if existing[1] is None and adapter_series is not None:
                s.execute(
                    Benchmark.__table__.update()
                    .where(Benchmark.id == slug)
                    .values(series=adapter_series),
                )
                updated += 1
            continue
        s.execute(insert(Benchmark).values(
            id=slug,
            display_name=getattr(adapter, "display_name", slug),
            upstream_kind=upstream.kind,
            upstream_locator=upstream.locator,
            # `revision` is optional on the adapter; the DB column is
            # NOT NULL so we substitute "main" for adapters that don't
            # pin a specific revision.
            upstream_revision=upstream.revision or "main",
            license_spdx=getattr(adapter, "license_spdx", "UNKNOWN"),
            license_url=getattr(adapter, "license_url", ""),
            splits=list(getattr(adapter, "splits", ())) or ["test"],
            series=adapter_series,
            imported_by="seed_test_data.py",
        ))
        inserted += 1
    return inserted + updated


def _auto_register_benchmarks(
    db_url: str, hf_org: str, hf_token: str | None,
) -> dict[str, str]:
    """Register every adapter in REGISTRY by reading its manifest from
    HF Hub. Returns {benchmark_id: status} for stderr logging.

    Status values:
    - `ok registered=N` — manifest fetched + N tasks upserted.
    - `not_published` — HF returned 401 (repo not found) or 404 (repo
      exists but `manifest.json` missing). Treated as a normal status,
      not an error: it's the expected state for adapters that exist
      in code but haven't been published to PRHW yet. The stub seed
      already inserted a Benchmark row so the SPA still shows them
      grouped under their series.
    - `error <type>: <msg>` — anything else (network failure, bad
      manifest schema, etc). Still doesn't abort the seed.
    """
    import asyncio

    from huggingface_hub.errors import (
        EntryNotFoundError,
        RepositoryNotFoundError,
    )
    from loom_benchmarks.registry import REGISTRY

    from loom_benchmark_tool.register_cmd import run_register

    results: dict[str, str] = {}
    for slug in sorted(REGISTRY):
        try:
            stats = asyncio.run(run_register(
                benchmark=slug,
                hf_org=hf_org,
                hf_token=hf_token,
                db_url=db_url,
                registered_by="seed_test_data.py:auto_register",
            ))
            results[slug] = f"ok registered={stats['registered']}"
        except (RepositoryNotFoundError, EntryNotFoundError):
            # Not published yet — this is the steady-state for any
            # adapter the Loom team hasn't run `loom_benchmark_tool
            # publish` on yet. Quiet skip; the SPA still shows the
            # stub row because `_seed_benchmarks_from_entrypoints`
            # already inserted it with `task_count=0`.
            results[slug] = "not_published"
        except Exception as exc:
            results[slug] = f"error {type(exc).__name__}: {exc}"
    return results


def _auto_import_benchmarks(
    db_url: str, limit: int,
    minio_endpoint: str, minio_access_key: str, minio_secret_key: str,
    bucket: str, cache_dir: Path,
) -> dict[str, str]:
    """Import a small slice of every registered benchmark so the SPA
    dropdown is populated + submittable on first boot.

    Returns {benchmark_id: status} where status is "ok N" / "skip reason"
    / "error <msg>"; printed to stderr by the caller for visibility.
    """
    import asyncio

    from loom_benchmarks.registry import REGISTRY

    from loom.trajectory.storage import MinioObjectStore
    from loom_benchmark_tool.import_cmd import run_import

    store = MinioObjectStore(
        endpoint_url=minio_endpoint,
        access_key=minio_access_key,
        secret_key=minio_secret_key,
    )
    # Ensure the target bucket exists. `run_import` writes objects but
    # doesn't bootstrap the bucket; on a fresh MinIO that's a NoSuchBucket
    # ClientError that fails every benchmark before the first task lands.
    import boto3
    from botocore.config import Config
    from botocore.exceptions import ClientError
    s3 = boto3.client(
        "s3",
        endpoint_url=minio_endpoint,
        aws_access_key_id=minio_access_key,
        aws_secret_access_key=minio_secret_key,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )
    try:
        s3.head_bucket(Bucket=bucket)
    except ClientError:
        try:
            s3.create_bucket(Bucket=bucket)
            sys.stderr.write(f"seed: created MinIO bucket {bucket!r}\n")
        except ClientError as exc:
            sys.stderr.write(
                f"seed: warning — couldn't create bucket {bucket!r}: {exc}. "
                "Auto-import will likely fail; create the bucket manually.\n",
            )

    results: dict[str, str] = {}
    for slug in sorted(REGISTRY):
        try:
            stats = asyncio.run(run_import(
                benchmark=slug,
                db_url=db_url,
                object_store=store,
                bucket=bucket,
                cache_dir=cache_dir,
                limit=limit,
                imported_by="seed_test_data.py:auto_import",
            ))
            results[slug] = f"ok converted={stats['converted']}"
        except Exception as exc:
            # Common causes: no network, upstream removed/renamed,
            # MinIO not ready yet. The dev stack should still come up.
            results[slug] = f"error {type(exc).__name__}: {exc}"
    return results


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--db-url", default=DB_URL,
        help="Postgres URL (default: docker-compose.test.yml exposes 55432).",
    )
    parser.add_argument(
        "--mode", choices=("test", "dev"), default="test",
        help=(
            "test (default) — system-test fixture: hello-world Task, "
            "card-e2e RateCard, tokens. `dev` — what `loom service up` "
            "calls: tokens + benchmark slate, no placeholder rows."
        ),
    )
    parser.add_argument(
        "--task-id", default="hello-world",
        help=(
            "Which fixture under tests/fixtures/tasks/ to register in "
            "test mode. Ignored in dev mode."
        ),
    )
    parser.add_argument(
        "--print", choices=("team", "worker", "admin", "all"), default="team",
        help=(
            "Which token to print to stdout (default: team). "
            "`admin` is dev-only — see issue #295."
        ),
    )
    parser.add_argument(
        "--auto-register", dest="auto_register",
        action=argparse.BooleanOptionalAction, default=True,
        help=(
            "Dev mode only: walk every adapter in REGISTRY and run "
            "`register` against HF Hub. Default: on. Pass "
            "--no-auto-register to skip — benchmarks then need a "
            "manual `python -m loom_benchmark_tool register` to show "
            "up in the SPA."
        ),
    )
    parser.add_argument(
        "--local-import", dest="local_import",
        action="store_true", default=False,
        help=(
            "Dev mode only: in addition to (or instead of) registering "
            "from HF, run the local-import path (`run_import`) for each "
            "adapter. Slow + network-heavy + requires MinIO; use for "
            "air-gapped deployments or to populate MinIO with bundles "
            "before HF Hub has them. Default: off."
        ),
    )
    parser.add_argument(
        "--auto-import-limit", type=int, default=20,
        help=(
            "Tasks per benchmark for --local-import (default 20). "
            "Ignored when --local-import is off."
        ),
    )
    parser.add_argument(
        "--hf-org", default=os.environ.get("LOOM_HF_ORG", "PRHW"),
        help="HF namespace to register from (env: LOOM_HF_ORG).",
    )
    parser.add_argument(
        "--hf-token", default=os.environ.get("HF_TOKEN"),
        help="HF read token (optional for public datasets).",
    )
    parser.add_argument(
        "--minio-endpoint",
        default=os.environ.get("LOOM_MINIO_ENDPOINT", ""),
        help="MinIO URL for auto-import (env: LOOM_MINIO_ENDPOINT).",
    )
    parser.add_argument(
        "--minio-access-key",
        default=os.environ.get("LOOM_MINIO_ACCESS_KEY", ""),
        help="MinIO access key (env: LOOM_MINIO_ACCESS_KEY).",
    )
    parser.add_argument(
        "--minio-secret-key",
        default=os.environ.get("LOOM_MINIO_SECRET_KEY", ""),
        help="MinIO secret key (env: LOOM_MINIO_SECRET_KEY).",
    )
    parser.add_argument(
        "--bucket", default=os.environ.get("LOOM_TASK_BUCKET", "loom-tasks"),
        help="MinIO bucket for task uploads (default: loom-tasks).",
    )
    parser.add_argument(
        "--cache-dir",
        default=os.environ.get(
            "LOOM_BENCHMARK_CACHE",
            str(Path.home() / ".cache" / "loom-benchmarks"),
        ),
        help="Local upstream-fetch cache (default: ~/.cache/loom-benchmarks).",
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

    # Test-mode task fixture is loaded ONLY when --mode test (or
    # explicit --task-id). Dev mode skips it entirely.
    config: dict | None = None
    checksum: str | None = None
    if args.mode == "test":
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
            scopes=[
                "admin:tokens",
                "admin:rate_cards",
            ],
            team_id=None,
            issued_at=now, expires_at=None,
        ))

        if args.mode == "test":
            # Task may already exist if called twice for the same
            # fixture (e.g., stack_up + a second test re-seeding).
            # Skip in that case so repeated calls don't crash the
            # test session.
            task_exists = s.execute(
                select(Task.id).where(Task.id == args.task_id),
            ).scalar_one_or_none()
            if not task_exists:
                s.execute(insert(Task).values(
                    id=args.task_id, checksum=checksum,
                    config=config, source=f"fixture://{args.task_id}",
                ))
            # Test-mode rate card — `card-e2e` is the placeholder
            # contract tests rely on. Real default rate cards (for
            # anthropic/openai/google) are seeded separately by the
            # Gateway's startup logic in dev mode.
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

        if args.mode == "dev":
            inserted = _seed_benchmarks_from_entrypoints(s)
            # Print to stderr so token-parsing wrappers (loom service
            # up) aren't confused by an extra stdout line.
            if inserted:
                sys.stderr.write(
                    f"seed: registered {inserted} benchmark adapter(s) "
                    "from loom_benchmarks entry-points\n",
                )

        s.commit()
    engine.dispose()

    # Default path: register from HF Hub. `run_register` opens its
    # own async session per benchmark; no shared sync session.
    if args.mode == "dev" and args.auto_register:
        sys.stderr.write(
            f"seed: registering benchmarks from HF "
            f"(org={args.hf_org})…\n",
        )
        results = _auto_register_benchmarks(
            db_url=args.db_url,
            hf_org=args.hf_org,
            hf_token=args.hf_token,
        )
        # Per-slug detail only for `ok` and real `error` rows. The
        # `not_published` case is the steady-state for any adapter
        # that hasn't been published to PRHW yet — it's expected, not
        # alarming, and printing one line per slug just adds noise.
        # Summarized on a single line below.
        not_published = sorted(
            s for s, status in results.items() if status == "not_published"
        )
        for slug, status in sorted(results.items()):
            if status == "not_published":
                continue
            sys.stderr.write(f"seed:   {slug}: {status}\n")
        if not_published:
            sys.stderr.write(
                f"seed:   {len(not_published)} adapter(s) not yet "
                f"published to {args.hf_org} — registered as stubs, "
                f"task_count=0: {', '.join(not_published)}\n"
            )
            sys.stderr.write(
                "seed:   to populate: "
                "`python -m loom_benchmark_tool publish <slug>` "
                "(needs HF write token for the target org)\n"
            )

    # Opt-in: local-import path for air-gapped deploys. Slower + needs
    # MinIO; only fires when explicitly requested.
    if args.mode == "dev" and args.local_import:
        missing_minio = [
            f for f, v in (
                ("--minio-endpoint", args.minio_endpoint),
                ("--minio-access-key", args.minio_access_key),
                ("--minio-secret-key", args.minio_secret_key),
            ) if not v
        ]
        if missing_minio:
            sys.stderr.write(
                "seed: skipping local-import — missing "
                f"{', '.join(missing_minio)}. Set LOOM_MINIO_* env "
                "vars or pass the flags directly.\n",
            )
        else:
            sys.stderr.write(
                f"seed: local-importing benchmarks "
                f"(limit={args.auto_import_limit} per benchmark)…\n",
            )
            results = _auto_import_benchmarks(
                db_url=args.db_url,
                limit=args.auto_import_limit,
                minio_endpoint=args.minio_endpoint,
                minio_access_key=args.minio_access_key,
                minio_secret_key=args.minio_secret_key,
                bucket=args.bucket,
                cache_dir=Path(args.cache_dir),
            )
            for slug, status in sorted(results.items()):
                sys.stderr.write(f"seed:   {slug}: {status}\n")

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
