# Local development workflow

Local development uses Docker Compose through `loom service`. Kubernetes is
reserved for shared cluster targets and protected rollout workflows.

## Prerequisites

- Docker with the Compose plugin (`docker compose version`)
- `uv` and Python 3.11
- Node.js and npm for SPA development

## Install the workspace

```bash
uv python install 3.11
uv sync --locked --all-packages --extra dev --python 3.11
source .venv/bin/activate
cp .env.example .env
```

Edit `.env` with only the provider credentials needed for your test. Do not
commit it.

## Start the local stack

```bash
loom service up
```

This command renders the local Compose configuration, starts the services,
runs database migrations, and creates the local access token. Inspect status
and logs with:

```bash
loom service status
docker compose --env-file .env -f deploy/docker-compose.dev.yml ps
docker compose --env-file .env -f deploy/docker-compose.dev.yml logs --tail=200
```

Use `loom service up --help` and `loom service status --help` for the exact
options supported by the installed candidate.

## Non-default local ports

The Compose file binds services to loopback addresses by default. If another
local service already uses a port, set the corresponding `LOOM_DEV_*_PORT`
value in `.env`, then use `docker compose ... ps` to see the actual published
ports.

When PostgreSQL or the Control Plane use non-default host ports, pass matching
targets to `loom service up` so the host-side migration and bootstrap steps
reach the stack. For example, with PostgreSQL on `15432` and the Control Plane
on `28080`:

```bash
loom service up --environment local \
  --db-url 'postgresql+psycopg://loom:loom@localhost:15432/loom' \
  --cp-url http://localhost:28080
```

Adapt the database user, password, database name, and ports to the values in
your `.env`; do not put credentials in documentation or shell history shared
with others.

### Fresh database migration recovery

On a fresh database, Control Plane intentionally refuses to start until its
Alembic schema is at head. If Compose reports that `control-plane` is
unhealthy and its logs say that the schema is not at Alembic head, migrate the
database before starting the rest of the stack:

```bash
dc() {
  docker compose --env-file .env -f deploy/docker-compose.dev.yml "$@"
}

dc up -d --wait postgres

dc run --rm --no-deps control-plane sh -ec '
  export LOOM_DB_URL="$LOOM_CP_DB_URL"
  alembic -c migrations/alembic.ini upgrade head
  alembic -c migrations/alembic.ini current
'

dc restart control-plane
loom service up --environment local
dc ps
```

The `current` output should report the repository's Alembic head revision.
Re-running `loom service up` completes token seeding, batch-runner credential
creation, and container recreation that the initial failed startup skipped.
Reuse `--db-url` and `--cp-url` above if you configured non-default ports.

This procedure is for a fresh database or one behind the current checkout.
If the database was migrated by newer code, use that compatible checkout or a
separate local database; do not stamp its revision to make older code start.

## Develop the SPA

Run the backend stack first, then start Vite in a second terminal:

```bash
cd web
npm install
npm run dev
```

Vite serves the SPA on port 5173 and proxies `/api` to the local service on
port 8090.

## Test changes

Run focused tests while iterating, then the affected repository gates:

```bash
uv run --no-sync ruff check src tests packages
uv run --no-sync pytest tests/unit tests/contract tests/property tests/loom_cli

cd web
npm test
npm run typecheck
npm run lint
npm run build
```

Docker-touching changes also require the relevant integration or system tests.
The shared CI planner selects the required lanes from changed paths.

Keep integration fixtures isolated while removing costs unrelated to their
assertions. The frozen global autoscaling harness creates fresh EC P-256 TLS
identities for its algorithm-independent lifecycle scenarios, avoiding repeated
RSA private-key validation costs during readback. It still exercises real
certificate, issuer, private-key matching, file-permission, and Linux descriptor
checks. The shared bootstrap helper defaults to RSA; credential metadata,
mismatched private-key, and wrong-issuer tests explicitly cover both RSA and EC.
This fixture choice does not change production cryptography or cache validated
credentials between reads or test cases.

## Stop or reset

Preserve local volumes:

```bash
loom service down --environment local
```

Delete Compose volumes only when you explicitly want a clean local data reset:

```bash
loom service down --environment local -v
```

The volume-deleting command is destructive and cannot restore prior local
Postgres or object-store data.

## Shared environments

Do not use this local workflow to mutate staging or production. Personal
`dev-<name>` candidates go through the remote environment API. Protected
staging and production targets use the candidate-bound cluster rollout and its
backup, registry publication, migration, release-gate, smoke, and convergence
evidence.
