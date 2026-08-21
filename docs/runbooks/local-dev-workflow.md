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
loom service up --environment local
```

This command renders the local Compose configuration, starts the services,
runs database migrations, and creates the local access token. Inspect status
and logs with:

```bash
loom service status --environment local
docker compose ps
docker compose logs --tail=200
```

Use `loom service up --help` and `loom service status --help` for the exact
options supported by the installed candidate.

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
