"""CLI for the loom-benchmarks ingestion pipeline (Plan 14).

Subcommands:
- `list` — print every registered benchmark name + license.
- `import` — fetch upstream, convert each instance, upload bundle to
  MinIO, upsert benchmarks + tasks rows in Postgres.
- `verify` — placeholder; full impl ships in Plan 16.
"""
