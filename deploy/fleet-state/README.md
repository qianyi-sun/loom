# Fleet-state schema example

`schema-v1.example.toml` is synthetic validator input for the global fleet
state format. It is not a live manifest, cannot activate capacity, and must not
replace the per-environment files under `deploy/environment-state/`.
It also does not register a dry-run pool executor, bind an ownership key, or
create reservation, permit, inventory, or release records.

The current read-only diagnostic compares those environment files and reports
conflicts without choosing a winner or mutating controllers, partitions,
associations, resource vectors, or capacity ceilings:

```bash
uv run --no-sync python -m loom_capacity_manager.fleet_state inventory-legacy \
  deploy/environment-state/development.toml \
  deploy/environment-state/staging.toml \
  deploy/environment-state/production.toml
```

Exit status `2` means an input is invalid or the environments disagree. Output
is bounded JSON and contains no credentials or absolute source paths.
