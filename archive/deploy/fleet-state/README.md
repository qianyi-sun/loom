# Global Fleet State

> Archived predecessor. Current fleet-state behavior is documented in
> `deploy/fleet-state/README.md` and `docs/architecture/global-fleet-capacity-manager.md`.
> Only this predecessor README is archived; the `schema-v1.example.toml` it
> describes remains in the active `deploy/fleet-state/` directory.

This directory defines the future single source for fleet-owned physical-pool,
resource-domain, tier, account-template, and protocol generations.

`schema-v1.example.toml` is synthetic documentation—not a live fleet manifest.
It cannot activate capacity and must not be copied over the current
environment-state files. The repository intentionally contains no live global
fleet manifest while development, staging, and production disagree about the
GB10 and OLDLAB inventories and envelopes.

A reviewed operator reconciliation must resolve every reported legacy conflict
before creating a live manifest. The validator reports those conflicts; it
never chooses an environment copy, merges allowed-node lists, or changes a
controller, partition, association, resource vector, or capacity ceiling.

Diagnostic inventory:

```bash
python -m loom_capacity_manager.fleet_state inventory-legacy \
  deploy/environment-state/development.toml \
  deploy/environment-state/staging.toml \
  deploy/environment-state/production.toml
```

An exit status of `2` means drift exists or an input is invalid. Output is
bounded JSON and contains no credentials or absolute source paths.
