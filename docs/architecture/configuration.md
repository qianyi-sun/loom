# Configuration Schema

[`config/loom-schema.toml`](../../config/loom-schema.toml) is the source of
truth for service settings and cluster render settings. Edit the schema, then
regenerate its derived artifacts; do not edit generated settings modules by
hand.

## Schema sections

- `service_prefix` maps each runtime component to its `LOOM_<PREFIX>_...`
  environment-variable prefix.
- `service_config` defines runtime settings, their Python types, defaults,
  consuming services, environment overrides, and optional Kubernetes Secret
  projection.
- `render_config` defines operator-facing cluster configuration and the values
  available to Kubernetes templates.

A `service_config` entry is Secret-backed when it has a `secret` table. Secret
entries may use one shared key or per-service keys. Required values have no
runtime default; optional entries use their declared default or `None` where
the generated type permits it.

`render_config` supports scalar values plus the schema's list and table field
forms. Descriptions are copied into generated configuration surfaces, so they
must describe current behavior and safe operator use.

## Generated outputs

Run:

```bash
uv run --no-sync loom config codegen
```

The generator updates:

- `src/loom_control_plane/config/_generated.py`
- `src/loom_llm_gateway/config/_generated.py`
- `src/loom_service/config/_generated.py`
- `src/loom_worker/config/_generated.py`
- `config/cluster-config.example.toml`

`src/loom_cli/data/loom-schema.toml` is the packaged runtime copy of the
canonical schema, not a codegen output. It must remain byte-identical to
`config/loom-schema.toml`; the cluster-config packaging tests enforce that
copy boundary.

Kubernetes templates use the schema-aware environment-block helper at render
time, so runtime environment names and Secret references remain aligned with
the generated settings classes.

Check for drift without modifying files:

```bash
uv run --no-sync loom config codegen --check
```

The schema loader and generator tests under `tests/loom_config/` validate the
grammar, settings snapshots, environment derivation, Secret bootstrap, and
template macro behavior.

## Cluster profiles and overrides

`loom cluster` loads operator values from TOML into the generated
`ClusterConfig` model. Relative paths resolve according to the owning setting's
loader; profile-specific paths should stay beside the profile when the schema
description says they are profile-relative.

Environment-specific cluster files belong under `deploy/environments/`. Use
those profiles for durable settings and reserve environment variables for
runtime service injection and documented CLI overrides. Secrets belong in
Secret-backed settings or explicit credential-source files, never in committed
profiles or command arguments.

See [cluster deployment](cluster-deploy.md) for render and preflight commands.
