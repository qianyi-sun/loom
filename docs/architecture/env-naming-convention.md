# Environment naming convention

Loom has three formal deploy environments. Each is referred to by two names, used
in different contexts. Mixing them is a bug; this document pins which name goes
where.

## The two names

| long form (`environment`) | short form (`short_name`) |
| --- | --- |
| `development` | `dev` |
| `staging` | `staging` |
| `production` | `prod` |

Only `development`/`production` actually differ between the two forms;
`staging` is spelled the same in both. The distinction still matters because
tooling keys off the field it reads.

## Where each form is used

**Long form (`environment`)** — filenames and file-lookup keys:

- `deploy/environments/<environment>.toml` (profile) and its
  `deploy/environments/<environment>.cluster.toml`
- `deploy/environment-state/<environment>.toml` (DB-backed desired state)
- The `environment` field inside those files, and `expected_environment`
  passed to `load_environment_state_profile`.

**Short form (`short_name`)** — everything user- or surface-facing:

- Kubernetes namespace: `loom-<short_name>` (`loom-dev`, `loom-staging`,
  `loom-prod`)
- Public route + API base: `https://yylx.world/<short_name>` and
  `.../<short_name>/api`
- Image tag prefix: `<short_name>-<sha7>`
- Rollout operator groups: `loom-<short_name>-operators`
- Autoscaler systemd timer names: `loom-autoscaler-<pool>-<short_name>.timer`

## Mapping between the two

A profile file carries both fields, so a component that receives one form
resolves the other by scanning `deploy/environments/*.toml`:

- Given a short form (e.g. a broker `--env dev`), match on the `short_name`
  field, then use that file's `environment` value to locate the env-state file.
- Given a long form (a filename), read its `short_name` field for the
  surface-facing values.

`scripts/validate_environment_isolation.py` enforces the per-environment
identity (namespace, route, buckets, secret refs, allowed refs) and that the
three route surfaces (`/dev`, `/staging`, `/prod`) are distinct.
