# Config consolidation

> **Cross-repo issue/PR refs:** `#N` in this document points to the
> pre-2026-06-26 `carinrc/loom` archive tracker (numbering was reset on
> the new canonical repo `qianyi-sun/loom`). See
> [`../repo-migration.md`](../repo-migration.md).

> **Status**: shipped in #150 (closes #146). Single schema at
> `config/loom-schema.toml` now drives Pydantic Settings codegen,
> k8s template env blocks, Secret bootstrap, and operator
> `cluster-config.toml`. Motivation traced to PR #128 staging-smoke
> runs #3 and #8, which caught two latent production bugs whose root
> cause was the same: configuration invariants spread across files
> that didn't know about each other.

## Problem

Every new config knob today requires editing four to seven files. The
fan-out for `step_jwt_signing_key` (a real example, fixed in PR #128)
covered:

1. `src/loom_control_plane/config.py` — Pydantic field
2. `src/loom_llm_gateway/config.py` — same field, different prefix
3. `src/loom_cli/templates/k8s/control-plane.yaml.j2` — `valueFrom: secretKeyRef` env block
4. `src/loom_cli/templates/k8s/llm-gateway.yaml.j2` — same
5. `docs/operator-runbook.md` — Secret bootstrap example
6. `.github/workflows/cluster-smoke.yml` — Secret seed
7. `.github/workflows/staging-smoke.yml` — Secret seed

Nothing validates that the seven edits stay consistent. Bug #3 of the
staging smoke run is exactly the failure mode: the Pydantic field
existed, the templates didn't wire the env var, both pods
CrashLoopBackOff'd on first deploy.

A second, smaller fan-out exists for operator-tunable cluster knobs
(`image_tag`, `ingress_host`, `replicas`, storage sizes): three places
each (`cluster_config.py` dataclass → Jinja2 template var → docs
example).

## Solution

A single schema file at `config/loom-schema.toml` is the source of
truth for both fan-outs. Pydantic Settings classes, k8s template env
blocks, operator-facing `cluster-config.toml`, Secret bootstrap, and a
new `loom cluster doctor` are all derived from it.

The schema has two top-level sections, one per fan-out kind:

- `[service_config.*]` — runtime values consumed by pods. Drives
  Pydantic fields, k8s env blocks, Secret keys, bootstrap.
- `[render_config.*]` — operator-tunable manifest knobs. Drives
  `cluster-config.toml` fields, Jinja2 template variables.

Within `service_config`, the *presence* of a `secret = { ... }`
sub-table is what marks an entry as Secret-backed. No tag field, no
enum to memorize — the data shape says what it is.

## Schema grammar

```toml
# config/loom-schema.toml — single source of truth.

[meta]
version = 1

# Each service's env-var prefix. Used to derive env-var names from
# entry names: env_var = f"LOOM_{prefix}_{entry_name.upper()}".
[service_prefix]
control-plane = "CP"
llm-gateway   = "GW"
loom-service  = "SVC"
worker        = "WORKER"

# ─── service_config: Pydantic fields + k8s env blocks ───

[service_config.step_jwt_signing_key]
used_by     = ["control-plane", "llm-gateway"]
python_type = "SecretStr"
required    = true
secret      = { key = "step-jwt-signing-key", generate = "openssl rand -hex 64" }
description = "HS256 step-JWT key. CP mints, gateway verifies."

[service_config.db_url]
used_by     = ["control-plane", "llm-gateway", "loom-service"]
python_type = "PostgresDsn"
required    = true
# Per-service Secret keys so DB credentials rotate independently.
secret      = { key_per_service = {
                  control-plane = "cp-db-url",
                  llm-gateway   = "gw-db-url",
                  loom-service  = "svc-db-url" } }

[service_config.minio_endpoint]
used_by     = ["control-plane", "loom-service", "worker"]
python_type = "str"
default     = "http://loom-minio:9000"

[service_config.bind_port]
used_by             = ["control-plane", "llm-gateway", "loom-service"]
python_type         = "int"
default_per_service = { control-plane = 8080, llm-gateway = 9100, loom-service = 8090 }

[service_config.log_level]
used_by     = ["control-plane", "llm-gateway", "loom-service", "worker"]
python_type = "LogLevel"
default     = "info"

# ─── render_config: cluster-config.toml + Jinja2 template vars ───

[render_config.image_tag]
python_type = "str"
default     = "0.7"
description = "Image tag applied to every loom-* image."

[render_config.ingress_host]
python_type = "str"
default     = "loom.example.com"

[render_config.ingress_class_name]
python_type = "str"
default     = "nginx"

[render_config.ingress_tls_secret_name]
python_type = "str"
default     = "loom-tls"

[render_config.ingress_cert_manager_cluster_issuer]
python_type = "str"
default     = ""

[render_config.replicas]
python_type = "table"
fields      = { service = 2, control_plane = 2, gateway = 2, web = 0, worker = 3 }

[render_config.postgres_image]
python_type = "str"
default     = "postgres:16"

[render_config.postgres_storage_gi]
python_type = "int"
default     = 50

[render_config.worker_subprocess_gateway_url]
python_type = "str"
default     = "http://host.docker.internal:30443/openai/v1"
```

### Field semantics

`service_config.*`:

| Field | Meaning |
|---|---|
| `used_by` | List of services that consume this. Drives Pydantic field emission (one field per service) + k8s env block emission |
| `python_type` | One of: `str`, `int`, `bool`, `float`, `Path`, `SecretStr`, `PostgresDsn`, `HttpUrl`, `LogLevel`. Mapped to the right import in codegen |
| `required` | If true, codegen emits a no-default field; Pydantic raises if missing. Default false |
| `default` | Single literal default shared across services |
| `default_per_service` | Per-service literal defaults (use when, e.g., bind ports differ) |
| `secret` | Presence marks the entry as Secret-backed. Has *exactly one of* `key` (single Secret key) or `key_per_service` (different Secret key per service); the loader raises if both or neither are set. Optional `generate` is a shell command stdout-substituted into Secret bootstrap. Without `secret`, the env block uses literal `value:` |
| `description` | Surfaced in `loom cluster doctor` output and the generated cluster-config example |

`render_config.*`:

| Field | Meaning |
|---|---|
| `python_type` | Same enum as service_config, plus `table` for nested dict-shaped knobs (e.g. `replicas`) |
| `default` | Default value (operator overrides via `cluster-config.toml`) |
| `fields` | Only for `python_type = "table"`: defines the inner sub-keys and their defaults |
| `description` | Surfaced in generated `cluster-config.example.toml` comments |

### Env var derivation

```
env_var(service, entry) = f"LOOM_{service_prefix[service]}_{entry_name.upper()}"
```

Audit of every env var declared in `loom_*/config.py` today matches
this rule. An optional per-entry `env_override = { service = "..." }`
escape hatch is reserved for future backwards-compatibility cases; no
entry needs it on day one.

## Derived artifacts

| Artifact | Derivation | Committed? |
|---|---|---|
| `src/loom_*/config/_generated.py` | Codegen from `service_config`. One Pydantic class per service, with the right fields, types, defaults, and env-var prefixes | yes — preserves mypy typing; CI verifies it matches schema |
| `src/loom_*/config.py` | Reduced to a re-export of `_generated.ServiceSettings` plus any one-off helpers (e.g., gateway's `LocalProviderConfig` parser stays here) | yes |
| K8s `*.yaml.j2` env blocks | Templates call a Jinja2 macro that loops over `schema.service_config_for("<service>")` and emits `valueFrom`/`value` blocks for required, defaulted, and secret-backed entries. A small number of template-local entries are still explicit when the Kubernetes renderer supplies a value that the service treats as optional at runtime, such as admin secret file paths or the worker subprocess gateway URL. | n/a (templates) |
| `src/loom_cli/cluster_config.py` | Replaced with a generic loader walking `render_config`. Unknown TOML keys raise as today | yes |
| `config/cluster-config.example.toml` | Generated from `render_config` defaults + descriptions, committed so operators have a copy-paste starting point | yes |
| `loom cluster doctor` | New CLI. Walks the schema against a target cluster: every required Secret key exists in `loom-secrets`, every rendered env var is present in each running pod's env block, and no orphan settings exist. "Rendered env var" means required, defaulted, secret-backed, or explicitly injected by the Kubernetes template; optional runtime-derived settings can stay unset without producing `missing_env`. Exit 1 on any violation. Wired into `loom cluster preflight` | new code |
| `loom cluster bootstrap-secrets` | New CLI. Walks `secret` entries, emits one `kubectl create secret generic loom-secrets --from-literal=...` line. `--rotate` runs each entry's `generate:` command and substitutes the new value | new code |

## Codegen rules (`_generated.py`)

For each service `S`, emit a `BaseSettings` subclass with:

- `env_prefix = f"LOOM_{service_prefix[S]}_"`
- `env_file = ".env"`, `extra = "forbid"` (matches today's `SettingsConfigDict`).
- One field per `service_config.E` entry where `S in E.used_by`. Field shape is resolved by these rules, applied in order:
  1. `E.required = true` → emit `name: T` with no default (Pydantic raises if env var missing).
  2. Else `E.default` set → emit `name: T = <default>`.
  3. Else `E.default_per_service` set → emit `name: T = <default_per_service[S]>`.
  4. Else → emit `name: T | None = None` (matches today's `anthropic_api_key: SecretStr | None = None` / `admin_secret_file: Path | None = None` shape).
- `secret = { ... }` and `python_type` are independent: `secret` only affects how the k8s env block is rendered (`valueFrom` vs `value`) and whether the entry feeds `bootstrap-secrets`. The Python field type is whatever `python_type` says.

The generated file carries a `# AUTOGENERATED — do not edit. Regenerate with `loom config codegen`.` header. CI runs `loom config codegen --check`, which re-renders both:

1. Every service's `src/loom_*/config/_generated.py`.
2. `config/cluster-config.example.toml` (from `render_config` defaults + descriptions).

and fails if either differs from the working tree. This catches the "operator copies stale `cluster-config.example.toml`" failure mode that the current 4–7 file fan-out can't catch today.

## K8s template macro

`src/loom_cli/templates/k8s/_env.j2` (new):

```jinja2
{%- macro env_block(service) -%}
env:
{%- for entry in schema.service_config_for(service) %}
  - name: {{ entry.env_var_for(service) }}
{%- if entry.secret %}
    valueFrom:
      secretKeyRef:
        name: loom-secrets
        key: {{ entry.secret_key_for(service) }}
{%- else %}
    value: {{ entry.value_for(service) | string | tojson }}
{%- endif %}
{%- endfor %}
{%- endmacro %}
```

Templates call it once per container:

```jinja2
{% import "_env.j2" as env %}
...
    containers:
      - name: control-plane
        image: loom-control-plane:{{ image_tag }}
        {{ env.env_block("control-plane") | indent(8) }}
```

Rule for hand-written env entries in the current templates:

- **Already a Pydantic field today** (`LOOM_CP_LLM_GATEWAY_URL`, `LOOM_CP_ADMIN_SECRET_FILE`, `LOOM_CP_MINIO_ACCESS_KEY`, etc.): moves into the schema. The macro emits the env block; no template-local override.
- **K8s-deployment-shape only, not in any Pydantic class** (`LOOM_ENV=production`): stays as a template-local literal appended after the macro call. These aren't operator-tunable runtime values — they're consequences of "this is a k8s deployment."

The rule is mechanical; implementation doesn't need to make case-by-case judgment calls.

## Migration safety

Two golden tests pin the refactor:

1. **`tests/loom_config/test_settings_snapshot.py`** — for each service, dump `ServiceSettings.model_fields` (name, annotation string, default, required) to a JSON golden file *before* the migration starts. Post-migration the assert must pass byte-for-byte. Catches any silent semantic drift in Pydantic field shape.
2. **`tests/loom_cli/test_cluster_render.py`** — already pins rendered YAML byte-for-byte. The migration keeps it green at every commit. The schema-driven macro must emit the same env block content that the hand-written template emits today, modulo non-semantic whitespace (which the golden test already normalizes).

If both stay green through every migration commit, the refactor is safe.

## Sequencing

Each step is independently revertible:

1. Land schema file + `src/loom_config/{loader,codegen,render}.py`. Not yet wired.
2. Codegen `_generated.py` files. Golden-test them against today's `config.py` field shape.
3. Per service, swap `config.py` to re-export from `_generated.py`. Keep call sites unchanged.
4. Per template, swap env block to the schema-driven macro. Render golden test stays green.
5. Replace `cluster_config.py` with the generic loader against `render_config`. Regenerate `config/cluster-config.example.toml`.
6. Add `loom cluster doctor` + wire into `loom cluster preflight`.
7. Add `loom cluster bootstrap-secrets` + update `docs/operator-runbook.md`, `.github/workflows/cluster-smoke.yml`, `.github/workflows/staging-smoke.yml` to use it.

## Edge cases handled at implementation time

- **Gateway's `LocalProviderConfig` parser** (multi-name env-var pattern `LOOM_GW_LOCAL_<NAME>_BASE_URL` + `_API_KEY`) stays as hand-written code in `loom_llm_gateway/config.py`. The schema covers one-name-one-entry; complex parsers remain imperative.
- **Worker-only Path fields** (`docker_socket`, `fixtures_root`, `benchmark_cache`, `trajectory_cache_dir`) map cleanly: `used_by = ["worker"]`, `python_type = "Path"`, optional via no `required`.
- **`extra = "forbid"`** stays on the generated `SettingsConfigDict`. Any env var not in the schema fails fast at service startup — same protection the hand-written classes have today.
- **Boolean envs** (`dev_reload`, `enable_worker_vllm`, `team_registration_open`) use `python_type = "bool"`; Pydantic's standard truthy-string parsing applies.
- **`generate` is a shell command, not a function name.** `loom cluster bootstrap-secrets` runs it via `subprocess.run` with `shell=False` after `shlex.split`. The operator sees the exact command in `--dry-run` output before any cluster mutation.

## Out of scope

- **Storage backend swap.** Decision deferred. Whichever backend wins, the schema handles it as a one-entry edit instead of the current 5+ file fan-out.
- **Migration of cluster_config_per-component fields (e.g. `worker_max_concurrent`)** that are operator-facing but also flow into a service env var. Surface in schema as a single `service_config` entry with `default_per_service` if needed; defer if a clean shape doesn't fall out.

## Related

- Tracking issue: carinrc#146 (historical archive).
- Caught and motivated by: carinrc PR #128 (staging smoke) runs #3 and #8.
- Will simplify: future object-store backend swap (decision tracked separately).
