# Multiple per-developer dev environments

Status: design

Refs: #857 (dev/staging/prod identity), #894 (subdomain-per-env),
`docs/architecture/issue45-worker-autoscaler-design.md` (worker-pool
autoscaler), #892 (prod-pressure drain/reclaim).

## Goal

Let each developer bring up a **persistent, isolated dev environment on the
shared fleet** (co-located with staging/prod), **self-service and
guardrailed**, sharing the external-Slurm worker capacity that the shipped
autoscaler arbitrates. This generalizes the fixed three-environment identity
contract (`development`/`staging`/`production`) to **N per-developer dev
instances** named `dev-<name>`.

## Non-goals

- Ephemeral per-PR / per-branch preview environments (a possible later use of
  the same primitives).
- A general new scheduler. Capacity reuses the **shipped** per-pool autoscaler
  (`min_slots`/`max_slots` per `(environment, pool)`) + Slurm's own scheduler for
  contention + #892 prod-pressure reclaim. This design *adds* the pieces the
  shipped code lacks for many co-tenant pools — a fleet budget cap, pool-scoped
  demand, and prod-first reclaim wiring — but not a new arbitration engine.
- The local laptop dev path (`deploy/local/`, kind + in-cluster `k8s_worker`)
  is unchanged and remains the zero-dependency offline path.

## Identity model — everything derived from `name`

A dev instance is identified by one slug `name` plus an `owner`. Every identity
field is a **pure, non-overridable function of `name`** — the core guardrail, so
an instance can never point at another instance's or a base env's
namespace/DB/bucket/route/pool:

| Field | Derived value |
|---|---|
| runtime env (new class) | `dev-<name>` |
| namespace | `loom-dev-<name>` |
| database | `loom_dev_<name>` (on the shared dev-Postgres) |
| DB role | `loom_dev_<name>` (granted only on its own database) |
| buckets | `loom-dev-<name>-{tasks,trajectories,artifacts}` |
| route | `<name>.dev.yylx.world` (subdomain, #894) — interim `/dev-<name>` path |
| worker pool | `dev-<name>` |
| secrets/tokens | per-instance (secret-store key, service + worker tokens) |
| autoscaler policy key | `(environment=dev-<name>, pool=dev-<name>)` |

**Name rules:** `^[a-z][a-z0-9-]{1,20}$`; the base-env identifiers
(`development`, `dev`, `staging`, `production`, `prod`) and any name whose
derived identity collides with a base env are **reserved and rejected**.

## Data plane — shared fixture, per-instance logical isolation

The naive shape (a full per-instance stack: Postgres + MinIO + control-plane +
service + gateway + routers) is ~10 pods **each**, on the same fleet as prod —
an untenable idle tax at N developers. Instead:

- **One shared `loom-dev-shared` fixture** on the fleet — a single dev-Postgres
  + single dev-MinIO — deployed and operated once (operator-managed base
  deployment, not per developer).
- Each instance gets **strong logical isolation** on that fixture:
  - a **dedicated database** `loom_dev_<name>` with a **dedicated role**
    granted only on that database (a compromised/buggy instance CP cannot reach
    another instance's data);
  - **dedicated buckets** `loom-dev-<name>-*` (buckets are already the object
    tenancy boundary);
  - a **dedicated namespace**, secrets, and tokens.
- Per instance, only the **control-plane + service** run (config points at the
  shared fixture via `--storage external` / an external DB URL + object-store
  endpoint). No per-instance Postgres/MinIO pods.

This is *not* logical multi-tenancy inside one database (row-level tenancy was
rejected for weak isolation). Separate databases + roles + buckets + namespaces
+ tokens is full isolation — just one shared server rather than N.

## Control plane — per instance, created suspended

Per instance: namespace `loom-dev-<name>`, control-plane + service Deployments,
an ingress route. **Created suspended** (workloads scaled to 0) by default, with
**lazy resume** on first API hit / trial submit. A persistent-but-idle instance
therefore costs ~its DB rows + bucket metadata and near-zero compute.

## Worker / execution — shared Slurm via the autoscaler

`k8s_worker` is disabled; trials run on the external Slurm fleet via a
per-instance pool `dev-<name>`. Each instance registers one
`WorkerPoolAutoscalerPolicy` with `min_slots = 0` and `max_slots =
min(requested, PER_INSTANCE_CAP)`.

The shipped autoscaler reconciles each policy **independently** — it scales a
pool's desired slots from *that pool's own* queued demand between min/max and
submits Slurm jobs for it. Realities verified against the code (not assumed),
which shape this design:

- **No policy `submit_priority` / `preemptible` field.** `submit_priority` lives
  on `Trial` and only orders trial *claiming*, never autoscaler allocation. The
  policy carries only min/max slots, thresholds, cooldowns
  (`WorkerPoolAutoscalerPolicy`, `schema.py:1606`).
- **No fleet-level arbitration or budget in Loom.** Each pool clamps only to its
  own `max_slots`; nothing sums slots across pools, so Σ max_slots can exceed the
  fleet unchecked and cross-pool contention is left entirely to **Slurm's own
  scheduler**.
- **Prod-pressure reclaim (#892) is a per-pool, externally-signalled drain** —
  something POSTs a `prod-pressure` signal (carrying `preemptible`) to a specific
  `(env, pool)`; there is no automatic "prod busy → drain dev" logic inside the
  autoscaler.
- **Demand isn't pool-scoped unless the trial pins it** — a queued trial with no
  `requires_caps.worker_pool` counts as demand for *every* pool.

So making N dev pools coexist safely with staging/prod requires these additions,
which are part of this work:

1. **Pool-scoped demand.** The guarded endpoint configures each instance so its
   trials carry `requires_caps.worker_pool = "dev-<name>"`, so a dev pool only
   scales for its own trials (not staging/prod backlog).
2. **Fleet budget admission cap.** Extend the policy upsert / guarded endpoint to
   reject a dev policy when `Σ (dev max_slots) > DEV_FLEET_BUDGET` — the Loom-side
   ceiling the shipped code lacks, so N devs can't collectively exceed the dev
   tier's share.
3. **Prod-first reclaim.** Reuse the #892 mechanism by having the prod-pressure
   driver target every `dev-<name>` pool with `preemptible = true` (staging with a
   longer grace) whenever prod demand exceeds free capacity. This is the
   env-priority path the team chose (caps + #892 reclaim, **not** Slurm QoS —
   #896) and is in flight (inc-2 #1151).
4. **Fair-share among dev pools.** Loom has none today; with a small
   `PER_INSTANCE_CAP` + `DEV_FLEET_BUDGET`, total dev contention is bounded and
   Slurm's scheduler fair-shares the dev pools' pending jobs. A Loom-side
   fair-share pass is a follow-up only if Slurm-level fairness proves
   insufficient.

## Provisioning — self-service, enforced server-side

Self-service safety comes from a **server-side guarded endpoint**, never from a
client-side check (a developer cannot be trusted to run the validator or to
call the raw admin policy API — they could set a priority above staging).

- **`POST /api/v1/dev-instances {name}`** (auth: any submitting user). The
  control-plane derives the identity, runs the guardrail validator, and — only
  if the whole shape is inside the dev envelope — provisions: namespace,
  database + role, buckets, secrets, tokens, control-plane + service, the
  (capped, dev-tier, preemptible) autoscaler policy, and the route. **This is
  the only path that writes a `dev-<name>` autoscaler policy.**
- **`DELETE /api/v1/dev-instances/{name}`** (owner or operator): drops the
  policy (draining `dev-<name>` Slurm jobs), deletes the namespace, drops the
  route, and drops the database/buckets unless `--keep-data`.
- **`GET /api/v1/dev-instances`** (list; `--mine` filters by owner label).

**`loom dev create|destroy|list|status`** is a thin client over these
endpoints — it does not touch `cluster`/admin APIs directly. `create` waits for
readiness by default (`--no-wait` to skip).

## Guardrail validator — static (base envs) + runtime (instances)

The existing `scripts/validate_environment_isolation.py` is a **static** check
over the three **committed** base-env profiles, and requires exactly those three.
Dev instances are created at **runtime** (self-service, not committed TOML), so
the generalization is split:

- **New pure module** `loom/dev_instance.py` — the source of truth used by the
  endpoint *and* unit-tested:
  - `derive_identity(name) -> DevInstanceIdentity` — the pure function behind the
    identity table above.
  - `validate_dev_instance(name, requested_policy, existing_instances, base_envs)
    -> list[error]` — enforces: name rules + reserved names; **every** identity
    field equals the derived value; namespace/DB/buckets/route/tokens **distinct
    across all live instances + base envs**; the `requested_policy` envelope
    (`actuator = slurm`, `max_slots ≤ PER_INSTANCE_CAP`, `min_slots ∈ [0, cap]`,
    `Σ live dev max_slots ≤ DEV_FLEET_BUDGET`; there is no policy
    priority/preemptible field — prod-first reclaim is a driver concern, below);
    route under the dev subdomain/prefix; no prod-adjacent grants. The guarded
    endpoint calls this **fail-closed before any mutation**.
- **Static validator** keeps validating the three committed base-env profiles
  unchanged, and gains **one** addition: assert the base-env identities never
  fall inside the reserved `dev-<name>` derived space (so the two namespaces can
  never collide).
- A **periodic drift audit** (a scheduled control-plane job) re-runs
  `validate_dev_instance` over the live labeled namespaces + policies.

Concrete envelope defaults (operator-tunable via control-plane config):
`PER_INSTANCE_CAP = 2` slots; `DEV_FLEET_BUDGET = 8` slots (a small fraction of
the fleet). Prod-first reclaim for dev pools is configured on the prod-pressure
driver, not a policy field.

## Lifecycle

Persistent by default. Idle cost is near-zero via created-suspended + lazy
resume + `min_slots = 0`. A later phase adds an **abandonment GC**: a scheduled
control-plane job flags instances with no activity for ~30 days → notifies the
owner → auto-suspends → reclaims after a further grace period. Destroy is
restricted to the owner or an operator.

## Error handling

- `create` validates **before any mutation** and is idempotent (get-or-create
  namespace/DB/role/buckets/secrets/policy), so a re-run converges rather than
  half-applying.
- `destroy` is idempotent and **drains `dev-<name>` Slurm jobs before deleting
  the namespace**, so no workers are orphaned.
- Name collisions and out-of-envelope requests are rejected up front with a
  clear reason.

## Testing

- **Unit:** identity derivation (pure function) and the generalized validator —
  table-driven over collisions, the envelope (priority/cap/budget/preemptible),
  distinctness across N + base envs, and reserved names.
- **Contract:** per-instance rendered manifests are isolated — extend the
  existing isolation golden tests to a sample `dev-<name>`.
- **Integration:** `create → up → policy registered → seed worker token → a
  trial runs on the `dev-<name>` pool → destroy leaves nothing orphaned` (no
  dangling Slurm jobs, policy gone, namespace gone).
- **Negative guardrail:** attempts to override identity toward a base env,
  exceed `PER_INSTANCE_CAP` / `DEV_FLEET_BUDGET`, raise priority above the dev
  tier, or collide with another instance are all rejected.

## Phasing (one spec, staged implementation)

1. Identity derivation + generalize the isolation contract 3 → N + guardrail
   validator (+ reserved names).
2. Shared `loom-dev-shared` Postgres/MinIO fixture + per-instance provisioning
   (database + role + buckets) via `cluster up --storage external`.
3. Guarded `POST/DELETE/GET /api/v1/dev-instances` endpoint + `loom dev` CLI.
4. Autoscaler resource management: `DEV_FLEET_BUDGET` admission cap +
   `PER_INSTANCE_CAP` at policy upsert; pool-scoped demand
   (`requires_caps.worker_pool`) on dev trials; prod-first reclaim wiring
   (prod-pressure driver targets `dev-<name>` pools).
5. Lifecycle: created-suspended + lazy resume (abandonment GC later).

## Open questions / dependencies / risks

- **Fair-share across dev pools:** resolved — Loom does *no* fleet-level
  arbitration; contention falls to Slurm's scheduler. This design bounds it with
  `PER_INSTANCE_CAP` + `DEV_FLEET_BUDGET` (so total dev demand is small) and
  leans on Slurm's fair-share for the rest. A Loom-side fair-share pass is a
  follow-up only if that proves insufficient.
- **Prod-first reclaim wiring** (#892) is a per-pool *external* signal with no
  automatic "prod busy → drain dev" logic in the autoscaler. This design's phase
  4 adds a driver that posts `preemptible = true` prod-pressure to `dev-<name>`
  pools under prod demand — aligned with #896 (caps + #892 reclaim, not Slurm
  QoS); depends on #896 inc-2 (#1151).
- **Subdomain routing** (`<name>.dev.yylx.world`) needs wildcard DNS + a
  `*.dev.yylx.world` certificate — the #894/#1114 subdomain work. Interim:
  `/dev-<name>` path on the existing ingress.
- **Live autoscaler validation** is gated on #906 → #896 (real Slurm workers
  connected). The multi-dev capability + the budget/demand/reclaim additions are
  buildable and unit/integration-testable now; validating *contended*
  autoscaling on the real fleet depends on those landing.
- **Model egress** from pods is intercepted on the current fleet (bb8-1 MITM);
  real-task validation uses the OracleAgent / offline-provisioned bundles or a
  local model until a clean-egress path exists.
