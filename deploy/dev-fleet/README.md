# Shared-development fleet assets

This directory contains the render-only global capacity-authority profile plus
compatibility inputs for the disabled global development-fleet implementation.
No file here authorizes capacity or cluster mutation.

`shared-fixture.yaml`, `dev-fleet-autoscaler.env.example`, and the global
autoscaler service/timer are installation templates for the disabled
registry-driven development supervisor. Do not apply or install them merely
because they are present in the repository. The checked-in development
environment-state profile keeps its pool policies and external supervisors
disabled, and any running global-development timer sourced from this directory
is configuration drift.

The personal-development lifecycle uses a separate candidate builder,
activation agent, capacity-agent installation, and global-manager projection
checkpoint. A ready lifecycle operation includes an initial non-executable
capacity publication, but it does not activate the fixture or global autoscaler
templates in this directory and does not grant physical worker capacity.

The already-configured capacity-agent service now runs two independent trusted
reporter loops with the same owner-only agent database URL, guard URL, and
reporter bearer token: demand publication and executable protected-release
publication. This adds no credential, install, start, enable, apply, or
activation action, and the executable new-capacity ceiling remains exactly `0`.

The global manager's permanent v1 dry-run pool-executor protocol is also
separate from these legacy assets. Its reservation, permit, inventory, and
release receipts remain `executable: false`. The distinct v2 contracts are
`executable: true`, but this directory activates no scheduler-facing executor
service for them. Use the
[pool-executor dry-run runbook](../../docs/runbooks/global-fleet-pool-executor-dry-run.md)
for the implemented rehearsal and recovery procedure.

The shared personal-management package has its own
[shadow rehearsal](../../docs/runbooks/personal-dev-management-plane-shadow.md),
separate from the
[global-capacity zero-ceiling rehearsal](../../docs/runbooks/executable-global-capacity-bridge-rehearsal.md).
Both the personal-management shadow and the global-capacity zero-ceiling shadow
must report ready before the later acceptance interlock can be considered.
Neither shadow readiness enables personal mutations or physical capacity.

After both shadows and one exact protected release are ready, use the
[zero-capacity acceptance runbook](../../docs/runbooks/personal-dev-zero-capacity-acceptance.md).
Its `render-acceptance` and `status-acceptance` commands bind the enabled
management plane to an owner-only acceptance plan, exact global-manager
execution identity, a monotonic configuration-epoch floor, ceiling zero,
reviewed builder/scanner inputs, and an immutable rollback shadow. Repository
assets alone do not authorize its live apply.

`personal-dev-activation-agent.yaml.example` is the operator template for the
independently keyed stable-route activation agent. Replace its image and Secret
placeholders with reviewed immutable values before an authorized apply. The
template does not enable the service-side controller, restricted builder, or
physical worker capacity.

`capacity-pool-executor.toml.example` and
`loom-capacity-pool-executor.service` package the later two-pool executable
bridge inertly. They bind distinct GB10 and OLDLAB controller-local identities,
credentials, journals, and state directories, but the checked-in ceiling is
exactly zero and the systemd unit only validates configuration. It has no
install target and must never be started or enabled from this package. See the
[executable bridge rehearsal](../../docs/runbooks/executable-global-capacity-bridge-rehearsal.md).

The separate `loom-capacity-pool-executor-prepared.service` and `.timer` run
only the prepared zero-ceiling inventory path. The oneshot validates one exact
controller-local config and digest-pinned inventory policy, registers its
executor binding, publishes a journaled read-only `scontrol`/`squeue` snapshot,
heartbeats the confirmed journal head, and exits. It refuses shadow, active,
and drain-only authority and cannot construct the scheduler-mutation backend.
The service has no install target; the timer is installable only for an
explicit #906 window. Repository presence does not authorize installation,
start, or enablement.

## Capacity authority package

`capacity-control-plane.toml` is the reviewed, non-secret render source for
only the one global management authority: its independent PostgreSQL database,
migration/bootstrap Job, capacity-manager Deployment and internal Service, and
least-access NetworkPolicies. It does not render the shared application
fixture, any personal application, a pool executor, a scheduler-facing daemon,
or an activation operation.

Trusted image CI must first publish the exact manager image digest for the
commit being reviewed. Capture deterministic render evidence with a reviewed,
non-nil authority UUID. Replace the shape-valid example digest and UUID below
with those reviewed values:

```bash
install -d -m 0700 artifacts/capacity
render_path="$(mktemp artifacts/capacity/control-plane.XXXXXX.yaml)"
uv run --no-sync loom admin capacity-control-plane render \
  --file deploy/dev-fleet/capacity-control-plane.toml \
  --manager-image ghcr.io/qianyi-sun/loom-capacity-manager@sha256:1111111111111111111111111111111111111111111111111111111111111111 \
  --authority-incarnation 11111111-1111-4111-8111-111111111111 \
  > "$render_path"
printf 'rendered evidence: %s\n' "$render_path" >&2
```

That command preserves the default policy-disabled shadow manifest. A
zero-ceiling preparation render additionally requires an independently
reviewed canonical execution-policy file and its exact SHA-256:

```bash
execution_policy=/absolute/owner-only/path/execution-policy.json
test -f "$execution_policy" && test ! -L "$execution_policy"
test "$(stat -c %u "$execution_policy")" = "$(id -u)"
test "$(stat -c %a "$execution_policy")" = 600
execution_policy_sha256="$(sha256sum "$execution_policy" | awk '{print $1}')"
mapfile -t external_manager_client_cidrs < <(
  printf '%s\n' \
    '<observed-oldlab-or-operator-source>/32' \
    '<observed-gb10-source>/32' \
  | sort -u
)
external_manager_client_args=()
for cidr in "${external_manager_client_cidrs[@]}"; do
  external_manager_client_args+=(--external-manager-client-cidr "$cidr")
done

uv run --no-sync loom admin capacity-control-plane render \
  --file deploy/dev-fleet/capacity-control-plane.toml \
  --manager-image ghcr.io/qianyi-sun/loom-capacity-manager@sha256:1111111111111111111111111111111111111111111111111111111111111111 \
  --authority-incarnation 11111111-1111-4111-8111-111111111111 \
  --execution-policy-file "$execution_policy" \
  --execution-policy-sha256 "$execution_policy_sha256" \
  "${external_manager_client_args[@]}" \
  > "$render_path"
chmod 0600 "$render_path"
```

The renderer validates the file before stdout, creates one immutable
digest-addressed ConfigMap, and binds its full digest separately. The projected
policy is copied into a manager-UID-owned mode-`0600` file on a memory-backed
volume; only that copied directory reaches the manager read-only. Supplying
only one policy argument, missing external-client evidence, unsafe
ownership/mode, changed bytes, a noncanonical payload, or the wrong digest
fails closed. The repeated external-client values are 1–8 sorted, unique,
reviewed `/32` or `/128` effective source routes for the controller/operator
paths; broader or guessed routes are rejected.

The referenced `loom-capacity-manager` Kubernetes Secret is not rendered or
created. It must already contain exactly the keys consumed by this release:

- PostgreSQL: `postgres-user`, `postgres-password`, `postgres-database`;
- manager database: `database-url`;
- authorization registries: `principals.json`,
  `ownership-public-keys.json`;
- manager server trust: `server-ca.pem`, `server-certificate.pem`,
  `server-private-key.pem`, `client-ca.pem`; and
- dedicated in-pod probe identity: `health-certificate.pem`,
  `health-private-key.pem`.

Only the credential-preparation init containers mount the projected Secret.
They copy this exact bounded file set to UID-owned mode-0600 regular files on a
memory-backed volume. The migration and manager application containers mount
only that prepared runtime directory, read-only. The copier pins one Kubernetes
`..data` generation by directory descriptor, validates every standard key
symlink, and rechecks the generation before installation; a projection rotation
therefore fails closed instead of mixing credentials. A percent-encoded
`database-url` keeps its original SQLAlchemy meaning; migration escapes percent
signs only while passing the URL through Alembic's ConfigParser.

The initial migration records its generated bootstrap UUID in a canonical
append-only seed event in the same transaction. A reviewed replacement consumes
only that one pristine seed and records its own binding event while holding the
authority row lock. A legacy markerless database permits only same-UUID
backfill; duplicate, malformed, contradictory, or later different reserved
evidence fails closed. The DNS-label-safe, length-bounded migration Job name
combines its migration head
and manager-image digest with a digest of the canonical complete Job spec and
exact head. Any immutable spec change creates a new Job rather than attempting
to patch an existing Job. PostgreSQL connections use fixed 10-second connect,
30-second lock, and 300-second statement bounds; the Job has a 900-second active
deadline. PostgreSQL's startup probe allows up to ten minutes for initialization
or recovery before liveness can restart it.

After a separately authorized #906 deployment, the read-only status check is:

```bash
uv run --no-sync loom admin capacity-control-plane status \
  --namespace loom-dev \
  --kubeconfig /absolute/path/to/kubeconfig
```

Success is exactly one canonical line:

```json
{"executable_new_capacity_ceiling":0,"status":"ready"}
```

Any other output or nonzero exit status is a failed readiness/evidence check.
The command performs an mTLS probe inside the manager Pod. Readiness also
requires the mounted server certificate to contain the `127.0.0.1` IP SAN and
the `loom-capacity-manager.loom-dev.svc.cluster.local` DNS SAN. The command does
not expose the manager or copy Secret contents through command arguments.

## Activation

No asset in this directory authorizes live activation. The capacity CLI is
non-installing and render/status-only. Protected prepare, activate, drain,
retire, and abort routes are database authority mutations and may be used only
in #906's explicit operator window. Prepared executor registration and
inventory are automatic after each controller's prepared-only timer is
explicitly enabled. The separately packaged active timer requires the exact
owner-reviewed positive runtime artifact and remains disabled until activation.

The latest read-only live audit found no `loom-dev` deployment and confirmed
that environment-local OLDLAB and GB10 autoscalers remain authoritative. The
global path therefore is not operational merely because these assets exist.
The complete policy/freeze, render, shadow-deploy, prepare, inventory,
readiness, activation, emergency drain, retirement, and prepared-only abort
sequence is in the
[executable bridge rehearsal](../../docs/runbooks/executable-global-capacity-bridge-rehearsal.md).

Do not apply `shared-fixture.yaml`, install the legacy global autoscaler
service/timer, create DNS, or enable capacity from this directory before those
merge gates pass. The only activation authority is the re-scoped operations
gate in #906 under the global fleet design.

Do not run `kubectl apply` on rendered capacity-control-plane YAML during this
repository slice. A live apply belongs only to #906's explicit operator change
window after its pre-activation evidence and rollback gates are approved. No
CLI subcommand implements apply, install, start, or a ceiling change; the
least-scope manager HTTP transition and separately enabled controller-local
active service implement the protected runtime path.

The implemented interfaces and disabled authority boundaries are documented in
[`Personal development environments`](../../docs/architecture/multi-dev-environments.md),
[`Global fleet capacity manager`](../../docs/architecture/global-fleet-capacity-manager.md),
and
[`Global development-fleet autoscaler`](../../docs/architecture/global-dev-fleet-autoscaler.md).
