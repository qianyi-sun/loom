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

The global manager's fenced pool-executor protocol is also separate from these
legacy assets. Its reservation, permit, inventory, and release receipts are
`executable: false`; this directory contains no scheduler-facing executor
service for them. Use the
[pool-executor dry-run runbook](../../docs/runbooks/global-fleet-pool-executor-dry-run.md)
for the implemented rehearsal and recovery procedure.

`personal-dev-activation-agent.yaml.example` is the operator template for the
independently keyed stable-route activation agent. Replace its image and Secret
placeholders with reviewed immutable values before an authorized apply. The
template does not enable the service-side controller, restricted builder, or
physical worker capacity.

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
only that prepared runtime directory, read-only. A percent-encoded
`database-url` keeps its original SQLAlchemy meaning; migration escapes percent
signs only while passing the URL through Alembic's ConfigParser.

The first reviewed bootstrap-authority replacement is recorded by an
append-only audit marker in the same database transaction. Replaying the same
UUID backfills a marker missing from an earlier bootstrap implementation and is
otherwise idempotent; duplicate, contradictory, or later different binding
evidence fails closed. The DNS-label-safe, length-bounded migration Job name
combines its migration head
and manager-image digest with a digest of the canonical complete Job spec and
exact head. Any immutable spec change creates a new Job rather than attempting
to patch an existing Job.

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

No asset in this directory authorizes activation. The capacity profile is
render-only. The checked-in legacy fixture/timer still requires replacement,
and the obsolete global-development writer must be removed. The
zero-executable protected claim path and both pool-local executors must be
completed before Package 5 performs the fleet-wide freeze, adoption,
zero-capacity rehearsal, and bounded cutover.

Do not apply `shared-fixture.yaml`, install the legacy global autoscaler
service/timer, create DNS, or enable capacity from this directory before those
merge gates pass. The only activation authority is the re-scoped operations
gate in #906 under the global fleet design.

Do not run `kubectl apply` on rendered capacity-control-plane YAML during this
repository slice. A live apply belongs only to #906's explicit operator change
window after its pre-activation evidence and rollback gates are approved.

The implemented interfaces and disabled authority boundaries are documented in
[`Personal development environments`](../../docs/architecture/multi-dev-environments.md),
[`Global fleet capacity manager`](../../docs/architecture/global-fleet-capacity-manager.md),
and
[`Global development-fleet autoscaler`](../../docs/architecture/global-dev-fleet-autoscaler.md).
