# Shared development fleet

> Archived predecessor. Current development-fleet behavior is documented in
> `deploy/dev-fleet/README.md` and `docs/architecture/global-dev-fleet-autoscaler.md`.
> Only this predecessor README is archived. The YAML, environment example, and
> service/timer files named below remain in the active `deploy/dev-fleet/`
> directory and are not archive-local assets.

This directory contains legacy-inert assets from the first `loom dev`
implementation. Packages 2–4 of the global fleet design must replace or
converge them before use. In particular, the current manifest and
global-development timer must not be installed or activated as written.

## Target architecture

- The converged fixture runs in `loom-dev` with one development PostgreSQL
  server and one MinIO server. Every environment gets a derived role/database
  and three buckets. Database `PUBLIC` connectivity is revoked, and the
  fixture admin sidecar creates a dedicated MinIO user/policy that can name
  only those three buckets. Shared root credentials never enter an instance
  namespace.
- The trusted lifecycle service in `loom-dev` owns the durable personal
  environment registry and guarded lifecycle API. It renders candidate
  frontend, control-plane, gateway, service, migration, Service, and Ingress
  objects into `loom-dev-<name>` and installs the candidate-independent
  capacity agent/claim guard. Requests claim an operation and return `202`; an
  independently-sessioned runner executes or resumes the fenced lifecycle.
- The global capacity manager allocates across production, staging, and every
  personal environment. Pool-local OLDLAB and GB10 executors apply its exact
  grants. The earlier global-development timer is a legacy implementation
  input and must not be activated beside the global writer.

## Activation

No asset in this directory currently authorizes activation. Package 4 must
first replace the manifest namespace with `loom-dev`, install the trusted
lifecycle and personal-candidate boundaries, and remove the obsolete
global-development SQLite writer. Packages 2 and 3 must provide protected
claim admission and both pool-local executors. Package 5 then performs the
fleet-wide freeze, adoption, zero-capacity rehearsal, and bounded cutover.

Do not apply `shared-fixture.yaml`, install
`loom-global-dev-fleet-autoscaler.*`, create DNS, or enable capacity from this
directory before those merge gates pass. The only activation authority is the
re-scoped operations gate in #906 under the global fleet design.
