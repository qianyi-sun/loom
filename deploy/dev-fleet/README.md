# Shared development fleet

This directory contains legacy-inert assets from the first `loom dev`
implementation. Packages 2–4 of the global fleet design must replace or
converge them before use. In particular, the current manifest and
global-development timer must not be installed or activated as written.

The application now contains the zero-executable personal lifecycle,
candidate-builder boundary, independently owned capacity guard/agent installer,
and global-manager projection checkpoint. Those components deliberately do not
turn these legacy assets into an activation manifest.

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

No asset in this directory currently authorizes activation. The checked-in
legacy fixture/timer must be replaced by a reviewed `loom-dev` infrastructure
release and the obsolete global-development SQLite writer must be removed.
The zero-executable protected claim path and both pool-local executors must be
completed before Package 5 performs the fleet-wide freeze, adoption,
zero-capacity rehearsal, and bounded cutover.

Do not apply `shared-fixture.yaml`, install
`loom-global-dev-fleet-autoscaler.*`, create DNS, or enable capacity from this
directory before those merge gates pass. The only activation authority is the
re-scoped operations gate in #906 under the global fleet design.

Before enabling the personal lifecycle service in `loom-dev`, the operator must
provide an immutable `personal_dev_capacity_agent_image`, the exact oldlab and
GB10 capability JSON, and distinct owner-only regular files for the lifecycle
bearer token and lifecycle client CA/certificate/private key, plus a separate
reporter-agent client CA/certificate/private key. The lifecycle bearer principal
needs `capacity:project:development` and `capacity:read`; its plaintext token is
never copied into a personal namespace. Each dynamically generated reporter
gets a distinct token whose hash is registered in the manager projection. The
privileged lifecycle client certificate is also never copied into a personal
namespace; only the lower-authority reporter-agent certificate is installed.
operator must also set the exact manager and shared-PostgreSQL namespace,
pod-label-key, pod-label-value, and TCP port selectors used by the generated
egress policy, plus the cluster DNS namespace/label/port selector (UDP and TCP).
The service fails startup if any of these inputs is missing, mutable, malformed,
or not owner-only; an incorrect network selector prevents the agent Deployment
from becoming ready.
