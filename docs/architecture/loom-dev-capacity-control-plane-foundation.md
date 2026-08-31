# `loom-dev` Capacity Control-Plane Foundation Design

**Status:** Approved for autonomous implementation under the global-fleet design and issue #906.

**Date:** 2026-08-11

## Purpose

Package 5 needs a deployable home for the global capacity authority before any
fleet cutover can be rehearsed. The manager implementation and its independent
`capacity_*` migration tree exist, but the repository has no release artifact
that installs them. The only checked-in `loom-dev` fixture is explicitly legacy
and inert.

This slice adds a reviewed, render-only release for the trusted capacity
foundation in `loom-dev`:

- one independent capacity PostgreSQL StatefulSet and Service;
- one capacity migration and authority-bootstrap Job;
- one global capacity-manager Deployment and ClusterIP Service;
- default-deny and least-access NetworkPolicies; and
- an mTLS health probe and read-only operator status command that require the
  manager to report `executable_new_capacity_ceiling = 0`.

It does not install pool executors, change GB10 or OLDLAB scheduling, deploy
personal applications, expose the manager outside the cluster, apply manifests,
or authorize Package 5 activation.

## Design choice

### Selected: focused typed renderer

A dedicated Python renderer owns this small infrastructure boundary. It accepts
validated non-secret release inputs, builds Kubernetes objects as typed mapping
structures, and serializes deterministic multi-document YAML. A narrow
`loom admin capacity-control-plane` command exposes `render` and read-only
`status` operations.

This keeps the global authority independent of application-cluster profiles and
lets tests inspect the parsed objects instead of matching template text.

### Rejected: extend `loom cluster`

The existing cluster renderer has mature release and image-drift machinery, but
it models an application deployment: application PostgreSQL, MinIO, frontend,
control plane, gateways, service, workers, and ingress. Making the global
capacity authority conditional inside that surface would couple its lifecycle
to every environment and obscure the rule that exactly one manager serves all
environments.

### Rejected: extend legacy environment-state apply

Environment-state apply is coupled to legacy per-environment autoscaler and
rollout-broker contracts being retired by the global-fleet migration. Reusing it
would preserve the authority topology this work replaces.

## Release inputs

The checked-in TOML profile contains only non-secret infrastructure policy:

- schema version `1`;
- namespace, fixed to `loom-dev`;
- existing Kubernetes Secret name;
- digest-pinned PostgreSQL image;
- capacity database PVC size and optional storage class;
- resource requests and limits for PostgreSQL, migration, and manager;
- Kubernetes DNS namespace, pod label selector, and port; and
- exact labels permitted to reach the manager.

Two generation-specific values are mandatory command inputs:

- the manager image as a complete immutable OCI reference ending in a 64-hex
  `@sha256:` digest; and
- the expected capacity authority incarnation as a UUID.

They are command inputs because a repository commit cannot know the registry
digest that trusted CI will publish for that same commit. The rendered manifest
contains the exact values and can be captured and reviewed as rollout evidence.
Mutable tags, zero digests, non-`loom-dev` namespaces, unknown TOML fields,
unsafe resource quantities, and invalid Kubernetes names fail before YAML is
emitted.

Per-container resources are capped at 64 CPUs and 1 TiB of memory, and the
capacity PostgreSQL claim is capped at 64 TiB. These high safety bounds catch
accidental unbounded quantities without constraining the checked-in profile.

The referenced Secret is never rendered or mutated. It must already contain the
following keys:

- `postgres-user`, `postgres-password`, and `postgres-database` for PostgreSQL;
- `database-url` for the migration and manager;
- `principals.json` containing only bearer-token hashes;
- `ownership-public-keys.json` containing executor verification public keys;
- `server-ca.pem`, `server-certificate.pem`, and `server-private-key.pem` for
  manager TLS;
- `client-ca.pem` for mandatory client-certificate validation; and
- `health-certificate.pem` and `health-private-key.pem` for the in-pod health
  client.

The health certificate must be signed by `client-ca.pem`, and
`server-certificate.pem` must cover both
`loom-capacity-manager.loom-dev.svc.cluster.local` and `127.0.0.1` so the
in-pod probe can validate the same server identity without weakening TLS.

## Rendered objects

Objects are emitted in dependency order:

1. `Namespace/loom-dev`, enforcing the latest Restricted Pod Security profile;
2. `Service/loom-capacity-postgres`;
3. `StatefulSet/loom-capacity-postgres` with one persistent volume;
4. `Job/loom-capacity-migrate-<migration-head>-<image-prefix>-<template-prefix>`;
5. `Service/loom-capacity-manager` on TCP 8443;
6. `Deployment/loom-capacity-manager` with one replica and `Recreate` strategy;
7. one default-deny policy and explicit DNS, database, and manager access
   policies.

Every Pod disables service-account token automount, runs with RuntimeDefault
seccomp, drops all capabilities, forbids privilege escalation, uses a read-only
root filesystem where the upstream PostgreSQL image permits it, and declares
requests and limits. The manager and migration containers run as UID/GID 65532.
Their init containers run the same immutable manager image and copy the exact
projected Secret file set into a memory-backed volume as UID-owned mode-0600
regular files. Only the init containers mount the projected Secret. The
application containers mount the prepared memory-backed runtime directory
read-only and never consume Kubernetes Secret symlinks directly. Credential
preparation opens and pins one projected `..data` generation, reads every key
through that held generation directory, verifies the standard `key ->
..data/key` links, and rechecks the binding before installation. A concurrent
Secret rotation fails closed and leaves no partial runtime directory.

The manager Service is ClusterIP-only. Manager ingress permits TCP 8443 from
pods bearing the trusted capacity-agent label in any namespace and from the
trusted lifecycle-service label in `loom-dev`. mTLS remains the authentication
boundary if a pod label is spoofed. PostgreSQL ingress permits TCP 5432 only
from the manager and migration labels. Manager and migration egress permits
only PostgreSQL and the configured cluster DNS selector. PostgreSQL receives no
egress exception.

## Migration and authority bootstrap

The migration Job uses the manager image, copies only its required database URL
credential, runs `alembic -c capacity_migrations/alembic.ini upgrade head`, and
then binds the configured authority incarnation.

Binding is idempotent when the database already contains the expected UUID. If
the stored UUID differs, bootstrap may replace the migration-generated UUID only
when all of these facts hold in one locked transaction:

- writer epoch is zero;
- recovery mode is `shadow`;
- increase freeze is enabled;
- executable, pending-slot, pending-job, and submission-rate ceilings are all
  zero; and
- every other `capacity_*` table is empty.

Any other state fails closed without changing the UUID. This permits a new
database to receive a reviewed stable identity but prevents a delayed or
misconfigured Job from reincarnating an authority that has already been used.
The initial schema migration writes one canonical append-only seed event beside
its generated UUID in the same transaction. Replacement requires that exact
pristine seed, holds the authority row lock, and writes one append-only binding
event in the replacement transaction. A different later UUID therefore cannot
replace the reviewed identity even before writer registration. A legacy
markerless database permits only same-UUID backfill. Exact replay is idempotent;
duplicate, malformed, or contradictory reserved events fail closed.
Percent-encoded database URLs are escaped only at Alembic's ConfigParser
boundary and retain their original SQLAlchemy meaning.
Both the Alembic and authority-binding connections enforce non-overridable
10-second connection, 30-second lock, and 300-second statement timeouts. The
Job also has a 900-second active deadline. PostgreSQL has a startup probe with a
ten-minute initialization/recovery allowance, after which its existing
readiness and liveness probes retain their normal bounds.
The database constraints continue to prohibit a nonzero executable ceiling.
Nil authority UUIDs are rejected before the migration command reads its
database URL or changes schema state, and command failures emit no database
diagnostic or URL.

The DNS-label-safe, length-bounded Job name includes the Alembic head and
manager image digest prefix, plus a digest of the canonical complete Job spec
and exact migration head. Any immutable spec change—including the authority
UUID, Secret name, or migration resources—therefore creates a new Job rather
than attempting to patch an existing Job template.

## Manager startup and health

The manager starts only with owner-only runtime files and the exact expected
authority UUID. Its existing startup checks require the independent schema at
head, the expected authority incarnation, and the current database writer
fence. It listens only on HTTPS with mandatory client certificates.

A new health-probe module performs a real HTTPS request with the dedicated
health client certificate. Before the request, it also parses the exact server
certificate used by the Deployment and requires both the loopback IP SAN and
`loom-capacity-manager.loom-dev.svc.cluster.local` DNS SAN. Success requires all
of the following exact facts:

- HTTP status 200;
- canonical JSON object fields `status` and
  `executable_new_capacity_ceiling` only;
- `status == "ready"`; and
- `executable_new_capacity_ceiling == 0` with integer type.

The Deployment uses that module for startup and readiness. Liveness checks only
that the TLS listener still accepts TCP connections, so a temporary database
outage makes the Pod unready without creating a tight restart loop. The manager
already permanently revokes readiness when its writer fence or zero ceiling
changes; normal deployment reconciliation then requires an explicit restart
after the operator resolves the state.

`loom admin capacity-control-plane status` executes the same probe inside the
manager Pod through `kubectl exec`. It is read-only, prints the exact health JSON
on success, forwards no secret material through arguments or output, and returns
nonzero for missing workloads, mTLS failure, manager unready state, or a nonzero
ceiling. The in-Pod probe ignores proxy environment variables, and the wrapper
adds a 15-second process timeout outside kubectl's 10-second request timeout.

## CLI behavior

Render:

```text
loom admin capacity-control-plane render \
  --file deploy/dev-fleet/capacity-control-plane.toml \
  --manager-image ghcr.io/qianyi-sun/loom-capacity-manager@sha256:<64-hex> \
  --authority-incarnation <uuid>
```

The command writes YAML to stdout only. There is deliberately no `apply`,
`activate`, or ceiling option.

Status:

```text
loom admin capacity-control-plane status \
  --namespace loom-dev \
  --kubeconfig <path>
```

The status command accepts only the fixed `loom-dev` namespace. It invokes a
fixed executable and fixed credential paths in the manager container.

## Image publication

`deploy/Dockerfile.capacity-manager` packages only the repository Python
runtime and `capacity_migrations`. It runs the manager as UID/GID 65532 and is
registered as the `loom-capacity-manager` release image in the typed component
ownership manifest. Existing image planning, native AMD64/ARM64 builds,
job-local untrusted PR scanning, and trusted push publication are therefore
inherited without a new workflow-owned image allowlist. Official publication
never reuses candidate bytes: each hosted `publish` architecture rebuilds its
Docker archive from the protected release commit, scans that source-fresh
archive, and only then loads and pushes it. PR build archives remain local to
their untrusted jobs and are not uploaded; the publisher never downloads,
loads, scans as release, attests, or publishes them. Both untrusted validation
and the trusted release scan use pinned Trivy v0.74.0 with scan type `image`, OS
and library vulnerability types, a `20m0s` timeout, `CRITICAL` severity, exit
code 1, unfixed findings included, the `vuln` scanner only, and caching disabled.
The trusted scan selects fixed config and a reviewed ignore file generated
outside the checkout. Fixable findings are remediated in their images first.
The temporary policy covers the three unfixed CRITICAL Perl CVEs
(CVE-2026-13221, CVE-2026-42496, and CVE-2026-8376) only for the Debian Perl
packages required by Debian base runtimes, the agent toolchain, and the
staging-compatible PostgreSQL 17.4 rehearsal image, CVE-2026-43185 only for
the agent compiler's
`linux-libc-dev`, and CVE-2025-7458, CVE-2026-6653, and CVE-2023-45853 only
for required PostgreSQL 17.4 rehearsal dependencies. Each exception carries
exact Debian PURL scopes and a review statement and expires at 2026-09-12 UTC;
policy generation fails closed at that boundary.
A repository-owned installer accepts only the architecture-
specific v0.74.0 release archive whose repository-pinned SHA-256 matches,
avoiding any third-party action forbidden by repository policy. The publisher
captures the one digest emitted by the push and uses the immutable subject only
for registry validation, SLSA v1 attestation, and strict verification; it never
resolves the mutable architecture build tag.

The predicate binds the Trivy scanner name/version, controlled config and
ignore hashes, explicit exception IDs, package scopes, statements, and expiries,
release URL and architecture archive digest, scan-report digest, release
commit/tree/ref/run/attempt, Dockerfile, context, and platform.
Official records accept only `trusted-rebuild`, binding the archive built in
the current release run; no candidate artifact identity enters the predicate
or record. Only after architecture attestation verification does the publisher
upload one canonical immutable record containing those source identities,
mode, subject name/digest, and scan digest. The manifest publisher
downloads exactly the AMD64 and ARM64 records for the current image and run,
validates the two-record set fail-closed, re-verifies each recorded immutable
subject, and joins only those two digests. It creates only a temporary
`manifest-${HEAD_SHA}` tag, captures the creation digest once, validates,
attests, and verifies by that digest, and only then promotes the release SHA and
branch tags. Until the
live lease broker recognizes this new image key, its AMD64 build uses a hosted
runner like the other newly introduced trusted control-plane images.

## Failure handling

- Invalid config or release identity returns CLI exit code 2 and emits no YAML.
- Secret absence, projection rotation, or an unexpected projected file set
  fails the init container.
- Migration errors use fixed client and Job deadline bounds and leave the
  manager unready.
- Authority mismatch on a used database never mutates the database.
- A stale schema, incorrect UUID, unavailable database, lost writer fence, or
  nonzero ceiling makes `/healthz` return 503 and status fail.
- Network selector mistakes deny traffic rather than broadening it.
- The renderer never contacts Kubernetes, a registry, Slurm, or a live
  capacity manager.

## Verification

Unit tests parse rendered YAML and prove exact object identity, namespace,
immutable images, Secret references, Pod Security settings, probes, resource
bounds, ClusterIP-only exposure, and least-access network rules. Mutation-style
tests reject mutable images, zero digests, namespace drift, unknown fields,
invalid resources, and authority-binding attempts against any used database.

The credential-copy tests use Kubernetes-style `..data` symlinks and prove
owner-only nonsymlink destinations, exact file sets, idempotence, drift refusal,
single-generation rotation refusal, and cleanup after partial failure. Health
tests use a real loopback mTLS server
and prove rejection of missing/untrusted clients, malformed JSON, wrong fields,
not-ready state, and any nonzero or non-integer ceiling.

CLI tests prove render is stdout-only, status uses fixed in-container paths, and
no apply/activate surface exists. Component-ownership tests prove the new
Dockerfile is owned and included in the derived image matrix. Repository Ruff,
Mypy, relevant unit/integration tests, manifest validation, and Docker image
build complete before the branch is proposed for merge.

## Activation boundary

Merging this slice publishes deployment machinery; it does not deploy it. A
live apply remains part of #906's explicit operator change window and requires
the Package 5 pre-activation evidence, both pool-local executors, complete
legacy-writer freeze/adoption evidence, and #896/#822 closure. Even after an
authorized apply, this release is incapable of authorizing new capacity while
the database and health contracts remain at their repository-enforced ceiling
of zero.
