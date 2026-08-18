# Personal-development scanner-cache preparation

## Status and purpose

The personal-development management-plane shadow renders a 20 GiB scanner
cache PVC and mounts it into the management service. Zero-capacity acceptance
binds exact Trivy binary, vulnerability database, Java database, and finding
policy digests. The service verifies those digests before it enables the
restricted builder.

The current package has no authority that can put the two databases on a new
PVC. The acceptance runbook verifies owner-local database archives, but no
reviewed component consumes those archives. An empty PVC therefore makes an
otherwise valid acceptance deployment fail at startup.

This design adds a release-bound, networkless cache image and a management init
container. The init container verifies every protected cache file and publishes
one immutable generation on the existing PVC before the management process
starts. It neither enables personal lifecycle operations nor changes physical
capacity.

## Requirements

- Shared infrastructure remains only in `loom-dev`; personal applications
  remain in the `loom-dev-` namespace family derived from each owner name.
- The global manager remains the only capacity authority and its executable
  new-capacity ceiling remains exactly `0` throughout shadow and acceptance.
- Scanner preparation uses no Secret, service-account token, hostPath, node
  path, `kubectl cp`, runtime database download, or mutable image reference.
- The protected source commit pins the upstream Trivy database OCI manifest
  digests. Protected image CI records the extracted files and publishes the
  cache image as part of the exact personal-development trusted release.
- The service image's Trivy stage is
  `aquasec/trivy@sha256:be1190afcb28352bfddc4ddeb71470835d16462af68d310f9f4bca710961a41e`,
  whose AMD64 and ARM64 members report exactly version `0.70.0`. Release
  assembly rejects a different runtime version before it records the binary
  hash.
- The management service cannot modify the protected database files. Trivy may
  write only its separate `fanal` runtime cache.
- Publication is idempotent, bounded, crash-safe, and atomic at the generation
  directory boundary.
- An incomplete, changed, ambiguous, oversized, linked, or non-regular input
  fails closed without changing a valid generation.
- Shadow remains inert: lifecycle and builder flags are false, activation has
  zero replicas, and preparing scanner data grants no application or capacity
  authority.

## Considered approaches

### Dedicated cache image and init container

Protected CI builds `ghcr.io/qianyi-sun/loom-personal-dev-scanner-cache` from
digest-pinned upstream Trivy DB OCI artifacts. The personal trusted release
binds its image digest and the hashes of all four protected files. A restricted
init container publishes the files to the PVC.

This is the selected approach. It gives Kubernetes an ordinary immutable image
transport, needs no runtime network or credentials, co-mounts the RWO volume in
the management Pod, and leaves the general `loom-service` image free of roughly
one gigabyte of scanner data.

### Embed the databases in `loom-service`

This has the same trust model and a simpler image list, but every ordinary Loom
service deployment would pull and retain the large personal-development cache.
The databases are personal-management data rather than application runtime
code, so that coupling is rejected.

### Download or stream databases during the live window

A Job could download mutable registry tags, use temporary credentials to read
an object store, or receive operator-local files through an exec stream. These
paths add runtime network, credentials, transfer recovery, and mutable-source
failure modes. Even with a final hash check they are a weaker operational
authority than a protected, digest-pinned image, so they are rejected.

## Immutable source and release binding

The checked-in file
`deploy/dev-fleet/personal-dev-scanner-cache-lock.json` is canonical JSON with
no trailing newline. It contains exactly:

```json
{
  "binary_sha256": {
    "linux/amd64": "379d59f24a4a828c55de5f0b91b6805cc35d13580180b658820e648611256166",
    "linux/arm64": "5bf6066f08c972e0575660eaeb87b4f1bac0e527076dcbf88184bc9baa353f65"
  },
  "database": {
    "image": "ghcr.io/aquasecurity/trivy-db@sha256:01edd081af12fd613776b0db66ac23ce62c9d25802d8ee57671394c10ca3530b",
    "layer_sha256": "cafb664d1c10b65e06b317f86171d65ed1f17b1f4de594a7232e16c0848f3590"
  },
  "java_database": {
    "image": "ghcr.io/aquasecurity/trivy-java-db@sha256:58ef30d104106166d34f36c9861f2c5eb88d3279341fd4838bb5694d8998c436",
    "layer_sha256": "bcc9ee0a8aa79524502cf892eda69e2180b54a3c7bd54c874b564201d2bdfc10"
  },
  "schema_version": 1,
  "trivy_version": "v0.70.0"
}
```

The loader rejects tags, unexpected repositories, zero or malformed digests,
unknown fields, a noncanonical payload, and a Trivy version different from the
pinned installer policy. CI reads both OCI manifests by digest and requires one
expected database layer with the recorded digest and media type. It then runs
the pinned Trivy 0.70.0 binary with `--download-db-only` and
`--download-java-db-only`, passing only the two digest references as explicit
repositories. The download occurs once per workflow, and its exact output is a
short-lived workflow artifact consumed by both cache-image platform builds.

The personal trusted-release schema advances to version 2. Its image set adds
`personal_dev_scanner_cache`. It also adds this exact `scanner` object:

```json
{
  "binary_platform": "linux/amd64",
  "binary_sha256": "1111111111111111111111111111111111111111111111111111111111111111",
  "cache_identity_sha256": "2222222222222222222222222222222222222222222222222222222222222222",
  "database_metadata_sha256": "3333333333333333333333333333333333333333333333333333333333333333",
  "database_sha256": "4444444444444444444444444444444444444444444444444444444444444444",
  "java_database_metadata_sha256": "5555555555555555555555555555555555555555555555555555555555555555",
  "java_database_sha256": "6666666666666666666666666666666666666666666666666666666666666666",
  "lock_sha256": "7777777777777777777777777777777777777777777777777777777777777777",
  "trivy_version": "v0.70.0"
}
```

Protected CI computes these values from the published cache image and the
AMD64 member of the published service image. It requires the extracted AMD64
and ARM64 service binaries to equal the lock's hashes, which were independently
derived from the pinned Trivy 0.70.0 release archives. It also rejects
disagreement between the AMD64 and ARM64 cache-image database files. The cache
identity is the SHA-256 of this framed canonical payload:

```text
loom-personal-dev-scanner-cache-v1\0
CANONICAL_SCANNER_FIELDS_WITHOUT_CACHE_IDENTITY_SHA256
```

The acceptance-plan schema keeps its existing public scanner fields for API
compatibility and adds the cache-identity digest plus both metadata hashes.
Plan validation requires its binary and cache hashes to equal the trusted
release, not merely to have valid SHA-256 syntax. The management Pod is pinned
to `kubernetes.io/arch=amd64`, matching the binary platform recorded by the
release. The scanner can still inspect both AMD64 and ARM64 candidate archives.

## Cache image

`deploy/Dockerfile.personal-dev-scanner-cache` uses
`python:3.11-slim@sha256:9c900dea9e8fb7e16277c179b555cc72d29a352dbc33cff48ad5a0412fd5bfc7`
as its multi-architecture base. The workflow supplies the verified materialized
directory as the named `personal-dev-scanner-cache` build context; ordinary
checkout files cannot impersonate that context. The image contains only:

- the cache installer module and the minimal `loom` package marker;
- `/opt/loom-personal-dev-scanner-cache/assets/db/trivy.db`;
- `/opt/loom-personal-dev-scanner-cache/assets/db/metadata.json`;
- `/opt/loom-personal-dev-scanner-cache/assets/java-db/trivy-java.db`; and
- `/opt/loom-personal-dev-scanner-cache/assets/java-db/metadata.json`.

The image runs as UID/GID `65531`, has no shell entrypoint contract, and invokes
`python -m loom.personal_dev_scanner_cache_init`. CI validates the named-context
file inventory and hashes both before and after each build, then scans and
attests both platform members through the existing image release path. The
trusted release contains only its immutable multi-platform digest.

## Installer contract

The new module `loom.personal_dev_scanner_cache_init` accepts:

```text
--source-root SOURCE_ROOT
--destination-root DESTINATION_ROOT
--cache-identity-sha256 SHA256
--database-sha256 SHA256
--database-metadata-sha256 SHA256
--java-database-sha256 SHA256
--java-database-metadata-sha256 SHA256
```

Each source file is opened with `O_NOFOLLOW|O_CLOEXEC`, must be a single-link
regular file, and is read through a descriptor with before/after identity
checks. Limits are 4 GiB for each database, 64 KiB for each metadata file, and
8 GiB for the total copied payload. Metadata must be bounded JSON with the
exact expected Trivy database schema and timestamp field types; its original
bytes remain protected by the release hash. Error messages identify only the
fixed logical field, never a caller-controlled path.

The destination layout is:

```text
/var/lib/loom-personal-dev-scanner/
  generations/
    CACHE_IDENTITY_SHA256/
      identity.json
      db/metadata.json
      db/trivy.db
      java-db/metadata.json
      java-db/trivy-java.db
      fanal/
```

For a missing generation the installer:

1. validates the destination root and `generations` directory without following
   links;
2. creates one private staging directory under `generations`;
3. copies and hashes each source into a new single-link regular file;
4. writes canonical `identity.json` containing all expected hashes;
5. makes protected directories `0555`, protected files `0444`, and `fanal`
   `0770` for the management service's runtime group;
6. fsyncs every file and directory; and
7. renames the complete staging directory to its digest name in one filesystem
   operation.

If the exact generation already exists, the installer revalidates it and exits
without rewriting it. A mismatched existing digest-named generation is a hard
failure; the installer never deletes or repairs it automatically. Stale
installer staging directories are bounded by name, owner, age, and count and
are removed only before a new publication.

An atomically replaced `active-generation` file records the last successfully
selected digest for retention only; the management service still uses its
rendered exact generation path. After selecting the new generation, the
installer retains that generation and the previously active valid generation,
then removes other installer-owned generations through descriptor-relative,
no-follow traversal. It refuses more than 16 entries, any unknown entry shape,
or a deletion set above 16 GiB. A crash before cleanup is repaired by the next
identical init. At steady state the PVC therefore contains at most two complete
generations, preserving one quick rollback while bounding disk use.

The management process runs as UID/GID `65532`. Protected cache files remain
owned by installer UID `65531` and are not writable by UID `65532`; only the
group-writable `fanal` directory is mutable. Service startup continues to hash
the Trivy executable and both database files before builder authority exists.
It additionally verifies both metadata hashes and the canonical identity file.

## Kubernetes rendering

Shadow and acceptance render the same cache init container in the management
Deployment. It uses the trusted release's
`personal_dev_scanner_cache` image, exact release hashes as literal arguments,
the existing scanner PVC mounted read-write, and these controls:

- `automountServiceAccountToken: false` inherited from the Pod;
- no environment variables, Secret, token, network, or API mount;
- read-only root filesystem and a bounded memory-backed `/tmp`;
- non-root UID/GID `65531`, all capabilities dropped, no privilege escalation,
  and RuntimeDefault seccomp;
- finite CPU/memory requests and limits; and
- the existing `Recreate` Deployment strategy, so no previous management
  process reads the PVC during a release transition.

The management container uses this exact cache path:

```text
/var/lib/loom-personal-dev-scanner/generations/CACHE_IDENTITY_SHA256
```

It keeps the PVC read-write only for `fanal`; the protected directories are
filesystem read-only by ownership and mode. The management Pod has an AMD64
node selector. No new ServiceAccount, RBAC verb, NetworkPolicy egress, Secret,
PVC, Job, or resource identity is added, so the rendered package remains 33
resources.

The scanner identity environment variable is release-bound in shadow as well
as acceptance. Shadow still leaves the finding-policy digest empty and the
builder disabled. Acceptance supplies the finding-policy digest and can enable
the builder only after its plan is proven equal to the release and runtime
status is ready.

## Status, rollout, and rollback

Shadow and acceptance status require:

- the exact cache init image and arguments in the expected render;
- the exact generation cache path and scanner identity;
- the management Deployment available after the init container succeeds; and
- all existing RuntimeClass, manager-ceiling, worker-absence, migration,
  Secret-key, and namespace interlocks.

The acceptance runbook no longer asks the operator for local scanner database
archives. It verifies the cache lock, trusted-release scanner record, and
rendered init container instead. Rollback reapplies the reviewed shadow from
the same release, so it selects the same generation and does not copy or delete
cache data.

Deployment ordering is:

1. merge the implementation through protected CI;
2. publish and verify one trusted release containing all four internal images;
3. render and byte-review the new inert shadow;
4. server-side diff and apply the new shadow;
5. require cache init completion, management readiness, zero manager ceiling,
   no dynamic namespace, and no personal worker; and
6. only then prepare a later zero-capacity acceptance plan.

The existing e398 shadow remains a valid rollback artifact for its own release,
but it cannot be used as the acceptance rollback after the release schema and
resource bytes change. Acceptance must bind the new release's exact shadow
manifest.

## Testing

Unit tests exercise the real filesystem installer with descriptor races,
links, special files, wrong hashes, malformed metadata, size bounds, partial
staging, idempotent replay, tampered existing generations, and atomic
publication. A subprocess test runs the module entry point with real files.

Configuration and trusted-release tests reject schema drift, mutable database
sources, image-set drift, platform mismatch, and any plan/release scanner
disagreement. Renderer tests assert the exact init container, node selector,
generation path, security context, resources, volume ownership, no token or
Secret, no network authority, unchanged resource count, and inert shadow
flags. CI package-boundary tests build and inspect the cache image contract and
prove the runbook contains no local archive or copy path.

Before merge, the focused personal-management, release-evidence, component
ownership, workflow, image-contract, secret-scan, Ruff, and mypy suites run.
The protected image workflow then builds, scans, attests, publishes, and reads
back both cache-image platforms before assembling trusted release version 2.

## Non-goals

- This work does not resolve the GHCR credential for personal candidate image
  publication.
- This work does not create the public DNS record.
- This work does not activate OLDLAB or GB10 capacity, submit a task, start a
  pool executor, or change `min_slots`.
- This work does not add automatic vulnerability database refresh. Updating
  the two pinned OCI manifests is a normal reviewed source change followed by a
  new protected trusted release.
