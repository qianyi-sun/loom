# Task-image builder host-release v2 correction

Status: approved design; implementation and live rollout remain closed.

## Decision

Replace the unused, operationally impossible `host-release-v1` supply contract
with a snapshot-bound `host-release-v2` contract before any Phase 1 apply.  The
new release authenticates each Ubuntu package against the signed suite that
actually publishes that exact artifact:

- `quota` `4.06-1build6` comes from `noble`;
- `libsubid4` `1:4.13+dfsg1-4ubuntu3.2` comes from `noble-updates`; and
- `uidmap` `1:4.13+dfsg1-4ubuntu3.2` comes from `noble-updates`.

All Ubuntu metadata and packages are fetched from the official timestamped
Ubuntu Snapshot Service at `20260820T000000Z`.  Every suite index, package,
runtime artifact, and archive keyring remains digest-bound.  A new inert bundle
assembler downloads the exact release closure into a private temporary tree,
runs the same production verifier that host convergence uses, and atomically
publishes the verified directory without overwriting an existing path.

This correction does not install packages, create storage, change Slurm,
enable a service, certify a node, submit a builder job, activate Phase 2, or
rerun task `4139e767`.

## Failure evidence and root cause

The first real bundle rehearsal after PR #1492 deterministically failed with
`package is absent from the signed index`.  The checked-in v1 release exposes
only `noble-updates/main` for each architecture but requires the base-release
`quota` package.  The v1 unit fixture synthesized all three packages into one
index and therefore could not expose the mismatch.

Live signed metadata proves the split on both architectures.  The exact quota
digests in v1 occur in `noble/main`, while the exact subordinate-ID packages
occur in `noble-updates/main`.  The base `noble` `Packages.xz` also contains two
valid concatenated XZ streams.  The existing parser requires exactly one stream
and rejects the valid index as incomplete.  Ubuntu's signed metadata, package
rows, and package bytes are correct; the Loom release model and test fixture are
the defects.

The same preflight reconfirmed independent operational blockers: the dedicated
`/var/lib/loom-task-builder` ext4 `prjquota` mount is absent on reachable GB10
nodes, noninteractive administrative authority is absent, `trt-gb10-7` is
unreachable, and OLDLAB access is unavailable.  Those conditions are not
weakened or remediated by this release correction.

## Version and compatibility boundary

There is no live v1 state to migrate.  No Phase 1 node apply, Slurm apply,
receipt, or conformance envelope has been produced.  Therefore the active
candidate may replace v1 before first use, but it must not silently redefine a
v1 schema:

- add `deploy/task-image-builder/host-release-v2.json` with schema
  `loom.task-image-builder-host-release/v2` and release `host-release-v2`;
- remove the active `host-release-v1.json` file;
- add an explicit `host_release_manifest = "host-release-v2.json"` binding to
  `prerequisites-v1.toml` and make all consumers resolve that safe basename
  from the policy rather than hard-code a release filename;
- derive maintenance drain reasons from the loaded release name rather than a
  literal v1 string;
- produce a v2 prerequisite evidence envelope and v2 node/controller fragment
  schemas wherever the source shape or release constant changes; and
- retain receipt schema versions whose structure is unchanged, while binding
  every new receipt to the v2 release name and digest.  A v1-bound receipt is
  rejected by the ordinary release-name and digest checks.

The Phase 1 policy schema remains v1 because `host_release_manifest` is an
additive manifest-location field and the containment, cluster, resource, and
activation semantics are unchanged.  Older code ignores the new field but
cannot find the removed v1 release, so mixed candidates fail closed.

## Host-release v2 contract

The v2 release keeps the existing architecture map, archive keyring identity,
package artifact fields, and rootless runtime binding.  It changes the Ubuntu
source model as follows:

```json
{
  "schema": "loom.task-image-builder-host-release/v2",
  "release": "host-release-v2",
  "runtime_manifest": "rootless-runtime-v1.json",
  "ubuntu": {
    "os_id": "ubuntu",
    "version_id": "24.04",
    "snapshot": "20260820T000000Z",
    "component": "main",
    "signer_fingerprint": "F6ECB3762474EDA9D21B7022871920D1991BC93C",
    "keyring_name": "ubuntu-archive-keyring.gpg",
    "keyring_sha256": "80a36b0a6de2f69f49d2df75ef473ccde121e9e190b9ea01d20a4f63778d5c31"
  },
  "repositories": {
    "amd64": {
      "base_url": "https://snapshot.ubuntu.com/ubuntu/20260820T000000Z",
      "indexes": {
        "noble": {
          "inrelease_path": "dists/noble/InRelease",
          "inrelease_size": 255850,
          "inrelease_sha256": "cdb2f31d809f589719a53c6ad15f255b27569c4059542ada282aaa21b8e164b0",
          "packages_path": "dists/noble/main/binary-amd64/Packages.xz",
          "packages_size": 1401160,
          "packages_sha256": "2a6a199e1031a5c279cb346646d594993f35b1c03dd4a82aaa0323980dd92451"
        },
        "noble-updates": {
          "inrelease_path": "dists/noble-updates/InRelease",
          "inrelease_size": 126125,
          "inrelease_sha256": "79d2a1c90ce4f14c98867053190c64a9018ac993702fe5146081873f3da526bf",
          "packages_path": "dists/noble-updates/main/binary-amd64/Packages.xz",
          "packages_size": 1215608,
          "packages_sha256": "f43e6d13c95ac3db303163064a024de9718e61191b26f89584288d83842e8419"
        }
      }
    }
  },
  "packages": {
    "amd64": {
      "quota": {
        "source_suite": "noble",
        "package": "quota",
        "version": "4.06-1build6"
      },
      "libsubid4": {
        "source_suite": "noble-updates",
        "package": "libsubid4",
        "version": "1:4.13+dfsg1-4ubuntu3.2"
      }
    }
  }
}
```

The abbreviated example omits unchanged fields and the parallel `arm64` rows.
The checked-in manifest contains both complete architecture rows.  The arm64
index digests are `4a1901e6124fb0a111f5dffc8f5c14474f449e2ecfa71f2eaf0b29917edb53f9`
for `noble` and
`573cec116d4f4effc0b2cacbff28fd542182debd30d94ec40be996286690fba5`
for `noble-updates`; the signed InRelease files are shared across the two
architectures and retain the sizes and digests shown above.

The loader accepts exactly the suites `noble` and `noble-updates`, exactly the
official snapshot base URL derived from `ubuntu.snapshot`, and exactly one
`source_suite` per required package.  Each package source must name an index in
the same architecture row.  Mutable archive URLs, security/PPA suites,
credentials in URLs, duplicate paths, unknown fields, and unsafe relative paths
are rejected.

## Offline bundle layout and verification

Each architecture bundle contains exactly:

```text
ubuntu-archive-keyring.gpg
apt/noble.InRelease
apt/noble.Packages.xz
apt/noble-updates.InRelease
apt/noble-updates.Packages.xz
packages/<three exact deb filenames>
runtime/<four exact runtime artifact filenames>
```

Verification snapshots caller-owned inputs through no-follow descriptors into
an owner-private directory before parsing or consuming them.  It then:

1. verifies exact layout, regular-file type, ownership-independent writable-bit
   restrictions, and per-class size bounds;
2. checks every pinned metadata size and SHA-256 digest;
3. verifies each InRelease with `gpgv`, the pinned keyring digest, and the exact
   Ubuntu archive signing fingerprint;
4. verifies that the signed suite is exact and that the InRelease authenticates
   the architecture-specific `Packages.xz` digest and size;
5. decodes every valid XZ stream under one aggregate 64 MiB expansion bound,
   accepting only XZ-specified zero padding in four-byte units between streams;
6. rejects missing streams, truncated streams, malformed or nonzero trailing
   bytes, non-aligned padding, and expansion beyond the bound;
7. selects each required package only from its declared source suite and checks
   the exact package row, `.deb` bytes, control metadata, and setuid allowlist;
8. verifies the existing runtime archive, binary digest, and static-link
   contracts; and
9. returns only private snapshot paths to the host converger and erases that
   snapshot at the established lifecycle boundary.

The bundle digest covers every relative path and payload digest in sorted order,
so the added suite metadata changes the digest even when package/runtime bytes
are otherwise unchanged.

## Reproducible inert bundle assembler

Add `scripts/ops/task_image_builder_host_bundle.py` with one `assemble` command.
It accepts the v2 release, runtime manifest, exact archive keyring, native
architecture, and an absent output path.  It has no install, apply, SSH, Slurm,
service, or activation command.

The assembler:

- requires regular, non-writable authority inputs and an owner-controlled
  output parent;
- creates a mode-0700 temporary directory in the output parent;
- fetches only the exact HTTPS URLs from the v2 manifest, constraining redirects
  to HTTPS and applying metadata/artifact byte ceilings;
- writes files with owner-only permissions and fsyncs each completed file;
- invokes `verify_host_bundle` over the assembled tree;
- closes the verifier's private snapshot;
- fsyncs the temporary tree and output parent; and
- commits with Linux `renameat2(RENAME_NOREPLACE)` so an existing file, symlink,
  or directory is never overwritten.

Any fetch, size, digest, signature, metadata, verification, fsync, or commit
failure removes only the validated temporary tree and leaves the requested
output absent.  An already-existing output is a hard failure.  The assembler is
not added to the authority-component manifest because its bytes convey no
authority until the independently bound production verifier accepts them.

## Evidence and operator flow

The v2 evidence source records `snapshot`, the ordered suite set
`["noble", "noble-updates"]`, component, signer fingerprint, and keyring digest.
Each installed-package entry records its `source_suite` in addition to its
existing name, version, architecture, filename, size, and artifact digest.  The
v2 JSON schema fixes `release_name` to `host-release-v2` and preserves the
Phase 1 inert fields:

```json
{
  "production_certification_allowed": false,
  "certified_nodes": [],
  "blockers": ["phase2_guard_provider_release_missing"]
}
```

The runbook first stages a root/owner-controlled candidate, assembles and
verifies both bundles, and records their digests.  Cluster inventory/checks may
continue, but apply remains prohibited until the independent storage,
administrative-authority, node-reachability, and OLDLAB-access blockers are
resolved.  Neither CI nor merge contacts a cluster or mutates live state.

## Testing and acceptance

CI uses deterministic local fixtures and no network.  Required tests cover:

- the actual v1 topology, where quota is absent from the updates index, fails;
- exact v2 two-suite bundles pass on x86_64 and aarch64;
- suite/package swaps, missing suites, mutable URLs, digest/size changes,
  signature/signer changes, extra layout entries, and source-suite drift fail;
- valid concatenated XZ streams and aligned stream padding pass under the
  aggregate bound;
- arbitrary trailing bytes, non-aligned/nonzero padding, truncated streams,
  empty input, and excessive expansion fail;
- assembler success is verified before atomic publication, an existing output
  is never replaced, and every failure cleans only its temporary tree;
- policy, host convergence, maintenance, evidence, conformance, schema, and
  runbook consumers resolve and bind v2 with no active hard-coded v1 path or
  release reason; and
- the full Phase 1 focused suite, Ruff, strict mypy for authority sources,
  shell syntax, schema validation, diff checks, and protected repository/image/
  cluster/staging gates pass.

After merge, but before remote staging, run the assembler against the official
snapshot for both architectures and immediately rerun the production verifier.
The exact output bundle digests become operational inputs.  This networked
rehearsal is evidence for the operator change record, not a CI dependency.

## Rollback and stop conditions

Before any live apply, rollback is simply selection of an earlier repository
candidate; v1 remains unusable and must not be staged as a fallback.  If any v2
bundle cannot be assembled and verified exactly, Phase 1 remains blocked.

After future Phase 1 apply begins, the existing host and maintenance receipts
own rollback.  They bind the v2 release digest and reject a different candidate.
This correction does not grant authority to create the external storage mount,
repair node networking, obtain administrator credentials, or activate the
builder.  Missing any of those prerequisites remains a hard stop.
