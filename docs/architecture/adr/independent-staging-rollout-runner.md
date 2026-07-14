# ADR: Independent Staging Rollout Runner

Status: accepted

Date: 2026-07-13

Tracking: [#803](https://github.com/qianyi-sun/loom/issues/803)

Temporary amendment (2026-07-14): [#822](https://github.com/qianyi-sun/loom/issues/822)
keeps the full 15-host SSH/trust inventory but excludes `trt-gb10-7` from the
active staging target. Until a separate merged re-admission change passes, the
acceptance boundary is all 14 active GB10 hosts and 140 slots, with node 7
persistently stopped/unreachable and no runtime override.

## Context

The rollout driver was restartable, but the supported staging procedure still
depended on Qianyi's checkout, user-level systemd manager, private GB10 key,
credentials, and terminal. Sharing those inputs with more users would not fix
stale refs, whole-rollout concurrency, backup ownership, or attribution. It
would also weaken the existing owner-only private-key check.

Hongjian, Devansh, and Qianyi need to update and test the same protected
staging environment independently. The owner policy permits testing only after
code has merged. No operator may select a pull-request ref, feature branch,
tag, historical commit, local checkout, image tag, alternate remote, or a
different physical target.

The three operators already have host-admin and Docker authority on
`platform-dev`. The runner therefore provides a narrow, attributable, and
repeatable operational interface; it is not a confidentiality boundary against
a deliberately malicious root-equivalent administrator.

## Decision

Install one root-managed runner on `platform-dev` with a non-login
`loom-rollout` service account and a `loom-staging-operators` group containing
`qianyi`, `hongjian`, and `devansh`. The root-owned client exposes only:

```text
loom-staging-rollout start [--dry-run]
loom-staging-rollout status [REQUEST_ID]
loom-staging-rollout logs REQUEST_ID [--follow]
loom-staging-rollout resume REQUEST_ID
loom-staging-rollout cancel REQUEST_ID --reason TEXT
```

The broker derives the caller from the authenticated sudo context and records
the initiating and attempt operators. It accepts no caller, repository, ref,
SHA, tag, path, cluster, secret, concurrency, skip, force-lock, or arbitrary
command override.

### Merged-only candidate binding

For every new request, the broker validates the fixed
`https://github.com/qianyi-sun/loom.git` origin, fresh-fetches exactly
`refs/heads/dev`, resolves its 40-character head SHA, and derives
`staging-<sha7>`. That immutable binding is written to a private request and
attempt envelope before the driver starts. A resume reuses the original SHA,
image tag, backup, config digest, and rollout ID even when `dev` has advanced.

Rollback of code is a merged revert on `dev` followed by a new request. Direct
deployment of an older SHA is not an operational rollback mechanism.

### Service authority and secrets

The root installation owns the runner source, venv, client, broker, sudoers,
policy config, and install ledger. The service account owns the candidate
checkout, request ledger, kubeconfig, generated worker env, runtime locks, and
a dedicated Ed25519 GB10 deploy identity. Under #822, that public key is
bootstrapped to the exact 14-host active set; the full 15-host checked-in
topology remains validated and is retained for legacy-ledger revocation. The
private key remains mode 0600 and is never shared with an operator.

The root venv is built only with a fixed root-owned `/usr/local/bin/uv` and the
safe resolved target of `/usr/bin/python3`, which must be Python 3.11 or newer
and remain under `/usr`. Because the repository intentionally treats
`pyproject.toml` rather than a tracked `uv.lock` as its cross-environment
dependency authority, the installer synchronizes the freshly selected merged
candidate without editable installs and without the inapplicable `--frozen`
flag. A source-SHA change forces another synchronization.

The service account receives named ACLs only for the declared staging token,
catalog, and data paths. Secret values stay in protected file sources and are
excluded from argv, request/status JSON, journals, rollout evidence, and
summaries. This does not make secrets inaccessible to the already
root-equivalent administrators; a stronger boundary would require a separate
runner host and credential rotation.

### Backup and locking

A non-dry `start` creates and verifies a new immutable backup manifest before
the rollout can mutate staging. It never relies on a mutable `latest` pointer.
A failed backup keeps the prior valid backup and prevents unit launch.

The broker launch mutex atomically arbitrates request admission. One separate
full-lifecycle lock covers the detached driver from pending through terminal
bookkeeping, so two image tags cannot interleave. Existing short protected
mutation leases remain in place for `cluster up` and environment-state steps;
reusing one of those leases for the parent driver would self-deadlock.

### Failure, recovery, and break-glass

An active request rejects another `start` or `resume` with safe request
metadata; it is never queued or preempted. Disconnects do not stop the
service-owned user unit. Operators inspect it through `status` and `logs`,
resume only the recorded request, and cancel only with a bounded recorded
reason. They do not edit `state.json`, envelopes, locks, or evidence.

Root break-glass first disables new admission and preserves the private request
ledger. Repair or reinstall the broker from a clean, freshly fetched merged
`dev` checkout, then resume the existing request through the same service-owned
envelope and pinned SHA. Do not fall back to Qianyi's user unit, private key,
an arbitrary driver argv, or a newly selected candidate.

Uninstall is fail-closed: remove admission, acquire maintenance under the same
launch lock, prove no request is active, revoke the exact service public key on
every host in the root-owned revocation ledger, remove only installer-recorded
ACLs/memberships/linger/key material, and retain request and rollout evidence.
A legacy ledger can contain all 15 inventory hosts; a #822-era install records
the 14 active hosts. A failed upgrade retains that durable ledger so uninstall
cannot leave stale remote trust.

## Alternatives considered

### Share Qianyi's key and personal runner

Rejected. It creates an unrevocable shared human identity, conflicts with the
private-key mode gate, preserves the stale-checkout and manual-backup problems,
and cannot provide whole-rollout attribution or serialization.

### Give every operator a complete runner and key

Rejected. Three independently drifting checkouts, venvs, kubeconfigs, backup
procedures, and 15-host key distributions enlarge the failure surface without
solving contention over one staging environment.

### Continue `sudo -u qianyi systemd-run --user`

Rejected as a supported path. It attributes work to the wrong user, depends on
Qianyi's checkout and linger, and permits stale or unbounded candidate inputs.

### Reuse the protected mutation lease for the whole driver

Rejected. Child rollout steps acquire that lease themselves. The parent needs
a separate lifecycle lock.

## Consequences

All three operators receive the same narrow staging command surface and can
start, observe, resume, or cancel work without another person's login session
or private key. A new rollout may take longer because backup creation is now a
required broker-owned phase. Only one full staging rollout can be active.

The service and audit boundary remains operational rather than adversarial
while the operators retain root/Docker access. Production authority and GitHub
Environment approval are unchanged. The backup, protected preflight,
environment-state, release-gate, and smoke gates apply to all 14 active GB10
hosts under #822; the full 15-host inventory remains validated and cannot be
changed through a runtime host override.

## Validation and acceptance

Repository tests cover fixed merged-only candidate selection, immutable
envelopes, caller attribution, lifecycle locking, backup-before-launch,
redaction, cancellation/resume, installer idempotence and recovery, exact GB10
trust bootstrap/revocation, secret-boundary scanning, advisory CODEOWNERS
routing, and full CI selection.

Repository verification is necessary but not live acceptance. Installation on
shared `platform-dev` is allowed only after the implementation has merged into
`dev`. Live acceptance must then prove both Hongjian and Devansh dry-run with
distinct authenticated identities and the same fresh merged SHA, unauthorized
and concurrent-start rejection, detached cross-operator observation, all 14
active GB10 hosts converged, node 7 stopped/unreachable, every existing release
gate and smoke passed, and zero
raw secret matches. #803 remains open for its broader identity inventory and
rotation scope after this operator-independence slice lands.
