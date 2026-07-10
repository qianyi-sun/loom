# ADR: v1 Workload Trust Contract

Status: accepted

Date: 2026-07-09

Tracking: [#755](https://github.com/qianyi-sun/loom/issues/755). The durable
untrusted-workload implementation is explicitly deferred to
[#758](https://github.com/qianyi-sun/loom/issues/758).

## Context

Loom can retain legacy TaskSet transform compatibility fields and a dormant
subprocess helper, but those facts do not make user-authored transform code
safe to execute. In particular, a failed or best-effort `os.unshare` call does
not isolate filesystem, PID, UID, mount, output, or service-secret boundaries.
It is not untrusted-workload isolation and cannot authorize execution.

The first v1.0 release needs a reviewable contract that is shared by service
startup, TaskSet materialization, protected cluster preflight, the release
manifest, and the live release gate. It must fail closed before an uploaded
transform can obtain input or invoke a runner, and it must not turn deployment
logs into a source of raw configuration or secret data.

## Decision

Loom v1.0 permits exactly this workload-trust profile:

```toml
workload_trust_mode = "internal_trusted"
taskset_transforms_enabled = false
taskset_transform_network_isolated = false
untrusted_workload_isolation = false
```

TaskSet transforms are unavailable in that mode. A manifest that declares
`transform` fails with `transform_unavailable_in_internal_trusted` before any
transform blob, source blob, verifier blob, or subprocess is fetched or run.
Compatibility storage and the dormant helper remain non-authorizing: neither
legacy transform flags nor a best-effort `os.unshare` result may change that
outcome.

Staging and production are protected environments. Their profile tuple is
checked during protected preflight and checked again before `cluster up` can
lease or apply resources, including when an operator passes `--skip-preflight`.
The release manifest records the structural four-field contract, and the
release gate requires the manifest and the live `loom-service` environment to
converge on the same tuple.

The protected checks emit only secret-safe structural and expected evidence.
They must not emit raw invalid profile, manifest, or live env values. Evidence
may name a failed field or report an expected/observed structural mismatch, but
must continue to redact values that could contain a secret.

## Alternatives considered

### Enable the partial transform subprocess sandbox

Rejected. The legacy helper and best-effort `os.unshare` do not provide the
required arbitrary-code isolation boundary. Treating a missing or degraded
boundary as permission to execute would make an implementation detail into a
security claim.

### Accept a transform when its network-isolated legacy flag is true

Rejected. Network isolation alone cannot contain filesystem, process,
credential, output, or service-boundary access. The accepted v1 tuple therefore
keeps every transform and untrusted-isolation capability flag false.

### Enforce only at runtime

Rejected. A runtime-only check leaves configuration review, protected
preflight, release manifests, and live deployment convergence unable to prove
the same release contract. The value must be carried through each release
boundary.

## Consequences

User-brought TaskSets may continue to use declarative sources and trial-time
verifiers, but v1 does not offer arbitrary user transform execution. Existing
manifest compatibility fields can remain stored, provided they never authorize
fetching or running transform code.

This is a release-boundary decision, not a claim that OCI, gVisor, Kata, user
namespaces, seccomp, or any other arbitrary-code isolation substrate ships in
v1. Those capabilities and their adversarial validation belong to post-v1
#758. They must not be implied by v1 documentation, manifests, or staging
evidence.

The contract adds config, preflight, release-manifest, and release-gate checks.
It also makes a candidate with a malformed or divergent protected tuple
ineligible for promotion instead of silently falling back to legacy behavior.

## Validation and ownership

[`src/loom/workload_trust.py`](../../../src/loom/workload_trust.py) defines
`WorkloadTrustContract`, the canonical rule source. The protected profile,
service settings, TaskSet materializer, release manifest, and live deployment
check consume the same four-field tuple. Regression coverage proves accepted
and rejected tuples, transform no-execution before fetch/run, protected
preflight and `--skip-preflight` enforcement, manifest serialization, and live
`loom-service` environment convergence.

#755 owns the v1 fail-closed contract and its candidate-bound release evidence.
#758 owns any later untrusted arbitrary-code execution design and implementation.
Repository tests establish the contract but do not replace the final protected
staging candidate evidence required by the v1 release matrix.
