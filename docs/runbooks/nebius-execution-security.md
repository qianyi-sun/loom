# Nebius execution security acceptance and incident runbook

Use this runbook only after separate authority exists for the exact Nebius
project, cluster, target namespace, traffic, credentials, and test images. The
repository-only implementation does not grant that authority.

## Preconditions and evidence record

Record the git candidate SHA, image digests, image-admission policy digest and
signer IDs, cluster ID, Kubernetes version, node-image ID, CNI/version,
RuntimeClass name/handler, node-group IDs, target ID, tester, timestamps, and
the resulting artifact root. Reject aliases such as `latest` or an unrecorded
cluster context.

Before any hostile test:

1. Confirm the target is disabled and the actuator Deployment has zero
   replicas.
2. Read the RuntimeClass and node runtime configuration. Reject `runc`, an
   absent handler, or a handler whose target-node installation cannot be
   proven.
3. Server-side dry-run the namespace, service accounts, RBAC, and
   NetworkPolicies. Confirm the attempt account receives `no` for Secret,
   ConfigMap, ServiceAccount, Pod exec/log, NetworkPolicy, Node, Namespace,
   CRD, and impersonation access.
4. Verify every Pod image digest has a fresh valid signed admission bundle and
   that the Control Plane has only public keys. Prove an unknown key, a changed
   SBOM digest, an expired statement, an extra image, and high/critical/unknown
   severity each fail reservation.
5. Read back the Trial routing generation, selected pool/adapter/target,
   routing reason and digest; confirm the lease, create command, and history
   carry the same identity. Confirm no active worker claim or lease from
   another environment or logical pool can be selected as fallback.

Save command output rather than only exit status. Redact tokens and credentials;
never place them in the evidence directory.

## Packet and credential matrix

Create one bounded attempt through the owning interface. Do not create a
freehand privileged debug Pod as a substitute. From hostile code in the
attempt, prove:

| Probe | Required result |
| --- | --- |
| Loopback and declared same-Pod sidecar | Allowed only on declared ports. |
| Cluster DNS UDP/TCP 53 | Allowed through the selected kube-dns Pods. |
| LLM Gateway TCP/9100 through the runtime loopback proxy | Allowed; call is attributed to the exact team/trial/step/JWT ID, while hostile code cannot read the raw token or broker identity. |
| Object service TCP/9000 | Denied; the trusted Gateway performs bounded upload and commit. |
| Another attempt Pod/Service, Control Plane, Postgres, xDS, generic egress proxy, Kubernetes API | Denied. |
| Public IPv4/IPv6, RFC1918, link-local, node IP, service CIDR except named peers, cloud metadata endpoints | Denied. |
| DNS-selected alternate endpoint or changed-label Pod | Denied. |
| Service-account token paths, image-pull credentials, Nebius metadata token, raw model-provider key | Absent. |

Repeat the matrix from two teams and both directions. A timeout alone is weak
evidence: retain CNI policy verdicts or packet captures from the trusted side
and prove the intended Gateway call and Gateway-owned object commit succeeded
during the same run.

## Workload identity and revocation matrix

Use only the service-execution broker path; do not mint or inject a workload
token through `/admin/step-tokens`. Confirm its audit record proves a maximum
600-second lifetime, a unique `jti`, explicit provider binding including null, and exact
team, trial, lease, generation, role, candidate, task revision, command, and
runtime-contract claims. Confirm the durable `service_execution.step_token.minted`
audit event has the same non-secret identifiers.

From hostile task code, prove `LOOM_EXECUTION_*` identity is absent, reading
PID 1 environment through `/proc` is denied, raw broker token issuance cannot
be completed, and the loopback proxy still refreshes and completes an
attributed model call. Attempt broker, admin, health, and metrics routes through
the proxy and prove they fail before a token is minted. Verify no token appears in Job YAML, Secrets, process
environment, runtime plan, stdout/stderr, termination message, or artifacts.

Then exercise each mutation independently. Every changed value must receive
403 before any upstream request: team, trial, lease, generation, role, step,
candidate, task revision, command identity, runtime contract, provider
connection, missing provider binding, and missing JWT ID. Cancel the lease and
repeat immediately; retry to a new generation and replay the old token; wait
past expiry and replay again. Retain Gateway logs proving zero upstream calls.

## Hostile RuntimeClass matrix

Run reviewed escape and containment tests against the actual handler:

- syscall/kernel escape corpus appropriate to the runtime;
- `/proc`, `/sys`, cgroup, device, mount, namespace, ptrace, capability, and
  host-process isolation;
- host filesystem/socket discovery and container-runtime/Kubernetes socket
  access;
- fork/process, memory, CPU, ephemeral-storage, inode, file-descriptor, and
  log/artifact exhaustion;
- init, main, verifier, and sidecar failure, timeout, OOM, eviction, node loss,
  actuator restart, ambiguous create, and UID-preconditioned cleanup;
- sandbox overhead, image pull/start latency, steady-state CPU/memory, and
  cleanup latency at the intended concurrency.

Any escape, host visibility, unbounded resource, ordinary-Pod fallback,
unrecorded cross-pool placement, duplicate adapter authority, or missing cleanup evidence is a hard failure. Disable
the target, revoke active generations, preserve evidence, and do not continue
to a performance or traffic canary.

Before the hostile matrix, use
`scripts/ops/nebius_runtime_marker_smoke.py` for the smaller operational check.
It requires an exact digest-pinned image and full candidate SHA, refuses to
overwrite an existing smoke namespace or RuntimeClass, and creates one
restricted, non-root Pod with `runtimeClassName: loom-sandbox`. The Pod performs
no network or escape probes. It checks only the in-sandbox gVisor marker and
expected UID/GID, records Pod/node/image/container identity, and deletes the
namespace and RuntimeClass even on failure.

Run the smoke only during an authorized window in which the pinned handler is
installed on a disposable execution node:

```bash
candidate_sha="$(git rev-parse HEAD)"
image="ghcr.io/qianyi-sun/loom-execution-actuator@sha256:<reviewed-digest>"
python scripts/ops/nebius_runtime_marker_smoke.py \
  --kubeconfig /path/to/owner-only-kubeconfig \
  --candidate-sha "$candidate_sha" \
  --image "$image" \
  --runtime-class \
    deploy/terraform/nebius/modules/execution-target/runtime/loom-sandbox-runtime-class.yaml \
  --evidence-dir /path/to/new-owner-only-evidence-directory
```

This marker smoke proves only that the pinned handler starts a Pod on the
profile-labelled execution node. It is not containment, network-policy, or
#1551 security acceptance; do not expand it into those test suites during a
routine runtime bring-up.

## Enablement sequence

After all negative and positive tests pass, attach the evidence digests to
issue #1551. Enable only the tested target, start one actuator replica, and run
one non-production canary. Read back the lease/generation, Job and Pod UIDs,
RuntimeClass, node, NetworkPolicies, Gateway call, object commits, runtime
result, cleanup event, and absence of remaining resources. Increase traffic in
separately approved steps; never infer capacity from quota or an online node.

## Incident response

For suspected escape, credential exposure, unauthorized network reachability,
bad image admission, cross-team access, or stale-token acceptance:

1. Disable the exact target to stop new reservations.
2. Revoke active lease generations through cancel/timeout; verify Gateway
   rejects their tokens.
3. Preserve the affected Job/Pod/node/CNI/audit evidence before deletion when
   safe. Do not exec into hostile Pods.
4. Delete Jobs with recorded UID preconditions and verify namespace resources
   are gone. Quarantine affected nodes; rebuild them rather than returning them
   to service after a suspected escape.
5. Rotate compromised image-admission keys, Gateway/step-JWT keys, object
   authorities, or provider credentials according to exposure. Raw provider
   credentials should never have been present in a Pod; if found, treat it as
   a Critical control failure.
6. Determine affected teams, trials, image digests, generations, JWT IDs,
   object prefixes, and upstream calls from durable audit/state records.
7. Re-enable only after the original matrix passes on the replacement runtime
   and node image, with a new reviewed evidence record.
