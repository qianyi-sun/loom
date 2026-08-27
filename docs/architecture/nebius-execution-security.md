# Nebius hostile-workload security decision

Status: repository controls accepted for issue #1551; live target acceptance is
blocked until the sandbox RuntimeClass and packet-level tests in the runbook
pass on the exact Nebius cluster and node image. The target and actuator remain
disabled. Accountable owner: `@qianyi-sun`. The Loom platform owner owns token,
admission, audit, and kill-switch behavior; the Nebius infrastructure owner
owns cluster, CNI, node-image, RuntimeClass, and node-rebuild evidence.

## Decision and non-negotiable boundary

Task, agent, setup, verifier, and task-supplied sidecar code is hostile. It may
read and corrupt every file and process in its own Pod. It must not obtain a
Kubernetes API credential, cloud/provider credential, another attempt's data,
or a route outside the explicit execution egress set.

`linux-amd64-cpu-pod-v1` requires a reviewed sandboxed RuntimeClass. Pod
Security `restricted`, a namespace, NetworkPolicy, and a dedicated node group
are defense in depth; they do not replace the hostile-code isolation boundary.
A missing, unhealthy, shared-kernel, or unaccepted RuntimeClass rejects
placement. There is no ordinary-Pod fallback and no cross-environment fallback.
Cross-pool recovery is allowed only after Loom revokes the old attempt
authority, proves resource/seat cleanup, returns the Trial to queued state, and
persists a new routing generation for the next attempt. It is never an
in-place fallback for the same authoritative generation. One-attempt-per-node
would be a separate execution class with its own wipe and cost acceptance; it
is not implied by this decision.

Repository merge is not live acceptance. It does not create a Nebius project,
cluster, node group, CNI policy, RuntimeClass, trust key, credential, or route.

## Trust and authority boundaries

| Boundary | Authority | Enforced rule |
| --- | --- | --- |
| Trial and immutable runtime identity | Loom/Postgres | Routing generation binds the selected pool/adapter; lease generation binds team, trial attempt, candidate SHA, task revision, command identity, runtime contract, and image admission evidence. |
| Pod lifecycle | Namespace actuator | It may create/read/delete Jobs and read Pods in one namespace. It cannot read Secrets, exec, read logs, mutate policy, nodes, namespaces, or cluster resources. |
| Workload identity | Control Plane and Gateway | A service token is at most 600 seconds, has a unique audit ID, explicitly binds even a null provider route, and atomically carries lease, generation, role, candidate, task, command, and runtime digests. Gateway re-reads all current authority before upstream dispatch. |
| Cloud and upstream credentials | Nodes, registry, Gateway, object service | Nodes pull images. Pods receive no Kubernetes token, image-pull credential, Nebius key, registry key, or raw model-provider key. Model calls use only Gateway. |
| Network | CNI NetworkPolicy | Attempt Pods are selected by `app.kubernetes.io/component=execution-unit`, denied ingress and egress by default, then allowed only DNS and Gateway TCP/9100 through exact namespace and Pod selectors. Object storage is Gateway-only. |
| Image supply chain | Control Plane trust roots | Every unique task, runtime, and sidecar digest needs fresh Ed25519-signed Linux/x86_64 evidence naming SBOM, provenance, vulnerability-report, and policy digests. Unknown signers, missing/extra images, expiry, tamper, or high/critical/unknown severity reject reservation. |
| Result and cleanup | Loom generation fence | Gateway calls require the current credential generation. Output commit is bound to the observed Pod/resource generation and has a bounded post-revocation flush window. Job deletion waits for committed output or records explicit unavailability at the cleanup deadline; UID preconditions prevent deleting a reused object. |

The generic `loom-egress-proxy` is intentionally not reachable from attempt
Pods: it is a provider forward proxy intended for Gateway and direct access
would bypass Gateway authorization. A future execution proxy must authenticate
the same attempt/generation identity and receive separate threat review before
its endpoint is added.

The signed image bundle is part of the canonical runtime plan stored on the
lease, its digest, the create command, Job annotations, and the step token.
Only public verification keys are configured in the Control Plane. The empty
default keyring rejects every service-execution reservation. Private signing
keys remain in the trusted build system and never enter Loom or a workload.

## Threat register

| Threat | Severity before controls | Repository control | Residual disposition / owner |
| --- | --- | --- | --- |
| Container or shared-kernel escape | Critical | Sandbox RuntimeClass is mandatory; restricted Pod context; no host namespace/path/device/privilege. | **Traffic blocker.** Exact Nebius runtime has not yet passed hostile tests. Nebius infrastructure owner. |
| Cloud metadata, control-plane, or lateral movement | Critical | Default-deny ingress/egress; no IP blocks; only exact DNS and Gateway peers. | Packet behavior must be accepted on the target CNI. Nebius infrastructure owner. |
| Kubernetes or registry credential theft | Critical | Attempt service-account automount false; no Secret RBAC; no imagePullSecrets; node identity performs pulls. | Verify projected-token paths absent and node identity is image-pull-only. Nebius infrastructure owner. |
| Raw model-provider credential theft | Critical | Provider credentials stay behind Gateway; workload token contains no provider secret; Gateway rechecks provider binding. | No direct generic egress proxy route. Loom platform owner. |
| Stale attempt replay after cancel/retry | High | Lease/generation, role, immutable runtime identity, provider binding, 600-second TTL, unique JWT audit ID, live DB recheck. | Clock and revocation latency acceptance required. Loom platform owner. |
| Cross-team artifact or model access | Critical | Team is bound in lease/JWT and rechecked; object access remains trial-scoped; namespace network peer is service-level, not tenant authority. | Run two-team negative tests on every target. Loom platform owner. |
| Mutable, unsigned, vulnerable, or wrong-platform image | Critical | Digest pins plus signed SBOM/provenance/vulnerability/policy evidence; exact image-set and Linux/x86_64 checks; high/critical/unknown denied. | Configure production trust roots and exercise rotation/revocation. Loom platform owner. |
| Actuator compromise | High | Namespace-only Job/Pod RBAC, no Secret/log/exec/policy permissions, no Nebius credential. | Database credential scope/rotation and image acceptance remain deployment gates. Loom platform owner. |
| Sidecar privilege or hidden service | High | Sidecars are declared in the signed runtime plan, digest-pinned, resource-bounded, restricted, and share the same Pod/network policy. | Undeclared external dependencies reject conversion. Loom platform owner. |
| Resource exhaustion and log/artifact flooding | High | Exact CPU/memory/storage limits, active deadline, bounded volumes/logs/artifacts/events, no retry fallback. | Load and quota tests remain target acceptance. Joint owners. |
| Cleanup ambiguity or object-name reuse | High | Durable desired state, revocation-first transitions, cleanup debt, Job UID delete precondition, exact identity readback. | Node/disk wipe evidence remains required for a dedicated-node class. Nebius infrastructure owner. |
| Missing security evidence or observability | High | Fail-closed parser, durable lease/history/command/event evidence, token-mint admin audit event, bounded metrics. | Export and retention must be verified in the production logging account. Loom platform owner. |

No Critical or High risk is accepted by exception. A row with remaining live
evidence is a rollout blocker, not a waived risk.

## Kill switch and rollback

The primary kill switch is to mark the exact execution target disabled or
unhealthy so no new lease can bind. For active work, transition each lease to
cancel/timeout; that revokes its generation before the actuator deletes the
UID-matched Job. Scaling the actuator to zero stops reconciliation but does not
revoke active tokens and is therefore not a complete kill switch. Removing a
NetworkPolicy, changing RuntimeClass to `runc`, broadening a selector, or
changing a claimed Trial's pool or issuing a second adapter authority for the
same attempt is forbidden rollback behavior. A later cross-pool retry must use
the revocation, cleanup, queued-state, and new-route sequence above.

Trust-key rotation is additive: configure old and new public keys, admit and
run a new-key canary, stop issuing old-key evidence, wait for every old bundle
to expire, then remove the old public key. A compromised key is removed
immediately, its targets are disabled, active generations are revoked, and
affected image digests are quarantined before re-enablement.

## Repository credential and output path

The Gateway now owns a credential-free workload broker. It binds the direct
network peer to the current observed Pod IP, Pod UID, lease, resource
generation, and role. The Job, Secret, task environment, and frozen runtime
plan contain no step token. PID 1 removes broker identity from every child
environment, is non-dumpable on Linux, and keeps refreshed tokens only inside a
loopback proxy. That proxy accepts only the explicit model-call route set and
rejects broker, admin, health, metrics, and other Gateway paths before minting
a token. Provider keys remain exclusively in Gateway.

The same broker accepts a canonical bounded output inventory, streams parts
through the common Artifact commit protocol, verifies object size/hash readback
and the semantic `result.json` identity, then writes one immutable manifest and
commit marker. The termination summary names the durable session and digests.
An ordinary successful termination without matching durable evidence fails
closed. Cancellation revokes model-call authority immediately but leaves only a
five-minute output flush window; deletion then requires committed evidence or
an explicit `unavailable` ledger state.

These are repository controls, not live acceptance.

## Explicit remaining live gates

- prove a supported Kata, gVisor, or separately reviewed sandbox RuntimeClass
  on the exact Nebius Kubernetes/node-image version;
- run the hostile runtime, cross-team, metadata, lateral-network, DNS,
  Gateway, object-store, stale-token, cleanup, and performance matrix;
- configure and rotate real image-admission public keys and verify admission
  logs/retention;
- prove the broker peer identity, PID-1 `/proc` isolation, token refresh,
  semantic output commit, post-revocation flush, and cleanup-deadline behavior
  on the exact sandbox runtime/CNI/object service;
- prove trajectory, usage, diagnostics, and any declared extra artifacts use
  the same bounded committed inventory in a real canary.

Until all gates pass, `deploy/k8s/nebius-execution-actuator.yaml` remains at
zero replicas and the target stays non-executable.

Repository regression coverage lives in
`tests/unit/test_execution_image_admission.py`,
`tests/unit/test_execution_actuator_kubernetes.py`,
`tests/integration/test_service_execution_leases.py`, and
`tests/integration/test_execution_actuator_k3s.py`. The disposable k3s suite
exercises real NetworkPolicy packet behavior and the Job/runtime path, but its
`runc` test RuntimeClass is explicitly not hostile-runtime acceptance.
