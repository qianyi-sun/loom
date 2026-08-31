# Nebius execution security baseline

Status: accepted baseline for issue #1551. The project does not treat benchmark
tasks as a high-assurance hostile multi-tenant sandbox and does not require a
custom kernel-isolation runtime, escape corpus, or adversarial packet matrix
before ordinary development execution.

## Decision

Nebius uses the managed Kubernetes default container runtime. A custom gVisor,
Kata, or dedicated-node runtime may be added later as optional defense in depth
for a workload that has a demonstrated need, but it is not part of the baseline
execution class and must not block normal service validation.

The baseline keeps inexpensive controls that are already normal Kubernetes
practice:

- execution nodes have no public IP and use a dedicated tainted node group;
- workload images are immutable digest references;
- attempt Pods run non-root with `RuntimeDefault` seccomp, dropped capabilities,
  no privilege escalation, no host namespaces, no hostPath, and explicit
  resource/deadline limits;
- service-account token automount is disabled for attempt Pods;
- the actuator has namespace-scoped Job/Pod permissions and receives no Nebius
  credential;
- model-provider credentials remain behind Loom Gateway;
- cleanup uses durable lease state and Kubernetes UID preconditions;
- the target can be disabled independently without changing OLDLAB or GB10.

These controls protect against common configuration mistakes and accidental
cross-workload access. They are not a claim that Loom contains intentionally
malicious kernel-escape code.

## Acceptance

The security slice is accepted when repository tests cover the restricted Pod
shape and scoped actuator permissions, and one real digest-pinned non-root Pod
runs on a Nebius execution node and is deleted afterward. The execution group
must then return to zero nodes. Network, Gateway, model, verifier, artifact, and
scheduler behavior are validated by their owning end-to-end canary rather than
duplicated as a separate adversarial security program.

The 2026-08-28 live smoke met the runtime portion: a digest-pinned non-root Pod
completed on the real Nebius execution group, and the temporary Pod, access
resources, and execution node were removed. The later gVisor work demonstrated
provider feasibility but is not required by this decision.

## Operations

Keep the actuator disabled until the normal deployment inputs exist: target
configuration, namespace, database secret, immutable actuator image, current
capacity observation, and a successful bounded end-to-end canary. This is an
operational readiness boundary, not a hostile-code gate.

For a suspected credential leak or cross-tenant data exposure, disable the
target, cancel affected leases, preserve relevant logs and object identities,
delete UID-matched Jobs, and rotate the credential that was actually exposed.
Do not require a full cluster rebuild unless evidence indicates node-level
compromise.
