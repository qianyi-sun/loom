# Nebius execution security check

Use this check during a separately authorized Nebius development validation.
It confirms the project's normal Kubernetes baseline; it is not an adversarial
or high-assurance sandbox certification.

## Preconditions

Record the candidate SHA, digest-pinned image, cluster and node-group IDs,
namespace, tester, and timestamps. Keep credentials and kubeconfig contents out
of evidence.

Confirm before creating work:

1. the target is still disabled for general traffic;
2. execution nodes have no public IP and the group is bounded to `0..1`;
3. the namespace and actuator RBAC are environment-local;
4. the attempt Pod has no service-account token or cloud credential;
5. the image is referenced by digest and resource/deadline limits are present.

## Bounded validation

Run one ordinary non-root Pod first, then the owning Loom end-to-end canary.
The Pod must use the execution-node selector and toleration, the default managed
container runtime, `RuntimeDefault` seccomp, dropped capabilities, no privilege
escalation, no host namespaces or hostPath, and token automount disabled.

Record Pending/Running/Succeeded state, selected node, image/imageID, logs, and
Pod UID. Delete the Pod and verify it is absent. Return the execution group to
zero nodes and verify the provider readback. A custom RuntimeClass is not
required.

For the end-to-end canary, submit one minimal development task through Loom and
follow the same Trial through scheduling, Job/Pod creation, model call,
verifier result, artifact persistence, cleanup, and scale-down. That canary is
the meaningful readiness test; a standalone Pod is only infrastructure proof.

## Failure handling

Stop at the first failed stage. Preserve secret-safe diagnostics, delete only
the temporary resources owned by the run, and return the execution group to
zero. Fix the owning configuration or code path before retrying.

If a credential or cross-tenant data leak is observed, disable the target,
cancel affected leases, rotate the exposed credential, and retain the relevant
audit/object identifiers. Node quarantine or rebuild is warranted only when
there is evidence of node-level compromise.
