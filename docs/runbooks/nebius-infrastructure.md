# Nebius infrastructure operations

This runbook provisions and accepts one shared Kubernetes execution cluster
with the three isolated environment bindings defined by
`config/service-execution-topology.json`. It is intentionally separate from
Kubernetes workload bootstrap: Terraform establishes the cloud foundation; a
later, independently reviewed step installs environment-local Loom resources
and exercises them.

## Authority boundary

Repository validation, a successful Terraform plan, an existing web-console
session, available quota, and an account balance are read-only evidence. None
authorizes resource creation or spend.

Before each mutable operation, record all of the following in the owning issue:

1. exact cluster scope, environment bindings, tenant, project, region, profile,
   and saved-plan hash;
2. resources created or changed and the maximum node count;
3. current Nebius hourly and 730-hour monthly estimate, including disks,
   public IPs, object storage, egress, taxes, and any items the calculator omits;
4. budget/alert status, cleanup deadline, destroy owner, and residual-cost list;
5. explicit owner approval for that exact operation and cost envelope.

The 2026-08-27 read-only development preflight found zero Kubernetes clusters,
200 non-GPU vCPUs of quota, and no configured billing budget. The console's
bounded two-node design estimate was about USD 0.14/hour or USD 102.20 per
730-hour month at its maximum. This historical observation is **not** a future
price quote or authorization. Refresh it immediately before apply. No live
resource was created during that preflight.

## Authentication and state prerequisites

Use one Nebius CLI profile and one existing project for the shared cluster. Browser login
alone does not configure the CLI. For a human-driven, auditable development
apply:

```bash
export LOOM_NB_PROFILE=loom-development-eu-north1
export LOOM_NB_PROJECT=project-REPLACE-DEVELOPMENT-EU-NORTH1
nebius profile create \
  --profile "$LOOM_NB_PROFILE" \
  --endpoint api.nebius.cloud \
  --federation-endpoint auth.nebius.com \
  --parent-id "$LOOM_NB_PROJECT"
nebius profile list
```

Do not print or commit the profile configuration. Automation must use a
dedicated service account with reviewed least privilege and an authorized-key
profile, never a copied human token.

The recurring capacity observer uses a Terraform-managed authorized public key
with no `expires_at`. Generate the key pair once in a protected directory, put
only the public PEM in `capacity_observer_public_key_pem`, and install the full
Nebius credential JSON through the runtime bootstrap below. The SDK exchanges
that key for short-lived access tokens and refreshes them automatically. Do not
substitute a copied human access token or a static registry token.

The cluster needs a pre-existing versioned state bucket with object access
limited to its operators and CI identity. Bootstrap that bucket in a separately
approved operation, enable access logging and retention appropriate to state,
and record its resource ID. Never put access keys in `.tfbackend` files; supply
short-lived or profile-backed S3 credentials through the approved secret
channel. Reuse the existing development remote-state key so convergence cannot
fork the live cluster into a new state, and confirm `use_lockfile = true`.

## Read-only preflight and saved plan

From the repository root, use exact pinned Terraform `1.16.0`:

```bash
python3 scripts/check_nebius_iac.py
terraform fmt -check -recursive deploy/terraform/nebius
terraform -chdir=deploy/terraform/nebius/modules/execution-target init -backend=false
terraform -chdir=deploy/terraform/nebius/modules/execution-target validate
terraform -chdir=deploy/terraform/nebius/modules/execution-target test
```

Copy the sole shared-cluster input and its existing backend anchor to a protected
directory. Replace the placeholder tenant, project, globally unique bucket
name, and state bucket; do not change topology fields without changing and
reviewing the canonical topology contract.

```bash
export LOOM_NB_TARGET=development-eu-north1
export LOOM_NB_INPUT=/secure/path/$LOOM_NB_TARGET.tfvars.json
export LOOM_NB_BACKEND=/secure/path/$LOOM_NB_TARGET.s3.tfbackend
export LOOM_NB_PLAN=/secure/path/$LOOM_NB_TARGET.tfplan
terraform -chdir=deploy/terraform/nebius/stack init -reconfigure -backend-config="$LOOM_NB_BACKEND"
terraform -chdir=deploy/terraform/nebius/stack validate
terraform -chdir=deploy/terraform/nebius/stack plan -var-file="$LOOM_NB_INPUT" -out="$LOOM_NB_PLAN"
terraform -chdir=deploy/terraform/nebius/stack show -json "$LOOM_NB_PLAN" > "$LOOM_NB_PLAN.json"
shasum -a 256 "$LOOM_NB_PLAN" "$LOOM_NB_PLAN.json"
```

Review that the plan touches only the shared cluster state, nodes have no public
IP fields, the API is private unless exact non-world operator CIDRs were
approved, both node groups forbid reservations, execution minimum remains zero,
all three environment bindings retain distinct namespaces/evidence prefixes,
and no unrelated project resource changes. Scan the JSON for secrets before
retaining it as evidence.

## Authorized apply and convergence

Apply only the reviewed plan hash:

```bash
terraform -chdir=deploy/terraform/nebius/stack apply "$LOOM_NB_PLAN"
terraform -chdir=deploy/terraform/nebius/stack output -json > /secure/path/$LOOM_NB_TARGET.outputs.json
terraform -chdir=deploy/terraform/nebius/stack plan -detailed-exitcode -var-file="$LOOM_NB_INPUT"
```

The second plan must exit `0`; exit `2` is drift and blocks acceptance. Redact
operator addresses and identifiers before publishing evidence. Confirm in the
Nebius API or console that the cluster and both node groups are Ready, the
execution group has the configured autoscaling bounds, node interfaces have no
assigned public IPs, the registry exists, the evidence bucket is versioned and
audited, and only the intended groups have access.

Nebius provider `0.6.46` has no Terraform resource for provider monitoring,
alerts, dashboards, or billing budgets. The target outputs are the durable
identity input to #1552's collector, alert, and accounting layer. Account-side
budget enforcement is separately configurable, while telemetry and cost
readback remain acceptance evidence; audit logs alone are not a substitute.

For a private control plane, run `kubectl` from an approved VM in the same
region and subnet. For an approved restricted public endpoint, use `--external`.
Always write a private mode-0600 kubeconfig outside the repository:

```bash
export LOOM_NB_CLUSTER=mk8scluster-REPLACE
export LOOM_NB_KUBECONFIG=/secure/path/$LOOM_NB_TARGET.kubeconfig
nebius mk8s cluster get-credentials --profile "$LOOM_NB_PROFILE" --id "$LOOM_NB_CLUSTER" --internal --kubeconfig "$LOOM_NB_KUBECONFIG"
chmod 600 "$LOOM_NB_KUBECONFIG"
kubectl --kubeconfig "$LOOM_NB_KUBECONFIG" cluster-info
kubectl --kubeconfig "$LOOM_NB_KUBECONFIG" get nodes -o wide
kubectl --kubeconfig "$LOOM_NB_KUBECONFIG" get pods -A
```

## Live smoke order

Run live acceptance in this order; stop and clean up at the first failed gate.

1. Apply only `development-eu-north1` with execution bounds `0..1`.
2. Prove Terraform convergence and cloud-side Ready/readback.
3. Create only the development binding namespace with its canonical topology
   labels and install only its environment-local identities/policies.
4. Run a system-node connectivity pod and record node identity, DNS, HTTPS
   egress, logs, exit code, and deletion.
5. Run one execution pod with the required toleration and node selector. Record
   Pending to Running to Succeeded, selected node, image digest, stdout, and
   artifact upload/readback. Delete the pod and verify no workload remains.
6. From execution minimum zero, submit one bounded pod, observe the execution
   group scale to one, complete it, delete it, and observe scale-down to zero.
7. Create the separately reviewed staging/production bindings without traffic,
   then attempt cross-environment namespace, bucket-prefix, and credential
   access; acceptance requires denial.
8. Only after development passes, repeat the same gates in staging. Production
   work and traffic require separate approval. A second cluster or region is
   outside baseline scope.

A generic Linux Pod proves Kubernetes scheduling and network plumbing only. It
does not prove the Loom scheduler, worker, model, verifier, or artifact path.
Use the managed cluster's default container runtime with the repository's
restricted, non-root Pod settings. A custom sandbox RuntimeClass is optional
defense in depth, not an admission requirement for this project.

## Persistent development runtime

After the platform Deployment and its database/admin Secrets exist, attach the
development execution runtime once:

```bash
scripts/ops/apply_nebius_development_runtime.sh \
  --kubeconfig /secure/path/development-eu-north1.kubeconfig \
  --nebius-credentials /secure/path/capacity-observer-credentials.json
```

The operation is idempotent. It reuses the existing single-scope Loom collector
token, applies the active actuator and one-minute collector, and waits for a
scheduled collector Job. It does not create a user Job or change the node-group
target count. Subsequent users only upload/submit through Loom; the persisted
scheduler, lease/outbox, actuator, and managed `0..1` autoscaler complete the
connection automatically.

Kubernetes rotates the actuator's projected ServiceAccount token. Nebius
refreshes access tokens from the non-expiring authorized key. Node-group image
pulls use the attached node service account instead of a registry login token.
The Loom batch-runner and capacity-collector identities are non-expiring and
single-scope. Human browser sessions and user API tokens may expire without
interrupting an already persisted Batch. A missing/revoked key or permission
makes the minute collector fail and capacity evidence become stale, which
fails closed before new reservations; it is an incident, not a periodic renewal
procedure.

## Workload placement contract

System components select `loom.nebius/node-role=system`. Attempt pods select
`loom.nebius/node-role=execution` and tolerate exactly:

```yaml
tolerations:
  - key: loom.nebius/execution
    operator: Equal
    value: "true"
    effect: NoSchedule
nodeSelector:
  loom.nebius/node-role: execution
```

Use digest-pinned images from the target registry. Do not grant attempt pods the
node registry service account or Terraform credentials. Add an identity to the
evidence-writers group only through a reviewed target-local bootstrap change.

## Upgrade, scaling, and incidents

- A cluster/node upgrade affects all three bindings. Disable new placement,
  preserve `max_surge=1` and `max_unavailable=0`, and record active-workload
  retry/drain evidence before restoring each environment.
- Change only the shared cluster variable file per plan. Quota exhaustion must leave
  work queued and observable; never increase limits or swap to reserved/GPU
  capacity silently.
- On a target incident, set routing health false, stop new placement, preserve
  existing state/evidence, and diagnose before scaling or rebuilding. A healthy
  cluster alone does not restore Loom traffic authority.
- Rotate human and service-account credentials independently. After rotation,
  prove the old credential fails, the new identity has only its expected target
  access, and no kubeconfig or key entered source control or logs.

## Recovery and destruction rehearsal

Remote-state/configuration recovery of the shared cluster remains required. A
second cluster, eu-west1 target, or regional failover is not a baseline recovery
mechanism and must not be created without a separately accepted RTO/RPO,
isolation, compliance, or capacity requirement.

Destruction is a distinct owner-approved operation. First export redacted
outputs, provider-side resource lists, evidence-object hashes, state version,
and the final zero-drift plan. Confirm artifact retention and that no live Loom
route references the target. Review `terraform plan -destroy`, disclose the
residual state bucket, evidence objects, logs, snapshots, public IPs, and egress
charges, then obtain explicit approval for the saved destroy plan.

After destroy, read back the Nebius project: cluster, node groups, shared VPC,
registry, IAM resources, and evidence bucket must match the approved retention
decision. A restore rehearsal starts from the versioned remote state and
reviewed target inputs, applies to the same isolated target, repeats the entire
live-smoke order, and ends with a zero-drift plan. Never delete the state bucket
until its retention and recovery window have expired under separate approval.
