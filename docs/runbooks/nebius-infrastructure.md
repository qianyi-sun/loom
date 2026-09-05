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

The 2026-09-02 live development readback found a 200 non-GPU-vCPU quota;
refresh it rather than treating that historical value as current. Per the
2026-09-05 owner revision of #1538, acceptance ends at the maximum batch
concurrency supported by the current account quota and accepted task resource
profile. Neither 200 concurrent tasks nor the pending 512-vCPU / 16-VM request
is a closure prerequisite. Quota is an admission ceiling, not a reservation or
proof of regional stock.
Refresh price and quota immediately before apply and retain the provider request
IDs with the owning issue.

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

Do not print or commit the profile configuration. Automation uses the dedicated
`loom-development-eu-north1-terraform-automation` service account and an
authorized-key profile without `expires_at`, never a copied human token. This
identity is not attached to a VM. The current monolithic state manages tenant
IAM groups, so its reviewed bootstrap membership must be able to read and
update those groups; splitting IAM into a separate state is required before
that tenant-level permission can be narrowed.

The recurring capacity observer uses a Terraform-managed authorized public key
with no `expires_at`. Generate the key pair once in a protected directory, put
only the public PEM in `capacity_observer_public_key_pem`, and install the full
Nebius credential JSON through the runtime bootstrap below. The SDK exchanges
that key for short-lived access tokens and refreshes them automatically. Do not
substitute a copied human access token or a static registry token.

The cluster uses a pre-existing versioned state bucket and the separate
`loom-development-eu-north1-terraform-state` service account. Its active S3
access key has no `expires_at`; the AWS-like key ID and secret live only in the
`loom-nebius-terraform-state` macOS Keychain item. The checked-in wrapper reads
that item into the Terraform process environment and rejects ambient AWS
credentials. Never put access keys in `.tfbackend` files. Reuse the existing
development remote-state key so convergence cannot fork the live cluster into
a new state, and confirm `use_lockfile = true`.

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
reviewing the canonical topology contract. The committed active envelope is
`0..8` regular `16vcpu-64gb` execution nodes with 64 Pods per node. Its 128-vCPU
execution envelope supports 56
concurrent 2-vCPU tasks while preserving 2 vCPUs and 8 GiB per node for
platform overhead. This smaller node shape avoids depending on currently
unavailable 32- and 48-vCPU regional inventory and consumes at most eight of
the twelve historically available VM slots. The capacity policy also retains
the historical expansion proposal (`0..10` at `48vcpu-192gb`, 200 tasks,
512 vCPUs / 16 VMs). Those target fields are not an acceptance requirement and
must not trigger expansion automatically.

Before the final #1538 batch, retain a fresh quota snapshot and the derivation
of its maximum: subtract existing shared-account usage and required system
capacity, then account for VM, CPU, memory and disk limits and per-node
allocatable overhead for the unchanged task profile. Change only Loom's exact
execution envelope through reviewed configuration if the existing 56-task
ceiling is below that maximum. Do not stop or resize other users' resources to
recover headroom. Record physical stock separately; a smaller stock-limited
wave is useful evidence but cannot silently replace the quota-derived target.

```bash
export LOOM_NB_TARGET=development-eu-north1
export LOOM_NB_INPUT=/secure/path/$LOOM_NB_TARGET.tfvars.json
export LOOM_NB_BACKEND=/secure/path/$LOOM_NB_TARGET.s3.tfbackend
export LOOM_NB_PLAN=/secure/path/$LOOM_NB_TARGET.tfplan
scripts/ops/with_nebius_terraform_state_credentials.sh terraform -chdir=deploy/terraform/nebius/stack init -reconfigure -backend-config="$LOOM_NB_BACKEND"
terraform -chdir=deploy/terraform/nebius/stack validate
scripts/ops/with_nebius_terraform_state_credentials.sh terraform -chdir=deploy/terraform/nebius/stack plan -var-file="$LOOM_NB_INPUT" -out="$LOOM_NB_PLAN"
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
scripts/ops/with_nebius_terraform_state_credentials.sh terraform -chdir=deploy/terraform/nebius/stack apply "$LOOM_NB_PLAN"
scripts/ops/with_nebius_terraform_state_credentials.sh terraform -chdir=deploy/terraform/nebius/stack output -json > /secure/path/$LOOM_NB_TARGET.outputs.json
scripts/ops/with_nebius_terraform_state_credentials.sh terraform -chdir=deploy/terraform/nebius/stack plan -detailed-exitcode -var-file="$LOOM_NB_INPUT"
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

For a private control plane, use the Terraform-managed `deployment_access`
gateway. Its fixed public allocation, subnet, boot disk, SSH operator key, and
resource-scoped instance service account are part of the same remote state as
the cluster. The VM identity is obtained from instance metadata, so normal
deployment does not depend on a human browser session, copied access token, or
expiring authorized key. Nebius does not support an access permit scoped to a
Managed Kubernetes cluster, so the identity uses the minimum supported project
`editor` permit plus an explicit registry permit; it is not a task worker and
cannot receive Loom trials.

The gateway stays running so the private control plane remains recoverably
reachable. User execution still scales independently from zero to four and back
to zero. Do not substitute a date-named VM, temporary public endpoint, unrelated
user VM, or ad hoc tunnel. For an approved restricted public endpoint, use
`--external`. Always write a private mode-0600 kubeconfig outside the repository:

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

1. Apply only `development-eu-north1` with the committed execution bounds, the
   pinned `16vcpu-64gb` shape, and 64 Pods per node. The current-inventory
   profile is `0..8` and requires readback of at least 200 non-GPU vCPUs and 12
   VM slots, with sufficient remaining headroom after shared-account usage.
   Reconcile any later envelope to the fresh quota-derived target through its
   reviewed plan; do not wait for or activate the historical 200-task proposal.
2. Prove Terraform convergence and cloud-side Ready/readback.
3. Create only the development binding namespace with its canonical topology
   labels and install only its environment-local identities/policies.
4. Run a system-node connectivity pod and record node identity, DNS, HTTPS
   egress, logs, exit code, and deletion.
5. Through the normal user API, upload an ordinary supported TaskSet that has
   no hand-written `service_execution` field, create a `backend=nebius` Batch,
   and record its automatically frozen input manifest, runtime plan, target,
   images, Trial and Job identity.
6. From execution minimum zero, observe that Batch's Job move Pending to
   Running to Succeeded; verify model/verifier output and immutable artifact
   readback; then verify Job cleanup and execution-node scale-down to zero.
   A manually created Pod or manually patched Task binding is not evidence for
   this gate.
7. Establish the current-quota maximum as described above, then run bounded
   increasing true-overlap stages ending at that target. The checked
   `accepted_concurrency=56` policy currently produces stages 1, 20, 40, 56;
   passing these proves that envelope, not that 56 is the account maximum.
   At every stage prove the persisted concurrency seats, simultaneous running
   Jobs/Pods, node-backed capacity, successful
   results, artifact digests, released seats, no orphan Jobs, and return to zero
   execution nodes. Merely submitting a large queued batch does not pass. Stop,
   diagnose, and clean up before advancing after any failed stage.
8. Create the separately reviewed staging/production bindings without traffic,
   then attempt cross-environment namespace, bucket-prefix, and credential
   access; acceptance requires denial.
9. Only after development passes, repeat the same gates in staging. Production
   work and traffic require separate approval. A second cluster or region is
   outside baseline scope.

A generic Linux Pod proves Kubernetes scheduling and network plumbing only. It
does not prove the Loom scheduler, worker, model, verifier, or artifact path.
Use the managed cluster's default container runtime with the repository's
restricted, non-root Pod settings. A custom sandbox RuntimeClass is optional
defense in depth, not an admission requirement for this project.

## Persistent development runtime

Runtime YAML is generated from the checked execution topology and physical
binding rather than copied and edited per environment. Render into a new or
empty directory and retain `render-manifest.json` with release evidence:

```bash
uv run --no-sync python scripts/ops/render_nebius_runtime.py \
  --environment development \
  --image cr.eu-north1.nebius.cloud/REGISTRY/loom-execution-actuator@sha256:DIGEST \
  --capacity-policy deploy/k8s/nebius-development-capacity-policy.json \
  --output /secure/path/nebius-development-render
```

The renderer resolves the target and namespace from
`config/service-execution-topology.json`, resolves the shared provider IDs from
`config/nebius-runtime-physical-binding.json`, injects only a digest-pinned
image, rejects a cross-environment capacity policy, and writes a checksum
ledger. Staging and production use the same command with their environment and
their separately reviewed target-bound capacity policy. Rendering does not
deploy or enable traffic.

After the platform Deployment and its database/admin Secrets exist, attach the
development execution runtime once:

```bash
scripts/ops/apply_nebius_development_runtime.sh \
  --kubeconfig /secure/path/development-eu-north1.kubeconfig \
  --nebius-credentials /secure/path/capacity-observer-credentials.json \
  --service-execution-runtime-profile /secure/path/service-execution-runtime-profile.json \
  --execution-actuator-image cr.eu-north1.nebius.cloud/REGISTRY/loom-execution-actuator@sha256:DIGEST
```

From outside the Nebius VPC, run the same convergence through the managed
gateway. Pin the gateway host key once after Terraform creates or replaces the
VM, verify the fingerprint against the apply evidence, and retain that
`known_hosts` file with the operator SSH key:

```bash
scripts/ops/mirror_nebius_release_via_gateway.py \
  --gateway GATEWAY_IP \
  --ssh-key /secure/path/deployment-access-ed25519 \
  --known-hosts /secure/path/deployment-access-known-hosts \
  --candidate-sha MERGED_DEV_SHA \
  --target-registry cr.eu-north1.nebius.cloud/REGISTRY \
  --gateway-image ghcr.io/qianyi-sun/loom-llm-gateway@sha256:DIGEST \
  --control-plane-image ghcr.io/qianyi-sun/loom-control-plane@sha256:DIGEST \
  --service-image ghcr.io/qianyi-sun/loom-service@sha256:DIGEST \
  --execution-actuator-image ghcr.io/qianyi-sun/loom-execution-actuator@sha256:DIGEST \
  --execution-runtime-image ghcr.io/qianyi-sun/loom-execution-runtime@sha256:DIGEST \
  --output /secure/path/nebius-release-mirror.json

scripts/ops/collect_nebius_runtime_evidence_via_gateway.py \
  --gateway GATEWAY_IP \
  --ssh-key /secure/path/deployment-access-ed25519 \
  --known-hosts /secure/path/deployment-access-known-hosts \
  --service-image cr.eu-north1.nebius.cloud/REGISTRY/loom-service@sha256:DIGEST \
  --execution-runtime-image cr.eu-north1.nebius.cloud/REGISTRY/loom-execution-runtime@sha256:DIGEST \
  --output-dir /secure/path/runtime-admission-evidence

uv run --no-sync python scripts/ops/prepare_nebius_runtime_profile.py \
  --candidate-sha MERGED_DEV_SHA \
  --mirror-record /secure/path/nebius-release-mirror.json \
  --evidence-summary /secure/path/runtime-admission-evidence/summary.json \
  --service-release-record /secure/path/service-amd64.json \
  --execution-runtime-release-record /secure/path/execution-runtime-amd64.json \
  --signing-key /secure/path/image-admission-signing-key.pem \
  --signing-key-id nebius-development-YYYY-MM \
  --output-profile /secure/path/service-execution-runtime-profile.json \
  --output-keyring /secure/path/image-admission-keyring.json \
  --output-policy /secure/path/image-admission-policy.json

scripts/ops/apply_nebius_development_runtime_via_gateway.sh \
  --gateway "$(terraform -chdir=deploy/terraform/nebius/stack output -json deployment_access | jq -r .public_address | cut -d/ -f1)" \
  --ssh-key /secure/path/deployment-access-ed25519 \
  --known-hosts /secure/path/deployment-access-known-hosts \
  --cluster-id mk8scluster-REPLACE \
  --nebius-credentials /secure/path/capacity-observer-credentials.json \
  --image-admission-keyring /secure/path/image-admission-keyring.json \
  --gateway-image cr.eu-north1.nebius.cloud/REGISTRY/loom-llm-gateway@sha256:DIGEST \
  --control-plane-image cr.eu-north1.nebius.cloud/REGISTRY/loom-control-plane@sha256:DIGEST \
  --service-image cr.eu-north1.nebius.cloud/REGISTRY/loom-service@sha256:DIGEST \
  --execution-actuator-image cr.eu-north1.nebius.cloud/REGISTRY/loom-execution-actuator@sha256:DIGEST \
  --execution-runtime-image cr.eu-north1.nebius.cloud/REGISTRY/loom-execution-runtime@sha256:DIGEST \
  --service-execution-runtime-profile /secure/path/service-execution-runtime-profile.json
```

The mirror helper accepts only the five expected digest-pinned amd64 release
images. It downloads a checksum-pinned `crane` binary into a mode-0700 remote
temporary directory, obtains destination credentials from the gateway VM's
attached Nebius service account, copies each image under a release-specific
tag, verifies that the destination digest is unchanged, and writes an
owner-only local result. It needs no human Nebius session, registry password,
sudo access, or persistent credential-helper installation. Repeating it for
the same merged candidate is idempotent.

Every runtime apply also converges the repository-owned execution-capacity and
execution-admission policies. For the current provider quota, both the global
and `nebius-cpu` admission ceilings are 56 leases; this must match the 56-job
pending/create limits and the eight-node execution envelope rather than retain a
bootstrap single-trial ceiling.
It also explicitly enables the control-plane canonical materializer with its
checked polling, claim TTL, concurrency, and source-retention settings. A
restart reclaims expired materialization leases from PostgreSQL; no operator
must reconnect a completed Batch or copy artifacts manually.

The canonical materializer also needs the namespace-local control-plane to
MinIO TCP 9000 path: both control-plane egress and MinIO ingress must admit it.
The repository NetworkPolicy and CLI render include this connection. If compute
and output commit succeed but materialization retries with an S3 connection
timeout, compare a control-plane MinIO health probe with the service's probe and
check both policy directions. Reconcile the corrected repository policy, then
let the existing materialization outbox retry; do not rerun the model, rewrite
Trial state, or remove the retained source bundle to recover the transfer.

Uploaded TaskSets retain the source `task.toml` identity while registering the
Task under a team/TaskSet-qualified catalog ID. Canonical trajectory events and
ATIF use the Trial's referenced catalog Task ID; comparing it directly with the
embedded source ID is invalid. A historical `task_identity_drift` failure must
not be repaired by rewriting Task or Trial rows. Deploy the corrected
materializer and verify a fresh ordinary-user submission; keep the original
failed Trial and committed source evidence for diagnosis. This identity mapping
does not relax output manifest, producer, runtime-result, or digest validation.

The evidence collector uses checksum-pinned Trivy and `crane` binaries on the
gateway, authenticates to Nebius Registry with the VM service account, rejects
CRITICAL findings, and atomically retains owner-only CycloneDX SBOMs, complete
vulnerability reports, and the exact execution-runtime binary digest. The
profile helper binds those results to the mirrored images and protected amd64
release records, then signs the immutable profile with a persistent Ed25519
key. Add `--create-signing-key` only for the first bootstrap; later releases
reuse the same owner-only key. Admission metadata does not expire at runtime;
removing or replacing the public key is the explicit revocation mechanism. The
execution-runtime release must exactly match the profile candidate. An
unchanged service image may retain a protected `dev` release record only when
that release is a Git ancestor of the profile candidate, so path-scoped trusted
publication remains deployable without fabricating a new service release.

The helper accepts only digest-pinned platform images, transfers only the
reviewed runtime manifests plus the capacity observer credential into a
mode-0700 temporary directory, obtains an internal kubeconfig with the VM's
attached identity, rolls out Control Plane then Service, applies the idempotent
runtime, and deletes the remote and local staging directories. No human Nebius
token is copied to the gateway.

## Canonical destination and staging attachment

The owner-selected final canonical destination for #1765 is the existing live
staging Loom database and object store. The development helper above is not a
staging attachment or a data migration tool: its Control Plane, Service,
Gateway and database assumptions remain Nebius-local. Keep development
history intact and distinguish new staging work from any separately approved
historical-data migration.

The materializer accepts separate source and canonical ObjectStore instances,
but current application startup binds both to the same configured store.
Before calling external canonical persistence deployed, #1765 must add durable
independent spool configuration, retain canonical Gateway input reads, and
verify complete transfer and acknowledgement-gated GC across the two stores.
Keep Pod-facing output ingestion inside Nebius: its peer-IP/lease check must
not be bypassed by moving it behind an unrelated public proxy.

Staging activation requires persistent, scoped connectivity for the Nebius
actuator/Gateway to the single staging DB, Gateway to canonical inputs,
staging Control Plane to the source spool, and collector to Control Plane.
Do not substitute a temporary SSH tunnel or a copied personal login token.
Use the protected staging render/install/broker lane for changes to existing
staging services, and a separately scoped Nebius attachment for execution
components. Read the current broker status and coordinate the request owner
before any deployment; a failed backup must be resolved without skipping the
mandatory pre-mutation backup or reusing another initiator's request.

## Automated staged acceptance

After the exact protected candidate is deployed and the execution pool has
returned to zero nodes, build its one-task acceptance TaskSet directly from the
deployed runtime profile. This keeps task image and candidate identity coupled
and declares two required task artifacts plus trajectory, usage, and verifier
output:

```bash
uv run --no-sync python scripts/ops/build_nebius_acceptance_taskset.py \
  --runtime-profile /secure/path/service-execution-runtime-profile.json \
  --output /secure/path/nebius-acceptance-taskset

loom eval nebius-acceptance \
  --taskset-dir /secure/path/nebius-acceptance-taskset \
  --model MODEL_ID \
  --candidate-sha MERGED_DEV_SHA \
  --capacity-policy deploy/k8s/nebius-development-capacity-policy.json \
  --output /secure/path/nebius-acceptance-MERGED_DEV_SHA
```

The command uses the persisted `loom auth login` session and only normal user
APIs. Omitting `--provider` uses the already configured Gateway model and needs
only the user's `read:own,submit` scopes; it does not create a Provider
Connection or require `providers:manage`. To select an existing connection,
add `--provider MODEL_PROVIDER_CONNECTION` with a model available on that
connection. Neither mode copies provider credentials into the acceptance bundle.

It uploads and waits for the ordinary TaskSet, then runs bounded stages ending
at `accepted_concurrency` (currently `1,20,40,56`, deduplicated). There is no
fixed 200-task endpoint. Explicit repeated `--stage` values permit a smaller
canary, but cannot prove the policy ceiling unless that ceiling actually runs.
Stages above 100 use multiple legal sampling combinations. Monitor evidence
comes from `service_execution.targets` and batch-scoped observed lease states,
not `resources.pools` legacy workers or the top-level Trial `claimed` count.
Each stage must observe the requested number of simultaneously running
execution units backed by Ready nodes, all canonical successes, complete
bundle downloads, and a return to zero execution nodes and drained compute
demand before the next stage. The acceptance verifier intentionally holds each Pod for
three minutes so scale-from-zero timing cannot make a fast model response hide
the real-overlap measurement.

Every Trial's complete authenticated archive is retained under the evidence
directory. The runner verifies the response digest, safe archive inventory,
`bundle.json`, `checksums/SHA256SUMS`, every declared file size and SHA-256,
and the candidate frozen on the Batch. A timeout cancels the acceptance Batch
best-effort so failed validation does not intentionally leave paid work
running. The final `acceptance.json` and checksum sidecar contain sanitized
Batch, lifecycle, node, overlap, and bundle evidence. The canonical archive
places execution payloads below `files/`, including
`files/artifacts/answer.txt` and `files/trajectory/events.jsonl`; required-output
validation uses those archive paths without dropping the full inventory or
digest checks. `progress.json` retains completed stage and current Batch
evidence if later validation fails. No database, MinIO, kubectl, Nebius CLI,
or hand-connected transfer is part of the test path.

`accepted=true` in `acceptance.json` means the requested batch/download/compute
cleanup stages passed. `maximum_proven_concurrency` and
`policy_ceiling_reached` describe only those observed stages;
`quota_maximum_acceptance=not_evaluated` deliberately leaves the fresh
account-quota derivation to the owning #1538 preflight. A configured 56-task
ceiling is not independently a proof of the current account maximum.

Source retention is a separate phase. A canonical-ready Trial can retain its
source for the configured 86400-second recovery window while compute is zero
and canonical downloads work. Immediate acceptance records each source cleanup
state and retain-until timestamp, and reports `source_gc_complete=false` until
GC is actually observed. Do not shorten retention, reset retries or rerun the
model/verifier to make an immediate smoke appear fully garbage-collected.
After the recorded recovery window, perform a read-only continuation into a
new evidence directory:

```bash
loom eval nebius-acceptance \
  --resume-cleanup /secure/path/nebius-acceptance-MERGED_DEV_SHA \
  --output /secure/path/nebius-cleanup-MERGED_DEV_SHA
```

This continuation reads the saved acceptance identities and fresh ordinary
user API state; it never submits or cancels a Batch, changes retention or
deletes source objects. Exit status 2 means cleanup is still pending, not that
GC passed; status 0 requires observed completion and a healthy zero-compute
readback. Preserve the original bundle/evidence and each subsequent report.

Normal convergence preserves the existing `loom-nebius-model-provider`
Secret and hashes its current value into the Gateway rollout annotation
without exposing it. Pass `--model-provider-api-key-file` only for an explicit
provider-key bootstrap or rotation; that file must be owner-only and is removed
from the gateway staging directory after apply.

Likewise, omit `--image-admission-keyring` during ordinary convergence to reuse
the existing public key Secret. Pass it only for the first bootstrap or an
explicit signing-key rotation. Its content digest is stamped onto the Control
Plane pod template so an authorized rotation always rolls the verifier.

The runtime profile is a protected non-secret deployment input whose images
must exactly match the images being rolled out. The helper validates its schema,
pool/class identity and image-admission coverage, persists it as a Kubernetes
Secret before the first Deployment references it, and stamps its content
digest on the Service pod template. Changing the profile therefore causes an
ordinary rolling restart of the accepting API instead of leaving it on stale
configuration. The Control Plane reads the immutable profile snapshot from the
accepted Batch rather than from mutable deployment configuration.

The operation is idempotent. It applies the development-only Control Plane
patch that enables the `nebius-cpu` scheduler and loads its image-admission
keyring from the existing Secret, converges the repository-owned current-56
capacity policy through the authenticated admin API, reuses the existing
single-scope Loom collector token, applies the active actuator and one-minute
collector, and waits for both rollouts plus a scheduled collector Job. The
admin token never leaves the Control Plane Pod. The provider-neutral base
manifest stays disabled. The operation does not create a user Job or change the
node-group target count. Subsequent users only upload/submit through Loom; the
persisted scheduler, lease/outbox, actuator, and managed autoscaler complete the
connection automatically and return execution nodes to zero after demand
drains. The applied bound is currently `0..8`; the historical expansion target
is inactive and is not required for current-quota acceptance. A higher accepted
envelope requires a fresh quota/headroom readback and reviewed plan, not an
edit to an acceptance report.

The recurring capacity observation publishes desired, creating, ready, failed,
and deleting execution-node counts together with its source timestamp. The Loom
Monitor shows those values, current executable slots separately from configured
headroom, canonical transfer backlog/age/retries, last acknowledgement, and
source-cleanup debt. A stale observation blocks new admission rather than
reusing the last displayed node count as executable capacity.

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

- A cluster/node upgrade affects all three bindings. Disable new placement.
  The execution group deliberately uses `max_surge=0` and
  `max_unavailable=1` so execution-node replacement adds no surge demand above
  the accepted quota envelope; record active-workload retry/drain evidence
  before restoring each environment. The fixed system group retains its
  independent surge policy.
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
