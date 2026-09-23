# Independent Nebius deployment

Nebius is the image publication path for both development and production.
`nebius-candidate` publishes the seven manifest-owned AMD64 images from an exact
`dev` commit. Production promotes those same immutable digest references through
`release-promotion-gate`, then the protected `dev` to `main` pull request and
`main-promotion-gate`. A `main` push does not rebuild or publish images to GHCR.
Use the candidate publication output as the source of image references; never
substitute a mutable branch tag during promotion.

Production still requires separately reviewed environment inputs, release-owner
and Production Environment approval. Run
`scripts/ops/verify_production_release_gate.sh` from the promoted `main` checkout
with the approved candidate, image selector and release-gate run before applying
manifests. Checked-in examples are not production deployment authorization.
`nebius-rollout` automates the independent integration environment only; it does
not turn a `main` merge into an automatic production rollout.

Before applying production, protect each of the seven approved image digests
with the release manifest's unused SemVer `prod_tag` in the same Nebius image
repository. Keep these tags for running production and retained rollback versions;
never move an existing release tag to another digest. Integration retention reads
integration workloads, not production workloads, so a candidate-only tag can
otherwise expire after production stops sharing integration's current version.
Its existing policy preserves images with non-candidate tags, including SemVer
release tags.

After the production evidence verifier succeeds, use the operator's configured
registry credentials for each approved `IMAGE_REF` (`repository@sha256:...`) and
`PROD_TAG` from the release evidence:

```sh
skopeo copy --preserve-digests \
  "docker://${IMAGE_REF}" "docker://${IMAGE_REF%@*}:${PROD_TAG}"
test "$(skopeo inspect --format '{{.Digest}}' \
  "docker://${IMAGE_REF%@*}:${PROD_TAG}")" = "${IMAGE_REF##*@}"
```

Do not apply production until all seven release tags read back the approved
digests. Render and deploy the original digest references, not the tags. Adding
these retention tags does not rebuild the images or create a second publisher.

`scripts/ops/deploy_nebius_platform.py` plans or applies the output of
`render_nebius_platform.py`. Its default is a read-only cluster preflight; cloud
mutation requires explicit `--apply`. This is the supported hosted deployment entrypoint. `loom cluster up` is
limited to disposable development targets; the shared-cluster rollout broker
and its CLI command are retired.

### Protected read-only installation inventory

Before qualifying a managed multi-person installation, dispatch the existing
protected Nebius workflow from `dev` with the explicit inspection operation:

```bash
gh workflow run nebius-rollout.yml --repo qianyi-sun/loom --ref dev -f operation=inspect
```

This uses the same protected `nebius-integration` environment, pinned SSH host and
cluster identity checks as rollout, but cannot select the rollout job. It does
not require automatic rollout to be enabled and performs no Kubernetes, database,
DNS or cloud mutation. It shares rollout concurrency so the two workflow modes
do not race each other.

Download `nebius-inspect-RUN_ID-ATTEMPT` for the sanitized
`management-preflight.json` artifact. It contains the configured candidate,
namespace identities, node allocatable resources, declared Pod requests including
init containers and overhead, services/ingress, PVC sizes and storage classes.
It excludes Secret values, Pod environment/commands, annotations, kubeconfig and
configuration payloads. Failed or incomplete inventory fails the command rather
than being treated as an empty cluster.

`observed` means inventory succeeded, not that personal environments are ready.
The configured candidate is read from the platform ConfigMap, which rollout can
update before migrations and workload replacement complete. It is not proof of
the running workload versions; use successful candidate-bound rollout evidence
and workload readback to qualify that separately.
No child capacity allowance is inferred from a naive request sum. Wildcard
DNS/TLS, provisioning IAM, management installation, live Nebius quota and pool
limits, and installed concurrent-owner acceptance still require qualification.
Keep this evidence outside the repository.

### Render management manifests

Prepare the protected `loom.nebius-management-deployment.v1` JSON described in
[the management architecture](../architecture/nebius-primary-platform.md#management-deployment-manifests)
outside the repository. Its nested `installation.provider_runtime` is mandatory
for this deployment. Set its Kubernetes endpoint to the foundation's exact API
origin, `ca_file` to `/var/run/loom-management-kubernetes/ca.crt`,
`credentials_file` to `/var/run/loom-management-kubernetes/credentials.json`, and
`cloud_credentials_file` to `/var/run/loom-management-cloud/credentials.json`.
These are explicit projected-file paths, not an ambient operator login.

```bash
uv run --no-sync python scripts/ops/render_nebius_management.py \
  --deployment /secure/management-deployment.json \
  --candidate /secure/publication/candidate.json \
  --runtime-profile /secure/publication/runtime-profile.json \
  --output /secure/management-render
```

The output directory must not already exist. It and its files are private; errors
do not echo input values. `rendered-not-installed` reports the manifest list,
candidate, revision and fixed platform-resource envelope. Rendering validates
source identity, registry/digest binding and configuration, not successful remote
CI/publication or installed readiness. Select a protected `dev` publication, never
a personal snapshot or the retired integration branch.

All referenced Secrets belong only to the management namespace:
`loom-platform-db` (admin/service credentials and database CA),
`loom-management-db-tls`, `loom-platform-auth` (secret-store master key),
`loom-admin-secret`, `loom-management-publications` (`token`, read-only GitHub),
`loom-management-kubernetes` (`ca.crt`, `credentials.json`),
`loom-management-cloud` (`credentials.json`), and `loom-platform-storage`
(`backup-access-key`, `backup-secret-key`). Identical names in another namespace
do not authorize copying that namespace's values. Preserve generated keys and
their recovery material; Kubernetes/cloud identities must be separately scoped.

Do not pass this render to the standalone platform deployer or apply it manually.
Management ownership/readback, initial Secret delivery, HTTPS ingress/TLS,
off-node backup and the protected management rollout must be qualified before
activation. No `nebius-rollout` management-install operation is introduced by the
render-only command, and it purchases no capacity or storage.

## Before the first application

Use the independently configured Terraform platform state and its cluster ID/API
endpoint to fill the environment configuration. Create the dedicated integration
nodes and the two namespaces, then provision referenced secrets into their owning
namespace. This secret bootstrap is separate: the renderer never writes secret
values. The deployer checks names of nonempty keys through a Kubernetes template;
it never requests or records the values. Provision public DNS and TLS certificates
for the configured hostname and a private PostgreSQL certificate/CA for the exact
service hostname. Configure access to Nebius Registry and the independent backup
bucket. A renderer success does not prove these dependencies exist.

Render the published image references, review the Kubernetes output, then deploy
from an operator checkout. First prepare the
[locked operator environment](operator-runbook.md#locked-operator-environment).
Use `--no-sync` for the deployment command so it cannot implicitly modify that
environment. The deployment machine does not need the image source commit
checked out:

```sh
uv run --no-sync python scripts/ops/deploy_nebius_platform.py \
  --render-dir /secure/nebius-render \
  --kubeconfig /secure/nebius-kubeconfig \
  --expected-cluster-id mk8scluster-EXACT_ID \
  --evidence-dir /secure/nebius-deployment-evidence
```

Repeat the same command with `--apply` after reviewing the target and preflight.
Serialize deployments of this environment through the caller's workflow concurrency
group; do not run independent operator deployments simultaneously. This script
does not introduce another lock broker or silently steal an active deployment.

## Validation and phases

Historical Nebius databases at ambiguous revisions `0133`–`0136` require the
[qualified lineage conversion](nebius-lineage-conversion.md) before their first
dev migration. Preserve the backup and writer-quiescence requirements in that
runbook; ordinary deployment does not infer or stamp the historical lineage.

The deployer reads the known phase YAML once, taking environment settings from
the application ConfigMap. It checks
that resources belong to the two integration namespaces (plus their dedicated
collector RBAC). Reviewed replica/resource tuning and operator notes beside the
rendered files do not require re-signing, rehashing or a fresh Git checkout.
There is no hash inventory, candidate signature wrapper or deterministic rerender
gate. The existing control plane verifies runtime image admission at its own
boundary; the deployment scripts only transport that configuration.
The kubeconfig server must equal the separately configured API endpoint, its
cluster name must identify the expected Nebius cluster, TLS verification must use
the cluster CA, and provisioned integration nodes must have Nebius provider IDs.
The current observed Nebius kubeconfig convention is a name ending in
`cluster-<id suffix>` and Node provider IDs use `nebius://computeinstance-...`.

For an existing database that needs a new migration or candidate/configuration,
the first mutation is a new Job from the existing backup CronJob. That Job must
finish successfully before configuration, PostgreSQL, migrations or services are
changed. The backup uploader verifies the stored object's byte count and checksum
metadata before reporting success. A missing backup CronJob blocks the upgrade.

For a fresh database, the order is namespaces/configuration, PostgreSQL and
readiness, backup CronJob installation, the candidate migration Job, services and
readiness, catalog/capacity configuration, execution actuator, public entry, then
HTTPS API/frontend smoke. Installing backup immediately after database readiness
allows a partially completed first installation to resume with backup protection.

Completed candidate Jobs are reused. Pending Jobs are waited for. Failed Jobs
require a diagnosed fix followed by explicit `--retry-failed-jobs`; only the failed
Jobs belonging to this rendered candidate can then be replaced. The deployer never
deletes namespaces, PVCs, healthy Jobs, or unrelated resources. A retained database
PVC without its StatefulSet blocks application and requires an explicit restore.

Every attempt leaves a separate sanitized JSON phase record, including the candidate version, target
and failed phase. Evidence excludes kubeconfig material,
API endpoint details and secret values. A failed attempt is not automatically
rolled back: restore and database-version compatibility must be reviewed against
the retained backup before a downgrade. Reapplying a known candidate is not proof
that a database restore or schema downgrade is safe.

HTTPS health and frontend routing smoke are deployment checks only. User login,
real workload execution, result integrity and fault-recovery acceptance remain the
separate pure-Nebius E2E milestone.

**Verifying the deployed version (#2009):** confirm the rendered candidate SHA
actually reached the cluster by comparing it against what the running app
reports, not just this deployment's own logs. Open the target URL, click the
version entry at the bottom of the sidebar, and check the frontend commit
matches the candidate; separately curl `<target>/api/v1/version` (or open it
in the details) for the backend's own commit, keeping in mind one response is
evidence for the responding instance only — for a multi-replica rollout, check
more than one before declaring the rollout complete. Record this comparison
(URL, expected candidate, observed frontend/backend commits) as deployment
evidence; a green deploy script run or merged PR is not by itself live
version acceptance.
