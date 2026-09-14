# Independent Nebius deployment

`scripts/ops/deploy_nebius_platform.py` plans or applies the output of
`render_nebius_platform.py`. Its default is a read-only cluster preflight; cloud
mutation requires explicit `--apply`. This entry point does not use the legacy
rollout broker, OLDLAB, GB10, `dev` deployment workflow, or a shared database.

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
from an operator checkout. The deployment machine does not need the image source
commit checked out:

```sh
uv run --frozen python scripts/ops/deploy_nebius_platform.py \
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
