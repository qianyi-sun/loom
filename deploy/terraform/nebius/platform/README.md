# Independent integration platform state

This root adds dedicated system, execution, CI and release node groups, four
native storage buckets with three scoped identities, and one fixed public IPv4
allocation to the existing Nebius cluster. Existing cluster/network/registry IDs
are inputs. This root does not import, mutate or own the previous execution
stack or its deployment gateway.

Copy `../integration-platform.tfvars.json.example` outside the checkout and fill
IDs from the existing foundation's reviewed outputs. Configure the existing S3
backend with a **different state key**, for example
`nebius/development/eu-north1/integration-platform.tfstate`. The backend key must
remain within the operator's permitted prefix. Never use the execution stack's
`terraform.tfstate` key. Keep credentials in the existing operator profile and
protected environment, not in tfvars or command arguments.

On the configured macOS operator host, run from the repository root. The existing
wrapper reads the state identity from Keychain into the Terraform child process;
it rejects ambient AWS credentials and never places them in the backend file.

```sh
scripts/ops/with_nebius_terraform_state_credentials.sh terraform -chdir=deploy/terraform/nebius/platform init -backend-config=/secure/platform.s3.tfbackend
scripts/ops/with_nebius_terraform_state_credentials.sh terraform -chdir=deploy/terraform/nebius/platform plan -var-file=/secure/platform.tfvars.json -out=/secure/platform.tfplan
terraform -chdir=deploy/terraform/nebius/platform show /secure/platform.tfplan
scripts/ops/with_nebius_terraform_state_credentials.sh terraform -chdir=deploy/terraform/nebius/platform apply /secure/platform.tfplan
scripts/ops/with_nebius_terraform_state_credentials.sh terraform -chdir=deploy/terraform/nebius/platform plan -var-file=/secure/platform.tfvars.json -detailed-exitcode
```

The final readback should report no changes. Save the plan digest, non-secret
output IDs and readback beside deployment evidence. Reinitialize from the same
versioned backend to recover local state; do not import resources from the
execution state or force-unlock an active operation.

One 4-vCPU/16-GiB system node remains running. Other node groups start at zero:
execution 0–2, CI 0–2 and release 0–1. ARC listeners and the platform must use the
dedicated system node selector/tolerations; only queued work scales other groups.
All nodes are private. Registry pull identity has no publication credential.
The existing shared cluster's costs are unchanged; review current compute,
disk, public IP and storage prices before applying the saved plan.

Bucket access-key resources use explicit secret delivery and output only resource
IDs. Retrieve each credential through its authorized operator path into the
namespace Secret, never Terraform output or rendered manifests. Canonical and
backup buckets are versioned; source objects use the existing acknowledged
retention/GC contract. There is no calendar deletion of completed user output.

After apply, follow `docs/runbooks/nebius-platform.md` and
`docs/ops/nebius-runners.md` from the repository root. DNS, certificates, scoped
Secrets, runner registration, signed publication, restore exercises and real
workload acceptance are separate operations; successful Terraform alone does
not establish their completion. Do not destroy versioned buckets or retire the
previous platform until migration/retirement acceptance authorizes it.
