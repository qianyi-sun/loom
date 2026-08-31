# Nebius execution-target infrastructure

This directory owns one Terraform state for the shared Loom Nebius execution
cluster. It creates networking, registry, evidence storage, IAM identities, one
Managed Kubernetes control plane, a fixed system node group, and an autoscaled
execution node group. Development, staging, and production remain distinct
logical target bindings inside that cluster.

It does **not** authorize cloud creation, quota changes, billing, credentials,
production deployment, Kubernetes workload bootstrap, or Loom traffic. A web
console login is not a Nebius CLI profile. Before every plan or apply, follow
the authorization and cost gate in
[`docs/runbooks/nebius-infrastructure.md`](../../../docs/runbooks/nebius-infrastructure.md).

## Layout

- `modules/execution-target`: reusable, version-pinned target resources.
- `stack`: the only root module operators plan and apply.
- `targets`: the sole shared-cluster example. Its development-named anchor is
  retained deliberately so the existing live state converges instead of
  creating replacement infrastructure.
- `backends`: the existing development remote-state anchor with a native lock
  file. The state bucket is an independently authorized prerequisite.

The cluster example is checked against the three environment bindings in
`config/service-execution-topology.json`. Each binding has a distinct namespace,
target ID, health identity, and evidence prefix, while all three bind the same
physical cluster scope, region, and failure domain. A second cluster, state, or
region requires a separately accepted requirement and owner decision.

## Version and offline validation

The contract pins Terraform `1.16.0` and `nebius/nebius` `0.6.46`. Provider
locks contain both `darwin_arm64` and `linux_amd64` checksums.

```bash
python3 scripts/check_nebius_iac.py
terraform fmt -check -recursive deploy/terraform/nebius
terraform -chdir=deploy/terraform/nebius/modules/execution-target init -backend=false
terraform -chdir=deploy/terraform/nebius/modules/execution-target validate
terraform -chdir=deploy/terraform/nebius/modules/execution-target test
terraform -chdir=deploy/terraform/nebius/stack init -backend=false
terraform -chdir=deploy/terraform/nebius/stack validate
```

These checks exercise repository structure and mocked Terraform plans only.
They do not prove Nebius credentials, quota, capacity, price, creation,
convergence, Kubernetes access, pod execution, autoscaling, isolation, disaster
recovery, or destruction.

Provider `0.6.46` exposes Managed Kubernetes audit logging but no native
monitoring, alert, dashboard, or budget resources. This stack enables audit
logs and exports stable target/cluster/node-group identities for #1552's
collector and accounting work. Account-level budget enforcement is separately
configurable; cluster health must never be treated as cost or alert evidence.

## Plan shape

After the runbook gates are satisfied, prepare the one shared state anchor:

```bash
cp deploy/terraform/nebius/targets/development-eu-north1.tfvars.json.example /secure/path/development-eu-north1.tfvars.json
cp deploy/terraform/nebius/backends/development-eu-north1.s3.tfbackend.example /secure/path/development-eu-north1.s3.tfbackend
terraform -chdir=deploy/terraform/nebius/stack init -reconfigure -backend-config=/secure/path/development-eu-north1.s3.tfbackend
terraform -chdir=deploy/terraform/nebius/stack plan -var-file=/secure/path/development-eu-north1.tfvars.json -out=/secure/path/development-eu-north1.tfplan
terraform -chdir=deploy/terraform/nebius/stack show -json /secure/path/development-eu-north1.tfplan > /secure/path/development-eu-north1.plan.json
```

Never commit populated variables, backend credentials, kubeconfigs, plan
files, state, tokens, or private keys. Apply only the reviewed saved plan and
only after the owner has approved its exact target, resources, maximum hourly
and monthly cost, cleanup deadline, and residual-cost list.
