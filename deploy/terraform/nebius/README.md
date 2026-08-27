# Nebius execution-target infrastructure

This directory owns one independently planned and applied Terraform state per
Loom Nebius execution target. It creates target-local networking, registry,
evidence storage, IAM identities, a Managed Kubernetes control plane, a fixed
system node group, and an autoscaled execution node group.

It does **not** authorize cloud creation, quota changes, billing, credentials,
production deployment, Kubernetes workload bootstrap, or Loom traffic. A web
console login is not a Nebius CLI profile. Before every plan or apply, follow
the authorization and cost gate in
[`docs/runbooks/nebius-infrastructure.md`](../../../docs/runbooks/nebius-infrastructure.md).

## Layout

- `modules/execution-target`: reusable, version-pinned target resources.
- `stack`: the only root module operators plan and apply.
- `targets`: reviewed examples for the four topology targets. Copy exactly one
  outside the repository or to an ignored `.tfvars.json` path and replace only
  placeholders.
- `backends`: partial S3 backend examples with distinct state keys and native
  lock files. The state bucket is an independently authorized prerequisite.

The target examples are checked against
`config/service-execution-topology.json`. Networks, service CIDRs, project IDs,
CLI profiles, evidence buckets, and state keys are target-specific. Combining
environments or regions in one state is unsupported.

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
logs and exports stable target/cluster/node-group identities for the separate
collector and budget work in #1552. Metrics/alerts and a provider billing
budget must therefore be live-read back before this infrastructure issue can be
accepted; the repository must not infer them from cluster health.

## Plan shape

After the runbook gates are satisfied, prepare one target at a time:

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
