# Environment naming convention

Loom uses `development`, `staging`, and `production` as environment identities.
Hosted environments use Nebius. Local development must explicitly opt in to local
execution; an environment name alone does not enable a worker backend.

| Environment identity | Public route convention |
| --- | --- |
| `development` | `/dev` |
| `staging` | `/staging` |
| `production` | `/prod` |

Release evidence uses the full environment identity and records the exact public
route and API base. A route abbreviation is not a cluster identity or deployment
authorization.

Native Nebius deployment takes reviewed environment JSON. Its `namespace`,
`execution_namespace`, `target_id`, `cluster_scope_id`, region, public host,
database identity, buckets and secret references are explicit configuration.
Do not derive them from a route abbreviation or infer live values from the
integration example. Keep environment credentials and data stores separate.

Use `scripts/ops/render_nebius_platform.py` and the
[Nebius deployment procedure](../ops/nebius-deployment.md). The former shared-cluster
profile discovery, environment-state deployment and autoscaler timer conventions
are retired. Automated native staging/production rollout awaits reviewed inputs
and approval wiring; release promotion and production evidence controls remain.
