# Inactive shared-development fleet assets

The files in this directory are retained as compatibility inputs for the
disabled global development-fleet implementation. They are not part of the
current `loom dev` lifecycle and do not authorize capacity or cluster
mutation.

`shared-fixture.yaml`, `dev-fleet-autoscaler.env.example`, and the global
autoscaler service/timer are installation templates for the disabled
registry-driven development supervisor. Do not apply or install them merely
because they are present in the repository. The checked-in development
environment-state profile keeps its pool policies and external supervisors
disabled, and any running global-development timer sourced from this directory
is configuration drift.

The personal-development lifecycle uses a separate candidate builder,
activation agent, capacity-agent installation, and global-manager projection
checkpoint. A ready lifecycle operation includes an initial non-executable
capacity publication, but it does not activate the fixture or global autoscaler
templates in this directory and does not grant physical worker capacity.

`personal-dev-activation-agent.yaml.example` is the operator template for the
independently keyed stable-route activation agent. Replace its image and Secret
placeholders with reviewed immutable values before an authorized apply. The
template does not enable the service-side controller, restricted builder, or
physical worker capacity.

The implemented interfaces and disabled authority boundaries are documented in
[`Personal development environments`](../../docs/architecture/multi-dev-environments.md),
[`Global fleet capacity manager`](../../docs/architecture/global-fleet-capacity-manager.md),
and
[`Global development-fleet autoscaler`](../../docs/architecture/global-dev-fleet-autoscaler.md).
