# Task image materialization

Hosted task-image builds run on Nebius through the execution actuator. The
control plane owns durable materialization identity and retry state; a
Kubernetes Job performs one build attempt. Local Docker image preparation is
part of the separate local-development path.

## Identity and admission

`loom.task_image_materialization` records content-addressed task inputs and
architecture-specific materializations. Task registration and submission ensure
required materializations exist. Frozen task configuration and bundle identity
prevent an update from changing an in-flight build.

`loom_execution_actuator.task_image_controller` reconciles native build Jobs
for an execution target. It uses the same capacity admission transaction lock
as native trials and records renewable waits when capacity is unavailable.
A waiting record is not a reservation or proof of executable capacity. See
[native capacity fairness](nebius-primary-platform.md#native-task-image-capacity-fairness).

The native renderer owns Job resources, source inputs, build configuration and
registry output identity. Supported architecture and workload requirements must
match the selected execution class; unsupported inputs must be rejected rather
than weakened to fit a build.

## Attempts, publication and cleanup

The materialization lease owns retries. Kubernetes Jobs do not independently
retry a build. Each durable attempt records resource observations and its Job
UID. Heartbeats and lease epochs fence state changes and completion so an old
process cannot publish readiness for a replacement attempt.

Readiness requires immutable image publication and the matching materialization
record. A configured tag or Kubernetes completion alone is insufficient. Native
execution admission consumes the resulting immutable task-image evidence.

Resource reservations remain charged until the controller confirms UID-fenced
cleanup. Cancellation, expiry and controller restarts must not authorize a new
attempt while an old resource may still exist. Publication evidence and retained
image references must remain readable for historical results and recovery.

## Compatibility and verification

Published migrations and historical materialization, publication and build-grant
records remain intact. The [shared-cluster retirement record](../historical/shared-cluster-retirement-2026-09.md)
explains removed providers and retained database lineage. Historical identifiers
do not make those providers eligible for new hosted execution.

Focused coverage includes `test_nebius_task_image_controller`,
`test_nebius_task_image_renderer`, native capacity integration and application
migration tests. The [service execution contract](nebius-service-execution.md)
and [platform contract](nebius-primary-platform.md) describe the surrounding
admission, artifact and recovery boundaries. Live workload acceptance remains
separate from repository checks.
