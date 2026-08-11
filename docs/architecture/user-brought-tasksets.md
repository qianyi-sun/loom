# User-Brought TaskSets

A TaskSet is a private, team-owned collection of task bundles submitted to a
deployed Loom service. It is separate from the operator-managed native
benchmark catalog. TaskSet IDs have the stable form
`ts/<team-id>/<team-local-slug>`.

## Access and lifecycle

TaskSet routes require a current team. Submission, rebuild, and deletion also
require the `submit` scope and an identified user; an unowned legacy team token
cannot create user-facing TaskSets.

Only the owning team can list, inspect, select, rebuild, or delete a TaskSet.
Current visibility is always `private`. Deletion is soft deletion: the TaskSet
stops being visible and its materialized tasks stop being selectable while
object cleanup follows the lifecycle policy.

Statuses are:

- `materializing` — accepted and queued or running;
- `ready` — all selected tasks materialized;
- `partial` — usable tasks exist and some inputs failed;
- `failed` — no usable result was produced; and
- `deleted` — soft-deleted by its owner.

Task filtering rechecks TaskSet ownership even when callers supply exact task
IDs. A TaskSet must be `ready` or `partial` before its tasks can run.

## Manifest

Submissions contain `manifest.yaml` and, depending on the source, optional
verifier and bundle files. The current schema is strict
`loom.taskset/v1`/`UserTaskSet`:

```yaml
apiVersion: loom.taskset/v1
kind: UserTaskSet
metadata:
  name: my-coding-tasks
  display_name: My Coding Tasks
intents:
  - trajectory_generation
source:
  type: hf
  locator: namespace/dataset
  revision: 1.2.3
  split: test
instance_mapping:
  prompt: row.question
  answer: row.solution
  task_id: row.id
task_template:
  task:
    id: "{{ instance.task_id }}"
    name: "{{ metadata.display_name }} - {{ instance.task_id }}"
  environment:
    os: linux
    docker_image: ghcr.io/example/coding-task:1.0
  agent:
    name: default
limits:
  max_instances: 500
  timeout_per_task_s: 300
```

Supported source types are `hf`, `git`, `https`, `jsonl-inline`, and
`bundle-upload`. Row-oriented evaluation requires a manifest-level `pytest` or
`script` verifier. A bundle upload contains complete task directories and can
use each task's own verifier.

The intake and materializer reject absolute paths, traversal, symlinks,
hardlinks, device entries, oversized manifests/bundles, quota overflow, and
unexpected manifest fields. In the supported `internal_trusted` workload mode,
`transform` is unavailable; declaring one fails before source, verifier, or
transform blobs are fetched.

## Materialization

Submission stores the manifest and uploaded blobs under a team-scoped object
prefix, creates a `task_sets` row and current `task_set_manifests` row, and
queues a `task_set_materialization_jobs` record. The materializer claims jobs
with a lease, converts valid instances into canonical task bundles and `tasks`
rows, records bounded error summaries, and publishes the resulting status and
task count.

Rebuild creates a new materialization job for the stored manifest. Claim
fencing prevents concurrent active jobs for the same TaskSet and allows stale
claims to be recovered.

## CLI

Sign in first, then use:

```text
loom tasksets submit DIRECTORY [--format text|json]
loom tasksets list [--format table|json]
loom tasksets status ID_OR_SLUG [--format text|json]
loom tasksets rebuild ID_OR_SLUG [--format text|json]
loom tasksets delete ID_OR_SLUG [--format text|json]
```

The service routes are implemented in
[`src/loom_service/routes/tasksets.py`](../../src/loom_service/routes/tasksets.py).
Manifest validation and materialization live under `src/loom/taskset_*`, and
the persistence model is in [`src/loom/db/schema.py`](../../src/loom/db/schema.py).
