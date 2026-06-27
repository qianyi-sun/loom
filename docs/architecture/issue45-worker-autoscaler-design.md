# Issue 45 Worker Autoscaler Design

Status: design
Date: 2026-06-27

## Goal

Complete #45 acceptance for shared OLDLAB and GB10 worker-pool autoscaling.
The system must reconcile desired capacity, scale up under queued demand, scale
down only after safe drain, keep minimum warm capacity, and expose decisions and
state through API, CLI, Monitor, Prometheus, docs, and public-beta evidence.

## Existing State

OLDLAB already has an elastic Slurm controller that submits worker jobs when
queued trials exceed active plus pending slots, cancels pending jobs after the
queue drains, records Slurm jobs, and publishes Slurm capacity metrics.
It does not actively drain and scale down running worker jobs.

GB10 has #43 node-agent lifecycle management. Control Plane desired state
drives image tag, pool, max concurrency, env config version, rollout policy,
and per-host reports. It does not yet express active, draining, or stopped
capacity intent.

Workers currently register status and capacity, and resource-pool summaries
show fresh active worker slots. There is no shared autoscaler policy object or
per-worker drain contract.

## Design

### Shared Autoscaler Policy Model

Add a Control Plane owned worker-pool autoscaler policy table keyed by
`environment`, `pool_name`, and `actuator`.

Fields:

- `enabled`
- `actuator`: `slurm` or `gb10`
- `min_slots`, `max_slots`
- `scale_up_threshold_slots`
- `scale_down_idle_seconds`
- `scale_up_cooldown_seconds`, `scale_down_cooldown_seconds`
- `drain_timeout_seconds`
- `force` and `disabled_reason`
- actuator config JSON for pool-specific settings
- last decision state: action, reason, desired slots, actual slots, pending
  slots, draining slots, blocked reason, error, and timestamp

The policy is the durable source of desired capacity. Existing Slurm registry
and GB10 desired state remain actuator-specific execution state.

### Worker Drain Contract

Add drain fields to `workers`:

- `drain_state`: `active`, `draining`, `drained`
- `drain_requested_at`
- `drain_reason`
- `drain_owner`

Worker claim must exclude workers whose `drain_state != active`. Heartbeat
continues while draining, and resource summaries expose draining workers and
draining slots. A worker with no assigned claimed/running trials after drain is
safe to release.

Normal autoscaler scale-down always requests drain first. Forced termination is
only allowed when policy `force=true` or an admin action explicitly requests it.

### Autoscaler Reconciliation Engine

Create a shared reconciliation module that:

1. Loads policy.
2. Reads resource-pool summary and in-flight trials.
3. Computes actual active, pending, draining, occupied, free, and idle age.
4. Computes desired slots:
   - never below `min_slots`;
   - scale up when eligible queued demand exceeds usable capacity by
     `scale_up_threshold_slots`;
   - scale down when capacity has been idle for
     `scale_down_idle_seconds`;
   - never above `max_slots`;
   - respects up/down cooldowns.
5. Emits a typed decision: `noop`, `scale_up`, `request_drain`,
   `release_drained`, `blocked`, or `error`.
6. Hands the decision to the selected actuator.
7. Persists the decision to policy status and Prometheus gauges/counters.

### OLDLAB Slurm Actuator

Extend the existing Slurm controller instead of replacing it.

Scale up:

- Submit jobs until desired slots are covered by running plus pending jobs.
- Preserve existing dedupe, pending cap, max jobs, stale replacement, and
  Slurm reconciliation safeguards.

Scale down:

- Select running Slurm worker jobs only when capacity exceeds desired slots.
- Mark linked workers draining.
- Wait for in-flight trials to finish.
- Once a draining worker reaches zero claimed/running trials, cancel the Slurm
  job or let idle-exit release it, then mark the job completed/cancelled
  according to Slurm observation.
- Do not cancel running jobs with active trials unless forced.

Pending jobs:

- Continue cancelling pending jobs when the queue drains and pending slots
  exceed desired slots.

Deployment boundary:

- When the Control Plane runs in Kubernetes but Slurm CLI and munge credentials
  exist only on the OLDLAB submit host, set
  `actuator_config.external_runner=true`.
- The in-pod autoscaler loop skips external-runner policies. A runner on the
  submit host executes the same reconciler with external policies enabled, so
  `sbatch`, `squeue`, `sacct`, and `scancel` run only where Slurm is available
  while API/status/metrics still come from the shared policy row.
- Slurm submissions are exclusive by default to preserve the full-node
  production capacity contract. A policy may set `actuator_config.exclusive=false`
  for deliberately shared nodes when the requested CPU, memory, and worker
  concurrency represent a tested partial-node slice.

### GB10 Node-Agent Actuator

Extend GB10 desired state with per-host capacity intent.

New desired state shape:

- `target_slots`
- `host_intents`: map hostname to `active`, `draining`, or `stopped`
- `desired_active_hosts`
- policy metadata in `rollout_policy`

Node-agent behavior:

- `active`: current #43 behavior; worker should be up and current.
- `draining`: update local env with `LOOM_WORKER_DRAIN=1` or call the drain API,
  stop claiming new trials, wait for no in-flight trials, then stop compose.
- `stopped`: keep worker compose stopped and report `apply_state=stopped`.

The Control Plane does not SSH into GB10. It changes desired state; each host
pulls and applies locally. Scale-up chooses stopped hosts first, then raises
concurrency within configured max only if the policy allows it. Scale-down
drains hosts before stop. Canary behavior from #43 remains available for
rollout-safe changes.

### API, CLI, Monitor, Metrics

Admin API:

- upsert/read policy endpoint:
  `/admin/worker-pool-autoscaler-policies/{environment}/{pool_name}`
- status endpoint:
  `/admin/worker-pool-autoscalers/status`
- Manual overrides use reviewed policy updates, GB10 desired-state updates, or
  the documented Slurm/Docker break-glass playbooks.

CLI:

- `loom admin worker-pools autoscaler status`
- Policy mutation remains on the admin API so operators can apply reviewed JSON
  policy documents through curl or automation.

Monitor/API resource summary:

- Add desired slots, pending slots, draining slots, idle age, last decision,
  blocked reason, and actuator error per pool.

Prometheus:

- desired/actual/pending/draining slots by pool
- idle-window age by pool
- scale-up and scale-down decisions by reason
- blocked/error status
- drain waiting state through autoscaler decision reason

## Public-Beta Validation

Validation must use real public-beta capacity.

OLDLAB:

- Configure min warm x86 capacity.
- Submit queued workload that exceeds warm capacity and verify Slurm scale-up.
- Let queue drain, verify workers enter draining and running jobs release only
  after in-flight trials finish.
- Validate stale heartbeat replacement and Slurm cancel/replacement paths.

GB10:

- Configure policy on top of #43 node-agent desired state.
- Scale down a subset of GB10 capacity through desired host intent; verify no
  active trials are interrupted.
- Scale back up through node-agent timers without per-host manual SSH/Docker
  commands.
- Validate interrupted node-agent or failed apply appears as blocked/error.

All submitted trials must reach terminal states or explicit operator-cancel
states. No duplicate workers, stuck queued/claimed/running trials, or silent
missing architecture capacity are acceptable.

## Testing Plan

- Unit tests for policy validation and desired-capacity math.
- Unit tests for worker drain eligibility and claim exclusion.
- Unit and integration tests for Slurm scale-up, pending cancellation, running
  drain, safe release, stale replacement, actuator failure, cooldowns, and
  min/max bounds.
- Unit and integration tests for GB10 host intent, node-agent plan/apply, stop
  after drain, interrupted agent report, and desired-vs-current status.
- CLI tests for status/set/drain JSON and text outputs.
- Monitor/resource summary tests for desired, pending, draining, and last
  decision fields.
- Metrics refresher tests for autoscaler gauges and decision counters.
- Public-beta smoke evidence for OLDLAB and GB10 full acceptance.

## Rollout And Safety

- Default policies are disabled.
- Enabling a policy requires explicit admin config.
- Scale-down requires idle window and cooldown.
- Draining prevents new claims before release.
- Forced termination is opt-in and visible.
- Manual Docker Compose and Slurm commands remain documented break-glass
  fallback paths.

## Acceptance Mapping

- Scale up/down OLDLAB: Slurm actuator desired-capacity reconciliation and
  public-beta evidence.
- Scale up/down GB10: host intent desired state plus node-agent apply and
  public-beta evidence.
- Min/max/cooldowns: policy model and decision tests.
- Drain before termination: worker drain contract, claim exclusion, safe
  release tests, public-beta evidence.
- API/CLI/Monitor/Prometheus: new endpoints, commands, resource fields, and
  metrics tests.
- Failure validation: stale heartbeat, failed actuator, interrupted node-agent,
  Slurm cancellation/replacement tests and live evidence.
- Docs: updated runbooks for policy config, disable/rollback, manual override,
  and validation.
