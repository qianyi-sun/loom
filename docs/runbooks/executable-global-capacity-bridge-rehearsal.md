# Executable global capacity bridge rehearsal

This package is deliberately inert. It renders one global manager in `loom-dev`
and one validation-only executor configuration per physical pool: GB10 and
OLDLAB. A merge does not authorize an apply, activation, ceiling change,
systemd install, start, or enable operation.

Personal applications use `loom-dev-<owner>` namespaces. `loom-dev` is shared
infrastructure only; never create or accept `loom-dev-shared`.

## Render and inspect

Render each controller-local artifact into an owner-only evidence directory.
The checked-in profile has an immutable image and an executable ceiling of
zero; it must remain zero in rendered service environments.

```bash
install -d -m 0700 artifacts/capacity
uv run --no-sync loom admin capacity-control-plane render-executor \
  --file deploy/dev-fleet/capacity-pool-executor.toml.example \
  --pool gb10 --output config > artifacts/capacity/gb10-executor.json
uv run --no-sync loom admin capacity-control-plane render-executor \
  --file deploy/dev-fleet/capacity-pool-executor.toml.example \
  --pool oldlab --output service-environment > artifacts/capacity/oldlab-service.env
grep -Fx 'LOOM_CAPACITY_EXECUTOR_EXECUTABLE_CEILING=0' artifacts/capacity/oldlab-service.env
```

Check that each rendered controller artifact has a distinct executor identity,
state directory, journal, bearer credential, TLS identity, ownership key, and
controller binding. Do not copy credentials between pools or into a personal
namespace. Validate staged ownership and permissions offline: state directories
and private credentials are owner-only regular files.

The checked-in systemd unit is a `Type=oneshot` validation unit with
`--validate-only`, a zero-ceiling `ExecCondition`, and no `[Install]` section.
Review it with `systemd-analyze verify`; do not install, start, enable, or run
it as part of this rehearsal.

## Read-only evidence

After a separately authorized manager deployment, record `GET
/v2/status/executors` and `GET /v2/status/subjects/{subject_id}`. Capture
manager execution state and ceiling, executor lease/checkpoint, inventory,
quarantine, and blockers. The expected deployed ceiling is exactly `0`.

A status response can prove an exact active physical Slurm-job intent, but
scheduler evidence alone never proves a worker is available. A subject's
`active_capacity_intent_count` and `active_capacity_slots` describe bounded,
exact manager evidence. `worker_available` needs fresh protected personal guard
database worker-registration evidence for the exact subject, incarnation,
deployment generation, and intent ID, with no later drain or release.
The service observes this only through a dedicated per-environment observer
login. It has `CONNECT`, guard-schema `USAGE`, and `EXECUTE` on the bounded
observation function only; its password stays in the lifecycle credential-seed
Secret and is never an executor configuration or a runbook value.
Otherwise report `capacity_status=waiting`, `worker_available=false`, and
`worker-registration-pending`. Pod/application readiness is independent and is
never worker evidence.

## Fake-permit/restart rehearsal

Use only the test harness or a disconnected controller fixture. Exercise
restart/replay against the same controller-local journal, then submit a fake
permit and verify that zero ceiling prevents new capacity. Preserve the journal
and record its rejection/checkpoint; never replace it with an empty journal.
This runbook never mutates live Slurm, Kubernetes, or systemd.
