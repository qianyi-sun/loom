# Production-pressure worker control

The systemd timer in this directory is the durable runtime bridge between the
private production and staging Control Planes. It runs every 30 seconds from
one exact, root-installed staging runner candidate. A missed boot interval is
reconciled by `Persistent=true`; loss of the production pressure source posts
a fail-closed synthetic shortfall to staging.

Install on the authorized runner host only after the exact candidate has been
published under `/opt/loom-staging-runner/candidates/<full-sha>`. The checked-in
service is a template and must never be installed with an unresolved
`${GIT_SHA}` token or from an ambient checkout:

```bash
GIT_SHA=<approved-40-character-lowercase-candidate-sha>
if [ "${#GIT_SHA}" -ne 40 ]; then
  echo "GIT_SHA must contain exactly 40 characters" >&2
  exit 2
fi
case "$GIT_SHA" in
  (*[!0-9a-f]*) echo "GIT_SHA must be lowercase hexadecimal" >&2; exit 2 ;;
esac
CANDIDATE_ROOT="/opt/loom-staging-runner/candidates/$GIT_SHA"
sudo test -x "$CANDIDATE_ROOT/venv/bin/python"
sudo test -f \
  "$CANDIDATE_ROOT/repo/scripts/ops/prod_pressure_worker_control.py"

rendered_service="$(mktemp)"
trap 'rm -f "$rendered_service"' EXIT
sudo -u loom-rollout -- \
  "$CANDIDATE_ROOT/venv/bin/python" -I -B \
  "$CANDIDATE_ROOT/repo/scripts/ops/render_prod_pressure_worker_control_service.py" \
  --git-sha "$GIT_SHA" >"$rendered_service"
sudo install -o root -g root -m 0644 \
  "$rendered_service" \
  /etc/systemd/system/loom-prod-pressure-worker-control.service
sudo install -o root -g root -m 0644 \
  "$CANDIDATE_ROOT/repo/deploy/worker-capacity/loom-prod-pressure-worker-control.timer" \
  /etc/systemd/system/loom-prod-pressure-worker-control.timer
sudo install -o root -g loom-rollout -m 0640 \
  "$CANDIDATE_ROOT/repo/deploy/worker-capacity/prod-pressure-worker-control.env.example" \
  /etc/loom/prod-pressure-worker-control.env
```

Edit only the non-secret URLs, environment, pool, and bounded timing values in
the installed env file. The two token settings must remain `file:` references
to root-managed files readable by `loom-rollout`; never put raw bearer tokens
in the environment file, unit, journal, or evidence.

Enable and verify the continuous reconcile:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now loom-prod-pressure-worker-control.timer
sudo systemctl start loom-prod-pressure-worker-control.service
systemctl status loom-prod-pressure-worker-control.timer \
  loom-prod-pressure-worker-control.service
sudo -u loom-rollout jq \
  '{status, pressure, worker_control, fail_closed}' \
  /var/lib/loom-prod-pressure-worker-control/latest.json
```

The service must fail if the staging POST fails. A production GET failure is
different: the evidence records `fail_closed=true` and the staging POST must
succeed with a synthetic shortfall. Do not enable the timer until both CP URLs,
token files, and the staging GB10 desired-state row are present.

For an `actuator = "slurm"` target, this timer writes only the Control Plane
drain intent and claim fence. The candidate-bound external autoscaler on the
Slurm submit host remains the sole `scancel` authority. It immediately cancels
pending and zero-in-flight jobs, holds busy non-preemptible jobs until natural
completion, and cancels busy preemptible jobs only after grace. Capacity is not
released until Slurm reads the job back terminal. Successful cancellation
requests awaiting that read-back are durable and idempotent across timer or
submit-host restart; failed cancellations remain draining and appear in the
autoscaler decision reason as `cancel_failed`.

The checked-in unit is deliberately single-target. Multi-pool or per-sandbox
coverage must be added as reviewed, candidate-bound service instances (or a
repository change that renders repeated `--target ENVIRONMENT:POOL` arguments),
not by editing an installed unit or starting ambient watch loops. Separate
developer sandbox Control Planes require separate instances because they have
different URLs and token files.
