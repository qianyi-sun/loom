# Production-pressure worker control

The systemd timer in this directory is the durable runtime bridge between the
private production and staging Control Planes. It runs every 30 seconds from
the fixed, root-installed staging runner checkout. A missed boot interval is
reconciled by `Persistent=true`; loss of the production pressure source posts
a fail-closed synthetic shortfall to staging.

Install on the authorized runner host after the candidate is merged and the
fixed `/opt/loom-staging-runner` checkout/venv is refreshed:

```bash
sudo install -o root -g root -m 0644 \
  deploy/worker-capacity/loom-prod-pressure-worker-control.service \
  /etc/systemd/system/loom-prod-pressure-worker-control.service
sudo install -o root -g root -m 0644 \
  deploy/worker-capacity/loom-prod-pressure-worker-control.timer \
  /etc/systemd/system/loom-prod-pressure-worker-control.timer
sudo install -o root -g loom-rollout -m 0640 \
  deploy/worker-capacity/prod-pressure-worker-control.env.example \
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
