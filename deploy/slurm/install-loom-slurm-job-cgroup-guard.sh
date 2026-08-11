#!/usr/bin/env bash
# Install the Loom Slurm job cgroup guard on a non-exclusive worker node.
#
# The guard delegates the pids controller into each opted-in job cgroup and
# registers an allocation-capped loom-job-<id>.slice so Docker's systemd cgroup
# driver has a valid, contained parent. This installer copies the repo-owned
# guard + systemd unit into place, pins the node's exact Slurm NodeName, and
# enables the service. It is idempotent (safe to re-run) and validates the node
# name against Slurm so the guard cannot silently no-op on a mistyped name.
#
# Usage (as root, from a repo checkout):
#   sudo deploy/slurm/install-loom-slurm-job-cgroup-guard.sh <SLURM_NODENAME>
# where <SLURM_NODENAME> is the exact, case-sensitive value from
#   sinfo -N -h -o '%N'
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

GUARD_SRC="$REPO_ROOT/scripts/ops/slurm_job_cgroup_guard.py"
UNIT_SRC="$SCRIPT_DIR/loom-slurm-job-cgroup-guard.service"
GUARD_DST="/usr/libexec/loom-slurm-job-cgroup-guard"
ENV_DST="/etc/loom/slurm-job-cgroup-guard.env"
UNIT_DST="/etc/systemd/system/loom-slurm-job-cgroup-guard.service"
SERVICE="loom-slurm-job-cgroup-guard"

node="${1:-}"
if [ -z "$node" ]; then
  echo "usage: sudo $0 <SLURM_NODENAME>" >&2
  echo "  (exact, case-sensitive NodeName from 'sinfo -N -h -o %N')" >&2
  exit 2
fi
if [ "$(id -u)" -ne 0 ]; then
  echo "error: must run as root — writes /usr/libexec + /etc/systemd/system and drives systemd" >&2
  exit 1
fi

# Fail closed on a NodeName that Slurm does not recognise: a mistyped name would
# leave the guard running but matching no jobs (squeue --nodelist=<name> empty),
# so contained workers would fail their cgroup-parent check with no obvious cause.
if command -v sinfo >/dev/null 2>&1; then
  if ! sinfo -N -h -o '%N' | grep -qxF -- "$node"; then
    echo "error: '$node' is not a Slurm NodeName on this cluster" >&2
    echo "       known NodeNames:" >&2
    sinfo -N -h -o '  %N' >&2
    exit 2
  fi
else
  echo "warning: 'sinfo' not found — skipping NodeName validation" >&2
fi

for f in "$GUARD_SRC" "$UNIT_SRC"; do
  [ -r "$f" ] || { echo "error: missing repo artifact: $f" >&2; exit 1; }
done

install -m 0755 "$GUARD_SRC" "$GUARD_DST"
install -d -m 0755 /etc/loom
printf 'LOOM_GUARD_NODE=%s\n' "$node" > "$ENV_DST"
chmod 0644 "$ENV_DST"
install -m 0644 "$UNIT_SRC" "$UNIT_DST"

systemctl daemon-reload
systemctl enable --now "$SERVICE"
# Re-exec the guard against the freshly-installed script + node pin.
systemctl try-restart "$SERVICE"

echo "installed $SERVICE for node '$node'"
echo "  guard:  $GUARD_DST"
echo "  env:    $ENV_DST"
echo "  active: $(systemctl is-active "$SERVICE")"
