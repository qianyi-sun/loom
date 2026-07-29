# Developer sandbox live-overlap authority

This component is the persistent producer for issue #1023 overlap receipts. It
does not accept a path, command, host, URL, account, user, node, or allocation
from the operator. The installed exact-tree node authority exposes one fixed
`collect-live-overlap` transaction on `trt-eai-oldlab-2` and one fixed
`observe-live-overlap-job` check on each pool's source host. OLDLAB is observed
from `trt-eai-oldlab-2`; GB10 is observed from `trt-gb10-1` (`gx10-01c7`).
`trt-gb10-7` is absent from every allowed-node set and is rejected explicitly.

The producer consumes the already-installed adapter config
`/etc/loom/shared-capacity-adapters/<sandbox>-<pool>.toml`. Its complete content
must equal the closed mapping compiled into
`developer_sandbox_live_authority.py`. It then reads:

- canonical `[object]` adapter output at
  `/var/lib/loom-shared-capacity/observations/<sandbox>-<pool>.json`;
- canonical lifecycle state at
  `/srv/loom/developer-sandboxes/<sandbox>/sandbox-state.json`;
- the fixed sandbox Control Plane's autoscaler policy and active Slurm job
  registry, using the existing root-only admin secret file without persisting
  or printing the token;
- `systemctl show` for the one fixed sandbox unit, without reading its
  environment or command line;
- `scontrol show job` for the exact registry job ID on the fixed pool source
  host. GB10 reaches that host only through the installed forced-command node
  transport.

The CP policy must bind the adapter request UUID, lease epoch, candidate SHA,
active lease, non-exclusive Slurm actuator, account, and allowed-node set.
Exactly one CP job must be running for the sandbox/pool/candidate. The Slurm
readback must independently match its job ID, derived job name, account,
stable service user (`loom-sandbox-qianyi`, `loom-sandbox-hongjian`, or
`loom-sandbox-devansh`), node, CPU, memory, PID comment, GPU allocation, and
non-exclusive state. Personal login users are not accepted as Slurm batch
users.

## Collect one pair

Run from the clean exact candidate on `trt-eai-oldlab-2`. `AUTHORITY_TREE` is
the tree installed by the external-root node-authority bootstrap;
`CANDIDATE_TREE` is the sandbox workload tree recorded in `sandbox-state.json`.
The UUID makes the outer node-authority transaction idempotent.

```bash
python3 scripts/ops/developer_sandbox_live_authority.py collection-envelope \
  --sandbox qianyi \
  --pool gb10 \
  --candidate-sha "$CANDIDATE_SHA" \
  --candidate-tree "$CANDIDATE_TREE" \
  --authority-tree "$AUTHORITY_TREE" \
  --job-id "$SLURM_JOB_ID" \
  --collection-id "$COLLECTION_UUID" \
  | sudo /usr/local/libexec/loom-developer-sandbox-node-authority transact
```

Repeat for the six sandbox/pool pairs with a distinct collection UUID. Do not
invoke `collect` or `observe-slurm-job` directly; those are fixed installed
node-authority helpers.

The immutable producer result is:

```text
/var/lib/loom-developer-sandbox-live-authority/
  overlap/<pool>/<sandbox>/<candidate_sha>/<job_id>.json
```

Every directory is root-owned mode `0700`; every lock, high-water record,
transaction, and receipt is root-owned mode `0600`, single-link, canonical
JSON. A private flock serializes collection. The transaction persists the
sanitized receipt payload before publication, so recovery can complete a
`prepared` or `receipt-written` operation without re-querying live state.
Adapter sequence and time high-water marks reject regression and replay; an
immutable job-path conflict fails closed.

## Time contract

The four observations are deliberately not assigned one synthetic timestamp:

- `capacity_sample.observed_at` is the adapter's original time;
- `job_readback.observed_at` is the Slurm query time;
- `service_readback.observed_at` is the systemd query time;
- top-level `observed_at` is collection completion.

At collection, the adapter row must be no more than 120 seconds old and not
future-dated beyond five seconds. Slurm and systemd collection must complete
within 30 seconds, their times must not exceed completion, and the source files
are read again after live queries to detect replacement or mutation. The
acceptance importer independently checks the same ordering and bounded span.

Stop on missing or ambiguous CP jobs, inactive service, stale adapter output,
candidate/request/lease drift, personal-user identity, node or resource drift,
source replacement, transport failure, high-water regression, receipt
collision, or any unsafe ownership/mode/link metadata. Never substitute raw
SSH, a manually written JSON file, or copied command output.
