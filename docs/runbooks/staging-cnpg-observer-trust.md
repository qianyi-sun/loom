# Staging CNPG observer trust preparation

This controller-side tool prepares a dedicated read-only observer identity for
OLDLAB3–5. It does not contact those hosts, install their observer endpoints,
change PostgreSQL or Slurm, or activate capacity. `prepared-not-observed` means
only that local key and trust files are ready. Fresh successful observation
through the installed fixed transport remains required for application handoff.

## Inputs and source authority

Run on OLDLAB1 as root from a clean, root-owned checkout of the exact merged
`qianyi-sun/loom:dev` head after its required protected checks. The tool reuses
the rollout host installer's source and installation-record validation. The
installed runner source must be an ancestor of that head. Isolated Python
requires neither `PYTHONPATH` nor a developer environment.

Supply a root-owned, single-link, nonsymlink mode-`0600` inventory under
root-controlled parents. Its full bytes must match the independently reviewed
SHA-256. The format is sorted-key JSON without insignificant whitespace and
with one trailing newline:

- Top-level fields: integer `schema_version: 1` and `nodes`.
- Exactly three ordered node objects: `trt-eai-oldlab-3`, `trt-eai-oldlab-4`,
  `trt-eai-oldlab-5`.
- Each node has exactly `node`, `address`, `port`, and `host_key`.
- `address` is canonical private IPv4 or unscoped ULA IPv6; `port` is an integer
  from 1 through 65535. Endpoints must be distinct.
- `host_key` is independently verified canonical `ssh-ed25519 <base64>` without
  comments. Obtain it through an already trusted administrative channel, not
  an unauthenticated host-key scan.

This allowlist does not authorize OLDLAB2 access. Do not guess addresses or
keys. Endpoint provisioning must independently verify node and release identity.

## Prepare the controller

Substitute the reviewed merged checkout and inventory digest:

```bash
sudo -n /usr/bin/python3 -I -B \
  /root/loom-ops-<merged-sha>/scripts/ops/staging_cnpg_observer_controller.py \
  --inventory-file /root/loom-cnpg-observer-inventory.json \
  --inventory-sha256 <reviewed-inventory-sha256>
```

The tool generates one nonrotating Ed25519 key and publishes:

| Path | Ownership/mode | Purpose |
| --- | --- | --- |
| `/var/lib/loom-staging-rollout/cnpg-observer-ed25519` | `loom-rollout:loom-rollout 0600` | Dedicated client identity |
| `/etc/loom/staging-cnpg-observer.pub` | `root:root 0444` | Public key for endpoint provisioning |
| `/etc/loom/staging-cnpg-observer-ssh-config` | `root:root 0444` | Literal host/address/port mappings |
| `/etc/loom/staging-cnpg-observer-known-hosts` | `root:root 0444` | Host-key pins under node aliases |

Original private key material and recovery records remain under
`/var/lib/loom-cnpg-observer-controller` (`root:root 0700`). Never transfer its
private key to a worker. The service can read its client identity but cannot
modify root-owned mappings, pins, or the original key.

## Interruption, refusal, and endpoint handoff

Retry the same command with identical inventory bytes and digest after an
interruption. It resumes the original key, including lost publication replies;
it does not replace a missing or changed installed key. Completed identical
preparation is a no-op. Unknown outputs, changed inventory, unsafe file types
or permissions, and mismatched recovery records refuse without adoption or
silent repair. Retain the inventory and state for reconciliation. Host-key or
identity rotation is a separate coordinated operation, not state deletion.

After the protected release also includes the dedicated node endpoint installer,
provision each endpoint through the trusted administrative channel with this
public key and the exact release observer digest. Its forced command must expose
only the fixed read-only observer, never a shell or general root command. Verify
fresh runtime observation using the installed rollout candidate before relying
on this channel. Local preparation is not completed endpoint provisioning or
fleet activation evidence.
