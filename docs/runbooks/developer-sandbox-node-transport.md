# Developer sandbox node-authority transport

The developer-sandbox installer uses a dedicated, root-owned SSH transport to
reach the fixed node authority. This is not a general remote shell and is not
an alternative source of cluster or deployment authority.

## Security contract

The checked-in route authority is
`deploy/developer-sandboxes/node-authority-transport.toml`. It contains only
the closed node names, canonical hostnames, IP addresses, ports, initiators,
verbs, and jump relationships. It contains no public key, private key,
password, agent socket, token, or secret. The quarantined `trt-gb10-7` node is
absent from the node, route, role, and proxy sets.

Runtime transport has these fixed properties:

- the remote account is always `qianyi`;
- only the root-owned installed identity and known-hosts authority are used;
- user and system identity discovery, SSH agents, passwords, keyboard
  interaction, host-key learning, connection sharing, forwarding, and PTYs are
  disabled;
- authority keys have a `restrict` forced-command entry and can invoke only
  `/usr/local/libexec/loom-developer-sandbox-node-authority transact` or
  `check`;
- the GB10 publisher role can invoke only `check`, including the fixed
  runtime-proof sources on OLDLAB1/2;
- the separate GB10 jump key has no SSH forwarding privilege. Its forced
  command maps a closed target name to one fixed internal address on port 22
  and relays that byte stream. It has no mapping for node 7 or another port.

OLDLAB2 reaches OLDLAB directly on `192.168.50.13` through
`192.168.50.17`. It reaches `trt-gb10-1` at `207.35.188.227:2221`, then uses
the forced jump stream for the declared GB10 internal addresses. The GB10
publisher on `gx10-01c7` reaches its declared peers directly on the
`192.168.20.0/24` network. For runtime proof only, it reaches OLDLAB1 at
`207.35.188.227:2321`, then uses OLDLAB1's forced closed proxy to OLDLAB2 at
`192.168.50.14:22`. No other OLDLAB target or port is mapped.

## External-root bootstrap

The tool never generates a key. A direct external root administrator must
provide:

1. one root-owned mode-`0600` identity file for every role required by the
   current initiator;
2. a matching root-owned mode-`0600` or `0644` Ed25519 public-key file for
   each identity;
3. a root-owned mode-`0600` or `0644` known-hosts file whose Ed25519 entries
   exactly equal the endpoints required by that initiator.

Private material must not be placed in the repository, `/shared_work`, a user
home, an SSH agent, or command output. Use filenames in evidence, never file
contents.

Plan the OLDLAB2 client bootstrap from a clean, exact, root-owned candidate:

```bash
python3 scripts/ops/developer_sandbox_node_transport.py bootstrap-client \
  --identity oldlab2-controller=/root/bootstrap/oldlab2-controller \
  --public-key oldlab2-controller=/root/bootstrap/oldlab2-controller.pub \
  --identity oldlab2-gb10-jump=/root/bootstrap/oldlab2-gb10-jump \
  --public-key oldlab2-gb10-jump=/root/bootstrap/oldlab2-gb10-jump.pub \
  --known-hosts /root/bootstrap/oldlab2-known-hosts
```

The plan reports only roles, SHA-256 digests, and public-key fingerprints.
Apply the unchanged command with `--execute`. The bootstrap validates that
every public key matches its supplied identity, copies the assets to fixed
root-owned paths below
`/etc/loom/developer-sandbox-node-transport`, and pins the installed digests
in a canonical root-only policy.

On `trt-eai-oldlab-1`, bootstrap the `oldlab1-publisher` client role with the
same command shape and its exact OLDLAB direct-route known-hosts set. On
`trt-gb10-1`, bootstrap the `gb10-1-publisher` and
`gb10-1-oldlab-jump` client roles with the same command shape and their exact
GB10/OLDLAB route known-hosts set. Both publisher authority roles are
check-only; the jump role can relay only the fixed OLDLAB2 endpoint.

Each target separately receives only the public keys required by its closed
server role. The following table is the complete machine-derived
`_server_roles` matrix for the checked-in route file; the input set must equal
the row exactly:

| Server node(s) | Exact public-key roles |
| --- | --- |
| `oldlab-1` | `gb10-1-oldlab-jump`, `gb10-1-publisher`, `oldlab1-publisher`, `oldlab2-controller` |
| `oldlab-2` | `gb10-1-publisher`, `oldlab1-publisher`, `oldlab2-controller` |
| `oldlab-3` through `oldlab-5` | `oldlab1-publisher`, `oldlab2-controller` |
| `trt-gb10-1` | `gb10-1-publisher`, `oldlab2-controller`, `oldlab2-gb10-jump` |
| `trt-gb10-2` through `trt-gb10-6`, and `trt-gb10-8` through `trt-gb10-15` | `gb10-1-publisher`, `oldlab2-controller` |

In particular, the GB10 submit publisher adds
`gb10-1-publisher` authority to OLDLAB1/2 and the
`gb10-1-oldlab-jump` proxy on OLDLAB1. Those keys are not optional additions
to an earlier controller-only server bootstrap. `trt-gb10-7` has no row and
must not receive a managed transport key.

```bash
python3 scripts/ops/developer_sandbox_node_transport.py bootstrap-server \
  --public-key oldlab2-controller=/root/bootstrap/oldlab2-controller.pub
```

Substitute all and only the roles in the target's table row. The server
bootstrap installs the root-owned forced-command program and policy, atomically
preserves unrelated `qianyi` authorized-key entries, and rejects a changed or
duplicate managed marker.

Both plan modes are read-only. Planning and execution both use an auditable
direct-login operational gate because planning reads the private trust inputs:
real UID and effective UID must both be zero, no `SUDO_*` marker may be
present, and Linux audit `/proc/self/loginuid` must exist and equal zero.
Consequently `sudo env -i` and equivalent environment clearing do not satisfy
the gate. The gate also rejects `/.dockerenv`, `/run/.containerenv`, an
explicit container environment, and Docker, containerd, Kubernetes, Podman,
libpod, or LXC cgroup identities. Both `/proc/self/cgroup` and
`/proc/1/cgroup` must be readable and stable; absence or ambiguity fails
closed, so a privileged container or host-root bind is not a bootstrap path.
The candidate checkout, every source asset and external trust asset,
and every parent component must remain root-owned,
non-symlinked, and not group/world writable through descriptor-pinned
readback. Copied authority bytes are also compared with the exact requested
Git commit blobs.

This gate is deliberately not described as cryptographic separation from an
actor who already has arbitrary root: such an actor can in principle alter or
bypass any local userspace check. Authorization remains the external
administrator's reviewed direct-login procedure and the independently
provided fixed identities, public keys, and host pins. Existing installed
identities, host pins, policy, or managed authorized-key entries are never
silently rotated or overwritten. Rotation requires a separately reviewed
candidate and explicit external-root operation.

## Transactional upgrade

Use `upgrade` after at least one client or server role is installed. It
automatically detects those installed roles from the validated client/server
policies; there is no role selector. Run it first without `--execute` from the
new clean, exact, root-owned candidate, then apply that unchanged command with
`--execute`:

```bash
python3 scripts/ops/developer_sandbox_node_transport.py upgrade
python3 scripts/ops/developer_sandbox_node_transport.py upgrade --execute
```

An unchanged role/route set takes no trust inputs. Existing private identities,
public keys, known-host pins, unrelated authorized-key lines, and managed
authorized-key identities are preserved and revalidated. Private identities
are not copied into upgrade snapshots. Removing an existing client identity
role fails closed and requires a separately reviewed retirement operation.

If the candidate adds a client role, supply exactly one root-owned identity and
one matching public key for each newly required role. If it adds only a server
role, supply exactly its new public key. If the initiator's exact endpoint set
changes, supply one replacement `--known-hosts`; it is rejected when the
endpoint set is unchanged. Extra, missing, ambient, symlinked, or
group/world-writable-parent inputs fail closed.

Upgrade creates a root-only lock, append-only journal, active marker, and
non-secret snapshot. The active marker makes `invoke`, `forced`,
`proxy-client`, `check-client`, and `check-server` fail closed throughout the
transaction. The node-authority dispatcher checks that marker immediately
after acquiring its shared/exclusive runtime lock and before reading policy or
accepting a request. Therefore a crash in `prepared`, `admission-disabled`,
`assets-replaced`, or `committed` state cannot admit a transaction or read-only
check; only a later direct-root upgrade invocation may consume and recover the
marker. Program, route, policy, public-key, host-pin, and managed
authorized-key updates use fsynced atomic replacement; the dispatcher is
replaced last. A failed readback restores the snapshot. A later direct-root
`upgrade --execute` rolls back an interrupted pre-commit transaction or
finalizes a fully committed one before evaluating the new candidate.

When both node authority and transport change, upgrade node authority first by
its direct-root transaction, read it back locally, then upgrade transport and
read back both transport roles. This ordering leaves the old forced transport
pointing only at a fully valid authority. Do not widen sudoers or temporarily
install an unrestricted SSH path to bridge the two upgrades.

## Readback and runtime use

After bootstrap, run local checks before the sandbox installer:

```bash
/usr/local/libexec/loom-developer-sandbox-node-transport check-client
/usr/local/libexec/loom-developer-sandbox-node-transport check-server
```

The checks revalidate root ownership, mode, link count, route/config/program
digests, known-host endpoint closure, identity digests, public fingerprints,
and the exact forced authorized-key lines. Any drift fails closed.

Root-owned callers send the canonical node-authority envelope on stdin:

```bash
/usr/local/libexec/loom-developer-sandbox-node-transport invoke \
  --node trt-gb10-2 \
  --verb check
```

There is no runtime flag for a user, host, address, port, route, jump,
identity, known-hosts file, command, password, or agent. Transport failure
returns a bounded error without relaying remote stderr.

Slurm maintenance uses the same fixed `invoke` transport and canonical
envelope. The admitted authority actions are `slurm-node-converge` for
non-controller nodes, `slurm-controller-converge` only for `oldlab-1` or
`trt-gb10-1`, read-only `slurm-check`, and receipt-bound `slurm-rollback`.
The transport carries no profile, root, restart, accounting, path, or
arbitrary flag. Node and domain must match the installed authority policy, and
`trt-gb10-7` has no route or authority identity.

The host-side fleet journal is the only orchestration surface. It calls
compute nodes first, persists each authority receipt and check readback,
then calls the controller. A retry sends the identical envelope and requires
the identical persisted receipt. Rollback sends only the exact prior request
ID; node authority verifies the prior node/domain/sandbox/candidate/tree plus
the still-current Slurm policy journal and snapshot digests before invoking
the fixed rollback path. The journal itself must be one canonical closed
object bound to the exact operation, cluster, physical hostname, Slurm node,
candidate, snapshot and rollback-target paths, controller-only accounting
flag, restart flag, terminal phase, and ordered timestamps. The snapshot
manifest is canonical and closed to the six reviewed Slurm/Docker/guard
paths. Each present row records root ownership, a non-group/world-writable
live mode, one link, size, and SHA-256, and is bound to one root-owned,
mode-`0600`, single-link archive file; each absent row has only null metadata
and no corresponding archive. The authority requires the separate canonical
`accounting-cas.json` archive exactly when the journal's controller-only
`apply_accounting` binding is true and forbids it on compute-node snapshots.
The receipt CAS covers both the complete manifest digest and the exact
accounting archive digest (or an explicit null accounting identity), so
rollback rejects foreign rows, missing or extra rows, archive metadata drift,
changed bytes, replaced accounting state, or an accounting attachment on the
wrong node role.
