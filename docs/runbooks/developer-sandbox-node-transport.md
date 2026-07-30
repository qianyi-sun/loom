# Developer sandbox node-authority transport

The developer-sandbox installer uses a dedicated, root-owned SSH transport to
reach the fixed node authority. This is not a general remote shell and is not
an alternative source of cluster or deployment authority.

## Security contract

The checked-in route authority is
`deploy/developer-sandboxes/node-authority-transport.toml`. It contains only
the closed node names, canonical hostnames, IP addresses, ports, initiators,
verbs, and jump relationships. It contains no public key, private key,
password, agent socket, token, or secret. `trt-gb10-7` is included in this
infrastructure inventory so it receives the same persistent authority,
transport, systemd, shared-path, and capacity foundation as the other GB10
hosts. The 2026-07-29 owner correction supersedes #822's static exclusion.
When node 7 is busy, the candidate-owned drain/quiescence gate delays
disruptive convergence without cancelling or preempting external jobs.

Runtime transport has these fixed properties:

- the remote account is always `qianyi`;
- only the root-owned installed identity and known-hosts authority are used;
- user and system identity discovery, SSH agents, passwords, keyboard
  interaction, host-key learning, connection sharing, forwarding, and PTYs are
  disabled;
- authority keys have a `restrict` forced-command entry and can invoke only
  `/usr/local/libexec/loom-developer-sandbox-node-authority transact` or
  `check`;
- sshd runs that forced dispatcher as `qianyi`. The root-owned transport
  install directory is mode `0755`, and only the secret-free `routes.toml` and
  `server-policy.json` are mode `0644`, so the dispatcher can validate its
  installed server identity before acting. Private identities, `known_hosts`,
  `client-policy.json`, upgrade records, and snapshots remain root-only
  (`0600` files below private `0700` directories where applicable);
- the dispatcher parses `SSH_ORIGINAL_COMMAND` before privilege transition and
  executes only the existing exact node-authority sudo command for `transact`
  or `check`. It passes a clean environment and does not preserve
  `SSH_ORIGINAL_COMMAND` or any `SUDO_*` state through sudo. The node authority
  independently requires the exact `qianyi` sudo caller, UID/GID, verb, and
  `SUDO_COMMAND`; there is one sudo transition, no nested sudo, and no
  wildcard-argument sudo rule;
- the GB10 publisher role can invoke only `check`, including the fixed
  runtime-proof sources on OLDLAB1/2;
- the separate GB10 jump key has no SSH forwarding privilege. Its forced
  command maps a closed target name to one fixed internal address on port 22
  and relays that byte stream. The node 7 mapping is fixed to
  `192.168.20.77:22`; no role can select another address or port.

OLDLAB1 uses `192.168.50.103` for its LAN self-route. OLDLAB2 reaches OLDLAB1
at that same address and reaches OLDLAB2 through OLDLAB5 directly on
`192.168.50.14` through `192.168.50.17`. It reaches `trt-gb10-1` at
`207.35.188.227:2221`, then uses the forced jump stream for the declared GB10
internal addresses. The GB10 publisher on `gx10-01c7` reaches its declared
peers directly on the `192.168.20.0/24` network. For runtime proof only, it
reaches OLDLAB1 at `207.35.188.227:2321`, then uses OLDLAB1's forced closed
proxy to OLDLAB2 at `192.168.50.14:22`. No other OLDLAB target or port is
mapped.

## Persistent-root bootstrap

Before handling any trust asset, render the complete ceremony checklist:

```bash
python3 scripts/ops/developer_sandbox_node_transport.py bootstrap-inventory
```

This command needs no root authority and accepts no argument, path, trust
asset, secret, or execution flag. It reads only the checked-in route config and
emits closed canonical JSON containing all 20 canonical node/hostname rows,
each node's exact server roles, the three initiators' exact client roles and
required known-host endpoint tokens, and an empty transport-exclusion set.
The output is informational inventory, not an authority receipt, installation
attestation, host-key pin, or mutation authorization.

The JSON inventory is the machine source of truth for the external ceremony.
The human-readable role and verb tables below are contract-tested projections
of the same checked-in config; do not maintain a separate hand-written role
list.

Every one-shot Docker/chroot request uses bootstrap envelope schema version 2
and includes a caller-generated 64-hex `operation_id`. The request digest binds
that identifier together with the action, exact candidate, node, bundle, and
closed input inventory. Reuse one operation ID only to replay the same
operation and expected result. Generate a new operation ID for a later
readback or lifecycle phase so that authority-only, transport-installed, and
post-convergence evidence remain separate immutable receipts; never delete or
rewrite an earlier receipt to make a changed readback fit.
Use `scripts/ops/developer_sandbox_node_docker_request.py` to render the
canonical envelope. For readback, bind `--transport-expectation absent` before
transport installation, `server` on server-only nodes after installation, and
`client-server` on the three initiators. The renderer emits only digests and
public metadata; run it through the Docker-root ceremony when its closed input
directory contains a private identity.

The tool never generates a key. A host-root administrator, including the
repository's one-shot Docker/chroot bootstrap channel, must provide:

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
GB10/OLDLAB route known-hosts set. Their authority verbs are closed to the
checked-in role policy:

| Publisher authority role | Exact verbs |
| --- | --- |
| `oldlab1-publisher` | `check`, `transact` |
| `gb10-1-publisher` | `check` |

Thus `oldlab1-publisher` may perform its fixed publication transactions and
checks, while `gb10-1-publisher` is check-only. The separate jump role can
relay only the fixed OLDLAB2 endpoint.

Each target separately receives only the public keys required by its closed
server role. The following table is the complete machine-derived
`_server_roles` matrix for the checked-in route file; the input set must equal
the row exactly:

| Server node(s) | Exact public-key roles |
| --- | --- |
| `oldlab-1` | `gb10-1-oldlab-jump`, `gb10-1-publisher`, `oldlab1-publisher`, `oldlab2-controller` |
| `oldlab-2` | `gb10-1-publisher`, `oldlab1-publisher`, `oldlab2-controller` |
| `oldlab-3` through `oldlab-5` | `oldlab1-publisher`, `oldlab2-controller` |
| `trt-gb10-1` | `gb10-1-publisher`, `oldlab1-publisher`, `oldlab2-controller`, `oldlab2-gb10-jump` |
| `trt-gb10-2` through `trt-gb10-15` | `gb10-1-publisher`, `oldlab2-controller` |

In particular, the GB10 submit publisher adds
`gb10-1-publisher` authority to OLDLAB1/2 and the
`gb10-1-oldlab-jump` proxy on OLDLAB1. Those keys are not optional additions
to an earlier controller-only server bootstrap. `trt-gb10-7` receives the same
managed server roles as every other non-controller GB10 host. That host-side
foundation is part of the same all-15 infrastructure and capacity-eligible
contract.

```bash
python3 scripts/ops/developer_sandbox_node_transport.py bootstrap-server \
  --public-key oldlab2-controller=/root/bootstrap/oldlab2-controller.pub
```

Substitute all and only the roles in the target's table row. The server
bootstrap installs the root-owned forced-command program and policy, atomically
preserves unrelated `qianyi` authorized-key entries, and rejects a changed or
duplicate managed marker. The public modes above reveal only the checked-in
route contract, public-key fingerprints, and the managed `authorized_keys`
digest; they expose no private key, host-key database, request payload, client
identity policy, or authority state.

Both plan and execution require real and effective UID zero plus a persistent
host-root view whose `/` is the root of visible PID 1 and whose PID 1 is
systemd. This admits either host root or the fixed one-shot Docker/chroot
channel; it does not require a direct-root login. The Docker transaction uses
the exact-candidate image entrypoint and read-only request, Git bundle, and
trust-input mounts described in `developer-sandboxes.md`. It copies trust
inputs into a root-owned host `/run` stage before invoking these functions.
The container exits after installed-state readback and is never part of
runtime transport.

The candidate checkout, every source asset and external trust asset, and every
parent component remain root-owned, non-symlinked, and not group/world writable
through descriptor-pinned readback. Copied authority bytes are compared with
the exact requested Git commit blobs. Existing installed identities, host
pins, policy, or managed authorized-key entries are never silently rotated or
overwritten. Rotation requires a separately reviewed candidate and explicit
persistent-root transaction.

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
check; only a later persistent-root upgrade invocation may consume and recover the
marker. Program, route, policy, public-key, host-pin, and managed
authorized-key updates use fsynced atomic replacement; the dispatcher is
replaced last. A failed readback restores the snapshot. A later persistent-root
`upgrade --execute` rolls back an interrupted pre-commit transaction or
finalizes a fully committed one before evaluating the new candidate.
The upgrade explicitly admits the previous exact `0700` install-root plus
`0600` route/server-policy generation, snapshots those modes, migrates them
transactionally to `0755`/`0644`, and restores the old modes as well as bytes
on rollback. Mixed or partially widened legacy metadata is rejected.

When both node authority and transport change, upgrade node authority first by
its persistent-root transaction, read it back locally, then upgrade transport and
read back both transport roles. This ordering leaves the old forced transport
pointing only at a fully valid authority. Do not widen sudoers or temporarily
install an unrestricted SSH path to bridge the two upgrades.

The final Docker `readback` is the node-stack completion boundary, not merely a
self-consistency check. For every expected transport role it compares the
installed transport program and route configuration byte-for-byte with the
staged exact candidate, persists both SHA-256 digests with the candidate
SHA/tree in the canonical receipt result, and fails closed on a stale but
internally consistent transport generation. An `authority-upgrade` receipt by
itself is therefore not node-stack convergence. Keep the authority and
transport transactions separate: a transport role or endpoint change may
require its own closed key or `known_hosts` inputs and must retain its
independent snapshot and rollback domain.

## Readback and runtime use

After bootstrap, run local checks before the sandbox installer:

```bash
/usr/local/libexec/loom-developer-sandbox-node-transport check-client
/usr/local/libexec/loom-developer-sandbox-node-transport check-server
```

The checks revalidate root ownership, mode, link count, route/config/program
digests, known-host endpoint closure, identity digests, public fingerprints,
and the exact forced authorized-key lines. Any drift fails closed. The
persistent Docker `readback` additionally binds those installed program and
route bytes to the exact candidate; standalone `check-client` or
`check-server` proves only installed-generation self-consistency.

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
`trt-gb10-7` has a persistent route and authority identity for infrastructure
convergence, readback, and capacity operation. Disruptive convergence remains
subject to the candidate-owned drain/quiescence gate; an external job must
never be cancelled or preempted to make the gate pass.

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
