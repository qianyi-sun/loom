# Personal-development builder runtime

## Decision

Personal candidate builds run through one operator-installed gVisor runtime on
the four OLDLAB Kubernetes agent nodes. The runtime uses gVisor
`release-20260810.0` with the KVM platform. The control-plane node is never
eligible. Both `linux/amd64` and `linux/arm64` candidate images are built on
these amd64 agents; BuildKit's release-pinned `buildkit-qemu-aarch64` helper
provides arm64 emulation inside the gVisor sandbox.

The RuntimeClass is an independently installed, measured cluster capability.
The management-plane shadow remains inert while this capability is installed:
builder preparation stays false, activation replicas stay zero, no personal or
build namespace is created, and the global executable-new-capacity ceiling
stays exactly zero.

## Why this design

Three designs were considered.

1. KVM gVisor on the existing amd64 agents, with emulated arm64 builds. This is
   the selected design. Every eligible node has `/dev/kvm`, the KVM modules,
   more than 6 TB free under the K3s data filesystem, and the exact K3s and
   containerd versions used by the profile. It preserves one Kubernetes trust
   boundary and does not couple builder availability to the separate GB10
   Slurm rollout.
2. Systrap gVisor on every node. This avoids the KVM prerequisite but is slower
   on bare metal and gives no benefit on the measured nodes, all of which pass
   KVM conformance.
3. Native builds on an amd64 OLDLAB node and an arm64 GB10 Kubernetes node.
   GB10 is a Slurm pool, not a measured Kubernetes builder pool. Joining it to
   K3s would couple two independent rollouts, add cluster authority to a compute
   pool, and block personal development on unrelated autoscaler work.

The existing `hostUsers: false` builder field is removed. As of the pinned
gVisor release, upstream issue `google/gvisor#13303` remains open and runsc does
not advertise Kubernetes Pod user-namespace support through CRI. Pretending
otherwise makes every builder Pod fail before start. The replacement is not a
weaker runc Pod: candidate code remains behind the KVM gVisor syscall boundary.
The Pod separates a capability-free trusted client from an authority-free
rootless BuildKit native sidecar. The client alone receives the contract,
presigned capabilities, source workspace, artifact destination, verification,
and upload logic; it keeps RuntimeDefault seccomp, no-new-privileges, a
read-only root filesystem, and an empty capability set. The sidecar receives no
contract, Secret, source, output, or service-account token. It temporarily
admits only unconfined seccomp plus `SETUID` and `SETGID` in its bounding set so
RootlessKit can create the user namespace and execute the two pinned
ID-mapping helpers. `/bin/setpriv --nnp` then starts BuildKit, so BuildKit and
every Dockerfile `RUN` descendant have no-new-privileges. Separate container
PID and mount namespaces prevent candidate code from observing the client or
its authority-bearing files. Network policy remains default-deny, and runsc
uses its sandbox network stack with host UDS/FIFO and raw packet writes
disabled.

## Measured release and profile

The checked-in runtime profile is the complete public, non-secret description
of the installed capability. Its SHA-256 is calculated over the exact profile
file bytes, including the final newline. The profile fixes these inputs:

- gVisor version `release-20260810.0` and tag commit
  `5ceb9a5fd5750d6c73dd166441f28306039300d0`;
- archive
  `https://storage.googleapis.com/gvisor/releases/release/20260810/x86_64/gvisor.tar.bz2`;
- archive SHA-512
  `3de91138cda15682c11807387f6ecad9e7c8932262018a2813277e1b4efa03efe33b0a948e148c6b1ccfe7345bfab5d5e0d072519505465751273898bae19c62`;
- the exact five regular-file archive members, their required `gvisor-bin`
  parent-directory entry, sizes, archive/install modes, and SHA-256 digests;
- K3s `v1.36.2+k3s1`, containerd `v2.3.2-k3s2`, Linux amd64, `/dev/kvm`,
  and loaded `kvm` plus `kvm_intel` modules;
- all installation paths, containerd handler bytes, and runsc flags; and
- RuntimeClass identity and the profile-label encoding.

The archive is supplied as an owner-controlled local input. The installer does
not use the gVisor Debian package because its post-install hook may reconfigure
and reload an unrelated Docker daemon. It does not download as root and does
not follow a mutable package repository.

The archive becomes a root-owned, read-only release at
`/opt/loom/gvisor/release-20260810.0`. The versioned directory preserves the
adjacency of `runsc`, `containerd-shim-runsc-v1`, and `gvisor-bin/*` required by
release enforcement. A root-owned
`/usr/local/bin/containerd-shim-runsc-v1` symlink exposes the shim on K3s's
service PATH. The runsc binary path is absolute in the shim configuration, so
PATH cannot select a different runsc.

## Containerd and runsc contract

K3s owns its generated containerd configuration. The installer creates the
documented v3 extension template rather than copying a generated config:

```toml
{{ template "base" . }}

[plugins.'io.containerd.cri.v1.runtime'.containerd.runtimes.'runsc-personal-dev']
  runtime_type = "io.containerd.runsc.v1"
[plugins.'io.containerd.cri.v1.runtime'.containerd.runtimes.'runsc-personal-dev'.options]
  TypeUrl = "io.containerd.runsc.v1.options"
  ConfigPath = "/etc/containerd/runsc-personal-dev.toml"
```

The template is installed only when the destination is absent or already
byte-identical. An unrelated existing template is a stop condition; the
installer never merges TOML text heuristically.

`/etc/containerd/runsc-personal-dev.toml` fixes the absolute runsc binary and
these runsc flags:

| Flag | Value | Reason |
| --- | --- | --- |
| `platform` | `kvm` | Measured bare-metal platform |
| `platform_device_path` | `/dev/kvm` | No device discovery ambiguity |
| `network` | `sandbox` | Candidate traffic stays in netstack |
| `directfs` | `false` | Keep filesystem access in the less-privileged gofer |
| `file-access` | `exclusive` | Root filesystem is not externally mutated |
| `file-access-mounts` | `shared` | Kubernetes projections and emptyDir remain coherent |
| `host-uds` | `none` | No host Unix socket bridge |
| `host-fifo` | `none` | No host FIFO bridge |
| `net-raw` | `false` | Remove raw-socket authority |
| `allow-packet-socket-write` | `false` | Prevent crafted L2 writes |
| `allow-flag-override` | `false` | Pod annotations cannot widen the profile |
| `sidecar-release-enforcement-policy` | `ALWAYS` | All adjacent helpers must match runsc |
| `gvisor-marker-file` | `true` | Provide an in-sandbox proof signal |
| `oci-seccomp` | `true` | Enforce the Pod's RuntimeDefault profile in the sandbox |
| `host-settings` | `check` | Fail mandatory host checks without mutating the host |
| `watchdog-action` | `panic` | Terminate a nonresponsive sentry |
| `restore-spec-validation` | `enforce` | Never restore a mismatched specification |
| `allow-suid` | `false` | Do not permit set-ID elevation |
| `debug`, `strace`, `profile` | `false` | No debug-induced attack-surface widening |

The installer stages files but never restarts K3s. Activation is a separately
recorded operator action, allowing the exact staged bytes and rollback
conditions to be reviewed first.

## RuntimeClass and node eligibility

The cluster-scoped RuntimeClass is named `loom-personal-dev-builder` and uses
handler `runsc-personal-dev`. It carries annotation
`loom.dev/runtime-profile-sha256=<profile SHA-256>`.

Kubernetes label values cannot contain a 64-character SHA-256. The profile is
therefore encoded without truncation as two node selectors:

- `loom.dev/personal-dev-runtime-profile-a=<hex characters 0..31>`;
- `loom.dev/personal-dev-runtime-profile-b=<hex characters 32..63>`.

The RuntimeClass also selects `kubernetes.io/os=linux` and
`kubernetes.io/arch=amd64`. Only nodes 2 through 5 receive the two labels, and
only after the node-local verifier passes against the active generated
containerd config. The full digest is also recorded in the node annotation
`loom.dev/personal-dev-runtime-profile-sha256` for human and evidence review.

The non-secret management profile records the exact runtime handler and runtime
profile digest even while `prepared=false`. Shadow status derives the two label
values from that digest and requires the RuntimeClass handler, annotation, and
scheduling object to match. Acceptance status repeats the check against the
acceptance plan and requires the plan binding to equal the non-secret profile.
A handler and annotation without exact scheduling are insufficient. The
dynamic builder manifest supplies no architecture node selector. Both
target-platform Jobs consequently use the measured amd64 runtime nodes; the
target platform remains bound in the immutable build contract and OCI output.

## Installer state machine

The node-local Python installer has five operations:

- `preflight` performs read-only host, archive, destination, K3s, containerd,
  module, device, disk, service PATH, and current-process checks.
- `install` repeats preflight, safely extracts only the five expected regular
  members, verifies every byte before publication, installs exact configuration
  files, fsyncs the containing directories, and reports canonical JSON. It is
  idempotent only for a complete byte-identical installation.
- `verify-staged` verifies ownership, modes, link targets, every digest, and
  exact config bytes without requiring containerd to have reloaded them.
- `verify-active` additionally checks the active K3s agent, generated containerd
  v3 runtime entry, exact K3s/containerd versions, and runsc version.
- `remove` requires a byte-identical managed installation, then removes only
  the exact files and empty directories named by the profile. RuntimeClass and
  node-label removal, absence of handler Pods, and the subsequent K3s-agent
  restart remain operator interlocks outside the node-local command.

Any partial, writable, non-root-owned, multiply linked, symlinked (except the
one exact shim link), version-drifted, or unexpected destination fails closed.
The installer prints no environment, kubeconfig, Secret, or arbitrary command
output.

Rollback is intentionally an operator sequence, not a broad recursive delete.
After the RuntimeClass and node eligibility labels are removed and no Pod uses
the handler, `remove` returns the node to the measured previously absent state
only if every managed file still matches the recorded profile. A nonmatching
file is preserved for investigation.

## Sequential rollout

Node rollout order is agents 2, 3, 4, then 5. The control-plane node is never
staged. Each node uses this bounded transaction:

1. Record node identity, Ready/DiskPressure state, scheduled Pod identities,
   Longhorn health, and the global capacity ceiling.
2. Cordon the node. Do not drain it: live Longhorn instance-manager and
   PostgreSQL primary PDBs have zero allowed disruptions on some nodes, and
   force-draining would violate the storage safety boundary.
3. Run node-local `preflight`, `install`, and `verify-staged` with the exact
   archive and profile.
4. Restart only `k3s-agent`. K3s uses `KillMode=process`, allowing containerd
   shims to preserve running tasks across the bounded restart.
5. Require the node to return Ready with no DiskPressure, the previous running
   Pods to remain healthy, Longhorn to remain healthy, and `verify-active` to
   pass.
6. Apply the exact profile labels and annotation, uncordon the node, and
   re-prove all guardrails before moving to the next agent. The RuntimeClass is
   still absent, so the labels alone cannot schedule a builder.

If any step fails, keep the node cordoned, remove no evidence, use `remove` to
return only byte-identical managed files to their previously absent state,
restart the agent, and require the baseline before proceeding. A failure on one
node stops the fleet rollout.

After all four nodes pass, server-side diff and apply the exact RuntimeClass.
Then run one digest-pinned, node-bound smoke Pod on each agent and require
`/proc/gvisor/kernel_is_gvisor`, nonroot identity, no effective capabilities,
and the exact RuntimeClass binding. On the first node, additionally run
the exact restricted-client/native-sidecar contract. The proof captures both
container identities and image IDs, requires the sidecar preflight with only
the two ID-mapping capabilities, observes `NoNewPrivs=1` on BuildKit itself,
proves the client cannot see RootlessKit or BuildKit through `/proc`, and runs
both `linux/amd64` and `linux/arm64` Dockerfile steps with `NoNewPrivs=1`,
including arm64 through the image's trusted QEMU helper. This ordering avoids
referring to a RuntimeClass before it exists while keeping the class absent
until all nodes are actively verified. The baseline-enforced smoke namespace,
with restricted audit/warn, and its Pods are temporary operator-owned resources
and are deleted after both container logs, the canonical Pod, and terminal
status are captured. No personal or build namespace is used for smoke testing.

## Evidence and readiness

The owner-only evidence directory records, without credentials:

- profile, archive, installed-member, runsc-config, K3s-template, and
  RuntimeClass hashes;
- each node's preflight, staged, active, restart, Pod continuity, smoke, label,
  and uncordon receipts;
- RuntimeClass server-side diff and live canonical JSON;
- the conformance client and sidecar logs, image IDs, mount/PID separation,
  BuildKit and Dockerfile no-new-privileges evidence, and both OCI platforms;
- five Ready nodes, no DiskPressure, Longhorn health, and package resource
  inventory;
- absence of personal/build namespaces and personal workers; and
- the exact global authority identity, execution state, execution epoch, and
  executable-new-capacity ceiling zero before and after every mutation.

Installing the runtime removes the `runtime_class_missing` shadow blocker but
does not authorize the management-plane apply. The protected #1280 window,
new CI-approved trusted release, fresh shadow render, full server-side diff
audit, and all existing shadow stop conditions remain mandatory.

## Testing

Repository tests cover strict profile parsing, archive member and digest
validation, safe extraction, idempotence, partial-state rejection, config
publication, staged and active verification, profile-label encoding,
RuntimeClass/profile consistency, unsupported host user-namespace removal,
amd64 scheduling for both target platforms, and acceptance status drift.

Operational verification covers the behavior that unit tests cannot simulate:
K3s v3 template rendering, shim discovery, KVM startup, gVisor marker evidence,
rootless BuildKit inside gVisor, QEMU arm64 execution, Pod continuity across
each K3s-agent restart, and exact rollback.

## Non-goals

This work does not enable builder preparation, activation, personal
Deployments, personal workers, task submission, or physical capacity. It does
not change the GB10 or OLDLAB Slurm controllers. It does not open the #1280
shadow or acceptance window. It does not claim that RuntimeClass installation
alone makes the multi-person environment accepted.
