# Personal-development rootless BuildKit sidecar

## Status and scope

This design corrects the protected personal-development builder runtime after
the issue #1280 acceptance window
`issue-1280-runtime-20260820T162525Z-6226d5c6f06c`. The KVM gVisor fleet
installation and node-pinned probes passed, but the exact builder image failed
before BuildKit startup:

```text
[rootlesskit:parent] error: failed to start the child: fork/exec /proc/self/exe: operation not permitted
```

The failure log SHA-256 is
`ffb7235e088b34d679741c1c4a3ff5bf843cb78809bbc687c224167c487c08ad`.
The fleet was completely rolled back, the RuntimeClass was deleted, and the
global executable-new-capacity ceiling remained zero. This design changes the
builder Pod contract and its conformance proof. It does not activate personal
development, change Slurm, submit work, or raise that ceiling.

## Root cause

The failed image uses RootlessKit `3.0.1`. RootlessKit starts its child with
`CLONE_NEWUSER | CLONE_NEWNS`, then executes `newuidmap` and `newgidmap` to
install the image's 65,536 subordinate-ID ranges. The image gives those two
helpers only these file capabilities:

- `/usr/bin/newuidmap`: `cap_setuid=ep`;
- `/usr/bin/newgidmap`: `cap_setgid=ep`.

The previous Pod contract made both required operations impossible:

1. `seccompProfile: RuntimeDefault` rejected RootlessKit's namespace-creating
   clone. The identical `/proc/self/exe` error reproduces under runc and stops
   occurring when only that filter is removed.
2. `allowPrivilegeEscalation: false` set `no_new_privs`, and `drop: [ALL]`
   removed `SETUID` and `SETGID` from the capability bounding set. Consequently
   the two exact file-capability helpers could not install the subordinate-ID
   maps even after namespace creation was allowed.

RootlessKit `3.1.0` does not change this mechanism, so a version-only update is
not a correction. A local digest-pinned `RUN` build passes with the exact image
when seccomp is unconfined and only `SETUID` plus `SETGID` are restored to the
bounding set. The outer process remains UID/GID 1000 with zero effective
capabilities. gVisor's `allow-suid=false` remains compatible: it ignores
set-ID bits, while file capabilities are independently bounded and
`no_new_privs` still suppresses their elevation.

## Approaches considered

### Selected: capability-free client plus isolated rootless sidecar

Run the trusted candidate client and the disposable BuildKit daemon in separate
containers in one Pod. The client retains the restricted security contract and
is the only container that receives the build contract and presigned source and
artifact capabilities. A native Kubernetes sidecar receives the narrow
RootlessKit startup authority but no Secret, contract, source directory,
artifact path, or service-account token. The containers share only one Unix
socket volume.

This isolates the trusted client from Dockerfile `RUN` processes even though
BuildKit must retain `--oci-worker-no-process-sandbox` under Kubernetes. A
malicious build can terminate or corrupt its disposable daemon, causing the
attempt to fail, but cannot inspect the client's PID or mount namespace, read
its capability projection, or alter an artifact after the client verifies it.

### Rejected: broaden the existing single container

Adding unconfined seccomp, `SETUID`, `SETGID`, and privilege escalation to the
current container makes RootlessKit start, but BuildKit and the trusted Python
client remain in one PID namespace. With process sandboxing disabled, candidate
code can signal and potentially ptrace the client that holds upload authority.
gVisor protects the host; it does not make that intra-sandbox authority sharing
acceptable.

### Rejected: custom RootlessKit or single-ID mappings

A Loom-specific launcher could write a one-entry UID/GID map without the two
helpers, but real base images contain multiple owners and require subordinate
IDs for correct extraction and output. Forking RootlessKit to implement
security-critical multi-ID mapping would create more privileged code than the
two pinned, established helpers and would still require namespace syscalls.

## Pod and namespace contract

Each target-platform Job keeps one Pod and uses the exact measured gVisor
RuntimeClass. `shareProcessNamespace` is explicitly false and
`automountServiceAccountToken` is false.

The dynamic build namespace uses Pod Security Admission `baseline` enforcement
at `v1.36`, with `restricted` audit and warning at `v1.36`. Baseline is the
narrowest standard that admits RootlessKit's required startup contract. Only
the management service account can create workloads there, the ResourceQuota
still permits exactly two Jobs and two Pods, and candidate code receives no
Kubernetes credential.

### Trusted client container

The regular `builder` container:

- runs as UID/GID 1000 and non-root;
- uses RuntimeDefault seccomp;
- sets `allowPrivilegeEscalation=false`;
- drops every Linux capability;
- has a read-only root filesystem;
- is the only container mounting the immutable contract ConfigMap, the
  per-attempt capability Secret, and the bounded workspace;
- calls `/usr/bin/buildctl` against the shared Unix socket instead of starting
  a daemon; and
- verifies every returned OCI archive before constructing and uploading the
  bounded personal-development artifact.

### Rootless BuildKit native sidecar

An init container named `buildkitd` has `restartPolicy: Always`, making it a
native sidecar on Kubernetes 1.36. It:

- uses the same digest-pinned trusted builder image;
- runs as UID/GID 1000 and non-root;
- has a read-only root filesystem;
- drops all capabilities, then adds only `SETUID` and `SETGID` to the bounding
  set;
- uses `allowPrivilegeEscalation=true` and seccomp `Unconfined` only inside the
  gVisor sandbox;
- starts an immutable Loom sidecar launcher that fails unless the gVisor
  marker, UID/GID 1000, zero effective capabilities, exact
  `SETUID|SETGID` bounding set, `NoNewPrivs=0`, and seccomp mode 0 are present,
  then execs `/usr/bin/rootlesskit` with command `/bin/setpriv --nnp
  /usr/bin/buildkitd`; BuildKit and every descendant therefore have
  `no_new_privs=1` after the subordinate-ID maps exist;
- uses the native snapshotter and `--oci-worker-no-process-sandbox`;
- mounts only private BuildKit state, private temporary storage, and the shared
  socket directory; and
- receives no contract, Secret, source, output, or service-account mount.

The startup probe calls digest-pinned-image `/usr/bin/buildctl debug workers`
through the Unix socket. Kubernetes does not start the regular client until
the native sidecar startup probe succeeds. When the client exits, Job-sidecar
semantics terminate the sidecar without keeping the Job alive.

The trusted image build fails unless the base still supplies the expected
RootlessKit version and binary plus the exact `newuidmap`/`newgidmap` file
capabilities. The fail-closed sidecar launcher is a checked-in trusted-image
source and its preflight marker is captured in bounded sidecar logs. These are
part of trusted image publication evidence, not mutable node prerequisites.

## Data and authority flow

1. The client reads the immutable build contract and presigned capabilities.
2. Before reading either capability, it fails unless its own gVisor marker,
   UID/GID 1000, empty effective and bounding capability sets,
   `NoNewPrivs=1`, and RuntimeDefault seccomp mode are present. It then
   downloads and verifies the source archive into its private workspace.
3. `buildctl` sends the selected source context to BuildKit over the Unix
   socket. The source directory is not mounted into the sidecar.
4. BuildKit executes the untrusted Dockerfile in its rootless subordinate-ID
   namespace behind the KVM gVisor boundary and existing NetworkPolicy.
5. The OCI exporter streams the result back over the session to the client's
   bounded destination file. The sidecar does not mount that output path.
6. The client verifies platform metadata, digests, sizes, and archive structure,
   creates the canonical artifact, and uploads it with its one-attempt
   capability.

The shared Pod network is necessary for base-image egress. The trusted client
opens no listener, and its capability files are absent from the sidecar mount
namespace. A build can exfiltrate its own source or fail its attempt; it cannot
obtain the upload capability or Kubernetes authority.

## Failure and cleanup behavior

The client uses a bounded readiness wait before the first build. Socket absence,
worker-probe failure, daemon exit, build failure, output verification failure,
or sidecar restart makes the attempt fail closed. Jobs retain `backoffLimit: 0`,
the attempt lease fences late results, and the existing controller removes the
ephemeral namespace. BuildKit state and both temporary volumes are `emptyDir`
and disappear with the Pod.

The runtime rollout remains fail-closed. A new trusted release and issue #1280
window must install the exact RuntimeClass, prove all four gVisor nodes, then
run the two-container conformance. Failure never permits a runc substitution,
single-container fallback, security-context relaxation beyond this exact
sidecar, capacity-ceiling change, or personal acceptance.

## Verification

Repository tests must prove:

- exact PSA baseline enforcement and restricted audit/warn versions;
- explicit false shared PID namespace and absent service-account token;
- the client retains the restricted security context and is the sole consumer
  of contract, capability, workspace, and output volumes;
- the sidecar has only the two required bounding capabilities, unconfined
  seccomp, privilege escalation, private state/tmp mounts, native-sidecar
  lifecycle, and no authority-bearing mounts;
- the client calls `buildctl` directly at the exact Unix address;
- trusted-image checks bind RootlessKit and both helper file capabilities; and
- status and cleanup logic accept only the exact two-container contract.

Protected operational conformance must additionally prove:

- the initial sidecar process is UID/GID 1000 with `CapEff=0`,
  `CapBnd=SETUID|SETGID`, and seccomp disabled inside gVisor;
- BuildKit and Dockerfile `RUN` observe `NoNewPrivs=1`;
- the client observes zero effective and bounding capabilities,
  `NoNewPrivs=1`, and RuntimeDefault seccomp;
- the client cannot observe RootlessKit or BuildKit processes in its `/proc`;
- the sidecar has none of the contract/capability/workspace mounts;
- digest-pinned amd64 and QEMU arm64 `RUN` steps report the expected machines
  and OCI platform metadata; and
- the temporary namespace is removed and all fleet, Longhorn, Secret,
  namespace, Pod-continuity, and executable-capacity-zero invariants still pass.
