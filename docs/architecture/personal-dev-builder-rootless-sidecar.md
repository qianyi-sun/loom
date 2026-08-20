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
bounding set. The outer process remains UID/GID 1000 with zero inheritable,
permitted, effective, and ambient capability sets. gVisor's `allow-suid=false`
remains compatible: it ignores set-ID bits, while file capabilities are
independently bounded and `no_new_privs` still suppresses their elevation.

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

### Rejected: an allow-all Localhost seccomp profile

Baseline permits a `Localhost` seccomp profile, so a node-installed profile
with `SCMP_ACT_ALLOW` could preserve the Baseline label while reporting
seccomp mode 2. It would not reduce the permitted syscall set, would add a
mutable prerequisite to every eligible node, and would make the label imply a
restriction that does not exist. The explicit `privileged` PSA label plus an
exact Loom admission contract is simpler, fail-closed, and auditable in the
same protected release as the workload.

## Pod and namespace contract

Each target-platform Job keeps one Pod and uses the exact measured gVisor
RuntimeClass. `shareProcessNamespace` is explicitly false and
`automountServiceAccountToken` is false.

The dynamic build namespace uses Pod Security Admission `privileged`
enforcement at `v1.36`, with `restricted` audit and warning at `v1.36`.
Kubernetes Baseline explicitly rejects a container whose seccomp type is
`Unconfined`; describing this exception as Baseline would therefore make every
builder Job fail admission. The namespace label is an honest statement about
the one standard-policy exception, not the workload's effective authority.

A Loom ValidatingAdmissionPolicy supplies the application-specific boundary
that the standard policies do not express. For every builder Job submitted by
the management principal it binds the exact trusted image digest and measured
RuntimeClass, the single restricted client plus single native sidecar shape,
separate PID namespace, disabled service-account token and service links,
non-host namespaces, exact security contexts, exact volume separation, and
finite resource envelope. It also requires exactly one non-indexed completion,
forbids alternate Job controllers and failure/success policies, and rejects
client hooks, extra probes, termination-message path changes, resource claims,
supplemental groups, and alternate Pod scheduling fields. It also binds the
Job and Pod-template metadata, couples each platform to the same-lease contract
and capability volumes, forbids ConfigMap/Secret item remapping or optionality,
and keeps every `emptyDir` disk-backed with no recursive mount override.
Quantity comparisons use Kubernetes' `Quantity.compareTo` API so equivalent
canonical forms are accepted while different resource envelopes are denied.
The same policy requires immutable, attempt/platform-named ConfigMaps and
Secrets with exact key inventories and bounded values, plus the exact
container defaults and hard counts in the builder LimitRange and
ResourceQuota. The ConfigMap quota is exactly three: one slot for Kubernetes'
injected `kube-root-ca.crt` plus one immutable contract for each target
platform. Their variable contract and capability values remain visible only to
the client; a permitted object name is not permission to change the supporting
object's authority shape. Every namespaced builder resource also couples its
attempt label to the enclosing attempt Namespace.
The same policy binds `default-deny` and `builder-egress` to their exact
generated selectors, peers, ports, and private/reserved-network exclusions; a
permitted resource name cannot be used to install wider egress. NetworkPolicy
and delegated RoleBinding metadata also reject finalizers, owner references,
annotations, or extra labels that could delegate authority or wedge teardown.
The privileged builder Namespace itself has the same non-delete metadata
boundary: its exact attempt labels are required, the attempt UUID is coupled to
the Namespace name, and only Kubernetes' matching injected
`kubernetes.io/metadata.name` label is optional. Namespace and namespaced-object
deletes remain shape-independent so the controller can remove an object drifted
by a higher-authority principal.
The management RBAC cannot create Pods directly; the Job controller can create
only Pods from a template that passed this policy. Only the management service
account can create workloads in the namespace, the ResourceQuota still permits
exactly two Jobs and two Pods, and candidate code receives no Kubernetes
credential.

### Trusted client container

The regular `builder` container:

- runs as UID/GID 1000 and non-root;
- uses RuntimeDefault seccomp;
- sets `allowPrivilegeEscalation=false`;
- drops every Linux capability and fails unless its inheritable, permitted,
  effective, bounding, and ambient sets are all empty;
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
  marker, UID/GID 1000, zero inheritable, permitted, effective, and ambient
  capability sets, exact `SETUID|SETGID` bounding set, `NoNewPrivs=0`, and
  seccomp mode 0 are present, then execs `/usr/bin/rootlesskit` with command
  `/bin/setpriv --nnp /usr/bin/buildkitd`; BuildKit and every descendant
  therefore have `no_new_privs=1` after the subordinate-ID maps exist;
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
capabilities and the executable foreign-architecture QEMU helper:
`buildkit-qemu-aarch64` in the amd64 image and `buildkit-qemu-x86_64` in the
arm64 image. The fail-closed sidecar launcher is a checked-in trusted-image
source and its preflight marker is captured in bounded sidecar logs. These are
part of trusted image publication evidence, not mutable node prerequisites.
The RootlessKit binary binding is architecture-specific: SHA-256
`79e43c95bb160488b6cb839da16750f7c590fb307b9c2e2d0421dd73fdc557cc`
for amd64 and
`27dfdece833e7ababf64ac5ac37b55b631d614e51e23d2f3505b2881f22c1fce`
for arm64 in the pinned multi-platform base.

## Data and authority flow

1. The client reads the immutable build contract and presigned capabilities.
2. Before reading either capability, it fails unless its own gVisor marker,
   UID/GID 1000, empty inheritable, permitted, effective, bounding, and ambient
   capability sets, `NoNewPrivs=1`, and RuntimeDefault seccomp mode are
   present. It then downloads and verifies the source archive into its private
   workspace.
3. `buildctl` sends the selected source context to BuildKit over the Unix
   socket. The source directory is not mounted into the sidecar.
4. BuildKit executes the untrusted Dockerfile in its rootless subordinate-ID
   namespace behind the KVM gVisor boundary and admission-bound
   NetworkPolicies.
5. The OCI exporter streams the result back over the session to the client's
   bounded destination file. The sidecar does not mount that output path.
6. The client verifies platform metadata, digests, sizes, and archive structure,
   creates the canonical artifact, and uploads it with its one-attempt
   capability.

The shared Pod network is necessary for base-image egress. Exact policies
separate DNS and shared MinIO from public HTTP(S), exclude IPv4 private,
reserved, documentation, benchmark, multicast, and deprecated transition
space, and admit IPv6 only from `2000::/3` after excluding its IETF-special,
documentation, 6to4, and documentation-expansion ranges. IPv4-mapped, NAT64,
Teredo, ULA, link-local, multicast, and unallocated IPv6 therefore cannot turn
public egress into an internal route. The trusted client opens no listener,
and its capability files are absent from the sidecar mount namespace. A build
can exfiltrate its own source or fail its attempt; it cannot obtain the upload
capability or Kubernetes authority.

## Failure and cleanup behavior

The client uses a bounded readiness wait before the first build. Socket
absence, worker-probe failure, daemon exit, build failure, or output
verification failure makes the attempt fail closed. As soon as each Job
completes, the controller reads its single Pod and requires the client to have
exited zero and both client and sidecar restart counts to remain zero before
publication; this happens before the completed-Job TTL can remove an early
platform Pod. Jobs retain `backoffLimit: 0`, the attempt lease fences late
results, and the existing controller removes the ephemeral namespace.
BuildKit state and both temporary volumes are `emptyDir` and disappear with
the Pod.

The runtime rollout remains fail-closed. A new trusted release and issue #1280
window must install the exact RuntimeClass, prove all four gVisor nodes, then
run the two-container conformance. Failure never permits a runc substitution,
single-container fallback, security-context relaxation beyond this exact
sidecar, capacity-ceiling change, or personal acceptance.

## Verification

Repository tests must prove:

- exact PSA privileged enforcement and restricted audit/warn versions;
- the management admission policy binds builder Jobs to the release's trusted
  image, measured RuntimeClass, exact two-container security shape, isolated
  mounts, non-host namespaces, and finite resources;
- every builder Job has one completion and no auxiliary client execution,
  termination-file disclosure, supplemental authority, resource claim, or
  alternate scheduling/controller path;
- Job and Pod-template metadata cannot add finalizers or workload-selecting
  labels, each platform uses its same-lease contract/capability pair, and
  volume sources cannot become optional, remapped, memory-backed, or
  recursively remounted;
- resource and volume quantities use semantic Kubernetes Quantity comparison,
  admitting equivalent forms such as `1000m` for one CPU but no larger value;
- the same policy admits only the exact builder default-deny and egress shapes,
  denies selector/peer/port/CIDR or metadata widening, constrains the delegated
  RoleBinding metadata, and permits cleanup of drifted policies;
- the contract ConfigMap, capability Secret, LimitRange, and ResourceQuota
  retain their immutable, attempt-bound, finite shapes;
- explicit false shared PID namespace and absent service-account token;
- the client retains the restricted security context and is the sole consumer
  of contract, capability, workspace, and output volumes;
- the sidecar has only the two required bounding capabilities, unconfined
  seccomp, privilege escalation, private state/tmp mounts, native-sidecar
  lifecycle, and no authority-bearing mounts;
- the client calls `buildctl` directly at the exact Unix address;
- trusted-image checks bind RootlessKit and both helper file capabilities;
- trusted-image checks bind the target-architecture-specific foreign QEMU
  helper; and
- status, completion, and cleanup logic accept only the exact two-container
  contract with zero restarts.

Protected operational conformance must additionally prove:

- the initial sidecar process is UID/GID 1000 with `CapInh=0`, `CapPrm=0`,
  `CapEff=0`, `CapBnd=SETUID|SETGID`, `CapAmb=0`, and seccomp disabled inside
  gVisor;
- BuildKit and Dockerfile `RUN` observe `NoNewPrivs=1`;
- the client observes zero inheritable, permitted, effective, bounding, and
  ambient capabilities, `NoNewPrivs=1`, and RuntimeDefault seccomp;
- the client cannot observe RootlessKit or BuildKit processes in its `/proc`;
- the sidecar has none of the contract/capability/workspace mounts;
- digest-pinned amd64 and QEMU arm64 `RUN` steps report the expected machines
  and OCI platform metadata; and
- the temporary namespace is removed and all fleet, Longhorn, Secret,
  namespace, Pod-continuity, and executable-capacity-zero invariants still pass.
