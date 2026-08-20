# Personal-development Rootless BuildKit Sidecar Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> `superpowers:subagent-driven-development` (recommended) or
> `superpowers:executing-plans` to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the contradictory single-container RootlessKit contract with
an authority-free native BuildKit sidecar while keeping the capability-bearing
client fully restricted.

**Architecture:** Each target-platform Job has a restricted regular client and
a restartable init-container BuildKit sidecar in separate PID and mount
namespaces. The client alone owns the contract, presigned capabilities,
workspace, verification, and upload; the sidecar owns only disposable BuildKit
state and the shared Unix socket. The KVM gVisor RuntimeClass remains the host
boundary. Because Kubernetes Baseline rejects explicit seccomp `Unconfined`,
the dynamic build namespace uses PSA privileged enforcement with restricted
audit and warning plus an exact Loom admission contract for builder Jobs and
their network policies.

**Tech Stack:** Python 3.11, pytest, Kubernetes 1.36 native sidecars,
ValidatingAdmissionPolicy CEL, gVisor/runsc, RootlessKit 3.0.1, BuildKit 0.32.2,
Bash, jq, Docker/OCI.

**Spec:** `docs/architecture/personal-dev-builder-rootless-sidecar.md`

## Global constraints

- The global executable-new-capacity ceiling remains exactly `0` throughout
  implementation and release work.
- No personal task, Slurm job, database/DNS/Secret value, physical capacity,
  or personal lifecycle mutation is authorized by this plan.
- The client container remains UID/GID 1000, RuntimeDefault-seccomp,
  `allowPrivilegeEscalation=false`, read-only-rootfs, and capability-free.
- The sidecar is non-privileged and gets only `SETUID` and `SETGID` in its
  capability bounding set; its initial inheritable, permitted, effective, and
  ambient sets must remain empty.
- The sidecar receives no contract, Secret, source workspace, output path,
  service-account token, projected volume, CSI volume, or image-pull Secret.
- PSA privileged is permitted only in exact attempt-bound builder namespaces
  and the temporary operator smoke namespace; personal namespaces remain
  restricted.
- Every builder Job is admission-bound to the trusted image, measured
  RuntimeClass, non-host namespaces, exact client/sidecar security contexts,
  isolated mounts, one completion, no auxiliary execution path, and finite
  resources compared with Kubernetes Quantity semantics.
- Builder NetworkPolicies are admission-bound to exact selectors, DNS and
  shared-MinIO peers, public HTTP(S) ports, and private/reserved-network
  exclusions; their deletion remains shape-independent for safe cleanup.
- Builder ConfigMaps, Secrets, LimitRanges, and ResourceQuotas are
  admission-bound to immutable, attempt-named, finite shapes; their deletion
  remains shape-independent.
- RootlessKit runs BuildKit through `/bin/setpriv --nnp`; BuildKit and every
  Dockerfile `RUN` descendant must observe `NoNewPrivs=1`.
- `shareProcessNamespace` is explicitly false and `hostUsers` remains absent.
- Process-sandbox disabling is allowed only in the authority-free sidecar.
- No runc fallback or unmeasured RuntimeClass is permitted.
- Plans and designs stay under `docs/architecture`; do not recreate
  `docs/superpowers`.

---

### Task 1: Render the exact two-container security and admission contract

**Files:**

- Modify: `tests/unit/test_personal_dev_builder_manifest.py`
- Modify: `tests/unit/test_personal_dev_control_plane_render.py`
- Modify: `tests/unit/test_personal_dev_control_plane_status.py`
- Modify: `src/loom/personal_dev_builder_manifest.py`
- Modify: `src/loom/personal_dev_control_plane_render.py`
- Modify: `src/loom/personal_dev_control_plane_status.py`

**Interfaces:**

- Consumes: `PersonalDevBuilderManifestConfig.builder_image` and the existing
  attempt-bound namespace/Job naming contract.
- Produces: a Job whose regular container is `builder`, whose restartable init
  container is `buildkitd`, and whose socket address is
  `unix:///var/run/loom-buildkit/buildkitd.sock`.
- Produces: `_dynamic_namespace_valid()` behavior that accepts restricted
  personal namespaces and only exact privileged/restricted-versioned builder
  namespaces.

- [ ] **Step 1: Write failing manifest tests for authority separation**

Extend `test_builder_manifest_is_attempt_bound_restricted_and_finite` and add
`test_buildkit_sidecar_has_only_rootless_startup_authority` with assertions
equivalent to:

```python
labels = namespace["metadata"]["labels"]
assert labels | {
    "pod-security.kubernetes.io/enforce": "privileged",
    "pod-security.kubernetes.io/enforce-version": "v1.36",
    "pod-security.kubernetes.io/audit": "restricted",
    "pod-security.kubernetes.io/audit-version": "v1.36",
    "pod-security.kubernetes.io/warn": "restricted",
    "pod-security.kubernetes.io/warn-version": "v1.36",
} == labels

spec = job["spec"]["template"]["spec"]
assert spec["shareProcessNamespace"] is False
assert spec["automountServiceAccountToken"] is False
assert len(spec["containers"]) == 1
assert len(spec["initContainers"]) == 1

client = spec["containers"][0]
assert client["name"] == "builder"
assert client["securityContext"]["allowPrivilegeEscalation"] is False
assert client["securityContext"]["capabilities"] == {"drop": ["ALL"]}
assert {mount["name"] for mount in client["volumeMounts"]} == {
    "contract", "attempt-capability", "workspace", "tmp-client", "buildkit-run"
}

sidecar = spec["initContainers"][0]
assert sidecar["name"] == "buildkitd"
assert sidecar["restartPolicy"] == "Always"
assert sidecar["command"] == ["/usr/local/bin/loom-personal-dev-buildkitd"]
assert "args" not in sidecar
assert sidecar["securityContext"] == {
    "allowPrivilegeEscalation": True,
    "capabilities": {"drop": ["ALL"], "add": ["SETGID", "SETUID"]},
    "readOnlyRootFilesystem": True,
    "runAsNonRoot": True,
    "seccompProfile": {"type": "Unconfined"},
}
assert {mount["name"] for mount in sidecar["volumeMounts"]} == {
    "buildkit-run", "buildkit-state", "tmp-buildkit"
}
assert not ({"contract", "attempt-capability", "workspace"} & {
    mount["name"] for mount in sidecar["volumeMounts"]
})
```

Also assert that both containers use the exact same immutable image, the socket
mount is read-only in the client and writable in the sidecar, the sidecar
startup probe calls `/usr/bin/buildctl --addr
unix:///var/run/loom-buildkit/buildkitd.sock debug workers`, and no environment
entry contains a Secret reference.

- [ ] **Step 2: Write failing admission and status tests**

In `test_personal_dev_control_plane_render.py`, require the namespace CEL to
select exact security labels by family:

```python
assert "startsWith('loom-dev-')" in expression
assert "pod-security.kubernetes.io/enforce'] == 'restricted'" in expression
assert "startsWith('loom-build-')" in expression
assert "pod-security.kubernetes.io/enforce'] == 'privileged'" in expression
assert "pod-security.kubernetes.io/enforce-version'] == 'v1.36'" in expression
assert "pod-security.kubernetes.io/audit-version'] == 'v1.36'" in expression
assert "pod-security.kubernetes.io/warn-version'] == 'v1.36'" in expression
```

In `test_personal_dev_control_plane_status.py`, change the healthy builder
namespace fixture to the exact six PSA labels and add mutations for privileged,
audit/warn, and version drift. Personal namespace fixtures must remain
restricted and continue passing.

- [ ] **Step 3: Run the focused tests and verify RED**

Run:

```bash
PYTHONPATH=src:. /home/hongjian/loom/.venv/bin/pytest \
  tests/unit/test_personal_dev_builder_manifest.py \
  tests/unit/test_personal_dev_control_plane_render.py \
  tests/unit/test_personal_dev_control_plane_status.py -q
```

Expected: failures show the existing restricted builder namespace, absent
native sidecar, absent explicit PID isolation, unconditional restricted CEL,
and status rejection of privileged builder namespaces.

- [ ] **Step 4: Implement the minimal manifest contract**

In `personal_dev_builder_manifest.py`:

- add exact `v1.36` PSA labels;
- set `shareProcessNamespace: False`;
- keep the regular client security context unchanged;
- add the restartable `buildkitd` init container with the exact launcher,
  security context, startup probe, and private mounts described above;
- add `buildkit-run`, `buildkit-state`, `tmp-client`, and `tmp-buildkit`
  `emptyDir` volumes with finite `sizeLimit` values;
- replace the client's `tmp` mount with `tmp-client` and add a read-only socket
  mount; and
- give the sidecar an explicit finite resource envelope without increasing the
  existing per-container 4 CPU / 8 GiB ceiling.

The sidecar command must be:

```python
["/usr/local/bin/loom-personal-dev-buildkitd"]
```

- [ ] **Step 5: Implement family-specific admission and status validation**

Change the namespace policy validation to one parenthesized CEL expression:

```text
(personal-family && enforce == 'restricted') ||
(builder-family && enforce == 'privileged' && enforce-version == 'v1.36' &&
 audit == 'restricted' && audit-version == 'v1.36' &&
 warn == 'restricted' && warn-version == 'v1.36')
```

Change `_dynamic_namespace_valid()` so the personal branch retains restricted
enforcement while the builder branch requires all six exact labels. Do not
make builder labels acceptable to personal namespaces or vice versa.

- [ ] **Step 6: Run focused tests and verify GREEN**

Run the Step 3 command. Expected: all selected tests pass with no warning.

- [ ] **Step 7: Commit Task 1**

```bash
git add src/loom/personal_dev_builder_manifest.py \
  src/loom/personal_dev_control_plane_render.py \
  src/loom/personal_dev_control_plane_status.py \
  tests/unit/test_personal_dev_builder_manifest.py \
  tests/unit/test_personal_dev_control_plane_render.py \
  tests/unit/test_personal_dev_control_plane_status.py
git commit -m "fix(dev): isolate rootless buildkit authority"
```

---

### Task 2: Make the trusted client use the sidecar socket

**Files:**

- Modify: `tests/unit/test_personal_dev_sandbox_builder.py`
- Modify: `src/loom/personal_dev_sandbox_builder.py`

**Interfaces:**

- Consumes: absolute `/usr/bin/buildctl` and address
  `unix:///var/run/loom-buildkit/buildkitd.sock`.
- Produces: `_build_images(..., buildctl_path: Path, buildkit_address: str)` and
  matching `run_personal_dev_sandbox_build()`/CLI parameters.
- Removes: daemon ownership and `BUILDKITD_FLAGS`/`XDG_RUNTIME_DIR` from the
  capability-bearing client.

- [ ] **Step 1: Write a failing direct-client command test**

Import `_build_images` in `test_personal_dev_sandbox_builder.py`. Use the real
contract and real small OCI fixture, but monkeypatch only `subprocess.run` to
record each command and write the fixture to the `dest=` output. Assert every
command starts with:

```python
[
    "/usr/bin/buildctl",
    "--addr=unix:///var/run/loom-buildkit/buildkitd.sock",
    "build",
]
```

Assert no command or environment contains `buildctl-daemonless`, `BUILDKITD`,
`BUILDKITD_FLAGS`, `ROOTLESSKIT`, or `XDG_RUNTIME_DIR`. Add rejection tests for
a relative buildctl path and any address other than the exact Unix address.
Add a client-identity test using temporary marker/status files that requires
UID/GID 1000, `CapInh=0`, `CapPrm=0`, `CapEff=0`, `CapBnd=0`, `CapAmb=0`,
`NoNewPrivs=1`, and seccomp mode 2, and rejects each drift independently.

- [ ] **Step 2: Run the client tests and verify RED**

```bash
PYTHONPATH=src:. /home/hongjian/loom/.venv/bin/pytest \
  tests/unit/test_personal_dev_sandbox_builder.py -q
```

Expected: `_build_images` has the old daemonless parameter and command.

- [ ] **Step 3: Implement direct Buildctl invocation**

Add a fail-closed client identity check before either capability file is read.
The check must require `/proc/gvisor/kernel_is_gvisor`, UID/GID 1000, empty
inheritable, permitted, effective, bounding, and ambient capability sets,
`NoNewPrivs=1`, and seccomp mode 2. Keep its parser independently testable with
explicit marker/status inputs.

Rename the parameter and CLI option to `buildctl_path` /
`--buildctl-path`; add `buildkit_address` / `--buildkit-address`; validate the
path is absolute and the address equals the exact constant. Invoke:

```python
[
    str(buildctl_path),
    f"--addr={buildkit_address}",
    "build",
    # existing frontend, local, platform, label, and OCI output arguments
]
```

Retain finite timeout, closed stdin, suppressed child output, real OCI
verification, size accounting, canonical artifact creation, and upload.

- [ ] **Step 4: Run the client tests and verify GREEN**

Run the Step 2 command. Expected: all selected tests pass.

- [ ] **Step 5: Commit Task 2**

```bash
git add src/loom/personal_dev_sandbox_builder.py \
  tests/unit/test_personal_dev_sandbox_builder.py
git commit -m "fix(dev): connect builder client to isolated daemon"
```

---

### Task 3: Bind the trusted image's RootlessKit prerequisites

**Files:**

- Modify: `tests/ops/test_personal_dev_control_plane_package_boundary.py`
- Modify: `deploy/Dockerfile.personal-dev-builder`
- Create: `deploy/personal-dev-builder/loom-personal-dev-buildkitd`
- Modify: `config/component-ownership.toml`

**Interfaces:**

- Consumes: immutable `moby/buildkit:rootless` base digest already checked in.
- Produces: build-time failure unless `/usr/bin/buildctl`, RootlessKit `3.0.1`,
  the architecture-bound RootlessKit SHA-256 (`amd64`:
  `79e43c95bb160488b6cb839da16750f7c590fb307b9c2e2d0421dd73fdc557cc`;
  `arm64`:
  `27dfdece833e7ababf64ac5ac37b55b631d614e51e23d2f3505b2881f22c1fce`),
  BusyBox `setpriv`, and the two exact helper file-capability xattrs exist.
- Produces: `/usr/local/bin/loom-personal-dev-buildkitd`, a checked-in launcher
  that validates its exact gVisor/security identity before execing RootlessKit.
- Produces: build-time failure unless the amd64 image contains
  `buildkit-qemu-aarch64` and the arm64 image contains
  `buildkit-qemu-x86_64`, so either native image can build the other target.

- [ ] **Step 1: Write failing Dockerfile contract assertions**

Add an ops test that requires the Dockerfile to bind:

```python
assert "rootlesskit version 3.0.1" in dockerfile
assert "79e43c95bb160488b6cb839da16750f7c590fb307b9c2e2d0421dd73fdc557cc" in dockerfile
assert "27dfdece833e7ababf64ac5ac37b55b631d614e51e23d2f3505b2881f22c1fce" in dockerfile
assert "ARG TARGETARCH" in dockerfile
assert "/bin/setpriv" in dockerfile
assert "/usr/bin/newuidmap" in dockerfile
assert "0100000280000000000000000000000000000000" in dockerfile
assert "/usr/bin/newgidmap" in dockerfile
assert "0100000240000000000000000000000000000000" in dockerfile
assert "COPY deploy/personal-dev-builder/loom-personal-dev-buildkitd" in dockerfile
```

Add launcher assertions that require the gVisor marker, UID/GID 1000,
`CapInh=0000000000000000`, `CapPrm=0000000000000000`,
`CapEff=0000000000000000`, `CapBnd=00000000000000c0`,
`CapAmb=0000000000000000`, `NoNewPrivs=0`, `Seccomp=0`, `/bin/setpriv --nnp`,
the exact BuildKit socket, native snapshotter, and no-process-sandbox flag.
Execute the launcher in a shell test with fixture commands/status so each drift
fails before RootlessKit exec.

- [ ] **Step 2: Run the ops test and verify RED**

```bash
PYTHONPATH=src:. /home/hongjian/loom/.venv/bin/pytest \
  tests/ops/test_personal_dev_control_plane_package_boundary.py -q
```

Expected: the inherited binaries and xattrs are not yet bound.

- [ ] **Step 3: Add immutable build-time checks**

Create the launcher with static commands only:

```sh
exec /usr/bin/rootlesskit /bin/setpriv --nnp /usr/bin/buildkitd \
  --addr=unix:///var/run/loom-buildkit/buildkitd.sock \
  --oci-worker-no-process-sandbox \
  --oci-worker-snapshotter=native
```

Before that exec, require the exact gVisor/status values above, create private
`HOME` and `XDG_RUNTIME_DIR` directories, and print one bounded
`loom-buildkitd-preflight` marker without environment contents.

Extend the existing root `RUN` instruction without adding packages. Use
`sha256sum`, exact `rootlesskit --version` output, `test -x /bin/setpriv`, and
Python `os.getxattr(path, "security.capability").hex()` comparisons for both
helpers. Copy the launcher as root-owned mode 0555, add it to the component's
`source_paths`, and keep the final image user exactly `1000:1000`.

- [ ] **Step 4: Run the ops test and build the image**

```bash
PYTHONPATH=src:. /home/hongjian/loom/.venv/bin/pytest \
  tests/ops/test_personal_dev_control_plane_package_boundary.py -q
docker build --file deploy/Dockerfile.personal-dev-builder \
  --tag loom-personal-dev-builder:rootless-sidecar-test .
```

Expected: the test and image build pass, and the final image still declares
UID/GID `1000:1000`.

- [ ] **Step 5: Commit Task 3**

```bash
git add config/component-ownership.toml \
  deploy/Dockerfile.personal-dev-builder \
  deploy/personal-dev-builder/loom-personal-dev-buildkitd \
  tests/ops/test_personal_dev_control_plane_package_boundary.py
git commit -m "fix(dev): bind rootless builder prerequisites"
```

---

### Task 4: Replace operational conformance with the exact sidecar proof

**Files:**

- Modify: `tests/ops/test_personal_dev_control_plane_package_boundary.py`
- Modify: `docs/runbooks/personal-dev-builder-runtime.md`
- Modify: `docs/architecture/personal-dev-builder-runtime.md`
- Modify: `docs/architecture/personal-dev-builder-rootless-sidecar.md`

**Interfaces:**

- Consumes: the exact image digest from the next trusted release and the
  existing measured RuntimeClass/fleet profile.
- Produces: a temporary two-container conformance Pod and owner-only evidence
  for both containers, both target architectures, NNP inheritance, mount
  separation, and PID isolation.

- [ ] **Step 1: Write failing runbook structure and parser tests**

Require the runbook to contain all of these exact contracts:

```python
assert '"pod-security.kubernetes.io/enforce": "privileged"' in runbook
assert '"pod-security.kubernetes.io/audit": "restricted"' in runbook
assert 'restartPolicy: "Always"' in runbook
assert 'command: ["/usr/local/bin/loom-personal-dev-buildkitd"]' in runbook
assert 'capabilities: {drop: ["ALL"], add: ["SETGID", "SETUID"]}' in runbook
assert 'seccompProfile: {type: "Unconfined"}' in runbook
assert 'shareProcessNamespace: false' in runbook
assert '/usr/bin/buildctl' in runbook
assert '/usr/bin/buildctl-daemonless.sh' not in runbook
assert 'loom-nnp=1' in runbook
```

Extend the existing embedded Bash/Python/jq parser test to render the
conformance Pod and assert the sidecar has no `script`, `workspace`,
`attempt-capability`, or contract mount while the client has no state/tmp
sidecar mount.

- [ ] **Step 2: Run the ops tests and verify RED**

Run the Task 3 Step 2 command. Expected: old single-container conformance and
restricted smoke namespace assertions fail.

- [ ] **Step 3: Update the rollout smoke namespace**

Change only the temporary operator-owned smoke namespace to privileged
enforcement at `v1.36`, retaining restricted audit and warning at `v1.36`.
Update `assert_smoke_namespace_owned()` to require the exact labels. The four
simple node-pinned gVisor probes retain their restricted container contexts.

- [ ] **Step 4: Render and verify the two-container conformance Pod**

Replace the daemonless command with a regular `conformance` client plus native
`buildkitd` sidecar. The client mounts the ConfigMap, workspace, private tmp,
and the socket read-only; the sidecar mounts only socket, state, and private
tmp. Set the sidecar launcher/security context exactly as Task 1. Set the client
to `allowPrivilegeEscalation=false`, drop all capabilities, read-only rootfs,
and RuntimeDefault seccomp. Set `shareProcessNamespace=false` explicitly.

The conformance script must:

- require the gVisor marker and UID/GID 1000;
- require client `CapInh=0`, `CapPrm=0`, `CapEff=0`, `CapBnd=0`, `CapAmb=0`,
  `NoNewPrivs=1`, and seccomp mode 2;
- prove no `/proc/*/cmdline` exposes RootlessKit or BuildKit;
- call `/usr/bin/buildctl --addr=unix:///var/run/loom-buildkit/buildkitd.sock`;
- run digest-pinned amd64 and QEMU arm64 Dockerfile steps;
- make each `RUN` require and print `NoNewPrivs=1`; and
- retain exact OCI config platform verification.

Capture bounded logs for both `container/conformance` and
`container/buildkitd`, exact image IDs, init/regular container statuses, and
the canonical live Pod. Require the launcher's bounded preflight marker to
record UID/GID 1000, zero inheritable, permitted, effective, and ambient sets,
bounding set `00000000000000c0`, NNP 0, and seccomp mode 0 before accepting the
build logs.

- [ ] **Step 5: Align architecture documentation**

Remove claims that the one trusted wrapper both holds authority and runs
daemonless BuildKit. Document why explicit seccomp `Unconfined` requires PSA
privileged, how the Loom admission policy replaces Baseline with an exact
application-specific boundary, the authority-free sidecar, the exact helper
capability mechanism, NNP after mapping, separate PID/mount namespaces, and
the unchanged KVM gVisor host boundary.

- [ ] **Step 6: Run the ops tests and verify GREEN**

Run the Task 3 Step 2 command. Expected: all ops package-boundary tests pass,
including Bash, Python, and jq parsing.

- [ ] **Step 7: Commit Task 4**

```bash
git add docs/architecture/personal-dev-builder-runtime.md \
  docs/architecture/personal-dev-builder-rootless-sidecar.md \
  docs/runbooks/personal-dev-builder-runtime.md \
  tests/ops/test_personal_dev_control_plane_package_boundary.py
git commit -m "docs(dev): prove isolated rootless sidecar"
```

---

### Task 5: Correct the PSA and application-specific admission boundary

**Files:**

- Modify: `tests/unit/test_personal_dev_builder_manifest.py`
- Modify: `tests/unit/test_personal_dev_control_plane_render.py`
- Modify: `tests/unit/test_personal_dev_control_plane_status.py`
- Modify: `tests/ops/test_personal_dev_control_plane_package_boundary.py`
- Modify: `src/loom/personal_dev_builder_manifest.py`
- Modify: `src/loom/personal_dev_control_plane_render.py`
- Modify: `src/loom/personal_dev_control_plane_status.py`
- Modify: `docs/architecture/personal-dev-builder-runtime.md`
- Modify: `docs/runbooks/personal-dev-builder-runtime.md`

**Interfaces:**

- Consumes: `release.images.personal_dev_builder` and the runtime class from
  the shadow profile or exact acceptance plan.
- Produces: privileged/restricted-versioned builder namespace labels and
  management admission validations that bind every builder Job and both
  builder NetworkPolicies to their exact contracts.

- [ ] **Step 1: Write failing regression tests for the real PSA boundary**

Change the manifest, render, status, and runbook expectations from Baseline to
privileged at `v1.36`. Add render assertions requiring the builder-Job
validation to contain the literal trusted builder image and measured runtime
class and to constrain all of these observable fields:

```python
assert "spec.template.spec.runtimeClassName" in builder_contract
assert release.images.personal_dev_builder in builder_contract
assert profile.builder.runtime_class_name in builder_contract
assert "spec.template.spec.containers.size() == 1" in builder_contract
assert "spec.template.spec.initContainers.size() == 1" in builder_contract
assert "shareProcessNamespace == false" in builder_contract
assert "automountServiceAccountToken == false" in builder_contract
assert "enableServiceLinks == false" in builder_contract
assert "seccompProfile.type == 'Unconfined'" in builder_contract
assert "capabilities.add == ['SETGID','SETUID']" in builder_contract
assert "readOnly == true" in builder_contract
```

Also require non-host network/PID/IPC, absent `hostUsers`, one restricted
client, one native sidecar, exact commands, no sidecar environment or
authority-bearing mounts, exact emptyDir/configMap/Secret volume families,
and explicit finite resource requests and limits.

Require one non-indexed completion and no alternate Job controller,
failure/success policy, client lifecycle hook, extra client probe,
termination-message path, resource claim, supplemental group, or alternate Pod
scheduler. Resource-map and `emptyDir` quantities must use
`quantity(string(value)).compareTo(quantity(expected)) == 0`; direct Quantity
equality does not accept the exact Kubernetes 1.36 object.

Bind Job and Pod-template metadata so finalizers, owner references, generated
names, or extra workload-selecting labels cannot widen or wedge the exception.
Couple each Job platform to the same-lease contract and capability volume.
Forbid ConfigMap/Secret item remapping or optionality, alternate `emptyDir`
media, and recursive-read-only mount overrides.

Require the supporting ConfigMap and Secret to be immutable, same-lease and
same-platform named, exact-keyed, bounded, and metadata constrained. Require
the exact LimitRange container defaults and ResourceQuota hard counts,
including three ConfigMaps so Kubernetes' injected `kube-root-ca.crt` and both
platform contract ConfigMaps can coexist. These support-object shape
validations must allow `DELETE`.

Require separate builder-only validations for the exact `default-deny` and
`builder-egress` NetworkPolicy shapes. The latter must bind the sandbox Pod
selector, DNS and shared-MinIO selectors and ports, both public IP blocks,
HTTP(S) ports, and every private/reserved CIDR exclusion. NetworkPolicy and
delegated RoleBinding metadata must reject annotations, finalizers, owner
references, generated names, and extra labels. Shape validations must allow
`DELETE` so higher-authority drift cannot wedge cleanup.

Require the builder Namespace's non-delete metadata to reject annotations,
finalizers, owner references, generated names, and extra labels; permit only
the exact attempt/PSA labels plus Kubernetes' matching injected Namespace-name
label, and couple the attempt UUID label to the Namespace name. Namespace
deletion must remain shape-independent so higher-authority drift cannot wedge
cleanup.

- [ ] **Step 2: Run the focused tests and verify RED**

```bash
PYTHONPATH=src:. .venv/bin/pytest \
  tests/unit/test_personal_dev_builder_manifest.py \
  tests/unit/test_personal_dev_control_plane_render.py \
  tests/unit/test_personal_dev_control_plane_status.py \
  tests/ops/test_personal_dev_control_plane_package_boundary.py -q
```

Expected: the old Baseline labels fail, the builder Job has no exact custom
admission validation, and service links are not explicitly disabled.

- [ ] **Step 3: Implement the minimal exact boundary**

Set builder and smoke namespaces to PSA privileged enforcement pinned to
`v1.36`, retaining restricted audit/warn at `v1.36`. Set
`enableServiceLinks: false` on builder Pods. Pass the exact trusted builder
image and active runtime class into `_management_resource_admission()` and add
builder-only CEL validations for the Job and NetworkPolicy fields listed in
Step 1. Keep the existing Secret and resource-family validations and all
personal-namespace behavior unchanged.

- [ ] **Step 4: Run the focused tests and verify GREEN**

Run the Step 2 command. Expected: all selected tests pass.

- [ ] **Step 5: Compile the policy against Kubernetes without mutation**

Use server-side dry-run against the protected cluster to compile the rendered
ValidatingAdmissionPolicy and dry-run the exact builder namespace and Job. A
builder Namespace annotation, finalizer, owner reference, generated name, extra
label, or mismatched attempt label and a mutated privileged container, host
namespace, image, RuntimeClass, mount, or sidecar Secret reference must be
denied. Also admit the exact two generated
NetworkPolicies, deny selector, internal-peer, public-exclusion, and
default-deny widening, and prove that deletion remains possible after an admin
drifts a policy. In a disposable Kubernetes 1.36 cluster, admit the exact Job
and equivalent `1000m` CPU quantity while denying repeated completions, client
hooks/probes/termination-file disclosure, sidecar hooks, supplemental root
groups, alternate scheduling, extended termination grace, a widened resource
request, volume-source remapping/optionality/media changes, platform-volume
cross-wiring, recursive mount changes, finalizers, and extra Pod labels. No
protected-cluster object may be persisted.

- [ ] **Step 6: Commit Task 5**

```bash
git add src/loom/personal_dev_builder_manifest.py \
  src/loom/personal_dev_control_plane_render.py \
  src/loom/personal_dev_control_plane_status.py \
  tests/unit/test_personal_dev_builder_manifest.py \
  tests/unit/test_personal_dev_control_plane_render.py \
  tests/unit/test_personal_dev_control_plane_status.py \
  tests/ops/test_personal_dev_control_plane_package_boundary.py \
  docs/architecture/personal-dev-builder-rootless-sidecar.md \
  docs/architecture/personal-dev-builder-rootless-sidecar-implementation-plan.md \
  docs/architecture/personal-dev-builder-runtime.md \
  docs/runbooks/personal-dev-builder-runtime.md
git commit -m "fix(dev): bind privileged builder admission"
```

---

### Task 6: Verify the implementation and prepare the protected release

**Files:**

- Modify only if verification exposes a scoped defect in Tasks 1–4.

**Interfaces:**

- Consumes: all Task 1–5 commits.
- Produces: fresh test, local image, local rootless two-container, review, and
  protected-PR evidence. It does not produce a live cluster rollout from an
  unmerged commit.

- [ ] **Step 1: Run formatting and focused verification**

```bash
git diff --check origin/dev...HEAD
PYTHONPATH=src:. /home/hongjian/loom/.venv/bin/pytest \
  tests/unit/test_personal_dev_builder_manifest.py \
  tests/unit/test_personal_dev_sandbox_builder.py \
  tests/unit/test_personal_dev_control_plane_render.py \
  tests/unit/test_personal_dev_control_plane_status.py \
  tests/ops/test_personal_dev_control_plane_package_boundary.py -q
```

- [ ] **Step 2: Run the full ops and unit suites**

```bash
PYTHONPATH=src:. /home/hongjian/loom/.venv/bin/pytest tests/ops -q
PYTHONPATH=src:. /home/hongjian/loom/.venv/bin/pytest tests/unit -q
```

Expected: zero failures. Record exact pass counts from fresh output.

- [ ] **Step 3: Run local two-container rootless conformance**

Build `loom-personal-dev-builder:rootless-sidecar-test`. Start one daemon
container as UID/GID 1000, non-privileged, cap-drop ALL plus SETUID/SETGID,
seccomp/AppArmor unconfined, read-only-rootfs, with only private state/tmp and a
named socket volume. Start a separate no-new-privileges, cap-drop ALL,
read-only-rootfs client container with the socket volume read-only. Require:

- daemon outer `CapInh=0`, `CapPrm=0`, `CapEff=0`,
  `CapBnd=00000000000000c0`, `CapAmb=0`, and `NoNewPrivs=0`;
- BuildKit worker readiness;
- client `CapInh=0`, `CapPrm=0`, `CapEff=0`, `CapBnd=0`, `CapAmb=0`, and
  `NoNewPrivs=1`;
- client cannot see `rootlesskit` or `buildkitd` in `/proc`;
- digest-pinned amd64 and arm64 `RUN` steps both print `loom-nnp=1`; and
- both OCI configs carry the requested platform.

Use exact named diagnostic containers/volumes, stop them in a trap, and remove
only those validated names after logs are captured.

- [ ] **Step 4: Perform iterative self-review**

Review the full `origin/dev...HEAD` diff against every spec requirement and
threat boundary. Check for sidecar Secret/workspace leakage, mutable image
references, broad capabilities, PSA family confusion, missing finite limits,
unparsed runbook programs, and claims without executable evidence. Correct any
finding with a new failing test and rerun Steps 1–3. Repeat until one complete
review finds no actionable problem.

- [ ] **Step 5: Push and open a PR**

```bash
git push --set-upstream origin feat/personal-dev-rootless-sidecar
gh pr create --repo qianyi-sun/loom --base dev \
  --head feat/personal-dev-rootless-sidecar \
  --title "fix(dev): isolate rootless BuildKit authority" \
  --body $'Fixes #1280\n\nReplaces the contradictory single-container RootlessKit contract with an authority-free native BuildKit sidecar. The capability-bearing client remains restricted; the sidecar has only the two ID-mapping capabilities inside KVM gVisor.\n\nVerification evidence and the protected-rollout boundary are recorded in the final review comment.'
```

The PR body must link issue #1280, state the reproduced two-stage root cause,
explain why the sidecar is safer than a broadened single container, report
fresh test/local-conformance evidence, and state that the cluster remains
rolled back at capacity ceiling zero.

- [ ] **Step 6: Require protected gates and a new trusted release**

Do not merge around a failure. After the exact head passes every required
check, merge normally, verify the squash patch identity, wait for the protected
personal-dev image publication, and verify every required job plus the exact
trusted-release and evidence hashes. Only that merged release can bind a fresh
issue #1280 runtime window.

- [ ] **Step 7: Repeat the protected runtime rollout**

Use `docs/runbooks/personal-dev-builder-runtime.md` from the exact merged
commit. Re-establish all stop conditions, keep the ceiling zero, roll OLDLAB
agents sequentially, and require the new two-container amd64/arm64 conformance.
On any failure, preserve evidence and execute the exact reverse rollback. Only
after conformance, namespace cleanup, and all final invariants pass may the
separate management-shadow and zero-capacity acceptance work resume.
