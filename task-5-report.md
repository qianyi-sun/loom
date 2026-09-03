# Task 5 report — patched rootless runtime and cgroup-native OCI executor

## Scope

- Worktree: `/home/hongjian/loom/.worktrees/task-image-builder-phase2c-supervisor`
- Branch: `feat/task-image-builder-phase2c-supervisor`
- Starting HEAD: `eb7481ecfec561fa38cb45f923ff2f22c6282a9b`
- Dispatch constraints followed: no subagents, no `.superpowers/**` or `docs/superpowers/**` edits, no push, no PR, no production activation or registry publication.

## Changed files

- Created `deploy/task-image-builder/Dockerfile.rootless-runtime-v2`
- Created `deploy/task-image-builder/rootless-runtime-v2.json`
- Created `tests/ops/test_task_image_builder_rootless_runtime_v2.py`
- Created `cmd/loom-task-image-builder-supervisor/checksum.go`
- Created `cmd/loom-task-image-builder-supervisor/checksum_test.go`
- Created `cmd/loom-task-image-builder-supervisor/download.go`
- Created `cmd/loom-task-image-builder-supervisor/download_test.go`
- Created `cmd/loom-task-image-builder-supervisor/executor.go`
- Created `cmd/loom-task-image-builder-supervisor/executor_test.go`
- Created `cmd/loom-task-image-builder-supervisor/oci.go`
- Created `cmd/loom-task-image-builder-supervisor/oci_test.go`
- Created `cmd/loom-task-image-builder-supervisor/process_linux.go`
- Created `cmd/loom-task-image-builder-supervisor/process_linux_test.go`
- Created `cmd/loom-task-image-builder-supervisor/process_other.go`
- Modified `cmd/loom-task-image-builder-supervisor/config.go`
- Modified `cmd/loom-task-image-builder-supervisor/config_test.go`
- Created this report, `task-5-report.md`

## RED/GREEN TDD evidence

Baseline before Task 5 production changes:

```text
docker run --rm --privileged -v "$PWD:/src:ro" -w /src golang:1.23.4-bookworm go test ./cmd/loom-task-image-builder-supervisor
ok github.com/qianyi-sun/loom/cmd/loom-task-image-builder-supervisor
```

Runtime supply-chain RED:

```text
.venv/bin/pytest -q tests/ops/test_task_image_builder_rootless_runtime_v2.py
4 failed: missing Dockerfile.rootless-runtime-v2 and rootless-runtime-v2.json
```

Runtime supply-chain GREEN:

```text
.venv/bin/pytest -q tests/ops/test_task_image_builder_rootless_runtime_v2.py
4 passed
```

Launch/download/checksum RED:

```text
docker run --rm --privileged -v "$PWD:/src:ro" -w /src golang:1.23.4-bookworm go test ./cmd/loom-task-image-builder-supervisor -run 'Test(Launch|Download|Checksum)'
failed to compile with missing TaskImageBundleFileV1, BundleMetadataSHA256, LaunchInCgroup, and checksum/download symbols
```

Launch/download/checksum GREEN:

```text
docker run --rm --privileged -v "$PWD:/src:ro" -w /src golang:1.23.4-bookworm go test ./cmd/loom-task-image-builder-supervisor -run 'Test(Launch|Download|Checksum)'
ok github.com/qianyi-sun/loom/cmd/loom-task-image-builder-supervisor
```

Executor/OCI RED:

```text
docker run --rm --privileged -v "$PWD:/src:ro" -w /src golang:1.23.4-bookworm go test ./cmd/loom-task-image-builder-supervisor -run 'Test(Executor|OCI|NativeBuild)'
failed to compile with missing BuildPlan, NewExecutor, OCIOutput, ValidateOCIOutput, and OCI descriptor symbols
```

Executor/OCI GREEN:

```text
docker run --rm --privileged -v "$PWD:/src:ro" -w /src golang:1.23.4-bookworm go test ./cmd/loom-task-image-builder-supervisor -run 'Test(Executor|OCI|NativeBuild)'
ok github.com/qianyi-sun/loom/cmd/loom-task-image-builder-supervisor
```

## Runtime artifact evidence

Runtime v2 pins:

- Dockerfile frontend: `docker/dockerfile:1.20@sha256:26147acbda4f14c5add9946e2fd2ed543fc402884fd75146bd342a7f6271dc1d`
- Go toolchain image: `golang:1.26-alpine3.23@sha256:b17af760035fc2f338eed92d448a6c67f2d45438844fc6c60678fa5f99e44b57`
- Go version asserted in-build: `go1.26.7`
- BuildKit base: `moby/buildkit:rootless@sha256:504731e577c20559c00f968f33219f30115e70be29ab96728d1d06e963fc494b`
- BuildKit source commit: `991535e0973488b6a429096d21fa13f81f2d89d8`
- BuildKit source archive SHA-256: `ebc242057b1eee67eb14ead8def52c3770c6793c8c8ac0c53d41983b085360f4`
- RootlessKit source commit: `62d2101fbbe4f79bc845a337c4e868d27ff602c9`
- RootlessKit source archive SHA-256: `51aa4e79847ce9ad48e76a7b824f13ab323b4b90bc13a692e9c8035b8da9340a`
- Dependency override: `go get golang.org/x/crypto@v0.55.0` for BuildKit and RootlessKit source trees only.
- Reproducibility flags: `-trimpath -buildvcs=false`; BuildKit daemon also uses the reviewed static/seccomp build tags.

The final runtime inventory is exactly seven members:

```text
buildctl
buildkitd
buildkit-runc
rootlesskit
rootlessctl
slirp4netns
fuse-overlayfs
```

Fresh amd64 verification command:

```text
docker buildx build --no-cache-filter verified-runtime --platform linux/amd64 -f deploy/task-image-builder/Dockerfile.rootless-runtime-v2 --target verified-runtime --load -t loom-task-image-builder-rootless-runtime-v2:amd64 .
```

Fresh amd64 checksum output:

```text
/runtime/buildctl: OK
/runtime/buildkitd: OK
/runtime/buildkit-runc: OK
/runtime/rootlesskit: OK
/runtime/rootlessctl: OK
/runtime/slirp4netns: OK
/runtime/fuse-overlayfs: OK
```

Fresh arm64 verification command:

```text
docker buildx build --no-cache-filter verified-runtime --platform linux/arm64 -f deploy/task-image-builder/Dockerfile.rootless-runtime-v2 --target verified-runtime --load -t loom-task-image-builder-rootless-runtime-v2:arm64 .
```

Fresh arm64 checksum output:

```text
/runtime/buildctl: OK
/runtime/buildkitd: OK
/runtime/buildkit-runc: OK
/runtime/rootlesskit: OK
/runtime/rootlessctl: OK
/runtime/slirp4netns: OK
/runtime/fuse-overlayfs: OK
```

Recorded member hashes:

| Member | amd64 SHA-256 | arm64 SHA-256 |
| --- | --- | --- |
| `buildctl` | `5c594a04284993e440e7d6fa8f007e15351df0118b3ee2f29e1a1baaf5a23a83` | `c2e0830c20f1122e671d24a5a4fe91b5ab71e8bae80fc650ebbffec3168e155b` |
| `buildkitd` | `85a89191d7dc2ee53a06f54aaf7969df62092602e31b7dfbc008e1b67e6908c4` | `6c117ff680f6b94f28cca78961c6bee3ff7b49fa1e159c2db63322e7f2ecd549` |
| `buildkit-runc` | `b886d74fee2529334f7dcdd75a0a7a9e4935efb5554f96d2cdd26a564aa91c8c` | `1f04f37ef4b2fba6fbbcc13c910b0f94ca067902daa59727edbccf75b5d9d441` |
| `rootlesskit` | `ab942b0add070aa7ff4be0f366b5fd20c6c99a485f071556a59d5564c5d8841b` | `3871394a3917b1c9d12f04f2d2296b9a870fb94be5d3ff812410777a77e922cd` |
| `rootlessctl` | `5f04200c8a5167f73b04b790fe59ebfb7fbffb505521002ef8bdaf254e220a96` | `d415cfe3f60e4cd00a9fc8b20c18dc8b5df99b56a9e1a513715e56ef71e4bf94` |
| `slirp4netns` | `e8d0440de8d8c87072138883bc27cfa02f8b0e8a504badbf335c41f794788cc2` | `fbd8a9cabc716dc53e7c5a00bc7b3e91dbe0eab6b40e6d606b1b34c2ce80cfc0` |
| `fuse-overlayfs` | `1684ef18c337702a0378a4e9942802770c83b11aed6a93c445d43e641a1f3c90` | `34c9995c929dd52f45cca985858d7e58d9a9626104bc2610db218aaa11115c23` |

## Fresh final verification

Formatting:

```text
docker run --rm --privileged -v "$PWD:/src" -w /src golang:1.23.4-bookworm gofmt -w cmd/loom-task-image-builder-supervisor/...
exit 0
```

Runtime static tests:

```text
.venv/bin/pytest -q tests/ops/test_task_image_builder_rootless_runtime_v2.py
4 passed in 0.07s
```

Full supervisor package in privileged pinned Go fixture with the extracted amd64 runtime mounted:

```text
docker run --rm --privileged -v "$PWD:/src:ro" -v "/tmp/loom-task5-runtime.UGK1OH/runtime:/native-runtime:ro" -e LOOM_TASK_IMAGE_BUILDER_NATIVE_RUNTIME=/native-runtime -w /src golang:1.23.4-bookworm go test -v ./cmd/loom-task-image-builder-supervisor
PASS
ok github.com/qianyi-sun/loom/cmd/loom-task-image-builder-supervisor 0.309s
```

Native BuildKit fixture result inside that run:

```text
TestNativeBuildFixtureRequiresExactRuntimeHelpersAndHostPrerequisites
newuidmap unavailable in pinned Go fixture; rootless BuildKit native execution requires subuid mapping helper
SKIP
```

Static checks:

```text
docker run --rm --privileged -v "$PWD:/src:ro" -w /src golang:1.23.4-bookworm go vet ./cmd/loom-task-image-builder-supervisor
exit 0

git diff --check
exit 0
```

## Implementation notes

- `LoadConfig` now binds all seven runtime members exactly: `buildctl`, `buildkitd`, `buildkit-runc`, `rootlesskit`, `rootlessctl`, `slirp4netns`, and `fuse-overlayfs`.
- `LaunchInCgroup` opens the fixed executable with `O_NOFOLLOW`, hashes the fd-backed file before launch, executes via `/proc/self/fd/<fd>`, sets `SysProcAttr.UseCgroupFD=true` and `CgroupFD`, rejects caller authority in env/extra fds, verifies child cgroup identity after start, and treats `ENOSYS` as fixed unsupported-platform failure with no fork/exec fallback.
- `DownloadBundle` accepts the sealed bundle capability, pins TLS 1.3/CA/server name, disables proxy and redirect behavior, creates files with `openat2(RESOLVE_BENEATH|NO_SYMLINKS|NO_MAGICLINKS)`, enforces count/byte quotas, verifies content and metadata hashes, fsyncs materialized files/directories, and unlinks partial files on failure.
- `Executor.Start` launches fixed RootlessKit with `--net=slirp4netns`, host-loopback disabled, IPv6 enabled, sandbox/seccomp auto, fixed `--slirp4netns-binary`, fixed state dir, and fixed rootless BuildKit with the OCI worker, rootless mode, containerd worker disabled, `fuse-overlayfs` snapshotter, fixed `--oci-worker-binary`, process sandboxing enabled, and root/cache paths under the job directory.
- `Executor.Build` invokes the pinned `buildctl` with builtin `dockerfile.v0`, server-derived context/Dockerfile paths under the job directory, `--no-cache`, no cache import/export, and OCI output below the job directory.
- `ValidateOCIOutput` streams the OCI tar while hashing it, rejects links/devices/path escapes/duplicates, validates descriptor digest and size, requires one platform-matched manifest/config, and returns the immutable top-level manifest digest plus tar SHA-256.

## Self-review checklist

- Executable/descriptor leaks: checked. Runtime executables are fixed absolute paths, opened with `O_NOFOLLOW`, hashed before use, launched by fd path, and no `ExtraFiles` are inherited.
- TOCTOU: checked. Config member paths are verified under the content-addressed release root; launch hashes the opened executable fd before exec; bundle writes use `openat2` against the transferred directory fd.
- clone3 fallback: checked. `LaunchInCgroup` maps `ENOSYS` to `ErrCloneIntoCgroupUnsupported` and does not write `cgroup.procs` or retry with ordinary fork/exec.
- Path escape: checked. Bundle paths, build component paths, and OCI tar names reject absolute, dirty, parent-traversing, symlink, magiclink, link, and device entries.
- Quota cleanup: checked. Bundle count/byte quotas are validated before and during materialization; failed downloads unlink created files in reverse order.
- Secret/log leakage: checked. Bundle capabilities are decoded from sealed `SecretBuffer` memory and no production logging of payloads/secrets was added.
- OCI graph confusion: checked. Strict JSON decoding, digest/size validation, exact one-manifest requirement, and platform binding are covered by tests.
- Cache/network escape: checked. Build plan rejects host networking, insecure entitlements, devices, CDI, SSH, arbitrary binds, remote frontends, and cache import/export; buildctl argv contains no cache import/export or host socket authority.
- Processes outside cgroup: checked. Launch verifies child cgroup identity; executor close sends SIGTERM, escalates to bounded kill, and fails if the build-egress cgroup is not empty.
- Nondeterministic supply-chain inputs: checked. Dockerfile syntax, Go image, BuildKit base, BuildKit/RootlessKit archives, slirp4netns/fuse-overlayfs downloads, Go version, x/crypto override, build flags, version metadata, and per-arch binary hashes are pinned and verified.

## Concerns / blocker evidence

The dispatch-required real native no-cache RootlessKit/BuildKit build could not be honestly completed in the pinned Go fixture because the fixture lacks rootless subuid/subgid mapping prerequisites:

```text
newuidmap unavailable in pinned Go fixture; rootless BuildKit native execution requires subuid mapping helper
```

Additional manual probes during the task showed the extracted runtime helpers themselves were present and executable, `/dev/fuse` and user namespaces were available, but rootless startup as root failed with:

```text
No subuid ranges found for user 0 ("root")
```

A non-root static subuid attempt failed with:

```text
fork/exec /proc/self/exe: operation not permitted
```

Therefore the implementation, static policy gates, OCI validation, runtime checksum verification, and full Go package tests are green, but a real native amd64 no-cache BuildKit build remains blocked on providing a pinned fixture with `newuidmap`/`newgidmap` and usable non-root subuid/subgid ranges. No emulation was substituted for that proof.
