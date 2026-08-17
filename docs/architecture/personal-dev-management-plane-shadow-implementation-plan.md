# Personal Development Management-Plane Shadow Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> `superpowers:subagent-driven-development` (recommended) or
> `superpowers:executing-plans` to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deliver a deterministic, digest-pinned, render-only shadow deployment
and read-only status command for the personal-development management plane in
`loom-dev`, with personal mutation disabled and no physical-capacity authority.

**Architecture:** A strict TOML profile and owner-only trusted-release binding
feed a pure Python YAML renderer. The renderer emits only shared storage,
migration, management service, inert builder/activation authority, RBAC, and
NetworkPolicies. A separate credential copier prepares bounded file-shaped
Secrets, and a read-only status observer proves the rendered shadow contract
against Kubernetes and the zero-ceiling global manager.

**Tech Stack:** Python 3.11, dataclasses and Pydantic v2, `tomllib`, PyYAML,
Kubernetes YAML, argparse, pytest, Ruff, mypy, GitHub protected image releases.

## Global Constraints

- `loom-dev` is the only shared infrastructure namespace; never create
  `loom-dev-shared`.
- Personal application namespaces are `loom-dev-<owner>` and are not rendered
  by this package.
- The shadow package sets `LOOM_SVC_DEV_INSTANCES_ENABLED=false`,
  `LOOM_SVC_PERSONAL_DEV_BUILDER_ENABLED=false`, and activation-agent replicas
  to `0`.
- `min_slots` defaults to `0`; pool weights do not exist; the only physical
  capabilities are OLDLAB x86_64 and GB10 arm64.
- Every Loom image is an immutable `@sha256:<64 lowercase hex>` reference from
  one exact trusted source commit and tree. PostgreSQL and MinIO are external
  immutable digest references bound by the same release-evidence document.
- Rendering, status, and tests never create Secret values, apply Kubernetes
  resources, mutate a database, query or mutate Slurm, or change a capacity
  ceiling.
- Final YAML records the canonical render-input digest and trusted-release
  digest. Its own SHA-256 is external evidence and is not embedded recursively.
- The generic cluster renderer and the retired rollout broker are not extended.

---

### Task 1: Strict profile and trusted-release binding

**Files:**

- Create: `src/loom/personal_dev_control_plane_config.py`
- Create: `deploy/dev-fleet/personal-dev-control-plane.toml`
- Create: `tests/unit/test_personal_dev_control_plane_config.py`

**Interfaces:**

- Produces:
  `load_personal_dev_control_plane_profile(path: Path) -> PersonalDevControlPlaneProfile`.
- Produces:
  `load_personal_dev_trusted_release(path: Path, expected_sha256: str) -> PersonalDevTrustedRelease`.
- Produces `PersonalDevControlPlaneProfile.canonical_bytes()` and
  `PersonalDevTrustedRelease.canonical_bytes()` for render-input hashing.
- The trusted-release document has exactly these top-level fields:
  `schema_version`, `source_sha`, `source_tree`, `images`, and
  `release_evidence_sha256`.
- `images` has exactly `loom_service`, `personal_dev_builder`,
  `personal_dev_activation_agent`, `postgres`, `minio`, and `minio_client`.

- [x] **Step 1: Write failing profile and release tests**

  Add tests that load the checked-in profile and this exact canonical release
  fixture (write it with mode `0600` and no trailing newline):

  ```python
  release = {
      "schema_version": 1,
      "source_sha": "1" * 40,
      "source_tree": "2" * 40,
      "images": {
          "loom_service": "ghcr.io/qianyi-sun/loom-service@sha256:" + "3" * 64,
          "personal_dev_builder": (
              "ghcr.io/qianyi-sun/loom-personal-dev-builder@sha256:" + "4" * 64
          ),
          "personal_dev_activation_agent": (
              "ghcr.io/qianyi-sun/loom-personal-dev-activation-agent@sha256:" + "5" * 64
          ),
          "postgres": "docker.io/library/postgres@sha256:" + "6" * 64,
          "minio": "quay.io/minio/minio@sha256:" + "7" * 64,
          "minio_client": "quay.io/minio/mc@sha256:" + "9" * 64,
      },
      "release_evidence_sha256": "8" * 64,
  }
  ```

  Assert the checked-in profile resolves exactly `loom-dev`, `loom-dev-`,
  `min_slots_default == 0`, `max_slots_limit == 8`, activation replicas `0`,
  both personal flags false, and the sorted pool capabilities
  `[("gb10", "arm64"), ("oldlab", "x86_64")]` with no weight field.

  Parameterize rejection of unknown/missing TOML keys, wrong namespace/prefix,
  nonzero minimum, maximum outside `0..8`, enabled personal flags, nonzero
  activation replicas, duplicate/missing/extra pools, a weight key, unsafe DNS
  labels, noncanonical Secret/PVC DNS identities, and non-HTTPS public origin.

  Parameterize release rejection for unsafe owner/mode/link count/symlink,
  size above 1 MiB, trailing newline, noncanonical JSON, wrong supplied digest,
  unknown/missing fields, non-40-character source identities, mutable tags,
  zero/uppercase/short digests, wrong repositories for Loom images, and
  duplicate digest references.

- [x] **Step 2: Run the tests and observe the missing-module failure**

  Run:

  ```bash
  PYTHONPATH=src:. /home/hongjian/loom/.venv/bin/python -m pytest -q \
    tests/unit/test_personal_dev_control_plane_config.py
  ```

  Expected: collection fails because
  `loom.personal_dev_control_plane_config` does not exist.

- [x] **Step 3: Implement the minimal strict models and loaders**

  Use frozen dataclasses for profile subobjects and Pydantic v2 models with
  `ConfigDict(extra="forbid", frozen=True)` for untrusted TOML/JSON input.
  Validate these exact constants:

  ```python
  NAMESPACE = "loom-dev"
  PERSONAL_NAMESPACE_PREFIX = "loom-dev-"
  REQUIRED_POOLS = {"oldlab": "x86_64", "gb10": "arm64"}
  REQUIRED_IMAGE_KEYS = {
      "loom_service",
      "personal_dev_builder",
      "personal_dev_activation_agent",
      "postgres",
      "minio",
      "minio_client",
  }
  ```

  Read the release file with `lstat`, `O_NOFOLLOW`, descriptor identity recheck,
  a 1 MiB bound, current UID, mode `0600`, and link count one. Require canonical
  bytes from `json.dumps(..., sort_keys=True, separators=(",", ":"),
  ensure_ascii=True, allow_nan=False).encode("ascii")` and compare the supplied
  lowercase SHA-256 with `hmac.compare_digest`.

  The checked-in TOML contains no secrets and fixes all shadow values. It names
  the three Secrets, management/service identities, scanner-cache PVC, future
  RuntimeClass name, storage class and sizes, public origin, ingress
  class/issuer, finite quotas, canonical protocol JSON, and exact pool
  capabilities. Builder preparation is explicitly false; unmeasured runtime,
  scanner, database, and policy digests belong only in the later acceptance
  plan.

- [x] **Step 4: Run focused tests, format, lint, and type-check**

  Run:

  ```bash
  PYTHONPATH=src:. /home/hongjian/loom/.venv/bin/python -m pytest -q \
    tests/unit/test_personal_dev_control_plane_config.py
  PYTHONPATH=src:. /home/hongjian/loom/.venv/bin/python -m ruff format --check \
    src/loom/personal_dev_control_plane_config.py \
    tests/unit/test_personal_dev_control_plane_config.py
  PYTHONPATH=src:. /home/hongjian/loom/.venv/bin/python -m ruff check \
    src/loom/personal_dev_control_plane_config.py \
    tests/unit/test_personal_dev_control_plane_config.py
  PYTHONPATH=src:. /home/hongjian/loom/.venv/bin/python -m mypy \
    src/loom/personal_dev_control_plane_config.py
  ```

  Expected: all pass with no warnings.

- [x] **Step 5: Commit the configuration contract**

  ```bash
  git add src/loom/personal_dev_control_plane_config.py \
    deploy/dev-fleet/personal-dev-control-plane.toml \
    tests/unit/test_personal_dev_control_plane_config.py
  git commit -m "feat(dev): define protected personal control-plane profile"
  ```

---

### Task 2: Bounded projected-credential preparation

**Files:**

- Create: `src/loom/personal_dev_secret_init.py`
- Create: `tests/unit/test_personal_dev_secret_init.py`

**Interfaces:**

- Produces `CredentialProfile = Literal["management-files",
  "activation-public", "activation-private"]`.
- Produces
  `copy_projected_credentials(source: Path, destination: Path, *, profile: CredentialProfile) -> None`.
- Produces `python -m loom.personal_dev_secret_init --profile ... --source ... --destination ...`.
- `management-files` contains exactly:
  `admin-secrets.toml`, `config.json`, `capacity-lifecycle-token`,
  `capacity-lifecycle-ca.pem`, `capacity-lifecycle-certificate.pem`,
  `capacity-lifecycle-private-key.pem`, `capacity-reporter-ca.pem`,
  `capacity-reporter-certificate.pem`, and
  `capacity-reporter-private-key.pem`.
- Public/private profiles contain only `public-key` and `private-key`,
  respectively.

- [x] **Step 1: Write failing projection and destination tests**

  Reuse Kubernetes' real `..data -> ..generation` plus per-key symlink layout in
  the fixtures. Cover exact copy, idempotent identical replay, projection
  generation change before commit, key-link replacement, source/destination
  symlinks, non-regular input, missing/extra/empty/oversize files, wrong
  destination owner/mode/link count, partial pre-existing destination, changed
  replay, and cleanup after interrupted staging.

  Assert the destination directory is `0700`, each file is `0600`, every file
  is a single-link regular file owned by the process UID, and no source Secret
  path appears in an exception message.

- [x] **Step 2: Run the tests and observe the missing-module failure**

  ```bash
  PYTHONPATH=src:. /home/hongjian/loom/.venv/bin/python -m pytest -q \
    tests/unit/test_personal_dev_secret_init.py
  ```

- [x] **Step 3: Implement descriptor-pinned exact copying**

  Follow the proven generation-snapshot algorithm in
  `src/loom_capacity_manager/secret_init.py`, but keep profile names and errors
  personal-management-specific. Open source/generation directories with
  `O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC`, bound each file to 1 MiB, revalidate
  `..data`, generation inode, visible key set, and every key symlink immediately
  before atomic rename. Never log payloads or filenames that could contain
  user-supplied text.

- [x] **Step 4: Run tests, lint, and mypy**

  ```bash
  PYTHONPATH=src:. /home/hongjian/loom/.venv/bin/python -m pytest -q \
    tests/unit/test_personal_dev_secret_init.py
  PYTHONPATH=src:. /home/hongjian/loom/.venv/bin/python -m ruff check \
    src/loom/personal_dev_secret_init.py \
    tests/unit/test_personal_dev_secret_init.py
  PYTHONPATH=src:. /home/hongjian/loom/.venv/bin/python -m mypy \
    src/loom/personal_dev_secret_init.py
  ```

- [x] **Step 5: Commit credential preparation**

  ```bash
  git add src/loom/personal_dev_secret_init.py \
    tests/unit/test_personal_dev_secret_init.py
  git commit -m "feat(dev): prepare bounded personal control-plane credentials"
  ```

---

### Task 3: Deterministic shadow renderer

**Files:**

- Create: `src/loom/personal_dev_control_plane_render.py`
- Create: `tests/unit/test_personal_dev_control_plane_render.py`
- Create: `tests/ops/test_personal_dev_control_plane_package_boundary.py`

**Interfaces:**

- Consumes `PersonalDevControlPlaneProfile` and `PersonalDevTrustedRelease`.
- Produces frozen `RenderedPersonalDevControlPlane` with fields
  `yaml_text: str`, `input_sha256: str`, `release_sha256: str`, and
  `resource_count: int`.
- Produces
  `render_shadow_personal_dev_control_plane(profile, release) -> RenderedPersonalDevControlPlane`.
- YAML ordering is stable by the tuple `(scope, apiVersion, kind, namespace,
  name)`; mapping keys are emitted in deterministic insertion order.

- [x] **Step 1: Write failing complete-resource and invariant tests**

  Assert exact identities and document counts for Namespace; PostgreSQL and
  MinIO StatefulSets/Services/PVC templates; scanner-cache PVC; migration Job;
  management and activation ServiceAccounts; namespaced Roles/RoleBindings;
  management ClusterRoles/ClusterRoleBindings whose otherwise cluster-wide
  verbs are fail-closed by principal-specific admission policies; management
  Deployment/Service/Ingress; activation Deployment; and NetworkPolicies.

  Parse every YAML document and assert:

  ```python
  service_env["LOOM_SVC_DEV_INSTANCES_ENABLED"] == "false"
  service_env["LOOM_SVC_PERSONAL_DEV_BUILDER_ENABLED"] == "false"
  service_env["LOOM_SVC_K8S_WORKER_ENABLED"] == "false"
  activation_deployment["spec"]["replicas"] == 0
  ```

  Assert no Deployment/StatefulSet named or labeled as Control Plane, Gateway,
  web, worker, family orchestrator, pipeline orchestrator, or global
  autoscaler. Assert no Slurm setting, nonzero executable ceiling, pool weight,
  Secret value, bearer token, private key, source checkout path,
  `loom-dev-shared`, mutable image, `hostPath`, privileged container, host
  network/PID/IPC, Docker socket, or service-account token in builder Jobs.

  Assert all containers are non-root, drop all capabilities, disallow privilege
  escalation, use RuntimeDefault seccomp, have read-only roots where the image
  permits, and declare finite requests/limits; Jobs also have finite deadlines.
  Assert the management
  init container calls `loom.personal_dev_secret_init` for management/public
  profiles and the activation init container uses only the private profile.

  Assert scalar `secretKeyRef` keys are exactly `postgres-user`,
  `postgres-password`, `postgres-database`, `svc-db-url`,
  `dev-instance-database-admin-url`, `minio-access-key`, `minio-secret-key`, and
  `secret-store-master-key`. File-shaped Secret projections expose exactly the
  Task 2 key sets.

  Assert the combination of RBAC and fail-closed `ValidatingAdmissionPolicy`
  bindings lets management create/read/update/delete only derived
  `loom-dev-*` application namespaces, attempt-scoped `loom-build-*` builder
  namespaces, and each family's exact resources. Personal Secret authority is
  limited to the lifecycle's fixed generated names; builder Secret authority
  is limited to attempt-capability names. The activation agent can get
  candidate Deployments and apply stable Services/Ingresses only in
  `loom-dev-*`, and gains that authority only through lifecycle-created
  per-namespace RoleBindings. The policies match the exact service-account
  principals and reject either principal against `loom-dev`, other namespaces,
  unrelated Secrets, nodes, leases outside bounded names, and all
  Slurm/external-host authority.

  Set `automountServiceAccountToken: false` everywhere. Management and
  activation pods receive explicit short-lived, audience-bound projected API
  tokens plus only `kube-root-ca.crt` and their namespace; builder Jobs receive
  no service-account token. NetworkPolicy permits Kubernetes API egress only
  to the exact CIDR and port in the strict profile, never `0.0.0.0/0`.

- [x] **Step 2: Run tests and observe the missing-renderer failure**

  ```bash
  PYTHONPATH=src:. /home/hongjian/loom/.venv/bin/python -m pytest -q \
    tests/unit/test_personal_dev_control_plane_render.py \
    tests/ops/test_personal_dev_control_plane_package_boundary.py
  ```

- [x] **Step 3: Implement pure resource builders and canonical input digest**

  Build ordinary Python dictionaries and serialize with
  `yaml.safe_dump_all(..., explicit_start=False, sort_keys=False,
  default_flow_style=False)`. Do not shell out or read live state.

  Compute `input_sha256` over this exact framed payload:

  ```python
  b"loom-personal-dev-shadow-render-v1\0" \
      + profile.canonical_bytes() \
      + b"\0" \
      + release.canonical_bytes()
  ```

  Add label `app.kubernetes.io/managed-by=loom-personal-dev-control-plane`,
  32-character `loom.dev/render-input` and `loom.dev/trusted-release` labels,
  and full `loom.dev/render-input-sha256` and
  `loom.dev/trusted-release-sha256` annotations to every resource and pod
  template. Full SHA-256 values are annotations because Kubernetes label
  values are limited to 63 characters. Migration Job name includes the first
  16 characters of both digests so immutable changes create a new Job.

  The service container uses the immutable `loom_service` image for both the
  management Deployment and candidate-independent capacity-agent setting. It
  uses loopback discard endpoints for unused shared Control Plane/Gateway
  clients and NetworkPolicy blocks those routes. Builder and activation images
  come only from their dedicated release entries.

- [x] **Step 4: Run focused tests and static checks**

  ```bash
  PYTHONPATH=src:. /home/hongjian/loom/.venv/bin/python -m pytest -q \
    tests/unit/test_personal_dev_control_plane_config.py \
    tests/unit/test_personal_dev_secret_init.py \
    tests/unit/test_personal_dev_control_plane_render.py \
    tests/ops/test_personal_dev_control_plane_package_boundary.py
  PYTHONPATH=src:. /home/hongjian/loom/.venv/bin/python -m ruff check \
    src/loom/personal_dev_control_plane_config.py \
    src/loom/personal_dev_secret_init.py \
    src/loom/personal_dev_control_plane_render.py \
    tests/unit/test_personal_dev_control_plane_*.py \
    tests/unit/test_personal_dev_secret_init.py \
    tests/ops/test_personal_dev_control_plane_package_boundary.py
  PYTHONPATH=src:. /home/hongjian/loom/.venv/bin/python -m mypy \
    src/loom/personal_dev_control_plane_config.py \
    src/loom/personal_dev_secret_init.py \
    src/loom/personal_dev_control_plane_render.py
  ```

- [x] **Step 5: Commit the shadow renderer**

  ```bash
  git add src/loom/personal_dev_control_plane_render.py \
    tests/unit/test_personal_dev_control_plane_render.py \
    tests/ops/test_personal_dev_control_plane_package_boundary.py
  git commit -m "feat(dev): render inert personal management plane"
  ```

  Post-review corrections: the shared Namespace uses the cross-package
  `loom-operator` ownership label; admission rechecks exact personal/builder
  namespace shapes, exact resource names, and family-specific resource
  ownership on every mutation (including the separately fenced personal
  capacity lifecycle); dynamic workloads cannot use an indirect Secret or API
  token or image-pull reference to widen that authority; activation routes bind
  exact owners, generations, ClusterIP selectors and ports, Ingress hosts and
  backends, TLS, class, and annotations; and PostgreSQL and MinIO use separate
  ingress policies with disjoint callers.

---

### Task 4: Operator render command

**Files:**

- Create: `src/loom_cli/personal_dev_control_plane_cmd.py`
- Modify: `src/loom_cli/admin_cmd.py`
- Create: `tests/loom_cli/test_personal_dev_control_plane_cmd.py`

**Interfaces:**

- Produces:
  `loom admin personal-dev-control-plane render --file PROFILE --trusted-release-file RELEASE --trusted-release-sha256 SHA256`.
- Stdout contains YAML only. One canonical JSON evidence line goes to stderr
  with schema, source SHA/tree, input/release digest, resource count, and final
  YAML SHA-256.
- No apply, enable, prepare, activate, or acceptance subcommand exists.

- [x] **Step 1: Write failing parser/output/error tests**

  Cover successful exact bytes, absent required arguments, abbreviated/unknown
  options, unsafe release file, validation failure before stdout, broken pipe,
  and deterministic repeated output. Assert `--help` says render-only, shadow,
  personal mutations disabled, and physical capacity unchanged.

  Assert `loom admin --help` lists the command while `loom service up` and
  `loom dev` parsers remain unchanged.

- [x] **Step 2: Run and observe missing command**

  ```bash
  PYTHONPATH=src:. /home/hongjian/loom/.venv/bin/python -m pytest -q \
    tests/loom_cli/test_personal_dev_control_plane_cmd.py
  ```

- [x] **Step 3: Implement the narrow argparse handler**

  Parse with `allow_abbrev=False`. Load and validate every input before writing
  stdout. Use `sys.stdout.write(rendered.yaml_text)` once, then write this
  sorted compact stderr record:

  ```json
  {"input_sha256":"<sha256>","mode":"shadow","release_sha256":"<sha256>","resource_count":1,"schema":"loom-personal-dev-control-plane-render-v1","source_sha":"<sha>","source_tree":"<tree>","yaml_sha256":"<sha256>"}
  ```

  The shown resource count is a shape example; emit the actual positive count.
  Redact validation errors to stable messages and never echo a Secret value or
  release-file payload.

- [x] **Step 4: Run CLI plus affected admin tests**

  ```bash
  PYTHONPATH=src:. /home/hongjian/loom/.venv/bin/python -m pytest -q \
    tests/loom_cli/test_personal_dev_control_plane_cmd.py \
    tests/loom_cli/test_admin_cmd.py
  PYTHONPATH=src:. /home/hongjian/loom/.venv/bin/python -m ruff check \
    src/loom_cli/personal_dev_control_plane_cmd.py src/loom_cli/admin_cmd.py \
    tests/loom_cli/test_personal_dev_control_plane_cmd.py
  ```

- [x] **Step 5: Commit the render command**

  ```bash
  git add src/loom_cli/personal_dev_control_plane_cmd.py \
    src/loom_cli/admin_cmd.py \
    tests/loom_cli/test_personal_dev_control_plane_cmd.py
  git commit -m "feat(cli): expose personal management shadow render"
  ```

---

### Task 5: Read-only shadow status

**Files:**

- Create: `src/loom/personal_dev_control_plane_status.py`
- Modify: `src/loom_cli/personal_dev_control_plane_cmd.py`
- Create: `tests/unit/test_personal_dev_control_plane_status.py`
- Modify: `tests/loom_cli/test_personal_dev_control_plane_cmd.py`

**Interfaces:**

- Produces frozen `PersonalDevShadowStatus` with
  `ready: bool`, `blockers: tuple[str, ...]`, `input_sha256: str | None`,
  `release_sha256: str | None`, `manager_ceiling: int | None`, and bounded
  component summaries.
- Produces
  `observe_personal_dev_shadow_status(runner: KubectlRunner, *, expected: RenderedPersonalDevControlPlane, namespace: str = "loom-dev") -> PersonalDevShadowStatus`.
- Produces:
  `loom admin personal-dev-control-plane status --namespace loom-dev --kubeconfig PATH --file PROFILE --trusted-release-file RELEASE --trusted-release-sha256 SHA256`.
- Status emits one sorted compact JSON line and performs no write verb.
- Status requires the same trusted inputs as render and computes the expected
  manifest locally; live labels alone are not accepted as proof that images or
  package-owned RBAC still match the trusted release.

- [x] **Step 1: Write the failing status matrix**

  Use an injected fake runner that records exact argv and returns bounded JSON.
  Cover namespace missing; shared objects absent; StatefulSet/Deployment not
  ready; migration absent/failed/running/succeeded; init failure; mutable or
  changed images; mismatched render/release labels; personal flags missing,
  malformed, or true; activation replicas nonzero; RuntimeClass/PVC absent;
  unexpected `loom-dev-*` or `loom-build-*` namespace; package-owned cluster binding drift;
  manager probe unavailable/malformed/nonzero; and a complete healthy shadow.

  Require sorted stable blocker codes and bounded names/counts only. The
  healthy result is exactly:

  ```json
  {"blockers":[],"manager_ceiling":0,"mode":"shadow","ready":true,"schema":"loom-personal-dev-control-plane-status-v1"}
  ```

  The real result also includes the two digests and bounded component fields;
  tests compare the complete canonical object.

  Assert every Kubernetes call uses only `get`, `list`, or `exec` of the
  already-deployed manager's read-only health command. Reject kubectl contexts
  with an empty or mismatched current context unless `--kubeconfig` selects the
  reviewed file explicitly.

- [x] **Step 2: Run tests and observe the missing observer failure**

  ```bash
  PYTHONPATH=src:. /home/hongjian/loom/.venv/bin/python -m pytest -q \
    tests/unit/test_personal_dev_control_plane_status.py \
    tests/loom_cli/test_personal_dev_control_plane_cmd.py -k status
  ```

- [x] **Step 3: Implement bounded parsing and command wiring**

  Define a protocol:

  ```python
  class KubectlRunner(Protocol):
      def run(self, argv: Sequence[str], *, timeout_seconds: int) -> CompletedProcess[str]: ...
  ```

  Query exact labeled resources in `loom-dev`, list Namespace names once, read
  the named RuntimeClass, and execute only the capacity manager's existing
  local mTLS health probe path through the existing
  `capacity-control-plane status` helper. Cap each response at 4 MiB, reject
  duplicate identities and unknown JSON shapes, and never read Secret objects.
  Use the probe's explicit read-only observation mode so a real nonzero ceiling
  is returned as evidence; keep the existing capacity status command on its
  stricter ready-and-zero default.

  Add `status` with `allow_abbrev=False`, namespace fixed to `loom-dev`, an
  absolute non-symlink kubeconfig, the exact profile/trusted-release binding,
  and a 60-second total timeout. Compare every package-owned live object with
  the locally rendered expected object while allowing only server-added fields.
  Exit `0` only for `ready=true`; otherwise emit the canonical status and exit
  `1`.

- [x] **Step 4: Run status, CLI, and capacity status regressions**

  ```bash
  PYTHONPATH=src:. /home/hongjian/loom/.venv/bin/python -m pytest -q \
    tests/unit/test_personal_dev_control_plane_status.py \
    tests/loom_cli/test_personal_dev_control_plane_cmd.py \
    tests/loom_cli/test_capacity_control_plane.py \
    tests/loom_cli/test_capacity_control_plane_cmd.py
  PYTHONPATH=src:. /home/hongjian/loom/.venv/bin/python -m ruff check \
    src/loom/personal_dev_control_plane_status.py \
    src/loom_cli/personal_dev_control_plane_cmd.py \
    tests/unit/test_personal_dev_control_plane_status.py \
    tests/loom_cli/test_personal_dev_control_plane_cmd.py
  PYTHONPATH=src:. /home/hongjian/loom/.venv/bin/python -m mypy \
    src/loom/personal_dev_control_plane_status.py \
    src/loom_cli/personal_dev_control_plane_cmd.py
  ```

- [x] **Step 5: Commit read-only status**

  ```bash
  git add src/loom/personal_dev_control_plane_status.py \
    src/loom_cli/personal_dev_control_plane_cmd.py \
    tests/unit/test_personal_dev_control_plane_status.py \
    tests/loom_cli/test_personal_dev_control_plane_cmd.py
  git commit -m "feat(dev): observe personal management shadow status"
  ```

  Post-review correction: the observer retains at most eight internally
  consistent, immutable, successful terminal migration Job/Pod pairs as
  historical evidence while still requiring the exact current trusted
  migration. Malformed, failed, running, unpaired, or excess history blocks
  readiness. It also accepts Kubernetes' exact `NamespaceList` shape, requires
  exact list API versions, rejects pathologically nested JSON, and runs every
  kubectl command against a read-only anonymous snapshot of one owner-only,
  flattened, self-contained kubeconfig with no external credential files or
  plugins.

  Post-review input correction: the non-secret TOML profile is also read from
  one current-user-owned, single-link descriptor-stable regular file so a path
  or in-place race cannot change render authority after review. Pathologically
  nested trusted-release JSON is converted to the same stable invalid-input
  result as every other malformed release rather than escaping the loader.

---

### Task 6: Exact shadow rehearsal and package documentation

**Files:**

- Modify: `deploy/dev-fleet/README.md`
- Modify: `docs/architecture/multi-dev-environments.md`
- Create: `docs/runbooks/personal-dev-management-plane-shadow.md`
- Modify: `docs/runbooks/README.md`
- Modify: `tests/ops/test_personal_dev_control_plane_package_boundary.py`
- Modify: `config/component-ownership.toml` only if the ownership validator
  reports one of the new source/test paths as unowned.

**Interfaces:**

- Documents an owner-only evidence directory, trusted release binding, render,
  Secret key inventory, server-side diff/apply boundary, readiness, rollback,
  and stop conditions.
- Contains no acceptance enablement, personal apply, Slurm query/mutation, or
  physical-capacity activation command.

- [x] **Step 1: Add failing documentation/package assertions**

  Assert the runbook names exact commands and expected canonical status, never
  creates `loom-dev-shared`, never uses mutable image tags, never prints Secret
  content, never enables either personal flag, never scales the activation
  agent above zero, and never provides a capacity-manager activation/apply or
  Slurm command.

  Assert `deploy/dev-fleet/README.md` links both this runbook and the separate
  capacity rehearsal and states that both shadows must be ready before the
  later acceptance interlock can be considered.

- [x] **Step 2: Run and observe the missing-runbook failure**

  ```bash
  PYTHONPATH=src:. /home/hongjian/loom/.venv/bin/python -m pytest -q \
    tests/ops/test_personal_dev_control_plane_package_boundary.py
  ```

- [x] **Step 3: Write the exact render/deploy/rollback procedure**

  The runbook creates an owner-only evidence directory, verifies the release
  file/digest, renders with the new CLI, records YAML SHA-256, performs
  `kubectl diff --server-side` with field manager
  `loom-personal-dev-control-plane`, applies only inside the explicit #1280
  shadow window, waits for storage/migration/deployment, and runs read-only
  status.

  Secret provisioning remains a comment naming the approved secret channel and
  exact key sets; no `kubectl create secret --from-literal` example is allowed.
  Rollback reapplies the previous reviewed shadow YAML, never deletes PVCs, and
  stops if any personal namespace exists.

- [x] **Step 4: Run docs/package/security checks**

  ```bash
  PYTHONPATH=src:. /home/hongjian/loom/.venv/bin/python -m pytest -q \
    tests/ops/test_personal_dev_control_plane_package_boundary.py \
    tests/unit/test_cli_docs_examples.py \
    tests/ops/test_component_ownership_manifest.py
  ! git grep -n 'loom-dev-shared' -- deploy config
  ! git grep -n 'executable_new_capacity_ceiling[" =:]*[1-9]' -- \
    deploy/dev-fleet docs/runbooks/personal-dev-management-plane-shadow.md
  git diff --check
  ```

- [x] **Step 5: Commit documentation**

  ```bash
  git add deploy/dev-fleet/README.md docs/architecture/multi-dev-environments.md \
    docs/runbooks/personal-dev-management-plane-shadow.md docs/runbooks/README.md \
    tests/ops/test_personal_dev_control_plane_package_boundary.py \
    config/component-ownership.toml
  git commit -m "docs(dev): add personal management shadow rehearsal"
  ```

  Post-review correction: the rehearsal uses strict Bash error propagation and
  fresh output paths, blocks builder namespaces as well as personal namespaces,
  and re-renders the previous profile/release to byte-compare rollback YAML
  before any rollback apply. It also stops before rollback mutation unless the
  current and previous non-migration resource identity sets are equal; an
  identity-changing rollback requires a separately reviewed cleanup plan
  because server-side apply does not prune stale resources. Immediately after
  each reviewed server-side diff and before each apply, it rechecks the exact
  artifact and kubeconfig bytes, absence of dynamic namespaces, read-only
  kubeconfig safety, and the global manager's ready zero ceiling. The same
  identity interlock now covers forward apply: a first installation requires
  no existing package-owned top-level objects, while an upgrade or rollback
  requires the live, previous-reviewed, and new non-derived identity sets to
  agree.

---

### Task 7: Full verification, iterative review, and normal PR

**Files:**

- Modify only files required by evidence-backed review findings.

**Interfaces:**

- Produces a normal `dev` PR linked to #1280 and #906.
- Produces no live Kubernetes, database, registry, systemd, Slurm, DNS, or
  Secret mutation.

- [ ] **Step 1: Run focused personal and capacity suites**

  ```bash
  PYTHONPATH=src:. /home/hongjian/loom/.venv/bin/python -m pytest -q \
    tests/unit/test_personal_dev_*.py \
    tests/unit/test_service_dev_instance_runtime.py \
    tests/integration/test_personal_dev_capacity_runtime.py \
    tests/loom_cli/test_personal_dev_control_plane_cmd.py \
    tests/loom_cli/test_service_cmd.py \
    tests/ops/test_personal_dev_control_plane_package_boundary.py \
    tests/unit/test_capacity_*.py \
    tests/integration/test_capacity_*.py \
    tests/loom_cli/test_capacity_control_plane.py \
    tests/loom_cli/test_capacity_control_plane_cmd.py
  ```

- [ ] **Step 2: Run formatting, lint, typing, config, and migration checks**

  ```bash
  mapfile -t changed_python < <(
    git diff --name-only --diff-filter=ACMR origin/dev -- '*.py'
  )
  PYTHONPATH=src:. /home/hongjian/loom/.venv/bin/python -m ruff format --check \
    "${changed_python[@]}"
  PYTHONPATH=src:. /home/hongjian/loom/.venv/bin/python -m ruff check \
    src/loom src/loom_cli tests
  PYTHONPATH=src:. /home/hongjian/loom/.venv/bin/python -m mypy \
    src/loom src/loom_cli
  PYTHONPATH=src:. /home/hongjian/loom/.venv/bin/python -m pytest -q \
    tests/integration/test_alembic_migrations.py \
    tests/integration/test_capacity_management_migrations.py
  ```

  Migration tests use only their existing disposable PostgreSQL fixture.

- [ ] **Step 3: Run repository/security/package gates**

  Read the final-head `.github/workflows/ci.yml` and execute the current
  changed-file planner, repository hygiene, secret scan, runtime payload,
  component ownership, and package-boundary commands selected for this diff.
  At minimum run:

  ```bash
  test ! -e docs/superpowers
  ! git grep -n 'loom-dev-shared' -- deploy config
  ! git grep -n 'executable_new_capacity_ceiling[" =:]*[1-9]' -- \
    deploy/dev-fleet docs/architecture/personal-dev-management-plane-deployment.md \
    docs/runbooks/personal-dev-management-plane-shadow.md
  git diff --check origin/dev...HEAD
  git status --short --branch
  ```

- [ ] **Step 4: Iteratively self-review until no concrete finding remains**

  Review exact-file races, JSON/TOML canonicalization, digest framing, YAML
  determinism, self-referential hashes, Secret key separation, init-copy races,
  service flags, builder reachability, activation key isolation, RBAC verbs and
  namespace scope, admission-policy bypasses, NetworkPolicy DNS/storage/manager
  routes, immutable Jobs, PVC preservation, status parser bounds, manager
  ceiling checks, error redaction, rollback, shared-app exclusion, and all paths
  that could reach scheduler mutation. For each finding, first add a failing
  regression test, observe the expected failure, implement the single fix, and
  rerun the focused plus affected broad suites.

- [ ] **Step 5: Rebase on current `origin/dev` and repeat verification**

  ```bash
  git fetch origin dev
  git rebase origin/dev
  git diff --check origin/dev...HEAD
  git status --short --branch
  ```

  Repeat Steps 1-3 at the rebased exact head.

- [ ] **Step 6: Use completion/review skills and open the PR**

  Invoke `superpowers:requesting-code-review`,
  `superpowers:verification-before-completion`, and
  `superpowers:finishing-a-development-branch`. Resolve every evidence-backed
  critical, important, and minor finding with the TDD loop.

  Push `feat/personal-dev-management-plane`, open a normal PR to `dev`, link
  #1280 and #906, and state that the package is render-only shadow with personal
  mutation disabled and no live state changed. Require exact-head
  `repository-checks`, `images-gate`, `cluster-smoke-gate`, and
  `staging-smoke-gate` before merge.

- [ ] **Step 7: Prove merge provenance and retain the operational branch**

  After merge, fetch `dev`, verify the squash tree equals the approved PR-head
  tree, and delete only this merged feature branch/worktree. Do not delete the
  `ops/906-capacity-live-cutover` worktree. Update #1280 and #906 with exact
  commit/run evidence and the unchanged boundary: shadow deployment is now
  packageable, but acceptance enablement and physical capacity remain separate
  gates.
