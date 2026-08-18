# Personal Development Zero-Capacity Acceptance Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> `superpowers:subagent-driven-development` (recommended) or
> `superpowers:executing-plans` to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deliver the protected release evidence, acceptance rendering,
runtime interlock, and read-only operator status required to exercise two
concurrent personal development applications while the global executable
capacity ceiling remains exactly zero.

**Architecture:** Keep the merged shadow profile immutable and inert. A
protected image-release job assembles one canonical trusted release, and a
separate owner-only acceptance plan binds that release, the reviewed shadow,
builder dependencies, two owners, rollback evidence, and one exact global
manager checkpoint. Acceptance rendering reuses the shadow resources but
enables only personal lifecycle/build/activation, while service startup,
activation routes, Kubernetes readiness, and operator status independently
fail closed on manager identity drift, expiry, or any nonzero ceiling.

**Tech Stack:** Python 3.11, Pydantic v2, dataclasses, FastAPI/httpx,
argparse, PyYAML, Kubernetes YAML and CEL, GitHub Actions/GHCR attestations,
pytest, Ruff, mypy.

## Global Constraints

- `loom-dev` contains shared infrastructure only; personal applications use
  `loom-dev-<owner>` and build sandboxes use `loom-build-*`.
- The checked-in TOML remains the inert rollback profile with
  `dev_instances_enabled=false`, `personal_dev_builder_enabled=false`, and
  `activation_agent_replicas=0`.
- Acceptance enables exactly those two service features and one activation
  replica through a digest-pinned plan; it never enables the in-cluster worker
  or personal Control Plane Slurm actuator.
- `min_slots` remains configurable with default `0`; there are no pool
  weights; OLDLAB x86_64 and GB10 arm64 remain the only advertised physical
  capabilities.
- The global manager is the only capacity authority. Acceptance requires its
  exact incarnation, configuration epoch, execution state, execution epoch,
  authenticated lifecycle principal, and executable new-capacity ceiling
  `0`.
- The release and acceptance plan are canonical owner-only mode-`0600`,
  single-link, non-symlink files with independently supplied SHA-256 values.
- All workload images are immutable digest references. The three Loom images
  come from one exact protected source commit/tree; PostgreSQL, MinIO, and the
  MinIO client are reviewed multi-architecture upstream indexes pinned with
  exact amd64 and arm64 members.
- Rendering and status remain read-only. They never create Secret values,
  apply manifests, mutate the database, call Slurm, submit scheduler work, or
  change a capacity ceiling.
- Physical one-slot execution and prepared-to-active transition remain in the
  separately reviewed #906 package.
- Plans and architecture records stay under `docs/architecture`; do not create
  `docs/superpowers`.

---

### Task 1: Protected aggregate personal-development trusted release

**Files:**

- Create: `deploy/dev-fleet/personal-dev-external-images.json`
- Create: `scripts/ci_personal_dev_trusted_release.py`
- Modify: `.github/workflows/images.yml`
- Modify: `tests/ops/test_ci_throughput_workflows.py`
- Create: `tests/ops/test_ci_personal_dev_trusted_release.py`

**Interfaces:**

- Produces
  `assemble_personal_dev_trusted_release(...) -> tuple[dict[str, object], dict[str, object]]`.
- Produces the artifact
  `personal-dev-trusted-release-run-<run>-attempt-<attempt>` containing exactly
  `trusted-release.json`, `trusted-release-evidence.json`, and
  `trusted-release.sha256`.
- `trusted-release.json` keeps the already merged schema:
  `schema_version`, `source_sha`, `source_tree`, `images`, and
  `release_evidence_sha256`.
- `personal-dev-external-images.json` records each external index reference and
  exact `linux/amd64` and `linux/arm64` member digest.

- [x] **Step 1: Write failing canonical assembly and workflow-boundary tests**

  Add fixtures for the three internal image pairs and this exact external
  shape:

  ```python
  external = {
      "schema_version": 1,
      "images": {
          "postgres": {
              "reference": "docker.io/library/postgres@sha256:" + "1" * 64,
              "members": {
                  "linux/amd64": "sha256:" + "2" * 64,
                  "linux/arm64": "sha256:" + "3" * 64,
              },
          },
          "minio": {
              "reference": "quay.io/minio/minio@sha256:" + "4" * 64,
              "members": {
                  "linux/amd64": "sha256:" + "5" * 64,
                  "linux/arm64": "sha256:" + "6" * 64,
              },
          },
          "minio_client": {
              "reference": "quay.io/minio/mc@sha256:" + "7" * 64,
              "members": {
                  "linux/amd64": "sha256:" + "8" * 64,
                  "linux/arm64": "sha256:" + "9" * 64,
              },
          },
      },
  }
  ```

  Assert deterministic bytes, the six exact final image references, the exact
  source commit/tree/run binding, platform members, scan-record digests, and
  `release_evidence_sha256 == sha256(canonical evidence bytes)`. Parameterize
  rejection of duplicate JSON fields, tags, zero/uppercase digests, wrong
  repositories, missing/extra platforms, changed source/tree/run, mixed
  internal subject names, mixed build modes, index/member mismatch, and extra
  files. Assert the workflow downloads exactly the six architecture records,
  reads all six immutable index manifests, verifies the three Loom manifest
  attestations, uploads only the exact three-file artifact, and adds the
  aggregate job to `images-gate`.

- [x] **Step 2: Run focused tests and confirm the missing producer failure**

  Run:

  ```bash
  PYTHONPATH=src:. /home/hongjian/loom/.venv/bin/python -m pytest -q \
    tests/ops/test_ci_personal_dev_trusted_release.py \
    tests/ops/test_ci_throughput_workflows.py
  ```

  Expected: the new producer import and aggregate workflow job are absent.

- [x] **Step 3: Implement the canonical external binding and assembler**

  Check in reviewed immutable indexes and their native members. Implement
  duplicate-rejecting canonical JSON reads and strict repository/digest checks.
  Reuse `validate_architecture_record()` and `validate_manifest_subjects()`
  from `scripts/ci_image_release_evidence.py` for the exact two-platform Loom
  indexes; do not reimplement their SLSA contract. For immutable upstream
  indexes, require exactly the pinned amd64 and arm64 members while accepting
  other well-formed upstream platform descriptors. Construct evidence before
  the release so no recursive hash exists:

  ```python
  evidence = {
      "schema_version": 1,
      "release": {
          "repository": repository,
          "ref": f"refs/heads/{ref_name}",
          "commit": source_sha,
          "tree": source_tree,
          "run_id": run_id,
          "run_attempt": run_attempt,
      },
      "internal_images": internal_evidence,
      "external_images": external_evidence,
  }
  evidence_bytes = canonical_json(evidence)
  release = {
      "schema_version": 1,
      "source_sha": source_sha,
      "source_tree": source_tree,
      "images": release_images,
      "release_evidence_sha256": hashlib.sha256(evidence_bytes).hexdigest(),
  }
  ```

  Write outputs through bounded ordinary files only after every input passes.

- [x] **Step 4: Add the protected aggregate workflow job**

  Run it only for an authorized dev/main trusted publication whose selected
  image set includes `service`, `personal-dev-builder`, and
  `personal-dev-activation-agent`. Download exact run/attempt architecture
  artifacts, read immutable indexes by digest, verify the three Loom manifest
  attestations against the exact source, run the assembler, revalidate both
  outputs, and upload the three-file artifact with retention at least 30 days.
  The job gets `actions: read`, `attestations: read`, `contents: read`, and
  `packages: read`; it gets no cluster, environment, or Secret authority.

- [x] **Step 5: Verify and commit the trusted release producer**

  Run:

  ```bash
  PYTHONPATH=src:. /home/hongjian/loom/.venv/bin/python -m pytest -q \
    tests/ops/test_ci_personal_dev_trusted_release.py \
    tests/ops/test_ci_throughput_workflows.py \
    tests/ops/test_ci_secret_isolation.py
  PYTHONPATH=src:. /home/hongjian/loom/.venv/bin/python -m ruff check \
    scripts/ci_personal_dev_trusted_release.py \
    tests/ops/test_ci_personal_dev_trusted_release.py
  git add .github/workflows/images.yml \
    deploy/dev-fleet/personal-dev-external-images.json \
    scripts/ci_personal_dev_trusted_release.py \
    tests/ops/test_ci_personal_dev_trusted_release.py \
    tests/ops/test_ci_throughput_workflows.py
  git commit -m "feat(dev): publish trusted personal development releases"
  ```

  Expected: all focused checks pass.

---

### Task 2: Canonical zero-capacity acceptance plan

**Files:**

- Modify: `src/loom/personal_dev_control_plane_config.py`
- Modify: `src/loom/db/schema_startup.py`
- Create: `tests/unit/test_personal_dev_control_plane_acceptance_config.py`

**Interfaces:**

- Produces
  `load_personal_dev_acceptance_plan(path: Path, expected_sha256: str) -> PersonalDevAcceptancePlan`.
- Produces
  `validate_personal_dev_acceptance_plan(profile, release, shadow_yaml_sha256, plan, *, now) -> None`.
- `PersonalDevAcceptancePlan.canonical_bytes()` is the exact render/runtime
  binding.

- [x] **Step 1: Write failing plan-loader and cross-input tests**

  Use one canonical fixture with exact nested objects `source`, `release`,
  `storage`, `activation`, `builder`, `manager`, `principals`, `quotas`,
  `acceptance_owners`, and `window`. The manager object has this exact shape:

  ```python
  {
      "authority_incarnation": "00000000-0000-0000-0000-000000000101",
      "configuration_epoch": 7,
      "execution_state": "shadow",
      "execution_epoch": 0,
      "executable_new_capacity_ceiling": 0,
  }
  ```

  The two owners each contain distinct nonzero `team_id` and `user_id` UUIDs.
  Assert rejection of unsafe file metadata/races, noncanonical bytes,
  duplicate/extra/missing fields, nonzero ceiling, active execution, incoherent
  epoch/state, mutable image, release/profile/shadow mismatch, schema-head
  mismatch, builder setting mismatch, protocol digest mismatch, duplicate
  owners, invalid quotas, expired/not-yet-open windows, expiry beyond the
  rollback window, and zero digests.

- [x] **Step 2: Run tests and confirm the loader is absent**

  Run:

  ```bash
  PYTHONPATH=src:. /home/hongjian/loom/.venv/bin/python -m pytest -q \
    tests/unit/test_personal_dev_control_plane_acceptance_config.py
  ```

  Expected: collection fails on the missing acceptance interfaces.

- [x] **Step 3: Implement frozen strict plan types and consistency validation**

  Reuse the trusted-release descriptor-pinned reader and canonical JSON helper.
  Parse timestamps only in canonical UTC `YYYY-MM-DDTHH:MM:SSZ` form. Permit
  only coherent non-executable manager states:

  ```python
  if plan.manager.execution_state == "shadow":
      coherent = plan.manager.execution_epoch == 0
  else:
      coherent = (
          plan.manager.execution_state in {"prepared", "drain-only"}
          and plan.manager.execution_epoch > 0
      )
  if not coherent or plan.manager.executable_new_capacity_ceiling != 0:
      raise PersonalDevAcceptancePlanError("personal-dev acceptance plan is invalid")
  ```

  Compare release images/source byte-for-byte, compare all finite quotas with
  the TOML profile, derive the protocol-map SHA-256 from canonical profile
  JSON, require exact builder publisher/registry/RuntimeClass, and require
  `window.started_at <= now < window.expires_at <= window.rollback_expires_at`.

- [x] **Step 4: Verify and commit the acceptance plan contract**

  Run:

  ```bash
  PYTHONPATH=src:. /home/hongjian/loom/.venv/bin/python -m pytest -q \
    tests/unit/test_personal_dev_control_plane_config.py \
    tests/unit/test_personal_dev_control_plane_acceptance_config.py
  PYTHONPATH=src:. /home/hongjian/loom/.venv/bin/python -m ruff check \
    src/loom/personal_dev_control_plane_config.py \
    tests/unit/test_personal_dev_control_plane_acceptance_config.py
  PYTHONPATH=src:. /home/hongjian/loom/.venv/bin/python -m mypy \
    src/loom/personal_dev_control_plane_config.py
  git add src/loom/personal_dev_control_plane_config.py \
    tests/unit/test_personal_dev_control_plane_acceptance_config.py
  git commit -m "feat(dev): bind zero-capacity acceptance plans"
  ```

---

### Task 3: Exact global-manager runtime and readiness interlock

**Files:**

- Modify: `src/loom_capacity_manager/api.py`
- Modify: `src/loom_capacity_manager/health_probe.py`
- Modify: `src/loom/personal_dev_capacity.py`
- Modify: `src/loom/personal_dev_runtime.py`
- Modify: `src/loom_service/personal_dev_lifecycle.py`
- Modify: `src/loom_service/app.py`
- Modify: `src/loom_service/routes/health.py`
- Modify: `src/loom_service/routes/dev_instances.py`
- Modify: `config/loom-schema.toml`
- Regenerate: `src/loom_service/config/_generated.py`
- Test: `tests/integration/test_capacity_manager_api.py`
- Test: `tests/unit/test_capacity_manager_health_probe.py`
- Test: `tests/unit/test_personal_dev_capacity.py`
- Test: `tests/unit/test_personal_dev_runtime.py`
- Test: `tests/unit/test_service_personal_dev_lifecycle.py`
- Test: `tests/unit/test_control_plane_app.py`
- Test: `tests/unit/test_dev_instance_routes.py`

**Interfaces:**

- Produces `PersonalDevCapacityManagerBinding` with exact authority,
  authenticated principal, configuration/execution epochs and state, and
  ceiling.
- Produces
  `CapacityManagerPersonalDevProjector.current_manager_binding() -> PersonalDevCapacityManagerBinding`.
- Produces `PersonalDevAcceptanceInterlock.assert_ready(*, now) -> None`.
- Produces unauthenticated, secret-free
  `GET /api/v1/health/personal-dev-acceptance` for the Kubernetes readiness
  probe; it returns only status and stable blocker codes.

- [x] **Step 1: Write failing manager binding, startup, route, and readiness tests**

  Assert `/v1/status` returns `observer_principal_id` from the authenticated
  `capacity:read` principal and that the projector rejects a missing/wrong
  principal, UUID, epoch, state, ceiling, response type, redirect, oversized
  body, or transport error. Assert the interlock accepts only exact equality,
  rejects expiry, and does not treat `bool` as an integer. Assert service
  startup closes its owned HTTP client after rejection, acceptance readiness
  returns 503 on drift, and both activation-intent read and acknowledgement
  reject before touching the database when the interlock is unavailable.

- [x] **Step 2: Run focused tests and observe exact failures**

  Run:

  ```bash
  PYTHONPATH=src:. /home/hongjian/loom/.venv/bin/python -m pytest -q \
    tests/unit/test_capacity_manager_health_probe.py \
    tests/unit/test_personal_dev_capacity.py \
    tests/unit/test_personal_dev_runtime.py \
    tests/unit/test_dev_instance_routes.py
  ```

  Expected: the binding and interlock interfaces do not exist.

- [x] **Step 3: Expose and parse the authenticated manager identity**

  Preserve the existing checkpoint method as a compatibility projection.
  Add the principal only to authenticated `/v1/status`, then parse selected
  fields with strict runtime types:

  ```python
  binding = PersonalDevCapacityManagerBinding(
      authority_incarnation=UUID(payload["authority_incarnation"]),
      observer_principal_id=payload["observer_principal_id"],
      configuration_epoch=payload["configuration_epoch"],
      execution_state=payload["execution_state"],
      execution_epoch=payload["execution_epoch"],
      executable_new_capacity_ceiling=payload["executable_new_capacity_ceiling"],
  )
  ```

  Extend the in-container health probe with an explicit identity-observation
  mode used only by operator status; keep its existing strict zero-ceiling
  default byte-compatible.

- [x] **Step 4: Implement service-owned acceptance interlock and continuous gate**

  Add schema-backed settings `personal_dev_acceptance_binding_json` (default
  `{}`), `personal_dev_acceptance_plan_sha256` (default empty), and
  `personal_dev_activation_public_key_sha256` (default empty). Parse one
  canonical binding only when both personal features are enabled. Validate the
  activation public-key file digest before loading the verifier. At startup,
  call the interlock before creating background tasks; retain it in app state,
  call it on every acceptance readiness request and before both activation
  routes, and close the projector on every startup/teardown path.

- [x] **Step 5: Regenerate config, verify, and commit the runtime interlock**

  Run:

  ```bash
  PYTHONPATH=src:. /home/hongjian/loom/.venv/bin/python -m loom_cli config codegen
  PYTHONPATH=src:. /home/hongjian/loom/.venv/bin/python -m pytest -q \
    tests/integration/test_capacity_manager_api.py \
    tests/unit/test_capacity_manager_health_probe.py \
    tests/unit/test_personal_dev_capacity.py \
    tests/unit/test_personal_dev_runtime.py \
    tests/unit/test_service_personal_dev_lifecycle.py \
    tests/unit/test_control_plane_app.py \
    tests/unit/test_dev_instance_routes.py
  PYTHONPATH=src:. /home/hongjian/loom/.venv/bin/python -m mypy \
    src/loom/personal_dev_capacity.py src/loom/personal_dev_runtime.py \
    src/loom_service/personal_dev_lifecycle.py
  git add config/loom-schema.toml src/loom_service/config/_generated.py \
    src/loom_capacity_manager/api.py src/loom_capacity_manager/health_probe.py \
    src/loom/personal_dev_capacity.py src/loom/personal_dev_runtime.py \
    src/loom_service/personal_dev_lifecycle.py src/loom_service/app.py \
    src/loom_service/routes/health.py src/loom_service/routes/dev_instances.py \
    tests/integration/test_capacity_manager_api.py \
    tests/unit/test_capacity_manager_health_probe.py \
    tests/unit/test_personal_dev_capacity.py tests/unit/test_personal_dev_runtime.py \
    tests/unit/test_service_personal_dev_lifecycle.py \
    tests/unit/test_control_plane_app.py tests/unit/test_dev_instance_routes.py
  git commit -m "feat(dev): enforce zero-capacity runtime interlock"
  ```

---

### Task 4: Acceptance renderer with immutable shadow rollback

**Files:**

- Modify: `src/loom/personal_dev_control_plane_render.py`
- Modify: `tests/unit/test_personal_dev_control_plane_render.py`

**Interfaces:**

- Keeps
  `render_shadow_personal_dev_control_plane(profile, release)` byte-stable.
- Produces
  `render_acceptance_personal_dev_control_plane(profile, release, plan, *, now) -> RenderedPersonalDevControlPlane`.
- Acceptance render-input hash domain is
  `b"loom-personal-dev-acceptance-render-v1\0" + profile + release + plan`.

- [x] **Step 1: Write failing deterministic acceptance render tests**

  Assert the same storage/PVC identities and immutable images as shadow; exact
  flags `true/true`, activation replicas `1`, readiness path
  `/api/v1/health/personal-dev-acceptance`, and no worker/Slurm authority.
  Assert every resource and pod template has the full
  `loom.dev/acceptance-plan-sha256` annotation and its 32-character label.
  Assert rendered builder/scanner/profile/protocol/public-key/manager settings
  exactly equal the plan and that shadow YAML bytes remain unchanged.

- [x] **Step 2: Run render tests and confirm acceptance rendering is absent**

  Run:

  ```bash
  PYTHONPATH=src:. /home/hongjian/loom/.venv/bin/python -m pytest -q \
    tests/unit/test_personal_dev_control_plane_render.py
  ```

- [x] **Step 3: Refactor one private mode-aware renderer and add acceptance mode**

  Keep public wrappers narrow. The mode-aware management environment sets:

  ```python
  flags = {
      "LOOM_SVC_DEV_INSTANCES_ENABLED": "true",
      "LOOM_SVC_PERSONAL_DEV_BUILDER_ENABLED": "true",
      "LOOM_SVC_PERSONAL_DEV_ACCEPTANCE_PLAN_SHA256": plan.sha256,
      "LOOM_SVC_PERSONAL_DEV_ACCEPTANCE_BINDING_JSON": plan.manager_runtime_json(),
      "LOOM_SVC_PERSONAL_DEV_ACTIVATION_PUBLIC_KEY_SHA256": (
          plan.activation.public_key_sha256
      ),
  }
  ```

  Construct scanner identity from the three plan digests, set the trusted
  launcher and finding-policy digests, preserve `K8S_WORKER_ENABLED=false`, and
  verify the plan's shadow manifest digest against the exact shadow bytes
  before emitting acceptance YAML.

- [x] **Step 4: Verify and commit acceptance rendering**

  Run focused tests, Ruff, and mypy on renderer/config, then:

  ```bash
  git add src/loom/personal_dev_control_plane_render.py \
    tests/unit/test_personal_dev_control_plane_render.py
  git commit -m "feat(dev): render zero-capacity personal acceptance"
  ```

---

### Task 5: Mode-aware operator status and CLI

**Files:**

- Modify: `src/loom/personal_dev_control_plane_status.py`
- Modify: `src/loom_cli/personal_dev_control_plane_cmd.py`
- Modify: `tests/unit/test_personal_dev_control_plane_status.py`
- Modify: `tests/loom_cli/test_personal_dev_control_plane_cmd.py`

**Interfaces:**

- Keeps `observe_personal_dev_shadow_status(...)` and existing shadow CLI
  output compatible.
- Produces
  `observe_personal_dev_acceptance_status(runner, *, expected, plan, namespace="loom-dev") -> PersonalDevAcceptanceStatus`.
- Adds render-only `render-acceptance` and read-only `status-acceptance`
  subcommands, each requiring both acceptance-plan arguments.
- Acceptance status adds booleans `application_ready`,
  `capacity_publication_ready`, and `worker_available`.

- [x] **Step 1: Write failing acceptance status and CLI matrix tests**

  Build a healthy fixture with management and activation deployments ready,
  exact RuntimeClass handler/profile annotation, exact manager identity, zero
  workers, and zero or valid managed dynamic namespaces. Assert canonical
  output has `worker_available: false`. Parameterize drift in plan digest,
  flags, activation replicas/readiness, RuntimeClass handler/profile, scanner
  inputs, manager identity/epoch/state/ceiling, window expiry, malformed
  personal/build namespace ownership, unexpected worker deployment, and
  response bounds. Assert neither command accepts partial plan arguments or an
  apply/activate option.

- [x] **Step 2: Run tests and confirm the mode interfaces are absent**

  Run:

  ```bash
  PYTHONPATH=src:. /home/hongjian/loom/.venv/bin/python -m pytest -q \
    tests/unit/test_personal_dev_control_plane_status.py \
    tests/loom_cli/test_personal_dev_control_plane_cmd.py
  ```

- [x] **Step 3: Implement bounded acceptance observation**

  Reuse the shadow resource/index comparison. In acceptance mode, require
  exact enabled flags, one ready activation replica, exact RuntimeClass
  binding, and the manager identity probe. Permit only well-formed dynamic
  namespaces with their exact managed-by family labels and bounded subject
  UUID labels. Add one read-only all-namespaces Deployment inventory to prove
  no personal worker exists. Set readiness facets independently:

  ```python
  application_ready = shared_ready and activation_ready
  capacity_publication_ready = manager_binding_matches and manager_ceiling == 0
  worker_available = False
  ready = application_ready and capacity_publication_ready and not blockers
  ```

- [x] **Step 4: Add explicit acceptance CLI handlers**

  Load profile, release, and plan before rendering or starting kubectl. Emit
  YAML only on stdout and one canonical evidence object on stderr. Status exit
  codes remain `0` ready, `1` observed but blocked, and `2` invalid local
  inputs. Preserve the anonymous immutable kubeconfig snapshot mechanism.

- [x] **Step 5: Verify and commit status/CLI**

  Run focused pytest, Ruff, and mypy, then:

  ```bash
  git add src/loom/personal_dev_control_plane_status.py \
    src/loom_cli/personal_dev_control_plane_cmd.py \
    tests/unit/test_personal_dev_control_plane_status.py \
    tests/loom_cli/test_personal_dev_control_plane_cmd.py
  git commit -m "feat(dev): observe zero-capacity personal acceptance"
  ```

---

### Task 6: Exact two-owner acceptance and rollback runbook

**Files:**

- Create: `docs/runbooks/personal-dev-zero-capacity-acceptance.md`
- Modify: `docs/runbooks/README.md`
- Modify: `deploy/dev-fleet/README.md`
- Modify: `docs/architecture/multi-dev-environments.md`
- Modify: `tests/ops/test_personal_dev_control_plane_package_boundary.py`

**Interfaces:**

- Documents evidence preparation, server-side diff/apply, status, concurrent
  two-owner create/update/policy/destroy/redeploy, and byte-reviewed rollback.
- Contains no Secret values, kubeconfig payload, physical activation command,
  or destructive cleanup shortcut.

- [x] **Step 1: Write failing runbook contract tests**

  Assert exact use of the trusted-release artifact, owner-only acceptance plan,
  pre/post status, manager identity/ceiling checks, two distinct authenticated
  owner sessions, concurrent `--min-slots 0` deploys and updates, cross-owner
  rejection checks, both destroy modes, retained-name redeploy, and rollback to
  the exact shadow manifest. Assert stop conditions for credential, expiry,
  RuntimeClass/scanner, Secret key, migration, manager, namespace, and worker
  drift.

- [x] **Step 2: Run the ops tests and observe the missing runbook**

  Run:

  ```bash
  PYTHONPATH=src:. /home/hongjian/loom/.venv/bin/python -m pytest -q \
    tests/ops/test_personal_dev_control_plane_package_boundary.py
  ```

- [x] **Step 3: Write the exact runbook and update indexes**

  Every mutable command is preceded by immutable artifact, kubeconfig identity,
  no-worker, and manager-ceiling assertions. Use `kubectl diff --server-side`
  before `kubectl apply --server-side --field-manager=loom-personal-dev-control-plane`.
  Rollback reapplies exact shadow bytes and never deletes storage or changes the
  global manager. Record acceptance results as owner-only canonical JSON.

- [x] **Step 4: Verify and commit operational documentation**

  Run the package-boundary and secret-scan suites, then:

  ```bash
  git add docs/runbooks/personal-dev-zero-capacity-acceptance.md \
    docs/runbooks/README.md deploy/dev-fleet/README.md \
    docs/architecture/multi-dev-environments.md \
    tests/ops/test_personal_dev_control_plane_package_boundary.py
  git commit -m "docs(dev): define zero-capacity personal acceptance"
  ```

---

### Task 7: Full verification, iterative review, and normal PR

**Files:**

- Modify only files required by findings from the review loop.

**Interfaces:**

- Produces a clean feature branch, focused commits, one pushed PR, exact-head
  passing checks, and no merged-branch residue.

- [ ] **Step 1: Run complete focused and regression verification**

  Run:

  ```bash
  PYTHONPATH=src:. /home/hongjian/loom/.venv/bin/python -m pytest -q \
    tests/unit/test_personal_dev_control_plane_config.py \
    tests/unit/test_personal_dev_control_plane_acceptance_config.py \
    tests/unit/test_personal_dev_control_plane_render.py \
    tests/unit/test_personal_dev_control_plane_status.py \
    tests/loom_cli/test_personal_dev_control_plane_cmd.py \
    tests/unit/test_personal_dev_capacity.py \
    tests/unit/test_personal_dev_runtime.py \
    tests/unit/test_service_personal_dev_lifecycle.py \
    tests/unit/test_service_personal_dev_builder.py \
    tests/unit/test_dev_instance_routes.py \
    tests/integration/test_capacity_manager_api.py \
    tests/ops/test_ci_personal_dev_trusted_release.py \
    tests/ops/test_ci_throughput_workflows.py \
    tests/ops/test_personal_dev_control_plane_package_boundary.py
  PYTHONPATH=src:. /home/hongjian/loom/.venv/bin/python -m ruff check \
    src scripts tests packages migrations capacity_guard_migrations \
    capacity_migrations
  PYTHONPATH=src:. /home/hongjian/loom/.venv/bin/python -m mypy src
  PYTHONPATH=src:. /home/hongjian/loom/.venv/bin/python -m loom_cli config codegen --check
  ```

- [ ] **Step 2: Self-review until one clean pass**

  Review the full base diff for authorization widening, Secret exposure,
  fail-open paths, boolean/integer coercion, races, unbounded I/O, timestamp
  ambiguity, mutable images, namespace-family confusion, worker/Slurm
  authority, rollback drift, and workflow privilege. Fix each finding with a
  reproducing test and rerun the affected suites. Repeat from a fresh diff
  until a complete pass finds no issue.

- [ ] **Step 3: Verify completion evidence immediately before push**

  Confirm clean status, exact base, no `docs/superpowers`, no Secret-like
  tracked files, and no changes in
  `/home/hongjian/loom/.worktrees/capacity-live-cutover`.

- [ ] **Step 4: Push and create the normal PR**

  ```bash
  git push -u origin feat/personal-dev-zero-capacity-acceptance
  gh pr create --base dev --head feat/personal-dev-zero-capacity-acceptance \
    --title "feat(dev): interlock zero-capacity personal acceptance" \
    --body-file /tmp/personal-dev-zero-capacity-pr.md
  ```

  The PR body records exact test commands, non-goals, trusted-release behavior,
  zero-ceiling guarantees, and live blockers without claiming a deployment.

- [ ] **Step 5: Monitor exact-head checks and finish the branch normally**

  Diagnose any failure from complete logs before changing code. After exact
  head approval and merge, prove the merged tree, then remove only the merged
  feature worktree/local branch/remote branch. Preserve active worktrees,
  open-PR branches, closed-unmerged branches, and unrelated user artifacts.
