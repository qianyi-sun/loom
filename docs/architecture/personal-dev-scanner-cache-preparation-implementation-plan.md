# Personal-development Scanner-cache Preparation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> `superpowers:subagent-driven-development` (recommended) or
> `superpowers:executing-plans` to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make every new personal-management scanner PVC deterministically
usable from one exact trusted release before the restricted builder can start.

**Architecture:** Protected CI materializes digest-pinned Trivy databases into
one immutable scanner-cache image and binds all file hashes into trusted release
version 2. A non-root, networkless init container atomically publishes and
revalidates a generation on the existing RWO PVC; the management service uses
that exact generation and independently verifies its scanner binding.

**Tech Stack:** Python 3.11, Pydantic v2, canonical JSON, descriptor-relative
POSIX filesystem operations, Docker Buildx, GitHub Actions, Kubernetes YAML,
PyYAML, pytest, Ruff, and mypy.

## Global Constraints

- `loom-dev` is the only shared infrastructure namespace; personal namespaces
  remain server-derived `loom-dev-OWNER` identities.
- Global executable new-capacity ceiling remains exactly `0`; this work never
  queries or mutates Slurm and never creates a worker.
- Trivy is exactly `v0.70.0`; upstream DB sources are the two immutable OCI
  manifests in `deploy/dev-fleet/personal-dev-scanner-cache-lock.json`.
- Runtime preparation has no network, Secret, service-account token, hostPath,
  node path, `kubectl cp`, or mutable image.
- Cache protected files are owned by UID `65531`, management runs as UID
  `65532`, and only the `fanal` directory is group-writable.
- Trusted release schema is exactly version `2` and adds the immutable
  `personal_dev_scanner_cache` image plus a complete scanner binding.
- The rendered package stays at 33 resources and shadow stays inert with
  lifecycle/builder false and activation replicas zero.
- Tests follow strict red-green-refactor; no production behavior is written
  before its focused test fails for the expected missing behavior.

---

### Task 1: Pinned scanner-cache lock and asset materializer

**Files:**

- Create: `deploy/dev-fleet/personal-dev-scanner-cache-lock.json`
- Create: `src/loom/personal_dev_scanner_cache.py`
- Create: `scripts/prepare_personal_dev_scanner_cache_assets.py`
- Create: `tests/unit/test_personal_dev_scanner_cache.py`
- Create: `tests/ops/test_prepare_personal_dev_scanner_cache_assets.py`

**Interfaces:**

- Produces
  `load_personal_dev_scanner_cache_lock(path: Path) -> PersonalDevScannerCacheLock`.
- Produces frozen `PersonalDevScannerCacheFiles` with the four file hashes and
  `canonical_value()` / `canonical_bytes()`.
- Produces
  `prepare_personal_dev_scanner_cache_assets(lock_path: Path, trivy: Path, output: Path) -> PersonalDevScannerCacheFiles`.
- The materializer writes exactly `db/{trivy.db,metadata.json}` and
  `java-db/{trivy-java.db,metadata.json}` plus canonical
  `scanner-cache-evidence.json`.

- [ ] **Step 1: Write failing lock-loader tests**

  Add literal expectations for the current manifest and layer digests and
  parameterize rejection of a tag, wrong repository, zero/uppercase/short
  digest, wrong layer digest, wrong Trivy version, unknown/missing field,
  noncanonical bytes, symlink, hard link, empty file, and file above 1 MiB:

  ```python
  def test_checked_in_scanner_cache_lock_is_exact() -> None:
      lock = load_personal_dev_scanner_cache_lock(_LOCK)
      assert lock.trivy_version == "v0.70.0"
      assert lock.binary_sha256["linux/amd64"] == (
          "379d59f24a4a828c55de5f0b91b6805cc35d13580180b658820e648611256166"
      )
      assert lock.database.image.endswith(
          "@sha256:01edd081af12fd613776b0db66ac23ce62c9d25802d8ee57671394c10ca3530b"
      )
      assert lock.java_database.layer_sha256 == (
          "bcc9ee0a8aa79524502cf892eda69e2180b54a3c7bd54c874b564201d2bdfc10"
      )
  ```

- [ ] **Step 2: Run the lock test and confirm the module is absent**

  Run:

  ```bash
  PYTHONPATH=src:. /home/hongjian/loom/.venv/bin/python -m pytest -q \
    tests/unit/test_personal_dev_scanner_cache.py
  ```

  Expected: collection fails because `loom.personal_dev_scanner_cache` does not
  exist.

- [ ] **Step 3: Implement the strict lock and canonical file binding**

  Use a duplicate-rejecting standard-library JSON parser, exact key-set checks,
  a descriptor-pinned 1 MiB read, and these public dataclasses. This module must
  retain a standard-library-only import graph because the cache image does not
  install project dependencies:

  ```python
  @dataclass(frozen=True, slots=True)
  class PersonalDevScannerCacheFiles:
      database_sha256: str
      database_metadata_sha256: str
      java_database_sha256: str
      java_database_metadata_sha256: str

  @dataclass(frozen=True, slots=True)
  class PersonalDevScannerCacheLock:
      schema_version: int
      trivy_version: str
      binary_sha256: Mapping[str, str]
      database: PersonalDevScannerCacheSource
      java_database: PersonalDevScannerCacheSource
      sha256: str
  ```

  Check in the canonical lock bytes from the design document with no trailing
  newline.

- [ ] **Step 4: Write failing real materializer tests**

  Build a mode-0555 fake Trivy executable that accepts only the two exact
  commands and writes small real SQLite-like fixture bytes plus bounded JSON.
  Assert exact argv, evidence bytes, output modes/inventory, source hash
  revalidation, empty destination requirement, no partial publish on failure,
  and rejection of extra/link/special/oversize outputs:

  ```python
  files = prepare_personal_dev_scanner_cache_assets(lock, trivy, output)
  assert files.database_sha256 == hashlib.sha256(b"vulnerability-db").hexdigest()
  assert set(path.relative_to(output).as_posix() for path in output.rglob("*")) == {
      "db", "db/metadata.json", "db/trivy.db",
      "java-db", "java-db/metadata.json", "java-db/trivy-java.db",
      "scanner-cache-evidence.json",
  }
  ```

- [ ] **Step 5: Run the materializer test and confirm the command is absent**

  Run:

  ```bash
  PYTHONPATH=src:. /home/hongjian/loom/.venv/bin/python -m pytest -q \
    tests/ops/test_prepare_personal_dev_scanner_cache_assets.py
  ```

  Expected: import or executable failure names the missing materializer.

- [ ] **Step 6: Implement bounded digest-only materialization**

  Invoke only:

  ```python
  [trivy, "image", "--download-db-only", "--db-repository",
   lock.database.image, "--cache-dir", staging, "--no-progress"]
  [trivy, "image", "--download-java-db-only", "--java-db-repository",
   lock.java_database.image, "--cache-dir", staging, "--no-progress"]
  ```

  Before invoking Trivy, read each manifest with
  `docker buildx imagetools inspect --raw` through a bounded subprocess and
  require the recorded media type, one expected layer, and exact layer digest.
  After both commands, descriptor-hash the exact four files, validate DB metadata
  `Version == 2`, Java metadata `Version == 1`, write canonical evidence, fsync,
  and rename the complete staging directory to the requested empty output.

- [ ] **Step 7: Verify and commit the source authority**

  Run:

  ```bash
  PYTHONPATH=src:. /home/hongjian/loom/.venv/bin/python -m pytest -q \
    tests/unit/test_personal_dev_scanner_cache.py \
    tests/ops/test_prepare_personal_dev_scanner_cache_assets.py
  PYTHONPATH=src:. /home/hongjian/loom/.venv/bin/python -m ruff check \
    src/loom/personal_dev_scanner_cache.py \
    scripts/prepare_personal_dev_scanner_cache_assets.py \
    tests/unit/test_personal_dev_scanner_cache.py \
    tests/ops/test_prepare_personal_dev_scanner_cache_assets.py
  PYTHONPATH=src:. /home/hongjian/loom/.venv/bin/python -m mypy \
    src/loom/personal_dev_scanner_cache.py \
    scripts/prepare_personal_dev_scanner_cache_assets.py
  git add deploy/dev-fleet/personal-dev-scanner-cache-lock.json \
    src/loom/personal_dev_scanner_cache.py \
    scripts/prepare_personal_dev_scanner_cache_assets.py \
    tests/unit/test_personal_dev_scanner_cache.py \
    tests/ops/test_prepare_personal_dev_scanner_cache_assets.py
  git commit -m "feat(dev): pin personal scanner cache assets"
  ```

---

### Task 2: Atomic non-root PVC installer

**Files:**

- Create: `src/loom/personal_dev_scanner_cache_init.py`
- Create: `tests/unit/test_personal_dev_scanner_cache_init.py`

**Interfaces:**

- Produces
  `install_personal_dev_scanner_cache(source_root: Path, destination_root: Path, *, expected: PersonalDevScannerCacheBinding) -> Path`.
- Produces `python -m loom.personal_dev_scanner_cache_init` with the seven
  digest/source arguments from the design plus `--scanner-binary-sha256`.
- Publishes `generations/CACHE_IDENTITY_SHA256` and atomically selects it in
  `active-generation`, retaining at most the new and previous valid generation.

- [ ] **Step 1: Write failing filesystem behavior tests**

  Cover fresh publication, literal identity JSON, modes, owner separation,
  idempotent replay, previous-generation retention, third-generation pruning,
  interrupted staging cleanup, and subprocess CLI behavior. Parameterize
  source/destination symlinks, hard links, FIFOs, wrong hashes, changed-during-
  read files, malformed metadata, unexpected entries, tampered generation,
  invalid active marker, more than 16 entries, and deletion above 16 GiB:

  ```python
  installed = install_personal_dev_scanner_cache(
      source, destination, expected=binding
  )
  assert installed == destination / "generations" / binding.cache_identity_sha256
  assert (destination / "active-generation").read_text() == (
      binding.cache_identity_sha256 + "\n"
  )
  assert stat.S_IMODE((installed / "db/trivy.db").stat().st_mode) == 0o444
  assert stat.S_IMODE((installed / "fanal").stat().st_mode) == 0o770
  ```

- [ ] **Step 2: Run the installer tests and observe the missing module**

  Run:

  ```bash
  PYTHONPATH=src:. /home/hongjian/loom/.venv/bin/python -m pytest -q \
    tests/unit/test_personal_dev_scanner_cache_init.py
  ```

- [ ] **Step 3: Implement the minimal secure installer**

  Use only descriptor-relative `os.open`, `os.mkdir`, `os.rename`, `os.replace`,
  `os.unlink`, and `os.rmdir` with `dir_fd` and no-follow checks. Copy in 1 MiB
  chunks with 4 GiB per DB, 64 KiB per metadata file, and 8 GiB total bounds.
  Fsync protected files, identity JSON, leaf directories, staging, generations,
  and root. A digest-named mismatch must raise
  `PersonalDevScannerCacheInstallError` without replacement.

  The canonical binding is:

  ```python
  @dataclass(frozen=True, slots=True)
  class PersonalDevScannerCacheBinding:
      cache_identity_sha256: str
      scanner_binary_sha256: str
      files: PersonalDevScannerCacheFiles
  ```

- [ ] **Step 4: Run red-green and mutation checks**

  Run the focused test, then temporarily mutate the database destination name
  and cache identity frame one at a time and confirm a focused test fails;
  restore each mutation before continuing.

- [ ] **Step 5: Verify and commit the installer**

  Run:

  ```bash
  PYTHONPATH=src:. /home/hongjian/loom/.venv/bin/python -m pytest -q \
    tests/unit/test_personal_dev_scanner_cache.py \
    tests/unit/test_personal_dev_scanner_cache_init.py
  PYTHONPATH=src:. /home/hongjian/loom/.venv/bin/python -m ruff check \
    src/loom/personal_dev_scanner_cache.py \
    src/loom/personal_dev_scanner_cache_init.py \
    tests/unit/test_personal_dev_scanner_cache_init.py
  PYTHONPATH=src:. /home/hongjian/loom/.venv/bin/python -m mypy \
    src/loom/personal_dev_scanner_cache.py \
    src/loom/personal_dev_scanner_cache_init.py
  git add src/loom/personal_dev_scanner_cache.py \
    src/loom/personal_dev_scanner_cache_init.py \
    tests/unit/test_personal_dev_scanner_cache_init.py
  git commit -m "feat(dev): publish scanner cache generations atomically"
  ```

---

### Task 3: Cache image and protected image workflow

**Files:**

- Create: `deploy/Dockerfile.personal-dev-scanner-cache`
- Modify: `deploy/Dockerfile.service`
- Modify: `config/component-ownership.toml`
- Modify: `.github/workflows/images.yml`
- Create: `tests/ops/test_personal_dev_scanner_cache_image.py`
- Modify: `tests/ops/test_ci_throughput_workflows.py`
- Modify: `tests/ops/test_component_ownership.py`

**Interfaces:**

- Adds release image component `personal-dev-scanner-cache` with digest name
  `loom-personal-dev-scanner-cache`.
- Adds workflow job `personal-dev-scanner-cache-assets` consuming the verified
  Trivy binary artifact and uploading one exact cache-assets artifact.
- Every cache-image build supplies
  `--build-context personal-dev-scanner-cache=/tmp/loom-personal-dev-scanner-cache`;
  other images never receive that context.

- [ ] **Step 1: Write failing Dockerfile and workflow contract tests**

  Assert the pinned Python index, UID `65531`, read-only asset paths, module
  entrypoint, no curl/wget/package install, exact component ownership, asset-job
  permissions `actions: read` and `contents: read`, one-day artifact retention, and conditional
  named context in both candidate and protected publish builds. Also assert the
  service Dockerfile's Trivy stage is exactly the version-0.70.0 index digest:

  ```python
  assert component.release_digest == "loom-personal-dev-scanner-cache"
  assert job["permissions"] == {"actions": "read", "contents": "read"}
  assert service_dockerfile.splitlines()[0] == (
      "FROM aquasec/trivy@sha256:"
      "be1190afcb28352bfddc4ddeb71470835d16462af68d310f9f4bca710961a41e AS trivy"
  )
  assert "--build-context" in candidate_build_script
  assert "personal-dev-scanner-cache=/tmp/loom-personal-dev-scanner-cache" in (
      candidate_build_script
  )
  ```

- [ ] **Step 2: Run tests and observe missing image/job failures**

  Run:

  ```bash
  PYTHONPATH=src:. /home/hongjian/loom/.venv/bin/python -m pytest -q \
    tests/ops/test_personal_dev_scanner_cache_image.py \
    tests/ops/test_component_ownership.py \
    tests/ops/test_ci_throughput_workflows.py
  ```

- [ ] **Step 3: Add the minimal cache image**

  The Dockerfile contains no `RUN` instruction:

  ```dockerfile
  FROM python:3.11-slim@sha256:9c900dea9e8fb7e16277c179b555cc72d29a352dbc33cff48ad5a0412fd5bfc7
  WORKDIR /opt/loom-personal-dev-scanner-cache
  COPY src/loom/__init__.py ./loom/__init__.py
  COPY src/loom/personal_dev_scanner_cache.py ./loom/personal_dev_scanner_cache.py
  COPY src/loom/personal_dev_scanner_cache_init.py ./loom/personal_dev_scanner_cache_init.py
  COPY --from=personal-dev-scanner-cache --chown=65531:65532 db ./assets/db
  COPY --from=personal-dev-scanner-cache --chown=65531:65532 java-db ./assets/java-db
  USER 65531:65532
  ENV PYTHONPATH=/opt/loom-personal-dev-scanner-cache
  ENTRYPOINT ["python", "-m", "loom.personal_dev_scanner_cache_init"]
  ```

  Replace only the first stage digest in `deploy/Dockerfile.service` with the
  exact version-0.70.0 index digest asserted in Step 1.

- [ ] **Step 4: Wire asset preparation and both build paths**

  The workflow downloads the existing verified AMD64 Trivy binary, runs the
  Task 1 script against the checked-in lock, hashes the complete artifact, and
  uploads it with retention one day. Add the job to candidate `build` and
  protected `publish` needs. Only when
  `matrix.image == 'personal-dev-scanner-cache'`, download the asset and append:

  ```bash
  build_args+=(
    --build-context
    "personal-dev-scanner-cache=/tmp/loom-personal-dev-scanner-cache"
  )
  ```

  Re-run the materializer immediately before and after the build only as hash
  checks; never redownload inside a platform job.

- [ ] **Step 5: Build a tiny fixture image locally**

  Add a pytest case that creates the four small real fixture files under its
  `tmp_path`, invokes this exact command through `subprocess.run`, and checks
  the result:

  ```python
  command = [
      "docker", "buildx", "build", "--platform", "linux/amd64", "--load",
      "--build-context", f"personal-dev-scanner-cache={assets}",
      "-f", "deploy/Dockerfile.personal-dev-scanner-cache",
      "-t", "loom-personal-dev-scanner-cache:test", ".",
  ]
  subprocess.run(command, cwd=_ROOT, check=True)
  subprocess.run(
      ["docker", "image", "inspect", "loom-personal-dev-scanner-cache:test"],
      check=True,
  )
  ```

  Create a container without starting it, copy the four files out, and require
  byte equality with the fixture.

- [ ] **Step 6: Verify and commit the image path**

  Run the three focused ops tests, YAML parse, component manifest validation,
  Ruff on changed Python tests, and then:

  ```bash
  git add deploy/Dockerfile.personal-dev-scanner-cache deploy/Dockerfile.service \
    config/component-ownership.toml .github/workflows/images.yml \
    tests/ops/test_personal_dev_scanner_cache_image.py \
    tests/ops/test_ci_throughput_workflows.py \
    tests/ops/test_component_ownership.py
  git commit -m "ci(dev): publish trusted scanner cache images"
  ```

---

### Task 4: Trusted release version 2 scanner evidence

**Files:**

- Modify: `scripts/ci_personal_dev_trusted_release.py`
- Modify: `tests/ops/test_ci_personal_dev_trusted_release.py`
- Modify: `.github/workflows/images.yml`

**Interfaces:**

- `assemble_personal_dev_trusted_release` additionally consumes the owner-only
  scanner-cache evidence file and extracted AMD64/ARM64 service scanner
  binaries.
- Release JSON schema becomes version 2, adds image key
  `personal_dev_scanner_cache`, and adds the exact `scanner` record from the
  design.
- Evidence records both cache-image platform subjects, locked DB sources,
  extracted file hashes, service binary hash, and cache identity frame.

- [ ] **Step 1: Extend fixtures and write failing release tests**

  Add the fourth internal image to both platform record/manifests fixtures.
  Assert exact scanner object, deterministic repeat, and rejection of missing
  cache image, wrong platform files, changed binary, binary hash not present in
  the version-0.70.0 lock, lock mismatch, metadata
  mismatch, cache identity mismatch, unknown field, and schema version 1:

  ```python
  assert release["schema_version"] == 2
  assert release["images"]["personal_dev_scanner_cache"].startswith(
      "ghcr.io/qianyi-sun/loom-personal-dev-scanner-cache@sha256:"
  )
  assert release["scanner"]["binary_platform"] == "linux/amd64"
  assert release["scanner"]["database_sha256"] == scanner_files.database_sha256
  ```

- [ ] **Step 2: Run and observe the version/image-set failure**

  Run:

  ```bash
  PYTHONPATH=src:. /home/hongjian/loom/.venv/bin/python -m pytest -q \
    tests/ops/test_ci_personal_dev_trusted_release.py
  ```

- [ ] **Step 3: Implement canonical scanner aggregation**

  Add `personal-dev-scanner-cache` to `_INTERNAL_IMAGES`. Read scanner evidence
  with the existing owner/size/identity checks, hash the supplied single-link
  service binaries and require their hashes to equal the lock's exact
  `linux/amd64` and `linux/arm64` values. Construct
  `PersonalDevScannerCacheBinding` with the AMD64 value and compute:

  ```python
  cache_identity_sha256 = hashlib.sha256(
      b"loom-personal-dev-scanner-cache-v1\0" + canonical_scanner_without_identity
  ).hexdigest()
  ```

  Emit release bytes without a trailing newline and evidence bytes with the
  existing canonical newline convention.

- [ ] **Step 4: Add protected readback of binary and cache files**

  In `personal-dev-trusted-release`, download all eight platform records and
  read back all four indexes. Pull/cache-create each cache image member without
  running it, copy the four asset files, and compare AMD64/ARM64 hashes to the
  asset evidence. Extract `/usr/local/bin/trivy` from both service members and
  pass them plus scanner evidence to `assemble`. Add the fourth image to the
  attestation verification loop and gate condition.

- [ ] **Step 5: Verify and commit release evidence**

  Run:

  ```bash
  PYTHONPATH=src:. /home/hongjian/loom/.venv/bin/python -m pytest -q \
    tests/ops/test_ci_personal_dev_trusted_release.py \
    tests/ops/test_personal_dev_scanner_cache_image.py \
    tests/ops/test_ci_throughput_workflows.py
  PYTHONPATH=src:. /home/hongjian/loom/.venv/bin/python -m ruff check \
    scripts/ci_personal_dev_trusted_release.py \
    tests/ops/test_ci_personal_dev_trusted_release.py
  PYTHONPATH=src:. /home/hongjian/loom/.venv/bin/python -m mypy \
    scripts/ci_personal_dev_trusted_release.py
  git add scripts/ci_personal_dev_trusted_release.py \
    tests/ops/test_ci_personal_dev_trusted_release.py \
    .github/workflows/images.yml
  git commit -m "feat(dev): bind scanner cache trusted releases"
  ```

---

### Task 5: Control-plane release and acceptance contracts

**Files:**

- Modify: `src/loom/personal_dev_control_plane_config.py`
- Modify: `tests/unit/test_personal_dev_control_plane_config.py`
- Modify: `tests/unit/test_personal_dev_control_plane_acceptance_config.py`

**Interfaces:**

- Adds `PersonalDevTrustedScanner` to `PersonalDevTrustedRelease`.
- Adds `personal_dev_scanner_cache` to `PersonalDevTrustedImages`.
- Adds `scanner_cache_identity_sha256`, `scanner_database_metadata_sha256`, and
  `scanner_java_database_metadata_sha256` to
  `PersonalDevAcceptanceBuilder`.
- `validate_personal_dev_acceptance_plan` requires all scanner hashes and
  images to equal the trusted release.

- [ ] **Step 1: Write failing strict version-2 release tests**

  Update the canonical fixture with the fourth image and scanner object. Assert
  canonical roundtrip and parameterize rejection of every missing/extra field,
  version 1, wrong repository/platform/version/lock/file digest, duplicate image
  digest, and cache identity not matching the framed scanner record.

- [ ] **Step 2: Write failing acceptance/release equality tests**

  Extend the acceptance fixture with the cache identity and two metadata hashes.
  Mutate each of binary, cache identity, DB, DB metadata, Java DB, Java
  metadata, cache image, and source lock independently and require
  `PersonalDevAcceptancePlanError`:

  ```python
  with pytest.raises(PersonalDevAcceptancePlanError):
      validate_personal_dev_acceptance_plan(
          profile, release, shadow_sha256, changed_plan, now=_NOW
      )
  ```

- [ ] **Step 3: Run both tests and observe schema failures**

  Run the two focused config test files; expected failures identify schema 1 and
  the missing scanner fields.

- [ ] **Step 4: Implement strict dataclasses, models, and equality checks**

  Use one `_TrustedScannerInput` with `extra="forbid"`, nonzero lowercase
  digests, literal platform `linux/amd64`, literal version `v0.70.0`, and a model
  validator that recomputes the cache identity. Include scanner data in release
  canonical bytes and acceptance canonical bytes.

- [ ] **Step 5: Verify and commit contracts**

  Run both focused tests, Ruff, and mypy, then commit the three files as:

  ```text
  feat(dev): require release-bound scanner caches
  ```

---

### Task 6: Management init, runtime revalidation, render, and status

**Files:**

- Modify: `config/loom-schema.toml`
- Modify: `src/loom_cli/data/loom-schema.toml`
- Modify: `src/loom_service/config/_generated.py`
- Modify: `src/loom_service/personal_dev_builder.py`
- Modify: `src/loom/personal_dev_control_plane_render.py`
- Modify: `src/loom/personal_dev_control_plane_status.py`
- Modify: `tests/unit/test_service_personal_dev_builder.py`
- Modify: `tests/unit/test_personal_dev_control_plane_render.py`
- Modify: `tests/unit/test_personal_dev_control_plane_status.py`
- Modify: `tests/loom_cli/test_personal_dev_control_plane_cmd.py`

**Interfaces:**

- Adds settings:
  `personal_dev_builder_scanner_cache_identity_sha256`,
  `personal_dev_builder_scanner_database_metadata_sha256`, and
  `personal_dev_builder_scanner_java_database_metadata_sha256`.
- Management environment always uses the release-bound scanner identity and
  exact `generations/CACHE_IDENTITY_SHA256` path; policy remains empty in shadow.
- Management Deployment gets cache init container
  `personal-dev-scanner-cache-init` and AMD64 node selector.
- Builder startup rehashes binary, DBs, metadata, and canonical identity before
  constructing any build authority.

- [ ] **Step 1: Write failing service startup revalidation tests**

  Extend `_settings` with a real identity file and metadata. Assert enabled
  startup succeeds only for exact values and fails separately for changed
  identity, DB metadata, Java metadata, cache path generation name, writable
  protected file, and binary hash:

  ```python
  with pytest.raises(RuntimeError, match="scanner cache binding"):
      build_personal_dev_builder_runtime(changed_settings, minio_client=object())
  ```

- [ ] **Step 2: Write failing render and status tests**

  Assert both modes render the exact cache image/args, UID 65531/GID 65532,
  no env/Secret/token/API mount, read-only root, finite resources, scanner PVC
  RW mount, AMD64 selector, release-bound path/identity, 33 resources, and
  unchanged inert flags. Status fixtures must block on any init/image/argument/
  path/hash drift through expected-render comparison.

- [ ] **Step 3: Run focused tests and confirm missing settings/init behavior**

  Run:

  ```bash
  PYTHONPATH=src:. /home/hongjian/loom/.venv/bin/python -m pytest -q \
    tests/unit/test_service_personal_dev_builder.py \
    tests/unit/test_personal_dev_control_plane_render.py \
    tests/unit/test_personal_dev_control_plane_status.py \
    tests/loom_cli/test_personal_dev_control_plane_cmd.py
  ```

- [ ] **Step 4: Implement runtime verification and generate config**

  Add `_installed_scanner_cache_binding(cache_directory: Path)` that requires
  the directory basename to be the configured cache identity, parses canonical
  `identity.json`, validates protected modes/owners, and hashes metadata through
  the existing descriptor helper. Compare all fields before registry or
  Kubernetes clients are constructed. Regenerate configuration with:

  ```bash
  PYTHONPATH=src:. /home/hongjian/loom/.venv/bin/python -m loom_cli config codegen
  ```

- [ ] **Step 5: Render the restricted init container**

  Add
  `_scanner_cache_init(profile, release) -> dict[str, Any]` returning this fixed
  shape:

  ```python
  {
      "name": "personal-dev-scanner-cache-init",
      "image": release.images.personal_dev_scanner_cache,
      "command": ["python", "-m", "loom.personal_dev_scanner_cache_init"],
      "args": [
          "--source-root", "/opt/loom-personal-dev-scanner-cache/assets",
          "--destination-root", "/var/lib/loom-personal-dev-scanner",
          "--cache-identity-sha256", release.scanner.cache_identity_sha256,
          "--scanner-binary-sha256", release.scanner.binary_sha256,
          "--database-sha256", release.scanner.database_sha256,
          "--database-metadata-sha256",
          release.scanner.database_metadata_sha256,
          "--java-database-sha256", release.scanner.java_database_sha256,
          "--java-database-metadata-sha256",
          release.scanner.java_database_metadata_sha256,
      ],
      "securityContext": _container_security(user=65531),
      "resources": _resources(profile.resources.management),
      "volumeMounts": [
          {"name": "scanner-cache", "mountPath": "/var/lib/loom-personal-dev-scanner"},
          {"name": "tmp", "mountPath": "/tmp"},
      ],
  }
  ```

  Place it before credential init containers, add
  `nodeSelector: {"kubernetes.io/arch": "amd64"}`, and set scanner env in shadow
  from `release.scanner`. Acceptance uses the same values and adds only policy/
  enablement bindings.

- [ ] **Step 6: Run full focused red-green verification**

  Run all four files from Step 3 plus both control-plane config files and the
  installer tests. Require no warnings, then run Ruff and mypy on all changed
  production modules.

- [ ] **Step 7: Commit the management integration**

  Commit generated and source files together as:

  ```text
  feat(dev): prepare scanner cache before management startup
  ```

---

### Task 7: Operational runbook and architecture boundaries

**Files:**

- Modify: `docs/runbooks/personal-dev-zero-capacity-acceptance.md`
- Modify: `docs/architecture/personal-dev-management-plane-deployment.md`
- Modify: `docs/architecture/multi-dev-environments.md`
- Modify: `deploy/dev-fleet/README.md`
- Modify: `tests/ops/test_personal_dev_control_plane_package_boundary.py`

**Interfaces:**

- Acceptance no longer requests or hashes local scanner binary/DB archives.
- It binds the checked-in lock, trusted-release scanner record, cache image,
  init container, ready management Deployment, zero ceiling, and no workers.
- Rollback remains exact reviewed shadow bytes from the same version-2 release.

- [ ] **Step 1: Write failing runbook boundary tests**

  Require lock hash, release scanner/cache image checks, init completion through
  rollout readiness, and repeated status interlocks. Forbid
  `scanner_database=`, `scanner_java_database=`, `scanner_binary=`,
  `kubectl cp`, runtime download flags, hostPath, or temporary egress.

- [ ] **Step 2: Run the package-boundary test and observe old archive inputs**

  Run:

  ```bash
  PYTHONPATH=src:. /home/hongjian/loom/.venv/bin/python -m pytest -q \
    tests/ops/test_personal_dev_control_plane_package_boundary.py
  ```

- [ ] **Step 3: Update the exact runbook**

  Replace local scanner paths with:

  ```bash
  scanner_cache_lock="$(pwd -P)/deploy/dev-fleet/personal-dev-scanner-cache-lock.json"
  scanner_cache_lock_sha256="$(sha256sum "$scanner_cache_lock" | awk '{print $1}')"
  test "$scanner_cache_lock_sha256" = \
    "$(jq -r .scanner.lock_sha256 "$trusted_release")"
  test "$(jq -r .scanner.cache_identity_sha256 "$trusted_release")" = \
    "$(jq -r .builder.scanner_cache_identity_sha256 "$acceptance_plan")"
  ```

  Keep the local finding policy, runtime profile, launcher profile, backup/
  restore evidence, two owner sessions, all zero-ceiling checks, and rollback
  procedure unchanged.

- [ ] **Step 4: Link the design and update current-state text**

  State that the scanner cache is release-bound and prepared in shadow without
  personal or capacity authority. Do not imply DNS, candidate GHCR publishing,
  gVisor rollout, or two-owner acceptance is complete.

- [ ] **Step 5: Verify and commit documentation**

  Run the package-boundary test and repository secret-scan test, then commit as:

  ```text
  docs(dev): operate release-bound scanner caches
  ```

---

### Task 8: Full verification, review, PR, and live inert shadow

**Files:**

- Modify only files required by verified review findings.

**Interfaces:**

- Produces a clean branch, protected PR, exact-head successful checks, trusted
  release version 2, and one separately reviewed inert-shadow manifest.
- Live apply retains ceiling zero and creates no personal/build namespace.

- [ ] **Step 1: Run the complete focused regression suite**

  Run:

  ```bash
  PYTHONPATH=src:. /home/hongjian/loom/.venv/bin/python -m pytest -q \
    tests/unit/test_personal_dev_scanner_cache.py \
    tests/unit/test_personal_dev_scanner_cache_init.py \
    tests/ops/test_prepare_personal_dev_scanner_cache_assets.py \
    tests/ops/test_personal_dev_scanner_cache_image.py \
    tests/ops/test_ci_personal_dev_trusted_release.py \
    tests/ops/test_ci_throughput_workflows.py \
    tests/ops/test_component_ownership.py \
    tests/unit/test_personal_dev_control_plane_config.py \
    tests/unit/test_personal_dev_control_plane_acceptance_config.py \
    tests/unit/test_personal_dev_control_plane_render.py \
    tests/unit/test_personal_dev_control_plane_status.py \
    tests/loom_cli/test_personal_dev_control_plane_cmd.py \
    tests/unit/test_service_personal_dev_builder.py \
    tests/ops/test_personal_dev_control_plane_package_boundary.py
  PYTHONPATH=src:. /home/hongjian/loom/.venv/bin/python -m ruff check \
    src scripts tests packages migrations capacity_guard_migrations \
    capacity_migrations
  PYTHONPATH=src:. /home/hongjian/loom/.venv/bin/python -m mypy src
  PYTHONPATH=src:. /home/hongjian/loom/.venv/bin/python -m loom_cli config codegen --check
  ```

- [ ] **Step 2: Perform one clean self-review pass**

  Review `git diff origin/dev HEAD` for Secret exposure, mutable sources,
  unbounded I/O, descriptor escapes, unsafe deletion, UID/mode mistakes,
  cache-identity ambiguity, RWO multi-attach, runtime network, image-plan gaps,
  release/platform mismatch, shadow enablement, admission/RBAC widening, worker
  creation, and physical-capacity mutation. Fix findings with a new failing test
  and repeat until one full pass finds no issue.

- [ ] **Step 3: Push and create the normal PR**

  Push `codex/1280-scanner-cache-preparation`, create a PR to `dev`, attach the
  design, red-green evidence, focused verification counts, and explicit
  DNS/GHCR/capacity non-goals. Enable repository-supported merge handling only
  after exact-head required checks succeed.

- [ ] **Step 4: Produce and verify the protected trusted release**

  After merge, run the protected image workflow for the exact merged commit,
  download the three-file trusted-release artifact, verify canonical bytes,
  release/evidence hashes, four internal multi-platform image readbacks,
  scanner lock/hash/platform binding, and attestations.

- [ ] **Step 5: Render and review the new inert shadow**

  From a clean exact-commit checkout, render shadow, require 33 resources,
  lifecycle/builder false, activation zero, init cache image exact, no dynamic
  namespaces, no personal worker, and manager ceiling zero. Record byte hash,
  server-side diff, rollback artifact, and cluster/storage health before apply.

- [ ] **Step 6: Apply and prove only the inert shadow**

  Apply with field manager `loom-personal-dev-control-plane`, wait for cache init
  and management readiness, run canonical shadow status, and recheck five Ready
  nodes, no DiskPressure, healthy Longhorn volumes, no dynamic namespaces, no
  workers, and exact zero ceiling. Do not render or apply acceptance until the
  separate gVisor, Secret, DNS, GHCR, backup/restore, and two-owner gates pass.
