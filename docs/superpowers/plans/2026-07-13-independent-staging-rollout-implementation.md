# Independent Staging Rollout Implementation Plan

> **Governance note (2026-07-14):** The CODEOWNERS entries and assertions in
> this historical implementation plan now provide advisory ownership routing
> only on `dev`; they are not a human approval or merge gate. The protected
> staging paths continue to select full CI through the planner.

> **Temporary capacity amendment (2026-07-14):** #822 supersedes this plan's
> active all-15-host acceptance statements while `trt-gb10-7` is unreachable.
> Keep all 15 aliases in the fixed SSH/legacy trust inventory, but require all
> 14 active hosts and 140 slots for broker preflight, rollout, release-gate, and
> smoke. Node 7 remains stopped with no runtime override until a separate
> merged re-admission change passes fresh evidence.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give Qianyi, Hongjian, and Devansh a supported, attributable command that independently deploys only the freshly fetched merged `origin/dev` head to Loom staging.

**Architecture:** A root-installed client enters a fixed broker as the non-login `loom-rollout` account. The broker authenticates the sudo caller, binds a request to the exact fetched `origin/dev` SHA, serializes the entire staging lifecycle, creates a verified backup, and launches the existing rollout driver in a detached systemd user unit with a private service-owned envelope. Existing rollout steps remain the deployment engine; the new layer owns candidate policy, backup preparation, request/attempt evidence, operator attribution, detached control, and host convergence.

**Tech Stack:** Python 3.11, argparse, frozen dataclasses, `tomllib`, POSIX `fcntl` locks, atomic/fsynced JSON and JSONL files, systemd user units, Git, kubectl, boto3/MinIO S3 API, Bash installation assets, pytest, Ruff, mypy.

## Global Constraints

- A new request deploys only the exact head of `refs/heads/dev` fetched from `https://github.com/qianyi-sun/loom.git` into `refs/remotes/origin/dev` at request time.
- `start` accepts no ref, SHA, tag, image tag, remote, path, environment, config, secret, smoke, skip, force, or free-form command override; `--dry-run` is its only option.
- A resume uses the original request's SHA, image tag, rollout ID, backup manifest path, and backup manifest digest even if `origin/dev` has advanced.
- The first attempt enforces the configured backup freshness window immediately before unit launch. Resume revalidates the original backup's path, ownership, component integrity, and digest but does not replace it or apply a new age cutoff: it is the pre-mutation restore point. A missing or tampered restore point fails closed and requires explicit recovery, not an automatic fresh backup of partially rolled state.
- The normal service identity is the non-login `loom-rollout` account; never copy Qianyi's private deploy key.
- The root-owned, non-editable broker/runtime installation and the service-writable candidate checkout are separate trust chains. Fresh-fetching `dev` may update candidate source but must not silently replace the broker code or venv; broker upgrades use the root installer from an already merged commit.
- The operator group is `loom-staging-operators` with Qianyi, Hongjian, and Devansh.
- Exactly one staging request may be pending or running. A second start/resume fails; it never queues, preempts, or accepts a force flag.
- The lifecycle lock is distinct from the existing per-mutation rollout lock so child `cluster up` and `environment-state` operations do not deadlock.
- A broker-created and service-owned envelope is mandatory before any non-dry-run `staging` driver creates rollout evidence or mutates staging.
- The broker creates and verifies Postgres, MinIO, and Kubernetes Secret backup components before publishing the driver envelope, active pointer, transient unit, or rollout evidence. A partial backup never advances `latest`; it records a safe request failure while the launch mutex is still held.
- All 14 active GB10 hosts under #822, protected preflight, environment-state, release-gate, catalog provisioning, and smoke acceptance remain required; the full 15-host topology stays validated for legacy trust and re-admission.
- Raw tokens, private keys, credential file bodies, complete environments, private endpoints, and unredacted subprocess errors never enter argv, request evidence, status output, logs returned to operators, or summaries.
- Current broad host-admin and Docker grants are not revoked in this slice. This is an operational safety and attribution boundary, not a confidentiality boundary against a deliberate root-equivalent administrator.
- No production authority changes, unmerged branch testing, pull-request ref testing, or shared-staging rollout before merge.
- Shared `platform-dev` staging acceptance starts only after the implementation PR has merged to `dev`; CI may use its isolated ephemeral kind cluster before merge.
- This plan advances the execute-only runner portion of #803. It does not close #803 or silently absorb the separate OLDLAB/GB10 human identity inventory, OLDLAB-2 UID normalization, token-rotation ACL preservation, and fresh-host bootstrap scope.

---

## File and responsibility map

Create the focused package `src/loom_cli/rollout/operator/`:

- `model.py`: immutable caller, candidate, request, attempt, active-pointer, driver-envelope, and safe status data contracts.
- `config.py`: strict `/etc/loom/staging-rollout.toml` schema, fixed defaults, ownership/mode validation, and config hashing.
- `store.py`: private request ledger, attempt events, envelope publication, active pointer, atomic no-replace writes, and directory fsync.
- `policy.py`: authenticated sudo caller derivation, operator-group authorization, and sanitized child environment.
- `candidate.py`: trusted runner checkout validation and exact fresh `origin/dev` binding.
- `lifecycle.py`: launch mutex, full-driver singleton, active-pointer reconciliation, and safe conflict details.
- `systemd.py`: fixed transient-unit launch/show/kill/journal interface for the `loom-rollout` user manager.
- `backup.py`: Postgres dump, MinIO bucket mirror, Secret export, metadata manifest, immutable verification, and `latest` publication.
- `preflight.py`: redacted installation, Docker/Buildx, kube, data path,
  credential fingerprint, backup-tool, fixed 15-host GB10 topology validation,
  and SSH checks for the exact merged 14-host active set under #822.
- `redaction.py`: exact-known-secret plus central Loom redaction for operator-visible logs/status.
- `broker.py`: the five public commands and `start --dry-run`.
- `worker.py`: detached driver execution from a finalized envelope, terminal bookkeeping, and lock release.

Modify the existing rollout path only where it must carry a trusted request:

- `src/loom_cli/rollout/cli.py`: private envelope mode and protected-staging enforcement.
- `src/loom_cli/rollout/context.py`: immutable request/original-initiator fields, attempt attribution, and pinned backup digest.
- `src/loom_cli/rollout/state.py`: backward-compatible request and current-attempt attribution.
- `src/loom_cli/rollout/evidence.py`: private atomic top-level writes and exact broker-selected rollout ID.
- `src/loom_cli/rollout/driver.py`: envelope/backup preflight before evidence creation and safe error persistence.
- `src/loom_cli/rollout/steps/candidate_source.py` and `subprocess_util.py`: bounded child environment plus centralized output redaction before persistence.
- `src/loom_cli/rollout/steps/s10_env_state.py`, `s11_cluster_up.py`: pass the validated envelope into existing protected mutation-lock acquisition.
- `src/loom_cli/rollout/steps/s99_summary.py`: include safe request/attempt attribution and redact summaries.
- `src/loom_cli/cluster_backup_guard.py`: recompute component bytes/digests during `backup check`.
- `src/loom_cli/rollout_lock.py`, `src/loom_cli/admin_cmd.py`, `src/loom_cli/cluster_cmd.py`: structured request/initiator fields in mutation-lock evidence.

Create host convergence assets:

- `deploy/staging-rollout/staging-rollout.toml`: non-secret policy template.
- `deploy/staging-rollout/loom-staging-rollout`: root-owned user client.
- `deploy/staging-rollout/loom-staging-rollout-broker`: root-owned fixed broker launcher.
- `deploy/staging-rollout/loom-staging-rollout.sudoers`: `NOSETENV` fixed-command rule.
- `scripts/ops/staging_rollout_host.py`: testable plan/install/check/uninstall convergence for the service account, checkout, venv, config, ACL, kubeconfig, linger, and wrappers.
- `scripts/ops/staging_rollout_gb10_trust.py`: testable bootstrap/check/revoke convergence for the exact service public key over the existing admin channel.
- `scripts/ops/verify_staging_rollout_secret_boundary.py`: metadata-only live scan that reports counts/paths, never matched values.

Update tracked configuration and documentation:

- `deploy/environments/staging.cluster.toml` and `deploy/worker-pools/gb10/ssh_config`: service-owned deploy-key path.
- `deploy/environment-state/staging.toml`: service-private generated GB10 worker-env paths while retaining the protected catalog input as a read-only source.
- `docs/architecture/adr/independent-staging-rollout-runner.md`: durable decision and trust-boundary ADR.
- `docs/runbooks/operator-runbook.md`, `docs/runbooks/staging-launch.md`, and `deploy/worker-pools/gb10/README.md`: installation, operation, failure, resume, revocation, and rollback.

## Execution preflight (run once when implementation starts)

- [ ] Refresh `origin/dev`, verify this worktree is still based on the current trusted `qianyi-sun/loom` `dev`, and stop for rebase/conflict review if it is not.
- [ ] Change #803 to `[WIP] [P1][Infra/Auth] Normalize Loom operator identities and host-access preflight`, set its project item to In Progress, and comment that this branch advances only the approved execute-only staging-runner slice.
- [ ] Reconfirm that no repository command from this branch will connect to shared `platform-dev` staging before the implementation PR is merged.

---

### Task 1: Define strict configuration and immutable request persistence

**Files:**
- Create: `src/loom_cli/rollout/operator/__init__.py`
- Create: `src/loom_cli/rollout/operator/model.py`
- Create: `src/loom_cli/rollout/operator/config.py`
- Create: `src/loom_cli/rollout/operator/store.py`
- Create: `tests/loom_cli/rollout/operator/__init__.py`
- Create: `tests/loom_cli/rollout/operator/test_config.py`
- Create: `tests/loom_cli/rollout/operator/test_store.py`

**Interfaces:**
- Produces `OperatorConfig.load(path: Path, *, expected_owner_uid: int = 0) -> OperatorConfig`.
- Produces `CallerIdentity`, `CandidateBinding`, `RolloutRequest`, `AttemptIdentity`, `ActivePointer`, `DriverEnvelope`, and `RequestEvent` frozen dataclasses.
- Produces `RequestStore.create_request`, `append_event`, `publish_attempt_envelope`, `set_active`, `clear_active_if_matches`, `read_active`, `read_request`, `read_attempt_envelope`, and `next_attempt_number`.
- Every persisted object has `schema_version=1`; request and attempt IDs match `^[a-z0-9][a-z0-9-]{7,79}$`.
- `request.json` is the immutable pre-backup record of authenticated caller plus candidate binding. An attempt `envelope.json` is a distinct immutable post-backup driver input and may be published only after the exact manifest path and digest are known.

- [ ] **Step 1: Write failing config/model tests**

```python
def test_config_rejects_non_root_owned_or_writable_file(tmp_path, monkeypatch):
    path = tmp_path / "staging-rollout.toml"
    path.write_text(VALID_CONFIG, encoding="utf-8")
    path.chmod(0o666)
    with pytest.raises(ConfigError, match="group/world writable"):
        OperatorConfig.load(path, expected_owner_uid=os.getuid())


def test_driver_envelope_keeps_attempt_actor_out_of_immutable_inputs():
    envelope = make_driver_envelope(attempt_number=2, attempt_operator="devansh")
    immutable = envelope.rollout_inputs()
    assert immutable["request_id"] == envelope.request_id
    assert immutable["initiating_operator"] == "hongjian"
    assert "attempt_operator" not in immutable
    assert "attempt_number" not in immutable
```

- [ ] **Step 2: Run the tests and verify the missing package fails**

Run: `uv run pytest tests/loom_cli/rollout/operator/test_config.py -q`

Expected: FAIL during import because `loom_cli.rollout.operator.config` does not exist.

- [ ] **Step 3: Implement the strict dataclasses and TOML loader**

Use frozen, slotted dataclasses and literal command/status types. The immutable driver input projection must include the exact backup path and digest:

```python
@dataclass(frozen=True, slots=True)
class DriverEnvelope:
    schema_version: int
    request_id: str
    rollout_id: str
    initiating_operator: str
    initiating_uid: int
    attempt_number: int
    attempt_operator: str
    attempt_uid: int
    remote_url: str
    target_ref: str
    resolved_sha: str
    image_tag: str
    fetched_at: str
    backup_manifest_path: str
    backup_manifest_sha256: str
    runner_config_sha256: str
    cluster_name: str
    namespace: str
    environment: str
    cp_url: str
    cluster_config_path: str
    rollout_root: str
    admin_token_source: str
    worker_token_source: str
    service_token_source: str
    expect_admin_token_fingerprint: str
    smoke_on_behalf_username: str
    smoke_on_behalf_team_id: str
    scope: str
    gb10_prep_concurrency: int
    resume: bool

    def rollout_inputs(self) -> dict[str, object]:
        return {
            "request_id": self.request_id,
            "rollout_id": self.rollout_id,
            "initiating_operator": self.initiating_operator,
            "initiating_uid": self.initiating_uid,
            "remote_url": self.remote_url,
            "target_ref": self.target_ref,
            "resolved_sha": self.resolved_sha,
            "image_tag": self.image_tag,
            "backup_manifest_path": self.backup_manifest_path,
            "backup_manifest_sha256": self.backup_manifest_sha256,
            "runner_config_sha256": self.runner_config_sha256,
        }
```

The loader must reject unknown keys, missing keys, wrong literal values, relative protected paths, non-root ownership, non-regular files, group/world write bits, a remote other than the single approved URL, a ref other than `refs/heads/dev`, a service user other than `loom-rollout`, and an environment other than `staging`.

- [ ] **Step 4: Write failing store durability tests**

```python
def test_create_request_is_private_and_no_replace(tmp_path):
    store = RequestStore(tmp_path)
    request = make_request(request_id="stg-20260713-abcdef12")
    path = store.create_request(request)
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    with pytest.raises(RequestStoreError, match="already exists"):
        store.create_request(request)


def test_clear_active_is_compare_and_delete(tmp_path):
    store = RequestStore(tmp_path)
    first = ActivePointer("req-first", 1, "unit-first", "pending")
    second = ActivePointer("req-second", 1, "unit-second", "pending")
    store.set_active(first)
    assert store.clear_active_if_matches(second) is False
    assert store.read_active() == first
    assert store.clear_active_if_matches(first) is True
    assert store.read_active() is None
```

- [ ] **Step 5: Implement atomic, fsynced persistence**

`create_request` and `publish_attempt_envelope` must publish without replacement. Write a mode-0600 temporary file in the target directory, flush and `os.fsync` the file, atomically link it to the final name so an existing target fails, unlink the temporary file, then fsync the directory. Mutable `active.json` uses temp-file + `os.replace` + directory fsync. JSONL events use one `O_APPEND` write under a per-request flock and fsync before returning. Never accept a caller-provided filesystem path; derive every path from the validated request ID.

- [ ] **Step 6: Run Task 1 tests and commit**

Run: `uv run pytest tests/loom_cli/rollout/operator/test_config.py tests/loom_cli/rollout/operator/test_store.py -q`

Expected: PASS.

```bash
git add src/loom_cli/rollout/operator tests/loom_cli/rollout/operator
git commit -m "feat(rollout): add private operator request store"
```

---

### Task 2: Authenticate callers and bind only a fresh merged candidate

**Files:**
- Create: `src/loom_cli/rollout/operator/policy.py`
- Create: `src/loom_cli/rollout/operator/candidate.py`
- Create: `tests/loom_cli/rollout/operator/test_policy.py`
- Create: `tests/loom_cli/rollout/operator/test_candidate.py`

**Interfaces:**
- Consumes `OperatorConfig` and `CandidateBinding` from Task 1.
- Produces `caller_from_sudo(config, environ, *, euid, groups) -> CallerIdentity`.
- Produces `sanitized_child_environment(config, *, service_uid) -> dict[str, str]`.
- Produces `bind_fresh_origin_dev(config, *, run, now) -> CandidateBinding`.

- [ ] **Step 1: Write caller-authentication and environment tests**

```python
def test_caller_comes_from_sudo_identity_not_argv(monkeypatch):
    identity = caller_from_sudo(
        config=make_config(),
        environ={"SUDO_USER": "hongjian", "SUDO_UID": "2002", "ACTOR": "qianyi"},
        euid=SERVICE_UID,
        groups=lambda name: {"loom-staging-operators"},
    )
    assert identity.username == "hongjian"
    assert identity.uid == 2002


def test_child_environment_drops_injection_vectors():
    env = sanitized_child_environment(make_config(), service_uid=SERVICE_UID)
    assert env["PATH"] == "/opt/loom-staging-runner/venv/bin:/usr/local/bin:/usr/bin:/bin"
    assert "PYTHONPATH" not in env
    assert "GIT_CONFIG_GLOBAL" not in env
    assert "LD_PRELOAD" not in env
```

Also cover missing sudo metadata, username/UID mismatch, broker running as the wrong effective UID, caller absent from `loom-staging-operators`, root as an unapproved caller, and malformed numeric UID.

- [ ] **Step 2: Run the policy tests and verify failure**

Run: `uv run pytest tests/loom_cli/rollout/operator/test_policy.py -q`

Expected: FAIL because `policy.py` is absent.

- [ ] **Step 3: Implement caller and child-environment policy**

The broker must use only sudo-provided `SUDO_USER`/`SUDO_UID`, verify the pair against `pwd`, verify primary or supplementary membership in the fixed group, and never accept actor data in command arguments. The child environment contains only fixed `HOME`, `USER`, `LOGNAME`, `PATH`, `XDG_RUNTIME_DIR`, `DBUS_SESSION_BUS_ADDRESS`, `KUBECONFIG`, locale, and the root-owned config path.

- [ ] **Step 4: Write candidate-binding tests**

```python
def test_binding_fetches_exact_dev_before_resolve(fake_runner):
    binding = bind_fresh_origin_dev(make_config(), run=fake_runner, now=fixed_now)
    assert fake_runner.argvs == [
        ["git", "-C", RUNNER_REPO, "remote"],
        ["git", "-C", RUNNER_REPO, "remote", "get-url", "--all", "origin"],
        ["git", "-C", RUNNER_REPO, "config", "--get-all", "remote.origin.pushurl"],
        ["git", "-C", RUNNER_REPO, "status", "--porcelain=v1", "--untracked-files=all"],
        ["git", "-C", RUNNER_REPO, "fetch", "--force", "--no-tags", "--prune", "--no-recurse-submodules", "origin", "+refs/heads/dev:refs/remotes/origin/dev"],
        ["git", "-C", RUNNER_REPO, "rev-parse", "--verify", "refs/remotes/origin/dev^{commit}"],
    ]
    assert binding.target_ref == "origin/dev"
    assert binding.image_tag == f"staging-{binding.resolved_sha[:7]}"
```

Add failing cases for a cached stale ref, fetch failure, extra or wrong remote, dirty checkout, symlinked checkout/config, group/world-writable checkout or `.git`, unexpected owner UID, malformed SHA, and a ref/tag/remote argument appearing anywhere in the API.

- [ ] **Step 5: Implement exact candidate binding**

Validate trusted path ownership/modes before Git. Require `git remote` to return only `origin`, require its only fetch URL to equal the configured URL, reject any `remote.origin.pushurl`, require a clean checkout, fetch the exact fixed refspec with force/prune/no-tags/no-submodules, and accept only 40 lowercase hexadecimal characters from `rev-parse`. Build the image tag internally as `staging-<sha7>`.

- [ ] **Step 6: Run Task 2 tests and commit**

Run: `uv run pytest tests/loom_cli/rollout/operator/test_policy.py tests/loom_cli/rollout/operator/test_candidate.py -q`

Expected: PASS.

```bash
git add src/loom_cli/rollout/operator/policy.py src/loom_cli/rollout/operator/candidate.py tests/loom_cli/rollout/operator/test_policy.py tests/loom_cli/rollout/operator/test_candidate.py
git commit -m "feat(rollout): bind operator requests to fresh dev"
```

---

### Task 3: Add the full-lifecycle singleton and systemd boundary

**Files:**
- Create: `src/loom_cli/rollout/operator/lifecycle.py`
- Create: `src/loom_cli/rollout/operator/systemd.py`
- Create: `tests/loom_cli/rollout/operator/test_lifecycle.py`
- Create: `tests/loom_cli/rollout/operator/test_systemd.py`

**Interfaces:**
- Consumes `RequestStore`, `ActivePointer`, `OperatorConfig`.
- Produces `LifecycleCoordinator.launch_guard()`, `driver_guard()`, `reserve_active()`, `reconcile_active()`, and `release_active()`.
- Produces `SystemdUserManager.start_attempt(envelope_path, unit_name)`, `show(unit_name)`, `terminate(unit_name)`, and `stream_journal(unit_name, follow)`.

- [ ] **Step 1: Write failing singleton tests**

```python
def test_second_request_fails_even_when_image_tags_differ(tmp_path):
    coordinator = make_coordinator(tmp_path)
    coordinator.reserve_active(pointer("req-a", "staging-aaaaaaa"))
    with pytest.raises(LifecycleBusyError) as caught:
        coordinator.reserve_active(pointer("req-b", "staging-bbbbbbb"))
    assert caught.value.safe_status["request_id"] == "req-a"
    assert caught.value.safe_status["image_tag"] == "staging-aaaaaaa"


def test_failed_unit_launch_records_failure_and_clears_matching_pointer(tmp_path):
    coordinator = make_coordinator(tmp_path, systemd=FailingSystemd())
    with pytest.raises(UnitLaunchError):
        coordinator.launch(make_envelope())
    assert coordinator.store.read_active() is None
    assert coordinator.store.read_events("req-a")[-1].event == "launch_failed"
```

Cover stale active pointer plus missing unit, running unit, completed unit, PID/boot-ID mismatch, compare-and-delete protection, and independent release of the existing per-mutation lock while the lifecycle lock remains held.

- [ ] **Step 2: Run singleton tests and verify failure**

Run: `uv run pytest tests/loom_cli/rollout/operator/test_lifecycle.py -q`

Expected: FAIL because lifecycle coordination is absent.

- [ ] **Step 3: Implement launch and driver locks**

Use `/run/loom-staging-rollout/launch.lock` for the availability-check/fetch/backup/envelope/active-pointer/unit-launch critical section and `/run/loom-staging-rollout/staging.driver.lock` for the worker's entire driver lifetime. The active pointer is written as `pending` only after backup verification and immutable envelope publication, immediately before systemd launch. Backup failure therefore has no pointer to clear; unit-launch failure records a safe terminal event and compare-and-deletes only the matching pointer. Reconciliation combines the pointer, request/attempt records, unit status, and rollout `state.json`; it does not infer success from one stale source.

- [ ] **Step 4: Write and implement fixed systemd command tests**

The exact launch shape must be assembled as a list, never a shell string:

```python
expected = [
    "systemd-run", "--user", "--collect", "--service-type=exec",
    "--unit", "loom-staging-rollout-req-a-1.service",
    "--property", "UMask=0077",
    "--property", "WorkingDirectory=/opt/loom-staging-runner/repo",
    "/usr/bin/env", "-i",
    "HOME=/var/lib/loom-staging-rollout",
    "USER=loom-rollout", "LOGNAME=loom-rollout",
    "PATH=/opt/loom-staging-runner/venv/bin:/usr/local/bin:/usr/bin:/bin",
    f"XDG_RUNTIME_DIR=/run/user/{SERVICE_UID}",
    f"DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/{SERVICE_UID}/bus",
    "KUBECONFIG=/var/lib/loom-staging-rollout/kubeconfig",
    "LOOM_STAGING_ROLLOUT_CONFIG=/etc/loom/staging-rollout.toml",
    "/opt/loom-staging-runner/venv/bin/python", "-m",
    "loom_cli.rollout.operator.worker", "run-attempt",
    "--envelope", "/var/lib/loom-staging-rollout/requests/req-a/attempts/1/envelope.json",
]
assert manager.start_argv(envelope, unit) == expected
```

Reject unit/request IDs outside the safe grammar. `show` requests only `ActiveState`, `SubState`, `Result`, `ExecMainStatus`, `MainPID`, and timestamps. `terminate` sends normal SIGTERM and does not delete state/evidence.

- [ ] **Step 5: Run Task 3 tests and commit**

Run: `uv run pytest tests/loom_cli/rollout/operator/test_lifecycle.py tests/loom_cli/rollout/operator/test_systemd.py -q`

Expected: PASS.

```bash
git add src/loom_cli/rollout/operator/lifecycle.py src/loom_cli/rollout/operator/systemd.py tests/loom_cli/rollout/operator/test_lifecycle.py tests/loom_cli/rollout/operator/test_systemd.py
git commit -m "feat(rollout): serialize detached staging attempts"
```

---

### Task 4: Create and cryptographically revalidate protected backups

**Files:**
- Create: `src/loom_cli/rollout/operator/backup.py`
- Create: `tests/loom_cli/rollout/operator/test_backup.py`
- Modify: `src/loom_cli/cluster_backup_guard.py`
- Modify: `tests/loom_cli/test_cluster_backup_guard.py`

**Interfaces:**
- Produces `BackupCreator.create(request: RolloutRequest) -> VerifiedBackup`.
- `VerifiedBackup` contains the immutable manifest path and SHA-256.
- Extends `validate_backup_manifest` to recompute each component's type, byte count, file count when applicable, and SHA-256. New optional strict arguments (`expected_owner_uid`, `require_private_files`, and `enforce_freshness`) preserve existing callers while letting first launch enforce age plus ownership/mode and resume enforce the same immutable contents without a new age cutoff.

- [ ] **Step 1: Write tamper-detection tests for the current guard**

```python
def test_validate_backup_manifest_rejects_component_changed_after_manifest(tmp_path):
    manifest = make_valid_manifest(tmp_path)
    (tmp_path / "postgres.dump").write_bytes(b"changed-after-manifest")
    problems = validate_backup_manifest(
        manifest,
        environment="staging",
        namespace="loom-staging",
    )
    assert any("sha256 does not match" in problem for problem in problems)


def test_resume_integrity_check_rejects_symlink_but_not_age(tmp_path):
    manifest = make_old_valid_manifest(tmp_path)
    replace_component_with_symlink(manifest, "postgres")
    problems = validate_backup_manifest(
        manifest,
        environment="staging",
        namespace="loom-staging",
        expected_owner_uid=os.getuid(),
        require_private_files=True,
        enforce_freshness=False,
    )
    assert not any("stale" in problem for problem in problems)
    assert any("symlink" in problem for problem in problems)
```

Run: `uv run pytest tests/loom_cli/test_cluster_backup_guard.py -q`

Expected: FAIL because current validation trusts recorded metadata.

- [ ] **Step 2: Recompute component metadata during validation**

Reuse a hardened `_component_metadata`; compare `kind`, `size_bytes`, `file_count`, and `sha256` with the manifest. In strict mode, use `lstat`/no-follow traversal to reject a symlinked manifest, component root, or nested file, require the service owner, mode 0600 files, and mode 0700 component directories. Return safe component-name diagnostics only; never include file contents.

- [ ] **Step 3: Write backup-coordinator failure-order tests**

```python
def test_partial_backup_never_publishes_latest_or_returns_manifest(tmp_path):
    creator = make_creator(tmp_path, minio=FailingMinioMirror())
    with pytest.raises(BackupError, match="minio_snapshot_failed"):
        creator.create(make_request())
    assert not (tmp_path / "backups" / "latest").exists()
    assert list((tmp_path / "backups").glob("*/backup-manifest.json")) == []


def test_backup_commands_never_put_credentials_in_argv(tmp_path):
    recorder = RecordingRunner()
    creator = make_creator(tmp_path, runner=recorder, secret_values=SECRET_VALUES)
    creator.create(make_request())
    rendered = json.dumps(recorder.argvs)
    for value in SECRET_VALUES:
        assert value not in rendered
```

- [ ] **Step 4: Implement backup creation in a private timestamped root**

Use a mode-0700 root under `/data/loom-staging/backups/<UTC>-<request-id>/`:

1. Stream `kubectl -n loom-staging exec statefulset/loom-postgres -- sh -ceu 'exec pg_dump -Fc -U "$POSTGRES_USER" -d "$POSTGRES_DB"'` directly to `postgres/loom.dump`; do not capture binary data in logs.
2. Read `minio-access-key` and `minio-secret-key` from `loom-secrets` in memory, start a bounded localhost-only `kubectl port-forward service/loom-minio`, and use boto3 to mirror the configured `loom-staging-trajectories` and `loom-staging-artifacts` buckets. Credentials never enter argv or an environment dump.
3. Export only configured restore Secrets (`loom-secrets`, `loom-admin-secret`, `loom-staging-tls`) into mode-0600 YAML files without printing stdout.
4. Call `write_backup_manifest`, call the strengthened `validate_backup_manifest`, compute the manifest SHA-256, and atomically replace the relative `latest` symlink only after every check passes.
5. Stop the port-forward in `finally`. Preserve a failed private component directory for diagnosis but do not publish it or delete the prior valid backup.

- [ ] **Step 5: Test the successful bundle and exact resume pin**

Assert the returned path is the timestamped manifest, not `backups/latest/backup-manifest.json`; assert the digest matches; assert the broker finalizes attempt 1 only after this return; assert a second attempt reads the original envelope and does not call `BackupCreator.create` again.

- [ ] **Step 6: Run Task 4 tests and commit**

Run: `uv run pytest tests/loom_cli/test_cluster_backup_guard.py tests/loom_cli/rollout/operator/test_backup.py -q`

Expected: PASS.

```bash
git add src/loom_cli/cluster_backup_guard.py src/loom_cli/rollout/operator/backup.py tests/loom_cli/test_cluster_backup_guard.py tests/loom_cli/rollout/operator/test_backup.py
git commit -m "feat(rollout): create and pin staging backups"
```

---

### Task 5: Require the broker envelope and carry attribution through rollout evidence

**Files:**
- Create: `src/loom_cli/rollout/operator/envelope.py`
- Create: `src/loom_cli/rollout/operator/redaction.py`
- Create: `tests/loom_cli/rollout/operator/test_envelope.py`
- Create: `tests/loom_cli/rollout/operator/test_redaction.py`
- Modify: `src/loom_cli/rollout/cli.py`
- Modify: `src/loom_cli/rollout/context.py`
- Modify: `src/loom_cli/rollout/state.py`
- Modify: `src/loom_cli/rollout/evidence.py`
- Modify: `src/loom_cli/rollout/driver.py`
- Modify: `src/loom_cli/rollout/steps/candidate_source.py`
- Modify: `src/loom_cli/rollout/steps/subprocess_util.py`
- Modify: `src/loom_cli/rollout/steps/s10_env_state.py`
- Modify: `src/loom_cli/rollout/steps/s11_cluster_up.py`
- Modify: `src/loom_cli/rollout/steps/s99_summary.py`
- Modify: `src/loom_cli/rollout_lock.py`
- Modify: `src/loom_cli/admin_cmd.py`
- Modify: `src/loom_cli/cluster_cmd.py`
- Modify: `tests/loom_cli/rollout/test_cli.py`
- Modify: `tests/loom_cli/rollout/test_driver.py`
- Modify: `tests/loom_cli/rollout/test_state.py`
- Modify: `tests/loom_cli/rollout/test_evidence.py`
- Modify: `tests/loom_cli/rollout/steps/test_candidate_source_invocation.py`
- Modify: `tests/loom_cli/test_rollout_lock.py`
- Modify: `tests/loom_cli/test_rollout_lock_cli.py`

**Interfaces:**
- Consumes `DriverEnvelope` and `OperatorConfig`.
- Produces `load_validated_envelope(path, config, *, effective_uid) -> DriverEnvelope`.
- Adds optional typed request/original-initiator/current-attempt fields to `RolloutContext` and `RolloutState`.
- Adds structured `request_id`, `initiating_operator`, `initiating_uid`, `attempt_number`, `attempt_operator`, and `attempt_uid` to mutation-lock evidence.

- [ ] **Step 1: Write protected-driver envelope tests**

```python
def test_non_dry_staging_refuses_before_evidence_without_envelope(tmp_path, monkeypatch):
    rc = invoke_rollout(staging_args(tmp_path), dry_run=False)
    assert rc == 2
    assert "broker-created request envelope is required" in captured_stderr()
    assert not (tmp_path / "rollouts").exists()


def test_envelope_mode_rejects_manual_candidate_overrides(tmp_path):
    rc = invoke_rollout([
        "staging", "--request-envelope", str(valid_envelope(tmp_path)),
        "--ref", "feature/unmerged",
    ])
    assert rc == 2
    assert "manual rollout overrides are forbidden in envelope mode" in captured_stderr()
```

Also reject envelope symlinks, wrong UID/mode/path root, wrong config hash, wrong remote/ref/environment, mismatched `staging-<sha7>`, replaced backup manifest, and missing backup component. Validate all of these before `EvidenceDirectory.ensure()`.

- [ ] **Step 2: Run focused CLI tests and verify failure**

Run: `uv run pytest tests/loom_cli/rollout/operator/test_envelope.py tests/loom_cli/rollout/test_cli.py -q`

Expected: FAIL on the new envelope requirements.

- [ ] **Step 3: Implement envelope-mode context construction**

Add a hidden `--request-envelope` argument and make manual `--ref` conditionally required. In envelope mode, construct every protected staging input from the validated envelope and repo preset; permit only the positional `staging`, the envelope path, and `--resume`. Use the envelope's exact rollout ID rather than image-tag discovery. Manual non-staging diagnostics and dry-run behavior remain backward compatible.

Update `RolloutContext.to_inputs_dict()` to include `request_id`, original initiator, exact backup manifest path, backup manifest digest, and runner config digest. Exclude current attempt number/operator so another authorized operator can resume the same immutable rollout inputs.

- [ ] **Step 4: Make state version 2 backward compatible**

`RolloutState.from_dict` must load version 1 with null request attribution, while every new save writes version 2. `DriverRecord` carries the current attempt number/operator/UID. Existing historical evidence remains readable; unknown versions still fail closed.

- [ ] **Step 5: Propagate attribution into mutation-lock evidence**

Add a hidden `--rollout-request-envelope` option to protected `cluster up` and `admin environment-state apply/check`. Load the same private envelope, then pass structured attribution to `RolloutLeaseManager.acquire`. The existing manual `--rollout-id` remains available for non-broker diagnostics, but broker steps pass the envelope and the lock event records both original and current-attempt identities.

`EnvStateStep` and `ClusterUpStep` pass the validated envelope path plus a rollout-local lock evidence path. They do not accept or synthesize an actor string from user argv.

- [ ] **Step 6: Bound candidate subprocesses and redact all persisted output**

Replace the candidate-source copy of `os.environ` with the broker's fixed allowlist and explicit non-secret rollout variables; drop `PYTHONPATH`, `PYTHONHOME`, Git overrides, loader injection, credential helpers, and inherited `.env` values. Before persisting subprocess stdout/stderr, `StepRecord.error`, `result.json.error`, or summary table text, apply exact-known-secret replacement followed by central redaction. Add request ID, original initiator, current attempt/operator, SHA, and image tag to `summary.md`; do not include token paths, raw exception payloads, or full environments. Sentinel tests cover raw token values, PEM markers/bodies, credential-bearing URLs, and multiline stderr.

- [ ] **Step 7: Run rollout and lock regressions**

Run:

```bash
uv run pytest \
  tests/loom_cli/rollout/operator/test_envelope.py \
  tests/loom_cli/rollout/operator/test_redaction.py \
  tests/loom_cli/rollout/test_cli.py \
  tests/loom_cli/rollout/test_driver.py \
  tests/loom_cli/rollout/test_state.py \
  tests/loom_cli/rollout/test_evidence.py \
  tests/loom_cli/rollout/steps/test_candidate_source_invocation.py \
  tests/loom_cli/test_rollout_lock.py \
  tests/loom_cli/test_rollout_lock_cli.py -q
```

Expected: PASS, including backward-compatible version-1 state fixtures.

- [ ] **Step 8: Commit the driver integration**

```bash
git add src/loom_cli/rollout src/loom_cli/rollout_lock.py src/loom_cli/admin_cmd.py src/loom_cli/cluster_cmd.py tests/loom_cli/rollout tests/loom_cli/test_rollout_lock.py tests/loom_cli/test_rollout_lock_cli.py
git commit -m "feat(rollout): require attributable staging envelopes"
```

---

### Task 6: Implement start/status/logs/resume/cancel and the detached worker

**Files:**
- Create: `src/loom_cli/rollout/operator/preflight.py`
- Create: `src/loom_cli/rollout/operator/broker.py`
- Create: `src/loom_cli/rollout/operator/worker.py`
- Create: `tests/loom_cli/rollout/operator/test_preflight.py`
- Create: `tests/loom_cli/rollout/operator/test_broker.py`
- Create: `tests/loom_cli/rollout/operator/test_worker.py`

**Interfaces:**
- Public surface is exactly `start [--dry-run]`, `status [REQUEST_ID]`, `logs REQUEST_ID [--follow]`, `resume REQUEST_ID`, `cancel REQUEST_ID --reason TEXT`.
- `worker run-attempt --envelope PATH` is a service-only internal surface.
- `PreflightReport` contains only named pass/fail checks and safe remediation.

- [ ] **Step 1: Write parser rejection tests**

```python
@pytest.mark.parametrize("argv", [
    ["start", "--ref", "origin/dev"],
    ["start", "--image-tag", "staging-deadbee"],
    ["start", "--config", "/tmp/config"],
    ["start", "--force"],
    ["resume", "req-a", "--ref", "origin/dev"],
    ["cancel", "req-a"],
])
def test_public_surface_rejects_unapproved_arguments(argv):
    assert broker_main(argv, dependencies=fakes()) == 2
```

Also require a non-empty, length-bounded cancellation reason; reject unknown request IDs, preview resume, resume of a done request, cancel of a terminal request, and a start/resume while another request owns staging.

- [ ] **Step 2: Write start and dry-run orchestration tests**

```python
def test_dry_run_fetches_and_records_preview_without_backup_unit_or_rollout(tmp_path):
    deps = fakes(tmp_path)
    rc = broker_main(["start", "--dry-run"], dependencies=deps)
    assert rc == 0
    assert deps.candidate.fetch_count == 1
    assert deps.backup.create_count == 0
    assert deps.systemd.start_count == 0
    assert deps.store.read_active() is None
    assert deps.store.latest_request().status == "preview"


def test_start_reserves_before_launch_and_returns_detached_request(tmp_path):
    deps = fakes(tmp_path)
    rc = broker_main(["start"], dependencies=deps)
    assert rc == 0
    assert deps.order == [
        "preflight", "fetch", "request", "backup-create",
        "envelope-finalize", "active", "systemd",
    ]


def test_backup_failure_never_publishes_envelope_or_starts_unit(tmp_path):
    deps = fakes(tmp_path, backup=FailingBackup())
    assert broker_main(["start"], dependencies=deps) == 1
    assert deps.systemd.start_count == 0
    assert deps.store.read_active() is None
    assert deps.store.attempt_envelopes("req-a") == []
    assert deps.store.read_events("req-a")[-1].event == "backup_failed"
```

- [ ] **Step 3: Implement redacted preflight and broker orchestration**

Preflight verifies checkout ownership/cleanliness, fixed origin, required executables and Python imports, Docker and `docker buildx`, kube context/namespace, data-root read/write/traverse, credential readability and expected fingerprints, catalog env readability, backup commands, all 14 active GB10 BatchMode connections with the service key, and the exact full-15 SSH topology. It reports fingerprints only as `sha256:<12-hex> len=<N>` and never variable values.

`start` authenticates first, acquires the non-blocking launch mutex, reconciles singleton availability, runs preflight, fresh-fetches, and creates an immutable request. It then either records a non-active preview or creates and verifies the backup synchronously. A real start publishes the finalized attempt-1 driver envelope, reserves the active pointer, and only then launches systemd. Backup failure records a safe terminal event and starts no unit or active pointer. A successful start returns request ID, SHA, image tag, unit, and safe status.

- [ ] **Step 4: Write worker ordering and resume tests**

```python
def test_worker_holds_lifecycle_lock_and_runs_only_finalized_envelope(tmp_path):
    deps = worker_fakes(tmp_path)
    assert run_attempt(valid_envelope(tmp_path), deps) == 0
    assert deps.order == [
        "driver-lock-acquire", "attempt-running", "driver-run", "attempt-done",
        "active-clear", "driver-lock-release",
    ]
    assert deps.backup.create_count == 0


def test_resume_reuses_original_sha_backup_and_rollout_id(tmp_path):
    deps = worker_fakes(tmp_path, original=failed_request())
    broker_main(["resume", "req-a"], dependencies=deps.broker)
    envelope = deps.store.read_attempt_envelope("req-a", 2)
    assert envelope.resolved_sha == ORIGINAL_SHA
    assert envelope.backup_manifest_path == ORIGINAL_BACKUP
    assert envelope.rollout_id == ORIGINAL_ROLLOUT_ID
    assert envelope.resume is True
    assert deps.backup.create_count == 0
```

The broker creates and verifies the backup before the first transient unit exists. The worker accepts only a finalized, service-owned envelope and never creates or changes a backup. Resume does not refetch, retag, recreate backup, change config, or edit `state.json`.

- [ ] **Step 5: Implement safe status/log/cancel behavior**

`status` without an ID shows the reconciled active request or the latest request. `logs` reads only the request's known unit and rollout log, applies exact-known-secret replacement followed by `loom.security.redaction.redact_text`, and supports line-buffered follow mode. `cancel` writes an authenticated cancel-request event with bounded reason, sends SIGTERM to the fixed unit, and leaves rollout evidence intact. Worker terminal bookkeeping observes the cancel marker and records `cancelled` without rewriting rollout state.

- [ ] **Step 6: Run operator integration tests and commit**

Run:

```bash
uv run pytest \
  tests/loom_cli/rollout/operator/test_preflight.py \
  tests/loom_cli/rollout/operator/test_broker.py \
  tests/loom_cli/rollout/operator/test_worker.py -q
```

Expected: PASS.

```bash
git add src/loom_cli/rollout/operator tests/loom_cli/rollout/operator
git commit -m "feat(rollout): add independent staging broker"
```

---

### Task 7: Add reproducible service installation and GB10 trust convergence

**Files:**
- Create: `deploy/staging-rollout/staging-rollout.toml`
- Create: `deploy/staging-rollout/loom-staging-rollout`
- Create: `deploy/staging-rollout/loom-staging-rollout-broker`
- Create: `deploy/staging-rollout/loom-staging-rollout.sudoers`
- Create: `scripts/ops/staging_rollout_host.py`
- Create: `scripts/ops/staging_rollout_gb10_trust.py`
- Create: `scripts/ops/verify_staging_rollout_secret_boundary.py`
- Create: `tests/ops/test_staging_rollout_install_assets.py`
- Create: `tests/ops/test_staging_rollout_host.py`
- Create: `tests/ops/test_staging_rollout_gb10_trust.py`
- Modify: `deploy/environments/staging.cluster.toml`
- Modify: `deploy/environment-state/staging.toml`
- Modify: `deploy/worker-pools/gb10/ssh_config`
- Modify: `.github/CODEOWNERS`
- Modify: `scripts/plan_ci_validations.py`
- Modify: `tests/ops/test_plan_ci_validations.py`

**Interfaces:**
- Host tool exposes `plan`, `install --smoke-on-behalf-team-id UUID`, `check --format json`, and `uninstall --retain-ledger`; repository/ref/path are fixed.
- Trust tool exposes `bootstrap --bootstrap-identity PATH`, `check`, and `revoke`; it accepts no target host/ref/user override, and hosts plus remote user come from the checked-in GB10 config.
- Revocation removes only the exact service public key.
- Uninstall preserves request/rollout evidence by default and refuses while active.

- [ ] **Step 1: Write static and dry-run installation tests**

```python
def test_client_and_sudoers_fix_the_broker_command():
    client = Path("deploy/staging-rollout/loom-staging-rollout").read_text()
    sudoers = Path("deploy/staging-rollout/loom-staging-rollout.sudoers").read_text()
    assert "sudo -n -u loom-rollout -- /usr/local/libexec/loom-staging-rollout-broker" in client
    assert "%loom-staging-operators ALL=(loom-rollout) NOPASSWD:NOSETENV:" in sudoers
    assert "/usr/local/libexec/loom-staging-rollout-broker *" in sudoers


def test_repo_configs_no_longer_reference_qianyi_private_deploy_key():
    files = [
        Path("deploy/environments/staging.cluster.toml"),
        Path("deploy/worker-pools/gb10/ssh_config"),
    ]
    for path in files:
        text = path.read_text(encoding="utf-8")
        assert "staging-gb10-rollout-ed25519" not in text
        assert "/var/lib/loom-staging-rollout/gb10-deploy-ed25519" in text

    env_state = Path("deploy/environment-state/staging.toml").read_text(
        encoding="utf-8"
    )
    assert "/shared_work/qianyi/loom-worker-capacity/staging-gb10-worker-" not in env_state
    assert "/var/lib/loom-staging-rollout/generated/staging-gb10-worker-" in env_state


def test_privileged_runner_paths_have_codeowners_and_full_ci_selection():
    owners = Path(".github/CODEOWNERS").read_text(encoding="utf-8")
    assert "/deploy/staging-rollout/" in owners
    assert "/src/loom_cli/rollout/operator/" in owners
    plan = plan_validations(
        changed_paths=["deploy/staging-rollout/loom-staging-rollout.sudoers"],
        labels=set(),
        event_name="pull_request",
    )
    assert plan.selected_heavy_checks() == set(HEAVY_CHECKS)
    assert plan.unowned_runtime is False
```

Run: `uv run pytest tests/ops/test_staging_rollout_install_assets.py -q`

Expected: FAIL because the assets do not exist.

- [ ] **Step 2: Implement the root-owned wrappers and sudoers rule**

The user client executes the single libexec broker through sudo without preserving environment. The libexec wrapper validates that sudo supplied `SUDO_USER`, `SUDO_UID`, and `SUDO_GID`, then execs the fixed Python module through `/usr/bin/env -i`, forwarding only those authenticated attribution fields plus fixed `HOME`, `USER`, `LOGNAME`, `PATH`, `XDG_RUNTIME_DIR`, `DBUS_SESSION_BUS_ADDRESS`, locale, kubeconfig, and config path. Both installed files are root-owned and non-writable by the operator group or service account. Validate sudoers with `visudo -cf` before atomic installation.

- [ ] **Step 3: Implement idempotent installation**

The installer must:

1. Require root and Ubuntu/systemd prerequisites.
2. Create `loom-staging-operators` and add `qianyi`, `hongjian`, and `devansh` without removing existing groups.
3. Create system user `loom-rollout` with home `/var/lib/loom-staging-rollout` and shell `/usr/sbin/nologin`; add it to `docker` only. Grant protected-input access with explicit named ACLs rather than assuming or broadening a shared secrets group.
4. Create root-owned `/opt/loom-staging-runner`, `/etc/loom`, `/usr/local/libexec`, and the installed venv, plus service-owned candidate checkout, private state, and `/run/loom-staging-rollout` runtime files with exact modes.
5. Clone only `https://github.com/qianyi-sun/loom.git` into the candidate checkout, verify the installer source commit is already reachable from `origin/dev`, and install the broker/runtime into the root-owned venv with `uv sync --no-editable --extra cluster --extra rollout --python 3.11`. Never accept a ref or local source checkout. Subsequent candidate fetches cannot modify the installed package or venv.
6. Generate the service Ed25519 key only if absent, mode 0600, and print only its fingerprint.
7. Install the fixed client/libexec/sudoers/config, compute redacted token fingerprints, record the supplied smoke team ID, and export the `loom-staging` kubeconfig to a mode-0600 service path. Apply named `loom-rollout` read/traverse ACLs only to the declared admin/service/worker/taskset-token and catalog source paths, and named/default `rwx` ACLs only to required `/data/loom-staging` directories. Generated GB10 worker env files live under the private service state root, not the shared Qianyi directory. Do not change existing ownership or expose secret contents.
8. Enable linger only for `loom-rollout`, verify the user manager, and run broker `start --dry-run` as a post-install check after GB10 trust has been installed.

Implement command execution behind an injected runner and filesystem/passwd/group adapters. Tests use a fake root plus recorded argv to prove a second install is a no-op, a failed validation never replaces sudoers/client/config, exact existing owner/mode/ACL entries survive, and uninstall refuses an active request while retaining the ledger.

- [ ] **Step 4: Implement service public-key trust bootstrap and revocation**

Bootstrap reads the service `.pub` file and sends it over stdin to a fixed
remote script for the exact 14-host active set under #822; it never prints the
key. The full 15-host topology remains fixed for validation and legacy-ledger
revocation/migration. The remote script inserts one comment-marked key
idempotently, preserves other `authorized_keys` entries, and enforces
directory/file modes. Revocation removes only the line whose decoded key
fingerprint matches the service public key and fails on ambiguity.

Test the exact active-14 bootstrap/check expansion, full-15 topology and legacy
revocation expansion, a repeated bootstrap, preservation of unrelated
keys/comments, partial-host failure reporting, ambiguous fingerprint refusal,
and exact-key revocation with fake SSH transports. No test fixture contains a
real private key.

- [ ] **Step 5: Implement safe uninstall and secret-boundary verification**

Uninstall first disables new broker admission, reconciles active state, and refuses if an attempt is pending/running. It removes wrappers, sudoers, group membership, linger, token/data ACLs, kubeconfig, and service GB10 trust in documented order. Request and rollout evidence remain unless a separate explicit archival decision is made.

The verifier reads configured secret values in memory and scans process argv, journal export, request ledger, rollout evidence, and summary. It outputs only artifact path, bytes scanned, and match count; a nonzero count exits 1 without printing the matched value or surrounding line.

- [ ] **Step 6: Protect the new authority paths and select full CI**

Add CODEOWNERS entries for the operator package, staging-rollout deploy assets, host convergence scripts, and their contract tests using the repository's existing governance owners. Extend `plan_ci_validations.py` so any of those privileged runtime paths selects all five heavy checks with an explicit `protected-staging-rollout` reason and is not reported as an unowned runtime path. Add positive tests for every prefix and a nearby non-matching path.

- [ ] **Step 7: Validate scripts and commit**

Run:

```bash
bash -n deploy/staging-rollout/loom-staging-rollout
bash -n deploy/staging-rollout/loom-staging-rollout-broker
visudo -cf deploy/staging-rollout/loom-staging-rollout.sudoers
uv run pytest tests/ops/test_staging_rollout_install_assets.py tests/ops/test_staging_rollout_host.py tests/ops/test_staging_rollout_gb10_trust.py tests/unit/test_gb10_systemd_templates.py -q
uv run pytest tests/ops/test_plan_ci_validations.py tests/unit/test_repo_governance_templates.py -q
uv run ruff check scripts/ops/staging_rollout_host.py scripts/ops/staging_rollout_gb10_trust.py scripts/ops/verify_staging_rollout_secret_boundary.py tests/ops/test_staging_rollout_*.py
```

Expected: every command exits 0.

```bash
git add .github/CODEOWNERS deploy/staging-rollout deploy/environments/staging.cluster.toml deploy/environment-state/staging.toml deploy/worker-pools/gb10/ssh_config scripts/ops scripts/plan_ci_validations.py tests/ops/test_staging_rollout_install_assets.py tests/ops/test_staging_rollout_host.py tests/ops/test_staging_rollout_gb10_trust.py tests/ops/test_plan_ci_validations.py tests/unit/test_gb10_systemd_templates.py tests/unit/test_repo_governance_templates.py
git commit -m "feat(ops): install the staging rollout service"
```

---

### Task 8: Document the durable boundary and operator runbook

**Files:**
- Create: `docs/architecture/adr/independent-staging-rollout-runner.md`
- Modify: `docs/runbooks/operator-runbook.md`
- Modify: `docs/runbooks/staging-launch.md`
- Modify: `deploy/worker-pools/gb10/README.md`
- Modify: `docs/superpowers/specs/2026-07-13-independent-staging-rollout-design.md`
- Modify: `tests/ops/test_rollout_dependency_contract.py`

- [ ] **Step 1: Scan all Markdown references before editing**

Run:

```bash
rg --files -g '*.md'
rg -n 'loom cluster rollout staging|systemd-run --user|staging-gb10-rollout-ed25519|Qianyi.*rollout|backup-manifest=/data/loom-staging/backups/latest' -g '*.md'
```

Expected: the current Qianyi-owned invocation and manual backup language is found in the operator and GB10 runbooks.

- [ ] **Step 2: Write the ADR**

Record context, accepted decision, operational/root-equivalent trust boundary, strict merged-only candidate semantics, separate lifecycle/per-mutation locks, backup-before-mutation rule, service deploy identity, request/attempt attribution, rejected shared-key/personal-runner options, rollback, and tests/live gates. Mark it Accepted with date 2026-07-13 and reference #803.

- [ ] **Step 3: Replace manual staging instructions with the five-command surface**

Document install/update, `start --dry-run`, `start`, `status`, `logs`, `resume`, and reasoned `cancel`; include safe failure diagnostics and exact rollback/revocation order. Remove the Qianyi-owned `systemd-run`, mutable `latest` manifest, manual ref/tag override, and private-key sharing as supported paths. Keep a clearly marked root break-glass path that still requires the original service-owned envelope and pinned SHA.

- [ ] **Step 4: Add documentation contract tests**

Assert the runbook names every public command, says `origin/dev` is fresh-fetched, forbids unmerged refs, preserves the full 15-host inventory while requiring the #822 active 14-host set, and no longer instructs operators to use Qianyi's key/unit. Assert the design status becomes `approved for implementation` until code/live acceptance is complete.

- [ ] **Step 5: Run doc contracts and commit**

Run: `uv run pytest tests/ops/test_rollout_dependency_contract.py -q`

Expected: PASS.

```bash
git add docs/architecture/adr/independent-staging-rollout-runner.md docs/runbooks/operator-runbook.md docs/runbooks/staging-launch.md deploy/worker-pools/gb10/README.md docs/superpowers/specs/2026-07-13-independent-staging-rollout-design.md tests/ops/test_rollout_dependency_contract.py
git commit -m "docs(rollout): document independent staging operation"
```

---

### Task 9: Run repository verification and merge before shared-staging use

**Files:**
- Modify only files required by failures whose root cause is inside this plan.
- Do not connect the branch checkout to shared `platform-dev` staging.

- [ ] **Step 1: Run focused operator/rollout tests**

Run:

```bash
uv run pytest \
  tests/loom_cli/rollout/operator \
  tests/loom_cli/rollout \
  tests/loom_cli/test_cluster_backup_guard.py \
  tests/loom_cli/test_rollout_lock.py \
  tests/loom_cli/test_rollout_lock_cli.py \
  tests/ops/test_staging_rollout_install_assets.py \
  tests/ops/test_staging_rollout_host.py \
  tests/ops/test_staging_rollout_gb10_trust.py \
  tests/ops/test_plan_ci_validations.py \
  tests/unit/test_repo_governance_templates.py \
  tests/ops/test_rollout_dependency_contract.py -q
```

Expected: PASS with zero failures.

- [ ] **Step 2: Run static checks and shell validation**

Run:

```bash
uv run ruff check src/loom_cli/rollout src/loom_cli/rollout_lock.py src/loom_cli/admin_cmd.py src/loom_cli/cluster_cmd.py scripts/ops/staging_rollout_host.py scripts/ops/staging_rollout_gb10_trust.py scripts/ops/verify_staging_rollout_secret_boundary.py tests/loom_cli/rollout tests/ops/test_staging_rollout_*.py
uv run mypy src/loom_cli/rollout/operator src/loom_cli/rollout/context.py src/loom_cli/rollout/state.py
git diff --check origin/dev...HEAD
```

Expected: every command exits 0.

- [ ] **Step 3: Run the isolated CI-equivalent gates**

Run integration and ephemeral kind checks selected by the changed paths. They may build images and create a throwaway kind cluster, but they must not call the shared staging broker or deploy to `platform-dev`.

- [ ] **Step 4: Perform a secret and policy review**

Verify no private key, token value, provider secret, host-local `.env`, kubeconfig, or generated backup/evidence is tracked. Search for forbidden candidate inputs and the old key path. Review every subprocess argv and request/status serialization field.

- [ ] **Step 5: Push and open the PR against `dev`**

Confirm `origin` is `https://github.com/qianyi-sun/loom.git`, push the branch, open a PR with `Advances #803`, add every CI label selected by repository rules, and enable squash auto-merge immediately. The PR description must state: shared staging has not been touched, live acceptance is post-merge only, and #803 remains open.

- [ ] **Step 6: Wait for current-head CI and merge**

Inspect every required check and resolve failures at root cause. Do not manually merge. Record the merged `dev` SHA; that SHA is the first candidate allowed for live runner installation and shared-staging acceptance.

---

### Task 10: Install from merged `dev` and perform live acceptance

**Files:**
- No unmerged repository files.
- Live evidence only under `/var/lib/loom-staging-rollout` and `/data/loom-staging`.
- Sanitized acceptance summary in #803.

- [ ] **Step 1: Prove the installation source is merged**

On `platform-dev`, fetch `origin`, verify the implementation PR's squash-merge SHA is reachable from `origin/dev`, then run the installer from a clean checkout of the exact current `origin/dev` and record that installation SHA. Do not copy the feature worktree or select the PR branch.

- [ ] **Step 2: Converge service identity and GB10 public-key trust**

Generate the service key through the installer, use the existing admin identity only for the one-time public-key bootstrap, and run service preflight. Required result: Docker/Buildx, kube context, data roots, protected source fingerprints, backup tools, and all 14 active GB10 SSH checks pass while the exact 15-host topology validates; the private key remains owner-only.

- [ ] **Step 3: Prove distinct operator attribution with dry runs**

Hongjian and Devansh each invoke `loom-staging-rollout start --dry-run` from their own authenticated OS session. Verify the request records have distinct username/UID pairs and both bind the same freshly fetched merged `origin/dev` SHA when dev has not moved between calls. If dev moves, rerun both against the new head and retain the earlier previews as non-active evidence.

- [ ] **Step 4: Prove authorization and singleton rejection**

Verify a non-member cannot enter the broker. Start one real request, then have the other operator issue a simultaneous start and confirm it fails with only safe active request details. Do not queue or preempt.

- [ ] **Step 5: Prove detached operation and cross-operator observation**

The initiating operator disconnects after `start` returns. Reconnect and verify `status`/`logs`; the other operator must be able to inspect the same request without Qianyi starting a process, providing a credential, refreshing a backup, or opening a terminal. Do not manufacture a driver/pod failure, edit `state.json`, or cancel a healthy rollout only to exercise resume.

- [ ] **Step 6: Verify the full existing rollout gate**

Require every existing rollout step to complete, all 14 active GB10 hosts to converge, node 7 to remain stopped/unreachable, environment-state/catalog/release-gate to pass, and admin-on-behalf smoke to succeed. Correlate request ID, attempt, rollout ID, SHA, image tag, unit, state, lock events, and summary.

- [ ] **Step 7: Run the live secret-boundary scan**

Run the metadata-only verifier against process argv, journald export, request ledger, rollout evidence, and summary. Required result: zero raw matches for every configured admin/service/worker/catalog/GB10 private source. Do not paste source values or matched context.

- [ ] **Step 8: Record the result without closing #803**

Comment on #803 with merged SHA, request/rollout IDs, both dry-run identities,
singleton rejection, exact active-14 convergence plus the fixed full-15
topology digest, node 7 stopped/unreachable evidence, release-gate/smoke
result, and zero-match secret scan. Remove `[WIP]` and return the Project card
to the correct non-active state if the broader identity inventory/rotation
work is not immediately continuing. Keep #803 open and name its remaining
OLDLAB/GB10 identity/bootstrap scope.

- [ ] **Step 9: Cleanup review**

Remove only process artifacts created for installation/testing: temporary bootstrap copies, failed port-forward sockets/processes, temporary kube exports, dry-run scratch output, and local QA logs. Preserve the checked-in plan/ADR/scripts, request and rollout evidence, verified backups, service key, installed runner, and all user-provided files.

---

## Completion gates

Repository implementation is ready for live installation only when Tasks 1-9 pass and the PR is merged into `dev`. The operator-independence slice is accepted only when Task 10 proves both operators' authenticated dry-run attribution, a real detached merged-code rollout, singleton rejection, all existing rollout gates, and zero raw secret leakage. #803 remains open until its separate canonical identity inventory/bootstrap/rotation acceptance is also implemented or explicitly split into a linked issue.
