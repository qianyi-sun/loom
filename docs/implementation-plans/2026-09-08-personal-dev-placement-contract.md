# Personal-dev placement contract implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the non-executable node eligibility and allocation identity boundary required by dynamically scheduled personal builds.

**Architecture:** Pure immutable Python domain records and deterministic eligibility filtering. Preserve the existing v1 provider unchanged. The surrounding dynamic-provider roadmap remains in the spec; this plan does not claim executable scheduling or goal completion.

**Tech Stack:** Python 3.11, dataclasses, pytest, Ruff.

**Spec:** `docs/architecture/personal-dev-dynamic-build-placement.md`

## Global Constraints

- No live infrastructure, credentials, scheduler submission, runtime activation, or capacity mutation.
- Initial compute eligibility is a subset of `trt-gb10-3` through `trt-gb10-15`; node 1 and node 2 remain excluded.
- Existing native v1 code, profiles and production composition remain unchanged.
- Input parsing does not authenticate observations; allocation identity is not a launch permit.
- Disable Python bytecode writes. Use the existing environment with this worktree's `src` first.

### Task 1: Pure placement and allocation identity contracts

**Files:**
- Create: `src/loom/personal_dev_build_placement.py`
- Create: `tests/unit/test_personal_dev_build_placement.py`

**Interfaces:** frozen, slots dataclasses; reject bool/string-as-integer coercion, mutable collections, empty/duplicate IDs, nonzero UUID violations, malformed digests and naive datetimes. All integer quantities must fit signed 64-bit, headroom/request/age positive, observed resources nonnegative. Normalize aware datetimes to UTC and node tuples to deterministic order. Digests are exactly 64 lowercase hex and not all zero. Node IDs are canonical (no leading zeros).

```python
@dataclass(frozen=True, slots=True)
class NativeBuildPlacementPolicy:
    allowed_node_ids: tuple[str, ...]
    runtime_profile_sha256: str
    cpu_millicores: int
    memory_bytes: int
    minimum_disk_free_bytes: int
    minimum_free_inodes: int
    max_observation_age_seconds: int

@dataclass(frozen=True, slots=True)
class NativeBuildNodeObservation:
    node_id: str
    boot_id: UUID
    observed_at: datetime
    architecture: str
    slurm_state: str
    reserved: bool
    kvm_available: bool
    available_cpu_millicores: int
    available_memory_bytes: int
    available_disk_bytes: int
    available_inodes: int
    certified_runtime_profile_sha256: str | None

def eligible_native_build_nodes(
    policy: NativeBuildPlacementPolicy,
    observations: tuple[NativeBuildNodeObservation, ...],
    *, now: datetime,
) -> tuple[NativeBuildNodeObservation, ...]: ...

@dataclass(frozen=True, slots=True)
class NativeBuildAllocationBinding:
    manager_reservation_id: UUID
    candidate_id: UUID
    candidate_sha256: str
    attempt_id: UUID
    attempt_lease_epoch: int
    owner_user_id: UUID
    runtime_profile_sha256: str
    slurm_cluster: str
    slurm_job_id: str
    node_id: str
    node_boot_id: UUID
```

Node IDs use exact inventory/Slurm names `trt-gb10-1` through `trt-gb10-15`,
not bare numeric suffixes or SSH destination hostnames. The allocation binding's
`slurm_cluster` must be exactly `trt-gb10`; other nonempty strings are invalid.
Observations may describe any canonical GB10 inventory node 1–15 so reserved
and controller observations can be reported but filtered; policy and allocation
binding permit only nodes 3–15. State/architecture are bounded nonempty strings
(max 64 characters), with only exact `IDLE`/`MIXED` and `aarch64` eligible.
Observation tuple length <=15; empty returns empty. Unknown/duplicate inventory
identity is malformed rather than silently merged. Exact age boundary accepted,
future timestamp ineligible, malformed `now` raises ValueError. Allocation job
ID is positive decimal <= signed 64-bit; reject `0`, leading zeros, arrays and
step suffixes. No JSON parser, CLI, new dependency, or production v1 integration.

- [ ] Write tests first. Hand-derived example policy: nodes (`trt-gb10-9`, `trt-gb10-3`), profile `a*64`, CPU=4000, memory=34359738368, free disk=21474836480, inodes=100000, max age=60; these are TEST VALUES, not approved operating defaults.
- [ ] Test node3 and node9 eligible together (returned in lexical order); node2, node1, unlisted node4, reserved, DRAIN/DOWN/MIXED+DRAIN, wrong architecture, absent KVM and absent/wrong certification excluded. Test exact resource equality and one-unit shortages independently, age 60/61 seconds and future reports.
- [ ] Test invalid input construction, duplicate node observations, malformed `now`, policy narrowing, and allocation binding preservation across two different owners/attempts/nodes. Mutation targets: omitting a single filter or accepting array/step job IDs must fail a named test.
- [ ] Run RED with imports inside tests if needed so the missing feature gives an explicit assertion failure, not an unexplained collection crash.

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src /home/hongjian/loom/.venv/bin/pytest tests/unit/test_personal_dev_build_placement.py -q -p no:cacheprovider
```

- [ ] Implement the records and pure filter using standard-library validation; no IO. A certificate digest match is only a comparison of upstream-verified evidence, never an authentication claim. Include that trust boundary in module/function documentation.
- [ ] Run the new suite plus existing native protocol suite; run Ruff format/check and `git diff --check`. Record command output and RED/GREEN evidence.

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src /home/hongjian/loom/.venv/bin/pytest tests/unit/test_personal_dev_build_placement.py tests/unit/test_personal_dev_native_builder_protocol.py -q -p no:cacheprovider
/home/hongjian/loom/.venv/bin/ruff check src/loom/personal_dev_build_placement.py tests/unit/test_personal_dev_build_placement.py
/home/hongjian/loom/.venv/bin/ruff format --check src/loom/personal_dev_build_placement.py tests/unit/test_personal_dev_build_placement.py
git diff --check
```

- [ ] Commit only the two implementation/test files as `feat(dev): define dynamic personal build placement contracts`; controller handles the documentation commit and independent review. Do not push or merge.

## Plan self-review

| Scope | Check | Result |
| --- | --- | --- |
| Task 1 | Policy/filter/identity interfaces agree with the spec's first slice | Yes; output is non-executable and v1 is unchanged |
| Task 1 | Resource test literals versus operating policy | Explicit test values only; no inferred default disk budget |
| Roadmap | Submission/runtime/manager integration and live acceptance | Intentionally separate follow-on work, not claimed complete by this plan |
