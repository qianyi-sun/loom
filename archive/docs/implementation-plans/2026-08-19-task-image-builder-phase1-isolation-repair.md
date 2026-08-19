# Task-image builder Phase 1 isolation repair implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> superpowers:subagent-driven-development (recommended) or
> superpowers:executing-plans to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking.

**Goal:** Repair the dormant Phase 1 rootless task-image builder prerequisites
so they cannot mutate legacy builder infrastructure, correctly parse live Slurm
23.11.4 output, require the controller submission identity, and verify physical
hosts against Slurm aliases while keeping activation and certification closed.

**Architecture:** Assign one cluster-specific QoS to each rootless builder and
treat all new Slurm objects as absent-or-exact additions. A small standard-library
Python helper owns semantic Slurm parsing and canonical fingerprints; the Bash
convergers retain host-authority and mutation sequencing. Evidence records the
controller identity, legacy guard, and Slurm-to-host binding, but Phase 1 still
certifies zero nodes.

**Tech Stack:** Bash 5, Python 3.11 standard library, TOML, JSON Schema Draft
2020-12, pytest, jsonschema, Ruff, Slurm 23.11.4 CLI contracts, GitHub protected
CI.

## Global constraints

- Work only in `/home/hongjian/loom/.worktrees/task-image-builder-phase1-isolation`.
- Target `dev` through PR, protected CI, and squash merge; never commit directly
  to `dev`.
- Do not add or modify any path under `docs/superpowers/**`.
- Do not run either prerequisite script in `apply` mode against a live cluster.
- Do not enable a task-image builder policy, supervisor, timer, node feature,
  provider, or node guard.
- Keep `production_certification_allowed = false`, `certified_nodes = []`, and
  unconditional blocker `phase2_guard_provider_release_missing`.
- Do not rerun task `4139e767`.
- Preserve legacy QoS `loom-task-image-builder`, reservation
  `loom-task-image-builder`, account `loom-staging`, user `loom-rollout`, fixed
  capacity, and supervisors without mutation.
- Use rootless QoS `loom-task-image-builder-rootless-oldlab` on OLDLAB and
  `loom-task-image-builder-rootless-gb10` on GB10.
- Rootless objects are absent-or-exact: add an absent object, accept an exact
  object, and reject drift before mutation. Never migrate or narrow an existing
  object.
- Rootless Slurm jobs remain a Phase 2 concern and must eventually omit
  `--exclusive`, reservations, and fixed `--nodelist` selection.
- Docker-group membership is not administrative authority and the Docker socket
  is forbidden.
- PR B, not this plan, owns UID-map package installation, controller identity
  creation, runtime dependency closure, cgroup changes, quota storage, node
  drains, and live convergence.

---

## File structure

- `deploy/task-image-builder/prerequisites-v1.toml` — declarative rootless and
  legacy-guard Slurm names, identities, limits, and cluster inventories.
- `scripts/ops/task_image_builder_slurm_readback.py` — typed parsing and
  canonicalization of `sacctmgr --parsable2` and `scontrol` readback only; no
  subprocess execution and no mutation.
- `deploy/slurm/converge-loom-task-image-builder-prerequisites.sh` — controller
  authority, preflight, additive Slurm convergence, partition rollback, and
  pre/post legacy fingerprint enforcement.
- `deploy/slurm/install-loom-task-image-builder-node-prerequisites.sh` — node
  policy/runtime installer plus read-only Slurm-alias-to-local-host proof.
- `scripts/ops/task_image_builder_prerequisite_conformance.py` — semantic
  verification of canonical Phase 1 evidence.
- `docs/evidence/task-image-builder-prerequisite-conformance-v1.schema.json` —
  strict evidence syntax for controller identity, legacy guard, Slurm state,
  node bindings, and host prerequisites.
- `tests/ops/test_task_image_builder_prerequisite_profile.py` — exact policy and
  non-collision assertions.
- `tests/ops/test_task_image_builder_slurm_readback.py` — isolated parser and
  fingerprint tests using observed Slurm output.
- `tests/ops/test_task_image_builder_prerequisite_converge.py` — executable
  fake-controller convergence, immutability, rollback, and idempotency tests.
- `tests/ops/test_task_image_builder_node_prerequisites_install.py` — executable
  fake-node alias/address binding and installer tests.
- `tests/ops/test_task_image_builder_prerequisite_conformance.py` — complete
  hand-derived evidence and fail-closed mutation matrix.
- `archive/docs/implementation-plans/2026-08-19-task-image-builder-phase-1-prerequisites.md`
  — historical plan marked as superseded for live convergence.
- `docs/evidence/README.md` — evidence-field and Phase 1 non-certification note.

---

### Task 1: Make the policy non-colliding

**Files:**

- Modify: `tests/ops/test_task_image_builder_prerequisite_profile.py`
- Modify: `tests/ops/test_task_image_builder_prerequisite_conformance.py`
- Modify: `deploy/task-image-builder/prerequisites-v1.toml`

**Interfaces:**

- Produces global table `legacy_guard` with keys `qos`, `reservation`,
  `account`, `user`, `max_jobs_per_user`, `max_submit_jobs_per_user`, and
  `max_wall`.
- Produces per-cluster keys `legacy_base_qos`, `legacy_reservation_node`, and
  `legacy_reservation_partition`.
- Produces per-cluster `slurm_qos` values consumed by the evidence verifier and
  Slurm converger.

- [ ] **Step 1: Write exact failing policy assertions**

Replace the shared-QoS assertion in
`test_phase_one_policy_is_dynamic_bounded_and_cannot_certify_production` with:

```python
expected_qos = {
    "oldlab": "loom-task-image-builder-rootless-oldlab",
    "gb10": "loom-task-image-builder-rootless-gb10",
}
assert policy["legacy_guard"] == {
    "qos": "loom-task-image-builder",
    "reservation": "loom-task-image-builder",
    "account": "loom-staging",
    "user": "loom-rollout",
    "max_jobs_per_user": 1,
    "max_submit_jobs_per_user": 1,
    "max_wall": "04:00:00",
}
for cluster_id, cluster in clusters.items():
    assert cluster["slurm_qos"] == expected_qos[cluster_id]
    assert cluster["slurm_qos"] != policy["legacy_guard"]["qos"]
assert clusters["oldlab"]["legacy_base_qos"] == "normal"
assert clusters["oldlab"]["legacy_reservation_node"] == "trt-eai-oldlab-6"
assert clusters["oldlab"]["legacy_reservation_partition"] == "all"
assert clusters["gb10"]["legacy_base_qos"] == "loom-staging"
assert clusters["gb10"]["legacy_reservation_node"] == "trt-gb10-2"
assert clusters["gb10"]["legacy_reservation_partition"] == "gb10"
```

Change the conformance fixture's rootless QoS and association values to use
`cluster["slurm_qos"]` instead of a literal legacy name.

- [ ] **Step 2: Run the focused tests and observe failure**

Run:

```bash
/home/hongjian/loom/.worktrees/task-image-builder-phase1-prerequisites/.venv/bin/python \
  -m pytest -q \
  tests/ops/test_task_image_builder_prerequisite_profile.py \
  tests/ops/test_task_image_builder_prerequisite_conformance.py
```

Expected: FAIL because `legacy_guard` and cluster-specific QoS values are
absent.

- [ ] **Step 3: Add the exact policy values**

Add this global table before the cluster array:

```toml
[legacy_guard]
qos = "loom-task-image-builder"
reservation = "loom-task-image-builder"
account = "loom-staging"
user = "loom-rollout"
max_jobs_per_user = 1
max_submit_jobs_per_user = 1
max_wall = "04:00:00"
```

Set the OLDLAB cluster values to:

```toml
slurm_qos = "loom-task-image-builder-rootless-oldlab"
legacy_base_qos = "normal"
legacy_reservation_node = "trt-eai-oldlab-6"
legacy_reservation_partition = "all"
```

Set the GB10 values to:

```toml
slurm_qos = "loom-task-image-builder-rootless-gb10"
legacy_base_qos = "loom-staging"
legacy_reservation_node = "trt-gb10-2"
legacy_reservation_partition = "gb10"
```

- [ ] **Step 4: Run the focused tests and verify they pass**

Run the command from Step 2.

Expected: both test modules PASS while certification remains closed.

- [ ] **Step 5: Commit the policy boundary**

```bash
git add deploy/task-image-builder/prerequisites-v1.toml \
  tests/ops/test_task_image_builder_prerequisite_profile.py \
  tests/ops/test_task_image_builder_prerequisite_conformance.py
git commit -m "fix(builder): isolate rootless Slurm QoS names"
```

---

### Task 2: Extend evidence with controller, legacy, and host binding state

**Files:**

- Modify: `docs/evidence/task-image-builder-prerequisite-conformance-v1.schema.json`
- Modify: `scripts/ops/task_image_builder_prerequisite_conformance.py`
- Modify: `tests/ops/test_task_image_builder_prerequisite_conformance.py`

**Interfaces:**

- Adds cluster field `controller_identity`.
- Adds `slurm.legacy_builder` containing current legacy QoS, association, and
  reservation state.
- Adds node field `slurm_identity` containing the Slurm alias, configured
  hostname/address, resolved addresses, and local hostnames/addresses.
- Produces `_legacy_builder_failures`, `_controller_identity_failures`, and
  `_slurm_identity_failures` for `verify_evidence`.

- [ ] **Step 1: Add failing complete-evidence fields and drift cases**

Add this controller identity to each cluster in `_evidence()`:

```python
"controller_identity": {
    "user": "loom-builder",
    "uid": 993,
    "group": "loom-task-builder",
    "gid": 980,
    "home": "/nonexistent",
    "shell": "/usr/sbin/nologin",
    "supplementary_groups": [],
},
```

For each node, derive a documentation-only address and add:

```python
address = (
    f"192.0.2.{len(nodes) + 10}"
    if cluster["id"] == "oldlab"
    else f"198.51.100.{len(nodes) + 10}"
)
physical_name = f"host-{node_name}"
slurm_identity = {
    "node_name": node_name,
    "node_hostname": physical_name,
    "node_addr": address,
    "resolved_addresses": [address],
    "local_hostnames": [physical_name],
    "local_addresses": [address],
}
```

Add `"slurm_identity": slurm_identity` to the node object. Add
`slurm.legacy_builder` with the exact policy names, legacy four-hour QoS,
required base/legacy QoS list, and active `SPEC_NODES` reservation:

```python
legacy = policy.raw["legacy_guard"]
"legacy_builder": {
    "qos": {
        "name": legacy["qos"],
        "flags": ["DenyOnLimit"],
        "priority": 0,
        "max_jobs_per_user": 1,
        "max_submit_jobs_per_user": 1,
        "max_wall": "04:00:00",
        "group_tres": {},
    },
    "association": {
        "cluster": cluster["slurm_cluster"],
        "account": legacy["account"],
        "user": legacy["user"],
        "qos": sorted([cluster["legacy_base_qos"], legacy["qos"]]),
        "default_qos": cluster["legacy_base_qos"],
    },
    "reservation": {
        "name": legacy["reservation"],
        "node": cluster["legacy_reservation_node"],
        "partition": cluster["legacy_reservation_partition"],
        "users": [legacy["user"]],
        "accounts": [legacy["account"]],
        "state": "ACTIVE",
        "flags": ["SPEC_NODES"],
    },
},
```

Add drift matrix entries that change the controller UID, legacy QoS wall,
reservation node, Slurm node alias, resolved address, and local hostname. Each
must expect a failure containing respectively `controller identity`, `legacy
builder`, or `Slurm host binding`.

- [ ] **Step 2: Run the conformance tests and observe schema failures**

Run:

```bash
/home/hongjian/loom/.worktrees/task-image-builder-phase1-prerequisites/.venv/bin/python \
  -m pytest -q tests/ops/test_task_image_builder_prerequisite_conformance.py
```

Expected: FAIL because the strict schema rejects the new fields.

- [ ] **Step 3: Define strict JSON Schema objects**

Add `$defs.controllerIdentity`, `$defs.slurmIdentity`, and
`$defs.legacyBuilder`. Every object has `additionalProperties: false` and
requires every field shown in Step 1. Address arrays use string items with
`format: "ipv4"` or `format: "ipv6"` through `anyOf`; hostname/name arrays are
unique and nonempty. Legacy `group_tres` is an object with
`additionalProperties: false` and no required properties, so only `{}` is
valid.

Require `controller_identity` in `$defs.cluster`, `slurm_identity` in
`$defs.node`, and `legacy_builder` in `$defs.slurm`.

- [ ] **Step 4: Implement semantic evidence verification**

Add exact controller comparison using the policy identity without subordinate
IDs. Add legacy comparison using `legacy_guard` and the cluster-specific legacy
fields. Require the legacy association QoS set to equal the sorted base and
legacy QoS names.

Extend `load_policy` to validate the complete `legacy_guard` mapping, require
the three per-cluster legacy fields, require two unique rootless QoS names, and
reject either rootless name when it equals the guarded legacy QoS.

For a node binding, implement these conditions:

```python
resolved = set(binding["resolved_addresses"])
local_addresses = set(binding["local_addresses"])
hostnames = {item.casefold() for item in binding["local_hostnames"]}
if (
    binding["node_name"] != node["name"]
    or not resolved
    or not resolved.issubset(local_addresses)
    or binding["node_hostname"].casefold() not in hostnames
):
    failures.append(f"{node['name']}: Slurm host binding is invalid")
```

Call all three new verifiers from `verify_evidence` after schema validation and
before existing host prerequisite checks.

- [ ] **Step 5: Run schema and conformance tests**

Run:

```bash
/home/hongjian/loom/.worktrees/task-image-builder-phase1-prerequisites/.venv/bin/python \
  -m pytest -q \
  tests/ops/test_task_image_builder_prerequisite_conformance.py \
  tests/ops/test_task_image_builder_prerequisite_profile.py
```

Expected: PASS, including every new negative case.

- [ ] **Step 6: Commit the evidence contract**

```bash
git add docs/evidence/task-image-builder-prerequisite-conformance-v1.schema.json \
  scripts/ops/task_image_builder_prerequisite_conformance.py \
  tests/ops/test_task_image_builder_prerequisite_conformance.py
git commit -m "fix(builder): attest legacy and host identity prerequisites"
```

---

### Task 3: Add semantic Slurm readback parsing

**Files:**

- Create: `scripts/ops/task_image_builder_slurm_readback.py`
- Create: `tests/ops/test_task_image_builder_slurm_readback.py`

**Interfaces:**

- `ReadbackError(ValueError)` — sanitized parser/contract failure.
- `parse_parsable2_row(payload: str, field_names: tuple[str, ...], *,
  allow_absent: bool) -> dict[str, str] | None` — exactly zero/one semantic row.
- `verify_account`, `verify_qos`, `verify_association`, and
  `verify_reservation` — return canonical JSON-compatible mappings or `None`
  only when absence is explicitly allowed.
- CLI subcommands `account`, `qos`, `association`, and `reservation` read raw
  Slurm output on stdin and print canonical sorted JSON or `null`.

- [ ] **Step 1: Write parser tests from live and normalized examples**

Create tests covering these exact rows:

```python
LIVE_EMPTY_TRES = (
    "loom-task-image-builder|DenyOnLimit|0|1|1|04:00:00|\n"
)
SENTINEL_EMPTY_TRES = (
    "loom-task-image-builder|DenyOnLimit|0|1|1|04:00:00||\n"
)
ROOTLESS_TRES = (
    "loom-task-image-builder-rootless-oldlab|DenyOnLimit|0|1|1|"
    "02:00:00|node=1,mem=32G,cpu=8|\n"
)
```

Assert that both empty-TRES rows produce the same canonical legacy QoS and that
`ROOTLESS_TRES` normalizes to `{"cpu": 8, "memory_mib": 32768, "nodes": 1}`.
Parameterize rejection for two rows, six fields, nine fields, duplicate TRES
keys, extra TRES keys, `mem=32000M`, extra flags, an eight-hour wall, and an
unexpected association QoS.

Add a reservation case with reordered tokens and flags and rejection cases for
a foreign node, inactive state, extra user, and missing `SPEC_NODES`.

- [ ] **Step 2: Run the new tests and observe import failure**

Run:

```bash
/home/hongjian/loom/.worktrees/task-image-builder-phase1-prerequisites/.venv/bin/python \
  -m pytest -q tests/ops/test_task_image_builder_slurm_readback.py
```

Expected: FAIL because the module does not exist.

- [ ] **Step 3: Implement exact row and value parsing**

Implement `parse_parsable2_row` so it strips only trailing newlines, rejects
embedded/multiple rows, accepts either exactly `field_count` parts or
`field_count + 1` parts whose final item is the empty `parsable2` sentinel, and
maps the semantic fields by name. Empty payload returns `None` only when
`allow_absent=True`.

Implement wall parsing for `HH:MM:SS` and `D-HH:MM:SS`, integer CPU/node TRES,
and binary `K`, `M`, `G`, or `T` memory converted exactly to MiB. Reject
fractional values, duplicates, unknown keys, and non-integral MiB results.

Canonicalize flags and QoS lists as sorted unique arrays. Parse reservation
tokens with `shlex.split`, reject duplicate keys, and compare exact
name/node/partition/users/accounts/state plus required `SPEC_NODES`.

- [ ] **Step 4: Implement the read-only CLI**

Use `argparse` subcommands whose expected values are explicit arguments. Read at
most 1 MiB from stdin, call the matching verifier, and print:

```python
print(json.dumps(result, sort_keys=True, separators=(",", ":")))
```

On `ReadbackError`, print only `error: Slurm readback is invalid` and exit 1;
never echo the raw row.

- [ ] **Step 5: Run parser tests and Ruff**

```bash
/home/hongjian/loom/.worktrees/task-image-builder-phase1-prerequisites/.venv/bin/python \
  -m pytest -q tests/ops/test_task_image_builder_slurm_readback.py
/home/hongjian/loom/.worktrees/task-image-builder-phase1-prerequisites/.venv/bin/ruff \
  check scripts/ops/task_image_builder_slurm_readback.py \
  tests/ops/test_task_image_builder_slurm_readback.py
```

Expected: both commands PASS.

- [ ] **Step 6: Commit the parser boundary**

```bash
git add scripts/ops/task_image_builder_slurm_readback.py \
  tests/ops/test_task_image_builder_slurm_readback.py
git commit -m "fix(builder): parse Slurm prerequisite readback semantically"
```

---

### Task 4: Make Slurm convergence additive and legacy-immutable

**Files:**

- Modify: `tests/ops/test_task_image_builder_prerequisite_converge.py`
- Modify: `deploy/slurm/converge-loom-task-image-builder-prerequisites.sh`

**Interfaces:**

- Consumes the policy keys from Task 1.
- Consumes the readback CLI from Task 3.
- Produces `LOOM_LEGACY_FINGERPRINT`, the SHA-256 of canonical legacy QoS,
  association, and reservation JSON.
- Produces rootless states `LOOM_ACCOUNT_CONVERGED`, `LOOM_QOS_CONVERGED`, and
  `LOOM_ASSOCIATION_CONVERGED` without accepting a legacy migration pre-state.

- [ ] **Step 1: Replace the fake controller with distinct legacy/rootless state**

Update the fixture policy to use rootless QoS
`loom-task-image-builder-rootless-test` plus the global/per-cluster legacy keys
from Task 1. Give the fixture separate files for rootless QoS and legacy QoS.
Set the legacy file to the observed single-delimiter row:

```python
LEGACY_QOS = "loom-task-image-builder|DenyOnLimit|0|1|1|04:00:00|\n"
ROOTLESS_QOS = (
    "loom-task-image-builder-rootless-test|DenyOnLimit|0|1|1|02:00:00|"
    "cpu=8,mem=32768M,node=1|\n"
)
```

Add fake legacy association and reservation output. Add fake `getent` and `id`
commands that return exact controller passwd/group/primary-GID rows for
`loom-builder` and `loom-task-builder`.

Change the fake `sacctmgr` mutation branch to accept only:

```text
--immediate add qos name=loom-task-image-builder-rootless-test flags=DenyOnLimit Priority=0 MaxJobsPU=1 MaxSubmitJobsPU=1 MaxWall=02:00:00 GrpTRES=cpu=8,mem=32768M,node=1
```

Add tests for:

- successful first apply adds only rootless objects and leaves every legacy
  state file byte-identical;
- second apply is idempotent;
- live single-delimiter legacy QoS passes preflight;
- legacy QoS, association, or reservation drift fails before partition or
  accounting mutation;
- rootless QoS drift fails rather than modifies;
- missing/wrong controller identity fails before mutation;
- post-apply legacy fingerprint mismatch fails closed;
- only `show reservation` is permitted; create/update/delete reservation is
  forbidden; and
- no command contains `exclusive`, `scancel`, `update nodename`, `features=`,
  or `delete`.

- [ ] **Step 2: Run converger tests and observe failure**

```bash
/home/hongjian/loom/.worktrees/task-image-builder-phase1-prerequisites/.venv/bin/python \
  -m pytest -q tests/ops/test_task_image_builder_prerequisite_converge.py
```

Expected: FAIL because the converger still hard-codes and modifies the legacy
QoS and has no identity/legacy guard.

- [ ] **Step 3: Load and validate the corrected policy**

Extend the embedded TOML reader to emit identity group/UID/GID/home/shell,
global legacy fields, cluster legacy fields, and the cluster-specific rootless
QoS. Reject:

```python
if cluster["slurm_qos"] == legacy["qos"]:
    raise SystemExit("rootless QoS collides with legacy")
if legacy["qos"] != "loom-task-image-builder":
    raise SystemExit("legacy QoS guard is not exact")
if legacy["reservation"] != "loom-task-image-builder":
    raise SystemExit("legacy reservation guard is not exact")
```

Keep the existing exact resource, priority, controller, architecture, and
closed-certification checks.

- [ ] **Step 4: Add controller identity readiness**

Before durable-config or Slurm accounting mutation, use `getent passwd` and
`getent group` plus `id -G` to require the exact UID/GID/home/shell, exact
primary group, and no supplementary groups. Failure text is
`controller builder identity is unavailable or unsafe` and contains no passwd
row.

- [ ] **Step 5: Add legacy pre/post fingerprints**

Read legacy QoS and association with bounded `sacctmgr` commands and the named
reservation with bounded `scontrol`. Pipe each raw result into the Task 3 CLI,
concatenate the three canonical JSON lines, and compute SHA-256. Preflight
stores the digest. After any apply/readback, recompute and require exact digest
equality before reporting convergence.

The rootless script must contain no legacy-targeted `add`, `modify`, `delete`,
`create reservation`, or `update reservation` command.

- [ ] **Step 6: Replace migration with absent-or-exact additions**

Use the Task 3 parser for account, rootless QoS, and rootless association. The
only QoS mutation is `sacctmgr --immediate add qos` with the exact rootless
name and limits when the parser returns `null`. Remove `legacy_qos` migration
classification and remove rootless `modify qos` entirely.

- [ ] **Step 7: Run converger and parser tests**

```bash
bash -n deploy/slurm/converge-loom-task-image-builder-prerequisites.sh
/home/hongjian/loom/.worktrees/task-image-builder-phase1-prerequisites/.venv/bin/python \
  -m pytest -q \
  tests/ops/test_task_image_builder_slurm_readback.py \
  tests/ops/test_task_image_builder_prerequisite_converge.py
```

Expected: syntax and all tests PASS.

- [ ] **Step 8: Commit the converger correction**

```bash
git add deploy/slurm/converge-loom-task-image-builder-prerequisites.sh \
  tests/ops/test_task_image_builder_prerequisite_converge.py
git commit -m "fix(builder): preserve legacy Slurm capacity"
```

---

### Task 5: Verify Slurm aliases against the physical node

**Files:**

- Modify: `tests/ops/test_task_image_builder_node_prerequisites_install.py`
- Modify: `deploy/slurm/install-loom-task-image-builder-node-prerequisites.sh`

**Interfaces:**

- Direct CLI becomes `sudo SCRIPT {check|apply} <cluster-id>
  <slurm-node-name> <offline-artifact-directory>`.
- `loom_node_load_policy(cluster_id, slurm_node_name)` validates inventory and
  architecture without comparing the Slurm alias to `hostname -s`.
- `loom_node_verify_slurm_identity(slurm_node_name)` proves NodeName,
  NodeHostName, NodeAddr, DNS resolution, and local interface ownership.

- [ ] **Step 1: Add failing alias/address binding tests**

Extend the fake command directory with `scontrol`, `getent`, `hostname`, and
`ip`. The accepted fixture returns:

```text
NodeName=node-1 NodeAddr=192.0.2.10 NodeHostName=physical-node-1 State=IDLE
```

while `hostname -s` returns `physical-node-1`, `getent ahosts 192.0.2.10`
returns only `192.0.2.10`, and `ip -o address show scope global` reports that
address. Pass `node-1` separately to the sourced installer function.

Add rejection cases for a policy-external Slurm name, mismatched returned
NodeName, foreign NodeHostName, unresolved NodeAddr, a resolved foreign
address, and a mixed local/foreign resolution set. Assert every rejection
happens before passwd, group, subordinate-ID, or runtime mutation.

Update the direct-CLI override test to use four arguments and assert the old
three-argument grammar returns usage status 2.

- [ ] **Step 2: Run installer tests and observe failure**

```bash
/home/hongjian/loom/.worktrees/task-image-builder-phase1-prerequisites/.venv/bin/python \
  -m pytest -q tests/ops/test_task_image_builder_node_prerequisites_install.py
```

Expected: FAIL because the installer still compares `hostname -s` directly to
the Slurm alias and accepts only three arguments.

- [ ] **Step 3: Implement inventory selection and binding proof**

Remove `LOOM_HOST_NODE`. Pass the selected Slurm node into the policy loader,
require exact membership in `builder_nodes`, then obtain one-line
`scontrol show node "$slurm_node" -o` output.

Parse exact `NodeName`, `NodeAddr`, and `NodeHostName` tokens. Resolve NodeAddr
through `getent ahosts`, normalize addresses with Python's `ipaddress` module,
and collect global local addresses from `ip -o address show scope global`.
Require the resolved set to be nonempty and a subset of the local set. Require
NodeHostName case-insensitively in the set from `hostname -s`, `hostname -f`,
and `hostname -A`.

Run this proof before runtime validation or identity preflight in check and
apply modes. Emit only `Slurm node identity does not match the local host` on
failure.

- [ ] **Step 4: Update and protect the direct CLI**

Require exactly four arguments and keep root required only for apply. Reject
test overrides for policy paths, install paths, architecture, local hostname or
address inputs, and skipped host checks. Invoke:

```bash
"loom_node_$1" "$2" "$3" "$4"
```

- [ ] **Step 5: Run installer tests and Bash syntax**

```bash
bash -n deploy/slurm/install-loom-task-image-builder-node-prerequisites.sh
/home/hongjian/loom/.worktrees/task-image-builder-phase1-prerequisites/.venv/bin/python \
  -m pytest -q tests/ops/test_task_image_builder_node_prerequisites_install.py
```

Expected: PASS, including physical-name-differs-from-Slurm-alias acceptance.

- [ ] **Step 6: Commit node identity verification**

```bash
git add deploy/slurm/install-loom-task-image-builder-node-prerequisites.sh \
  tests/ops/test_task_image_builder_node_prerequisites_install.py
git commit -m "fix(builder): bind Slurm aliases to physical hosts"
```

---

### Task 6: Reconcile documentation and run full PR A verification

**Files:**

- Modify: `archive/docs/implementation-plans/2026-08-19-task-image-builder-phase-1-prerequisites.md`
- Modify: `docs/evidence/README.md`
- Verify all files from Tasks 1-5

**Interfaces:**

- Marks the unsafe live-apply portion of the merged Phase 1 plan as superseded.
- Documents that controller identity, legacy guard, and Slurm host binding are
  prerequisite evidence, never certification evidence.

- [ ] **Step 1: Mark the historical plan as superseded**

Immediately below its title, add:

```markdown
> **Live-convergence status:** Superseded by
> `archive/docs/architecture/2026-08-19-task-image-builder-phase1-isolation-correction.md`.
> PR #1457 remains fail-closed and must not be applied without the isolation
> repair and the separate site-specific host convergence boundary.
```

- [ ] **Step 2: Update the evidence README**

State that v1 prerequisite evidence includes the dedicated controller Unix
identity, current immutable legacy builder contract, and verified Slurm
alias-to-local-host binding. State explicitly that a valid envelope still
contains `certified_nodes=[]` and cannot activate a builder.

- [ ] **Step 3: Run the complete focused suite**

```bash
/home/hongjian/loom/.worktrees/task-image-builder-phase1-prerequisites/.venv/bin/python \
  -m pytest -q \
  tests/ops/test_task_image_builder_prerequisite_profile.py \
  tests/ops/test_task_image_builder_prerequisite_conformance.py \
  tests/ops/test_task_image_builder_slurm_readback.py \
  tests/ops/test_task_image_builder_node_prerequisites_install.py \
  tests/ops/test_task_image_builder_prerequisite_converge.py
```

Expected: all focused tests PASS with zero failures.

- [ ] **Step 4: Run static verification**

```bash
bash -n deploy/slurm/converge-loom-task-image-builder-prerequisites.sh
bash -n deploy/slurm/install-loom-task-image-builder-node-prerequisites.sh
/home/hongjian/loom/.worktrees/task-image-builder-phase1-prerequisites/.venv/bin/ruff \
  check scripts/ops/task_image_builder_slurm_readback.py \
  scripts/ops/task_image_builder_prerequisite_conformance.py \
  tests/ops/test_task_image_builder_slurm_readback.py \
  tests/ops/test_task_image_builder_prerequisite_conformance.py \
  tests/ops/test_task_image_builder_prerequisite_profile.py \
  tests/ops/test_task_image_builder_node_prerequisites_install.py \
  tests/ops/test_task_image_builder_prerequisite_converge.py
git diff --check origin/dev...HEAD
if git diff --name-only origin/dev...HEAD | rg '^docs/superpowers/'; then exit 1; fi
```

Expected: every command exits 0 and the forbidden-path scan prints nothing.

- [ ] **Step 5: Verify the live delimiter read-only**

On OLDLAB, read the legacy QoS without mutation and pipe it into the new parser
using the exact legacy expectation. Do not run the converger in apply mode.

```bash
sudo -n timeout 30 sacctmgr --noheader --parsable2 show qos where \
  name=loom-task-image-builder \
  format=Name,Flags,Priority,MaxJobsPU,MaxSubmitJobsPU,MaxWall,GrpTRES \
  </dev/null \
  | /home/hongjian/loom/.worktrees/task-image-builder-phase1-prerequisites/.venv/bin/python \
      scripts/ops/task_image_builder_slurm_readback.py qos \
      --name loom-task-image-builder --flags DenyOnLimit --priority 0 \
      --max-jobs 1 --max-submit 1 --max-wall 04:00:00 --group-tres ''
```

Expected: exit 0 with canonical legacy QoS JSON and no raw secret-bearing
output.

- [ ] **Step 6: Commit documentation and any verification-only corrections**

```bash
git add archive/docs/implementation-plans/2026-08-19-task-image-builder-phase-1-prerequisites.md \
  docs/evidence/README.md
git commit -m "docs(builder): supersede unsafe Phase 1 convergence steps"
```

---

### Task 7: Review, open PR A, pass protected CI, and merge

**Files:**

- Review: every path in `git diff --name-only origin/dev...HEAD`
- Do not modify: `docs/superpowers/**`

**Interfaces:**

- Produces one PR targeting `dev` and one squash merge commit on `origin/dev`.
- Produces no live Slurm, host, registry, builder-policy, or task mutation.

- [ ] **Step 1: Invoke the requesting-code-review workflow**

Review the complete branch against the approved design, with special attention
to legacy target names, post-apply fingerprint enforcement, raw-output
sanitization, controller identity ordering, direct-CLI override rejection, and
all fail-closed paths. Correct every verified critical or important finding and
rerun Task 6 verification.

- [ ] **Step 2: Run verification-before-completion on the final HEAD**

Repeat Task 6 Steps 3 and 4 after the final correction commit. Record the exact
test count and command exit codes in the PR body.

- [ ] **Step 3: Push only the correction branch**

```bash
git push -u origin fix/task-image-builder-phase1-isolation
```

Expected: the remote branch points at the locally verified HEAD. No other
branch or tag is pushed.

- [ ] **Step 4: Open PR A against `dev`**

Create a non-draft PR whose summary names the legacy collision, real Slurm
delimiter fixture, controller identity contract, and alias binding. The risk
section states that merge performs no live mutation. The deployment note states
that OLDLAB and GB10 remain unconverged, certification stays false, the Phase 2
blocker remains, and task `4139e767` is not rerun.

- [ ] **Step 5: Wait for protected CI on the current head**

Require successful current-head results for `repository-checks`, `images-gate`,
`cluster-smoke-gate`, and `staging-smoke-gate`. A manual workflow result does
not replace a protected context. If the head changes, wait for the new head's
checks.

- [ ] **Step 6: Squash merge and verify `origin/dev`**

Use the repository's protected auto-merge path. After merge:

```bash
git fetch origin dev
merge_oid="$(gh pr view --json mergeCommit --jq '.mergeCommit.oid')"
git merge-base --is-ancestor "$merge_oid" origin/dev
mapfile -t pr_paths < <(gh pr diff --name-only)
git diff --exit-code HEAD "$merge_oid" -- "${pr_paths[@]}"
gh pr view --json state,mergedAt,mergeCommit
```

Expected: PR state `MERGED`, a non-null merge commit, and the reviewed branch
paths exactly match the squash merge commit while that merge commit is an
ancestor of `origin/dev`. Do not run cluster convergence after merge; PR B and
separate operational authorization remain required.
