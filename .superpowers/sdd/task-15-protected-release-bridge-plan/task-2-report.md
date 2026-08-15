# Task 2 Report: Trusted V2 Release Reporter Loop

## Summary

Implemented the trusted executable protected-release reporter loop and wired it into the capacity-agent process beside the existing demand loop. The agent now:

- publishes executable protected releases to the manager's existing `PUT /v2/reports/protected-releases/{subject_id}/{shape_instance_id}` endpoint with the existing reporter bearer identity,
- derives a stable UUIDv5 idempotency key from `(event_id, publication_digest, subject_id, subject_incarnation)`,
- acknowledges the local protected-release outbox only after manager acknowledgement,
- runs demand publication and executable protected-release publication as independent loops inside one process and one health surface,
- keeps readiness false until both sub-runtimes are ready,
- reuses the same owner-only database URL and reporter token, and
- makes no install/start/enable/apply/activation change and leaves the executable ceiling at `0`.

## Files changed

- Created: `src/loom_capacity_agent/executable_release_reporter.py`
- Created: `tests/unit/test_capacity_agent_executable_release_reporter.py`
- Modified: `src/loom_capacity_agent/client.py`
- Modified: `src/loom_capacity_agent/runtime.py`
- Modified: `src/loom_capacity_agent/__init__.py`
- Modified: `tests/unit/test_capacity_agent_client.py`
- Modified: `tests/unit/test_capacity_agent_runtime.py`
- Modified: `deploy/dev-fleet/README.md`
- Modified mechanically by required package gate formatting: `src/loom_capacity_agent/reporter.py`, `src/loom_capacity_agent/secret_init.py`

## Implementation details

### 1. Exact V2 executable protected-release publisher

Added `DemandReporterClient.publish_executable_protected_release(...)` and `ExecutableProtectedReleasePublishReceiptV2`.

Behavior:

- validates the trusted binding before network:
  - subject ID
  - subject incarnation
  - reporter incarnation
  - deployment generation
  - candidate identity algorithm / identity / publication digest
- validates the publication digest against `canonical_executable_digest(release)`
- serializes the manager payload with `canonical_executable_bytes(release)`
- sends the existing reporter bearer token to:
  - `/v2/reports/protected-releases/{subject_id}/{shape_instance_id}`
- enforces bounded receipts and exact JSON fields
- rejects mismatched intent / protected-release digest / executable flag / oversized or non-200 responses

### 2. Executable protected-release reporter runtime

Added `ExecutableProtectedReleaseReporterRuntime` plus `stable_release_publication_key(...)`.

Behavior:

- reads the next outbox event with `read_next_executable_protected_release(...)`
- marks itself ready without HTTP when no event is pending
- publishes with deterministic UUIDv5 idempotency
- acknowledges with `acknowledge_executable_protected_release_publication(...)` only after manager acknowledgement
- replays the same idempotency key after local crashes or local acknowledgement failures because the key is derived from stable publication identity
- keeps `ready = False` on failures and retries on the next loop iteration

### 3. Combined demand + release service runtime

Added `CapacityAgentServiceRuntime` in `src/loom_capacity_agent/runtime.py`.

Behavior:

- starts demand and executable protected-release loops in one `asyncio.TaskGroup`
- exposes composite health where `ready` is true only when both sub-runtimes are ready
- shares the same:
  - `DemandReporterClient`
  - SQLAlchemy engine
  - session factory
- preserves independent loop execution while sharing process resources
- keeps cleanup centralized in `_main_async()` so cancellation closes the shared HTTP client and engine cleanly

### 4. Documentation

Updated `deploy/dev-fleet/README.md` to state explicitly that the already-configured capacity-agent service runs both trusted loops with the existing guard URL and reporter token, adds no credential or activation action, and leaves the executable ceiling at `0`.

## TDD log

### RED 1: executable publisher method missing

Command:

```bash
uv run --no-sync pytest -q tests/unit/test_capacity_agent_client.py -k executable_protected_release
```

Relevant output:

```text
E   AttributeError: 'DemandReporterClient' object has no attribute 'publish_executable_protected_release'
3 failed, 12 deselected in 0.16s
```

### GREEN 1: executable publisher added

Command:

```bash
uv run --no-sync pytest -q tests/unit/test_capacity_agent_client.py -k executable_protected_release
```

Relevant output:

```text
11 passed, 12 deselected in 0.13s
```

Notes:

- expanded the client tests to cover exact request bytes, binding mismatches, invalid idempotency, changed receipts, oversized receipts, and non-200 responses.

### RED 2: executable release reporter module missing

Command:

```bash
uv run --no-sync pytest -q tests/unit/test_capacity_agent_executable_release_reporter.py
```

Relevant output:

```text
E   ModuleNotFoundError: No module named 'loom_capacity_agent.executable_release_reporter'
5 failed in 0.15s
```

### GREEN 2: executable release reporter runtime added

Command:

```bash
uv run --no-sync pytest -q tests/unit/test_capacity_agent_executable_release_reporter.py
```

Relevant output:

```text
5 passed in 0.14s
```

Notes:

- covered no-pending ready behavior, publish+ack, HTTP failure replay, local-ack failure replay with stable idempotency, and stable-key derivation.

### RED 3: composite service runtime missing

Command:

```bash
uv run --no-sync pytest -q tests/unit/test_capacity_agent_runtime.py
```

Relevant output:

```text
E   AttributeError: module 'loom_capacity_agent.runtime' has no attribute 'CapacityAgentServiceRuntime'
E   AttributeError: module 'loom_capacity_agent.runtime' has no attribute 'ExecutableProtectedReleaseReporterRuntime'
3 failed, 5 passed in 0.15s
```

### GREEN 3: combined service runtime added

Command:

```bash
uv run --no-sync pytest -q tests/unit/test_capacity_agent_runtime.py
```

Relevant output:

```text
8 passed in 0.18s
```

Notes:

- covered composite readiness, concurrent loop startup/cancellation, and `_main_async()` cleanup of shared resources.

### Focused Task 2 bundle GREEN

Command:

```bash
uv run --no-sync pytest -q tests/unit/test_capacity_agent_client.py tests/unit/test_capacity_agent_runtime.py tests/unit/test_capacity_agent_executable_release_reporter.py
```

Output:

```text
36 passed in 0.20s
```

## Full Task 2 gates

Initial gate run exposed package-local formatter drift:

```text
Would reformat: src/loom_capacity_agent/client.py
Would reformat: src/loom_capacity_agent/reporter.py
Would reformat: src/loom_capacity_agent/runtime.py
Would reformat: src/loom_capacity_agent/secret_init.py
Would reformat: tests/unit/test_capacity_agent_client.py
Would reformat: tests/unit/test_capacity_agent_executable_release_reporter.py
Would reformat: tests/unit/test_capacity_agent_runtime.py
```

Applied formatter, fixed import / `__all__` ordering with Ruff, then reran the exact required gates:

```bash
uv run --no-sync pytest -q tests/unit/test_capacity_agent_client.py tests/unit/test_capacity_agent_runtime.py tests/unit/test_capacity_agent_executable_release_reporter.py
uv run --no-sync ruff format --check src/loom_capacity_agent tests/unit/test_capacity_agent_client.py tests/unit/test_capacity_agent_runtime.py tests/unit/test_capacity_agent_executable_release_reporter.py
uv run --no-sync ruff check src/loom_capacity_agent tests/unit/test_capacity_agent_client.py tests/unit/test_capacity_agent_runtime.py tests/unit/test_capacity_agent_executable_release_reporter.py
uv run --no-sync mypy src/loom_capacity_agent
git diff --check
```

Final outputs:

```text
36 passed in 0.19s
20 files already formatted
All checks passed!
Success: no issues found in 17 source files
```

`git diff --check` returned cleanly.

## Self-review

### Requirements coverage

- V2 manager protected-release endpoint used: yes
- Existing reporter bearer identity reused: yes
- No new credential / service introduced: yes
- Executable ceiling unchanged at `0`: yes
- Pool executors not granted reporter authority: yes; all new work is capacity-agent side only
- Demand and release loops independent in one process: yes
- Crash / replay idempotency covered by tests: yes
- Protected outbox only acknowledged after manager acknowledgement: yes
- Composite readiness after both loops initialize / run ready: yes
- README updated with non-activation / no-install wording: yes

### Code review notes

- `stable_release_publication_key()` fails closed if the outbox digest no longer matches the release payload.
- Runtime acknowledgement uses the manager receipt's stable `receipt_digest`, which stays idempotent across replayed acknowledgements.
- Shared resources are created once in `_main_async()` and closed once on shutdown.

## Concerns

- The exact Task 2 gate command checked the whole `src/loom_capacity_agent` package and surfaced pre-existing formatter drift in `src/loom_capacity_agent/reporter.py` and `src/loom_capacity_agent/secret_init.py`. I kept those changes mechanical-only so the requested final gate run passes on the committed tree.
