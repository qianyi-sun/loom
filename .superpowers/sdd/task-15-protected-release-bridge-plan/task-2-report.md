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

## Fix round 1: startup-failure independence and executable receipt-digest validation

### Summary

Addressed two post-review findings:

- `CapacityAgentRuntime.run_forever()` now supervises initialization inside its retry loop, so a startup `CapacityAgentStoreError` no longer escapes the enclosing `TaskGroup` and cancel the release loop.
- `ExecutableProtectedReleaseReporterRuntime.run_forever()` now uses the same initialization-supervision pattern for symmetric robustness.
- `DemandReporterClient.publish_executable_protected_release()` now recomputes the manager receipt digest from `{intent_id, protected_release_sha256, executable}` and rejects mismatches before local protected acknowledgement can persist bad evidence.

### Files changed in fix round 1

- Modified: `src/loom_capacity_agent/client.py`
- Modified: `src/loom_capacity_agent/runtime.py`
- Modified: `src/loom_capacity_agent/executable_release_reporter.py`
- Modified: `tests/unit/test_capacity_agent_client.py`
- Modified: `tests/unit/test_capacity_agent_runtime.py`
- Modified: `.superpowers/sdd/task-15-protected-release-bridge-plan/task-2-report.md`

### RED 4: demand startup failure still cancels release progress

Command:

```bash
uv run --no-sync pytest -q tests/unit/test_capacity_agent_runtime.py -k "demand_initialization_without_blocking_release_progress or release_iteration_without_blocking_demand_publication"
```

Relevant output:

```text
ExceptionGroup: unhandled errors in a TaskGroup (1 sub-exception)
loom_capacity_agent.store.CapacityAgentStoreError: demand init unavailable
FAILED tests/unit/test_capacity_agent_runtime.py::test_service_runtime_retries_demand_initialization_without_blocking_release_progress
```

This confirmed the startup `initialize()` failure still escaped before retry supervision and collapsed service-level independence.

### RED 5: tampered executable receipt digest accepted

Command:

```bash
uv run --no-sync pytest -q tests/unit/test_capacity_agent_client.py -k tampered_receipt_digest
```

Relevant output:

```text
Failed: DID NOT RAISE <class 'loom_capacity_agent.client.DemandPublishError'>
```

This confirmed `publish_executable_protected_release()` accepted a syntactically valid but forged `receipt_digest`.

### GREEN 4: focused fixes

Commands:

```bash
uv run --no-sync pytest -q tests/unit/test_capacity_agent_runtime.py -k "demand_initialization_without_blocking_release_progress or release_iteration_without_blocking_demand_publication"
uv run --no-sync pytest -q tests/unit/test_capacity_agent_client.py -k tampered_receipt_digest
```

Outputs:

```text
2 passed, 8 deselected in 0.16s
1 passed, 23 deselected in 0.14s
```

### GREEN 5: full Task 2 suite after fix

Command:

```bash
uv run --no-sync pytest -q tests/unit/test_capacity_agent_client.py tests/unit/test_capacity_agent_runtime.py tests/unit/test_capacity_agent_executable_release_reporter.py
```

Output:

```text
39 passed in 0.22s
```

### Final post-fix gates

Final verification command:

```bash
uv run --no-sync pytest -q tests/unit/test_capacity_agent_client.py tests/unit/test_capacity_agent_runtime.py tests/unit/test_capacity_agent_executable_release_reporter.py
uv run --no-sync ruff format --check src/loom_capacity_agent tests/unit/test_capacity_agent_client.py tests/unit/test_capacity_agent_runtime.py tests/unit/test_capacity_agent_executable_release_reporter.py
uv run --no-sync ruff check src/loom_capacity_agent tests/unit/test_capacity_agent_client.py tests/unit/test_capacity_agent_runtime.py tests/unit/test_capacity_agent_executable_release_reporter.py
uv run --no-sync mypy src/loom_capacity_agent
git diff --check
```

Final outputs:

```text
39 passed in 0.22s
20 files already formatted
All checks passed!
Success: no issues found in 17 source files
```

`git diff --check` returned cleanly.

### Fix-round self-review

- Demand startup failure is now retried in-place without collapsing the release loop.
- Release iteration failure still does not block demand publication; the bidirectional service-runtime coverage now exercises both directions.
- Executable manager receipts must now carry the canonical SHA-256 digest for the validated acknowledgement payload before the local store can advance.

### Fix-round concerns

- The runtime independence test necessarily uses fake storage and publisher boundaries, but it runs the real `CapacityAgentRuntime`, real `ExecutableProtectedReleaseReporterRuntime`, and real `CapacityAgentServiceRuntime` together to exercise the actual retry/cancellation path rather than a mocked control-flow shell.

## Fix round 2: executable protected-release receipt parity with manager digest

### Summary

Addressed the remaining server/client parity bug in executable protected-release receipt validation:

- the unchanged manager still returns `_payload_digest(release.model_dump(mode="json", exclude_none=False))`,
- that value is exactly `canonical_executable_digest(publication.release)`, i.e. `publication.publication_digest`,
- the client had been validating a reduced digest over `{intent_id, protected_release_sha256, executable}` and therefore rejected genuine manager `200` responses,
- validation now requires the receipt digest to equal the already-validated canonical release/publication digest,
- tampered receipt digests are still rejected.

### Files changed in fix round 2

- Modified: `src/loom_capacity_agent/client.py`
- Modified: `tests/unit/test_capacity_agent_client.py`
- Modified: `.superpowers/sdd/task-15-protected-release-bridge-plan/task-2-report.md`

### RED 6: genuine manager digest rejected

Command:

```bash
uv run --no-sync pytest -q tests/unit/test_capacity_agent_client.py -k executable_protected_release
```

Relevant output:

```text
FAILED tests/unit/test_capacity_agent_client.py::test_publish_executable_protected_release_uses_exact_v2_reporter_request
FAILED tests/unit/test_capacity_agent_client.py::test_publish_executable_protected_release_accepts_manager_digest_for_full_release_payload[False]
FAILED tests/unit/test_capacity_agent_client.py::test_publish_executable_protected_release_accepts_manager_digest_for_full_release_payload[True]
E   loom_capacity_agent.client.DemandPublishError: capacity manager executable protected release receipt digest changed
```

This confirmed the client rejected both fresh and replayed receipts carrying the real manager digest for the full release payload.

### GREEN 6: client parity restored

Command:

```bash
uv run --no-sync pytest -q tests/unit/test_capacity_agent_client.py -k executable_protected_release
```

Output:

```text
14 passed, 12 deselected in 0.14s
```

Notes:

- the client now compares `receipt.receipt_digest` directly to `publication.publication_digest`,
- the reduced receipt-only digest helper was removed as unused,
- the tampered-digest rejection test still passes.

### GREEN 7: full Task 2 suite after fix round 2

Final verification command:

```bash
uv run --no-sync pytest -q tests/unit/test_capacity_agent_client.py tests/unit/test_capacity_agent_runtime.py tests/unit/test_capacity_agent_executable_release_reporter.py
uv run --no-sync ruff format --check src/loom_capacity_agent tests/unit/test_capacity_agent_client.py tests/unit/test_capacity_agent_runtime.py tests/unit/test_capacity_agent_executable_release_reporter.py
uv run --no-sync ruff check src/loom_capacity_agent tests/unit/test_capacity_agent_client.py tests/unit/test_capacity_agent_runtime.py tests/unit/test_capacity_agent_executable_release_reporter.py
uv run --no-sync mypy src/loom_capacity_agent
git diff --check
```

Final outputs:

```text
41 passed in 0.21s
20 files already formatted
All checks passed!
Success: no issues found in 17 source files
```

`git diff --check` returned cleanly.

### Fix-round self-review

- Client/server parity now matches the live manager contract: the client accepts the manager receipt digest for the canonical full executable release payload.
- Replay parity is covered explicitly; both `replayed=False` and `replayed=True` receipts are accepted when the digest matches the canonical release payload.
- Tampered digest rejection remains intact because the client compares against the trusted, locally recomputed `publication.publication_digest`.

### Fix-round concerns

- The parity fix assumes the manager continues returning the digest of the full `ExecutableProtectedReleaseV2` payload, which is what `execution_store.acknowledge_protected_release()` returns today via `_payload_digest(payload)`. If the manager contract later changes, the client and these tests will need to change together.
