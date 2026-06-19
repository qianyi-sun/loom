# Verifier protocol

Verifiers grade what an agent produced. Loom's verifier surface is a
single Protocol plus five concrete implementations; the result shape
is typed all the way down.

## Why a typed result

Harbor returned `dict[str, float|int] | None` and surfaced failures
as exceptions (`MissingTestDirError`, `ParsingError`,
`VerifierOutputNotFound`). Two problems:

1. **Failures looked like crashes**. A missing `tests/` dir aborted
   the trial instead of producing inspectable "no tests" data.
2. **Result fields were unstructured**. Per-check scores, confidence,
   structured outputs, and aggregation hints all lived in
   provider-specific dict keys.

Loom returns `VerifierResult` always — failures fill the `error`
field rather than raise. ATIF projection trusts the shape and never
has to catch verifier exceptions.

## The Protocol

`src/loom/verifier/base.py`:

```python
@runtime_checkable
class Verifier(Protocol):
    name: str

    async def verify(
        self,
        *,
        task: TaskConfig,
        env: Driver,            # sandbox handle: exec, fs, network
        artifacts_dir: PurePosixPath,
        trajectory: TrajectoryReader,
    ) -> VerifierResult: ...
```

Four inputs, one output. Verifiers can `env.exec(...)` inside the
agent's sandbox, read agent artifacts via `artifacts_dir`, and
inspect the full event stream via `trajectory`. They produce a
`VerifierResult`; they do not raise.

`VerifierFactory.register(name, ctor)` plugs a verifier in. The task
config picks one by `name` + per-task `args`.

## VerifierResult

```python
class VerifierResult:
    rewards: dict[str, float]            # the numeric score(s)
    checks: list[CheckResult] = []       # per-check breakdown
    confidence: float | None = None      # 0..1 if the verifier has one
    structured: dict[str, Any] | None    # opaque schema-typed payload
    error: VerifierError | None = None   # typed failure (not raised)


class CheckResult:
    name: str
    passed: bool
    score: float | None = None
    message: str | None = None
    duration_sec: float | None = None


class VerifierError:
    kind: Literal[
        "missing_tests", "parse_failure", "exec_failure",
        "timeout", "internal",
    ]
    message: str
    detail: dict[str, Any] = {}
```

Notes:

- **`rewards` is a dict**, not a single float. Multi-metric tasks
  (e.g., correctness + style) populate multiple keys; ATIF
  projection treats each key as a separate reward signal.
- **`error` is data**, not control flow. Setting `error` does not
  invalidate `rewards` / `checks`; a verifier can both report a
  partial score AND flag what went wrong.
- **Trial final-state promotion is stricter than ATIF projection.**
  A per-step `StepError` always makes the trial `failed`, because the
  agent/artifact/verifier phase itself failed. A `VerifierResult.error`
  with an empty `rewards` dict also makes the trial `failed` because the
  evaluator produced no usable score. A `VerifierResult.error` with
  explicit rewards stays a scored outcome, so a model/agent can receive
  `0.0` while the platform run still counts as `succeeded`.
- **`confidence` is bounded [0, 1]** (Pydantic-validated). Used by
  LLM-judge to mark uncertain calls; downstream consumers can filter
  on it.
- **`structured` is verifier-specific**. JSON schema not enforced
  here; verifiers that need stable shape document it themselves.

## Five concrete verifiers

| Name           | Class                           | When to use                                            |
|----------------|---------------------------------|--------------------------------------------------------|
| `pytest`       | `PytestVerifier`                | code tasks with a test suite; parses junit XML         |
| `script`       | `ScriptVerifier`                | task ships a script that writes `VerifierResult` JSON   |
| `structured`   | `StructuredOutputVerifier`      | agent produced JSON; verify against a schema           |
| `llm_judge`    | `LLMJudgeVerifier`              | grade open-ended output via an LLM with a rubric       |
| `composite`    | `CompositeVerifier`             | run N verifiers, aggregate via min / mean / weighted   |

`CompositeVerifier` is the only one that takes other verifiers as
args. The aggregator (`Aggregator.MIN`, `Aggregator.MEAN`,
`Aggregator.WEIGHTED`) reduces `rewards` dicts across children.

## Script verifier bundle contract

`ScriptVerifier` is the preferred adapter boundary for benchmark-specific
checks that can run inside the task sandbox. A task must set the script path
explicitly:

```toml
[verifier]
name = "script"

[verifier.args]
script_path = "/workspace/verifier/run.sh"
```

At runtime Loom creates `/loom/verifier/`, sets only
`LOOM_VERIFIER_OUTPUT=/loom/verifier/output.json`, and runs the configured
script in the agent sandbox. The script should derive task paths from its own
location or from explicit paths such as `/workspace`; it must not require
implicit variables such as `LOOM_TASK_DIR` or `LOOM_AGENT_OUTPUT` unless the
script sets safe defaults itself.

The script must write a `VerifierResult` JSON object to
`$LOOM_VERIFIER_OUTPUT`:

```json
{
  "rewards": {"score": 1.0},
  "checks": [{"name": "answer", "passed": true, "score": 1.0}],
  "structured": {"expected": "45", "got": "45"},
  "confidence": 1.0
}
```

A wrong model answer should normally be `rewards.score = 0.0` with
`checks[0].passed = false`, not a platform failure. Missing output or invalid
JSON is a verifier infrastructure failure and makes the trial failed.

## Why each lives where it does

| Concern            | Verifier's job?  | Driver's job? | Trajectory's job? |
|--------------------|------------------|---------------|--------------------|
| Run a subprocess   | yes (via `env.exec`) | yes (executes the call) | no |
| Filter to one event kind | yes | no | no (provides the cursor) |
| Decide pass/fail   | yes              | no            | no |
| Surface a crash    | yes (as `error`) | no            | no |
| Persist the result | no               | no            | no (worker writes ATIF) |

A verifier is **stateless across calls**. Per-call state goes into
the `VerifierResult.structured` field so downstream consumers can
inspect it without re-running.

## What about `verifier_env_mode`?

Harbor supports `SEPARATE` (verifier runs in its own container with
trusted deps) and `SHARED` (verifier runs in the agent's container,
cheap loop). Loom v0.7 ships SHARED only — agent images must ship
verifier deps (`pytest`). SEPARATE is tracked for v1.5; the
architectural lift is modest because the `Driver` Protocol already
supports multiple containers per trial.

## Adding a new verifier

1. Implement the `Verifier` Protocol.
2. Decide which `rewards` keys you'll emit and document them in
   your verifier's docstring.
3. Decide which `VerifierError.kind`s you can raise (use existing
   kinds when possible; only add a new literal if none fit).
4. Register at module load:

   ```python
   verifier_factory.register("my_verifier", MyVerifier)
   ```

5. Tests: at least the happy path + each error kind your verifier
   produces. Mock `Driver.exec` rather than spawning real
   subprocesses.

A verifier should be ~100–300 LOC including tests. The five shipped
ones live under `src/loom/verifier/` and are good shape references.

## What this is NOT

- **Not a grader for arbitrary file formats**. If you need a
  Python-rich domain check (image diffing, audio fidelity), keep
  domain logic inside `verify(...)` and emit a tight `structured`
  payload rather than expanding the `VerifierResult` shape.
- **Not a place for per-run policy**. License allowlists, budget
  guards, etc. live elsewhere (Gateway, scheduler).
- **Not async-required**. The Protocol is `async def verify` for
  IO uniformity; CPU-bound verifiers can just `return` without
  awaiting anything.

## See also

- [`trajectory-and-atif.md`](trajectory-and-atif.md) — how rewards
  end up in the ATIF projection
- [`driver-protocol.md`](driver-protocol.md) — the `env` parameter
  in `verify(...)`
- [`benchmark-adapter.md`](benchmark-adapter.md) — how a task picks
  its verifier
