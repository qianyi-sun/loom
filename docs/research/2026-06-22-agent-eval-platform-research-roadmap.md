# Loom Agent Evaluation Platform Research Roadmap

As of: 2026-06-22

This note plans the next research-and-engineering directions for Loom. It uses the paper corpus in
`docs/research/2026-06-22-agent-eval-platform-paper-corpus.md`.

The paper direction should not be "yet another benchmark". The stronger framing is:

> Loom is an agent-evaluation infrastructure and evaluation-science platform for reproducible,
> cost-aware, diagnosable, multi-team agent evaluation across existing benchmarks and production
> workloads.

## 1. Current Project Position

Loom already has several pieces that are unusually paper-relevant:

- A unified adapter surface for benchmarks, agents, drivers, and verifiers.
- Event-sourced JSONL trajectories and ATIF records.
- A gateway layer for providers, budgets, retries, and entitlement checks.
- Multi-team concepts: teams, billing/cost tracking, roles, run provenance, and operator UX.
- Scheduling and worker-execution design, including DRF scheduling and sandboxed runs.
- A catalog direction for onboarding heterogeneous benchmarks such as SWE-bench, LiveCodeBench,
  BFCL, BrowseComp, MMLU-Pro, MBPP, Terminal-Bench, and tau-bench-like workloads.

The gap is that most current agent-evaluation work still ends at a score table, leaderboard, or
single benchmark environment. Loom can be positioned as the missing operational layer between
agent research and repeatable evaluation practice.

## 2. Literature Map

The related work clusters into four lines.

### 2.1 From Static Benchmarks to Interactive Agent Environments

Representative work:

- Static/code/reasoning: HumanEval, MBPP, APPS, DS-1000, MMLU, MMLU-Pro, GPQA, LiveCodeBench,
  BigCodeBench.
- Software-engineering agents: SWE-bench, SWE-bench Verified, SWE-agent, UTBoost, CVE-Bench,
  ProjectEval, Terminal-Bench.
- Web/GUI/app agents: WebArena, VisualWebArena, OSWorld, AndroidWorld, AppWorld,
  TheAgentCompany, GAIA, BrowseComp.
- Tool-use and function calling: API-Bank, ToolLLM/ToolBench, StableToolBench, BFCL,
  ToolSandbox, tau-bench, tau2-bench, MCP-Bench.

Implication for Loom:

The platform should treat these as workloads, not as competitors to recreate. The publishable
question is how to run, compare, debug, and govern them under one execution and evidence model.

### 2.2 From Leaderboards to Evaluation Infrastructure

Representative work:

- HELM and BIG-bench established broad evaluation infrastructure for language models.
- Chatbot Arena showed platform-level evaluation can be publishable through ranking methodology,
  active sampling, and transparency.
- OpenCompass and Evalverse show universal LLM evaluation tooling.
- HAL and ARE show that agent-evaluation infrastructure itself is now an active research object.
- AI Agents That Matter argues that agent evaluation often overemphasizes accuracy, ignores cost,
  lacks holdouts, and conflates models with harnesses.

Implication for Loom:

The platform must differentiate from "a harness that runs many tasks". The strongest angles are
trace-causal diagnosis, contract-checked compatibility, verifier confidence, and cost-aware
evaluation allocation.

### 2.3 From MLOps to AgentEvalOps

Representative work:

- TFX, MLflow, ModelDB, and Data Validation for ML show how platform engineering can become
  systems research when it formalizes lifecycle, metadata, validation, and reproducibility.
- Hidden Technical Debt in ML Systems provides a vocabulary for boundary erosion, hidden coupling,
  configuration debt, and monitoring debt.
- Model Cards and Datasheets motivate machine-readable documentation and governance artifacts.

Implication for Loom:

Agent evaluation has its own version of hidden technical debt: benchmark versions, sandbox images,
provider entitlements, verifier dependencies, task materialization, network policies, judge
models, retries, cost ceilings, and artifact retention. A platform paper can formalize these as
contracts and audit records.

### 2.4 From Final Scores to Evaluation Integrity

Representative work:

- LLM-as-a-judge papers, evaluator-bias studies, TrustJudge, DecodingTrust, UTBoost, and
  StableToolBench show that evaluation itself is noisy and fallible.
- AgentBoard and recent agent-evaluation surveys call for fine-grained, scalable diagnostics.

Implication for Loom:

The platform should expose verifier confidence and failure provenance, not just success/failure
labels. This is especially important for multi-step agents where one final score hides many
intermediate failure modes.

## 3. Recommended Feature Roadmap

### Phase 0: Stabilize the Evaluation Matrix

Goal: make the current catalog dependable enough to produce research evidence.

Ship:

- Matrix tracker as a first-class table: benchmark x agent x provider x worker image x verifier.
- Provider entitlement and model-readiness preflight.
- License allowlist and dataset-source readiness checks.
- Structured malformed-output diagnostics.
- Service-mode agent support inside worker images.
- Script/oracle compatibility checks.
- High-concurrency finalize/writeback hardening.

Research value:

This phase turns today's operational issues into observable phenomena. Every failure should become
a typed compatibility or reliability signal rather than an opaque run crash.

### Phase 1: Run Observatory

Goal: convert ATIF and JSONL trajectories into queryable diagnostics.

Ship:

- A trajectory query API over runs, tasks, steps, tool calls, verifier events, retries, cost, and
  artifacts.
- Derived features: token/cost curves, action entropy, tool-error rates, patch/test chronology,
  verifier rerun agreement, timeout phase, sandbox fault class.
- A failure taxonomy: environment, benchmark materialization, provider, agent protocol, tool use,
  reasoning/planning, verifier, scheduler, artifact writeback.
- Cross-run comparison: same task across agents/providers, same agent across benchmark versions,
  same task before/after verifier change.

Research value:

This enables a paper on trace-causal agent evaluation: predicting, localizing, and explaining
agent failures using structured trajectories rather than only final scores.

### Phase 2: Compatibility and Readiness Compiler

Goal: make "can this run safely and reproducibly?" a compiled contract, not a tribal checklist.

Ship:

- `EvaluationContract` schema:
  - benchmark adapter version
  - task source and materializer
  - worker image and verifier dependencies
  - sandbox/network policy
  - required agent mode
  - provider/model entitlement
  - license and data-use policy
  - artifact retention expectations
  - cost/rate ceilings
- Contract validation before enqueue.
- Contract diff for benchmark version changes.
- Contract evidence in run cards.

Research value:

This supports a systems paper: contract-checked agent evaluation infrastructure. The analogy is
Data Validation for ML, but the validated object is a complete agent-evaluation execution plan.

### Phase 3: Verifier Confidence Layer

Goal: expose uncertainty in the evaluator itself.

Ship:

- Verifier cards: deterministic/scripted, unit-test, state-based, LLM judge, hybrid.
- Verifier confidence fields: agreement, rerun stability, oracle coverage, judge model,
  judge prompt hash, confidence interval, known blind spots.
- Weak-verifier detection: flaky tests, over-broad oracles, model-judge self-preference,
  inconsistent state checks.
- Diagnostic reruns when confidence is low.

Research value:

This can produce a method paper on evaluation integrity for agent benchmarks: when does a pass/fail
label deserve trust, and how can a platform detect low-confidence outcomes?

### Phase 4: Cost-Aware Reliable Evaluation

Goal: allocate evaluation budget to minimize ranking error and maximize diagnostic value.

Ship:

- Pass^k and repeated-run stability tracking for stochastic agents.
- Sequential allocation: stop early on clear wins/losses; spend more on uncertain comparisons.
- Cost-aware DRF scheduling across teams, providers, and benchmark classes.
- Ranking confidence and pairwise uncertainty in reports.

Research value:

This is a natural extension of AI Agents That Matter and Chatbot Arena-style ranking methodology:
agent evaluation should report performance under explicit cost and uncertainty budgets.

### Phase 5: Auditable AgentEvalOps

Goal: make multi-team evaluation defensible to researchers, operators, and governance reviewers.

Ship:

- Run cards, benchmark cards, verifier cards, and provider cards.
- Immutable provenance: model/provider, prompt, adapter versions, container digests, contract hash,
  artifact manifest, cost summary.
- Retention and redaction policies for trajectories and provider logs.
- Team-scoped audit views and release snapshots.

Research value:

This is less novel algorithmically but highly valuable for deployment and governance. It becomes
stronger when paired with Phase 1 or Phase 2.

## 4. Ranked Paper Directions

### Direction A: Trace-Causal Agent Evaluation

Core claim:

Final scores are too coarse for long-horizon agent evaluation. Structured trajectory evidence can
localize failures, explain score differences, and predict when additional evaluation runs will be
informative.

Method:

- Define a trajectory schema over agent actions, tool calls, verifier events, sandbox events,
  provider events, cost, and artifacts.
- Build failure attribution models/rules over ATIF trajectories.
- Evaluate on existing workloads: SWE-bench Verified, LiveCodeBench, BFCL, BrowseComp,
  Terminal-Bench, tau-bench or ToolSandbox, plus static controls such as MBPP and MMLU-Pro.
- Compare against final-score-only analysis, benchmark-specific logs, and generic observability.

Expected contribution:

- A taxonomy of cross-benchmark agent failure modes.
- A trace-derived diagnostic method.
- Evidence that trajectory-level diagnosis improves debugging efficiency, rerun allocation, or
  failure prediction without creating a new benchmark.

Scores:

| Criterion | Score | Notes |
|---|---:|---|
| Novelty | 5 | Goes beyond harness/leaderboard and beyond single-benchmark logs. |
| Feasibility | 4 | Loom already has trajectories; needs taxonomy, query layer, and experiments. |
| Impact | 5 | Directly useful to agent researchers and platform operators. |
| Evidence potential | 5 | Current matrix failures can seed labels and ablations. |
| Risk | 4 | Needs careful differentiation from HAL and AgentBoard. |

Best venues:

- MLSys, NeurIPS Datasets and Benchmarks, ICLR, ICML evaluation/systems workshops as stepping
  stones.

Recommendation:

This should be the primary paper direction.

### Direction B: Contract-Checked Agent Evaluation Infrastructure

Core claim:

Agent-evaluation failures often come from hidden incompatibilities across benchmark, sandbox,
agent protocol, model entitlement, verifier, and artifact policy. A typed evaluation contract can
prevent invalid runs and make platform results reproducible.

Method:

- Formalize `EvaluationContract`.
- Compile contracts from catalog manifests and runtime environment.
- Measure prevented invalid runs, reduced operator intervention, reduced flaky failures, and
  improved reproducibility across machines or worker images.

Scores:

| Criterion | Score | Notes |
|---|---:|---|
| Novelty | 4 | Strong systems contribution, especially if formalized well. |
| Feasibility | 5 | Aligns directly with current readiness/license/preflight issues. |
| Impact | 4 | Solves a painful problem for agent-eval platforms. |
| Evidence potential | 5 | Existing issue set gives concrete before/after metrics. |
| Risk | 2 | Less risky than Direction A, but may read as engineering unless evaluated rigorously. |

Best venues:

- MLSys, VLDB/SIGMOD industry/systems track, ICSE/AIware, NeurIPS Datasets and Benchmarks.

Recommendation:

This is the safest engineering-first paper. It can be combined with Direction A as the systems
substrate.

### Direction C: Cost-Aware Reliable Agent Evaluation

Core claim:

Agent evaluation should optimize for reliable conclusions under bounded cost, not maximize raw
run count.

Method:

- Track stochastic run variance and pass^k.
- Use sequential testing or bandit-style allocation for uncertain model/task pairs.
- Report ranking confidence and cost-normalized utility.

Scores:

| Criterion | Score | Notes |
|---|---:|---|
| Novelty | 4 | Cost-aware agent evaluation is underdeveloped. |
| Feasibility | 4 | Needs repeated runs and enough budget. |
| Impact | 5 | Strong for any team paying real provider costs. |
| Evidence potential | 4 | Requires carefully designed experiment matrix. |
| Risk | 3 | Must avoid becoming only a scheduling optimization paper. |

Best venues:

- NeurIPS/ICML evaluation methodology, MLSys, ICLR.

Recommendation:

Strong second paper or an ablation inside Direction A.

### Direction D: Verifier Confidence Layer

Core claim:

Benchmark pass/fail labels are not equally reliable. Agent-evaluation platforms should estimate
and report verifier confidence.

Method:

- Classify verifier types.
- Estimate agreement, stability, coverage, and judge bias.
- Trigger diagnostic reruns for low-confidence outcomes.

Scores:

| Criterion | Score | Notes |
|---|---:|---|
| Novelty | 4 | Timely due to LLM-as-judge reliability concerns. |
| Feasibility | 3 | Needs verifier-specific instrumentation. |
| Impact | 4 | Useful but narrower than trajectory-causal diagnosis. |
| Evidence potential | 5 | Strong related work and clear evaluation metrics. |
| Risk | 4 | Needs rigorous treatment of LLM judges and scripted verifiers. |

Best venues:

- ACL/EMNLP evaluation, NeurIPS Datasets and Benchmarks, MLSys.

Recommendation:

Good if Loom's verifier protocol becomes central soon.

### Direction E: Auditable AgentEvalOps

Core claim:

Multi-team agent evaluation needs machine-readable provenance and governance artifacts to make
results reproducible, accountable, and operationally useful.

Method:

- Define run cards, benchmark cards, verifier cards, and provider cards.
- Show how cards support reproducibility, policy compliance, and incident/debug workflows.

Scores:

| Criterion | Score | Notes |
|---|---:|---|
| Novelty | 3 | Strong applied value but less technical novelty alone. |
| Feasibility | 5 | Mostly platform/product work. |
| Impact | 4 | Valuable for enterprise and lab operations. |
| Evidence potential | 3 | Needs deployment or user-study evidence. |
| Risk | 2 | Low engineering risk, higher publication-risk if isolated. |

Best venues:

- FAccT, CSCW, MLSys industry track, data-centric AI venues.

Recommendation:

Use as a supporting contribution, not the main top-conference bet.

## 5. Suggested Mainline

The best top-venue attempt is:

**Trace-Causal Agent Evaluation with Contract-Checked Execution in Loom**

Paper shape:

1. Problem: agent evaluation is fragmented, costly, and opaque; final scores hide failure causes.
2. System: Loom unifies existing benchmarks through typed adapters, contracts, trajectories,
   verifiers, gateway, and scheduler.
3. Method: trace-causal failure attribution and queryable trajectory diagnostics.
4. Experiments:
   - Cross-benchmark failure taxonomy coverage.
   - Diagnostic accuracy against human/operator labels.
   - Reduction in invalid/failed runs after contracts.
   - Cost/reliability tradeoff using repeated runs and early stopping.
   - Case studies on SWE-bench, LiveCodeBench, BFCL, BrowseComp, and Terminal-Bench.
5. Result: platform-level evidence improves reproducibility, debugging, and cost-aware decision
   making without proposing a new benchmark.

This balances research novelty and engineering credibility. It also uses Loom's real strengths
instead of forcing the project into a crowded benchmark-paper lane.

## 6. Four-to-Six Week Execution Plan

Week 1:

- Convert the active matrix issues into a formal taxonomy.
- Add typed reason codes for readiness, provider, license, verifier, malformed output, sandbox,
  and writeback failures.
- Define `EvaluationContract v0`.

Week 2:

- Implement contract generation for current catalog entries.
- Add preflight validation and contract hash to runs.
- Store typed failure events in ATIF/trajectory output.

Week 3:

- Build Run Observatory v0:
  - run/task/step query API
  - cost and provider-event summaries
  - failure taxonomy dashboard/table
  - cross-run comparison view

Week 4:

- Run a controlled matrix across 5-7 existing workloads:
  - SWE-bench Verified or SWE-bench Lite
  - LiveCodeBench
  - BFCL
  - BrowseComp
  - Terminal-Bench
  - tau-bench or ToolSandbox if integration is ready
  - MBPP/MMLU-Pro as static controls

Week 5:

- Label a sample of failures manually.
- Compare trace-causal diagnosis to score-only and benchmark-log-only baselines.
- Measure invalid-run prevention and cost impact.

Week 6:

- Draft paper sections.
- Freeze an artifact snapshot.
- Prepare ablations:
  - without contracts
  - without trajectory features
  - without verifier confidence
  - final-score-only baseline

## 7. Key Differentiation Against Similar Work

Against HAL:

- HAL is a leaderboard/harness infrastructure with many rollouts and public logs.
- Loom should emphasize typed execution contracts, multi-team operational constraints,
  cost-aware scheduling, and causal failure attribution over the full lifecycle.

Against ARE:

- ARE focuses on scaling agent environments and evaluations.
- Loom should emphasize production evaluation operations, reproducibility contracts, and
  cross-benchmark diagnostics.

Against OpenHands:

- OpenHands is an agent-development platform.
- Loom should be an evaluation-operations platform that can run OpenHands-like agents but is not
  tied to one agent interface or software-engineering task family.

Against OpenCompass/Evalverse:

- These focus mostly on model/LLM evaluation.
- Loom should focus on interactive, sandboxed, tool-using, stateful agent runs with artifacts,
  verifiers, cost, and governance.

Against AgentBoard:

- AgentBoard provides analytical evaluation over multi-turn agents.
- Loom should contribute the execution and provenance substrate that makes such analysis reliable
  across many benchmark families and teams.

## 8. Main Risks

- The trace taxonomy may be too hand-engineered unless it is validated against human labels and
  operational outcomes.
- The system paper may read as engineering unless the contracts and diagnostics are formalized and
  evaluated quantitatively.
- Repeated-run experiments can become expensive; use small but diverse matrices and sequential
  allocation.
- Some trajectories may contain sensitive data or provider logs; redaction policy should be part
  of the platform design.
- Differentiation from HAL and ARE must be explicit in the introduction and experiments.

## 9. Immediate Engineering Priorities

Highest priority:

1. Close the compatibility/readiness matrix enough to run 5-7 benchmark families reliably.
2. Make every failed run produce a typed reason code and evidence pointer.
3. Add contract hashes and environment/provider/verifier metadata to ATIF output.
4. Build trajectory query and cross-run comparison primitives.
5. Add cost and uncertainty summaries to reports.

Do not prioritize:

- Creating a new benchmark dataset.
- Building a leaderboard as the primary contribution.
- Adding many more benchmark integrations before the current integrations produce high-quality
  traces and failure evidence.

## 10. Anchor References

- [Survey on Evaluation of LLM-based Agents](https://arxiv.org/abs/2503.16416)
- [AI Agents That Matter](https://openreview.net/forum?id=Zy4uFzMviZ)
- [Holistic Agent Leaderboard](https://arxiv.org/abs/2510.11977)
- [ARE: Scaling Up Agent Environments and Evaluations](https://arxiv.org/abs/2509.17158)
- [OpenHands](https://openreview.net/forum?id=OJd3ayDDoF)
- [SWE-bench](https://openreview.net/forum?id=VTF8yNQM66)
- [WebArena](https://openreview.net/forum?id=oKn9c6ytLx)
- [OSWorld](https://openreview.net/forum?id=tN61DTr4Ed)
- [AppWorld](https://aclanthology.org/2024.acl-long.850/)
- [ToolSandbox](https://aclanthology.org/2025.findings-naacl.65/)
- [tau-bench](https://openreview.net/forum?id=roNSXZpUDN)
- [tau2-bench](https://openreview.net/forum?id=LGmO9VvuP5)
- [LiveCodeBench](https://openreview.net/forum?id=chfJJYC3iL)
- [BigCodeBench](https://openreview.net/forum?id=YrycTjllL0)
- [HELM](https://openreview.net/forum?id=iO4LZibEqW)
- [Chatbot Arena](https://arxiv.org/abs/2403.04132)
- [Judging LLM-as-a-Judge](https://openreview.net/forum?id=uccHPGDlao)
- [LLM Evaluators Recognize and Favor Their Own Generations](https://openreview.net/forum?id=4NJBV6Wp0h)
- [TrustJudge](https://openreview.net/forum?id=4uPyOCeN6U)
- [OpenCompass](https://arxiv.org/html/2605.19276)
- [TFX](https://www.kdd.org/kdd2017/papers/view/tfx-a-tensorflow-based-production-scale-machine-learning-platform)
- [MLflow](https://people.eecs.berkeley.edu/~matei/papers/2018/ieee_mlflow.pdf)
- [Data Validation for Machine Learning](https://proceedings.mlsys.org/paper_files/paper/2019/hash/928f1160e52192e3e0017fb63ab65391-Abstract.html)
- [Hidden Technical Debt in Machine Learning Systems](https://papers.neurips.cc/paper/5656-hidden-technical-debt-in-machine-learning-systems)
- [Datasheets for Datasets](https://dl.acm.org/doi/10.1145/3458723)
- [Model Cards for Model Reporting](https://dl.acm.org/doi/10.1145/3287560.3287596)
