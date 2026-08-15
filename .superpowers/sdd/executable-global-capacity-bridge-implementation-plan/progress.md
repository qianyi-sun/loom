# SDD ledger — plan: docs/architecture/executable-global-capacity-bridge-implementation-plan.md
Task 1: complete (commits b3c0bc2..e75c81b, review clean)
Task 2: in progress (uncommitted; independent review open; known TRUNCATE regression-test transaction masking)
Task 2: pre-stabilization review — reject until NULL/FK binding, direct state insertion, trusted evidence binding, active stale fail-closed behavior, restart recovery, executable registration provenance, ceiling/rate policy, TRUNCATE, and migration/recovery coverage are adjudicated at current head
Task 2: fix round 1/5 in progress — Ruff formatting gate plus durable candidate provenance and active-writer replacement clamp findings
Task 2: fix round 1/5 (formatting and active-writer clamp addressed; durable candidate provenance remains open; commits e4fc0c1..68f20cc)
Task 2: fix round 2/5 in progress — persist and validate exact tagged git-sha1/source-sha256 candidate provenance
Task 2: fix round 2/5 (durable tagged candidate provenance addressed; commits 68f20cc..7b0bb5a)
Task 2: complete (commits e75c81b..7b0bb5a, review clean; independent gate 193 passed, Ruff format/check and mypy clean)
Task 3: fix round 1/5 in progress — executable model invariants, active failure freeze, allocation immutability/mode coupling, executable-history downgrade semantics, and non-vacuous scale-to-zero coverage
Task 3: fix round 1/5 (4/5 Important findings addressed; active-authority resolution/commit failure can still escape without freeze/evidence; commits cd229aa..c064d1b)
Task 3: fix round 2/5 in progress — store-owned raw-authority failure transition and hard failure when freeze/evidence cannot persist
Task 3: fix round 2/5 (active-authority failure fencing addressed, 0 Important open; commits c064d1b..6f395a1)
Task 3: minor (deferred): add post-write rollback fault injection for failed reconciliation persistence
Task 3: minor (deferred): add direct prepared/drain-only failure-transition regressions
Task 3: minor (deferred): assert ReconciliationFailurePersistenceError directly
Task 3: complete (commits 7b0bb5a..6f395a1, review clean; independent gates 19 focused + 80 relevant passed, Ruff format/check and mypy clean)
Task 4: fix round 1/5 in progress — retain ceiling charge through closing until both terminal fences, and enforce exact node-bound headroom
Task 4: minor (deferred): add database consistency/domain constraints for executor last_inventory_at and intent observed_state
Task 4: fix round 1/5 (closing charge addressed; exact multi-node/heterogeneous headroom remains open; commits bb433da..bc65b80)
Task 4: fix round 2/5 in progress — replace positional and duplicated aggregate node accounting with bounded exact selected-node feasibility
Task 4: fix round 2/5 (exact selected-node feasibility addressed, 0 Important open; commits bc65b80..28df5e0)
Task 4: complete (commits 6f395a1..28df5e0, review clean; independent gates 2 focused + 47 required + 99 regression passed, Ruff format/check and mypy clean)
Task 5: fix round 1/5 in progress — database-enforce the exact executable-v2 binding; atomically bind drain to protected claim admission; derive release from registration/revocation evidence; revoke all pre-existing executor-role privileges before granting only guard_0011
Task 5: initial review rejected (0 Critical, 4 Important; base 28df5e0, head 3c12a90)
Task 5: fix round 1/5 (3 original findings addressed, 2 Important open — executable claims have no protected terminal evidence; executor isolation revokes unrelated PUBLIC function access; commits 3c12a90..d588fa0)
Task 5: fix round 2/5 in progress — derive live claims from append-only protected terminal evidence and isolate the executor without changing unrelated application-role privileges
Task 5: fix round 2/5 (0/2 Important findings fully addressed — terminal projection can race an uncommitted claim; executor still inherits PUBLIC-schema function access; commits d588fa0..a862737)
Task 5: fix round 3/5 in progress — serialize claim/terminal visibility with one lifecycle-head-first lock order and remove inherited PUBLIC schema access while restoring intended application-role usage
Task 5: fix round 3/5 (2/2 Important findings addressed, 0 open; commits a862737..06f0c00)
Task 5: complete (commits 28df5e0..06f0c00, review clean; independent gates 88 required + 60 regression passed, Ruff format/check and mypy clean)
Task 6: in progress (base 06f0c00) — typed bounded argv-only Slurm observation, submission, conditional pending cancellation, and accounting high-water
Task 6: plan/test conflict resolved — narrow the obsolete package-wide no-Slurm source scan to an explicit permanent-V1 module allowlist; Task 6 explicitly requires slurm_backend.py in the same package
Task 6: initial review rejected (2 Critical, 4 Important; base 06f0c00, head 53ea834) — standard scontrol output incompatibility; executable/launcher TOCTOU; unbounded pipe cleanup; running %R parsing; incomplete TRES round-trip; unchecked scancel stdout
Task 6: minor (deferred): reject all disallowed C0/C1 output controls, not only NUL and CR
Task 6: fix round 1/5 in progress — close the six blocking authority, lifetime, parsing, resource-evidence, and cancellation-result gaps with focused RED/GREEN coverage
Task 6: fix round 1/5 (4/6 findings addressed, 2 open — delayed launcher execution-node authority; TRES parser/contract cardinality mismatch; commits 53ea834..dee08a6)
Task 6: fix round 2/5 in progress — submit a controller-verified, execution-node self-verifying launcher boundary and make declared generic TRES maxima round-trip through inventory/accounting
Task 6: fix round 2/5 (2/2 findings addressed, 0 open; commits dee08a6..33a1b78)
Task 6: minor (deferred): canonicalize or reject dot/repeated-slash launcher paths before sbatch because the delayed verifier rejects them
Task 6: complete (commits 06f0c00..33a1b78, review clean; independent gates 47 focused + 119 regression passed, Ruff format/check, strict mypy, and diff checks clean)
Task 7: in progress (base 33a1b78) — render exact operator-owned trusted launches and sign complete executable ownership metadata
Task 7: initial review rejected (1 Critical, 2 Important; base 33a1b78, head 950d266) — incompatible reinterpretation of manager profile_digest; expected controller identity absent at mutation boundary; signed trusted_launcher_sha256 incorrectly contains release digest
Task 7: fix round 1/5 in progress — preserve real manager profile-digest semantics with an authenticated renderer-policy commitment, enforce exact controller routing in Task 6 submit, and sign the launcher content digest separately from trusted release
Task 7: fix round 1/5 (3/3 findings addressed, 0 open; commits 950d266..745d113)
Task 7: complete (commits 33a1b78..745d113, review clean; independent gates 22 Task 7 + 48 Task 6 + 20 ownership + 161 combined passed, Ruff format/check, strict mypy, and diff checks clean)
Task 8: in progress (base 745d113) — journal-first executable ticks, exact v2 manager transport, conservative recovery, drain/cancel/release ordering, and no-resubmit uncertainty
Task 8: initial review rejected (0 Critical, 5 Important; base 745d113, head b1269f7) — unusable reservation acceptance binding; no pending-central replay; incomplete post-submit physical-binding recovery; unverified signed adoption; close without protected pre-start drain/revocation
Task 8: minor (deferred): replace pervasive executor orchestration `Any` types with exact receipt/work protocol types and production-client conformance coverage
Task 8: fix round 1/5 in progress — close the five production transport, replay, recovery, signature-verification, and drain-first gaps with focused RED/GREEN coverage
Task 8: fix round 1/5 (5/5 findings addressed, 0 open; commits b1269f7..2e0e452)
Task 8: complete (commits 745d113..2e0e452, review clean; independent gates 57 focused + 209 regression + 33 protected-boundary passed, Ruff format/check, strict MyPy, and diff checks clean)
Task 9: in progress (base 2e0e452) — strict owner-only pool configuration, singleton daemon, zero-ceiling inventory-only construction, and independent OLDLAB/GB10 binding
Task 9: initial review rejected (3 Critical, 3 Important; base 2e0e452, head ee01b9a) — CLI is validation scaffolding rather than daemon; arbitrary tick factory bypasses Task 8/execution-mode authority; incomplete immutable pool/key/controller cross-binding; inventory replay/receipt fence absent; signal lifecycle absent; validate-only skips journal security/lock
Task 9: minor (deferred): remove or derive `scheduler_mutations` because constant zero is misleading once active execution is wired
Task 9: fix round 1/5 in progress — build the real typed daemon assembly and close exact binding, journal replay, receipt, signal, and validate-only gaps with focused RED/GREEN coverage
Task 9: fix round 1/5 (5/6 findings addressed, 1 Critical open — manifest is self-derived rather than independently pinned and concrete Slurm executable authority is not compared; commits ee01b9a..2577008)
Task 9: fix round 2/5 in progress — pin a canonical complete per-pool manifest independently of mutable config and enforce exact concrete Slurm authority/executable binding
Task 9: fix round 2/5 (1/1 Critical finding addressed, 0 open; commits 2577008..6a0d626)
Task 9: complete (commits 2e0e452..6a0d626, review clean; independent gates 26 focused + 57 Task 8 regression passed, Ruff format/check, strict MyPy, CLI help, and diff checks clean)
Task 10: in progress (base 6a0d626) — authenticated global execution witness and fail-closed reciprocal fencing before both legacy scale-up mutation paths
Task 10: initial review rejected (2 Critical, 3 Important; base 6a0d626, head 88b3cdf) — self-asserted authentication; optional/bypassed live writers; scheduler-state mutation before fence; unsafe unbounded witness loading; fenced output indistinguishable from success
Task 10: fix round 1/5 in progress — add pinned cryptographic manager authentication, make all scale-up paths fail closed by default, fence before mutation/computation, harden file loading, and make fenced reports non-success
Task 10: fix round 1/5 (2/5 original findings addressed, 5 Important open — complete race-safe owner/mode/link/ctime validation; empty-policy queue and denied-activation bypasses; checked-in active callers omit trust inputs; empty GB10 removal is incorrectly scoped to OLDLAB; commits 88b3cdf..4c7cca7)
Task 10: minor (deferred): remove broad formatting churn from the two integration test files before final review
Task 10: minor (deferred): replace the integration module's automatic witness-synthesizing wrapper with explicit evidence at positive-path call sites
Task 10: fix round 2/5 in progress — close file-metadata races, fence empty/denied policy computation, bind every checked-in caller to independent trust inputs without losing drain, and derive empty-demand scope from retained broker work
Task 10: fix round 2/5 (4/5 findings addressed, 1 Important open — parsed-but-denied evidence commits a safe clamp but exits zero; commits 4c7cca7..8f0f0c6)
Task 10: fix round 3/5 in progress — classify parsed witness semantics before reconcile, preserve the committed drain-safe path, and return non-success for every denied witness state
Task 10: fix round 3/5 (1/1 finding addressed, 0 open; commits 8f0f0c6..34a97ed)
Task 10: complete (commits 6a0d626..34a97ed, review clean; independent gates 208 passed, Ruff format/check, strict MyPy, and diff checks clean)
Task 11 (superseded harness brief): BLOCKED at base 34a97ed — public authority ceiling fixed at one; active facts blocked; no safe drain-only retirement; no executable client heartbeat; no files or commit produced; evidence in task-11-report.md
Plan amendment: commits 5600159..b002464 — approved finite-envelope/authority-turnover design and two prerequisite tasks inserted; original harness is now Task 13
Task 11: in progress (base 7464029) — finite authority envelope and exact active immutable fact flow
Task 11: initial review rejected (0 Critical, 2 Important; base 7464029, head 493badb) — active pool facts omit immutable reporter-incarnation binding; active-fact coverage does not run executable reconciliation
Task 11: minor (deferred): remove the accidentally tracked `.superpowers` Task 11 implementation report before final integration
Task 11: fix round 1/5 in progress — bind active pool facts to the exact immutable reporter incarnation and prove fresh executable reconciliation consumes changed active facts
Task 11: fix round 1/5 (2 addressed, 0 open; commits 493badb..9244c44)
Task 11: complete (commits 7464029..9244c44, review clean; reported gates 127 passed, Ruff format/check and MyPy clean)
Task 12: in progress (base 9244c44) — explicit drain, conservative retirement, durable lifecycle evidence, executable heartbeat transport, and exact store injection
Task 12: initial review rejected (0 Critical, 5 Important; base 9244c44, head afa1877) — fabricated SQL retirement evidence; stale inventory safety after journal advance; release/retirement lock inversion; incomplete migration parity; incomplete blocker invariants
Task 12: fix round 1/5 in progress — root-cause and close database enforcement, freshness, lock-order, and verification gaps with focused RED/GREEN coverage
Task 12: fix round 1/5 (5 addressed, 0 open; commits afa1877..38343a5)
Task 12: minor (deferred): prepared→retired writer-replacement trigger validates non-null rather than exact derived payload/state evidence; final whole-branch review must triage and fix before merge if confirmed
Task 12: complete (commits 9244c44..38343a5, review clean; independent gates 226 passed, Ruff format/check, MyPy, and diff checks clean)
Task 13: in progress (base 38343a5) — real public/database/wire two-pool multi-owner integration harness
Task 13: scope amendment — honest boundary execution exposed a prerequisite production physical-bind identity defect plus test-only inventory payload rewriting and manual retirement evidence; production correctness governs the obsolete four-file-only brief under the approved full operational goal. Require one narrow production RED/GREEN commit followed by the four-file harness commit, with journal/wire/database payload identity and normal executor-driven retirement evidence.
Task 13: root-cause ruling — top-level inventory correctly carries `ExecutionContextV2`, not one allocation fence, because complete inventory may span per-record allocation epochs. Delete the inventory adapter and prove exact journal/wire/manager payload identity across allocation turnover; do not widen the production inventory contract.
Task 13: review at 27c2845 rejected (0 Critical, 3 Important) — in-place `capacity_0004` edit does not upgrade already-migrated databases; generic TRES mismatch still copies signed rather than observed scheduler values; retirement safety accepts an unauthenticated-by-content journal `+2` instead of the exact inventory requested/confirmed chain.
Task 13: fix round 1/5 in progress — add forward migration/upgrade regression, observed generic-TRES quarantine evidence, and exact post-inventory journal-chain authentication with focused RED/GREEN coverage.
Task 13: fix round 1/5 (3 addressed, 0 open; commit 585a5f9) — forward `capacity_0005` upgrade, observed generic-TRES mismatch, and canonical inventory journal confirmation accepted by scoped re-review with no Critical/Important breakage.
Task 13: minor (deferred): forward-migration regression upgrades an empty `capacity_0004` database and does not seed/assert invalidation of a pre-existing `retirement_safe=true` row; migration unconditionally clears those rows before replacing the constraint.
Task 13: complete (commits 38343a5..585a5f9, review clean; controller reruns 4 passed twice at exact head with all six canonical digests byte-identical)
Task 14: in progress (base 585a5f9) — inert deployment profile/status, personal readiness separation, rehearsal runbook, and zero-mutation governance
Task 14: initial review rejected (2 Critical, 5 Important; base 585a5f9, head c52188d) — read-only row locks and strict JSONB parsing make availability unreachable; legacy credential seeds cannot upgrade; subject status validation is not fully strict; teardown has a session race; downgrade does not restore guard_0012; image/service-user inputs are not bound
Task 14: fix round 1/5 in progress — prove real protected availability and close credential upgrade, strict status, teardown, downgrade, and immutable render-binding gaps with focused RED/GREEN coverage
Task 14: fix round 1/5 (1/7 findings addressed, 6 open; Critical prepared-event predicate regression introduced; commits c52188d..48dee20)
Task 14: fix round 2/5 in progress — restore exact prepared-event selection, add missing real database/upgrade/order/downgrade/wire regressions, and reject unknown intent-state keys
Task 14: fix round 2/5 (4 additional findings addressed, 4 open — incomplete finite manager state set; fake-only teardown order test; downgrade test does not isolate EXECUTE revocation; Task 14 report force-tracked; commits 48dee20..cd269de)
Task 14: fix round 3/5 in progress — accept the complete finite manager state set, prove teardown with real PostgreSQL sessions/reconnect refusal, isolate downgrade privileges, and untrack the local report
Task 14: fix round 3/5 (4/4 findings addressed, 0 open; commit 8f28467) — exact complete state set, deterministic real PostgreSQL teardown ordering, isolated downgrade/re-upgrade privileges, and local-only SDD evidence
Task 14: complete (commits 585a5f9..8f28467, review clean; exact-head gate 167 passed, Ruff format/check, strict MyPy, repository paths, docs hygiene, report exclusion, and diff checks clean)
Task 13: initial review rejected (2 Critical, 4 Important; base 38343a5, uncommitted head) — test-only physical-bind and inventory adapters; synthetic drain/retirement evidence; incomplete failure-matrix assertions; tautological durable-state counters; scheduler-only canonical inventory digest
Task 13: fix round 1/5 in progress — remove all boundary adapters/manual evidence, correct production journal/retirement contracts with focused RED/GREEN tests, strengthen durable mismatch/lifecycle assertions, and hash manager/journal/terminal evidence
Task 15: final review rejected at 4c11c9ef — 2 Critical, 6 Important, 2 Minor approved-plan gaps plus activation prerequisites: production multi-owner runtime/heartbeat/bootstrap absent; incomplete cancel ownership; drain-only disabled; pending withdrawal absent; drain/cancel/terminal recovery incomplete; protected candidate digest mismatch; retained drain fence incomplete; personal authority-coordinate constraint drift; streamed response and admission deadline gaps; static provenance, complete Slurm identity, and retirement database enforcement required before operational activation
Task 15A: in progress (base 4c11c9ef) — manager/protected-data remediation brief task-15-manager-remediation-brief.md dispatched to /root/manager_bridge_fixes
Task 15A: initial review rejected (0 Critical, 5 Important, 3 Minor; head 68d2bc373) — real personal publication digest collapsed; retained writer-transfer fence uses mutable epoch; guard audit upgrade replay incompatible; retirement CHECK NULL-bypassable; unmatched static provenance silently discarded; parity/report/churn minors
Task 15A: fix round 1/5 in progress — correct all five data/migration findings plus parity/report/churn minors with focused RED/GREEN coverage
Task 15A: fix round 1/5 (5 Important + 3 Minor findings addressed; commit 2e7278052; scoped re-review pending)
Task 15A: fix round 1/5 re-review (4/5 Important addressed; 1 Important open — historical registration audits inherit current-row provenance instead of event-time candidate_digest; 2 Minor cleanup findings open — report commit list and pre-remediation schema formatting churn)
Task 15A: fix round 2/5 in progress — preserve event-time audit provenance across register/reconfigure history and close both cleanup findings
Task 15A: fix round 2/5 (event-time audit history, report inventory, and schema churn addressed; commits 2e7278052..88b7f596c; scoped re-review pending)
Task 15A: fix round 2/5 re-review (3/3 findings addressed, 0 open; no new Critical/Important breakage; commits 2e7278052..88b7f596c)
Task 15A: complete (commits 4c11c9ef1..88b7f596c, review clean; focused/affected gates 16 + 137 + 218 + 246 + 138 passed, Ruff, strict MyPy, migration, and diff checks clean; full repository pytest deferred to exact-head final verification)
Task 15B: in progress (base 88b7f596c) — complete cancellation ownership, safe pending withdrawal, drain/cancel/terminal recovery, streamed manager bounds, and end-to-end admission deadlines
Task 15B: implementation at 3da9b30e7 under review — reported gates 119 focused/unit + 33 admission + 43 migration + 4 bridge + 34 ops passed; Ruff touched-files, strict MyPy, and diff checks clean
Task 15B: initial review rejected (1 Critical, 3 Important, 1 Minor; base 88b7f596c, head 3da9b30e7) — cancellation not anchored to protected physical job/signed expected shape; proof digest inert at backend; live/terminal conflicts can be ignored in submission and cancellation recovery; terminal evidence omits partition
Task 15B: minor (deferred to final review): complete crash/race safety matrix for post-scancel response loss, every cancellation replay outcome, withdrawal mismatch/race, and proof-digest rejection
Task 15B: fix round 1/5 in progress — anchor mutation to durable physical binding and signed envelope, enforce proof-token relation, require conflict-free live/accounting evidence, and add partition-complete terminal observations
Task 15B: fix round 1/5 pre-review concern confirmed — later intent-key drain/withdraw events hide the physical-bind record, so crash after withdrawal confirmation can strand a conclusively owned pending job; require durable retrieval plus restart regression before scoped re-review
Task 15B: fix round 1/5 (Critical cancellation anchoring/proof binding, 3 Important conflict/terminal findings, and pre-review physical-bind durability concern addressed; commits 3da9b30e7..781283ad4; scoped re-review pending)
Task 15B: fix round 1/5 re-review (4/5 findings addressed; 1 Important open — exact terminal can be adopted while the same job ID is live with a changed ownership token; commits 3da9b30e7..781283ad4)
Task 15B: minor (deferred to final review): pre-fix journal with an already-hidden legacy physical-bind record cannot reconstruct the new dedicated key; non-blocking because no live activation/journal exists
Task 15B: fix round 2/5 in progress — quarantine same-job live reuse before exact terminal adoption during unknown-submission recovery
Task 15B: fix round 2/5 (same-job live reuse blocks exact terminal adoption; commit ce84292a6; scoped re-review pending)
Task 15B: fix round 2/5 re-review (1/1 finding addressed, 0 Critical/Important open; no new Critical/Important breakage; commit ce84292a6)
Task 15B: complete (commits 88b7f596c..ce84292a6, review clean with 2 deferred Minor final-review items; exact-head gates 129 unit safety + 33 admission + 43 migration + 4 bridge + 34 ops passed, Ruff touched-files, strict MyPy, and diff checks clean)
Task 15C: in progress (base ce84292a6) — authenticated context, production multi-subject resolver/runtime assembly, journal-first heartbeat/drain-only, CSPRNG handoff/wrapper exchange, and production-entry two-pool acceptance
Task 15C: complete (implementation commit 228f6bd00; report/ledger metadata committed in follow-up; final gates 170 executor/runtime + 112 manager + bridge 4 passed twice + 39 ops/no-live passed, Ruff format/check, strict MyPy, diff check, and docs-superpowers guard clean; concern: existing Starlette/httpx deprecation warning only)
Task 15C follow-up: in progress (base 4c153be35) — close resumed production-entry harness/runtime gaps: public runtime builder/routed resolver/trusted process argv, approved profile-set binding with inert checked-in zero config, inventory terminal proof, handoff recovery, and journal-first sequence expectations; no live activation or positive runtime artifact defaults.
Task 15C follow-up: complete — production-entry harness now exercises public runtime builder, routed admission resolver, heartbeat loop, and trusted process argv; approved profile-set binding/inert checked-in CLI config fixed; final gates passed: 205 executor/runtime unit, 112 manager integration with existing Starlette warning, bridge 5 passed twice, 39 ops/no-live, 36 CLI, scoped Ruff on 23 pending Python files, strict MyPy, and diff/docs hygiene.
Task 15C follow-up fix round 1 review: 2 Important open at c8085c5f4 — executor acceptance/profile resolution bypasses public resolve_runtime_profile; shipped trusted wrapper exposes scoped credential without candidate executable/image authentication or race-free exec.
Task 15C follow-up fix round 2: complete — public runtime profile resolver shared by daemon assembly and executor render/acceptance; trusted wrapper pins candidate executable path/hash/owner/mode plus image digest, rejects invalid candidates before credential exposure, and execs verified `/proc/self/fd/<fd>`; lifecycle-store route request ruled out by Task 2/4/12/13 plan evidence and existing 404 route coverage; gates passed: 212 executor/runtime unit, bridge 5 passed twice, 112 manager integration with existing Starlette warning, 39 ops/no-live, 36 CLI, scoped Ruff on 7 pending Python files, strict MyPy, and diff/docs hygiene.
Task 15C follow-up fix round 2 re-review: 1 addressed, 2 Important open — executor selection/public resolver and lifecycle-route ruling accepted; current-UID-owned `0555` candidate remains mutable after hashing; `/proc/self/fd` exec uses a close-on-exec descriptor and real shebang candidates fail.
Task 15C follow-up fix round 3: complete — trusted wrapper now executes a sealed anonymous candidate snapshot, closes the mutable source descriptor before handoff/credential exposure, keeps only the sealed nonsecret descriptor inheritable for `/proc/self/fd` shebang exec, and preserves original argv/no-shell behavior; gates passed: focused RED/GREEN regressions, 214 executor/runtime unit, bridge 5 passed twice, 112 manager integration with existing Starlette warning, 39 ops/no-live, 36 CLI, scoped Ruff on 2 pending Python files, strict MyPy, and diff/docs hygiene.
Task 15C follow-up fix round 3 re-review: 2/2 prior Important findings addressed, 1 new Important open — sealed snapshot bytes were not independently rehashed after sealing, so a same-UID `/proc/<pid>/fd/<fd>` writer could mutate the unsealed memfd after the initial copy/hash and before seals.
Task 15C follow-up fix round 4: complete — trusted wrapper now independently rehashes the immutable sealed snapshot and constant-time compares it to the pinned candidate SHA-256 before physical binding, admission, handoff claim, credential exposure, or exec; gates passed: focused RED/GREEN regression, 215 executor/runtime unit, bridge 5 passed twice, 112 manager integration with existing Starlette warning, 39 ops/no-live, 36 CLI, scoped Ruff on 2 pending Python files, strict MyPy, and diff/docs hygiene.
