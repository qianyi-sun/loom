# Frontend Quality Boundary Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make every selected frontend change pass a strict, warning-free TypeScript, lint, unit/component coverage, production-build, deterministic Playwright, and accessibility boundary that is merge-blocking through `repository-checks`.

**Architecture:** Deliver #773 in three code stages: a type/test/coverage/CI foundation, a deterministic real-browser harness after #772 supplies the executable prefix-route contract, and the final axe gate after #777 supplies the accessible shared primitives. The current checked-in `web/src/api/schema.d.ts` contract remains the only wire-shape owner during this issue; pages consume it rather than replacing missing fields with local interfaces or casts. #778 separately replaces that legacy hand-stub with deterministic backend-model-driven OpenAPI generation. `workflow-plan` selects `web-checks`, and the stable `repository-checks` aggregate validates the selected job result. Keep #773 open after all repository work merges until one fixed staging candidate repeats the normal-user matrix and the #692 administrator matrix.

**Tech Stack:** TypeScript 5.5 strict mode, React 18, Vite 8, Vitest 4.1.8 with V8 coverage, Testing Library, Playwright Chromium 1.61.1, `@axe-core/playwright` 4.12.1, Python 3.11 planner contract tests, GitHub Actions.

## Global Constraints

- Scope is #773 only. #772 owns canonical redirects and prefix-stable executable assets; #777 owns accessible tokens/primitives; #692 owns secret-source authenticated staging sessions.
- Do not weaken `strict`, `noUnusedLocals`, `noUnusedParameters`, or any existing TypeScript compiler option.
- Do not add `exclude`, `@ts-ignore`, `@ts-expect-error`, `skipLibCheck` expansion, `any`, double assertions, or page-local response interfaces to make typechecking pass.
- During #773, the checked-in `web/src/api/schema.d.ts` owns Service API wire fields, `web/src/api/client.ts` owns transport and aliases wire types, and pages own view state only. #778 must replace the file's legacy hand-stub with deterministic offline OpenAPI generation; #773 must not invent a second interim schema system.
- A runtime field missing from the OpenAPI artifact is repaired at that ownership boundary and covered by a type-contract test; it is never recovered with `as`, `unknown as`, or a duplicate page interface.
- Retire all 105 baseline compiler errors: 54 `TS2304`, 38 `TS7031`, 3 `TS7006`, 3 `TS2339`, 2 `TS2345`, 2 `TS18046`, and one each of `TS2322`, `TS2352`, and `TS2353`.
- Vitest coverage includes all production `src/**/*.{ts,tsx}` and excludes only `src/api/schema.d.ts`; global floors are 80% statements, 80% lines, 80% functions, and 75% branches.
- Vitest and Playwright fail on unexpected console warnings/errors, real network from unit tests, unhandled rejections, React warnings, Router warnings, page errors, failed same-origin resources, HTML returned for JS/CSS, and an empty React root.
- Playwright uses deterministic mocked Service API responses locally. It never needs a credential, database, provider, external network, or protected GitHub secret.
- Browser coverage includes logged-out, normal-user, and administrator states at 1440×900 and 390×844 where layout differs.
- Serious and critical axe findings must be zero on every required page. No rule, selector, route, or impact suppression is allowed.
- `repository-checks` must fail when selected `web-checks` is cancelled, skipped unexpectedly, timed out, or failed.
- Use `npm ci` in CI; `package-lock.json` is the frozen dependency authority.
- Do not close #773 from repository-side checks. Final closure requires merged #777 primitives, the #692 admin-session path, and fixed-candidate staging evidence linked to #493 and #715.
- Raw credentials, session cookies, CSRF values, setup/reset/invite secrets, provider keys, bearer tokens, signed URLs, and browser storage state never enter traces, screenshots, artifacts, logs, or issue comments.

---

## Baseline and Error Ledger

At `d18526c417b4e2c85056869da8bbc62e34b15485`, `npm test` passes 49 files / 297 tests but emits an `ECONNREFUSED` attempt to `localhost:3000`. With all production files included and only `schema.d.ts` excluded, coverage is 78.34% statements, 70.26% branches, 77.50% functions, and 80.81% lines.

| Error family | Count | Exact ownership/remedy |
| --- | ---: | --- |
| `global` is undefined | 54 | Replace `global` with DOM-standard `globalThis` in the 14 affected test files. |
| untyped mock-call destructuring | 38 | Give shared helpers and returned spies the exact `FetchMock = MockInstance<typeof fetch>` type. |
| New Batch implicit/unknown values | 5 | Make `batchCall()` return `CreateBatchBody`; its `combinations` field then supplies callback and index types. |
| production contract/control-flow errors | 6 | Widen the token-label lookup key, make delivery readiness a type predicate, use an exhaustive subset switch, narrow setup lookup structurally, and consume the schema-owned Task response. |
| stale trial fixture contract | 1 | Add the runtime-real ownership fields to the API contract and prove them with `expectTypeOf`. |
| environment literal typo | 1 | Change `"prod"` to the declared `"production"` literal. |

## File Structure

- `web/src/api/schema.d.ts`: current checked-in wire contract; add only runtime-real Task, Team, and Trial ownership fields required by the baseline errors, pending the #778 generator migration.
- `web/src/api/client.ts`: transport plus aliases of schema-owned response/request types; no duplicate Task wire type.
- `web/src/test-utils/fetchMock.ts`: typed `FetchMock`, JSON response, URL, and request-body helpers shared by Vitest.
- `web/src/test-utils/qualityGuards.ts`: suite-wide unexpected-network, console, Router/React warning, and unhandled-rejection guard.
- `web/vitest.setup.ts`: install Testing Library matchers, cleanup, and the quality guards.
- `web/vite.config.ts`: V8 coverage inclusion, the sole generated-schema exclusion, and exact global thresholds.
- `web/src/__tests__/api-contract-types.test.ts`: compile-time ownership assertions for Task, Team, Trial, and batch request types.
- `web/src/__tests__/pages/TaskSetsList.test.tsx`: TaskSet list loading/error/empty/populated branches.
- `web/src/__tests__/pages/TaskSetSubmit.test.tsx`: required file, optional files, success, and API-error branches.
- `web/src/__tests__/pages/TaskSetDetail.test.tsx`: loading/not-found/error/tabs/rebuild/delete branches.
- `web/e2e/fixtures/api.ts`: deterministic logged-out/user/admin Service API router.
- `web/e2e/fixtures/guardedTest.ts`: Playwright fixture that enforces browser failure invariants.
- `web/e2e/routes.ts`: one route/state/heading matrix shared by navigation and axe specs.
- `web/e2e/logged-out.spec.ts`: canonical entry redirect and public setup/reset/invite error states.
- `web/e2e/user.spec.ts`: user list/detail route matrix on desktop and mobile.
- `web/e2e/admin.spec.ts`: administrator pages on desktop and mobile.
- `web/e2e/accessibility.spec.ts`: post-#777 serious/critical axe gate over the same matrix.
- `web/playwright.config.ts`: production-preview server, Chromium projects, deterministic retries, and secret-safe artifacts.
- `scripts/plan_ci_validations.py`: `web_checks` selection and reason reporting.
- `tests/ops/test_plan_ci_validations.py`: exact selection/non-selection contract.
- `tests/ops/test_ci_throughput_workflows.py`: workflow dependency and aggregate-result contract.
- `.github/workflows/ci.yml`: selected `web-checks` job and `repository-checks` enforcement.
- `docs/contributing/contributor-quickstart.md`: exact local frontend commands, thresholds, browser states, and dependency staging.

## Stage A — Type, Unit, Coverage, Build, and CI Foundation

### Task 1: Reassert the checked-in wire-type ownership boundary

**Files:**
- Create: `web/src/__tests__/api-contract-types.test.ts`
- Modify: `web/src/api/schema.d.ts`
- Modify: `web/src/api/client.ts`
- Modify: `web/src/pages/Tasks.tsx`

**Interfaces:**
- Consumes: `components["schemas"]`, `paths["/api/v1/tasks"]`, and `paths["/api/v1/batches"]` from the current checked-in `web/src/api/schema.d.ts` contract.
- Produces: exported `TaskList`, `Team`, `TrialDetail`, and `CreateBatchBody` aliases used by pages and tests without local response replicas.

- [ ] **Step 1: Add failing compile-time ownership assertions**

Create `web/src/__tests__/api-contract-types.test.ts`:

```ts
import { expectTypeOf, test } from "vitest";

import type { components, paths } from "../api/schema";

test("the checked-in API contract owns frontend-visible wire fields", () => {
  type Task = components["schemas"]["Task"];
  type Team = components["schemas"]["Team"];
  type Trial = components["schemas"]["Trial"];
  type CreateBatch = paths["/api/v1/batches"]["post"]["requestBody"]["content"]["application/json"];

  expectTypeOf<Task["name"]>().toEqualTypeOf<string | null>();
  expectTypeOf<Task["description"]>().toEqualTypeOf<string | null>();
  expectTypeOf<Task["agent_name"]>().toEqualTypeOf<string | null>();
  expectTypeOf<Task["verifier_name"]>().toEqualTypeOf<string | null>();
  expectTypeOf<Task["step_count"]>().toEqualTypeOf<number>();
  expectTypeOf<Team["disabled_at"]>().toEqualTypeOf<string | null>();
  expectTypeOf<Trial["submitted_by_user"]>().toEqualTypeOf<{
    id: string;
    username: string;
    team_id?: string | null;
    team_name?: string | null;
  } | null>();
  expectTypeOf<CreateBatch["combinations"]>().toMatchTypeOf<readonly unknown[] | undefined>();
});
```

- [ ] **Step 2: Run typecheck and prove ownership fields are missing**

Run:

```bash
cd web
npx tsc --noEmit --pretty false
```

Expected: non-zero with the recorded 105 baseline errors plus failures naming `Task.name`, `Team.disabled_at`, and `Trial.submitted_by_user`. Do not change `tsconfig.json`.

- [ ] **Step 3: Correct the contract-owned runtime fields**

In `components.schemas.Task`, add `name`, `description`, `agent_name`, `verifier_name`, and `step_count` with the exact types asserted above, and add `total: number` to `TaskList`. In `components.schemas.Team`, add nullable `disabled_at`, `disabled_reason`, `submissions_paused_at`, and `submissions_paused_reason`. In `components.schemas.Trial`, add nullable `owner_team`, `team_name`, and `submitted_by_user` matching the values already emitted by `trials._trial_row()` and `teams._serialize_team()`.

Export aliases from `web/src/api/client.ts` instead of redeclaring shapes:

```ts
export type TaskList =
  paths["/api/v1/tasks"]["get"]["responses"][200]["content"]["application/json"];
export type TaskRow = TaskList["items"][number];
export type CreateBatchBody =
  paths["/api/v1/batches"]["post"]["requestBody"]["content"]["application/json"];
```

Delete `TaskRow`, `TaskListResponse`, and the `as Promise<TaskListResponse>` conversion from `web/src/pages/Tasks.tsx`; keep the query call as:

```ts
api.listTasks({
  benchmark_id: benchmark || undefined,
  q: search.trim() || undefined,
  cursor: page.current ?? undefined,
  limit: "50",
});
```

- [ ] **Step 4: Prove the contract assertions and Task page pass**

Run:

```bash
cd web
npx vitest run src/__tests__/api-contract-types.test.ts src/__tests__/pages/Tasks.test.tsx
```

Expected: both files pass; no page-local Task response interface or promise cast remains.

- [ ] **Step 5: Commit the ownership slice**

```bash
git add web/src/api/schema.d.ts web/src/api/client.ts web/src/pages/Tasks.tsx \
  web/src/__tests__/api-contract-types.test.ts
git commit -m "fix(web): restore API type ownership (#773)"
```

### Task 2: Type every fetch mock and remove 99 test-source errors

**Files:**
- Create: `web/src/test-utils/fetchMock.ts`
- Modify: the 14 `TS2304` files listed in the baseline ledger
- Modify: `web/src/__tests__/pages/{AdminAccess,Benchmarks,Monitor,RunLibrary,Settings,Tasks,TrialDetail,UsageDashboard}.test.tsx`
- Modify: `web/src/__tests__/components/SubmitTrialModal.test.tsx`
- Modify: `web/src/__tests__/pages/NewBatch.test.tsx`
- Modify: `web/src/__tests__/lib/serverOrigin.test.ts`

**Interfaces:**
- Produces: `FetchMock = MockInstance<typeof fetch>`, `jsonResponse(body, status)`, `requestUrl(input)`, and `jsonRequestBody<T>(call)`.
- Consumes: native `fetch` parameter types; no Node `global` type is introduced.

- [ ] **Step 1: Add the typed fetch-test boundary**

Create `web/src/test-utils/fetchMock.ts`:

```ts
import type { MockInstance } from "vitest";

export type FetchMock = MockInstance<typeof fetch>;
export type FetchCall = Parameters<typeof fetch>;

export function requestUrl(input: FetchCall[0]): string {
  return input instanceof Request ? input.url : String(input);
}

export function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

export function jsonRequestBody<T>(call: FetchCall): T {
  const body = call[1]?.body;
  if (typeof body !== "string") throw new Error("expected JSON string request body");
  return JSON.parse(body);
}
```

- [ ] **Step 2: Replace all 54 browser-global errors mechanically**

Replace `global.fetch` with `globalThis.fetch` and `vi.spyOn(global, "fetch")` with `vi.spyOn(globalThis, "fetch")` in exactly:

```text
web/src/__tests__/api-client.test.ts
web/src/__tests__/components/Layout.test.tsx
web/src/__tests__/components/SubmitTrialModal.test.tsx
web/src/__tests__/frontend-config.test.ts
web/src/__tests__/hooks/providers.test.tsx
web/src/__tests__/pages/AdminAccess.test.tsx
web/src/__tests__/pages/Benchmarks.test.tsx
web/src/__tests__/pages/Home.test.tsx
web/src/__tests__/pages/InviteAccept.test.tsx
web/src/__tests__/pages/RateCardsAdmin.test.tsx
web/src/__tests__/pages/Settings.test.tsx
web/src/__tests__/pages/Tasks.test.tsx
web/src/__tests__/pages/TrialCompare.test.tsx
web/src/__tests__/pages/UsageDashboard.test.tsx
```

Run `rg -n '\bglobal\b' web/src/__tests__`; expected: no matches.

- [ ] **Step 3: Give every shared spy an exact callable type**

Change helper parameters and return annotations from unbound `ReturnType<typeof vi.spyOn>` / `ReturnType<typeof vi.fn>` to `FetchMock`. Import and use `requestUrl()` inside the 38 affected `.mock.calls.find`, `.filter`, and `.some` callbacks. The typed pattern is:

```ts
function setupFetch(): FetchMock {
  return vi.spyOn(globalThis, "fetch").mockImplementation(async (input, init) => {
    const url = requestUrl(input);
    if (url.endsWith("/api/v1/auth/me")) return jsonResponse(AUTH_ME);
    return jsonResponse({ detail: `unhandled ${url} ${init?.method ?? "GET"}` }, 404);
  });
}
```

This change covers the exact nine `TS7031` files named in the baseline table; do not annotate destructured callback parameters one by one with `any`.

- [ ] **Step 4: Type New Batch request inspection at the source**

Make `batchCall(spy: FetchMock)` return `{ url: string; body: CreateBatchBody } | null`, parse through `jsonRequestBody<CreateBatchBody>(found)`, and use optional chaining on `body.combinations`. This removes all three `TS7006` and both `TS18046` errors without asserting `unknown`.

Change the production-route fixture in `serverOrigin.test.ts` from `environment: "prod"` to `environment: "production"`.

- [ ] **Step 5: Run the compiler and confirm only production errors remain**

```bash
cd web
npx tsc --noEmit --pretty false 2>&1 | tee /tmp/loom-773-typecheck-after-tests.txt
```

Expected: none of `TS2304`, `TS7031`, `TS7006`, `TS18046`, `TS2322`, or `TS2353` remains; only the four production files in Task 3 may still fail.

- [ ] **Step 6: Commit the typed-test slice**

```bash
git add web/src/test-utils/fetchMock.ts web/src/__tests__
git commit -m "test(web): type browser-facing test mocks (#773)"
```

### Task 3: Fix the remaining production errors with real narrowing

**Files:**
- Modify: `web/src/pages/AdminAccess.tsx`
- Modify: `web/src/pages/BatchDetail.tsx`
- Modify: `web/src/pages/NewBatch.tsx`
- Modify: `web/src/pages/PasswordAction.tsx`
- Create: `web/src/__tests__/pages/PasswordAction.test.tsx`

**Interfaces:**
- Produces: `deliveryReady(value): value is ReadyDeliveryExport`; exhaustive `subsetIdentity`; structurally narrowed setup/reset lookup data.

- [ ] **Step 1: Add focused failing behavior tests**

Extend existing tests to assert: unknown token scopes render their raw value; ready delivery export calls the download endpoint; every five `subset_kind` values produces its stable suffix; setup lookup shows a team while reset lookup does not.

- [ ] **Step 2: Run the four focused files**

```bash
cd web
npx vitest run \
  src/__tests__/pages/AdminAccess.test.tsx \
  src/__tests__/pages/BatchDetail.test.tsx \
  src/__tests__/pages/NewBatch.test.tsx \
  src/__tests__/pages/PasswordAction.test.tsx
```

Expected: the new cases fail or the command reports the missing `PasswordAction.test.tsx` until it is created.

- [ ] **Step 3: Implement the four narrow fixes**

Widen only the lookup key, while preserving the literal option values:

```ts
const TOKEN_SCOPE_LABELS: ReadonlyMap<string, string> = new Map(
  TOKEN_SCOPE_OPTIONS.map(
    ({ value, label }): [string, string] => [value, label],
  ),
);
```

Make delivery readiness a predicate:

```ts
type ReadyDeliveryExport = DeliveryExport & {
  status: "ready";
  download_url: string;
};

function deliveryReady(
  value: DeliveryExport | null | undefined,
): value is ReadyDeliveryExport {
  return value?.status === "ready" && typeof value.download_url === "string";
}
```

Replace the final `subsetKind.replaceAll()` fallback with an exhaustive `switch` over `all`, `explicit`, `random_n`, `first_n`, and `last_n`. In `PasswordAction`, bind `const lookupData = lookup.data` and use `lookupData && "team" in lookupData` before reading `lookupData.team`; do not assert either query result.

- [ ] **Step 4: Establish a zero-error compiler baseline**

```bash
cd web
npx tsc --noEmit --pretty false
```

Expected: exit 0 and no output. Run `rg -n '@ts-(ignore|expect-error)|unknown as|as any' web/src`; expected: no new matches from #773.

- [ ] **Step 5: Commit the production narrowing slice**

```bash
git add web/src/pages web/src/__tests__/pages/PasswordAction.test.tsx
git commit -m "fix(web): satisfy strict production types (#773)"
```

### Task 4: Make Vitest warning-free and network-closed

**Files:**
- Create: `web/src/test-utils/qualityGuards.ts`
- Modify: `web/vitest.setup.ts`
- Modify: `web/src/test-utils/renderWithProviders.tsx`
- Modify: direct `MemoryRouter` tests in `AgentModelPicker`, `NavBar`, `ProviderCreate`, `ProviderDetail`, and `ProvidersList`
- Modify: the exact test files containing local `vi.restoreAllMocks()` lifecycle hooks:
  `AuthContext.test.tsx`, `api-client.test.ts`, `AgentModelPicker.test.tsx`,
  `EventTimeline.test.tsx`, `Layout.test.tsx`, `SubmitTrialModal.test.tsx`,
  `ModelsTab.test.tsx`, `frontend-config.test.ts`, `providers.test.tsx`,
  `useTrialEventStream.test.tsx`, `AdminAccess.test.tsx`,
  `BatchDetail.test.tsx`, `Benchmarks.test.tsx`, `Home.test.tsx`,
  `Monitor.test.tsx`, `NewBatch.test.tsx`, `ProviderCreate.test.tsx`,
  `ProviderDetail.test.tsx`, `ProvidersList.test.tsx`,
  `RateCardsAdmin.test.tsx`, `RunLibrary.test.tsx`, `Settings.test.tsx`,
  `Tasks.test.tsx`, `TrialCompare.test.tsx`, `TrialDetail.test.tsx`, and
  `UsageDashboard.test.tsx` under `web/src/__tests__/`.

**Interfaces:**
- Produces: `installQualityGuards()` with a rejecting default fetch mock and zero-tolerance console/unhandled-rejection assertions.

- [ ] **Step 1: Write a failing guard self-test**

Create `web/src/__tests__/quality-guards.test.ts` with one nested subprocess test invoking `vitest run` on a temporary spec that calls `console.warn("router warning")`, and assert the subprocess exits non-zero with `unexpected console.warn`. This proves the guard itself, without leaving an intentionally failing case in the normal suite.

- [ ] **Step 2: Implement suite-wide guards**

`qualityGuards.ts` installs a fresh default `vi.fn<typeof fetch>()` before each test that rejects with `Unexpected network request: METHOD URL`; it records `console.warn`, `console.error`, and browser `unhandledrejection` events. After each test it fails with the recorded messages, then removes the listener, unstubs globals, restores mocks, and clears timers. Remove file-local `vi.restoreAllMocks()` hooks so they cannot dismantle the guard before React Query settles.

- [ ] **Step 3: Opt into Router future behavior everywhere**

Pass these exact flags to the shared and five direct `MemoryRouter` instances:

```tsx
future={{ v7_startTransition: true, v7_relativeSplatPath: true }}
```

Use the same flags on `BrowserRouter` in `main.tsx` if React Router 6.30 emits either future warning there.

- [ ] **Step 4: Run the suite and prove clean output**

```bash
cd web
npm test 2>&1 | tee /tmp/loom-773-vitest-clean.txt
rg -n 'ECONNREFUSED|Warning:|React Router Future Flag|Unhandled|console\.(warn|error)' \
  /tmp/loom-773-vitest-clean.txt
```

Expected: 49 or more test files pass; the `rg` command returns no matches. Any unexpected request fails its owning test instead of opening a socket.

- [ ] **Step 5: Commit the test-hygiene slice**

```bash
git add web/vitest.setup.ts web/src/test-utils web/src/__tests__ web/src/main.tsx
git commit -m "test(web): fail on warnings and network leaks (#773)"
```

### Task 5: Install and enforce the frontend coverage floor

**Files:**
- Modify: `web/package.json`
- Modify: `web/package-lock.json`
- Modify: `web/vite.config.ts`
- Create: `web/src/__tests__/pages/TaskSetsList.test.tsx`
- Create: `web/src/__tests__/pages/TaskSetSubmit.test.tsx`
- Create: `web/src/__tests__/pages/TaskSetDetail.test.tsx`
- Modify: `web/src/__tests__/pages/RateCardsAdmin.test.tsx`
- Modify: `web/src/__tests__/pages/ProviderDetail.test.tsx`
- Modify: `web/src/__tests__/pages/RunLibrary.test.tsx`

**Interfaces:**
- Produces: `npm run test:coverage`, whose process exit code is the coverage gate.

- [ ] **Step 1: Add the exact coverage provider and script**

```bash
cd web
npm install --save-dev --save-exact @vitest/coverage-v8@4.1.8
```

Add `"test:coverage": "vitest run --coverage"` to `scripts`.

- [ ] **Step 2: Configure all-production coverage and exact thresholds**

Add to `test` in `vite.config.ts`:

```ts
coverage: {
  provider: "v8",
  include: ["src/**/*.{ts,tsx}"],
  exclude: ["src/api/schema.d.ts"],
  reporter: ["text", "json-summary", "lcov"],
  thresholds: {
    statements: 80,
    lines: 80,
    functions: 80,
    branches: 75,
  },
},
```

- [ ] **Step 3: Confirm the current baseline fails**

Run `npm run test:coverage`.

Expected: non-zero; baseline is approximately 78.34 statements / 70.26 branches / 77.50 functions / 80.81 lines once all production files are included.

- [ ] **Step 4: Cover the exact missing product branches**

Add these cases using `renderWithProviders`, `FetchMock`, and `userEvent`:

- TaskSets list: loading, rejected request, empty response, ready row, materializing row, failed row, evaluation-only label, and trajectory-only label.
- TaskSet submit: missing manifest, manifest-only success navigation, manifest plus verifier/transform FormData keys, structured API failure, and disabled pending submit.
- TaskSet detail: loading, 404, non-404 error, ready overview with warnings, empty errors tab, populated errors tab, rebuild invalidation, delete cancel, and successful delete navigation.
- Rate Cards: non-admin read-only rendering; one-entry and empty-entry cards; invalid JSON; non-object JSON; create rejection; create success.
- Provider Detail: loading, 404, models tab, edit success, test failure, and delete failure.
- Run Library: empty results, query failure, next/previous cursor, user scope, admin all-team scope, and export failure.

These scenarios target the untested TaskSet pages and the existing lowest branch/function files; they are behavior assertions, not coverage-only imports.

- [ ] **Step 5: Prove the fixed floor and sole exclusion**

```bash
cd web
npm run test:coverage
node -e 'const s=require("./coverage/coverage-summary.json").total; for (const k of ["statements","lines","functions"]) if (s[k].pct < 80) process.exit(1); if (s.branches.pct < 75) process.exit(1)'
```

Expected: both commands exit 0; report totals are at least 80/75/80/80. `rg -n 'exclude:' web/vite.config.ts` shows only the generated `src/api/schema.d.ts` coverage exclusion.

- [ ] **Step 6: Commit the coverage slice**

```bash
git add web/package.json web/package-lock.json web/vite.config.ts web/src/__tests__
git commit -m "test(web): enforce frontend coverage floor (#773)"
```

### Task 6: Add selected `web-checks` and feed `repository-checks`

**Files:**
- Modify: `scripts/plan_ci_validations.py`
- Modify: `tests/ops/test_plan_ci_validations.py`
- Modify: `tests/ops/test_ci_throughput_workflows.py`
- Modify: `.github/workflows/ci.yml`

**Interfaces:**
- Produces: `ValidationPlan.web_checks`, GitHub output `web_checks`, job `web-checks`, aggregate env `WEB_SELECTED` / `WEB_RESULT`.

- [ ] **Step 1: Write planner selection tests**

Parameterize paths that must select `web_checks=True`:

```python
[
    "web/src/App.tsx",
    "web/src/api/schema.d.ts",
    "web/package-lock.json",
    "web/playwright.config.ts",
    "deploy/Dockerfile.web",
    "deploy/nginx-spa.conf",
    "deploy/web-runtime-config.sh",
    "scripts/ops/frontend_route_smoke.py",
    "src/loom_cli/templates/k8s/ingress.yaml.j2",
    ".github/workflows/ci.yml",
]
```

Also assert `docs/user-guide.md` does not select it, `merge_group` does, and a planner change selects it.

- [ ] **Step 2: Run planner tests and confirm `web_checks` is absent**

```bash
uv run pytest tests/ops/test_plan_ci_validations.py -q
```

Expected: failures naming missing `ValidationPlan.web_checks`.

- [ ] **Step 3: Add `web_checks` to the planner**

Add it to `HEAVY_CHECKS`, the dataclass, output serialization, merge-group/planner-change selection, and select it for the exact/prefix paths above. A web source path may also select images/staging under existing rules; `web_checks` does not replace those jobs.

- [ ] **Step 4: Write the workflow contract before editing YAML**

Extend `test_repository_checks_context_is_parallel_aggregator` to require `web-checks` in `repository-checks.needs`, `web_checks` in workflow-plan outputs, and exact aggregate env values:

```python
"WEB_SELECTED": "${{ needs.workflow-plan.outputs.web_checks }}",
"WEB_RESULT": "${{ needs.web-checks.result }}",
```

Assert the job runs only when selected and contains, in order, `npm ci`, `npm run typecheck`, `npm run lint`, `npm run test:coverage`, and `npm run build`.

- [ ] **Step 5: Add the foundation job and aggregate enforcement**

Add `"typecheck": "tsc --noEmit"` to `web/package.json`. In `.github/workflows/ci.yml`, expose the planner output and create `web-checks` on Ubuntu with Node `20.19.5`, npm cache keyed by `web/package-lock.json`, working directory `web`, and a 15-minute timeout. Add it to `repository-checks.needs` and call:

```bash
require_selected "$WEB_SELECTED" web-checks "$WEB_RESULT"
```

- [ ] **Step 6: Verify workflow and frontend foundation locally**

```bash
uv run pytest tests/ops/test_plan_ci_validations.py \
  tests/ops/test_ci_throughput_workflows.py -q
cd web
npm ci
npm run typecheck
npm run lint
npm run test:coverage
npm run build
```

Expected: Python contract tests and all five frontend commands pass with zero warnings.

- [ ] **Step 7: Commit the CI foundation**

```bash
git add scripts/plan_ci_validations.py tests/ops/test_plan_ci_validations.py \
  tests/ops/test_ci_throughput_workflows.py .github/workflows/ci.yml \
  web/package.json web/package-lock.json
git commit -m "ci(web): add required frontend quality job (#773)"
```

At this point open the foundation PR with `Advances #773`; do not use a closing keyword. Keep #773 `[WIP]` / In Progress because browser, axe, and staging evidence remain.

## Stage B — Deterministic Browser Harness (after #772)

### Task 7: Build the guarded Playwright runtime

**Files:**
- Modify: `web/package.json`
- Modify: `web/package-lock.json`
- Create: `web/playwright.config.ts`
- Create: `web/e2e/fixtures/guardedTest.ts`

**Interfaces:**
- Produces: `test` fixture with `role: "logged-out" | "user" | "admin"`; collects browser failures and verifies `#root` is non-empty.

- [ ] **Step 1: Install exact browser dependencies**

```bash
cd web
npm install --save-dev --save-exact @playwright/test@1.61.1
npx playwright install chromium
```

Add scripts:

```json
"test:e2e": "npm run build && playwright test --grep-invert @axe",
"test:browser": "npm run build && playwright test"
```

- [ ] **Step 2: Configure a production preview and two viewport projects**

Use `baseURL: "http://127.0.0.1:4173"`, `webServer.command: "npm run preview -- --host 127.0.0.1 --port 4173"`, `reuseExistingServer: !process.env.CI`, `trace: "retain-on-failure"`, `screenshot: "only-on-failure"`, and `video: "off"`. Define projects `desktop` at 1440×900 and `mobile` at 390×844, both Chromium.

- [ ] **Step 3: Implement same-origin resource and runtime guards**

In `guardedTest.ts`, register `console`, `pageerror`, `requestfailed`, and `response` listeners before navigation. Fail on warning/error console messages, any page error, failed same-origin document/script/stylesheet/font/image request, non-2xx static response, JS without a JavaScript MIME, CSS without `text/css`, or JS/CSS returning `text/html`. After each case require `#root` to contain a child element. Exclude fulfilled API `fetch`/XHR responses from the static-asset rule; API completeness is enforced by Task 8's catch-all route.

- [ ] **Step 4: Add a deliberate guard regression test**

Create one test that fulfills `/assets/bad.js` with `200 text/html`, calls the guard validator as a pure function, and expects `asset returned HTML` in its result. Do not navigate to an intentionally broken page in the normal suite.

- [ ] **Step 5: Commit the browser foundation**

```bash
git add web/package.json web/package-lock.json web/playwright.config.ts web/e2e
git commit -m "test(web): add guarded Playwright runtime (#773)"
```

### Task 8: Add deterministic role fixtures and the complete route matrix

**Files:**
- Create: `web/e2e/fixtures/api.ts`
- Create: `web/e2e/routes.ts`
- Create: `web/e2e/logged-out.spec.ts`
- Create: `web/e2e/user.spec.ts`
- Create: `web/e2e/admin.spec.ts`

**Interfaces:**
- Consumes: route/state tables and `role` from `guardedTest.ts`.
- Produces: a closed API router; every unlisted `/api/v1/*` request returns 599 with `Unhandled deterministic API route` and fails the test.

- [ ] **Step 1: Implement the deterministic API router**

Fulfill `loom-frontend-config.json` with local/root metadata. Fulfill `/api/v1/auth/me` with 401 for logged-out, a fixed `Ada / Research` member for user, and a fixed `Qianyi / Admin` platform administrator for admin. Add exact stable bodies for overview, catalog/backends/agents/models, monitor summary, batches/trials, run library, provider connections/models, TaskSets, tokens/team detail, admin teams/requests/invites/audit, and rate cards. Reuse the schema-valid fixtures already exercised by the corresponding Vitest page files; move shared constants rather than creating a second variant. For all timestamps use fixed `2026-07-10T12:00:00Z`; for IDs use descriptive fixed strings such as `batch-user-1`, `trial-user-1`, `provider-1`, and `taskset-1`.

- [ ] **Step 2: Define one shared acceptance matrix**

```ts
export const loggedOutRoutes = [
  ["/", "Sign in to run and review evaluations"],
  ["/settings", "Sign in to run and review evaluations"],
  ["/auth/setup", "Set Password"],
  ["/auth/reset", "Reset Password"],
  ["/invites/accept", "Invite link required"],
] as const;

export const userRoutes = [
  ["/", "Team overview"],
  ["/batches/new", "New batch"],
  ["/monitor", "Monitor"],
  ["/library", "Run Library"],
  ["/providers", "Provider connections"],
  ["/task-sets", "Task Sets"],
  ["/settings", "Team Settings"],
  ["/batches/batch-user-1", "Deterministic batch"],
  ["/trials/trial-user-1", "trial-user-1"],
  ["/library/batches/batch-user-1", "Deterministic batch"],
  ["/providers/provider-1", "Deterministic provider"],
  ["/task-sets/taskset-1", "taskset-1"],
] as const;

export const adminRoutes = [
  ["/admin/access", "Team access"],
  ["/rate-cards", "Rate cards"],
] as const;
```

- [ ] **Step 3: Cover logged-out behavior**

Assert `/` settles at `/settings`, the sign-in/onboarding controls mount, setup/reset without tokens show their explicit error state, and invite-without-code shows `Invite link required`. Run the whole logged-out table in both viewport projects.

- [ ] **Step 4: Cover user and administrator navigation**

For every route, navigate directly, assert the heading, assert `#root` is non-empty, and assert the role-specific navigation does not expose admin links to a normal user. Run all user and admin table entries in desktop and mobile projects; this is the required 1440×900 / 390×844 matrix, not a screenshot-only visual test.

- [ ] **Step 5: Run the browser matrix offline**

```bash
cd web
npm run test:e2e
```

Expected: Chromium passes both projects; the deterministic API catch-all reports no request; no console/page/resource/root guard fires.

- [ ] **Step 6: Commit the deterministic journeys**

```bash
git add web/e2e
git commit -m "test(web): cover deterministic role journeys (#773)"
```

### Task 9: Make Playwright part of selected `web-checks`

**Files:**
- Modify: `.github/workflows/ci.yml`
- Modify: `tests/ops/test_ci_throughput_workflows.py`

**Interfaces:**
- Consumes: merged #772 route/same-origin asset checks and Tasks 7-8.
- Produces: browser artifacts only on failure and a merge-blocking browser result.

- [ ] **Step 1: Extend the workflow contract test**

Assert `web-checks` installs Chromium with `npx playwright install --with-deps chromium`, runs `npm run test:e2e`, and uploads `web/playwright-report` plus `web/test-results` only when `failure()`.

- [ ] **Step 2: Run the contract and confirm the browser step is absent**

```bash
uv run pytest tests/ops/test_ci_throughput_workflows.py \
  -k repository_checks_context_is_parallel_aggregator -q
```

Expected: fail on the missing Playwright command.

- [ ] **Step 3: Add browser execution after the production build**

Install Chromium after `npm ci`, keep the existing production build, run `npm run test:e2e`, and upload failure artifacts with five-day retention. Do not upload traces from passing runs.

- [ ] **Step 4: Verify local and workflow contracts**

```bash
cd web && npm run test:e2e
cd .. && uv run pytest tests/ops/test_plan_ci_validations.py \
  tests/ops/test_ci_throughput_workflows.py -q
```

Expected: both viewport projects and both Python files pass.

- [ ] **Step 5: Commit the browser CI stage**

```bash
git add .github/workflows/ci.yml tests/ops/test_ci_throughput_workflows.py
git commit -m "ci(web): gate deterministic browser journeys (#773)"
```

Open this as a second `Advances #773` PR after #772 has merged. Keep #773 open because the strict axe gate and fixed-candidate evidence remain.

## Stage C — Final Axe Gate (after #777)

### Task 10: Enforce zero serious/critical axe findings

**Files:**
- Modify: `web/package.json`
- Modify: `web/package-lock.json`
- Create: `web/e2e/accessibility.spec.ts`
- Modify: `.github/workflows/ci.yml`
- Modify: `tests/ops/test_ci_throughput_workflows.py`

**Interfaces:**
- Consumes: merged #777 accessible primitives and the shared route tables.
- Produces: `npm run test:a11y`, which fails on any serious or critical violation.

- [ ] **Step 1: Install the exact axe adapter**

```bash
cd web
npm install --save-dev --save-exact @axe-core/playwright@4.12.1
```

Add `"test:a11y": "npm run build && playwright test --grep @axe"`.

- [ ] **Step 2: Add the shared strict axe assertion**

For every logged-out, user, and admin matrix entry in both projects, navigate with the deterministic role fixture and run:

```ts
const result = await new AxeBuilder({ page })
  .withTags(["wcag2a", "wcag2aa", "wcag21a", "wcag21aa"])
  .analyze();
const blocking = result.violations.filter(
  ({ impact }) => impact === "serious" || impact === "critical",
);
expect(blocking, JSON.stringify(blocking, null, 2)).toEqual([]);
```

Do not call `disableRules`, `exclude`, `include`, or filter by route/selector.

- [ ] **Step 3: Run axe against the post-#777 baseline**

```bash
cd web
npm run test:a11y
```

Expected: all role/route/viewport cases pass with zero serious/critical findings. Any finding is fixed in the #777-owned primitive or its consuming page before #773 advances; it is never allowlisted here.

- [ ] **Step 4: Add axe to `web-checks`**

Update the workflow test first to require `npm run test:a11y`, then add that command after functional browser tests. `repository-checks` already consumes the single `web-checks` result, so no new branch-protection context is introduced.

- [ ] **Step 5: Commit the final repository gate**

```bash
git add web/package.json web/package-lock.json web/e2e/accessibility.spec.ts \
  .github/workflows/ci.yml tests/ops/test_ci_throughput_workflows.py
git commit -m "test(web): enforce serious accessibility gate (#773)"
```

Open this as a third `Advances #773` PR after #777 merges. Keep #773 open for #692-backed fixed-candidate staging evidence.

## Stage D — Documentation, Full Verification, and Fixed-Candidate Evidence

### Task 11: Document and verify the complete local boundary

**Files:**
- Modify: `docs/contributing/contributor-quickstart.md`

**Interfaces:**
- Consumes: Stages A-C.
- Produces: one copy/paste local frontend quality sequence matching CI.

- [ ] **Step 1: Update the contributor quickstart**

Replace `npm install` with `npm ci` for frozen installs and document:

```bash
cd web
npm ci
npm run typecheck
npm run lint
npm test
npm run test:coverage
npm run build
npx playwright install chromium
npm run test:e2e
npm run test:a11y
```

State the 80 statements/lines/functions and 75 branches floors, sole generated-schema exclusion, deterministic role states, two viewports, and zero serious/critical axe rule. State that #772 owns prefix-route smoke and #692 owns live admin-session acquisition.

- [ ] **Step 2: Run the full repository-side #773 boundary**

```bash
cd web
npm ci
npm run typecheck
npm run lint
npm run test:coverage
npm run build
npx playwright install chromium
npm run test:e2e
npm run test:a11y
cd ..
uv run pytest tests/ops/test_plan_ci_validations.py \
  tests/ops/test_ci_throughput_workflows.py \
  tests/ops/test_repository_verification_contract.py -q
git diff --check
```

Expected: every command exits 0; Vitest is warning/network clean; coverage is at least 80/75/80/80; browser and axe pass both viewports; workflow contracts pass; `git diff --check` is empty.

- [ ] **Step 3: Commit documentation**

```bash
git add docs/contributing/contributor-quickstart.md
git commit -m "docs(web): document frontend quality boundary (#773)"
```

### Task 12: Run fixed-candidate staging acceptance and only then close #773

**Files:**
- No repository file changes required; evidence is stored in the existing candidate evidence root and linked from issues with secrets removed.

**Interfaces:**
- Consumes: a fixed merged `dev` SHA/image, #772 route smoke, #777 axe-clean UI, and #692 browser-session source.
- Produces: sanitized logged-out, normal-user, and administrator staging evidence linked to #773, #493, and #715.

- [ ] **Step 1: Record immutable candidate identity before testing**

Record the full `dev` commit, web image digest, public route, frontend route/API bases, browser version, viewport, and test start time. Stop if the deployment does not match the recorded SHA/digest.

- [ ] **Step 2: Run logged-out staging acceptance**

Against `https://yylx.world/dev`, cover canonical entry, `/settings`, setup/reset error states, invite error state, desktop/mobile mount, console/page/resource guard, correct same-origin asset MIME, and axe serious/critical zero. Save only sanitized status/URL/MIME/violation counts; omit query secrets and cookies.

- [ ] **Step 3: Run the ordinary-user matrix**

Using the ordinary #493 user path, repeat Home, New Batch, Monitor, Run Library, Providers, TaskSets, Settings, and representative batch/trial/library/provider/taskset detail routes at both viewports. Record route outcome, heading/mount, console/resource result, and axe counts. Do not use DB, SSH, admin-only artifact links, or raw provider credentials.

- [ ] **Step 4: Run the #692 administrator matrix**

Acquire the browser session only through #692's secret-source mechanism, then cover Admin Access and Rate Cards at both viewports. Do not export storage state, cookies, CSRF values, setup/reset links, or invite codes.

- [ ] **Step 5: Reconcile CI and evidence before issue closure**

Confirm the fixed SHA has successful current-head `repository-checks` and that its selected `web-checks` result is success. Link the sanitized evidence package to #773, #493, and #715. Close #773 only if #777 is merged, #692 supplied the admin path, all three role matrices pass, and no serious/critical axe or browser guard failure remains. Otherwise retain `[Needs validation]` and record the exact failing route, role, viewport, candidate SHA, and reproduction.

---

## Stage/PR Boundaries

1. **Foundation PR:** Tasks 1-6. Typecheck, warning-free Vitest, coverage, build, selection, and aggregate enforcement. `Advances #773`.
2. **Browser PR after #772:** Tasks 7-9. Guarded Playwright role/viewport matrix. `Advances #773`.
3. **Axe PR after #777:** Task 10 plus Task 11 documentation. `Advances #773`.
4. **Live validation after #692:** Task 12. No repository PR unless validation exposes a focused #773 defect.

Every PR targets `dev`, enables squash auto-merge immediately, and uses `ci:images` because `web/package*.json` and `web/src/**` are image-sensitive. None may auto-close #773 before Task 12.
