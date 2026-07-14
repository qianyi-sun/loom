# Loom Frontend Hardening Design

Status: Proposed for implementation

Date: 2026-07-10

Tracking umbrella: [#771](https://github.com/qianyi-sun/loom/issues/771)

## Context

The 2026-07-10 frontend review found one live P0 entry-route failure and a
set of correctness, quality-gate, accessibility, performance, security, UX,
and maintainability gaps. The review covered `origin/dev` at
`bbe2c0a623ea063878b8c13d0d232a81a9f55f2d`, the deployed staging frontend,
desktop and mobile logged-out rendering, frontend source and tests, CI,
Ingress and Nginx configuration, runtime frontend configuration, and public
auth APIs.

The immediate failure is deterministic:

1. `https://yylx.world/dev` returns the SPA shell with HTTP 200.
2. The browser keeps the no-slash URL while the Ingress internally rewrites
   the request before proxying it to the web pod.
3. Vite's intentional `base: "./"` makes the shell resolve its assets against
   the document URL.
4. A document URL ending in `/dev` resolves `./assets/*` as root `/assets/*`.
5. Root assets return 404, so the React root stays empty.
6. `https://yylx.world/dev/` resolves the same files under `/dev/assets/*`
   and renders.

There is a second route-depth failure. A direct visit to a multi-segment route
such as `/dev/batches/example-id` resolves `./assets/*` as
`/dev/batches/assets/*`. That pseudo-asset path falls through to the SPA shell
and returns `200 text/html` instead of JavaScript or CSS. A redirect for only
the exact `/dev` prefix therefore cannot make the asset contract correct.

The surrounding quality gates did not catch the failure. ESLint, 297 Vitest
tests, and the Vite production build pass, while `npx tsc --noEmit` reports
105 errors and no required browser test loads the deployed HTML and referenced
assets. Other review findings are captured by #773 through #781, #783, and
existing #28, #212, #692, #493, and #715.

## Goals

- Restore a canonical, asset-safe browser entry contract for `/dev` and
  future `/prod` routes.
- Make frontend type, unit/component, production-build, browser, and
  accessibility checks required and trustworthy.
- Fix user-visible data completeness and mutation reliability defects.
- Make onboarding, navigation, responsive behavior, and terminology usable by
  ordinary users without internal knowledge.
- Replace blank startup, render, and lazy-chunk failures with recoverable,
  redacted error boundaries.
- Establish accessible shared interaction primitives and browser security
  headers.
- Reduce initial bundle and maintenance risk through explicit module
  boundaries and dependency modernization.
- Produce fixed-commit staging evidence for the complete normal-user and
  administrator browser journeys without raw secret disclosure.

## Non-goals

- Rewriting the frontend from React/Vite to another framework.
- Replacing the existing Service API or cookie/CSRF browser authentication
  model.
- Adding a root `/assets` route shared by prod and staging.
- Filtering internal teams in the browser by naming convention.
- Raising or suppressing Vite's bundle warning instead of reducing the
  initial dependency surface.
- Treating a passing Vite build as a substitute for TypeScript or browser
  validation.
- Combining every change into one unreviewable pull request.

## Approaches Considered

### 1. One frontend hardening branch and one large pull request

This maximizes apparent throughput but creates a high-conflict branch across
Ingress, CI, auth, pages, primitives, and dependencies. Failures are difficult
to attribute, auto-merge cannot provide incremental safety, and staging cannot
identify which behavior introduced a regression. This approach is rejected.

### 2. Fully independent pull requests opened in parallel

This gives fast initial activity but ignores shared boundaries. Accessibility,
destructive actions, responsive layouts, architecture decomposition, and
dependency upgrades overlap the same components and tests. Parallel changes
would repeatedly rebase or duplicate primitives. This approach is rejected.

### 3. Dependency-gated focused pull requests with parallel leaf lanes

This is the selected approach. Shared foundations merge first. Independent
correctness and infrastructure slices then run in parallel, followed by
component/UX work and dependency modernization. Each pull request has one
primary issue, explicit acceptance evidence, and a clean `dev` target with
squash auto-merge enabled.

## Architectural Decisions

### Prefix-stable assets and canonical routes are separate invariants

The web startup path derives the served `index.html` atomically from the
immutable build output and the validated runtime route path. For prefixed
deployments it converts generated `./assets/*` references to absolute
environment-prefixed asset URLs such as `/dev/assets/*` or `/prod/assets/*`.
For a root deployment it preserves the root build contract. The transform is
idempotent across container restarts and cannot retain or double-apply a
different environment's prefix.

This targeted asset transformation is preferred over injecting a global HTML
`<base>` because `<base>` changes every relative URL and form/navigation
semantic, while only build assets require runtime rebasing. It preserves one
immutable web image for dev and prod and makes exact, one-segment, and
multi-segment browser routes load the same canonical assets.

Canonical route enforcement still belongs before SPA HTML is returned.

The exact no-slash prefix must redirect before returning SPA HTML:

- `/dev` returns `308` with `Location: /dev/`.
- `/prod` returns `308` with `Location: /prod/` when production is deployed.
- Query strings are preserved.
- Deep links continue through the normal prefixed SPA route.

The current ingress-nginx admission/controller behavior rules out a dynamic
`permanent-redirect` annotation and an unanchored Exact path: request variables
are rejected by redirect validation, and host-wide regex promotion can make an
Exact path match deeper routes. The selected implementation therefore:

1. changes rewrite routes to accept only slash-prefixed paths such as
   `/dev/(...)` and `/prod/(...)`;
2. forwards an explicitly end-anchored exact prefix route to the existing web
   pod without the rewrite annotation; and
3. lets `nginx-spa.conf` return the 308 with `$is_args$args` before any shell is
   served.

Rendered tests are necessary but insufficient: the merged Ingress resources
and regex ordering are verified against the repository's real ingress-nginx
controller in cluster smoke.

React-side redirects and shared root assets are rejected because React cannot
run before its bundle loads and shared root assets weaken environment
isolation.

### Browser smoke validates the executable shell, not metadata alone

The route smoke must fetch the canonical entry shell, parse same-origin module
and stylesheet references, and verify every referenced asset returns HTTP 200
with an expected MIME type. Browser acceptance then navigates through the
no-slash entry, canonical entry, one-segment routes, and multi-segment list and
detail routes. It verifies that the React root mounts and fails on console
errors, failed same-origin resources, nested pseudo-asset URLs, or an HTML SPA
fallback returned with asset status 200.

Runtime config validation remains necessary but is no longer sufficient.

### Frontend CI is an explicit required boundary

The repository will add a dedicated frontend check that runs when frontend,
generated API types, SPA deploy configuration, or related workflow inputs
change. It runs:

1. deterministic dependency installation;
2. strict TypeScript checking;
3. ESLint with zero warnings;
4. unit/component tests and explicit coverage;
5. production build and bundle budget reporting;
6. focused browser navigation tests;
7. automated accessibility checks.

The aggregate repository context cannot pass if the required frontend check
fails. The optional image build remains a separate artifact validation and
does not replace source checks.

### Public team discovery is domain-owned

Team self-registration/discoverability is represented explicitly in the Team
domain and enforced by the public API. Internal and smoke teams default to
non-discoverable. The frontend renders the returned policy-backed set and does
not infer policy from team names.

### User mutations share one reliability contract

Destructive actions use one accessible modal and mutation-state contract:

- identify the target and consequence;
- require the appropriate confirmation level;
- submit once per target;
- remain open while pending;
- close or navigate only after success;
- render a redacted, retryable error in context after failure.

### Accessibility and responsive behavior are shared primitives

Contrast, focus, dialog, tabs, tables, landmarks, live regions, reduced
motion, responsive overflow, page titles, and filter behavior are implemented
through shared tokens and primitives. Page-specific patches are acceptable
only when the page has a genuinely unique semantic requirement.

### Refactoring follows correctness

Large page and API modules are decomposed after the correctness and shared
primitive contracts are green. Refactoring may not change externally visible
behavior without a focused issue and failing behavior test. Dependency major
upgrades run last so compatibility work does not obscure product defects.

## Execution Graph

### Foundation wave

- #772 canonical route and executable asset/browser smoke.
- #773 TypeScript and required frontend CI foundation.
- #692 staging-only authenticated admin browser-session path may proceed in
  parallel because it is an operational dependency and does not share web
  source files.
- #783 startup and route Error Boundaries begins after #773 establishes the
  type/browser harness and before lazy route work begins.

### Correctness and infrastructure wave

After the relevant foundation checks are available, these slices may run in
parallel in separate worktrees:

- #774 Run Library and Admin audit cursor completeness.
- #775 public-registration policy and semantic auth forms, after current PR
  #769 settles its overlapping `auth.py` baseline.
- #780 security headers and third-party font removal.

#780 waits for #772 because both touch rendered Ingress or Nginx contracts.

### Interaction and UX wave

- #777 establishes accessible tokens and shared primitives.
- #776 consumes the dialog/mutation primitive for destructive workflows.
- #779 applies responsive and information-architecture rules to product
  pages without duplicating the guided New Batch direction owned by #28.
- #28 implements and accepts the complete guided New Batch/provider/Monitor
  product flow after the shared form, tabs, error, and responsive contracts are
  available.

#776 and #779 do not independently create competing modal, tabs, table, or
form primitives.

### Architecture and modernization wave

- #778 decomposes large modules after product behavior is stable.
- #212 adds route-level bundle decomposition and budgets after #778 establishes
  stable module boundaries, using the reporting introduced by #773.
- #781 upgrades Router, lint, test, build, and React boundaries in bounded
  compatibility slices after #773, #777, and #778 are green.

### Final acceptance wave

- Roll a fixed `dev` commit and web image to staging from `platform-dev`.
- Run canonical-route and static-asset smoke.
- Run logged-out desktop/mobile browser journeys.
- Use the #692 secret-source path for authenticated administrator validation.
- Run a normal-user journey covering onboarding/login, provider/model
  readiness, batch submission, monitoring, pagination, detail/diagnosis,
  destructive-action failure recovery, and artifact download.
- Record browser console/network evidence, accessibility results, route and
  API bases, commit/image identity, IDs safe to share, and artifact integrity.
- Link evidence to #493 and #715 before closing #771.

## Pull Request and Issue State Rules

- Every implementation branch starts from current `origin/dev` in an isolated
  worktree and uses the `codex/` prefix.
- Each pull request targets `dev`, references one primary focused issue, and
  uses `Refs` or `Advances` until its issue acceptance is complete.
- Squash auto-merge is enabled immediately for every normal `dev` pull
  request; the four required CI contexts are its only merge authority.
- The actively implemented issue has a `[WIP]` title and Project Status
  `In Progress`; queued issues remain `Todo` without `[WIP]`.
- An issue moves to `[Needs validation]` when merged code still requires live
  evidence. It closes only after a fresh acceptance check proves the original
  failure is gone.
- Shared docs, architecture, runbooks, and generated contracts are updated in
  the same pull request as the behavior they describe.

## Acceptance Mapping

| Issue | Required proof |
| --- | --- |
| #772 | 308 canonical redirects, prefixed assets, deep-link browser mount, no same-origin asset failures |
| #773 | clean typecheck/lint/test/coverage/build/browser/a11y required CI |
| #774 | 51+ row forward/back traversal with no gaps or duplicates and correct filter reset |
| #775 | policy-backed team discovery plus semantic, keyboard-submittable onboarding forms |
| #776 | confirm/success/failure/retry/double-submit tests for destructive classes |
| #777 | contrast, keyboard, focus, table, landmark, live-region, motion and axe acceptance |
| #778 | typed, documented module/query/API boundaries with behavior preserved |
| #779 | responsive and visual regression matrix plus guided/shareable product workflows |
| #780 | restrictive tested headers, no required third-party font request, critical browser flows intact |
| #781 | warning-free bounded dependency upgrades on amd64/arm64 web builds |
| #212 | measured initial route chunks without suppressed build warnings |
| #692 | secret-source authenticated admin browser path without DB or raw secret handoff |
| #783 | startup, render, route and lazy-chunk failures recover without an empty root or secret leak |
| #28 | guided New Batch/provider/Monitor flow accepted through its own product criteria |
| #493/#715 | fixed-commit staging normal-user/admin evidence matrix |

## Failure Handling

- A failed check is classified and fixed; it is not skipped or converted to a
  warning to keep a pull request green.
- A live acceptance failure keeps the focused issue open and records the exact
  commit, route, browser state, response, and reproduction steps.
- A temporary mitigation requires a linked issue, bounded scope, rollback,
  removal criteria, and evidence needed to remove it.
- Raw credentials, bearer tokens, provider keys, session cookies, CSRF values,
  setup/reset links, or signed download URLs never enter issue comments,
  browser traces, screenshots, logs, or committed evidence.

## Completion Definition

#771 is a cross-milestone program assigned to its final v1.1 completion
horizon. Its v1.0 focused children remain independent release blockers tracked
by #715. The umbrella is complete only when both tranches are closed or an
explicit owner decision records why an item is no longer required, all
required CI checks pass at current `dev`, and a fixed-commit staging evidence
package proves the agreed desktop/mobile logged-out, normal-user, and
administrator journeys without operator-only shortcuts.
