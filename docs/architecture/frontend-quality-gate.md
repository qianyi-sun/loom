# Frontend quality gate

The required frontend gate is a repository-level prerequisite for any frontend
candidate. It is selected by `scripts/plan_ci_validations.py`, runs as
`web-checks`, and is enforced by `repository-checks`. A selected `web-checks`
result that is failed, cancelled, missing, or otherwise non-successful fails
the aggregate.

## Required layers

The gate uses a frozen `npm ci` install and requires:

1. strict TypeScript for production, unit-test, and Playwright sources without
   weakening `strict`, adding debt exclusions, or taking ownership of generated
   API declarations;
2. ESLint and Vitest with coverage floors of 80% statements, lines, and
   functions and 75% branches;
3. a successful production Vite build;
4. Chromium against that production build under a validated local `/dev` or
   `/prod` route prefix at 1440x900 and 390x844 for logged-out, user, and admin
   routes; and
5. zero serious or critical axe violations plus fail-closed page-error,
   console, unhandled-error, same-origin network, asset status, and MIME
   ledgers.

The Vite default is the relative-asset build. The Playwright
server reads one validated `BrowserHarnessConfig` from `LOOM_E2E_ORIGIN` and
`LOOM_E2E_ROUTE_PREFIX`; the origin must be credential-free local HTTP and the
prefix must be exactly `/dev` or `/prod`. The default is
`http://127.0.0.1:4173/dev`. `build-browser-test.mjs` supplies that prefix to
Vite and is the only command that compiles `IS_BROWSER_TEST_BUILD` as `true`.
Normal production builds compile the constant as `false`; URL state, runtime
configuration, HTTP responses, and endpoints cannot change it.

Playwright sets `reuseExistingServer: false` for local and CI execution. Every
browser evidence run must therefore invoke `build-browser-test.mjs` and start
its own prefix server; an occupied origin fails the run instead of accepting a
stale server, an ordinary production bundle, or an SPA fallback at the config
URL. This binds local recovery evidence to the browser-test bundle that carries
the compile-time marker.

`ApiHarness.install` installs deterministic local-only responses and returns an
`ApiFixture`. Scenario-neutral `ApiOverride` rules match an exact uppercase
method and route-relative path, derive the expected status from their response,
and default to cardinality one. They support delayed JSON, arbitrary typed text
(including deliberately invalid JSON with an explicit content type), HTTP
statuses, and network failure. `ApiFixture.ledger`, `expectRequest`, and fixture
teardown enforce exact method/path/status/cardinality. Exhausted overrides and
unknown API requests fail closed. These fixtures contain synthetic identities
only and must never receive local or live credentials.

`FailureSink.expectDiagnostic` is an exact, consumed diagnostic ledger for
browser-generated console and expected same-origin network events. An
unconsumed declaration fails teardown; recovery boundary errors must not be
allowlisted. Unexpected console and page errors retain only event kind,
route-relative location, and a bounded reference while message content is
redacted. All same-origin browser assets fail on non-success status, with
script and stylesheet MIME validation. `waitForReady` accepts either a stable
locator or a caller-provided asynchronous condition, so extensions can define
their own success marker without changing the generic harness.

## Ownership boundary

`config/component-ownership.toml` and
`scripts/component_ownership.py test-paths --lane frontend` are the authority
for component/test membership when that lane query is available. The workflow
feature-detects and consumes its output; before that command lands it runs the
complete Vitest suite. It does not maintain a second copy of the owned
TypeScript test globs. This document and the workflow own only the quality
policy, specialized browser harnesses, and aggregate behavior.

## Recovery extension contract

Recovery work may consume `BrowserHarnessConfig`, `ApiHarness.install`,
`ApiOverride`, `ApiFixture`, `RequestExpectation`, `FailureSink`,
`DiagnosticExpectation`, `waitForReady`, the production-build server, axe
integration, and the console/network/error guards, then add its own recovery
scenarios and specifications. Recovery UI and error-reporting behavior do not
belong in this foundation.

Any root-render fault seam used by recovery tests must be compiled only into an
explicit test build. A URL, runtime configuration value, live response, or
endpoint must never activate it. A lazily loaded fault fixture must explicitly
declare that a reload is required; switching the fixture must not imply that an
already loaded module changed in place.

## Candidate and broker acceptance

Passing repository CI is necessary but is not staging acceptance. A protected
rollout uses this gate only after the change has merged to `dev`, the
candidate has been fixed to that merged SHA, and the rollout coordinator has
authorized the broker-owned rollout. Candidate-bound browser evidence then
extends—not replaces—the repository gate. Local or Draft-PR work must never be
inserted into, used to re-resolve, or used to replace an already fixed rollout
candidate.
