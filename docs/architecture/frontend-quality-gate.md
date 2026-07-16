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
4. Chromium against that production build under the `/dev` route prefix at
   1440x900 and 390x844 for logged-out, user, and admin routes; and
5. zero serious or critical axe violations plus fail-closed page-error,
   console, unhandled-error, same-origin network, asset status, and MIME
   ledgers.

The Vite default remains the shipped relative-asset build. Only the local
browser test command supplies `VITE_E2E_ROUTE_BASE=/dev/`; this does not create
a runtime switch or a live endpoint. Browser API responses are deterministic,
local-only fixtures. Deliberate anonymous `401` and expired-link `404` console
messages are exempt only by exact message and bounded expected count.

## Ownership boundary

`config/component-ownership.toml` and
`scripts/component_ownership.py test-paths --lane frontend` are the authority
for component/test membership when that lane query is available. The workflow
feature-detects and consumes its output; before that command lands it runs the
complete Vitest suite. It does not maintain a second copy of the owned
TypeScript test globs. This document and the workflow own only the quality
policy, specialized browser harnesses, and aggregate behavior.

## Recovery extension contract

Recovery work may consume the production-build server, prefix router, browser
fixtures, axe integration, and console/network/error ledgers, then add its own
recovery scenarios and specifications. Recovery UI and error-reporting behavior
do not belong in this foundation.

Any root-render fault seam used by recovery tests must be compiled only into an
explicit test build. A URL, runtime configuration value, live response, or
endpoint must never activate it. A lazily loaded fault fixture must explicitly
declare that a reload is required; switching the fixture must not imply that an
already loaded module changed in place.

## Candidate and broker acceptance

Passing repository CI is necessary but is not staging acceptance. A later
rollout may use this gate only after the change has merged to `dev`, the
candidate has been fixed to that merged SHA, and the rollout coordinator has
authorized the broker-owned rollout. Candidate-bound browser evidence then
extends—not replaces—the repository gate. Local or Draft-PR work must never be
inserted into, used to re-resolve, or used to replace an already fixed rollout
candidate.
