# Frontend domain boundaries

The frontend keeps its existing routes, authentication, query cache and product
flows. The #777/#778/#779 consolidation changes the owning boundaries below;
it does not add a hosted Pipeline execution path or redesign New Batch.

## Modules and ownership

| Owner | Modules | Responsibility |
| --- | --- | --- |
| Service route owners | `src/loom_service/routes`, `wire_responses.py` | Runtime response validation and the OpenAPI wire contract. Existing Pydantic models remain authoritative. TypedDict responses describe previously untyped dictionaries, preserve extra fields and omit unset fields. |
| Frontend API maintainers | `web/src/api/core.ts` | Request transport, CSRF and unauthorized-handler state; auth session parsing retains its stricter classification and redirect policy in `auth.ts`. |
| Domain maintainers | `api/auth`, `runs`, `providers`, `catalog`, `admin`, `usage`, `library`, `overview`, `pipeline` | Domain requests and generated response aliases. `api/index.ts` composes the public API; there is no legacy `client.ts` wrapper. |
| Query owners | `api/queryKeys.ts`, domain hooks | Typed cache-root factories shared by reads and invalidation. Root-only keys intentionally invalidate suffixes; team and filter suffixes retain their existing isolation. |
| Catalog UI owners | `api/catalogViews.ts` | Normalize optional collections and legacy TaskSet diagnostics; generated wire declarations are never edited to match UI assumptions. |
| New Batch owners | `NewBatch`, `useNewBatch`, `newBatchState`, `NewBatchTaskSelection`, `NewBatchAdvancedSettings`, `NewBatchFields` | Route composition, query/mutation controller, pure state helpers, task selection and advanced form presentation. Existing `pages/newBatch` primitives remain in use. |
| Team access owners | `AdminAccess`, `useAdminAccess`, `adminAccessState`, `AdminTeams`, `AdminApiTokens`, `AdminLegacyRegistrations`, `AdminCliSetup` | Composition, mutations and section queries, pure labels, focused section presentation. Team/identity prerequisites remain shared; tokens, invites and registrations load only in their visible sections. |
| Monitor owners | `Monitor`, `MonitorBatches`, `MonitorTrials`, `MonitorHealth`, `MonitorResources`, `MonitorControls`, `monitorPresentation` | Routing/filter composition, independently owned views, reusable controls and pure summaries. |
| Model-picker owners | `AgentModelPicker`, `useAgentModelPicker`, `agentModelPickerState` | Presentation, catalog orchestration, pure selection helpers. |

Imports flow from route composition to domain hooks/components to API domains,
then transport and generated declarations. Pure helpers do not import pages or
perform requests. Presentation components can import controller **types**; they
must not call controller hooks or import page implementations. Shared transport
does not import domains. API domains may share types, not page-specific clients.

Reviewed containers are under 800 lines; shared picker/API modules are under
600. Generated `schema.d.ts` is exempt because its size follows the service
contract. These are review boundaries, not a reason to split cohesive helpers
or to compress formatting.

## Offline contract generation

From the repository root:

```sh
uv sync --locked --extra dev --python 3.11
cd web
npm ci
npm run gen-api
npm run gen-api -- --check
```

`scripts/export_openapi.py` uses the same router-registration function as the
service without creating settings, starting a lifespan, connecting to a DB or
reading credentials. It includes local/historical Pipeline contracts without
enabling those routes in hosted execution. The Node generator uses a temporary
directory, pinned lockfile dependencies and deterministic output, then removes
its temporary files. `PYTHON` can select a prepared interpreter.

The existing `web-checks` job verifies regeneration. Changes under
`src/loom_service/` or to the exporter select this job even without frontend
edits, so backend-only response drift cannot silently leave stale declarations.
This is part of `repository-checks`, not another required merge context.

Generated responses are the source of truth for the migrated wire domains.
Narrow UI projections, request drafts and the runtime-validated authentication
view remain outside generated output. Nullable artifact metadata and optional
TaskSet collections reflect actual service responses rather than the old stub.

## Benchmark discovery

Readiness already arrives in the bulk `GET /benchmarks` catalog response.
Recomputing it after each selection would repeat expensive task inspection.
`useBenchmarkDiscovery` therefore sends one debounced `POST /benchmarks/discover`
for the union of selected **tags** only. The server resolves/deduplicates
selectors, validates the selected catalog rows in bulk and unions their tags
with one query. Selecting one or 100 benchmarks has the same request count.
Missing selections fail explicitly; stale tags cannot authorize a filtered
submission while the new selection is pending or failed.

This supersedes #778's historical wording that combines tag and readiness
discovery in a new request. Readiness remains governed by the current catalog.

## Evidence and acceptance boundaries

Unit/page tests preserve submission, provider selection, hidden-section query
gating, cache invalidation, filter URL history, modal and tab contracts. Real
Postgres/API tests cover the changed response models and tag-union endpoint;
mock request-count tests additionally cover one and 100 selections.

The browser matrix runs the built SPA with synthetic local fixtures at 390×844,
768×1024, 1280×800 and 1440×900. It asserts document overflow, keyboard skip
focus, route context, table names, error/loading states and serious/critical axe
findings, and captures long-content screenshots. Layout assertions plus
reviewable screenshots avoid platform-dependent pixel baselines. `/dev` and
`/prod` runs use the same runtime basename contract.

Local evidence does not establish deployed staging acceptance or a complete
WCAG conformance claim. Safari/VoiceOver qualification and any candidate-bound
staging checklist remain explicit manual acceptance, with OS/browser/assistive
technology versions recorded by the person performing that check. Do not mark
those rows complete from Chromium, mocked identities or green CI.
