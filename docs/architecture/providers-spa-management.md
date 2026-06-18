# SPA provider-connection management

> **Status**: design — not yet shipped. Tracking issue: [#167].
> Closes the last gap from the #132 organic QA pass: the SPA can
> *select* provider connections but has no UI to create, test,
> rotate, or delete them. Users must drop to the CLI for every
> management operation.

## Problem

The SPA's `AgentModelPicker` (in `NewBatch`) lists existing provider connections and lets users submit `provider_connection_id` with a batch. But the SPA exposes only two provider endpoints from `web/src/api/client.ts`:

- `listProviderConnections()`
- `createProviderConnectionModel()` (manual model add)

The backend at `src/loom_service/routes/provider_connections.py` exposes **eleven** endpoints — full CRUD, test, model management, key rotation. A new BYO-provider user who lands on `NewBatch` with no connection has no path forward in the browser; they must shell out to `loom providers create` and friends, then come back.

This makes the browser product incomplete for v0.2/v0.3 provider onboarding.

## Goal

Ship a SPA management surface that matches the CLI's provider-connection capabilities end-to-end, so the browser workflow is self-sufficient.

## Backend API (already shipped)

All under `/api/v1/provider-connections` (hyphen, not underscore):

| Method | Path | Purpose |
|---|---|---|
| POST | `/provider-connections` | Create |
| GET | `/provider-connections` | List (team-scoped) |
| GET | `/provider-connections/{id}` | Detail |
| PATCH | `/provider-connections/{id}` | Update — used for both edit (allowed_models, pricing) AND rotate (`api_key` field) |
| DELETE | `/provider-connections/{id}` | Soft-delete (sets `deleted_at`, status=`disabled`) |
| POST | `/provider-connections/{id}/test` | Probe upstream; returns `{status: 'valid'\|'invalid', last_validated_at, last_validation_error}`. HTTP 200 even when the probe fails |
| GET | `/provider-connections/{id}/models` | List models |
| POST | `/provider-connections/{id}/models` | Add a manual model |
| POST | `/provider-connections/{id}/models/refresh` | Re-fetch from upstream |
| POST | `/provider-connections/{id}/models/{model_id:path}/hide` | Hide from picker |
| POST | `/provider-connections/{id}/models/{model_id:path}/unhide` | Reverse |

No dedicated rotate-key endpoint — rotation is `PATCH {api_key: "..."}`.

## SPA architecture

### Routes

| Route | Page | Purpose |
|---|---|---|
| `/providers` | `ProvidersList` | Table + "+ New connection" button. Empty-state CTA when none exist. |
| `/providers/new` | `ProviderCreate` | Form: name, type, base_url, api_key, allowed_models. On success, redirect to `?returnTo` if set, else `/providers/:id`. |
| `/providers/:id` | `ProviderDetail` | Three tabs (component-local state, not URL): **Overview** / **Models** / **Settings**. |

### Nav

Add `Providers` as a third team-scoped top-level nav item in `NavBar.tsx`:

```
New batch
Monitor
Providers    ← new
```

`Providers` is team-scoped, not admin-only — no scope gating.

### Detail tabs

**Overview** — read-only display:
- name, type, base_url
- allowed_models count
- last test status pill (green `valid` / red `invalid` / grey `untested`)
- created_at, updated_at
- **Test** button. Result inline:
  - `valid` → green pill, "Tested {timestamp}"
  - `invalid` → red pill + collapsible "Details" disclosure showing `last_validation_error`
  - Empty `last_validation_error` → fallback "Test failed; no error details reported by the provider"

**Models** — rich table:
- columns: model id, source (upstream/manual), hidden state (with tooltip: "Hidden models don't appear in New Batch's picker")
- action buttons: Refresh from upstream, Add manual model (modal)
- per-row action: Hide / Unhide
- backend returns every cached model row in one shot (no server-side pagination today; see `list_models` in `provider_connections.py`). v1 fetches all + a client-side filter input. Real providers cache 30-200 models post-refresh; >500 rows would slow the table — defer server-side pagination unless that materializes.

**Settings** — two sections:
- **Edit** (top): `allowed_models` form using shared `ProviderForm` in `mode='edit'`. `api_key` field is **not rendered** in edit mode — rotation has its own flow.
- **Danger zone** (bottom, red-bordered):
  - **Rotate API key** → opens `RotateKeyModal`. Paste new key + type connection name to confirm. Never pre-filled. Warning: "Replaces the stored API key. Calls in flight may complete with either the old or new key depending on timing. New trials use the new key."
  - **Delete connection** → opens `DeleteConnectionModal`. Type connection name to confirm. Warning: "Soft-delete. Existing trial records keep their `provider_connection_id` attribution for billing/audit. The connection becomes hidden from your team and cannot be used for new trials, but in-flight calls complete normally. The encrypted key is retained until Phase 5 cleanup."

### Integration with NewBatch

When `AgentModelPicker` finds no connections, render an empty-state CTA:

```tsx
<EmptyState>
  <p>No provider connections yet.</p>
  <Link to="/providers/new?returnTo=/batches/new">
    Create a provider
  </Link>
</EmptyState>
```

After create, the user returns to `/batches/new` via `returnTo`; the picker auto-refreshes via React Query invalidation.

## Components

```
pages/ProvidersList.tsx                  # ~80 LOC
pages/ProviderCreate.tsx                 # ~120 LOC
pages/ProviderDetail.tsx                 # ~280 LOC — tab shell + Overview tab + Settings tab inline
components/providers/ProviderForm.tsx    # ~150 LOC — shared Create + Edit; api_key only in Create
components/providers/ModelsTab.tsx       # ~180 LOC — largest file; rich table + 3 actions + modal
components/providers/RotateKeyModal.tsx  # ~80 LOC
components/providers/DeleteConnectionModal.tsx # ~70 LOC
components/providers/AddManualModelModal.tsx   # ~90 LOC
hooks/providers.ts                       # ~250 LOC — all mutation hooks (test, rotate, delete, edit,
                                         #   refreshModels, addModel, hideModel, unhideModel)
                                         #   with their invalidation logic; read-side uses inline useQuery
api/client.ts                            # MODIFIED: add 9 endpoints + rename existing
```

OverviewTab and SettingsTab are inline in `ProviderDetail.tsx` (read-mostly, ~80 LOC each, no reuse). `ModelsTab` gets its own file because it's rich + has dedicated test concerns.

## API client extensions

Add to `web/src/api/client.ts` (all typed against `paths` from `schema.d.ts`):

```ts
api.getProviderConnection(id)
api.createProviderConnection(payload)
api.updateProviderConnection(id, patch)            // covers Edit + Rotate
api.deleteProviderConnection(id)
api.testProviderConnection(id)
api.listProviderConnectionModels(id)
api.addProviderConnectionModel(id, model)          // renamed from createProviderConnectionModel
api.refreshProviderConnectionModels(id)
api.hideProviderConnectionModel(id, modelId)
api.unhideProviderConnectionModel(id, modelId)
```

`listProviderConnections()` already exists; reused.

### Hooks layer (`hooks/providers.ts`)

Mutation hooks wrap `api.updateProviderConnection(id, patch)` with semantic names:

- `useEditConnection(id)` — PATCH with allowed_models / pricing fields
- `useRotateConnectionKey(id)` — PATCH with `{api_key}`. Same endpoint, different UI intent.

Other mutation hooks (`useDeleteConnection`, `useTestConnection`, `useRefreshModels`, `useHideModel`, `useUnhideModel`, `useAddManualModel`) wrap their single endpoint each.

**Read-side** uses inline `useQuery({ queryKey: [...], queryFn: () => api.X() })` in each component — matches existing `Settings.tsx` pattern. No custom read-hook wrappers.

### React Query keys + targeted invalidation

| Key | Used by | Invalidated by |
|---|---|---|
| `["providers"]` | `ProvidersList` | create, update, delete, test |
| `["providers", id]` | `ProviderDetail` (Overview, Settings) | update, delete, test |
| `["providers", id, "models"]` | `ProviderDetail` (Models tab) | addModel, refreshModels, hideModel, unhideModel |

Mutations use scoped `invalidateQueries({ queryKey: ... })`. Delete also calls `removeQueries({ queryKey: ["providers", id] })` to nuke the dead detail.

All mutations are **pessimistic** (await server response). Provider state has real consequences — no value in optimistic UI for safety-critical actions.

## Error handling

- **Create form validation**: backend returns either `{detail: "string"}` or `{detail: [{loc, msg, type}, ...]}`; `apiFetch` stringifies both. For v1, render as a single banner above the form. Field-level inline errors deferred.
- **Test result on HTTP 200**: read `result.status` field, not HTTP code. Show red pill + details disclosure on `invalid`.
- **Rotate / Delete**: confirm-by-typing-connection-name in modal. Submit button disabled until match. Existing key never pre-filled or echoed.
- **404 on cross-team access**: backend returns 404 (not 403) to avoid leaking existence. UI shows "Connection not found" empty state.
- **401**: existing `apiFetch` handler clears token + bounces to sign-in (improved in #175).
- **5xx / network**: error toast at top of page; mutation leaves form inputs intact for retry.

## Testing

| Test file | Tests |
|---|---|
| `ProvidersList.test.tsx` | empty state CTA, populated table, row links to detail, "+ New" link |
| `ProviderCreate.test.tsx` | happy path → redirect, with `returnTo` → redirect there, 400 → inline banner, submit disabled while pending |
| `ProviderDetail.test.tsx` | Overview default, tab nav, 404 state, Test success path, Test invalid path (red pill + details) |
| `ModelsTab.test.tsx` | refresh, add manual, hide, unhide, modal cancel |
| `SettingsTab` (in ProviderDetail tests) | edit allowed_models PATCH, `api_key` NOT visible in edit mode, rotate flow, delete flow |
| `RotateKeyModal.test.tsx` | submit disabled until name matches, submit calls PATCH `{api_key}`, close cancels |
| `DeleteConnectionModal.test.tsx` | same shape, submit calls DELETE, success redirects |
| `api-client.test.ts` (MODIFIED) | round-trip mocks for each of the 9 new methods + the renamed one |

**Total ~30 new tests.** Full SPA suite goes from 111 → ~140 passing.

## Out of scope for v1

- URL-deep-linkable tab state (component state only; revisit if shareable URLs become needed).
- Bulk operations ("delete all unused connections").
- Visual provider connection diagram or test history graph.
- Server-side pagination on the Models table (v1: fetch all, client-side filter; backend doesn't paginate today).
- Field-level inline validation errors (v1: single banner per form).
- Mobile / narrow-viewport layouts (Loom SPA is desktop-only).

## Edge cases handled at implementation time

- **Existing `createProviderConnectionModel` call site**: one in the current codebase (in `AgentModelPicker` per the issue). Rename atomically in the same PR.
- **Models fetch is unbounded**: backend returns all cached rows. SPA renders the full list; client-side filter input narrows the view. Real fix (server-side pagination) deferred until a provider with >500 cached models surfaces operationally.
- **`ProviderForm` shared between Create + Edit**: `mode: 'create' | 'edit'` prop. `api_key` field renders only in `create` mode. Validation rules match the backend's per-field requirements.
- **NewBatch picker refresh**: relies on React Query's window-focus refetch + the mutation invalidation on the providers create page. No special wiring needed in NewBatch.

## Related

- Tracking issue: [#167].
- Closes the last gap from organic [#132] QA pass.
- Backend API already shipped in Phase 2 (PRs #51–64 per project memory).
- Bug fixes from same QA pass that landed this week: #158, #159, #160, #161, #163, #164, #165, #166.

[#167]: https://github.com/carinrc/loom/issues/167
[#132]: https://github.com/carinrc/loom/issues/132
