# Cursor Completeness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make every authorized Run Library batch and administrator audit event reachable through deterministic Next/Prev traversal, with cursor history reset synchronously whenever Run Library scope or filters change.

**Architecture:** Keep the Service API's keyset order as `created_at DESC, id DESC` and use the existing opaque base64 JSON `Cursor` for both endpoints. Run Library artifact filters will fill a page by scanning ordered, authorization-filtered candidate windows until `limit + 1` matching rows are found, so the API can return a truthful `next_cursor` without a page-truncation escape hatch. In the SPA, a shared `useCursorPage(resetKey)` hook owns session-local cursor history while URL parameters remain the shareable filter source of truth; Run Library and an extracted `AdminAuditLog` consume the hook and the existing `Pagination` component.

**Tech Stack:** Python 3.11, FastAPI, SQLAlchemy 2, PostgreSQL, pytest/httpx, TypeScript 5.5, React 18, TanStack Query 5, React Router 6, Vitest 4.1.8, Testing Library, and the Playwright Chromium harness delivered by #773.

## Global Constraints

- Scope is #774 only. Do not change unrelated Monitor, Tasks, or Benchmarks behavior while reusing their cursor-state and `Pagination` foundations.
- Preserve backend order exactly as `created_at DESC, id DESC`. A cursor predicate is exactly `created_at < cursor.timestamp OR (created_at = cursor.timestamp AND id < cursor.id)`.
- Keep cursors opaque and session-local. Do not put a cursor in Run Library URL search parameters, browser storage, or persisted React Query state.
- Keep Run Library scope and every server-side filter URL-backed: `scope`, `team_id`, `state`, `artifact_type`, `q`, `benchmark_id`, `agent_name`, `model_provider`, `model_name`, `provider_connection_id`, and `provider_model_id`.
- Changing any scope or filter value must expose `cursor = null` during that same render. An effect-only reset that first requests a new filter with the old cursor is not acceptable.
- The API applies authorization and ordinary batch filters before cursor traversal. A cursor never expands the caller's team or org-shared visibility.
- Artifact-filtered pages must be complete. Scan ordered candidates until `limit + 1` matching batches are found or the authorized result set is exhausted; never stop merely because one candidate window contains fewer than `limit` matches.
- Because the artifact-filtered API can guarantee a complete page, do not add a page-level `truncated` flag. Existing per-row `artifact_summary_truncated` and `artifact_inventory_truncated` fields remain separate preview-size signals.
- API regression data is exactly 53 authorized rows: 27 rows share one timestamp and 26 have distinct older timestamps. With `limit=17`, traversal must return page sizes `[17, 17, 17, 2]`, no duplicate ids, no missing ids, and `next_cursor = null` on page four.
- Run Library and Admin audit use explicit Prev/Next controls. While a page is loading, both controls are disabled and a polite status reports the loading page. On an error, Prev remains available when history exists. At the terminal page, Next is disabled and the visible status says the end of results was reached.
- Keep response shapes unchanged: `{ "items": [...], "next_cursor": string | null }`. No generated API schema update is required for #774.
- Backend work may start independently. Browser tests in `web/e2e/**` start only after #773's guarded Playwright fixture and deterministic API router are merged; rebase onto that exact `dev` baseline instead of creating a second browser harness.
- Playwright fixture data is deterministic and credential-free. Live staging acceptance uses the #692 authenticated administrator-session path and normal product APIs only; never use direct database reads, hidden maintenance endpoints, exported cookies, or raw bearer values.
- Raw credentials, session cookies, CSRF values, setup/reset/invite secrets, provider keys, signed URLs, and browser storage state must not appear in tests, traces, screenshots, evidence, commits, or issue comments.

---

## Current Failure and Contract Map

- `GET /api/v1/run-library/batches` already orders by `Batch.created_at.desc(), Batch.id.desc()` and emits `next_cursor`, but `web/src/pages/RunLibrary.tsx` neither sends a cursor nor renders pagination controls.
- The ordinary Run Library path applies `limit + 1` in SQL. The artifact-filtered path scans and serializes every authorized candidate before slicing, which is complete but unbounded. #774 keeps completeness while stopping as soon as one extra matching row proves a next page exists.
- `GET /api/v1/admin/audit-events` orders correctly, but its cursor is only the prior row UUID and requires a lookup. It will use the same timestamp/id cursor as Run Library, Batches, and Trials.
- `web/src/pages/AdminAccess.tsx` fetches `listAdminAuditEvents(50)` once at parent mount and renders an embedded `AuditRows`; no later audit event can be reached.
- `web/src/components/paginationState.ts` correctly implements forward history and stack pop, but pages manually own reset timing. `useCursorPage(resetKey)` will be the single reset boundary for #774.

## File Structure

- `src/loom_service/routes/run_library.py`: factor the timestamp/id predicate and bounded artifact-filtered page collector; serialize only the returned page.
- `tests/integration/test_service_run_library.py`: seed deterministic 53-row tied/non-tied data and prove ordinary, artifact-filtered, authorization-safe, terminal cursor traversal.
- `src/loom_service/routes/admin_audit.py`: replace UUID row-lookup cursors with the shared encoded timestamp/id cursor.
- `tests/integration/test_service_admin_audit.py`: prove encoded cursors, tied-row traversal, terminal null, invalid-cursor handling, and admin authorization.
- `web/src/hooks/useCursorPage.ts`: session-local cursor controller keyed by all API-affecting scope/filter values.
- `web/src/__tests__/hooks/useCursorPage.test.tsx`: forward/back history and synchronous reset contract.
- `web/src/components/Pagination.tsx`: accessible page/loading/error/terminal status and loading-safe button states.
- `web/src/__tests__/components/Pagination.test.tsx`: control labels, disabled states, and polite status text.
- `web/src/pages/RunLibrary.tsx`: add the cursor to query identity/request parameters and render shared pagination.
- `web/src/__tests__/pages/RunLibrary.test.tsx`: Next/Prev, terminal, loading, URL-backed filters, and no-stale-cursor reset.
- `web/src/components/admin/AdminAuditLog.tsx`: own the audit query, table, cursor history, and states outside the large Admin Access page.
- `web/src/__tests__/components/admin/AdminAuditLog.test.tsx`: audit Next/Prev/loading/error/empty/terminal behavior.
- `web/src/pages/AdminAccess.tsx`: render `AdminAuditLog` only in the administrator Audit tab.
- `web/src/__tests__/pages/AdminAccess.test.tsx`: prove audit data is lazy-loaded only after the Audit tab mounts.
- `web/e2e/fixtures/api.ts`: after #773, add deterministic two-page Run Library and Admin audit responses.
- `web/e2e/user.spec.ts`: after #773, traverse Run Library forward/back in Chromium.
- `web/e2e/admin.spec.ts`: after #773, traverse Admin audit forward/back in Chromium.
- `docs/architecture/run-library.md`: document stable order, artifact-filtered completeness, and cursor semantics.
- `docs/architecture/service-mode.md`: document the Admin audit keyset contract.
- `docs/user-guide.md`: explain URL-backed filters, session-local history, and visible page states.

## Interface Map

`src/loom_service/pagination.py` remains source-compatible:

```python
@dataclass(frozen=True)
class Cursor:
    submitted_at: datetime  # generic encoded sort timestamp; created_at for #774 endpoints
    id: UUID

def encode_cursor(c: Cursor) -> str: ...
def decode_cursor(s: str) -> Cursor: ...
```

`run_library.py` adds two private interfaces:

```python
def _batch_after_cursor(cursor: Cursor) -> Any: ...

async def _artifact_filtered_batch_rows(
    session: Any,
    stmt: Any,
    artifact_filters: dict[str, Any],
    *,
    limit: int,
) -> list[tuple[Batch, Team]]: ...
```

The collector receives a statement that already contains scope, authorization, structured filters, visibility, ordering, and any client cursor. It returns at most `limit + 1` artifact-matching rows in that same order.

The frontend hook contract is:

```ts
export interface CursorPageController {
  state: PageState;
  cursor: string | null;
  next: (cursor: string) => void;
  prev: () => void;
  reset: () => void;
}

export function useCursorPage(resetKey: string): CursorPageController;
```

`Pagination` adds optional status inputs without breaking existing callers:

```ts
export interface PaginationProps {
  state: PageState;
  hasNext: boolean;
  isLoading?: boolean;
  isError?: boolean;
  onNext: () => void;
  onPrev: () => void;
}
```

---

### Task 1: Prove and bound Run Library keyset traversal

**Files:**
- Modify: `tests/integration/test_service_run_library.py`
- Modify: `src/loom_service/routes/run_library.py`

**Interfaces:**
- Consumes: `Cursor`, `encode_cursor`, `decode_cursor`, `_apply_read_filter()`, `apply_batch_monitor_filters()`, and `_batch_has_matching_artifact()`.
- Produces: `_batch_after_cursor(cursor)` and `_artifact_filtered_batch_rows(session, stmt, artifact_filters, limit=...)` returning no more than one extra matching row.

- [ ] **Step 1: Add the exact 53-row fixture and traversal helper**

Add `timedelta` to the datetime import, then add these helpers after `run_library_setup`:

```python
from datetime import UTC, datetime, timedelta


def _seed_cursor_batches(
    *,
    postgres_url: str,
    team_id: UUID,
    task_id: str,
    provider_connection_id: UUID,
) -> tuple[list[str], str]:
    tied_at = datetime(2026, 7, 10, 12, 0, tzinfo=UTC)
    visible: list[tuple[UUID, datetime]] = []
    batch_rows: list[dict[str, object]] = []
    trial_rows: list[dict[str, object]] = []
    artifact_rows: list[dict[str, object]] = []

    for index in range(53):
        batch_id = UUID(int=10_000 + index)
        trial_id = UUID(int=20_000 + index)
        artifact_id = UUID(int=30_000 + index)
        created_at = (
            tied_at
            if index < 27
            else tied_at - timedelta(minutes=index - 26)
        )
        visible.append((batch_id, created_at))
        batch_rows.append(
            {
                "id": batch_id,
                "team_id": team_id,
                "name": f"cursor completeness {index:02d}",
                "description": "issue 774 deterministic traversal fixture",
                "task_filter": {
                    "subset_kind": "explicit",
                    "task_ids": [task_id],
                },
                "trial_config": {},
                "state": "finished",
                "result_status": "succeeded",
                "created_at": created_at,
                "finished_at": created_at,
                "created_by_token_prefix": "test:774",
                "expected_trial_count": 1,
                "n_per_task": 1,
                "backend": "docker",
                "combinations": [],
                "provider_connection_id": provider_connection_id,
                "provider_model_id": "gpt-4o-mini",
                "visibility": "org",
                "share_status": "shared",
            }
        )
        trial_rows.append(
            {
                "id": trial_id,
                "team_id": team_id,
                "batch_id": batch_id,
                "task_id": task_id,
                "config": {},
                "requires_caps": {},
                "state": "succeeded",
                "submitted_at": created_at,
                "started_at": created_at,
                "finished_at": created_at,
                "result": {"aggregate_reward": 1.0},
                "visibility": "org",
                "share_status": "shared",
            }
        )
        artifact_rows.append(
            {
                "id": artifact_id,
                "artifact_type": "training_data_export",
                "artifact_schema_version": "1.0",
                "name": f"cursor export {index:02d}",
                "team_id": team_id,
                "batch_id": batch_id,
                "trial_id": trial_id,
                "created_by": {
                    "kind": "trial",
                    "batch_id": str(batch_id),
                    "trial_id": str(trial_id),
                },
                "content_hash": f"sha256:{index:064x}",
                "storage": {
                    "backend": "object_store",
                    "bucket": "artifacts",
                    "key": f"cursor-774/{trial_id}/export.jsonl",
                    "media_type": "application/x-ndjson",
                    "size_bytes": 1,
                },
                "visibility": "org",
                "share_status": "shared",
                "redaction_state": "redacted",
                "safety_state": "safe",
                "retention": {"class": "shared_reusable"},
                "provenance": {
                    "batch_id": str(batch_id),
                    "trial_id": str(trial_id),
                    "source_trial_ids": [str(trial_id)],
                    "relation": "produced_from",
                },
                "artifact_metadata": {"fixture_index": index},
                "created_at": created_at,
            }
        )

    private_batch_id = UUID(int=90_000)
    private_trial_id = UUID(int=90_001)
    private_artifact_id = UUID(int=90_002)
    batch_rows.append(
        {
            "id": private_batch_id,
            "team_id": team_id,
            "name": "cursor completeness private",
            "description": "must remain outside cross-team traversal",
            "task_filter": {"subset_kind": "explicit", "task_ids": [task_id]},
            "trial_config": {},
            "state": "finished",
            "result_status": "succeeded",
            "created_at": tied_at,
            "finished_at": tied_at,
            "created_by_token_prefix": "test:774",
            "expected_trial_count": 1,
            "n_per_task": 1,
            "backend": "docker",
            "combinations": [],
            "provider_connection_id": provider_connection_id,
            "provider_model_id": "gpt-4o-mini",
            "visibility": "private",
            "share_status": "shared",
        }
    )
    trial_rows.append(
        {
            "id": private_trial_id,
            "team_id": team_id,
            "batch_id": private_batch_id,
            "task_id": task_id,
            "config": {},
            "requires_caps": {},
            "state": "succeeded",
            "submitted_at": tied_at,
            "started_at": tied_at,
            "finished_at": tied_at,
            "result": {"aggregate_reward": 1.0},
            "visibility": "private",
            "share_status": "shared",
        }
    )
    artifact_rows.append(
        {
            "id": private_artifact_id,
            "artifact_type": "training_data_export",
            "artifact_schema_version": "1.0",
            "name": "private cursor export",
            "team_id": team_id,
            "batch_id": private_batch_id,
            "trial_id": private_trial_id,
            "created_by": {"kind": "trial", "trial_id": str(private_trial_id)},
            "content_hash": "sha256:" + ("f" * 64),
            "storage": {
                "backend": "object_store",
                "bucket": "artifacts",
                "key": f"cursor-774/{private_trial_id}/export.jsonl",
                "media_type": "application/x-ndjson",
                "size_bytes": 1,
            },
            "visibility": "private",
            "share_status": "shared",
            "redaction_state": "redacted",
            "safety_state": "safe",
            "retention": {"class": "owner_only"},
            "provenance": {
                "batch_id": str(private_batch_id),
                "trial_id": str(private_trial_id),
                "relation": "produced_from",
            },
            "artifact_metadata": {},
            "created_at": tied_at,
        }
    )

    sync_engine = create_engine(postgres_url)
    with sync_engine.begin() as conn:
        conn.execute(insert(Batch), batch_rows)
        conn.execute(insert(Trial), trial_rows)
        conn.execute(insert(Artifact), artifact_rows)
    sync_engine.dispose()

    expected_ids = [
        str(batch_id)
        for batch_id, _created_at in sorted(
            visible,
            key=lambda item: (item[1], item[0].int),
            reverse=True,
        )
    ]
    return expected_ids, str(private_batch_id)


async def _walk_run_library_pages(
    client: httpx.AsyncClient,
    *,
    headers: dict[str, str],
    params: dict[str, str],
) -> tuple[list[str], list[int], list[str | None]]:
    cursor: str | None = None
    seen_cursors: set[str] = set()
    ids: list[str] = []
    page_sizes: list[int] = []
    returned_cursors: list[str | None] = []

    while True:
        request_params = dict(params)
        if cursor is not None:
            request_params["cursor"] = cursor
        response = await client.get(
            "/api/v1/run-library/batches",
            params=request_params,
            headers=headers,
        )
        assert response.status_code == 200, response.text
        body = response.json()
        page_ids = [item["id"] for item in body["items"]]
        ids.extend(page_ids)
        page_sizes.append(len(page_ids))
        returned_cursors.append(body["next_cursor"])
        cursor = body["next_cursor"]
        if cursor is None:
            break
        assert cursor not in seen_cursors
        seen_cursors.add(cursor)
        assert len(returned_cursors) < 10

    return ids, page_sizes, returned_cursors
```

- [ ] **Step 2: Add ordinary and artifact-filtered completeness tests**

Add these tests immediately after the existing default-scope test:

```python
async def test_run_library_cursor_walk_has_no_gaps_or_duplicates(
    run_library_setup: dict[str, object],
) -> None:
    expected_ids, private_id = _seed_cursor_batches(
        postgres_url=str(run_library_setup["postgres_url"]),
        team_id=run_library_setup["team_a"],
        task_id=str(run_library_setup["task_id"]),
        provider_connection_id=run_library_setup["conn_a"],
    )
    transport = httpx.ASGITransport(app=run_library_setup["app"])
    async with httpx.AsyncClient(transport=transport, base_url="http://svc") as client:
        ids, page_sizes, cursors = await _walk_run_library_pages(
            client,
            headers={"Authorization": f"Bearer {run_library_setup['raw_b']}"},
            params={
                "scope": "all",
                "q": "cursor completeness",
                "limit": "17",
            },
        )

    assert page_sizes == [17, 17, 17, 2]
    assert cursors[-1] is None
    assert ids == expected_ids
    assert len(ids) == 53
    assert len(set(ids)) == 53
    assert private_id not in ids


async def test_artifact_filtered_cursor_walk_is_complete_and_bounded(
    run_library_setup: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected_ids, private_id = _seed_cursor_batches(
        postgres_url=str(run_library_setup["postgres_url"]),
        team_id=run_library_setup["team_a"],
        task_id=str(run_library_setup["task_id"]),
        provider_connection_id=run_library_setup["conn_a"],
    )
    original_matcher = run_library_routes._batch_has_matching_artifact
    checked_batch_ids: list[UUID] = []

    async def counted_matcher(
        session: object,
        trials: list[Trial],
        filters: dict[str, object],
    ) -> bool:
        if trials and trials[0].batch_id is not None:
            checked_batch_ids.append(trials[0].batch_id)
        return await original_matcher(session, trials, filters)

    monkeypatch.setattr(
        run_library_routes,
        "_batch_has_matching_artifact",
        counted_matcher,
    )
    transport = httpx.ASGITransport(app=run_library_setup["app"])
    headers = {"Authorization": f"Bearer {run_library_setup['raw_b']}"}
    params = {
        "scope": "all",
        "q": "cursor completeness",
        "artifact_type": "training_data_export",
        "limit": "17",
    }
    async with httpx.AsyncClient(transport=transport, base_url="http://svc") as client:
        first = await client.get(
            "/api/v1/run-library/batches",
            params=params,
            headers=headers,
        )
        assert first.status_code == 200, first.text
        assert len(first.json()["items"]) == 17
        assert first.json()["next_cursor"] is not None
        assert len(checked_batch_ids) == 18

        checked_batch_ids.clear()
        ids, page_sizes, cursors = await _walk_run_library_pages(
            client,
            headers=headers,
            params=params,
        )

    assert page_sizes == [17, 17, 17, 2]
    assert cursors[-1] is None
    assert ids == expected_ids
    assert len(set(ids)) == 53
    assert private_id not in ids
```

- [ ] **Step 3: Run the focused backend tests and record the honest baseline**

Run:

```bash
uv run pytest tests/integration/test_service_run_library.py \
  -k 'cursor_walk_has_no_gaps_or_duplicates or artifact_filtered_cursor_walk_is_complete_and_bounded' -q
```

Expected before implementation: the ordinary characterization test passes because the existing non-artifact path already has a correct keyset. The artifact-filtered test fails at `assert len(checked_batch_ids) == 18` with `53 == 18`, proving that the endpoint scans the complete filtered set instead of stopping after one extra match.

- [ ] **Step 4: Add the shared batch predicate and bounded matching collector**

Add this code directly above `list_run_library_batches`:

```python
def _batch_after_cursor(cursor: Cursor) -> Any:
    return or_(
        Batch.created_at < cursor.submitted_at,
        and_(
            Batch.created_at == cursor.submitted_at,
            Batch.id < cursor.id,
        ),
    )


async def _artifact_filtered_batch_rows(
    session: Any,
    stmt: Any,
    artifact_filters: dict[str, Any],
    *,
    limit: int,
) -> list[tuple[Batch, Team]]:
    selected: list[tuple[Batch, Team]] = []
    scan_limit = max(limit + 1, 50)
    page_stmt = stmt

    while len(selected) <= limit:
        candidate_rows = [
            (batch, team)
            for batch, team in (
                await session.execute(page_stmt.limit(scan_limit))
            ).all()
        ]
        if not candidate_rows:
            break

        for batch, team in candidate_rows:
            trials = await _batch_trials(session, batch.id)
            if await _batch_has_matching_artifact(
                session,
                trials,
                artifact_filters,
            ):
                selected.append((batch, team))
                if len(selected) > limit:
                    break

        if len(selected) > limit or len(candidate_rows) < scan_limit:
            break

        last_batch = candidate_rows[-1][0]
        page_stmt = page_stmt.where(
            _batch_after_cursor(
                Cursor(submitted_at=last_batch.created_at, id=last_batch.id),
            )
        )

    return selected
```

This loop deliberately continues across a full candidate window with too few artifact matches. Its only terminal conditions are `limit + 1` matches, no remaining candidates, or a short final candidate window.

- [ ] **Step 5: Make `list_run_library_batches` page rows before serialization**

Replace the current cursor predicate and the block from `artifact_filtering = ...` through the return with:

```python
    if cursor:
        try:
            cur = decode_cursor(cursor)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        stmt = stmt.where(_batch_after_cursor(cur))

    artifact_filters = {
        "artifact_type": artifact_type,
        "owner_team_id": owner_team_id,
        "source_batch_id": source_batch_id,
        "source_trial_id": source_trial_id,
        "safety_state": safety_state,
        "provenance_relation": provenance_relation,
    }
    artifact_filtering = _artifact_filter_active(artifact_filters)
    if artifact_filtering:
        rows = await _artifact_filtered_batch_rows(
            session,
            stmt,
            artifact_filters,
            limit=limit,
        )
    else:
        rows = [
            (batch, team)
            for batch, team in (
                await session.execute(stmt.limit(limit + 1))
            ).all()
        ]

    has_more = len(rows) > limit
    page_rows = rows[:limit]
    serialized: list[dict[str, Any]] = []
    if artifact_filtering:
        for batch, team in page_rows:
            serialized.append(
                await _serialize_batch(request, session, ctx, batch, team)
            )
    else:
        batch_ids = [batch.id for batch, _team in page_rows]
        trial_rollups = await _batch_list_trial_rollups(session, batch_ids)
        artifact_summaries, truncated_artifact_summaries = (
            await _batch_list_artifact_summaries(session, batch_ids)
        )
        for batch, team in page_rows:
            serialized.append(
                _serialize_batch_list_item(
                    batch,
                    team,
                    trial_rollups.get(
                        batch.id,
                        (_empty_trial_summary(), None, 0.0),
                    ),
                    artifact_summaries.get(batch.id, _empty_artifact_summary()),
                    batch.id in truncated_artifact_summaries,
                )
            )

    next_cursor: str | None = None
    if has_more and page_rows:
        last_batch = page_rows[-1][0]
        next_cursor = encode_cursor(
            Cursor(submitted_at=last_batch.created_at, id=last_batch.id),
        )
    return {"items": serialized, "next_cursor": next_cursor}
```

Remove the earlier duplicate `if cursor:` block so the route applies the cursor exactly once.

- [ ] **Step 6: Run Run Library integration coverage GREEN**

Run:

```bash
uv run pytest tests/integration/test_service_run_library.py -q
```

Expected: all Run Library integration tests pass; both 53-row traversals return `[17, 17, 17, 2]`; the artifact first page checks exactly 18 matching candidates; private cross-team data never appears; the terminal cursor is null.

- [ ] **Step 7: Commit the Run Library backend slice**

```bash
git add src/loom_service/routes/run_library.py \
  tests/integration/test_service_run_library.py
git commit -m "fix(api): bound complete Run Library cursor pages (#774)"
```

### Task 2: Give Admin audit the same timestamp/id cursor contract

**Files:**
- Modify: `tests/integration/test_service_admin_audit.py`
- Modify: `src/loom_service/routes/admin_audit.py`

**Interfaces:**
- Consumes: `Cursor`, `encode_cursor()`, and `decode_cursor()` from `loom_service.pagination`.
- Produces: `GET /api/v1/admin/audit-events?limit=N&cursor=OPAQUE` ordered by `AdminAuditEvent.created_at DESC, AdminAuditEvent.id DESC` without a cursor-row lookup.

- [ ] **Step 1: Replace the three-row cursor test with exact 53-row traversal**

Add the imports and replace `test_admin_audit_endpoint_pages_with_cursor` with:

```python
from datetime import UTC, datetime, timedelta

from sqlalchemy import create_engine, insert, text

from loom.db.schema import AdminAuditEvent
from loom_service.pagination import decode_cursor


async def test_admin_audit_endpoint_pages_with_encoded_tie_breaker_cursor(
    audit_app: FastAPI,
    postgres_url: str,
) -> None:
    tied_at = datetime(2026, 7, 10, 12, 0, tzinfo=UTC)
    rows: list[dict[str, object]] = []
    expected: list[tuple[UUID, datetime]] = []
    for index in range(53):
        event_id = UUID(int=50_000 + index)
        created_at = (
            tied_at
            if index < 27
            else tied_at - timedelta(minutes=index - 26)
        )
        expected.append((event_id, created_at))
        rows.append(
            {
                "id": event_id,
                "created_at": created_at,
                "actor": "cursor-test-admin",
                "action": "cursor.test",
                "target_type": "cursor_fixture",
                "target_id": f"row-{index:02d}",
                "request_id": f"request-{index:02d}",
                "source_ip_hash": None,
                "user_agent_hash": None,
                "event_metadata": {"fixture_index": index},
            }
        )

    sync_engine = create_engine(postgres_url)
    with sync_engine.begin() as conn:
        conn.execute(insert(AdminAuditEvent), rows)
    sync_engine.dispose()

    expected_ids = [
        str(event_id)
        for event_id, _created_at in sorted(
            expected,
            key=lambda item: (item[1], item[0].int),
            reverse=True,
        )
    ]
    transport = httpx.ASGITransport(app=audit_app)
    ids: list[str] = []
    page_sizes: list[int] = []
    cursors: list[str | None] = []
    cursor: str | None = None
    first_page_last_created_at: datetime | None = None
    first_page_last_id: UUID | None = None

    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://svc",
    ) as client:
        while True:
            params = {"limit": "17"}
            if cursor is not None:
                params["cursor"] = cursor
            response = await client.get(
                "/api/v1/admin/audit-events",
                params=params,
                headers=_admin_headers(),
            )
            assert response.status_code == 200, response.text
            body = response.json()
            page_ids = [item["id"] for item in body["items"]]
            ids.extend(page_ids)
            page_sizes.append(len(page_ids))
            cursors.append(body["next_cursor"])
            if len(page_sizes) == 1:
                first_page_last_id = UUID(page_ids[-1])
                first_page_last_created_at = datetime.fromisoformat(
                    body["items"][-1]["created_at"]
                )
            cursor = body["next_cursor"]
            if cursor is None:
                break
            assert len(cursors) < 10

        invalid = await client.get(
            "/api/v1/admin/audit-events",
            params={"cursor": "not-an-encoded-cursor"},
            headers=_admin_headers(),
        )

    assert page_sizes == [17, 17, 17, 2]
    assert cursors[-1] is None
    assert ids == expected_ids
    assert len(ids) == 53
    assert len(set(ids)) == 53
    assert first_page_last_id is not None
    assert first_page_last_created_at is not None
    decoded = decode_cursor(cursors[0] or "")
    assert decoded.id == first_page_last_id
    assert decoded.submitted_at == first_page_last_created_at
    assert invalid.status_code == 400
    assert "invalid cursor" in invalid.json()["detail"]
```

Keep `test_admin_audit_endpoint_rejects_team_tokens`; it is the explicit authorization guard for the paginated endpoint.

- [ ] **Step 2: Run the Admin audit tests RED**

Run:

```bash
uv run pytest tests/integration/test_service_admin_audit.py \
  -k 'encoded_tie_breaker_cursor or rejects_team_tokens' -q
```

Expected before implementation: the team-token authorization test passes. The 53-row test fails when `decode_cursor()` receives the current bare UUID cursor, with `ValueError: invalid cursor`.

- [ ] **Step 3: Replace cursor-row lookup with shared decoding**

Replace `src/loom_service/routes/admin_audit.py` imports and route cursor logic with:

```python
from typing import Annotated, Any

from fastapi import APIRouter, HTTPException, Query
from sqlalchemy import and_, or_, select

from loom.db.schema import AdminAuditEvent
from loom_service.dependencies import AdminSessionAndCtx
from loom_service.pagination import Cursor, decode_cursor, encode_cursor
```

```python
    stmt = select(AdminAuditEvent).order_by(
        AdminAuditEvent.created_at.desc(),
        AdminAuditEvent.id.desc(),
    )
    if cursor is not None:
        try:
            decoded = decode_cursor(cursor)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        stmt = stmt.where(
            or_(
                AdminAuditEvent.created_at < decoded.submitted_at,
                and_(
                    AdminAuditEvent.created_at == decoded.submitted_at,
                    AdminAuditEvent.id < decoded.id,
                ),
            )
        )

    rows = (await session.execute(stmt.limit(limit + 1))).scalars().all()
    page = rows[:limit]
    next_cursor = (
        encode_cursor(
            Cursor(submitted_at=page[-1].created_at, id=page[-1].id),
        )
        if len(rows) > limit and page
        else None
    )
    return {
        "items": [_serialize(row) for row in page],
        "next_cursor": next_cursor,
    }
```

Delete the now-unused `UUID` import and the `select(AdminAuditEvent).where(id == cursor_id)` lookup.

- [ ] **Step 4: Run Admin audit integration coverage GREEN**

Run:

```bash
uv run pytest tests/integration/test_service_admin_audit.py -q
```

Expected: all tests pass; the 53 events traverse in four pages, tied timestamps follow descending UUID order, the first cursor decodes to the last row on page one, the fourth cursor is null, malformed cursors return 400, and team tokens remain 403.

- [ ] **Step 5: Commit the Admin audit backend slice**

```bash
git add src/loom_service/routes/admin_audit.py \
  tests/integration/test_service_admin_audit.py
git commit -m "fix(api): encode stable Admin audit cursors (#774)"
```

### Task 3: Add synchronous cursor reset and accessible shared page states

**Files:**
- Create: `web/src/hooks/useCursorPage.ts`
- Create: `web/src/__tests__/hooks/useCursorPage.test.tsx`
- Modify: `web/src/components/Pagination.tsx`
- Modify: `web/src/__tests__/components/Pagination.test.tsx`

**Interfaces:**
- Consumes: `PageState`, `initialPage`, `nextPage()`, and `prevPage()` from `web/src/components/paginationState.ts`.
- Produces: `useCursorPage(resetKey): CursorPageController` and backward-compatible `PaginationProps` with optional `isLoading`/`isError` inputs.

- [ ] **Step 1: Write the hook tests before the hook exists**

Create `web/src/__tests__/hooks/useCursorPage.test.tsx`:

```tsx
import { act, renderHook } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { useCursorPage } from "../../hooks/useCursorPage";

describe("useCursorPage", () => {
  it("moves forward and backward through session-local cursor history", () => {
    const { result } = renderHook(() => useCursorPage("scope=my"));

    expect(result.current.state).toEqual({ current: null, stack: [] });
    act(() => result.current.next("cursor-2"));
    expect(result.current.state).toEqual({
      current: "cursor-2",
      stack: [null],
    });
    act(() => result.current.next("cursor-3"));
    expect(result.current.state).toEqual({
      current: "cursor-3",
      stack: [null, "cursor-2"],
    });
    act(() => result.current.prev());
    expect(result.current.state).toEqual({
      current: "cursor-2",
      stack: [null],
    });
  });

  it("exposes page one synchronously when resetKey changes", () => {
    const { result, rerender } = renderHook(
      ({ resetKey }) => useCursorPage(resetKey),
      { initialProps: { resetKey: "scope=my|state=" } },
    );

    act(() => result.current.next("stale-cursor"));
    expect(result.current.cursor).toBe("stale-cursor");

    rerender({ resetKey: "scope=all|state=finished" });

    expect(result.current.cursor).toBeNull();
    expect(result.current.state).toEqual({ current: null, stack: [] });
    act(() => result.current.prev());
    expect(result.current.state).toEqual({ current: null, stack: [] });
  });

  it("resets explicitly without changing the key", () => {
    const { result } = renderHook(() => useCursorPage("audit"));

    act(() => result.current.next("audit-page-2"));
    act(() => result.current.reset());

    expect(result.current.cursor).toBeNull();
    expect(result.current.state.stack).toEqual([]);
  });
});
```

- [ ] **Step 2: Extend the Pagination tests with loading, error, and terminal states**

Append these tests inside the existing `describe("Pagination helpers", ...)` block:

```tsx
  it("disables both controls and announces a loading page", () => {
    render(
      <Pagination
        state={{ current: "cursor-2", stack: [null] }}
        hasNext
        isLoading
        onNext={() => undefined}
        onPrev={() => undefined}
      />,
    );

    expect(screen.getByRole("status")).toHaveTextContent("Loading page 2");
    expect(screen.getByRole("button", { name: /previous page/i })).toBeDisabled();
    expect(screen.getByRole("button", { name: /next page/i })).toBeDisabled();
  });

  it("keeps previous available after an error and blocks next", () => {
    render(
      <Pagination
        state={{ current: "cursor-2", stack: [null] }}
        hasNext
        isError
        onNext={() => undefined}
        onPrev={() => undefined}
      />,
    );

    expect(screen.getByRole("status")).toHaveTextContent(
      "Page 2 could not be loaded",
    );
    expect(screen.getByRole("button", { name: /previous page/i })).toBeEnabled();
    expect(screen.getByRole("button", { name: /next page/i })).toBeDisabled();
  });

  it("announces the terminal page and disables next", () => {
    render(
      <Pagination
        state={{ current: "cursor-4", stack: [null, "cursor-2", "cursor-3"] }}
        hasNext={false}
        onNext={() => undefined}
        onPrev={() => undefined}
      />,
    );

    expect(screen.getByRole("status")).toHaveTextContent(
      "Page 4, end of results",
    );
    expect(screen.getByRole("button", { name: /previous page/i })).toBeEnabled();
    expect(screen.getByRole("button", { name: /next page/i })).toBeDisabled();
  });
```

- [ ] **Step 3: Run the hook/component tests RED**

Run:

```bash
cd web
npx vitest run \
  src/__tests__/hooks/useCursorPage.test.tsx \
  src/__tests__/components/Pagination.test.tsx
```

Expected before implementation: the hook test fails to resolve `../../hooks/useCursorPage`; the Pagination test fails typechecking because `isLoading` and `isError` are not declared, and no `role="status"` text exists.

- [ ] **Step 4: Implement `useCursorPage(resetKey)`**

Create `web/src/hooks/useCursorPage.ts`:

```ts
import { useCallback, useEffect, useState } from "react";

import {
  initialPage,
  nextPage,
  prevPage,
  type PageState,
} from "../components/paginationState";

type KeyedPageState = {
  resetKey: string;
  page: PageState;
};

export interface CursorPageController {
  state: PageState;
  cursor: string | null;
  next: (cursor: string) => void;
  prev: () => void;
  reset: () => void;
}

export function useCursorPage(resetKey: string): CursorPageController {
  const [stored, setStored] = useState<KeyedPageState>(() => ({
    resetKey,
    page: initialPage,
  }));

  const state = stored.resetKey === resetKey ? stored.page : initialPage;

  useEffect(() => {
    setStored((current) =>
      current.resetKey === resetKey
        ? current
        : { resetKey, page: initialPage },
    );
  }, [resetKey]);

  const update = useCallback(
    (transition: (page: PageState) => PageState) => {
      setStored((current) => {
        const currentPage =
          current.resetKey === resetKey ? current.page : initialPage;
        return {
          resetKey,
          page: transition(currentPage),
        };
      });
    },
    [resetKey],
  );

  const next = useCallback(
    (cursor: string) => update((page) => nextPage(page, cursor)),
    [update],
  );
  const prev = useCallback(
    () => update((page) => prevPage(page)),
    [update],
  );
  const reset = useCallback(
    () => setStored({ resetKey, page: initialPage }),
    [resetKey],
  );

  return {
    state,
    cursor: state.current,
    next,
    prev,
    reset,
  };
}
```

The exposed `state` is derived as page one before the reconciliation effect runs. This is the synchronous guarantee that prevents React Query from constructing a new-filter request with an old cursor. The effect only makes the reset durable for later renders.

- [ ] **Step 5: Replace `Pagination.tsx` with the status-aware component**

Use this complete file:

```tsx
/** Cursor pagination with a session-local previous-cursor stack. */
import { Button } from "./Button";
import { PAGINATION_NEXT_HELP, PAGINATION_PREV_HELP } from "../lib/helpText";
import type { PageState } from "./paginationState";

export interface PaginationProps {
  state: PageState;
  hasNext: boolean;
  isLoading?: boolean;
  isError?: boolean;
  onNext: () => void;
  onPrev: () => void;
}

export default function Pagination({
  state,
  hasNext,
  isLoading = false,
  isError = false,
  onNext,
  onPrev,
}: PaginationProps): JSX.Element {
  const pageNumber = state.stack.length + 1;
  const status = isLoading
    ? `Loading page ${pageNumber}…`
    : isError
      ? `Page ${pageNumber} could not be loaded.`
      : hasNext
        ? `Page ${pageNumber}, more results available.`
        : `Page ${pageNumber}, end of results.`;

  return (
    <div
      className="flex items-center justify-between gap-3"
      aria-busy={isLoading}
    >
      <p
        className="text-xs text-slate-500"
        role="status"
        aria-live="polite"
      >
        {status}
      </p>
      <div className="flex items-center gap-2">
        <Button
          size="sm"
          onClick={onPrev}
          disabled={isLoading || state.stack.length === 0}
          aria-label="previous page"
          title={PAGINATION_PREV_HELP}
        >
          ← Prev
        </Button>
        <Button
          size="sm"
          onClick={onNext}
          disabled={isLoading || isError || !hasNext}
          aria-label="next page"
          title={PAGINATION_NEXT_HELP}
        >
          Next →
        </Button>
      </div>
    </div>
  );
}
```

- [ ] **Step 6: Run hook, component, type, and lint checks GREEN**

Run:

```bash
cd web
npx vitest run \
  src/__tests__/hooks/useCursorPage.test.tsx \
  src/__tests__/components/Pagination.test.tsx
npm run typecheck
npm run lint
```

Expected: all hook and Pagination tests pass; typecheck and ESLint exit zero with no warning. Existing Pagination callers compile because both new props are optional.

- [ ] **Step 7: Commit the shared frontend cursor slice**

```bash
git add web/src/hooks/useCursorPage.ts \
  web/src/__tests__/hooks/useCursorPage.test.tsx \
  web/src/components/Pagination.tsx \
  web/src/__tests__/components/Pagination.test.tsx
git commit -m "feat(web): add reset-safe cursor pagination (#774)"
```

### Task 4: Wire Run Library to URL-reset cursor history

**Files:**
- Modify: `web/src/pages/RunLibrary.tsx`
- Modify: `web/src/__tests__/pages/RunLibrary.test.tsx`

**Interfaces:**
- Consumes: `useCursorPage(resetKey)`, `Pagination`, and the existing `RunLibraryBatchList.next_cursor` type.
- Produces: API requests with `cursor` and `limit=50`; URL-backed filters with no URL cursor; Next/Prev/loading/error/terminal states.

- [ ] **Step 1: Add failing page tests for traversal and reset**

Add `act` and `useLocation` to the test imports, then add this location probe and deterministic fetch helper below `jsonResponse()`:

```tsx
function LocationProbe(): JSX.Element {
  const location = useLocation();
  return <output data-testid="location-search">{location.search}</output>;
}

function mockCursorRunLibrary(holdSecondPage = false) {
  const requests: URL[] = [];
  let resolveSecondPage: ((response: Response) => void) | null = null;
  const secondPage = holdSecondPage
    ? new Promise<Response>((resolve) => {
        resolveSecondPage = resolve;
      })
    : Promise.resolve(
        jsonResponse({
          items: [
            {
              ...sharedBatch,
              id: "batch-page-2",
              name: "cursor page two run",
              created_at: "2026-06-21T20:00:00Z",
            },
          ],
          next_cursor: null,
        }),
      );

  const fetchMock = vi
    .spyOn(globalThis, "fetch")
    .mockImplementation((input: RequestInfo | URL) => {
      const url = new URL(String(input), "http://localhost");
      if (url.pathname === "/api/v1/auth/me") {
        return Promise.resolve(
          jsonResponse({
            user: {
              id: "user-beta",
              username: "Beta",
              email: "beta@example.com",
              display_name: "Beta User",
              is_platform_admin: false,
            },
            teams: [{ id: "team-beta", name: "Beta Apps", role: "owner" }],
            current_team: { id: "team-beta", name: "Beta Apps", role: "owner" },
            role: "owner",
            scopes: ["read:own", "submit"],
            is_platform_admin: false,
            csrf_token: "csrf-test",
          }),
        );
      }
      if (url.pathname === "/api/v1/run-library/batches") {
        requests.push(url);
        if (url.searchParams.get("state") === "finished") {
          return Promise.resolve(
            jsonResponse({
              items: [
                {
                  ...sharedBatch,
                  id: "batch-filtered",
                  name: "filtered page one run",
                },
              ],
              next_cursor: null,
            }),
          );
        }
        if (url.searchParams.get("cursor") === "run-library-page-2") {
          return secondPage;
        }
        return Promise.resolve(
          jsonResponse({
            items: [{ ...sharedBatch, name: "cursor page one run" }],
            next_cursor: "run-library-page-2",
          }),
        );
      }
      return Promise.resolve(jsonResponse({ detail: `unhandled ${url}` }, 404));
    });

  return {
    fetchMock,
    requests,
    resolveSecondPage(response: Response): void {
      if (resolveSecondPage === null) {
        throw new Error("second page is not deferred");
      }
      resolveSecondPage(response);
    },
  };
}
```

Add these tests to the `RunLibrary` describe block:

```tsx
  it("traverses Next and Prev with loading and terminal states", async () => {
    const mock = mockCursorRunLibrary(true);
    const user = userEvent.setup();
    renderWithProviders(
      <>
        <RunLibrary />
        <LocationProbe />
      </>,
      { route: "/library?scope=all" },
    );

    expect(await screen.findByText("cursor page one run")).toBeInTheDocument();
    expect(screen.getByRole("status")).toHaveTextContent(
      "Page 1, more results available",
    );
    expect(screen.getByRole("button", { name: /previous page/i })).toBeDisabled();
    await user.click(screen.getByRole("button", { name: /next page/i }));

    expect(await screen.findByRole("status")).toHaveTextContent("Loading page 2");
    expect(screen.getByRole("button", { name: /previous page/i })).toBeDisabled();
    expect(screen.getByRole("button", { name: /next page/i })).toBeDisabled();
    await act(async () => {
      mock.resolveSecondPage(
        jsonResponse({
          items: [
            {
              ...sharedBatch,
              id: "batch-page-2",
              name: "cursor page two run",
            },
          ],
          next_cursor: null,
        }),
      );
    });

    expect(await screen.findByText("cursor page two run")).toBeInTheDocument();
    expect(screen.getByRole("status")).toHaveTextContent(
      "Page 2, end of results",
    );
    expect(screen.getByRole("button", { name: /next page/i })).toBeDisabled();
    expect(screen.getByTestId("location-search")).toHaveTextContent("scope=all");
    expect(screen.getByTestId("location-search")).not.toHaveTextContent("cursor");

    await user.click(screen.getByRole("button", { name: /previous page/i }));
    expect(await screen.findByText("cursor page one run")).toBeInTheDocument();
  });

  it("resets to page one before requesting a changed URL filter", async () => {
    const mock = mockCursorRunLibrary();
    const user = userEvent.setup();
    renderWithProviders(
      <>
        <RunLibrary />
        <LocationProbe />
      </>,
      { route: "/library?scope=all" },
    );

    expect(await screen.findByText("cursor page one run")).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: /next page/i }));
    expect(await screen.findByText("cursor page two run")).toBeInTheDocument();
    await user.selectOptions(screen.getByLabelText("State"), "finished");

    expect(await screen.findByText("filtered page one run")).toBeInTheDocument();
    const filteredRequests = mock.requests.filter(
      (url) => url.searchParams.get("state") === "finished",
    );
    expect(filteredRequests).toHaveLength(1);
    expect(filteredRequests[0].searchParams.get("cursor")).toBeNull();
    expect(screen.getByRole("button", { name: /previous page/i })).toBeDisabled();
    expect(screen.getByTestId("location-search")).toHaveTextContent("state=finished");
    expect(screen.getByTestId("location-search")).not.toHaveTextContent("cursor");
  });
```

Keep the existing empty and error rendering branches. Add one test for each using a fetch response of `{items: [], next_cursor: null}` and a `503 {detail: "library unavailable"}` response, asserting `No runs match this library view.` and `library unavailable` respectively.

- [ ] **Step 2: Run the Run Library page tests RED**

```bash
cd web
npx vitest run src/__tests__/pages/RunLibrary.test.tsx
```

Expected before implementation: tests fail because no previous/next buttons or page status exist and no request includes `cursor=run-library-page-2`.

- [ ] **Step 3: Add reset-keyed cursor query identity**

Add imports:

```ts
import Pagination from "../components/Pagination";
import { useCursorPage } from "../hooks/useCursorPage";
```

Immediately after the URL-derived filter constants, add:

```ts
  const page = useCursorPage(
    JSON.stringify([
      scope,
      teamId,
      state,
      artifactType,
      search,
      benchmarkId,
      agentName,
      modelProvider,
      modelName,
      providerConnectionId,
      providerModelId,
    ]),
  );
```

Append `page.cursor` to the query key and add these request parameters:

```ts
        cursor: page.cursor ?? undefined,
        limit: "50",
```

The query key must end with `providerModelId, page.cursor`; this makes every cursor a distinct React Query page while `resetKey` makes changed filters use null synchronously.

- [ ] **Step 4: Render pagination under every non-error/failed page body**

Add this footer immediately after `Card.Body` in the results card:

```tsx
        <Card.Footer>
          <Pagination
            state={page.state}
            hasNext={query.data?.next_cursor != null}
            isLoading={query.isPending || query.isFetching}
            isError={query.isError}
            onNext={() => {
              const cursor = query.data?.next_cursor;
              if (cursor) page.next(cursor);
            }}
            onPrev={page.prev}
          />
        </Card.Footer>
```

Do not condition the footer on `items.length`; a terminal empty result still needs its explicit status, and an empty later page must retain Prev.

- [ ] **Step 5: Run Run Library frontend checks GREEN**

```bash
cd web
npx vitest run \
  src/__tests__/hooks/useCursorPage.test.tsx \
  src/__tests__/components/Pagination.test.tsx \
  src/__tests__/pages/RunLibrary.test.tsx
npm run typecheck
npm run lint
```

Expected: all tests pass; the page-two request has the opaque cursor; filter reset's first request has no cursor; URL search retains filters and never contains cursor; typecheck/lint are clean.

- [ ] **Step 6: Commit the Run Library frontend slice**

```bash
git add web/src/pages/RunLibrary.tsx \
  web/src/__tests__/pages/RunLibrary.test.tsx
git commit -m "fix(web): expose complete Run Library pages (#774)"
```

### Task 5: Extract and paginate the administrator audit log

**Files:**
- Create: `web/src/components/admin/AdminAuditLog.tsx`
- Create: `web/src/__tests__/components/admin/AdminAuditLog.test.tsx`
- Modify: `web/src/pages/AdminAccess.tsx`
- Modify: `web/src/__tests__/pages/AdminAccess.test.tsx`

**Interfaces:**
- Consumes: `api.listAdminAuditEvents(50, cursor)`, `useCursorPage("admin-audit-events")`, and `Pagination`.
- Produces: `AdminAuditLog(): JSX.Element`, mounted only for a platform administrator's Audit tab.

- [ ] **Step 1: Add the failing extracted-component test**

Create `web/src/__tests__/components/admin/AdminAuditLog.test.tsx` with an admin `/auth/me` fixture and an audit route that returns page one `{items: [audit-1], next_cursor: "audit-page-2"}`, defers page two, then returns `{items: [audit-2], next_cursor: null}`. The test must assert, in order: page-one action visible; Next enabled and Prev disabled; clicking Next announces `Loading page 2` and disables both controls; resolving page two shows `audit.second`, enables Prev, disables Next, and announces `Page 2, end of results`; clicking Prev restores `audit.first`. Add two more cases: an empty response renders `No admin audit events.` and a 503 response renders `audit unavailable` while the status says `Page 1 could not be loaded.`

Use these exact event bodies:

```ts
const auditPageOne = {
  items: [{
    id: "audit-1",
    created_at: "2026-07-10T12:00:00Z",
    actor: "qianyi",
    action: "audit.first",
    target_type: "team",
    target_id: "team-1",
    request_id: null,
    source_ip_hash: null,
    user_agent_hash: null,
    metadata: {},
  }],
  next_cursor: "audit-page-2",
};

const auditPageTwo = {
  items: [{
    id: "audit-2",
    created_at: "2026-07-10T11:00:00Z",
    actor: "qianyi",
    action: "audit.second",
    target_type: "token",
    target_id: "prefix-2",
    request_id: null,
    source_ip_hash: null,
    user_agent_hash: null,
    metadata: {},
  }],
  next_cursor: null,
};
```

- [ ] **Step 2: Run the extracted-component test RED**

```bash
cd web
npx vitest run src/__tests__/components/admin/AdminAuditLog.test.tsx
```

Expected: module resolution fails because `AdminAuditLog.tsx` does not exist.

- [ ] **Step 3: Create the complete `AdminAuditLog` component**

Create `web/src/components/admin/AdminAuditLog.tsx`:

```tsx
import { useQuery } from "@tanstack/react-query";

import { api, type AdminAuditEvent } from "../../api/client";
import { useCursorPage } from "../../hooks/useCursorPage";
import { formatLocalDateTime } from "../../lib/dateTime";
import { Card } from "../Card";
import EmptyState from "../EmptyState";
import ErrorState from "../ErrorState";
import LoadingState from "../LoadingState";
import Pagination from "../Pagination";

function AuditRows({ events }: { events: AdminAuditEvent[] }): JSX.Element {
  if (events.length === 0) return <EmptyState label="No admin audit events." />;
  return (
    <div className="overflow-x-auto">
      <table className="min-w-full divide-y divide-slate-200 text-sm">
        <thead className="bg-slate-50 text-left text-xs uppercase tracking-wider text-slate-500">
          <tr>
            <th className="px-3 py-2 font-semibold">Time</th>
            <th className="px-3 py-2 font-semibold">Actor</th>
            <th className="px-3 py-2 font-semibold">Action</th>
            <th className="px-3 py-2 font-semibold">Target</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-slate-100 bg-white">
          {events.map((event) => (
            <tr key={event.id}>
              <td className="whitespace-nowrap px-3 py-2 text-slate-600">
                {formatLocalDateTime(event.created_at)}
              </td>
              <td className="whitespace-nowrap px-3 py-2 font-medium text-slate-800">
                {event.actor}
              </td>
              <td className="whitespace-nowrap px-3 py-2 font-mono text-xs text-slate-700">
                {event.action}
              </td>
              <td className="px-3 py-2 text-slate-600">
                {event.target_type}:{event.target_id}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export default function AdminAuditLog(): JSX.Element {
  const page = useCursorPage("admin-audit-events");
  const query = useQuery({
    queryKey: ["admin", "audit-events", page.cursor],
    queryFn: () => api.listAdminAuditEvents(50, page.cursor ?? undefined),
  });

  return (
    <Card>
      <Card.Header
        title="Audit log"
        description="Admin access decisions with actor, action, and target."
      />
      <Card.Body>
        {query.isPending ? <LoadingState label="Loading audit events…" /> : null}
        {query.isError ? <ErrorState error={query.error} /> : null}
        {query.data ? <AuditRows events={query.data.items} /> : null}
      </Card.Body>
      <Card.Footer>
        <Pagination
          state={page.state}
          hasNext={query.data?.next_cursor != null}
          isLoading={query.isPending || query.isFetching}
          isError={query.isError}
          onNext={() => {
            const cursor = query.data?.next_cursor;
            if (cursor) page.next(cursor);
          }}
          onPrev={page.prev}
        />
      </Card.Footer>
    </Card>
  );
}
```

- [ ] **Step 4: Replace parent-owned audit query and table**

In `AdminAccess.tsx`, remove `AdminAuditEvent` from the API imports, delete `AuditRows`, delete the parent `audit = useQuery(...)`, import `AdminAuditLog`, and replace the Audit tab card with:

```tsx
      {isAdmin && activeSection === "audit" ? <AdminAuditLog /> : null}
```

In the existing Admin Access test, assert no `/api/v1/admin/audit-events` request exists before clicking the Audit tab, then click Audit, await `team_registration.approve`, and assert exactly one audit request. This proves non-admin users and inactive tabs do not fetch administrator audit data.

- [ ] **Step 5: Run Admin audit frontend checks GREEN**

```bash
cd web
npx vitest run \
  src/__tests__/components/admin/AdminAuditLog.test.tsx \
  src/__tests__/pages/AdminAccess.test.tsx
npm run typecheck
npm run lint
```

Expected: traversal, loading, empty, error, terminal, and lazy-mount assertions pass; typecheck/lint are clean.

- [ ] **Step 6: Commit the Admin audit frontend slice**

```bash
git add web/src/components/admin/AdminAuditLog.tsx \
  web/src/__tests__/components/admin/AdminAuditLog.test.tsx \
  web/src/pages/AdminAccess.tsx \
  web/src/__tests__/pages/AdminAccess.test.tsx
git commit -m "fix(web): paginate administrator audit events (#774)"
```

### Task 6: Prove cursor traversal in the guarded browser and fixed candidate

**Files:**
- Modify: `web/e2e/fixtures/guardedTest.ts`
- Modify: `web/e2e/fixtures/api.ts`
- Create: `web/e2e/pagination.spec.ts`
- Modify: `docs/architecture/run-library.md`

**Interfaces:**
- Consumes: #773's closed deterministic API router and `role` fixture.
- Produces: fixture option `paginationScenario: boolean`; deterministic
  Run Library and administrator audit cursor pages; desktop/mobile browser
  proof that Next/Previous and filter reset issue the correct API requests.

- [ ] **Step 1: Add a failing pagination browser scenario**

Extend #773's fixture type with:

```ts
type PaginationOptions = {
  paginationScenario: boolean;
};

export const test = base.extend<RoleOptions & PaginationOptions>({
  paginationScenario: [false, { option: true }],
});
```

Create `web/e2e/pagination.spec.ts` with request capture before navigation:

```ts
import { expect, test } from "./fixtures/guardedTest";

test.describe("cursor pagination", () => {
  test.use({ paginationScenario: true });

  test("Run Library traverses forward, backward, and resets on filters", async ({ page }) => {
    const requests: string[] = [];
    page.on("request", (request) => {
      if (request.url().includes("/api/v1/run-library/batches")) {
        requests.push(request.url());
      }
    });
    await page.goto("/library");
    await expect(page.getByText("run-page-1-first")).toBeVisible();
    await page.getByRole("button", { name: "Next page" }).click();
    await expect(page.getByText("run-page-2-first")).toBeVisible();
    await expect(page.getByText("Page 2")).toBeVisible();
    await page.getByRole("button", { name: "Previous page" }).click();
    await expect(page.getByText("run-page-1-first")).toBeVisible();
    await page.getByRole("textbox", { name: "Search" }).fill("retry");
    await expect.poll(() => requests.at(-1) ?? "").not.toContain("cursor=");
    expect(requests.some((url) => url.includes("cursor=run-cursor-1"))).toBe(true);
  });

  test("administrator audit reaches its second page and terminal state", async ({ page }) => {
    test.info().annotations.push({ type: "role", description: "admin" });
    await page.goto("/admin/access");
    await page.getByRole("tab", { name: "Audit" }).click();
    await expect(page.getByText("audit-page-1-first")).toBeVisible();
    await page.getByRole("button", { name: "Next page" }).click();
    await expect(page.getByText("audit-page-2-first")).toBeVisible();
    await expect(page.getByRole("button", { name: "Next page" })).toBeDisabled();
    await page.getByRole("button", { name: "Previous page" }).click();
    await expect(page.getByText("audit-page-1-first")).toBeVisible();
  });
});
```

Use separate `test.describe` blocks with `test.use({ role: "user" })` and
`test.use({ role: "admin" })` in the final file; the annotation above is not a
substitute for the role fixture. This first RED intentionally omits those role
bindings so the closed API router returns 401/403 and proves the fixture
boundary is active.

- [ ] **Step 2: Run the new spec RED**

```bash
cd web
npm run test:e2e -- e2e/pagination.spec.ts
```

Expected: FAIL because `paginationScenario` and its cursor-aware deterministic
responses do not exist, and the administrator block is not yet bound to the
administrator role.

- [ ] **Step 3: Add deterministic cursor-aware responses**

In `web/e2e/fixtures/api.ts`, when `paginationScenario` is true, route exact
requests as follows while leaving the catch-all closed:

```ts
import type { RunLibraryBatch } from "../../src/api/client";

function runBatch(id: string, createdAt: string): RunLibraryBatch {
  return {
    id,
    team_id: "team-research",
    owner_team: { id: "team-research", name: "Research" },
    submitted_by_user: {
      id: "user-ada",
      username: "ada",
      team_id: "team-research",
      team_name: "Research",
    },
    name: id,
    description: null,
    task_filter: {},
    trial_config: {},
    backend: "docker",
    combinations: [],
    provider_connection_id: null,
    state: "succeeded",
    result_status: "success",
    visibility: "team",
    share_status: "shared",
    source_provenance: [],
    expected_trial_count: 1,
    created_by_token_prefix: "fixture",
    created_at: createdAt,
    finished_at: createdAt,
    trial_summary: { succeeded: 1 },
    aggregate_reward: 1,
    artifact_summary: {
      reports: 0,
      trajectories: 0,
      reusable_outputs: 0,
      logs_diagnostics: 0,
      raw_diagnostics: 0,
    },
  };
}

const runPages = new Map<string, object>([
  ["", { items: [runBatch("run-page-1-first", "2026-07-10T12:00:00Z")], next_cursor: "run-cursor-1" }],
  ["run-cursor-1", { items: [runBatch("run-page-2-first", "2026-07-10T11:00:00Z")], next_cursor: null }],
]);

const auditPages = new Map<string, object>([
  ["", { items: [{ id: "audit-page-1-first", actor: "admin", action: "team_registration.approve", target_type: "team", target_id: "team-1", created_at: "2026-07-10T12:00:00Z" }], next_cursor: "audit-cursor-1" }],
  ["audit-cursor-1", { items: [{ id: "audit-page-2-first", actor: "admin", action: "token.revoke", target_type: "token", target_id: "token-1", created_at: "2026-07-10T11:00:00Z" }], next_cursor: null }],
]);
```

Select each response with
`new URL(request.url()).searchParams.get("cursor") ?? ""`; do not infer pages
from request order. Bind the two describes to `role: "user"` and
`role: "admin"`, and replace the temporary role annotation.

- [ ] **Step 4: Run browser and full focused gates GREEN**

```bash
cd web
npm run test:e2e -- e2e/pagination.spec.ts
npm run typecheck
npm run lint
npm run test:coverage
cd ..
uv run pytest tests/integration/test_service_run_library.py \
  tests/integration/test_service_admin_audit.py -q
```

Expected: pagination passes in #773's desktop and mobile Chromium projects;
the browser guard reports no console/page/resource/root failure; typecheck,
lint, coverage, and both 53-row integration traversals pass.

- [ ] **Step 5: Document the cursor contract and commit**

Document stable `(created_at DESC, id DESC)` traversal, session-local cursor
history, synchronous filter reset, and terminal `next_cursor = null` in
`docs/architecture/run-library.md`.

```bash
git add web/e2e/fixtures/guardedTest.ts web/e2e/fixtures/api.ts \
  web/e2e/pagination.spec.ts docs/architecture/run-library.md
git commit -m "test(web): prove complete cursor traversal (#774)"
```

- [ ] **Step 6: Run fixed-candidate staging acceptance before closure**

After merge, record the exact `dev` SHA/web image and use normal user APIs to
traverse a real Run Library dataset with at least 53 tied/non-tied rows. Use
#692's secret-source administrator session to traverse Admin audit forward and
back. Record page IDs/order, cursor presence only (never raw cursor values),
terminal state, filter-reset result, browser console/network status, browser
version, and viewport. Link sanitized evidence to #774, #493, and #715. Keep
#774 `[Needs validation]` if any gap, duplicate, stale-filter request, disabled
control error, or browser guard failure remains.

---

## Implementation and PR Boundaries

1. Backend Run Library and Admin audit contracts (Tasks 1-2) may land as one
   PR after their 53-row integration fixtures pass.
2. Shared cursor state and Run Library UI (Tasks 3-4) land after #773's strict
   type/unit gate.
3. Admin audit extraction (Task 5) follows to avoid overlapping
   `AdminAccess.tsx` with #775/#776.
4. Browser proof (Task 6) starts only after #773's guarded Playwright fixture
   merges.

Every PR targets `dev`, uses `Advances #774`, carries the required validation
labels for its paths, and enables squash auto-merge immediately. Do not close
#774 until fixed-candidate staging evidence proves both user and administrator
traversal.
