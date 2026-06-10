/**
 * Rate-cards browse + publish.
 *
 * READ is open to any signed-in team user — they need to see the
 * pricing their calls are billed against. WRITE is gated by the
 * backend on the `admin:rate_cards` scope; the publish form below
 * still renders for team users (so they can read the JSON), but
 * pressing Publish returns 403 and we surface that as an ErrorState.
 *
 * The `isAdmin` UX signal hides the Publish form for non-admins so
 * we don't tease an action they can't take.
 */
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useState } from "react";

import { api } from "../api/client";
import { useAuth } from "../auth/useAuth";
import { Button } from "../components/Button";
import { Card } from "../components/Card";
import EmptyState from "../components/EmptyState";
import ErrorState from "../components/ErrorState";
import { Textarea } from "../components/Input";
import JsonViewer from "../components/JsonViewer";
import LoadingState from "../components/LoadingState";

const DEFAULT_BODY = `{
  "id": "default-2026Q3",
  "captured_at": "${new Date().toISOString()}",
  "table": {
    "id": "default-2026Q3",
    "entries": [
      {
        "provider": "openai",
        "model": "gpt-4o",
        "input_per_mtok": 5.0,
        "output_per_mtok": 15.0,
        "cache_read_per_mtok": 0.0,
        "cache_write_per_mtok": 0.0
      },
      {
        "provider": "anthropic",
        "model": "claude-opus-4-7",
        "input_per_mtok": 15.0,
        "output_per_mtok": 75.0,
        "cache_read_per_mtok": 1.5,
        "cache_write_per_mtok": 18.75
      }
    ]
  }
}`;

export default function RateCardsAdmin(): JSX.Element {
  const { isAdmin } = useAuth();
  const [bodyText, setBodyText] = useState(DEFAULT_BODY);
  const [localError, setLocalError] = useState<string | null>(null);

  const queryClient = useQueryClient();
  const list = useQuery({
    queryKey: ["rate-cards"],
    queryFn: () => api.listRateCards(),
  });
  const create = useMutation({
    mutationFn: (body: Record<string, unknown>) => api.createRateCard(body),
    onSuccess: () =>
      queryClient.invalidateQueries({ queryKey: ["rate-cards"] }),
  });

  const submit = (): void => {
    setLocalError(null);
    try {
      const parsed: unknown = JSON.parse(bodyText);
      if (
        typeof parsed !== "object" ||
        parsed === null ||
        Array.isArray(parsed)
      ) {
        setLocalError("expected a JSON object");
        return;
      }
      create.mutate(parsed as Record<string, unknown>);
    } catch (e) {
      setLocalError(e instanceof Error ? e.message : String(e));
    }
  };

  return (
    <div className="space-y-6">
      <header>
        <h1 className="text-2xl font-bold text-slate-900">Rate cards</h1>
        <p className="mt-1 text-sm text-slate-500">
          Pricing the LLM Gateway uses to derive `cost_usd` per call.
          Reads are open to all team users; publishing requires the{" "}
          <code className="rounded bg-slate-100 px-1 py-0.5 font-mono text-xs">
            admin:rate_cards
          </code>{" "}
          scope.
        </p>
      </header>

      <Card>
        <Card.Header title="Published" />
        <Card.Body>
          {list.isPending ? <LoadingState /> : null}
          {list.isError ? <ErrorState error={list.error} /> : null}
          {list.data ? (
            list.data.items.length === 0 ? (
              <EmptyState label="No rate cards published yet." />
            ) : (
              <JsonViewer data={list.data.items} expanded />
            )
          ) : null}
        </Card.Body>
      </Card>

      {isAdmin ? (
        <Card>
          <Card.Header
            title="Publish a new rate card"
            description="POSTs to the Gateway via the service-layer proxy."
          />
          <Card.Body className="space-y-3">
            <Textarea
              value={bodyText}
              onChange={(e) => setBodyText(e.target.value)}
              rows={14}
            />
            {localError ? (
              <div className="rounded-xl border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700">
                {localError}
              </div>
            ) : null}
            {create.isError ? <ErrorState error={create.error} /> : null}
            {create.isSuccess ? (
              <div className="rounded-xl border border-emerald-200 bg-emerald-50 px-4 py-3 text-sm text-emerald-700">
                Rate card published.
              </div>
            ) : null}
          </Card.Body>
          <Card.Footer>
            <div className="flex justify-end">
              <Button
                variant="primary"
                onClick={submit}
                disabled={create.isPending}
              >
                {create.isPending ? "Publishing…" : "Publish"}
              </Button>
            </div>
          </Card.Footer>
        </Card>
      ) : null}
    </div>
  );
}
