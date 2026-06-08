/**
 * Admin-only rate-card management. The list comes from the Gateway
 * proxy; the create form is a plain JSON paste (rate cards are
 * structurally simple — a `table` map plus `id` and `captured_at`).
 */

import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useState } from "react";

import { api } from "../api/client";
import EmptyState from "../components/EmptyState";
import ErrorState from "../components/ErrorState";
import JsonViewer from "../components/JsonViewer";
import LoadingState from "../components/LoadingState";

const DEFAULT_BODY = `{
  "id": "default-2026Q3",
  "captured_at": "${new Date().toISOString()}",
  "table": {
    "openai/gpt-4o": { "input_per_1k_tokens": 0.005, "output_per_1k_tokens": 0.015 },
    "anthropic/claude-3-5-sonnet": { "input_per_1k_tokens": 0.003, "output_per_1k_tokens": 0.015 }
  }
}`;

export default function RateCardsAdmin(): JSX.Element {
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
    <>
      <div className="loom-page-header">
        <h1>Rate cards (admin)</h1>
      </div>

      <div className="loom-card">
        <h2 style={{ marginTop: 0 }}>Published</h2>
        {list.isPending ? <LoadingState /> : null}
        {list.isError ? <ErrorState error={list.error} /> : null}
        {list.data ? (
          list.data.items.length === 0 ? (
            <EmptyState label="No rate cards published yet." />
          ) : (
            <JsonViewer data={list.data.items} />
          )
        ) : null}
      </div>

      <div className="loom-card">
        <h2 style={{ marginTop: 0 }}>Publish a new rate card</h2>
        <p className="loom-muted">
          POSTs to the Gateway via the service-layer proxy. Requires
          the <code>admin:rate_cards</code> scope.
        </p>
        <textarea
          className="loom-mono"
          value={bodyText}
          onChange={(e) => setBodyText(e.target.value)}
          rows={14}
          style={{ width: "100%" }}
        />
        <div style={{ marginTop: "0.6rem" }}>
          <button onClick={submit} disabled={create.isPending}>
            {create.isPending ? "Publishing…" : "Publish"}
          </button>
        </div>
        {localError ? (
          <div className="loom-error" style={{ marginTop: "0.6rem" }}>
            {localError}
          </div>
        ) : null}
        {create.isError ? (
          <div style={{ marginTop: "0.6rem" }}>
            <ErrorState error={create.error} />
          </div>
        ) : null}
        {create.isSuccess ? (
          <div
            style={{
              marginTop: "0.6rem",
              color: "var(--color-success)",
            }}
          >
            Rate card published.
          </div>
        ) : null}
      </div>
    </>
  );
}
