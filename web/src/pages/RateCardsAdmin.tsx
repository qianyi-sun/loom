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
import CommandSnippet from "../components/CommandSnippet";
import { DiagnosticPanel } from "../components/DiagnosticPanel";
import DocsCallout from "../components/DocsCallout";
import EmptyState from "../components/EmptyState";
import ErrorState from "../components/ErrorState";
import { Textarea } from "../components/Input";
import LoadingState from "../components/LoadingState";
import { rateCardExampleJson } from "../lib/quickstartSnippets";

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

type RateCardEntry = {
  cache_read_per_mtok?: number | null;
  cache_write_per_mtok?: number | null;
  input_per_mtok?: number | null;
  model?: string | null;
  output_per_mtok?: number | null;
  provider?: string | null;
};

type RateCard = {
  captured_at?: string | null;
  id?: string | null;
  table?: {
    entries?: RateCardEntry[] | null;
  } | null;
};

function moneyPerMtok(value?: number | null): string {
  if (typeof value !== "number" || Number.isNaN(value)) return "not set";
  return `$${value.toFixed(2)} / 1M tokens`;
}

function RateCardSummary({ items }: { items: RateCard[] }): JSX.Element {
  return (
    <div className="space-y-4">
      {items.map((card, index) => {
        const entries = card.table?.entries ?? [];
        return (
          <section
            key={card.id ?? index}
            className="rounded-lg border border-slate-200 bg-slate-50 p-4"
          >
            <div className="flex flex-wrap items-baseline justify-between gap-2">
              <h3 className="text-sm font-semibold text-slate-900">
                {card.id ?? `Rate card ${index + 1}`}
              </h3>
              <p className="text-xs text-slate-500">
                {entries.length} price {entries.length === 1 ? "entry" : "entries"}
                {card.captured_at ? ` - captured ${card.captured_at}` : ""}
              </p>
            </div>
            {entries.length === 0 ? (
              <p className="mt-3 text-sm text-slate-500">
                No model pricing entries are published in this card.
              </p>
            ) : (
              <div className="mt-3 overflow-x-auto">
                <table className="min-w-full divide-y divide-slate-200 text-sm">
                  <thead>
                    <tr className="text-left text-xs uppercase tracking-wider text-slate-500">
                      <th className="py-2 pr-4 font-medium">Provider</th>
                      <th className="py-2 pr-4 font-medium">Model</th>
                      <th className="py-2 pr-4 font-medium">Input</th>
                      <th className="py-2 pr-4 font-medium">Output</th>
                      <th className="py-2 pr-4 font-medium">Cache read</th>
                      <th className="py-2 pr-4 font-medium">Cache write</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-slate-100">
                    {entries.map((entry, entryIndex) => (
                      <tr key={`${entry.provider}-${entry.model}-${entryIndex}`}>
                        <td className="py-2 pr-4 text-slate-700">
                          {entry.provider ?? "unknown"}
                        </td>
                        <td className="py-2 pr-4 font-mono text-xs text-slate-700">
                          {entry.model ?? "unknown"}
                        </td>
                        <td className="py-2 pr-4 text-slate-700">
                          {moneyPerMtok(entry.input_per_mtok)}
                        </td>
                        <td className="py-2 pr-4 text-slate-700">
                          {moneyPerMtok(entry.output_per_mtok)}
                        </td>
                        <td className="py-2 pr-4 text-slate-700">
                          {moneyPerMtok(entry.cache_read_per_mtok)}
                        </td>
                        <td className="py-2 pr-4 text-slate-700">
                          {moneyPerMtok(entry.cache_write_per_mtok)}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </section>
        );
      })}
    </div>
  );
}

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

      <DocsCallout title="Rate-card JSON example" tone="info">
        <p>
          Provider connections can set{" "}
          <code className="rounded bg-white px-1 py-0.5 font-mono text-xs">
            rate_card_provider
          </code>{" "}
          when the provider's billing namespace differs from the connection
          name. Publish matching provider/model entries before switching a
          connection to rate-card pricing.
        </p>
        <CommandSnippet
          label="Rate-card entry"
          command={rateCardExampleJson()}
        />
      </DocsCallout>

      <Card>
        <Card.Header title="Published" />
        <Card.Body>
          {list.isPending ? <LoadingState /> : null}
          {list.isError ? <ErrorState error={list.error} /> : null}
          {list.data ? (
            list.data.items.length === 0 ? (
              <EmptyState label="No rate cards published yet." />
            ) : (
              <div className="space-y-4">
                <RateCardSummary items={list.data.items as RateCard[]} />
                <DiagnosticPanel
                  title="Rate-card diagnostics"
                  description="Raw published payloads for troubleshooting pricing imports or API compatibility."
                  blocks={[
                    {
                      title: "raw_rate_cards",
                      data: list.data.items,
                      expanded: true,
                    },
                  ]}
                />
              </div>
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
