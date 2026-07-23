/**
 * /providers — list of all team provider connections.
 * Empty state CTA + populated table. Closes #167 (slice 1 of 5).
 */
import { useQuery } from "@tanstack/react-query";
import { useEffect, useRef } from "react";
import { Link, useLocation } from "react-router-dom";

import { api } from "../api/client";
import { Card } from "../components/Card";
import CommandSnippet from "../components/CommandSnippet";
import LoadingState from "../components/LoadingState";
import { StatusPill } from "../components/StatusPill";
import { hostedProviderCommands } from "../lib/quickstartSnippets";
import { providerStatusSummary } from "../lib/providerDisplay";
import { currentServerOrigin } from "../lib/serverOrigin";

type Conn = {
  id: string;
  name: string;
  type: string;
  status?: string;
};

export default function ProvidersList(): JSX.Element {
  const location = useLocation();
  const headingRef = useRef<HTMLHeadingElement>(null);
  const { data, isLoading, error } = useQuery({
    queryKey: ["providers"],
    queryFn: () => api.listProviderConnections(),
  });
  const focusHeading =
    (location.state as { focusHeading?: boolean } | null)?.focusHeading === true;

  useEffect(() => {
    if (focusHeading && !isLoading && !error) headingRef.current?.focus();
  }, [error, focusHeading, isLoading]);

  if (isLoading) return <LoadingState />;
  if (error) {
    return (
      <Card>
        <Card.Body>
          <p className="text-red-700">Could not load provider connections.</p>
        </Card.Body>
      </Card>
    );
  }

  const items = ((data?.items ?? []) as Conn[]);

  if (items.length === 0) {
    return (
      <Card>
        <Card.Body className="space-y-4">
          <h1
            ref={headingRef}
            tabIndex={-1}
            className="text-2xl font-bold text-slate-900"
          >
            Provider connections
          </h1>
          <p className="text-sm text-slate-500">
            No provider connections yet. Create one to launch evaluations against
            your own model provider.
          </p>
          <Link
            to="/providers/new"
            className="inline-block rounded-md bg-accent px-4 py-2 text-sm font-medium text-white hover:bg-accent-hover"
          >
            + New connection
          </Link>
          <CommandSnippet
            label="Hosted API quickstart"
            command={hostedProviderCommands(currentServerOrigin()).join("\n")}
            helperText="Use env: or file: secret references; do not paste raw provider keys into saved commands."
          />
        </Card.Body>
      </Card>
    );
  }

  return (
    <div className="space-y-4">
      <header className="flex items-center justify-between">
        <h1
          ref={headingRef}
          tabIndex={-1}
          className="text-2xl font-bold text-slate-900"
        >
          Provider connections
        </h1>
        <Link
          to="/providers/new"
          className="rounded-md bg-accent px-4 py-2 text-sm font-medium text-white hover:bg-accent-hover"
        >
          + New connection
        </Link>
      </header>
      <Card>
        <table className="min-w-full">
          <thead>
            <tr className="border-b border-slate-200 text-left text-xs uppercase tracking-wider text-slate-500">
              <th className="px-4 py-2">Name</th>
              <th className="px-4 py-2">Type</th>
              <th className="px-4 py-2">Status</th>
            </tr>
          </thead>
          <tbody>
            {items.map((c) => {
              const status = providerStatusSummary(c.status);
              return (
                <tr key={c.id} className="border-b border-slate-100">
                  <td className="px-4 py-3">
                    <Link to={`/providers/${c.id}`} className="text-accent hover:underline">
                      {c.name}
                    </Link>
                  </td>
                  <td className="px-4 py-3 text-sm text-slate-600">{c.type}</td>
                  <td className="px-4 py-3">
                    <StatusPill variant={status.variant} title={status.description}>
                      {status.label}
                    </StatusPill>
                    <p className="mt-1 text-xs text-slate-500">
                      {status.description}
                    </p>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </Card>
    </div>
  );
}
