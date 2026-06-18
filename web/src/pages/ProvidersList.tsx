/**
 * /providers — list of all team provider connections.
 * Empty state CTA + populated table. Closes #167 (slice 1 of 5).
 */
import { useQuery } from "@tanstack/react-query";
import { Link } from "react-router-dom";

import { api } from "../api/client";
import { Card } from "../components/Card";
import LoadingState from "../components/LoadingState";
import { StatusPill, type StatusVariant } from "../components/StatusPill";

type Conn = {
  id: string;
  name: string;
  type: string;
  status?: string;
};

function pillVariant(s?: string): StatusVariant {
  if (s === "valid") return "success";
  if (s === "invalid") return "failed";
  return "neutral";
}

export default function ProvidersList(): JSX.Element {
  const { data, isLoading, error } = useQuery({
    queryKey: ["providers"],
    queryFn: () => api.listProviderConnections(),
  });

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
        <Card.Body className="space-y-4 text-center">
          <h1 className="text-2xl font-bold text-slate-900">Provider connections</h1>
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
        </Card.Body>
      </Card>
    );
  }

  return (
    <div className="space-y-4">
      <header className="flex items-center justify-between">
        <h1 className="text-2xl font-bold text-slate-900">Provider connections</h1>
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
            {items.map((c) => (
              <tr key={c.id} className="border-b border-slate-100">
                <td className="px-4 py-3">
                  <Link to={`/providers/${c.id}`} className="text-accent hover:underline">
                    {c.name}
                  </Link>
                </td>
                <td className="px-4 py-3 text-sm text-slate-600">{c.type}</td>
                <td className="px-4 py-3">
                  <StatusPill variant={pillVariant(c.status)}>
                    {c.status ?? "untested"}
                  </StatusPill>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </Card>
    </div>
  );
}
