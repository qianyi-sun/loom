/**
 * Settings page = login (when signed-out) + token paste + token list.
 * The page renders inside the `Layout` shell — when there's no token,
 * the layout switches into the centered card mode so this page reads
 * as a focused sign-in screen rather than a sidebar app shell.
 *
 * The Plan 21 read API only exposes list and revoke; mint goes into
 * Plan 22 (writes).
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
import LoadingState from "../components/LoadingState";
import { StatusPill } from "../components/StatusPill";

export default function Settings(): JSX.Element {
  const { token, setToken, clearToken, isAdmin } = useAuth();
  const [pasted, setPasted] = useState("");

  const queryClient = useQueryClient();
  const tokens = useQuery({
    queryKey: ["tokens"],
    queryFn: () => api.listTokens(),
    enabled: !!token,
    retry: false,
  });

  const revoke = useMutation({
    mutationFn: (prefix: string) => api.revokeToken(prefix),
    onSuccess: () =>
      queryClient.invalidateQueries({ queryKey: ["tokens"] }),
  });

  if (!token) {
    return (
      <Card>
        <Card.Body className="space-y-4">
          <div>
            <p className="text-xs uppercase tracking-wider text-slate-400">
              loom
            </p>
            <h1 className="mt-1 text-2xl font-bold text-slate-900">
              Sign in
            </h1>
            <p className="mt-2 text-sm text-slate-500">
              Paste a Loom bearer token to continue. Tokens stay in
              this browser's <code className="rounded bg-slate-100 px-1 py-0.5 font-mono text-xs">localStorage</code>;
              we never send them anywhere except in API requests.
            </p>
          </div>
          <Textarea
            placeholder="loom_team_… or loom_admin_…"
            value={pasted}
            onChange={(e) => setPasted(e.target.value)}
            rows={3}
            aria-label="bearer token"
            title="Paste a Loom team or admin bearer token."
          />
          <Button
            variant="primary"
            className="w-full"
            onClick={() => {
              const t = pasted.trim();
              if (t) setToken(t);
            }}
            disabled={!pasted.trim()}
            title="Use this token for API requests from this browser."
          >
            Sign in
          </Button>
          <p className="text-xs text-slate-400">
            Need a token? Run{" "}
            <code className="rounded bg-slate-100 px-1 py-0.5 font-mono text-xs">
              loom service up
            </code>{" "}
            — both team and dev-only admin tokens are printed at the end.
          </p>
        </Card.Body>
      </Card>
    );
  }

  return (
    <div className="space-y-6">
      <header>
        <h1 className="text-2xl font-bold text-slate-900">Settings</h1>
        <p className="mt-1 text-sm text-slate-500">
          Manage the bearer token in use by this browser session.
        </p>
      </header>

      <Card>
        <Card.Header
          title="Bearer token"
          description="Currently signed in."
          actions={
            <Button
              variant="secondary"
              onClick={clearToken}
              title="Remove this browser's saved token and return to sign in."
            >
              Sign out
            </Button>
          }
        />
        <Card.Body>
          <div className="flex flex-wrap items-center gap-3">
            <StatusPill variant={isAdmin ? "info" : "success"}>
              {isAdmin ? "Admin" : "Team"}
            </StatusPill>
            <code className="rounded bg-slate-100 px-2 py-1 font-mono text-xs text-slate-700">
              {token.slice(0, 16)}…
            </code>
          </div>
        </Card.Body>
      </Card>

      <Card>
        <Card.Header title="Active tokens" />
        <Card.Body className="p-0">
          {tokens.isPending ? <LoadingState /> : null}
          {tokens.isError ? (
            <div className="p-5">
              <ErrorState error={tokens.error} />
            </div>
          ) : null}
          {tokens.data ? (
            tokens.data.items.length === 0 ? (
              <EmptyState label="No tokens." />
            ) : (
              <div className="overflow-x-auto">
                <table className="min-w-full divide-y divide-slate-200 text-sm">
                  <thead>
                    <tr className="bg-slate-50/50">
                      <th className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-slate-500">
                        Prefix
                      </th>
                      <th className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-slate-500">
                        Type
                      </th>
                      <th className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-slate-500">
                        Scopes
                      </th>
                      <th className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-slate-500">
                        Issued
                      </th>
                      <th className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-slate-500">
                        Expires
                      </th>
                      <th className="px-4 py-3 text-left text-xs font-medium uppercase tracking-wider text-slate-500">
                        Status
                      </th>
                      <th className="px-4 py-3" />
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-slate-100">
                    {tokens.data.items.map((t) => (
                      <tr key={t.token_hash_prefix} className="hover:bg-slate-50">
                        <td className="px-4 py-3 font-mono text-xs text-slate-700">
                          {t.token_hash_prefix}
                        </td>
                        <td className="px-4 py-3 text-slate-700">{t.type}</td>
                        <td className="px-4 py-3 text-slate-600">
                          {t.scopes.join(", ")}
                        </td>
                        <td className="px-4 py-3 text-xs text-slate-500">
                          {t.issued_at.slice(0, 10)}
                        </td>
                        <td className="px-4 py-3 text-xs text-slate-500">
                          {t.expires_at?.slice(0, 10) ?? "—"}
                        </td>
                        <td className="px-4 py-3">
                          {t.revoked_at ? (
                            <StatusPill variant="cancelled">Revoked</StatusPill>
                          ) : (
                            <StatusPill variant="success">Active</StatusPill>
                          )}
                        </td>
                        <td className="px-4 py-3 text-right">
                          {!t.revoked_at ? (
                            <Button
                              size="sm"
                              variant="danger"
                              onClick={() => revoke.mutate(t.token_hash_prefix)}
                              disabled={revoke.isPending}
                              title="Revoke this token so it can no longer call the API."
                            >
                              Revoke
                            </Button>
                          ) : null}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )
          ) : null}
        </Card.Body>
      </Card>
    </div>
  );
}
