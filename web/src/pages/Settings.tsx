/**
 * Settings page = token paste + token list. The Plan 21 read API
 * only exposes list and revoke; mint goes into Plan 22 (writes).
 */

import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useState } from "react";

import { api } from "../api/client";
import { useAuth } from "../auth/useAuth";
import EmptyState from "../components/EmptyState";
import ErrorState from "../components/ErrorState";
import LoadingState from "../components/LoadingState";

export default function Settings(): JSX.Element {
  const { token, setToken, clearToken } = useAuth();
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

  return (
    <>
      <div className="loom-page-header">
        <h1>Settings</h1>
      </div>

      <div className="loom-card">
        <h2 style={{ marginTop: 0 }}>Bearer token</h2>
        {token ? (
          <>
            <p>
              You are signed in. The current token starts with{" "}
              <code className="loom-mono">{token.slice(0, 12)}…</code>
            </p>
            <button onClick={clearToken}>Sign out</button>
          </>
        ) : (
          <>
            <p className="loom-muted">
              Paste a Loom bearer token to sign in. Tokens are stored
              in this browser's <code>localStorage</code> only.
            </p>
            <textarea
              placeholder="loom_team_… or loom_admin_…"
              value={pasted}
              onChange={(e) => setPasted(e.target.value)}
              rows={3}
              style={{ width: "100%", marginBottom: "0.5rem" }}
            />
            <button
              onClick={() => {
                if (pasted.trim()) setToken(pasted.trim());
              }}
              disabled={!pasted.trim()}
            >
              Sign in
            </button>
          </>
        )}
      </div>

      {token ? (
        <div className="loom-card">
          <h2 style={{ marginTop: 0 }}>Active tokens</h2>
          {tokens.isPending ? <LoadingState /> : null}
          {tokens.isError ? <ErrorState error={tokens.error} /> : null}
          {tokens.data ? (
            tokens.data.items.length === 0 ? (
              <EmptyState label="No tokens." />
            ) : (
              <table>
                <thead>
                  <tr>
                    <th>Prefix</th>
                    <th>Type</th>
                    <th>Scopes</th>
                    <th>Issued</th>
                    <th>Expires</th>
                    <th>Revoked</th>
                    <th />
                  </tr>
                </thead>
                <tbody>
                  {tokens.data.items.map((t) => (
                    <tr key={t.token_hash_prefix}>
                      <td className="loom-mono">{t.token_hash_prefix}</td>
                      <td>{t.type}</td>
                      <td>{t.scopes.join(", ")}</td>
                      <td className="loom-muted">
                        {t.issued_at.slice(0, 10)}
                      </td>
                      <td className="loom-muted">
                        {t.expires_at?.slice(0, 10) ?? "—"}
                      </td>
                      <td>{t.revoked_at ? "yes" : "no"}</td>
                      <td>
                        {!t.revoked_at ? (
                          <button
                            onClick={() =>
                              revoke.mutate(t.token_hash_prefix)
                            }
                            disabled={revoke.isPending}
                          >
                            Revoke
                          </button>
                        ) : null}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )
          ) : null}
        </div>
      ) : null}
    </>
  );
}
