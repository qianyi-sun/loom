/**
 * Token-paste auth.
 *
 * - The bearer token is stored in `localStorage` under `loom_token`.
 * - On `setToken(t)`: clear React Query cache (no stale data from a
 *   previous team leaks into the new session) AND kick off a probe
 *   against the authenticated `/api/v1/tokens` endpoint. On 401, the
 *   global unauthorized handler clears the token and sets `tokenError`
 *   so the sign-in form can surface an inline message. On any other
 *   error, set `tokenError` to a short descriptive string.
 * - `clearToken()` also clears the cache so post-sign-out state is
 *   clean for the next sign-in.
 * - `tokenError` is cleared automatically on a successful sign-in or
 *   when the user explicitly retypes a token.
 * - `isAdmin` is derived from the token prefix; the backend enforces
 *   scopes on every request — the client check is convenience, not
 *   security.
 */

import { useQueryClient } from "@tanstack/react-query";
import {
  createContext,
  useEffect,
  useMemo,
  useRef,
  useState,
  type ReactNode,
} from "react";

import { apiFetch, setUnauthorizedHandler } from "../api/client";

export type AuthCtx = {
  token: string | null;
  isAdmin: boolean;
  /** Populated when the most recent sign-in probe failed; null otherwise. */
  tokenError: string | null;
  setToken: (t: string) => void;
  clearToken: () => void;
};

export const AuthContext = createContext<AuthCtx | null>(null);

const STORAGE_KEY = "loom_token";

export function isAdminToken(token: string | null): boolean {
  return typeof token === "string" && token.startsWith("loom_admin_");
}

export function AuthProvider({
  children,
}: {
  children: ReactNode;
}): JSX.Element {
  const queryClient = useQueryClient();
  const [token, setTokenState] = useState<string | null>(() => {
    try {
      return window.localStorage.getItem(STORAGE_KEY);
    } catch {
      return null;
    }
  });
  const [tokenError, setTokenError] = useState<string | null>(null);
  // Track whether the latest setToken initiated a probe; used by the
  // unauthorized-handler callback to know it should set tokenError
  // (vs background-request 401s, which just sign the user out silently).
  const probeInFlight = useRef(false);

  const clearToken = (): void => {
    window.localStorage.removeItem(STORAGE_KEY);
    setTokenState(null);
    setTokenError(null);
    queryClient.clear();
  };

  const setToken = (t: string): void => {
    setTokenError(null);
    queryClient.clear();
    window.localStorage.setItem(STORAGE_KEY, t);
    setTokenState(t);
    probeInFlight.current = true;
    // Fire-and-forget probe. apiFetch routes 401 through the global
    // unauthorized handler (set in the useEffect below), which calls
    // clearToken + sets tokenError. Other errors are caught here so
    // a network blip becomes a visible inline message.
    apiFetch<unknown>("/api/v1/tokens")
      .then(() => {
        probeInFlight.current = false;
      })
      .catch((err) => {
        probeInFlight.current = false;
        const status =
          typeof err === "object" && err !== null && "status" in err
            ? (err as { status: number }).status
            : 0;
        if (status === 401) {
          // unauthorized handler already cleared token + set tokenError
          return;
        }
        const detail =
          typeof err === "object" && err !== null && "detail" in err
            ? (err as { detail: string }).detail
            : "unknown error";
        setTokenError(`Could not verify token: ${detail}`);
      });
  };

  useEffect(() => {
    setUnauthorizedHandler(() => {
      const wasProbe = probeInFlight.current;
      probeInFlight.current = false;
      window.localStorage.removeItem(STORAGE_KEY);
      setTokenState(null);
      queryClient.clear();
      if (wasProbe) {
        setTokenError("Token is invalid or has been revoked.");
      } else {
        setTokenError(null);
      }
    });
  }, [queryClient]);

  const value = useMemo(
    () => ({
      token,
      isAdmin: isAdminToken(token),
      tokenError,
      setToken,
      clearToken,
    }),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [token, tokenError],
  );
  return (
    <AuthContext.Provider value={value}>{children}</AuthContext.Provider>
  );
}
