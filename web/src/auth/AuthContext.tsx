/** Browser-session auth state backed by `/api/v1/auth/*`. */

import { useQueryClient } from "@tanstack/react-query";
import {
  useCallback,
  useEffect,
  useMemo,
  useState,
  type ReactNode,
} from "react";

import {
  api,
  setCsrfToken,
  setUnauthorizedHandler,
  type AuthMe,
} from "../api/client";
import { AuthContext, type AuthCtx } from "./authContextValue";

function isUnauthorized(err: unknown): boolean {
  return (
    typeof err === "object" &&
    err !== null &&
    "status" in err &&
    (err as { status: number }).status === 401
  );
}

function errorDetail(err: unknown): string {
  if (typeof err === "object" && err !== null && "detail" in err) {
    const detail = (err as { detail: unknown }).detail;
    return typeof detail === "string" ? detail : JSON.stringify(detail);
  }
  return err instanceof Error ? err.message : "unknown error";
}

export function AuthProvider({ children }: { children: ReactNode }): JSX.Element {
  const queryClient = useQueryClient();
  const [me, setMe] = useState<AuthMe | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [authError, setAuthError] = useState<string | null>(null);

  const clearSessionState = useCallback((): void => {
    setCsrfToken(null);
    setMe(null);
    queryClient.clear();
  }, [queryClient]);

  const refreshMe = useCallback(async (): Promise<void> => {
    setIsLoading(true);
    try {
      const next = await api.authMe();
      setCsrfToken(next.csrf_token ?? null);
      setMe(next);
      setAuthError(null);
    } catch (err) {
      setCsrfToken(null);
      setMe(null);
      if (isUnauthorized(err)) {
        setAuthError(null);
        queryClient.clear();
      } else {
        setAuthError(`Could not load session: ${errorDetail(err)}`);
      }
    } finally {
      setIsLoading(false);
    }
  }, [queryClient]);

  useEffect(() => {
    setUnauthorizedHandler(() => {
      clearSessionState();
      setIsLoading(false);
    });
  }, [clearSessionState]);

  useEffect(() => {
    void refreshMe();
  }, [refreshMe]);

  const loginStart = useCallback(
    (email: string) => api.loginStart(email),
    [],
  );

  const loginComplete = useCallback(async (token: string): Promise<void> => {
    const next = await api.loginComplete(token);
    setCsrfToken(next.csrf_token ?? null);
    queryClient.clear();
    setMe(next);
    setAuthError(null);
  }, [queryClient]);

  const loginPassword = useCallback(async (
    username: string,
    password: string,
  ): Promise<void> => {
    const next = await api.loginPassword(username, password);
    setCsrfToken(next.csrf_token ?? null);
    queryClient.clear();
    setMe(next);
    setAuthError(null);
  }, [queryClient]);

  const acceptInvite = useCallback(
    async (code: string, email: string): Promise<AuthMe> => {
      const next = await api.acceptInvite({ code, email });
      setCsrfToken(next.csrf_token ?? null);
      queryClient.clear();
      setMe(next);
      setAuthError(null);
      return next;
    },
    [queryClient],
  );

  const switchTeam = useCallback(async (teamId: string): Promise<void> => {
    const next = await api.switchTeam(teamId);
    setCsrfToken(next.csrf_token ?? null);
    queryClient.clear();
    setMe(next);
    setAuthError(null);
  }, [queryClient]);

  const logout = useCallback(async (): Promise<void> => {
    try {
      await api.logout();
    } finally {
      clearSessionState();
      setAuthError(null);
    }
  }, [clearSessionState]);

  const value = useMemo<AuthCtx>(() => ({
    me,
    teams: me?.teams ?? [],
    currentTeamId: me?.current_team?.id ?? null,
    isAuthenticated: me !== null,
    isLoading,
    isAdmin: me?.is_platform_admin ?? false,
    authError,
    acceptInvite,
    loginStart,
    loginComplete,
    loginPassword,
    switchTeam,
    logout,
    refreshMe,
  }), [
    authError,
    acceptInvite,
    isLoading,
    loginComplete,
    loginPassword,
    loginStart,
    logout,
    me,
    refreshMe,
    switchTeam,
  ]);

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}
