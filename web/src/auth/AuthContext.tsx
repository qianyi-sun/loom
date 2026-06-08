/**
 * Token-paste auth. The bearer token is stored in `localStorage`
 * under the key `loom_token`; the AuthProvider mounts a callback on
 * the API client that clears the token on any 401 so callers are
 * automatically logged out when the service revokes their credential.
 */

import {
  createContext,
  useEffect,
  useMemo,
  useState,
  type ReactNode,
} from "react";

import { setUnauthorizedHandler } from "../api/client";

export type AuthCtx = {
  token: string | null;
  setToken: (t: string) => void;
  clearToken: () => void;
};

export const AuthContext = createContext<AuthCtx | null>(null);

const STORAGE_KEY = "loom_token";

export function AuthProvider({
  children,
}: {
  children: ReactNode;
}): JSX.Element {
  const [token, setTokenState] = useState<string | null>(() => {
    try {
      return window.localStorage.getItem(STORAGE_KEY);
    } catch {
      return null;
    }
  });

  const setToken = (t: string): void => {
    window.localStorage.setItem(STORAGE_KEY, t);
    setTokenState(t);
  };
  const clearToken = (): void => {
    window.localStorage.removeItem(STORAGE_KEY);
    setTokenState(null);
  };

  useEffect(() => {
    setUnauthorizedHandler(() => {
      clearToken();
    });
  }, []);

  const value = useMemo(
    () => ({ token, setToken, clearToken }),
    [token],
  );
  return (
    <AuthContext.Provider value={value}>{children}</AuthContext.Provider>
  );
}
