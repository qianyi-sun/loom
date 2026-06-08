import { useContext } from "react";

import { AuthContext, type AuthCtx } from "./AuthContext";

export function useAuth(): AuthCtx {
  const ctx = useContext(AuthContext);
  if (!ctx) {
    throw new Error("useAuth must be used within AuthProvider");
  }
  return ctx;
}
