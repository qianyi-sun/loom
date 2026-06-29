import { createContext } from "react";

import type { AuthMe, AuthTeam } from "../api/client";

export type AuthCtx = {
  me: AuthMe | null;
  teams: AuthTeam[];
  currentTeamId: string | null;
  isAuthenticated: boolean;
  isLoading: boolean;
  isAdmin: boolean;
  authError: string | null;
  loginStart: (email: string) => Promise<{ status: "sent"; login_token?: string }>;
  loginComplete: (token: string) => Promise<void>;
  loginPassword: (username: string, password: string) => Promise<void>;
  acceptInvite: (code: string, email: string) => Promise<AuthMe>;
  switchTeam: (teamId: string) => Promise<void>;
  logout: () => Promise<void>;
  refreshMe: () => Promise<void>;
};

export const AuthContext = createContext<AuthCtx | null>(null);
