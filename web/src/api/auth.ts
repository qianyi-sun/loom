import { type InviteLookup, type UserRegistrationEntry } from "./admin";
import { AuthSessionLoadError, apiBase, apiFetch, authHeaders, qs } from "./core";
import { type Team } from "./runs";

export interface AuthTeam {
  id: string;
  name: string;
  role: string;
}

export interface AuthMe {
  user: {
    id: string;
    username: string;
    email?: string | null;
    display_name: string | null;
    is_platform_admin: boolean;
  };
  teams: AuthTeam[];
  current_team: AuthTeam | null;
  role: string | null;
  scopes: string[];
  is_platform_admin: boolean;
  csrf_token: string;
}

export function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

export function isNonEmptyString(value: unknown): value is string {
  return typeof value === "string" && value.trim().length > 0;
}

export function parseAuthTeam(value: unknown): AuthTeam | null {
  if (!isRecord(value)) return null;
  if (!isNonEmptyString(value.id) || !isNonEmptyString(value.name) || !isNonEmptyString(value.role)) {
    return null;
  }
  return { id: value.id, name: value.name, role: value.role };
}

export function parseAuthMe(value: unknown): AuthMe {
  if (
    !isRecord(value) ||
    !isRecord(value.user) ||
    !Array.isArray(value.teams) ||
    !Array.isArray(value.scopes)
  ) {
    throw new AuthSessionLoadError("invalid");
  }
  const user = value.user;
  const teams = value.teams.map(parseAuthTeam);
  const currentTeam = value.current_team === null ? null : parseAuthTeam(value.current_team);
  if (
    !isNonEmptyString(user.id) ||
    !isNonEmptyString(user.username) ||
    !(typeof user.email === "string" || user.email === null) ||
    !(typeof user.display_name === "string" || user.display_name === null) ||
    typeof user.is_platform_admin !== "boolean" ||
    teams.some((team) => team === null) ||
    (value.current_team !== null && currentTeam === null) ||
    value.scopes.some((scope) => !isNonEmptyString(scope)) ||
    typeof value.is_platform_admin !== "boolean" ||
    value.is_platform_admin !== user.is_platform_admin ||
    (value.role !== null && !isNonEmptyString(value.role)) ||
    !isNonEmptyString(value.csrf_token)
  ) {
    throw new AuthSessionLoadError("invalid");
  }
  const parsedTeams = teams as AuthTeam[];
  if (
    currentTeam !== null &&
    !parsedTeams.some(
      (team) =>
        team.id === currentTeam.id && team.name === currentTeam.name && team.role === currentTeam.role,
    )
  ) {
    throw new AuthSessionLoadError("invalid");
  }

  return {
    user: {
      id: user.id,
      username: user.username,
      email: user.email,
      display_name: user.display_name,
      is_platform_admin: user.is_platform_admin,
    },
    teams: parsedTeams,
    current_team: currentTeam,
    role: value.role as string | null,
    scopes: value.scopes as string[],
    is_platform_admin: value.is_platform_admin,
    csrf_token: value.csrf_token,
  };
}

export async function parseAuthSessionResponse(response: Response): Promise<AuthMe> {
  if (response.status === 401) {
    throw new AuthSessionLoadError("unauthorized");
  }
  if (!response.ok) {
    // Session-producing responses can contain proxy diagnostics or echoed
    // request data. Classification never requires consuming their body.
    throw new AuthSessionLoadError("http");
  }
  if (response.status === 204) {
    throw new AuthSessionLoadError("invalid");
  }

  try {
    return parseAuthMe(await response.json());
  } catch (error) {
    if (error instanceof AuthSessionLoadError) throw error;
    throw new AuthSessionLoadError("invalid");
  }
}

export async function loadAuthSession(): Promise<AuthMe> {
  let response: Response;
  try {
    response = await fetch(`${apiBase()}/api/v1/auth/me`, {
      credentials: "include",
      headers: { Accept: "application/json" },
    });
  } catch {
    throw new AuthSessionLoadError("network");
  }

  return parseAuthSessionResponse(response);
}

export async function mutateAuthSession(path: string, body: unknown): Promise<AuthMe> {
  let response: Response;
  try {
    response = await fetch(`${apiBase()}${path}`, {
      method: "POST",
      body: JSON.stringify(body),
      // Session endpoints return JSON at their exact origin. A 307/308 must
      // never forward passwords, invitation tokens or one-use managed proofs.
      redirect: "error",
      credentials: "include",
      headers: {
        "Content-Type": "application/json",
        Accept: "application/json",
        ...authHeaders(undefined, "POST"),
      },
    });
  } catch {
    throw new AuthSessionLoadError("network");
  }

  return parseAuthSessionResponse(response);
}

export interface PublicTeam {
  id: string;
  name: string;
}

export const authApi = {
  authMe: loadAuthSession,
  publicTeams: () => apiFetch<{ items: PublicTeam[] }>("/api/v1/auth/public-teams"),
  loginPassword: (username: string, password: string) =>
    mutateAuthSession("/api/v1/auth/login", { username, password }),
  requestRegistration: (body: { username: string; team_id: string }) =>
    apiFetch<UserRegistrationEntry>("/api/v1/auth/registration-requests", {
      method: "POST",
      body: JSON.stringify({ ...body, metadata: {} }),
    }),
  setupLookup: (token: string) =>
    apiFetch<{ username: string; team: PublicTeam | null; expires_at: string }>(
      `/api/v1/auth/setup/lookup${qs({ token })}`,
    ),
  setupComplete: (body: { token: string; password: string; confirm_password: string }) =>
    apiFetch<{ status: string; user: { id: string; username: string } }>("/api/v1/auth/setup/complete", {
      method: "POST",
      body: JSON.stringify(body),
    }),
  requestPasswordReset: (username: string) =>
    apiFetch<{ status: "pending" }>("/api/v1/auth/password-reset-requests", {
      method: "POST",
      body: JSON.stringify({ username }),
    }),
  resetLookup: (token: string) =>
    apiFetch<{ username: string; expires_at: string }>(`/api/v1/auth/reset/lookup${qs({ token })}`),
  resetComplete: (body: { token: string; password: string; confirm_password: string }) =>
    apiFetch<{ status: string; user: { id: string; username: string } }>("/api/v1/auth/reset/complete", {
      method: "POST",
      body: JSON.stringify(body),
    }),
  loginStart: (email: string) =>
    apiFetch<{ status: "sent"; login_token?: string }>("/api/v1/auth/login/start", {
      method: "POST",
      body: JSON.stringify({ email }),
    }),
  loginComplete: (token: string) => mutateAuthSession("/api/v1/auth/login/complete", { token }),
  lookupInvite: (code: string) => apiFetch<InviteLookup>(`/api/v1/invites/lookup${qs({ code })}`),
  acceptInvite: (body: { code: string; email?: string | null }) =>
    mutateAuthSession("/api/v1/invites/accept", body),
  switchTeam: (teamId: string) => mutateAuthSession("/api/v1/auth/team", { team_id: teamId }),
  logout: () => apiFetch<void>("/api/v1/auth/logout", { method: "POST" }),
  getTeam: (teamId: string) => apiFetch<Team>(`/api/v1/teams/${teamId}`),
};
