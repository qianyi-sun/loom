import {
  type AdminTeam,
  type ApiTokenEntry,
  type InviteEntry,
  type InviteReveal,
  type InviteStatus,
  type PasswordResetRequestEntry,
  type TeamRegistrationApproval,
  type TeamRegistrationEntry,
  type UserRegistrationEntry,
} from "../api";
import { formatLocalDateTime } from "../lib/dateTime";
export function formatDate(value: string | null): string {
  return formatLocalDateTime(value);
}

export function downloadInviteLink(link: string, teamName: string | null): void {
  const blob = new Blob([`${link}\n`], { type: "text/plain" });
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = `${teamName ?? "loom"}-invite-link.txt`;
  anchor.click();
  URL.revokeObjectURL(url);
}

export function statusClass(status: InviteStatus): string {
  switch (status) {
    case "pending":
      return "text-amber-700";
    case "accepted":
      return "text-emerald-700";
    case "expired":
      return "text-slate-600";
    case "revoked":
      return "text-red-700";
  }
}

export const TOKEN_SCOPE_OPTIONS = [
  {
    value: "read:own",
    label: "Read own runs",
    description: "Read this team's runs, batches, artifacts, and metadata.",
  },
  {
    value: "submit",
    label: "Submit evaluations",
    description: "Create trials and batches for the current team.",
  },
  {
    value: "providers:manage",
    label: "Manage provider connections",
    description: "Create, update, test, and remove model provider credentials.",
  },
  {
    value: "tokens:manage",
    label: "Manage API tokens",
    description: "Create, rotate, revoke, and list user-owned API tokens.",
  },
] as const;

export const TOKEN_SCOPE_LABELS: ReadonlyMap<string, string> = new Map(
  TOKEN_SCOPE_OPTIONS.map((option): [string, string] => [option.value, option.label]),
);

export function formatTokenScopes(scopes: string[]): string {
  if (scopes.length === 0) return "No scopes";
  return scopes
    .map((scope) => TOKEN_SCOPE_LABELS.get(scope) ?? scope)
    .sort()
    .join(", ");
}

export function tokenName(token: ApiTokenEntry): string {
  return token.name?.trim() || token.token_hash_prefix;
}

export function tokenStatus(token: ApiTokenEntry): { label: string; className: string } {
  if (token.revoked_at) {
    return { label: "Revoked", className: "text-red-700" };
  }
  if (token.expires_at && Date.parse(token.expires_at) <= Date.now()) {
    return { label: "Expired", className: "text-slate-600" };
  }
  return { label: "Active", className: "text-emerald-700" };
}

export function tokenLifetimeDays(value: string): number {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed < 1) return 30;
  return Math.floor(parsed);
}

export type RevealedInvite = TeamRegistrationApproval | InviteReveal;

export type RevealedAccountLink = {
  id: string;
  kind: "setup" | "reset";
  username: string;
  teamName?: string | null;
  link: string;
  tokenPrefix?: string | null;
};

export type AccessSection = "requests" | "accounts" | "teams" | "invites" | "tokens" | "audit";

export type AccessDestructiveAction =
  | { kind: "reject-user-registration"; request: UserRegistrationEntry }
  | { kind: "reject-password-reset"; request: PasswordResetRequestEntry }
  | { kind: "reject-team-registration"; request: TeamRegistrationEntry }
  | { kind: "rotate-token"; token: ApiTokenEntry }
  | { kind: "revoke-token"; token: ApiTokenEntry }
  | { kind: "revoke-invite"; invite: InviteEntry }
  | { kind: "resend-invite"; invite: InviteEntry }
  | { kind: "enable-public-registration"; team: AdminTeam }
  | { kind: "disable-public-registration"; team: AdminTeam };

export const ADMIN_ACCESS_SECTIONS: Array<{ value: AccessSection; label: string }> = [
  { value: "requests", label: "Requests" },
  { value: "accounts", label: "Accounts" },
  { value: "teams", label: "Teams" },
  { value: "invites", label: "Invites" },
  { value: "tokens", label: "API tokens" },
  { value: "audit", label: "Audit" },
];

export const TEAM_OWNER_ACCESS_SECTIONS: Array<{ value: AccessSection; label: string }> = [
  { value: "invites", label: "Invites" },
  { value: "tokens", label: "API tokens" },
];
