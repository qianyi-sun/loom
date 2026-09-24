import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useState } from "react";
import { useSearchParams } from "react-router-dom";
import {
  api,
  type AccountActionApproval,
  type AdminTeam,
  type ApiTokenEntry,
  type ApiTokenReveal,
  type InviteEntry,
  type InviteRole,
  type InviteStatus,
  type PasswordResetRequestEntry,
  type TeamRegistrationEntry,
  type UserRegistrationEntry,
} from "../api";
import { queryKeys } from "../api/queryKeys";
import { useAuth } from "../auth/useAuth";
import {
  AccessDestructiveAction,
  AccessSection,
  ADMIN_ACCESS_SECTIONS,
  RevealedAccountLink,
  RevealedInvite,
  TEAM_OWNER_ACCESS_SECTIONS,
  tokenLifetimeDays,
  tokenName,
} from "./adminAccessState";

export function useAdminAccess() {
  const { isAdmin, isLoading, me } = useAuth();

  const [searchParams, setSearchParams] = useSearchParams();
  const section = (searchParams.get("tab") ?? "requests") as AccessSection;
  const setSection = (next: AccessSection): void => {
    const params = new URLSearchParams(searchParams);
    params.set("tab", next);
    setSearchParams(params);
  };

  const [actor, setActor] = useState("");

  const [rejectedReason, setRejectedReason] = useState<Record<string, string>>({});

  const [revealedInvites, setRevealedInvites] = useState<RevealedInvite[]>([]);

  const [revealedAccountLinks, setRevealedAccountLinks] = useState<RevealedAccountLink[]>([]);

  const [inviteStatus, setInviteStatus] = useState<InviteStatus>("pending");

  const [inviteEmail, setInviteEmail] = useState("");

  const [inviteTeamId, setInviteTeamId] = useState("");

  const [inviteRole, setInviteRole] = useState<InviteRole>("member");

  const [inviteMaxUses, setInviteMaxUses] = useState("1");

  const [inviteDomain, setInviteDomain] = useState("");

  const [tokenNameInput, setTokenNameInput] = useState("");

  const [tokenExpiresDays, setTokenExpiresDays] = useState("30");

  const [tokenScopes, setTokenScopes] = useState<string[]>(["read:own", "submit"]);

  const [revealedToken, setRevealedToken] = useState<ApiTokenReveal | null>(null);

  const [newTeamName, setNewTeamName] = useState("");

  const [teamNameEdits, setTeamNameEdits] = useState<Record<string, string>>({});

  const [approvalTeamIds, setApprovalTeamIds] = useState<Record<string, string>>({});

  const [approvalRoles, setApprovalRoles] = useState<Record<string, InviteRole>>({});

  const [accountApprovalRoles, setAccountApprovalRoles] = useState<Record<string, InviteRole>>({});

  const [destructiveAction, setDestructiveAction] = useState<AccessDestructiveAction | null>(null);

  const queryClient = useQueryClient();

  const currentRole = me?.current_team?.role ?? null;

  const canManageTeam = isAdmin || currentRole === "owner";

  const visibleSections = isAdmin ? ADMIN_ACCESS_SECTIONS : TEAM_OWNER_ACCESS_SECTIONS;

  const activeSection = visibleSections.some((item) => item.value === section)
    ? section
    : visibleSections[0].value;

  const registrations = useQuery({
    queryKey: queryKeys["admin"]("team-registrations", "pending"),
    queryFn: () => api.listTeamRegistrations("pending"),
    enabled: isAdmin && activeSection === "requests",
  });

  const adminTeams = useQuery({
    queryKey: queryKeys["admin"]("teams"),
    queryFn: () => api.listAdminTeams(),
    enabled: isAdmin,
  });

  const userRegistrationRequests = useQuery({
    queryKey: queryKeys["admin"]("user-registration-requests", "pending"),
    queryFn: () => api.listUserRegistrationRequests("pending"),
    enabled: isAdmin && (activeSection === "requests" || activeSection === "accounts"),
  });

  const passwordResetRequests = useQuery({
    queryKey: queryKeys["admin"]("password-reset-requests", "pending"),
    queryFn: () => api.listPasswordResetRequests("pending"),
    enabled: isAdmin && activeSection === "accounts",
  });

  const invites = useQuery({
    queryKey: queryKeys["invites"](inviteStatus),
    queryFn: () => api.listInvites({ status: inviteStatus }),
    enabled: canManageTeam && activeSection === "invites",
  });

  const tokens = useQuery({
    queryKey: queryKeys["api-tokens"](),
    queryFn: () => api.listTokens(),
    enabled: canManageTeam && activeSection === "tokens",
  });

  const adminTeamItems = adminTeams.data?.items ?? [];

  const selectedInviteTeamId = isAdmin ? inviteTeamId || adminTeamItems[0]?.id || "" : "";

  function revealInvite(data: RevealedInvite): void {
    setRevealedInvites((current) => [data, ...current.filter((item) => item.invite.id !== data.invite.id)]);
  }

  function revealAccountLink(data: AccountActionApproval, kind: RevealedAccountLink["kind"]): void {
    const link = kind === "setup" ? data.setup_link : data.reset_link;
    if (!link) return;
    const id = `${kind}:${data.user.id}:${link}`;
    setRevealedAccountLinks((current) => [
      {
        id,
        kind,
        username: data.user.username,
        teamName: data.team?.name ?? null,
        link,
        tokenPrefix: kind === "setup" ? data.setup_token_prefix : data.reset_token_prefix,
      },
      ...current.filter((item) => item.id !== id),
    ]);
  }

  const approve = useMutation({
    mutationFn: ({ id, teamId, role }: { id: string; teamId: string; role: InviteRole }) =>
      api.approveTeamRegistration(id, actor.trim(), {
        team_id: teamId,
        role,
      }),
    onSuccess: (data) => {
      revealInvite(data);
      queryClient.invalidateQueries({ queryKey: queryKeys["admin"]("team-registrations") });
      queryClient.invalidateQueries({ queryKey: queryKeys["admin"]("audit-events") });
      queryClient.invalidateQueries({ queryKey: queryKeys["invites"]() });
    },
  });

  const createTeam = useMutation({
    mutationFn: () => api.createAdminTeam({ name: newTeamName.trim() }, actor.trim()),
    onSuccess: () => {
      setNewTeamName("");
      queryClient.invalidateQueries({ queryKey: queryKeys["admin"]("teams") });
      queryClient.invalidateQueries({ queryKey: queryKeys["admin"]("audit-events") });
    },
  });

  const updateTeam = useMutation({
    mutationFn: ({ team, name }: { team: AdminTeam; name: string }) =>
      api.updateAdminTeam(team.id, { name: name.trim() }, actor.trim()),
    onSuccess: (team) => {
      setTeamNameEdits((current) => {
        const next = { ...current };
        delete next[team.id];
        return next;
      });
      queryClient.invalidateQueries({ queryKey: queryKeys["admin"]("teams") });
      queryClient.invalidateQueries({ queryKey: queryKeys["admin"]("audit-events") });
    },
  });

  const enablePublicRegistration = useMutation({
    mutationFn: (team: AdminTeam) => api.enableTeamPublicRegistration(team.id, actor.trim()),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: queryKeys["admin"]("teams") });
      queryClient.invalidateQueries({ queryKey: queryKeys["admin"]("audit-events") });
      queryClient.invalidateQueries({ queryKey: queryKeys["public-teams"]() });
    },
  });

  const disablePublicRegistration = useMutation({
    mutationFn: (team: AdminTeam) => api.disableTeamPublicRegistration(team.id, actor.trim()),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: queryKeys["admin"]("teams") });
      queryClient.invalidateQueries({ queryKey: queryKeys["admin"]("audit-events") });
      queryClient.invalidateQueries({ queryKey: queryKeys["public-teams"]() });
    },
  });

  const reject = useMutation({
    mutationFn: (id: string) => api.rejectTeamRegistration(id, actor.trim(), rejectedReason[id]),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: queryKeys["admin"]("team-registrations") });
      queryClient.invalidateQueries({ queryKey: queryKeys["admin"]("audit-events") });
    },
  });

  const createInvite = useMutation({
    mutationFn: () =>
      api.createInvite(
        {
          email: inviteEmail.trim(),
          team_id: isAdmin ? selectedInviteTeamId || undefined : undefined,
          role: inviteRole,
          expires_in_days: 7,
          max_uses: inviteMaxUses.trim() ? Number(inviteMaxUses) : null,
          allowed_domain: inviteDomain.trim() || null,
        },
        actor.trim() || undefined,
      ),
    onSuccess: (data) => {
      revealInvite(data);
      setInviteEmail("");
      queryClient.invalidateQueries({ queryKey: queryKeys["invites"]() });
      queryClient.invalidateQueries({ queryKey: queryKeys["admin"]("audit-events") });
    },
  });

  const revokeInvite = useMutation({
    mutationFn: (invite: InviteEntry) =>
      api.revokeInvite(invite.id, "revoked from admin access page", actor.trim()),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: queryKeys["invites"]() });
      queryClient.invalidateQueries({ queryKey: queryKeys["admin"]("audit-events") });
    },
  });

  const resendInvite = useMutation({
    mutationFn: (invite: InviteEntry) => api.resendInvite(invite.id, actor.trim()),
    onSuccess: (data) => {
      revealInvite(data);
      queryClient.invalidateQueries({ queryKey: queryKeys["invites"]() });
      queryClient.invalidateQueries({ queryKey: queryKeys["admin"]("audit-events") });
    },
  });

  const createToken = useMutation({
    mutationFn: () =>
      api.createToken(
        {
          name: tokenNameInput.trim(),
          type: "team",
          scopes: tokenScopes,
          expires_in_days: tokenLifetimeDays(tokenExpiresDays),
        },
        actor.trim() || undefined,
      ),
    onSuccess: (data) => {
      setRevealedToken(data);
      setTokenNameInput("");
      queryClient.invalidateQueries({ queryKey: queryKeys["api-tokens"]() });
      queryClient.invalidateQueries({ queryKey: queryKeys["tokens"]() });
      queryClient.invalidateQueries({ queryKey: queryKeys["admin"]("audit-events") });
    },
  });

  const rotateToken = useMutation({
    mutationFn: (token: ApiTokenEntry) => api.rotateToken(token.token_hash_prefix, actor.trim() || undefined),
    onSuccess: (data) => {
      setRevealedToken(data);
      queryClient.invalidateQueries({ queryKey: queryKeys["api-tokens"]() });
      queryClient.invalidateQueries({ queryKey: queryKeys["tokens"]() });
      queryClient.invalidateQueries({ queryKey: queryKeys["admin"]("audit-events") });
    },
  });

  const revokeToken = useMutation({
    mutationFn: (token: ApiTokenEntry) => api.revokeToken(token.token_hash_prefix, actor.trim() || undefined),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: queryKeys["api-tokens"]() });
      queryClient.invalidateQueries({ queryKey: queryKeys["tokens"]() });
      queryClient.invalidateQueries({ queryKey: queryKeys["admin"]("audit-events") });
    },
  });

  const approveUserRegistration = useMutation({
    mutationFn: (request: UserRegistrationEntry) =>
      api.approveUserRegistrationRequest(
        request.id,
        accountApprovalRoles[request.id] ?? request.role ?? "member",
      ),
    onSuccess: (data) => {
      revealAccountLink(data, "setup");
      queryClient.invalidateQueries({ queryKey: queryKeys["admin"]("user-registration-requests") });
      queryClient.invalidateQueries({ queryKey: queryKeys["admin"]("audit-events") });
    },
  });

  const rejectUserRegistration = useMutation({
    mutationFn: (request: UserRegistrationEntry) =>
      api.rejectUserRegistrationRequest(request.id, rejectedReason[request.id]),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: queryKeys["admin"]("user-registration-requests") });
      queryClient.invalidateQueries({ queryKey: queryKeys["admin"]("audit-events") });
    },
  });

  const approvePasswordReset = useMutation({
    mutationFn: (request: PasswordResetRequestEntry) => api.approvePasswordResetRequest(request.id),
    onSuccess: (data) => {
      revealAccountLink(data, "reset");
      queryClient.invalidateQueries({ queryKey: queryKeys["admin"]("password-reset-requests") });
      queryClient.invalidateQueries({ queryKey: queryKeys["admin"]("audit-events") });
    },
  });

  const rejectPasswordReset = useMutation({
    mutationFn: (request: PasswordResetRequestEntry) =>
      api.rejectPasswordResetRequest(request.id, rejectedReason[request.id]),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: queryKeys["admin"]("password-reset-requests") });
      queryClient.invalidateQueries({ queryKey: queryKeys["admin"]("audit-events") });
    },
  });

  const actorMissing = isAdmin && actor.trim().length === 0;

  const tokenCreateDisabled =
    actorMissing || tokenNameInput.trim().length === 0 || tokenScopes.length === 0 || createToken.isPending;

  const inviteCreateDisabled =
    actorMissing || !inviteEmail.trim() || (isAdmin && !selectedInviteTeamId) || createInvite.isPending;

  const latestRevealedInvite = revealedInvites[0];

  function resetDestructiveMutation(action: AccessDestructiveAction | null): void {
    switch (action?.kind) {
      case "reject-user-registration":
        rejectUserRegistration.reset();
        break;
      case "reject-password-reset":
        rejectPasswordReset.reset();
        break;
      case "reject-team-registration":
        reject.reset();
        break;
      case "rotate-token":
        rotateToken.reset();
        break;
      case "revoke-token":
        revokeToken.reset();
        break;
      case "revoke-invite":
        revokeInvite.reset();
        break;
      case "resend-invite":
        resendInvite.reset();
        break;
      case "enable-public-registration":
        enablePublicRegistration.reset();
        break;
      case "disable-public-registration":
        disablePublicRegistration.reset();
        break;
    }
  }

  function openDestructiveAction(action: AccessDestructiveAction): void {
    resetDestructiveMutation(action);
    setDestructiveAction(action);
  }

  function closeDestructiveAction(): void {
    resetDestructiveMutation(destructiveAction);
    setDestructiveAction(null);
  }

  async function confirmDestructiveAction(): Promise<void> {
    if (!destructiveAction) return;
    switch (destructiveAction.kind) {
      case "reject-user-registration":
        await rejectUserRegistration.mutateAsync(destructiveAction.request);
        break;
      case "reject-password-reset":
        await rejectPasswordReset.mutateAsync(destructiveAction.request);
        break;
      case "reject-team-registration":
        await reject.mutateAsync(destructiveAction.request.id);
        break;
      case "rotate-token":
        await rotateToken.mutateAsync(destructiveAction.token);
        break;
      case "revoke-token":
        await revokeToken.mutateAsync(destructiveAction.token);
        break;
      case "revoke-invite":
        await revokeInvite.mutateAsync(destructiveAction.invite);
        break;
      case "resend-invite":
        await resendInvite.mutateAsync(destructiveAction.invite);
        break;
      case "enable-public-registration":
        await enablePublicRegistration.mutateAsync(destructiveAction.team);
        break;
      case "disable-public-registration":
        await disablePublicRegistration.mutateAsync(destructiveAction.team);
        break;
    }
    setDestructiveAction(null);
  }

  const destructiveDialog = (() => {
    switch (destructiveAction?.kind) {
      case "reject-user-registration":
        return {
          title: "Reject account request",
          target: `${destructiveAction.request.username} (${destructiveAction.request.team_name ?? destructiveAction.request.team_id})`,
          consequence:
            "The pending account request will be rejected and no password setup link will be issued.",
          confirmLabel: "Reject account",
          pendingLabel: "Rejecting…",
          pending: rejectUserRegistration.isPending,
          error: rejectUserRegistration.error,
        };
      case "reject-password-reset":
        return {
          title: "Reject password reset",
          target: destructiveAction.request.username,
          consequence:
            "The pending password-reset request will be rejected and no reset link will be issued.",
          confirmLabel: "Reject reset",
          pendingLabel: "Rejecting…",
          pending: rejectPasswordReset.isPending,
          error: rejectPasswordReset.error,
        };
      case "reject-team-registration":
        return {
          title: "Reject team registration",
          target: `${destructiveAction.request.name} (${destructiveAction.request.contact_email})`,
          consequence: "The legacy team-registration request will be rejected and no invite will be issued.",
          confirmLabel: "Reject registration",
          pendingLabel: "Rejecting…",
          pending: reject.isPending,
          error: reject.error,
        };
      case "rotate-token":
        return {
          title: "Rotate API token",
          target: `${tokenName(destructiveAction.token)} (${destructiveAction.token.token_hash_prefix})`,
          consequence:
            "The current token will be revoked. Its replacement is shown once after server confirmation.",
          confirmLabel: "Rotate token",
          pendingLabel: "Rotating…",
          pending: rotateToken.isPending,
          error: rotateToken.error,
        };
      case "revoke-token":
        return {
          title: "Revoke API token",
          target: `${tokenName(destructiveAction.token)} (${destructiveAction.token.token_hash_prefix})`,
          consequence: "This token will no longer be able to authenticate to the Loom API.",
          confirmLabel: "Revoke token",
          pendingLabel: "Revoking…",
          pending: revokeToken.isPending,
          error: revokeToken.error,
        };
      case "revoke-invite":
        return {
          title: "Revoke invite",
          target: `${destructiveAction.invite.email} (${destructiveAction.invite.code_prefix})`,
          consequence: "The pending invite link will no longer be accepted.",
          confirmLabel: "Revoke invite",
          pendingLabel: "Revoking…",
          pending: revokeInvite.isPending,
          error: revokeInvite.error,
        };
      case "resend-invite":
        return {
          title: "Resend invite",
          target: `${destructiveAction.invite.email} (${destructiveAction.invite.code_prefix})`,
          consequence:
            "The current invite link will be invalidated. Its replacement is shown once after server confirmation.",
          confirmLabel: "Resend invite",
          pendingLabel: "Resending…",
          pending: resendInvite.isPending,
          error: resendInvite.error,
        };
      case "enable-public-registration":
        return {
          title: "Enable public registration",
          target: destructiveAction.team.name,
          consequence:
            "This team will appear on /auth/login and accept account requests until disabled again.",
          confirmLabel: "Enable public registration",
          pendingLabel: "Enabling…",
          pending: enablePublicRegistration.isPending,
          error: enablePublicRegistration.error,
        };
      case "disable-public-registration":
        return {
          title: "Disable public registration",
          target: destructiveAction.team.name,
          consequence: "This team will leave the public list and reject new account requests for its UUID.",
          confirmLabel: "Disable public registration",
          pendingLabel: "Disabling…",
          pending: disablePublicRegistration.isPending,
          error: disablePublicRegistration.error,
        };
      default:
        return {
          title: "",
          target: "",
          consequence: "",
          confirmLabel: "Confirm",
          pendingLabel: "Working…",
          pending: false,
          error: null,
        };
    }
  })();

  function userRegistrationBusy(request: UserRegistrationEntry): boolean {
    return (
      (approveUserRegistration.isPending && approveUserRegistration.variables?.id === request.id) ||
      (rejectUserRegistration.isPending && rejectUserRegistration.variables?.id === request.id)
    );
  }

  function passwordResetBusy(request: PasswordResetRequestEntry): boolean {
    return (
      (approvePasswordReset.isPending && approvePasswordReset.variables?.id === request.id) ||
      (rejectPasswordReset.isPending && rejectPasswordReset.variables?.id === request.id)
    );
  }

  function teamRegistrationBusy(request: TeamRegistrationEntry): boolean {
    return (
      (approve.isPending && approve.variables?.id === request.id) ||
      (reject.isPending && reject.variables === request.id)
    );
  }

  function tokenBusy(token: ApiTokenEntry): boolean {
    return (
      (rotateToken.isPending && rotateToken.variables?.token_hash_prefix === token.token_hash_prefix) ||
      (revokeToken.isPending && revokeToken.variables?.token_hash_prefix === token.token_hash_prefix)
    );
  }

  function inviteBusy(invite: InviteEntry): boolean {
    return (
      (revokeInvite.isPending && revokeInvite.variables?.id === invite.id) ||
      (resendInvite.isPending && resendInvite.variables?.id === invite.id)
    );
  }

  function publicRegistrationBusy(team: AdminTeam): boolean {
    return (
      (enablePublicRegistration.isPending && enablePublicRegistration.variables?.id === team.id) ||
      (disablePublicRegistration.isPending && disablePublicRegistration.variables?.id === team.id)
    );
  }

  function toggleTokenScope(scope: string, checked: boolean): void {
    setTokenScopes((current) => {
      if (checked) return current.includes(scope) ? current : [...current, scope];
      return current.filter((item) => item !== scope);
    });
  }
  return {
    isLoading,
    canManageTeam,
    userRegistrationRequests,
    accountApprovalRoles,
    setAccountApprovalRoles,
    userRegistrationBusy,
    approveUserRegistration,
    openDestructiveAction,
    isAdmin,
    actor,
    setActor,
    latestRevealedInvite,
    revealedInvites,
    revealedAccountLinks,
    revealedToken,
    visibleSections,
    activeSection,
    setSection,
    adminTeams,
    newTeamName,
    setNewTeamName,
    actorMissing,
    createTeam,
    teamNameEdits,
    publicRegistrationBusy,
    setTeamNameEdits,
    updateTeam,
    enablePublicRegistration,
    disablePublicRegistration,
    passwordResetRequests,
    passwordResetBusy,
    approvePasswordReset,
    tokens,
    tokenNameInput,
    setTokenNameInput,
    tokenExpiresDays,
    setTokenExpiresDays,
    tokenCreateDisabled,
    createToken,
    tokenScopes,
    toggleTokenScope,
    tokenBusy,
    registrations,
    approvalTeamIds,
    approvalRoles,
    setApprovalTeamIds,
    setApprovalRoles,
    teamRegistrationBusy,
    approve,
    rejectedReason,
    setRejectedReason,
    inviteEmail,
    setInviteEmail,
    selectedInviteTeamId,
    adminTeamItems,
    setInviteTeamId,
    me,
    inviteRole,
    setInviteRole,
    inviteMaxUses,
    setInviteMaxUses,
    inviteDomain,
    setInviteDomain,
    inviteCreateDisabled,
    createInvite,
    invites,
    inviteStatus,
    setInviteStatus,
    inviteBusy,
    destructiveAction,
    closeDestructiveAction,
    destructiveDialog,
    confirmDestructiveAction,
  };
}
export type AdminAccessViewState = ReturnType<typeof useAdminAccess>;
