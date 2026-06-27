import { useMutation, useQuery } from "@tanstack/react-query";
import { useMemo, useState } from "react";
import { Link, useLocation } from "react-router-dom";

import { api } from "../api/client";
import { Button } from "../components/Button";
import { Card } from "../components/Card";
import ErrorState from "../components/ErrorState";
import { Input } from "../components/Input";
import LoadingState from "../components/LoadingState";

type Mode = "setup" | "reset";

function useToken(): string {
  const location = useLocation();
  return useMemo(
    () => new URLSearchParams(location.search).get("token") ?? "",
    [location.search],
  );
}

export default function PasswordAction({ mode }: { mode: Mode }): JSX.Element {
  const token = useToken();
  const [password, setPassword] = useState("");
  const [confirmPassword, setConfirmPassword] = useState("");
  const isSetup = mode === "setup";
  const lookup = useQuery({
    queryKey: ["password-action", mode, token],
    queryFn: () => (isSetup ? api.setupLookup(token) : api.resetLookup(token)),
    enabled: token.length > 0,
    retry: false,
  });
  const complete = useMutation({
    mutationFn: () => (
      isSetup
        ? api.setupComplete({
          token,
          password,
          confirm_password: confirmPassword,
        })
        : api.resetComplete({
          token,
          password,
          confirm_password: confirmPassword,
        })
    ),
  });
  const disabled =
    token.length === 0 ||
    password.length === 0 ||
    confirmPassword.length === 0 ||
    complete.isPending;
  const username = lookup.data?.username ?? "";
  const teamName = "team" in (lookup.data ?? {}) ? lookup.data?.team?.name : null;

  return (
    <div className="mx-auto max-w-lg space-y-5">
      <header>
        <h1 className="text-2xl font-bold text-slate-900">
          {isSetup ? "Set Password" : "Reset Password"}
        </h1>
        <p className="mt-1 text-sm text-slate-500">
          {username ? `Account: ${username}${teamName ? ` / ${teamName}` : ""}` : "Account link"}
        </p>
      </header>
      <Card>
        <Card.Body className="space-y-4">
          {!token ? <ErrorState error={{ detail: "missing token" }} /> : null}
          {lookup.isPending && token ? <LoadingState /> : null}
          {lookup.isError ? <ErrorState error={lookup.error} /> : null}
          {lookup.data && !complete.isSuccess ? (
            <>
              <Input
                type="password"
                autoComplete="new-password"
                aria-label="Password"
                placeholder="Password"
                value={password}
                onChange={(event) => setPassword(event.target.value)}
              />
              <Input
                type="password"
                autoComplete="new-password"
                aria-label="Confirm password"
                placeholder="Confirm password"
                value={confirmPassword}
                onChange={(event) => setConfirmPassword(event.target.value)}
              />
              <Button
                variant="primary"
                className="w-full"
                disabled={disabled}
                onClick={() => complete.mutate()}
              >
                {isSetup ? "Set password" : "Reset password"}
              </Button>
            </>
          ) : null}
          {complete.isError ? <ErrorState error={complete.error} /> : null}
          {complete.isSuccess ? (
            <div className="space-y-3">
              <p className="text-sm font-medium text-emerald-700">
                Password updated for {complete.data.user.username}.
              </p>
              <Link className="text-sm font-medium text-accent hover:text-accent-hover" to="/settings">
                Sign in
              </Link>
            </div>
          ) : null}
        </Card.Body>
      </Card>
    </div>
  );
}
