/**
 * App shell. Sidebar on the left, scrollable main content area on the
 * right. Unauthenticated visitors are redirected to /settings unless
 * they are on a public onboarding route such as invite acceptance.
 */
import { Navigate, Outlet, useLocation } from "react-router-dom";

import { useAuth } from "../auth/useAuth";
import { cn } from "../lib/cn";
import NavBar from "./NavBar";

export default function Layout(): JSX.Element {
  const { isAuthenticated, isLoading, isAdmin, me } = useAuth();
  const loc = useLocation();
  const isSettings = loc.pathname.startsWith("/settings");
  const isInviteAccept = loc.pathname.startsWith("/invites/accept");
  const isPublicRoute = isSettings || isInviteAccept;

  if (isLoading) {
    return <div className="min-h-screen bg-slate-50" />;
  }

  if (!isAuthenticated && !isPublicRoute) {
    return <Navigate to="/settings" replace />;
  }

  if (!isAuthenticated) {
    return (
      <div className="min-h-screen bg-slate-50 px-4 py-8 sm:px-6 lg:px-8">
        <div
          data-testid="public-onboarding-shell"
          className={cn(
            "mx-auto w-full",
            isSettings ? "max-w-6xl" : "max-w-3xl",
          )}
        >
          <Outlet />
        </div>
      </div>
    );
  }

  return (
    <div className="flex min-h-screen flex-col bg-slate-50 lg:h-screen lg:flex-row">
      <NavBar
        isAdmin={isAdmin}
        currentTeamName={me?.current_team?.name ?? null}
        currentTeamRole={me?.current_team?.role ?? null}
      />
      <main className="min-w-0 flex-1 overflow-y-auto">
        <div className="mx-auto max-w-7xl animate-fade-in px-4 py-5 sm:px-6 lg:px-8 lg:py-8">
          <Outlet />
        </div>
      </main>
    </div>
  );
}
