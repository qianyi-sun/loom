/**
 * App shell. Sidebar on the left, scrollable main content area on the
 * right. Unauthenticated visitors are redirected to /settings (the
 * sign-in page); they see a centered card without the sidebar so
 * the blank state isn't confusing.
 */
import { Navigate, Outlet, useLocation } from "react-router-dom";

import { useAuth } from "../auth/useAuth";
import NavBar from "./NavBar";

export default function Layout(): JSX.Element {
  const { isAuthenticated, isLoading, isAdmin, me } = useAuth();
  const loc = useLocation();
  const isSettings = loc.pathname.startsWith("/settings");

  if (isLoading) {
    return <div className="min-h-screen bg-slate-50" />;
  }

  if (!isAuthenticated && !isSettings) {
    return <Navigate to="/settings" replace />;
  }

  if (!isAuthenticated) {
    return (
      <div className="flex min-h-screen items-center justify-center bg-slate-50 p-6">
        <div className="w-full max-w-md">
          <Outlet />
        </div>
      </div>
    );
  }

  return (
    <div className="flex h-screen bg-slate-50">
      <NavBar
        isAdmin={isAdmin}
        currentTeamRole={me?.current_team?.role ?? null}
      />
      <main className="flex-1 overflow-y-auto">
        <div className="mx-auto max-w-7xl px-8 py-8 animate-fade-in">
          <Outlet />
        </div>
      </main>
    </div>
  );
}
