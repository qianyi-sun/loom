/**
 * Sidebar navigation. Vertical column on left of the app shell.
 * `isAdmin` hides admin-only routes from the nav for team users —
 * those routes 403 today if hit by URL, and the rate-cards read
 * policy will open up in PR B; either way the link only adds noise
 * for non-admin team members so it's hidden here.
 *
 * The `loom` wordmark is intentionally typographic, not an image —
 * keeps the bundle small and matches the rest of the type discipline.
 */
import { NavLink } from "react-router-dom";

import { cn } from "../lib/cn";

const NAV_ITEMS = [
  { to: "/", label: "Home" },
  { to: "/batches/new", label: "New batch" },
  { to: "/monitor", label: "Monitor" },
  { to: "/library", label: "Run Library" },
  { to: "/providers", label: "Providers" },
];

const TEAM_ADMIN_NAV_ITEM = { to: "/admin/access", label: "Team access" };

const PLATFORM_ADMIN_NAV_ITEMS = [
  { to: "/rate-cards", label: "Rate cards" },
];

const LINK_BASE =
  "flex shrink-0 items-center rounded-lg px-3 py-2 text-sm font-medium transition-colors";
const LINK_INACTIVE = "text-slate-600 hover:bg-slate-100 hover:text-slate-900";
const LINK_ACTIVE = "bg-accent text-white hover:bg-accent-hover";

function formatRole(role: string | null): string | null {
  return role ? role.replace(/_/g, " ") : null;
}

export interface NavBarProps {
  isAdmin: boolean;
  currentTeamName?: string | null;
  currentTeamRole?: string | null;
}

export default function NavBar({
  isAdmin,
  currentTeamName = null,
  currentTeamRole = null,
}: NavBarProps): JSX.Element {
  const canManageTeam = isAdmin || currentTeamRole === "owner";
  const displayRole = formatRole(currentTeamRole);
  return (
    <nav
      aria-label="Primary"
      className="flex w-full shrink-0 flex-col gap-3 border-b border-slate-200 bg-white px-3 py-3 lg:h-full lg:w-56 lg:flex-col lg:border-b-0 lg:border-r lg:py-5"
    >
      <div className="px-3 lg:mb-3">
        <p className="text-lg font-bold tracking-tight text-slate-900">loom</p>
        <p className="text-xs uppercase tracking-wider text-slate-400">
          benchmark platform
        </p>
      </div>

      <div className="flex min-w-0 flex-1 gap-1 overflow-x-auto pb-1 lg:flex-col lg:overflow-x-visible lg:pb-0">
        {NAV_ITEMS.map((item) => (
          <NavLink
            key={item.to}
            to={item.to}
            className={({ isActive }) =>
              cn(LINK_BASE, isActive ? LINK_ACTIVE : LINK_INACTIVE)
            }
          >
            {item.label}
          </NavLink>
        ))}
        {canManageTeam ? (
          <>
            <div className="hidden px-3 pt-3 text-xs font-semibold uppercase tracking-wider text-slate-400 lg:block">
              {isAdmin ? "Admin" : "Team"}
            </div>
            <NavLink
              to={TEAM_ADMIN_NAV_ITEM.to}
              className={({ isActive }) =>
                cn(LINK_BASE, isActive ? LINK_ACTIVE : LINK_INACTIVE)
              }
            >
              {TEAM_ADMIN_NAV_ITEM.label}
            </NavLink>
            {isAdmin ? PLATFORM_ADMIN_NAV_ITEMS.map((item) => (
              <NavLink
                key={item.to}
                to={item.to}
                className={({ isActive }) =>
                  cn(LINK_BASE, isActive ? LINK_ACTIVE : LINK_INACTIVE)
                }
              >
                {item.label}
              </NavLink>
            )) : null}
          </>
        ) : null}
      </div>

      {currentTeamName || displayRole ? (
        <div
          aria-label="Current team"
          className="rounded-lg border border-slate-200 bg-slate-50 px-3 py-2 text-sm"
        >
          <p className="text-xs font-semibold uppercase tracking-wider text-slate-400">
            Current team
          </p>
          <p className="mt-1 truncate font-medium text-slate-900">
            {currentTeamName ?? "No team selected"}
          </p>
          {displayRole ? (
            <p className="mt-0.5 text-xs capitalize text-slate-500">
              {displayRole}
            </p>
          ) : null}
        </div>
      ) : null}

      <NavLink
        to="/settings"
        className={({ isActive }) =>
          cn(
            LINK_BASE,
            "w-fit lg:w-auto",
            isActive ? LINK_ACTIVE : LINK_INACTIVE,
          )
        }
      >
        Settings
      </NavLink>
    </nav>
  );
}
