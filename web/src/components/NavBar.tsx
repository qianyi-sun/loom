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
  { to: "/trials", label: "Trials" },
  { to: "/campaigns", label: "Campaigns" },
  { to: "/tasks", label: "Tasks" },
  { to: "/benchmarks", label: "Benchmarks" },
  { to: "/usage", label: "Usage" },
];

const ADMIN_NAV_ITEMS = [
  { to: "/rate-cards", label: "Rate cards" },
];

const LINK_BASE =
  "flex items-center rounded-lg px-3 py-2 text-sm font-medium transition-colors";
const LINK_INACTIVE = "text-slate-600 hover:bg-slate-100 hover:text-slate-900";
const LINK_ACTIVE = "bg-accent text-white hover:bg-accent-hover";

export interface NavBarProps {
  isAdmin: boolean;
}

export default function NavBar({ isAdmin }: NavBarProps): JSX.Element {
  return (
    <nav
      aria-label="Primary"
      className="flex h-full w-56 shrink-0 flex-col border-r border-slate-200 bg-white px-3 py-5"
    >
      <div className="mb-6 px-3">
        <p className="text-lg font-bold tracking-tight text-slate-900">loom</p>
        <p className="text-xs uppercase tracking-wider text-slate-400">
          benchmark platform
        </p>
      </div>

      <div className="flex flex-1 flex-col gap-1">
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
        {isAdmin ? (
          <>
            <div className="mt-3 px-3 text-xs font-semibold uppercase tracking-wider text-slate-400">
              Admin
            </div>
            {ADMIN_NAV_ITEMS.map((item) => (
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
          </>
        ) : null}
      </div>

      <NavLink
        to="/settings"
        className={({ isActive }) =>
          cn(LINK_BASE, isActive ? LINK_ACTIVE : LINK_INACTIVE)
        }
      >
        Settings
      </NavLink>
    </nav>
  );
}
