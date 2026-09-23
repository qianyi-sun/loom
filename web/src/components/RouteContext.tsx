import { useEffect } from "react";
import { Link, useLocation } from "react-router-dom";

const sections: Record<string, string> = {
  monitor: "Monitor",
  library: "Run Library",
  providers: "Providers",
  "task-sets": "Task sets",
  settings: "Settings",
  admin: "Team access",
  pipelines: "Pipelines",
  batches: "Batch",
  trials: "Trial",
  tasks: "Tasks",
  benchmarks: "Benchmarks",
  usage: "Usage",
  "rate-cards": "Rate cards",
  auth: "Sign in",
  invites: "Accept invitation",
};

export function RouteContext(): JSX.Element | null {
  const { pathname, search } = useLocation();
  const [section, detail] = pathname.split("/").filter(Boolean);
  const parent = sections[section] ?? "Page not found";
  let title = section ? parent : "Home";
  if (section === "batches" && detail === "new") title = "New batch";
  else if (section === "auth")
    title = detail === "setup" ? "Set up account" : detail === "reset" ? "Reset password" : "Sign in";
  else if (section === "pipelines" && pathname.includes("/artifacts/")) title = "Pipeline artifact";
  else if (section === "monitor") {
    const view = new URLSearchParams(search).get("view");
    title = `Monitor · ${view === "trials" ? "Trials" : view === "resources" ? "Resources" : "Batches"}`;
  } else if (detail && section !== "admin" && section !== "invites") {
    title = `${parent} · ${detail === "new" ? "New" : "Details"}`;
  }
  useEffect(() => {
    document.title = `${title} · Loom`;
  }, [title]);
  if (!section || section === "auth" || section === "invites") return null;
  const hasParent = Boolean(detail) && ["library", "providers", "task-sets", "pipelines"].includes(section);
  return (
    <nav aria-label="Breadcrumb" className="mb-4 text-sm text-slate-600">
      <ol className="flex flex-wrap items-center gap-2">
        <li>
          <Link className="inline-block rounded px-1 py-1 text-accent" to="/">
            Home
          </Link>
        </li>
        {hasParent && (
          <li>
            <span aria-hidden="true">/ </span>
            <Link className="inline-block rounded px-1 py-1 text-accent" to={`/${section}`}>
              {parent}
            </Link>
          </li>
        )}
        <li aria-current="page">
          <span aria-hidden="true">/ </span>
          {title}
        </li>
      </ol>
    </nav>
  );
}
