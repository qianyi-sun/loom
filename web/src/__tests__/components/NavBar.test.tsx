import { render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { afterEach, describe, expect, it } from "vitest";

import NavBar from "../../components/NavBar";
import { setFrontendConfigForTests } from "../../lib/frontendConfig";

function renderNav(
  isAdmin: boolean,
  currentTeamRole: string | null = null,
  currentTeamName: string | null = null,
  currentUsername: string | null = null,
) {
  return render(
    <MemoryRouter future={{ v7_startTransition: true, v7_relativeSplatPath: true }}>
      <NavBar
        isAdmin={isAdmin}
        currentTeamRole={currentTeamRole}
        currentTeamName={currentTeamName}
        currentUsername={currentUsername}
      />
    </MemoryRouter>,
  );
}

describe("NavBar", () => {
  afterEach(() => setFrontendConfigForTests(null));

  it("renders the team nav items", () => {
    renderNav(false);
    expect(screen.getByRole("link", { name: "Home" })).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "New batch" })).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Monitor" })).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Settings" })).toBeInTheDocument();
  });

  it("uses a responsive shell that becomes a sidebar only on large screens", () => {
    renderNav(false);
    expect(screen.getByRole("navigation", { name: "Primary" })).toHaveClass(
      "w-full",
      "lg:w-56",
      "lg:flex-col",
    );
  });

  it("shows the signed-in user, current team, and role in the globalThis navigation", () => {
    renderNav(false, "owner", "EAI", "Qianyi");

    const identityContext = screen.getByLabelText("Current user and team");
    expect(identityContext).toHaveTextContent("Signed in");
    expect(identityContext).toHaveTextContent("Qianyi / EAI");
    expect(identityContext).toHaveTextContent("owner");
  });

  it("shows a clear non-production environment identity in the globalThis navigation", () => {
    setFrontendConfigForTests({
      environment: "development",
      environmentLabel: "Development / staging",
      routePath: "/dev",
      apiBase: "/dev",
      apiRouteBase: "https://yylx.world/dev/api",
    });

    renderNav(false, "member", "EAI", "Dev User");

    const environment = screen.getByLabelText("Frontend environment");
    expect(environment).toHaveTextContent("Development / staging");
    expect(environment).toHaveTextContent("https://yylx.world/dev/api");
  });

  it("shows production identity without beta wording", () => {
    setFrontendConfigForTests({
      environment: "production",
      environmentLabel: "Production",
      routePath: "/prod",
      apiBase: "/prod",
      apiRouteBase: "https://yylx.world/prod/api",
    });

    renderNav(false, "member", "EAI", "Prod User");

    const environment = screen.getByLabelText("Frontend environment");
    expect(environment).toHaveTextContent("Production");
    expect(environment).not.toHaveTextContent(/beta/i);
  });


  it("hides admin-only links from a team user", () => {
    renderNav(false, "member");
    expect(
      screen.queryByRole("link", { name: "Rate cards" }),
    ).not.toBeInTheDocument();
    expect(
      screen.queryByRole("link", { name: "Team access" }),
    ).not.toBeInTheDocument();
    expect(screen.queryByText("Admin")).not.toBeInTheDocument();
  });

  it("shows team access to team owners without exposing platform admin tools", () => {
    renderNav(false, "owner");
    expect(
      screen.getByRole("link", { name: "Team access" }),
    ).toBeInTheDocument();
    expect(
      screen.queryByRole("link", { name: "Rate cards" }),
    ).not.toBeInTheDocument();
  });

  it("shows admin section for an admin token", () => {
    renderNav(true);
    expect(screen.getByText("Admin")).toBeInTheDocument();
    expect(
      screen.getByRole("link", { name: "Rate cards" }),
    ).toBeInTheDocument();
    expect(
      screen.getByRole("link", { name: "Team access" }),
    ).toBeInTheDocument();
  });
});
