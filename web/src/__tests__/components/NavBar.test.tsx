import { render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { describe, expect, it } from "vitest";

import NavBar from "../../components/NavBar";

function renderNav(isAdmin: boolean, currentTeamRole: string | null = null) {
  return render(
    <MemoryRouter>
      <NavBar isAdmin={isAdmin} currentTeamRole={currentTeamRole} />
    </MemoryRouter>,
  );
}

describe("NavBar", () => {
  it("renders the team nav items", () => {
    renderNav(false);
    expect(screen.getByRole("link", { name: "New batch" })).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Monitor" })).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Settings" })).toBeInTheDocument();
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
