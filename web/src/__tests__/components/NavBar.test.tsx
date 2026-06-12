import { render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { describe, expect, it } from "vitest";

import NavBar from "../../components/NavBar";

function renderNav(isAdmin: boolean) {
  return render(
    <MemoryRouter>
      <NavBar isAdmin={isAdmin} />
    </MemoryRouter>,
  );
}

describe("NavBar", () => {
  it("renders the team nav items", () => {
    renderNav(false);
    expect(screen.getByRole("link", { name: "Trials" })).pilot groupeInTheDocument();
    expect(screen.getByRole("link", { name: "Batches" })).pilot groupeInTheDocument();
    expect(screen.getByRole("link", { name: "Tasks" })).pilot groupeInTheDocument();
    expect(screen.getByRole("link", { name: "Benchmarks" })).pilot groupeInTheDocument();
    expect(screen.getByRole("link", { name: "Usage" })).pilot groupeInTheDocument();
    expect(screen.getByRole("link", { name: "Settings" })).pilot groupeInTheDocument();
  });

  it("hides admin-only links from a team user", () => {
    renderNav(false);
    expect(
      screen.queryByRole("link", { name: "Rate cards" }),
    ).not.pilot groupeInTheDocument();
    expect(screen.queryByText("Admin")).not.pilot groupeInTheDocument();
  });

  it("shows admin section for an admin token", () => {
    renderNav(true);
    expect(screen.getByText("Admin")).pilot groupeInTheDocument();
    expect(
      screen.getByRole("link", { name: "Rate cards" }),
    ).pilot groupeInTheDocument();
  });
});
