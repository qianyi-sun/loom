import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";

import DeleteConnectionModal from "../../../components/providers/DeleteConnectionModal";

describe("DeleteConnectionModal", () => {
  it("submit is disabled until connection name typed", async () => {
    const user = userEvent.setup();
    render(
      <DeleteConnectionModal connectionName="prod-x"
        onClose={vi.fn()} onSubmit={vi.fn()} />,
    );
    const submit = screen.getByRole("button", { name: /delete/i });
    expect(submit).toBeDisabled();
    await user.type(screen.getByLabelText(/type connection name/i), "prod-x");
    expect(submit).not.toBeDisabled();
  });

  it("submit calls onSubmit", async () => {
    const onSubmit = vi.fn();
    const user = userEvent.setup();
    render(
      <DeleteConnectionModal connectionName="x"
        onClose={vi.fn()} onSubmit={onSubmit} />,
    );
    await user.type(screen.getByLabelText(/type connection name/i), "x");
    await user.click(screen.getByRole("button", { name: /delete/i }));
    expect(onSubmit).toHaveBeenCalled();
  });
});
