import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";

import ProviderForm from "../../../components/providers/ProviderForm";

describe("ProviderForm", () => {
  it("create mode renders api_key field", () => {
    render(<ProviderForm mode="create" onSubmit={vi.fn()} />);
    expect(screen.getByLabelText(/api key/i)).toBeInTheDocument();
  });

  it("edit mode does NOT render api_key field (rotation has its own flow)", () => {
    render(
      <ProviderForm
        mode="edit"
        initial={{ name: "x", type: "openai-compatible", base_url: "https://" }}
        onSubmit={vi.fn()}
      />,
    );
    expect(screen.queryByLabelText(/api key/i)).not.toBeInTheDocument();
  });

  it("Advanced section is collapsed by default + reveals pricing fields when expanded", async () => {
    const user = userEvent.setup();
    render(<ProviderForm mode="create" onSubmit={vi.fn()} />);
    expect(screen.queryByLabelText(/pricing source/i)).not.toBeInTheDocument();
    await user.click(screen.getByText(/advanced/i));
    expect(screen.getByLabelText(/pricing source/i)).toBeInTheDocument();
  });

  it("calls onSubmit with the form values on submit", async () => {
    const onSubmit = vi.fn();
    const user = userEvent.setup();
    render(<ProviderForm mode="create" onSubmit={onSubmit} />);
    await user.type(screen.getByLabelText(/name/i), "test-conn");
    await user.type(screen.getByLabelText(/base url/i), "https://api.x");
    await user.type(screen.getByLabelText(/api key/i), "sk-x");
    await user.click(screen.getByRole("button", { name: /create/i }));
    expect(onSubmit).toHaveBeenCalledWith(
      expect.objectContaining({
        name: "test-conn",
        base_url: "https://api.x",
        api_key: "sk-x",
      }),
    );
  });

  it("submit button is disabled while pending=true is passed", () => {
    render(<ProviderForm mode="create" onSubmit={vi.fn()} pending />);
    expect(screen.getByRole("button", { name: /create/i })).toBeDisabled();
  });
});
