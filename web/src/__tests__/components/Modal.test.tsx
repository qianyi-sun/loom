import { fireEvent, render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";

import { Modal } from "../../components/Modal";

describe("Modal", () => {
  it("renders nothing when open is false", () => {
    const { container } = render(
      <Modal open={false} onClose={() => undefined} title="X">
        <p>body</p>
      </Modal>,
    );
    expect(container.querySelector('[role="dialog"]')).toBeNull();
  });

  it("renders title + description + body when open", () => {
    render(
      <Modal
        open
        onClose={() => undefined}
        title="Submit trial"
        description="Run this task once."
      >
        <p>body content</p>
      </Modal>,
    );
    expect(screen.getByRole("dialog")).toBeInTheDocument();
    expect(screen.getByText("Submit trial")).toBeInTheDocument();
    expect(screen.getByText("Run this task once.")).toBeInTheDocument();
    expect(screen.getByText("body content")).toBeInTheDocument();
  });

  it("calls onClose on Escape", async () => {
    const onClose = vi.fn();
    render(
      <Modal open onClose={onClose} title="X">
        <p>body</p>
      </Modal>,
    );
    fireEvent.keyDown(window, { key: "Escape" });
    expect(onClose).toHaveBeenCalledTimes(1);
  });

  it("calls onClose when the close button is clicked", async () => {
    const onClose = vi.fn();
    const user = userEvent.setup();
    render(
      <Modal open onClose={onClose} title="X">
        <p>body</p>
      </Modal>,
    );
    await user.click(screen.getByRole("button", { name: "close" }));
    expect(onClose).toHaveBeenCalled();
  });

  it("explains both modal close targets on hover", async () => {
    render(
      <Modal open onClose={() => undefined} title="X">
        <p>body</p>
      </Modal>,
    );

    expect(screen.getByRole("button", { name: "close modal" })).toHaveAttribute(
      "title",
      "Close this dialog without applying additional changes.",
    );
    expect(screen.getByRole("button", { name: "close" })).toHaveAttribute(
      "title",
      "Close this dialog without applying additional changes.",
    );
  });

  it("dialog has aria-modal and aria-labelledby pointing at the title", () => {
    render(
      <Modal open onClose={() => undefined} title="X" description="Y">
        <p>body</p>
      </Modal>,
    );
    const dialog = screen.getByRole("dialog");
    expect(dialog.getAttribute("aria-modal")).toBe("true");
    expect(dialog.getAttribute("aria-labelledby")).toBe("modal-title");
    expect(dialog.getAttribute("aria-describedby")).toBe("modal-description");
  });

  it("restores focus to the previously focused element on close", async () => {
    const onClose = vi.fn();
    function Harness({ open }: { open: boolean }): JSX.Element {
      return (
        <div>
          <button data-testid="trigger">trigger</button>
          <Modal open={open} onClose={onClose} title="X">
            <input data-testid="modal-input" />
          </Modal>
        </div>
      );
    }
    const { rerender } = render(<Harness open={false} />);
    const trigger = screen.getByTestId("trigger");
    trigger.focus();
    expect(document.activeElement).toBe(trigger);
    rerender(<Harness open={true} />);
    // Modal first focusable should be the input.
    expect(document.activeElement).toBe(screen.getByTestId("modal-input"));
    rerender(<Harness open={false} />);
    expect(document.activeElement).toBe(trigger);
  });
});
