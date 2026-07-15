import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { useState } from "react";
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

  it("uses unique title and description ids for concurrent dialogs", () => {
    render(
      <>
        <Modal
          open
          onClose={() => undefined}
          title="First dialog"
          description="First description"
        >
          <button type="button">First action</button>
        </Modal>
        <Modal
          open
          onClose={() => undefined}
          title="Second dialog"
          description="Second description"
        >
          <button type="button">Second action</button>
        </Modal>
      </>,
    );

    const dialogs = Array.from(
      document.querySelectorAll<HTMLElement>('[role="dialog"]'),
    );
    expect(dialogs).toHaveLength(2);
    const titleIds = dialogs.map((dialog) => dialog.getAttribute("aria-labelledby"));
    const descriptionIds = dialogs.map((dialog) =>
      dialog.getAttribute("aria-describedby"),
    );
    expect(new Set(titleIds).size).toBe(2);
    expect(new Set(descriptionIds).size).toBe(2);
    expect(titleIds.every((id) => id && document.getElementById(id))).toBe(true);
    expect(descriptionIds.every((id) => id && document.getElementById(id))).toBe(
      true,
    );
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

  it("calls onClose when the backdrop is clicked", async () => {
    const onClose = vi.fn();
    const user = userEvent.setup();
    render(
      <Modal open onClose={onClose} title="X">
        <p>body</p>
      </Modal>,
    );
    await user.click(screen.getByRole("button", { name: "close modal" }));
    expect(onClose).toHaveBeenCalledTimes(1);
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

  it("dialog has aria-modal and ids pointing at its title and description", () => {
    render(
      <Modal open onClose={() => undefined} title="X" description="Y">
        <p>body</p>
      </Modal>,
    );
    const dialog = screen.getByRole("dialog");
    expect(dialog.getAttribute("aria-modal")).toBe("true");
    const titleId = dialog.getAttribute("aria-labelledby");
    const descriptionId = dialog.getAttribute("aria-describedby");
    expect(titleId).toBeTruthy();
    expect(descriptionId).toBeTruthy();
    expect(document.getElementById(titleId ?? "")).toHaveTextContent("X");
    expect(document.getElementById(descriptionId ?? "")).toHaveTextContent("Y");
  });

  it("contains Tab focus and redirects outside focus attempts", async () => {
    const user = userEvent.setup();
    render(
      <div>
        <button data-testid="outside">outside</button>
        <Modal
          open
          onClose={() => undefined}
          title="Focus trap"
          footer={<button type="button">Last action</button>}
        >
          <input aria-label="First field" />
        </Modal>
      </div>,
    );
    const dialog = screen.getByRole("dialog");
    const close = screen.getByRole("button", { name: "close" });
    const last = screen.getByRole("button", { name: "Last action" });

    expect(screen.getByRole("textbox", { name: "First field" })).toHaveFocus();
    close.focus();
    await user.keyboard("{Shift>}{Tab}{/Shift}");
    expect(last).toHaveFocus();
    await user.tab();
    expect(close).toHaveFocus();

    screen.getByTestId("outside").focus();
    expect(dialog).toContainElement(document.activeElement as HTMLElement);
    expect(screen.getByTestId("outside")).not.toHaveFocus();
  });

  it("focuses the dialog when a non-dismissible dialog has no focusable child", () => {
    render(
      <Modal
        open
        dismissible={false}
        onClose={() => undefined}
        title="Required action"
      >
        <p>body</p>
      </Modal>,
    );
    expect(screen.getByRole("dialog")).toHaveFocus();
  });

  it("makes the background inert, locks scrolling, and restores prior state", () => {
    const previousOverflow = document.body.style.overflow;
    document.body.style.overflow = "auto";
    const { container, rerender } = render(
      <Modal open onClose={() => undefined} title="X">
        <p>body</p>
      </Modal>,
    );

    expect(container).toHaveAttribute("inert");
    expect(container).toHaveAttribute("aria-hidden", "true");
    expect(document.body.style.overflow).toBe("hidden");

    rerender(
      <Modal open={false} onClose={() => undefined} title="X">
        <p>body</p>
      </Modal>,
    );
    expect(container).not.toHaveAttribute("inert");
    expect(container).not.toHaveAttribute("aria-hidden");
    expect(document.body.style.overflow).toBe("auto");
    document.body.style.overflow = previousOverflow;
  });

  it("restores pre-existing background accessibility attributes exactly", () => {
    const host = document.createElement("div");
    host.setAttribute("inert", "");
    host.setAttribute("aria-hidden", "false");
    document.body.appendChild(host);
    const { rerender, unmount } = render(
      <Modal open onClose={() => undefined} title="X">
        <p>body</p>
      </Modal>,
      { container: host },
    );

    expect(host).toHaveAttribute("inert", "");
    expect(host).toHaveAttribute("aria-hidden", "true");
    rerender(
      <Modal open={false} onClose={() => undefined} title="X">
        <p>body</p>
      </Modal>,
    );
    expect(host).toHaveAttribute("inert", "");
    expect(host).toHaveAttribute("aria-hidden", "false");

    unmount();
    host.remove();
  });

  it("keeps direct body siblings added while open inert and restores them", async () => {
    const { rerender } = render(
      <Modal open onClose={() => undefined} title="X">
        <p>body</p>
      </Modal>,
    );
    const latePortal = document.createElement("div");
    latePortal.setAttribute("aria-hidden", "false");
    document.body.appendChild(latePortal);

    await waitFor(() => {
      expect(latePortal).toHaveAttribute("inert", "");
      expect(latePortal).toHaveAttribute("aria-hidden", "true");
    });
    rerender(
      <Modal open={false} onClose={() => undefined} title="X">
        <p>body</p>
      </Modal>,
    );
    expect(latePortal).not.toHaveAttribute("inert");
    expect(latePortal).toHaveAttribute("aria-hidden", "false");
    latePortal.remove();
  });

  it("keeps background and scroll locks until the final dialog closes", () => {
    function Harness({
      firstOpen,
      secondOpen,
    }: {
      firstOpen: boolean;
      secondOpen: boolean;
    }): JSX.Element {
      return (
        <>
          <Modal open={firstOpen} onClose={() => undefined} title="First">
            <button type="button">First action</button>
          </Modal>
          <Modal open={secondOpen} onClose={() => undefined} title="Second">
            <button type="button">Second action</button>
          </Modal>
        </>
      );
    }

    const previousOverflow = document.body.style.overflow;
    document.body.style.overflow = "scroll";
    const { container, rerender } = render(
      <Harness firstOpen secondOpen />,
    );
    expect(container).toHaveAttribute("inert");
    expect(document.body.style.overflow).toBe("hidden");
    const firstDialog = screen.getByText("First").closest('[role="dialog"]');
    expect(firstDialog?.closest('[data-loom-modal-overlay="true"]')).toHaveAttribute(
      "inert",
    );

    rerender(<Harness firstOpen secondOpen={false} />);
    expect(container).toHaveAttribute("inert");
    expect(document.body.style.overflow).toBe("hidden");
    expect(screen.getByRole("button", { name: "First action" })).toHaveFocus();

    rerender(<Harness firstOpen={false} secondOpen={false} />);
    expect(container).not.toHaveAttribute("inert");
    expect(document.body.style.overflow).toBe("scroll");
    document.body.style.overflow = previousOverflow;
  });

  it("restores focus to the control that opened the top dialog", async () => {
    function Harness(): JSX.Element {
      const [firstOpen, setFirstOpen] = useState(false);
      const [secondOpen, setSecondOpen] = useState(false);
      return (
        <div>
          <button type="button" onClick={() => setFirstOpen(true)}>
            Open first
          </button>
          <Modal
            open={firstOpen}
            onClose={() => setFirstOpen(false)}
            title="First"
          >
            <button type="button">First action</button>
            <button type="button" onClick={() => setSecondOpen(true)}>
              Open second
            </button>
          </Modal>
          <Modal
            open={secondOpen}
            onClose={() => setSecondOpen(false)}
            title="Second"
          >
            <button type="button">Second action</button>
          </Modal>
        </div>
      );
    }

    const user = userEvent.setup();
    render(<Harness />);
    await user.click(screen.getByRole("button", { name: "Open first" }));
    const secondTrigger = screen.getByRole("button", { name: "Open second" });
    await user.click(secondTrigger);
    expect(screen.getByRole("button", { name: "Second action" })).toHaveFocus();

    fireEvent.keyDown(window, { key: "Escape" });
    expect(screen.queryByRole("dialog", { name: "Second" })).not.toBeInTheDocument();
    expect(secondTrigger).toHaveFocus();
  });

  it("falls back inside the lower dialog when its opener can no longer focus", async () => {
    function Harness(): JSX.Element {
      const [firstOpen, setFirstOpen] = useState(false);
      const [secondOpen, setSecondOpen] = useState(false);
      const [secondTriggerDisabled, setSecondTriggerDisabled] = useState(false);
      return (
        <div>
          <button type="button" onClick={() => setFirstOpen(true)}>
            Open first
          </button>
          <Modal
            open={firstOpen}
            onClose={() => setFirstOpen(false)}
            title="First"
          >
            <button type="button">First fallback</button>
            <button
              type="button"
              disabled={secondTriggerDisabled}
              onClick={() => setSecondOpen(true)}
            >
              Open second
            </button>
          </Modal>
          <Modal
            open={secondOpen}
            onClose={() => setSecondOpen(false)}
            title="Second"
          >
            <button type="button" onClick={() => setSecondTriggerDisabled(true)}>
              Disable opener
            </button>
          </Modal>
        </div>
      );
    }

    const user = userEvent.setup();
    render(<Harness />);
    await user.click(screen.getByRole("button", { name: "Open first" }));
    await user.click(screen.getByRole("button", { name: "Open second" }));
    await user.click(screen.getByRole("button", { name: "Disable opener" }));
    fireEvent.keyDown(window, { key: "Escape" });

    expect(screen.getByRole("button", { name: "First fallback" })).toHaveFocus();
  });

  it("supports a non-dismissible dialog", () => {
    const onClose = vi.fn();
    render(
      <Modal open dismissible={false} onClose={onClose} title="Required action">
        <button type="button">Continue</button>
      </Modal>,
    );
    expect(screen.queryByRole("button", { name: "close" })).not.toBeInTheDocument();
    expect(
      screen.queryByRole("button", { name: "close modal" }),
    ).not.toBeInTheDocument();
    fireEvent.keyDown(window, { key: "Escape" });
    expect(onClose).not.toHaveBeenCalled();
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
