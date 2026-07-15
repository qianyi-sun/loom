import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { StrictMode, useLayoutEffect, useRef, useState } from "react";
import { describe, expect, it, vi } from "vitest";

import {
  Tabs,
  type TabItem,
  type TabsProps,
} from "../../components/Tabs";

type Value = "alpha" | "beta" | "gamma";

const ITEMS: readonly TabItem<Value>[] = [
  { value: "alpha", label: "Alpha" },
  { value: "beta", label: "Beta" },
  { value: "gamma", label: "Gamma" },
];

function ControlledTabs({
  items = ITEMS,
  initialValue = "alpha",
  activationMode,
  ariaLabel = "Example sections",
}: {
  items?: readonly TabItem<Value>[];
  initialValue?: Value;
  activationMode?: TabsProps<Value>["activationMode"];
  ariaLabel?: string;
}): JSX.Element {
  const [value, setValue] = useState<Value>(initialValue);
  return (
    <Tabs
      items={items}
      value={value}
      onValueChange={setValue}
      ariaLabel={ariaLabel}
      activationMode={activationMode}
      renderPanel={(activeValue) => <p>{activeValue} content</p>}
    />
  );
}

describe("Tabs", () => {
  it("owns unique tab/panel ids, selection, relationships, and one roving tab stop", () => {
    render(<ControlledTabs />);

    expect(
      screen.getByRole("tablist", { name: "Example sections" }),
    ).toHaveAttribute("aria-orientation", "horizontal");
    const tabs = screen.getAllByRole("tab");
    expect(tabs).toHaveLength(3);
    expect(tabs[0]).toHaveAttribute("aria-selected", "true");
    expect(tabs[0]).toHaveAttribute("tabindex", "0");
    expect(tabs[1]).toHaveAttribute("aria-selected", "false");
    expect(tabs[1]).toHaveAttribute("tabindex", "-1");

    const ids = new Set<string>();
    for (const tab of tabs) {
      const tabId = tab.id;
      const panelId = tab.getAttribute("aria-controls");
      expect(tabId).not.toBe("");
      expect(panelId).toBeTruthy();
      ids.add(tabId);
      ids.add(panelId ?? "");
      expect(document.getElementById(panelId ?? "missing")).toHaveAttribute(
        "aria-labelledby",
        tabId,
      );
    }
    expect(ids.size).toBe(6);

    const activePanel = screen.getByRole("tabpanel");
    expect(activePanel).toHaveTextContent("alpha content");
    expect(activePanel).toHaveAttribute("tabindex", "0");
    const inactivePanel = document.getElementById(
      tabs[1].getAttribute("aria-controls") ?? "missing",
    );
    expect(inactivePanel).toHaveAttribute("hidden");
    expect(inactivePanel).toBeEmptyDOMElement();
  });

  it("keeps generated relationships unique across multiple tab sets", () => {
    render(
      <>
        <ControlledTabs ariaLabel="First sections" />
        <ControlledTabs ariaLabel="Second sections" />
      </>,
    );

    const first = screen.getByRole("tablist", {
      name: "First sections",
    }).querySelectorAll('[role="tab"]');
    const second = screen.getByRole("tablist", {
      name: "Second sections",
    }).querySelectorAll('[role="tab"]');
    expect(first[0].id).not.toBe(second[0].id);
    expect(first[0].getAttribute("aria-controls")).not.toBe(
      second[0].getAttribute("aria-controls"),
    );
  });

  it("automatically activates on arrow, Home, and End navigation while skipping disabled tabs", async () => {
    const user = userEvent.setup();
    render(
      <ControlledTabs
        items={[
          ITEMS[0],
          { ...ITEMS[1], disabled: true },
          ITEMS[2],
        ]}
      />,
    );

    const alpha = screen.getByRole("tab", { name: "Alpha" });
    const beta = screen.getByRole("tab", { name: "Beta" });
    const gamma = screen.getByRole("tab", { name: "Gamma" });
    expect(beta).toBeDisabled();
    alpha.focus();

    await user.keyboard("{ArrowRight}");
    expect(gamma).toHaveFocus();
    expect(gamma).toHaveAttribute("aria-selected", "true");
    expect(screen.getByRole("tabpanel")).toHaveTextContent("gamma content");

    await user.keyboard("{ArrowRight}");
    expect(alpha).toHaveFocus();
    expect(alpha).toHaveAttribute("aria-selected", "true");

    await user.keyboard("{ArrowLeft}");
    expect(gamma).toHaveFocus();
    await user.keyboard("{Home}");
    expect(alpha).toHaveFocus();
    await user.keyboard("{End}");
    expect(gamma).toHaveFocus();
    expect(gamma).toHaveAttribute("tabindex", "0");
  });

  it("supports manual focus movement followed by Space or Enter activation", async () => {
    const user = userEvent.setup();
    render(<ControlledTabs activationMode="manual" />);

    const alpha = screen.getByRole("tab", { name: "Alpha" });
    const beta = screen.getByRole("tab", { name: "Beta" });
    const gamma = screen.getByRole("tab", { name: "Gamma" });
    alpha.focus();

    await user.keyboard("{ArrowRight}");
    expect(beta).toHaveFocus();
    expect(beta).toHaveAttribute("tabindex", "0");
    expect(alpha).toHaveAttribute("aria-selected", "true");
    expect(beta).toHaveAttribute("aria-selected", "false");

    await user.keyboard(" ");
    expect(beta).toHaveAttribute("aria-selected", "true");
    expect(screen.getByRole("tabpanel")).toHaveTextContent("beta content");

    await user.keyboard("{End}");
    expect(gamma).toHaveFocus();
    expect(beta).toHaveAttribute("aria-selected", "true");
    await user.keyboard("{Enter}");
    expect(gamma).toHaveAttribute("aria-selected", "true");
  });

  it("activates enabled tabs by click and ignores disabled tabs", async () => {
    const user = userEvent.setup();
    render(
      <ControlledTabs
        items={[
          ITEMS[0],
          { ...ITEMS[1], disabled: true },
          ITEMS[2],
        ]}
      />,
    );

    await user.click(screen.getByRole("tab", { name: "Beta" }));
    expect(screen.getByRole("tab", { name: "Alpha" })).toHaveAttribute(
      "aria-selected",
      "true",
    );
    await user.click(screen.getByRole("tab", { name: "Gamma" }));
    expect(screen.getByRole("tab", { name: "Gamma" })).toHaveAttribute(
      "aria-selected",
      "true",
    );
  });

  it("falls back safely when selected or focused tabs disappear or become disabled", async () => {
    const user = userEvent.setup();

    function DynamicTabs(): JSX.Element {
      const [items, setItems] = useState<readonly TabItem<Value>[]>(ITEMS);
      const [value, setValue] = useState<Value>("beta");
      return (
        <>
          <button
            type="button"
            onClick={() =>
              setItems((current) =>
                current.filter((item) => item.value !== "beta"),
              )
            }
          >
            Remove beta
          </button>
          <button
            type="button"
            onClick={() =>
              setItems((current) =>
                current.map((item) => ({ ...item, disabled: true })),
              )
            }
          >
            Disable all
          </button>
          <Tabs
            items={items}
            value={value}
            onValueChange={setValue}
            ariaLabel="Dynamic sections"
            renderPanel={(activeValue) => <p>{activeValue} content</p>}
          />
        </>
      );
    }

    render(<DynamicTabs />);
    screen.getByRole("tab", { name: "Beta" }).focus();
    const removeButton = screen.getByRole("button", { name: "Remove beta" });
    await user.click(removeButton);

    await waitFor(() => {
      expect(screen.getByRole("tab", { name: "Alpha" })).toHaveAttribute(
        "aria-selected",
        "true",
      );
    });
    expect(screen.queryByRole("tab", { name: "Beta" })).not.toBeInTheDocument();
    expect(screen.getByRole("tab", { name: "Alpha" })).toHaveAttribute(
      "tabindex",
      "0",
    );
    expect(screen.getByRole("tabpanel")).toHaveTextContent("alpha content");
    expect(removeButton).toHaveFocus();

    await user.click(screen.getByRole("button", { name: "Disable all" }));
    expect(
      screen.getAllByRole("tab").every((tab) => tab.hasAttribute("disabled")),
    ).toBe(true);
    expect(screen.getByRole("tab", { name: "Alpha" })).toHaveAttribute(
      "aria-selected",
      "true",
    );
    expect(screen.getByRole("tabpanel")).toHaveTextContent("alpha content");
  });

  it.each([
    {
      change: "removed",
      nextItems: [ITEMS[0], ITEMS[2]],
    },
    {
      change: "disabled",
      nextItems: [ITEMS[0], { ...ITEMS[1], disabled: true }, ITEMS[2]],
    },
  ])(
    "moves DOM focus to the roving fallback when the focused tab is $change",
    ({ nextItems }) => {
      const onValueChange = vi.fn();
      const renderTabs = (items: readonly TabItem<Value>[]) => (
        <StrictMode>
          <Tabs
            items={items}
            value="beta"
            onValueChange={onValueChange}
            ariaLabel="Permission sections"
            renderPanel={(activeValue) => <p>{activeValue} content</p>}
          />
        </StrictMode>
      );
      const { rerender } = render(renderTabs(ITEMS));
      screen.getByRole("tab", { name: "Beta" }).focus();
      expect(screen.getByRole("tab", { name: "Beta" })).toHaveFocus();

      rerender(renderTabs(nextItems));

      const alpha = screen.getByRole("tab", { name: "Alpha" });
      expect(alpha).toHaveAttribute("aria-selected", "true");
      expect(alpha).toHaveAttribute("tabindex", "0");
      expect(alpha).toHaveFocus();
      expect(onValueChange).toHaveBeenCalledWith("alpha");
    },
  );

  it.each([
    {
      change: "removed",
      nextItems: [ITEMS[0], ITEMS[2]],
    },
    {
      change: "disabled",
      nextItems: [ITEMS[0], { ...ITEMS[1], disabled: true }, ITEMS[2]],
    },
  ])(
    "never steals external focus when the selected tab is $change",
    ({ nextItems }) => {
      const onValueChange = vi.fn();
      const renderView = (items: readonly TabItem<Value>[]) => (
        <>
          <button type="button">Outside action</button>
          <Tabs
            items={items}
            value="beta"
            onValueChange={onValueChange}
            ariaLabel="Permission sections"
            renderPanel={(activeValue) => <p>{activeValue} content</p>}
          />
        </>
      );
      const { rerender } = render(renderView(ITEMS));
      const outside = screen.getByRole("button", { name: "Outside action" });
      outside.focus();
      expect(outside).toHaveFocus();

      rerender(renderView(nextItems));

      expect(screen.getByRole("tab", { name: "Alpha" })).toHaveAttribute(
        "aria-selected",
        "true",
      );
      expect(outside).toHaveFocus();
      expect(onValueChange).toHaveBeenCalledWith("alpha");
    },
  );

  it("invalidates focus ownership when external imperative focus runs before the recovery layout effect", () => {
    const onValueChange = vi.fn();

    function FocusDuringCommit({
      active,
      target,
    }: {
      active: boolean;
      target: { current: HTMLButtonElement | null };
    }): null {
      useLayoutEffect(() => {
        if (active) target.current?.focus();
      }, [active, target]);
      return null;
    }

    function CommitWindowView({
      items,
      moveFocusOutside,
    }: {
      items: readonly TabItem<Value>[];
      moveFocusOutside: boolean;
    }): JSX.Element {
      const outsideRef = useRef<HTMLButtonElement>(null);
      return (
        <>
          <button ref={outsideRef} type="button">
            Commit-window outside action
          </button>
          <FocusDuringCommit
            active={moveFocusOutside}
            target={outsideRef}
          />
          <Tabs
            items={items}
            value="beta"
            onValueChange={onValueChange}
            ariaLabel="Commit-window sections"
            renderPanel={(activeValue) => <p>{activeValue} content</p>}
          />
        </>
      );
    }

    const renderView = (
      items: readonly TabItem<Value>[],
      moveFocusOutside: boolean,
    ) => (
      <StrictMode>
        <CommitWindowView
          items={items}
          moveFocusOutside={moveFocusOutside}
        />
      </StrictMode>
    );
    const { rerender } = render(renderView(ITEMS, false));
    const alpha = screen.getByRole("tab", { name: "Alpha" });
    const alphaFocus = vi.spyOn(alpha, "focus");
    screen.getByRole("tab", { name: "Beta" }).focus();

    rerender(renderView([ITEMS[0], ITEMS[2]], true));

    expect(
      screen.getByRole("button", { name: "Commit-window outside action" }),
    ).toHaveFocus();
    expect(alphaFocus).not.toHaveBeenCalled();
    expect(screen.getByRole("tab", { name: "Alpha" })).toHaveAttribute(
      "aria-selected",
      "true",
    );
    expect(onValueChange).toHaveBeenCalledWith("alpha");
  });

  it("keeps single-source panel content mounted when a tablist becomes useful", async () => {
    const user = userEvent.setup();

    function GrowingTabs(): JSX.Element {
      const [showAll, setShowAll] = useState(false);
      const [value, setValue] = useState<Value>("alpha");
      const items = showAll ? ITEMS : ITEMS.slice(0, 1);
      return (
        <>
          <button type="button" onClick={() => setShowAll(true)}>
            Add sources
          </button>
          <Tabs
            items={items}
            value={value}
            onValueChange={setValue}
            ariaLabel="Growing sections"
            hideTabList={!showAll}
            renderPanel={() => <input aria-label="Panel draft" />}
          />
        </>
      );
    }

    render(<GrowingTabs />);
    expect(screen.queryByRole("tablist")).not.toBeInTheDocument();
    const draft = screen.getByRole("textbox", { name: "Panel draft" });
    await user.type(draft, "preserved");
    await user.click(screen.getByRole("button", { name: "Add sources" }));

    expect(
      screen.getByRole("tablist", { name: "Growing sections" }),
    ).toBeInTheDocument();
    expect(screen.getByRole("textbox", { name: "Panel draft" })).toBe(draft);
    expect(draft).toHaveValue("preserved");
  });

  it("deduplicates repeated values and handles an empty item list", () => {
    const { rerender } = render(
      <ControlledTabs items={[ITEMS[0], ITEMS[0], ITEMS[1]]} />,
    );
    expect(screen.getAllByRole("tab")).toHaveLength(2);

    rerender(<ControlledTabs items={[]} />);
    expect(screen.queryByRole("tablist")).not.toBeInTheDocument();
    expect(screen.queryByRole("tabpanel")).not.toBeInTheDocument();
  });
});
