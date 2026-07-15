import {
  useEffect,
  useId,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
  type KeyboardEvent,
  type ReactNode,
} from "react";

import { cn } from "../lib/cn";

export interface TabItem<Value extends string> {
  value: Value;
  label: ReactNode;
  disabled?: boolean;
  title?: string;
}

export interface TabRenderState {
  selected: boolean;
  disabled: boolean;
}

export interface TabsProps<Value extends string> {
  /** Every item value must be unique within this tab set. */
  items: readonly TabItem<Value>[];
  value: Value;
  onValueChange: (value: Value) => void;
  ariaLabel: string;
  renderPanel: (value: Value) => ReactNode;
  activationMode?: "automatic" | "manual";
  /** Keep one panel mounted without exposing a redundant one-item tablist. */
  hideTabList?: boolean;
  className?: string;
  tabListClassName?: string;
  tabClassName?: string | ((state: TabRenderState) => string);
  panelClassName?: string;
}

const FOCUS_RING =
  "focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-accent";

function uniqueItems<Value extends string>(
  items: readonly TabItem<Value>[],
): TabItem<Value>[] {
  const seen = new Set<Value>();
  return items.filter((item) => {
    if (seen.has(item.value)) return false;
    seen.add(item.value);
    return true;
  });
}

/**
 * Controlled, horizontal ARIA tabs.
 *
 * Automatic activation is the default because Loom's panels are local and
 * render without latency. Manual activation is available for future panels
 * that need focus to move independently before Enter or Space selects them.
 */
export function Tabs<Value extends string>({
  items,
  value,
  onValueChange,
  ariaLabel,
  renderPanel,
  activationMode = "automatic",
  hideTabList = false,
  className,
  tabListClassName,
  tabClassName,
  panelClassName,
}: TabsProps<Value>): JSX.Element | null {
  const generatedId = useId().replace(/:/g, "");
  const baseId = `tabs-${generatedId}`;
  const normalizedItems = useMemo(() => uniqueItems(items), [items]);
  const enabledItems = normalizedItems.filter((item) => !item.disabled);
  const selectedItem =
    enabledItems.find((item) => item.value === value) ??
    enabledItems[0] ??
    normalizedItems.find((item) => item.value === value);
  const selectedValue = selectedItem?.value;
  const [focusValue, setFocusValue] = useState<Value>(value);
  const previousControlledValue = useRef(value);
  const tabRefs = useRef(new Map<Value, HTMLButtonElement>());
  const tabListRef = useRef<HTMLDivElement>(null);
  const focusTokenRef = useRef(0);
  const focusOwnershipRef = useRef<{ value: Value; token: number } | null>(null);
  const previousEnabledValuesRef = useRef<Set<Value> | null>(null);
  const focusFallbackValue =
    enabledItems.find((item) => item.value === selectedValue)?.value ??
    enabledItems[0]?.value;

  const focusValueIsAvailable = enabledItems.some(
    (item) => item.value === focusValue,
  );
  const rovingValue = focusValueIsAvailable ? focusValue : selectedValue;

  // An external imperative focus can run after React renders an update but
  // before this component's layout effect. The document capture listener
  // invalidates ownership even if the previously focused tab was removed
  // before its own blur event could fire.
  useEffect(() => {
    const handleDocumentFocusIn = (event: globalThis.FocusEvent): void => {
      const target = event.target;
      if (
        target instanceof Node &&
        tabListRef.current?.contains(target)
      ) {
        return;
      }
      focusTokenRef.current += 1;
      focusOwnershipRef.current = null;
    };
    document.addEventListener("focusin", handleDocumentFocusIn, true);
    return () =>
      document.removeEventListener("focusin", handleDocumentFocusIn, true);
  }, []);

  // Compare the new committed enabled set with the previous commit. Restore
  // focus only when the still-current ownership token names a tab that was
  // enabled and has just disappeared or become disabled. Selection can still
  // normalize without this branch, so outside focus is never pulled back.
  useLayoutEffect(() => {
    const currentEnabledValues = new Set(
      enabledItems.map((item) => item.value),
    );
    const previousEnabledValues = previousEnabledValuesRef.current;
    const ownership = focusOwnershipRef.current;
    const ownedTabJustInvalidated =
      ownership !== null &&
      previousEnabledValues?.has(ownership.value) === true &&
      !currentEnabledValues.has(ownership.value);

    if (ownedTabJustInvalidated) {
      const ownershipToken = ownership.token;
      const fallback =
        focusFallbackValue === undefined
          ? undefined
          : tabRefs.current.get(focusFallbackValue);
      if (
        fallback &&
        !fallback.disabled &&
        focusOwnershipRef.current?.token === ownershipToken
      ) {
        setFocusValue(focusFallbackValue);
        fallback.focus();
      } else if (focusOwnershipRef.current?.token === ownershipToken) {
        focusTokenRef.current += 1;
        focusOwnershipRef.current = null;
      }
    }

    if (hideTabList && focusOwnershipRef.current !== null) {
      focusTokenRef.current += 1;
      focusOwnershipRef.current = null;
    }
    previousEnabledValuesRef.current = currentEnabledValues;
  }, [enabledItems, focusFallbackValue, hideTabList]);

  // A controlled value can become unavailable when permissions or other
  // dynamic inputs change. Select the first enabled item instead of leaving
  // the tablist without a selected tab or rendering an unrelated panel.
  useEffect(() => {
    if (selectedValue !== undefined && selectedValue !== value) {
      onValueChange(selectedValue);
    }
  }, [onValueChange, selectedValue, value]);

  // External controlled changes return the roving tab stop to the newly
  // selected tab. A removed/disabled focused item falls back the same way.
  useEffect(() => {
    if (previousControlledValue.current !== value) {
      previousControlledValue.current = value;
      setFocusValue(selectedValue ?? value);
      return;
    }
    if (!focusValueIsAvailable && selectedValue !== undefined) {
      setFocusValue(selectedValue);
    }
  }, [focusValueIsAvailable, selectedValue, value]);

  if (normalizedItems.length === 0) return null;

  const moveFocus = (nextValue: Value): void => {
    setFocusValue(nextValue);
    tabRefs.current.get(nextValue)?.focus();
    if (activationMode === "automatic" && nextValue !== selectedValue) {
      onValueChange(nextValue);
    }
  };

  const handleKeyDown = (
    event: KeyboardEvent<HTMLButtonElement>,
    currentValue: Value,
  ): void => {
    if (enabledItems.length === 0) return;
    const currentIndex = enabledItems.findIndex(
      (item) => item.value === currentValue,
    );
    if (currentIndex < 0) return;

    let nextValue: Value | undefined;
    switch (event.key) {
      case "ArrowRight":
        nextValue = enabledItems[(currentIndex + 1) % enabledItems.length].value;
        break;
      case "ArrowLeft":
        nextValue =
          enabledItems[
            (currentIndex - 1 + enabledItems.length) % enabledItems.length
          ].value;
        break;
      case "Home":
        nextValue = enabledItems[0].value;
        break;
      case "End":
        nextValue = enabledItems[enabledItems.length - 1].value;
        break;
      case "Enter":
      case " ":
        if (activationMode === "manual") {
          event.preventDefault();
          if (currentValue !== selectedValue) onValueChange(currentValue);
        }
        return;
      default:
        return;
    }

    event.preventDefault();
    moveFocus(nextValue);
  };

  return (
    <div className={className}>
      {hideTabList ? null : (
        <div
          ref={tabListRef}
          role="tablist"
          aria-label={ariaLabel}
          aria-orientation="horizontal"
          className={tabListClassName}
          onFocusCapture={(event) => {
            const focusedTab = Array.from(tabRefs.current.entries()).find(
              ([, tabNode]) => tabNode === event.target,
            );
            if (!focusedTab) return;
            const token = focusTokenRef.current + 1;
            focusTokenRef.current = token;
            focusOwnershipRef.current = { value: focusedTab[0], token };
            setFocusValue(focusedTab[0]);
          }}
          onBlurCapture={(event) => {
            const nextTarget = event.relatedTarget;
            if (
              nextTarget instanceof Node &&
              event.currentTarget.contains(nextTarget)
            ) {
              return;
            }
            const blurredTarget = event.target;
            if (
              blurredTarget instanceof HTMLButtonElement &&
              (blurredTarget.disabled || !blurredTarget.isConnected)
            ) {
              // Some browsers emit blur while applying disabled/removal. Keep
              // the token until the layout snapshot decides whether this was
              // an invalidation; a real outside focusin still clears it.
              return;
            }
            focusTokenRef.current += 1;
            focusOwnershipRef.current = null;
          }}
        >
          {normalizedItems.map((item) => {
            const selected = item.value === selectedValue;
            const disabled = item.disabled === true;
            const tabId = `${baseId}-tab-${encodeURIComponent(item.value)}`;
            const panelId = `${baseId}-panel-${encodeURIComponent(item.value)}`;
            const itemClassName =
              typeof tabClassName === "function"
                ? tabClassName({ selected, disabled })
                : tabClassName;

            return (
              <button
                key={item.value}
                ref={(node) => {
                  if (node) tabRefs.current.set(item.value, node);
                  else tabRefs.current.delete(item.value);
                }}
                id={tabId}
                type="button"
                role="tab"
                aria-selected={selected}
                aria-controls={panelId}
                aria-disabled={disabled || undefined}
                tabIndex={disabled ? -1 : item.value === rovingValue ? 0 : -1}
                disabled={disabled}
                title={item.title}
                className={cn(
                  FOCUS_RING,
                  "disabled:cursor-not-allowed disabled:opacity-60",
                  itemClassName,
                )}
                onClick={() => {
                  setFocusValue(item.value);
                  if (item.value !== selectedValue) onValueChange(item.value);
                }}
                onKeyDown={(event) => handleKeyDown(event, item.value)}
              >
                {item.label}
              </button>
            );
          })}
        </div>
      )}

      {normalizedItems.map((item) => {
        const selected = item.value === selectedValue;
        const tabId = `${baseId}-tab-${encodeURIComponent(item.value)}`;
        const panelId = `${baseId}-panel-${encodeURIComponent(item.value)}`;
        return (
          <div
            key={item.value}
            id={hideTabList ? undefined : panelId}
            role={hideTabList ? undefined : "tabpanel"}
            aria-labelledby={hideTabList ? undefined : tabId}
            hidden={!selected}
            tabIndex={hideTabList ? undefined : selected ? 0 : -1}
            className={cn(FOCUS_RING, panelClassName)}
          >
            {selected ? renderPanel(item.value) : null}
          </div>
        );
      })}
    </div>
  );
}
