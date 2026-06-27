export interface UsageCostLike {
  total_cost_usd?: number | null;
  estimated_cost_usd?: number | null;
  cost_currency?: string | null;
  cost_status?: string | null;
  pricing_modes?: string[] | null;
}

function hasKey<T extends object>(item: T, key: PropertyKey): boolean {
  return Object.prototype.hasOwnProperty.call(item, key);
}

export function usageCostAmount(item: UsageCostLike): number | null {
  if (hasKey(item, "estimated_cost_usd")) {
    return typeof item.estimated_cost_usd === "number"
      ? item.estimated_cost_usd
      : null;
  }
  return typeof item.total_cost_usd === "number" ? item.total_cost_usd : null;
}

export function formatUsageCost(item: UsageCostLike): string {
  const value = usageCostAmount(item);
  if (value == null) return "n/a";
  const currency = item.cost_currency ?? "USD";
  if (currency === "USD") return `$${value.toFixed(4)}`;
  return `${value.toFixed(4)} ${currency}`;
}

export function usageCostStatus(item: UsageCostLike): string {
  if (typeof item.cost_status === "string" && item.cost_status) {
    return item.cost_status;
  }
  return usageCostAmount(item) != null ? "estimated" : "unknown";
}

export function summarizeUsageCost(items: UsageCostLike[]): UsageCostLike {
  let estimatedCost = 0;
  let hasEstimatedCost = false;
  const statuses = new Set<string>();
  const pricingModes = new Set<string>();

  for (const item of items) {
    const amount = usageCostAmount(item);
    if (amount != null) {
      estimatedCost += amount;
      hasEstimatedCost = true;
    }
    const status = item.cost_status;
    if (typeof status === "string" && status) statuses.add(status);
    for (const mode of item.pricing_modes ?? []) {
      pricingModes.add(mode);
    }
  }

  let costStatus = "unknown";
  if (statuses.size === 1) {
    costStatus = [...statuses][0];
  } else if (statuses.size > 1) {
    costStatus = "mixed";
  } else if (hasEstimatedCost) {
    costStatus = "estimated";
  }

  return {
    estimated_cost_usd: hasEstimatedCost ? estimatedCost : null,
    cost_currency: hasEstimatedCost ? "USD" : null,
    cost_status: costStatus,
    pricing_modes: [...pricingModes],
  };
}
