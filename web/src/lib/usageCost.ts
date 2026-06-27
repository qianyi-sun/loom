export interface UsageCostLike {
  total_cost_usd?: number | null;
  estimated_cost_usd?: number | null;
  cost_currency?: string | null;
  cost_status?: string | null;
  pricing_modes?: string[] | null;
  usage_reporting_status?: string | null;
  usage_estimate_confidence?: string | null;
  partial_usage_llm_calls_count?: number | null;
  missing_usage_llm_calls_count?: number | null;
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

export function usageEstimateConfidence(item: UsageCostLike): string {
  if (
    typeof item.usage_estimate_confidence === "string"
    && item.usage_estimate_confidence
  ) {
    return item.usage_estimate_confidence;
  }
  if (
    typeof item.usage_reporting_status === "string"
    && item.usage_reporting_status
  ) {
    return item.usage_reporting_status;
  }
  return "unknown";
}

export function summarizeUsageCost(items: UsageCostLike[]): UsageCostLike {
  let estimatedCost = 0;
  let hasEstimatedCost = false;
  const statuses = new Set<string>();
  const pricingModes = new Set<string>();
  const confidences = new Set<string>();
  let partialUsageCalls = 0;
  let missingUsageCalls = 0;

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
    const confidence = item.usage_estimate_confidence;
    if (typeof confidence === "string" && confidence) {
      confidences.add(confidence);
    }
    partialUsageCalls += item.partial_usage_llm_calls_count ?? 0;
    missingUsageCalls += item.missing_usage_llm_calls_count ?? 0;
  }

  let costStatus = "unknown";
  if (statuses.size === 1) {
    costStatus = [...statuses][0];
  } else if (statuses.size > 1) {
    costStatus = "mixed";
  } else if (hasEstimatedCost) {
    costStatus = "estimated";
  }

  let usageConfidence = "unknown";
  if (confidences.has("missing")) {
    usageConfidence = "missing";
  } else if (confidences.has("partial") || confidences.size > 1) {
    usageConfidence = "partial";
  } else if (confidences.size === 1) {
    usageConfidence = [...confidences][0];
  }

  return {
    estimated_cost_usd: hasEstimatedCost ? estimatedCost : null,
    cost_currency: hasEstimatedCost ? "USD" : null,
    cost_status: costStatus,
    pricing_modes: [...pricingModes],
    usage_estimate_confidence: usageConfidence,
    partial_usage_llm_calls_count: partialUsageCalls,
    missing_usage_llm_calls_count: missingUsageCalls,
  };
}
