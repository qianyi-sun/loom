import type { StatusVariant } from "../components/StatusPill";

export interface ProviderStatusSummary {
  description: string;
  label: string;
  variant: StatusVariant;
}

export interface AllowedModelsSummary {
  description: string;
  label: string;
}

export function providerStatusSummary(
  status?: string | null,
): ProviderStatusSummary {
  if (status === "valid") {
    return {
      description:
        "Last provider test passed. Loom can use this connection for model requests.",
      label: "Ready",
      variant: "success",
    };
  }
  if (status === "invalid") {
    return {
      description:
        "Last provider test failed. Batches using this connection may fail until credentials or endpoint settings are fixed.",
      label: "Needs attention",
      variant: "failed",
    };
  }
  return {
    description: "Run a connection test before trusting this provider.",
    label: "Untested",
    variant: "neutral",
  };
}

export function allowedModelsSummary(
  allowedModels?: string[] | null,
): AllowedModelsSummary {
  if (allowedModels === null || allowedModels === undefined) {
    return {
      description:
        "Any discovered model on this provider connection can be selected.",
      label: "All discovered models",
    };
  }
  if (allowedModels.length === 0) {
    return {
      description:
        "No explicit model allow-list is configured. Add model IDs before using this connection.",
      label: "No allowed models",
    };
  }
  return {
    description: "The model picker only offers the configured allow-list.",
    label: `${allowedModels.length} allowed ${
      allowedModels.length === 1 ? "model" : "models"
    }`,
  };
}
