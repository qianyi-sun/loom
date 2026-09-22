/** Pure identity preview derived from the current task and combination inputs. */

import { agentLabel } from "../../lib/agentLabel";
import { DEFAULT_AGENT_NAME, NEBIUS_BACKEND, type ComboRow, type SubsetKind } from "./formState";

function truncateText(value: string, limit: number): string {
  if (value.length <= limit) return value;
  if (limit <= 3) return value.slice(0, limit);
  return `${value.slice(0, limit - 3).trimEnd()}...`;
}

function shortToken(value: string, limit = 32): string {
  const cleaned = value.trim().replace(/\s+/g, "-").replaceAll("|", "-").replace(/^-+|-+$/g, "");
  return truncateText(cleaned || "unknown", limit);
}

function shortModelName(value: string): string {
  let raw = value.trim().replace(/^\/+|\/+$/g, "");
  if (raw.includes("/")) raw = raw.split("/").pop() ?? raw;
  if (raw.startsWith("models/")) raw = raw.slice("models/".length);
  return shortToken(raw);
}

function joinCompact(items: string[], maxItems: number, clean = true): string {
  const shown = items.slice(0, maxItems).map((item) => (clean ? shortToken(item) : item));
  const suffix = items.length > maxItems ? `+${items.length - maxItems}` : "";
  return `${shown.join("+")}${suffix}`;
}

function identityTaskPart(args: {
  sourceIds: string[];
  subsetKind: SubsetKind;
  explicitCount: number;
}): { name: string; description: string } {
  if (args.subsetKind === "explicit") {
    return {
      name: args.explicitCount > 0 ? `explicit${args.explicitCount}` : "explicit",
      description: `${args.explicitCount} explicit task id(s)`,
    };
  }
  if (args.sourceIds.length > 0) {
    return {
      name: joinCompact(args.sourceIds, 3),
      description: args.sourceIds.join(", "),
    };
  }
  return { name: "custom", description: "selected tasks" };
}

function identitySubsetPart(args: {
  subsetKind: SubsetKind;
  subsetN: string;
  subsetSeed: string;
  explicitCount: number;
}): { name: string; description: string } {
  if (args.subsetKind === "explicit") {
    return {
      name: args.explicitCount > 0 ? `explicit${args.explicitCount}` : "explicit",
      description: `${args.explicitCount} explicit task id(s)`,
    };
  }
  if (args.subsetKind === "all") return { name: "", description: "all tasks" };
  const n = Number.parseInt(args.subsetN, 10);
  const nText = Number.isFinite(n) && n > 0 ? String(n) : "";
  if (args.subsetKind === "random_n") {
    const seed = args.subsetSeed.trim();
    return {
      name: `random${nText}`,
      description: `random ${nText || "sample"}${seed ? ` seed ${seed}` : ""}`,
    };
  }
  if (args.subsetKind === "first_n") {
    return {
      name: `first${nText}`,
      description: nText ? `first ${nText}` : "first tasks",
    };
  }
  if (args.subsetKind === "last_n") {
    return {
      name: `last${nText}`,
      description: nText ? `last ${nText}` : "last tasks",
    };
  }
  const exhaustive: never = args.subsetKind;
  throw new Error(`unsupported subset kind: ${String(exhaustive)}`);
}

function identityTagPart(tagFilters: Record<string, string[]>): string {
  return Object.entries(tagFilters)
    .filter(([, values]) => values.length > 0)
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([key, values]) => `${key}=${[...values].sort().join("|")}`)
    .join(", ");
}

function identityCombinationPart(rows: ComboRow[]): { name: string; description: string } {
  const names: string[] = [];
  const descriptions: string[] = [];
  for (const row of rows) {
    const agent = agentLabel(row.picker.agentName || DEFAULT_AGENT_NAME, row.picker.agentVersion);
    const samples = Number.parseInt(row.nPerTask, 10);
    const sampleText = Number.isFinite(samples) && samples > 0 ? samples : 1;
    const label = row.label.trim();
    const hasModel = Boolean(row.picker.modelName);
    const nameBase = label
      ? shortToken(label)
      : hasModel
        ? `${shortToken(agent)}/${shortModelName(row.picker.modelName)}`
        : shortToken(agent);
    names.push(`${nameBase} x${sampleText}`);

    if (hasModel) {
      descriptions.push(
        `${agent}/${row.picker.modelProvider || "model"}/${
          shortModelName(row.picker.modelName)
        } x${sampleText}`,
      );
    } else {
      descriptions.push(`${agent}/no-model x${sampleText}`);
    }
  }
  return {
    name: joinCompact(names, 2, false),
    description: descriptions.join("; "),
  };
}

function cleanIdentitySuffix(value: string): string {
  return truncateText(value.trim().replace(/\s+/g, " ").replaceAll("|", "-"), 48);
}

export function buildIdentityPreview(args: {
  sourceIds: string[];
  tagFilters: Record<string, string[]>;
  subsetKind: SubsetKind;
  subsetN: string;
  subsetSeed: string;
  explicitCount: number;
  rows: ComboRow[];
  suffix: string;
}): { name: string; description: string } {
  const task = identityTaskPart(args);
  const subset = identitySubsetPart(args);
  const tags = identityTagPart(args.tagFilters);
  const combos = identityCombinationPart(args.rows);
  const nameParts = [task.name];
  if (subset.name && subset.name !== task.name) nameParts.push(subset.name);
  let name = `${nameParts.join(" ")} | ${combos.name}`;
  const suffix = cleanIdentitySuffix(args.suffix);
  if (suffix) name = `${name} - ${suffix}`;
  const taskDescription = tags
    ? `Tasks: ${task.description}; subset: ${subset.description}; tags: ${tags}. `
    : `Tasks: ${task.description}; subset: ${subset.description}. `;
  return {
    name: truncateText(name, 160),
    description: (
      taskDescription +
      `Combinations: ${combos.description}. Backend: ${NEBIUS_BACKEND}.`
    ),
  };
}
