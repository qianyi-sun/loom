export function modelLabel(model: unknown): string {
  if (model == null) return "—";
  if (typeof model === "string") return model || "—";
  if (typeof model !== "object") return "—";

  const value = model as { provider?: unknown; name?: unknown };
  const provider = typeof value.provider === "string" ? value.provider : "";
  const name = typeof value.name === "string" ? value.name : "";

  if (provider && name) return `${provider}/${name}`;
  if (name) return name;
  return "—";
}
