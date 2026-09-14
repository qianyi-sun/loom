/** Include the selected version wherever agent identities are compared. */
export function agentLabel(name: unknown, version?: unknown): string {
  const agent = typeof name === "string" && name ? name : "—";
  return typeof version === "string" && version ? `${agent}@${version}` : agent;
}
