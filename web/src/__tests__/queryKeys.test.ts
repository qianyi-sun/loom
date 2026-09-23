import { QueryClient } from "@tanstack/react-query";
import { describe, expect, it } from "vitest";
import { queryKeys } from "../api/queryKeys";

describe("domain cache invalidation", () => {
  it("invalidates all pages of one collection without invalidating another domain", async () => {
    const client = new QueryClient();
    const pending = queryKeys.admin("team-registrations", "pending");
    const history = queryKeys.admin("team-registrations", "approved");
    const teams = queryKeys.admin("teams");
    for (const key of [pending, history, teams]) client.setQueryData(key, []);
    await client.invalidateQueries({ queryKey: queryKeys.admin("team-registrations") });
    expect(client.getQueryState(pending)?.isInvalidated).toBe(true);
    expect(client.getQueryState(history)?.isInvalidated).toBe(true);
    expect(client.getQueryState(teams)?.isInvalidated).toBe(false);
  });
  it("keeps provider caches isolated by active team", () => {
    const client = new QueryClient();
    client.setQueryData(queryKeys["provider-connections"]("team-a"), ["a"]);
    client.setQueryData(queryKeys["provider-connections"]("team-b"), ["b"]);
    expect(client.getQueryData(queryKeys["provider-connections"]("team-a"))).toEqual(["a"]);
    expect(client.getQueryData(queryKeys["provider-connections"]("team-b"))).toEqual(["b"]);
  });
});
