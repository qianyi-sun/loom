import { useQuery } from "@tanstack/react-query";
import { api } from "../api";
import { queryKeys } from "../api/queryKeys";
import { useDebouncedValue } from "./useDebouncedValue";

export function useBenchmarkDiscovery(ids: string[]) {
  const settledIds = useDebouncedValue(ids, 300);
  const query = useQuery({
    queryKey: queryKeys["benchmark-discovery"](settledIds),
    queryFn: () => api.discoverBenchmarks(settledIds),
    enabled: settledIds.length > 0,
    staleTime: 5 * 60 * 1000,
  });
  return { query, settledIds };
}
