/** TaskSet IDs are opaque API identifiers, not browser path segments. */
export function taskSetHref(id: string): string {
  return `/task-sets/detail?${new URLSearchParams({ id })}`;
}
