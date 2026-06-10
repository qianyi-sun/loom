/**
 * `cn(...)` — small wrapper around clsx for conditional Tailwind class
 * composition. Pure re-export with a curated type so call sites don't
 * have to remember whether to import `clsx` or `cn` (we keep it to
 * `cn` to mirror the shadcn-style convention most React teams now use).
 */
import clsx, { type ClassValue } from "clsx";

export function cn(...inputs: ClassValue[]): string {
  return clsx(...inputs);
}
