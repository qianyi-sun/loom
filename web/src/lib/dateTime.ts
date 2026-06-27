export type LocalDateTimeOptions = {
  fallback?: string;
  timeZone?: string;
};

function partValue(
  parts: Intl.DateTimeFormatPart[],
  type: Intl.DateTimeFormatPartTypes,
): string {
  return parts.find((part) => part.type === type)?.value ?? "";
}

export function formatLocalDateTime(
  value: Date | string | null | undefined,
  options: LocalDateTimeOptions = {},
): string {
  const fallback = options.fallback ?? "—";
  if (value == null || value === "") return fallback;

  const date = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(date.getTime())) return fallback;

  const parts = new Intl.DateTimeFormat("en-US", {
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hourCycle: "h23",
    timeZoneName: "short",
    timeZone: options.timeZone,
  }).formatToParts(date);

  const year = partValue(parts, "year");
  const month = partValue(parts, "month");
  const day = partValue(parts, "day");
  const hour = partValue(parts, "hour");
  const minute = partValue(parts, "minute");
  const zone = partValue(parts, "timeZoneName");
  if (!year || !month || !day || !hour || !minute) return fallback;
  return `${year}-${month}-${day} ${hour}:${minute}${zone ? ` ${zone}` : ""}`;
}
