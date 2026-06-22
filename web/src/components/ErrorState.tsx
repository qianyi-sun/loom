import type { ApiError } from "../api/client";
import { redactText } from "../lib/redaction";

export default function ErrorState({
  error,
}: {
  error: unknown;
}): JSX.Element {
  let title = "Something went wrong";
  let detail = "";
  if (error && typeof error === "object" && "status" in error) {
    const e = error as ApiError;
    title = `Error ${e.status}`;
    detail = e.detail;
  } else if (error instanceof Error) {
    detail = error.message;
  } else {
    detail = String(error);
  }
  return (
    <div className="rounded-xl border border-red-200 bg-red-50 px-5 py-4 text-sm">
      <p className="font-semibold text-red-800">{title}</p>
      {detail ? (
        <p className="mt-1 font-mono text-xs leading-relaxed text-red-700">
          {redactText(detail)}
        </p>
      ) : null}
    </div>
  );
}
