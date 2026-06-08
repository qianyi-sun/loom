import type { ApiError } from "../api/client";

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
    <div className="loom-error">
      <strong>{title}</strong>
      {detail ? <div className="loom-mono">{detail}</div> : null}
    </div>
  );
}
