export default function JsonViewer({
  data,
}: {
  data: unknown;
}): JSX.Element {
  return (
    <pre
      className="loom-mono"
      style={{
        background: "var(--color-bg)",
        border: "1px solid var(--color-border)",
        borderRadius: "4px",
        padding: "0.6rem",
        overflowX: "auto",
        maxHeight: "400px",
      }}
    >
      {JSON.stringify(data, null, 2)}
    </pre>
  );
}
