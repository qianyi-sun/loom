import { oracleSmokeBatchCommand } from "../lib/quickstartSnippets";
import { currentServerOrigin } from "../lib/serverOrigin";
export function CliSetupCommands({ token }: { token: string }): JSX.Element {
  const commands = [
    `export LOOM_API_TOKEN=${token}`,
    `loom auth login --server ${currentServerOrigin()} --token env:LOOM_API_TOKEN`,
    "loom auth whoami",
  ];
  return (
    <div className="space-y-2">
      <p className="text-sm font-medium text-emerald-950">CLI setup commands</p>
      <div className="space-y-1 rounded-lg border border-emerald-200 bg-white p-3">
        {commands.map((command) => (
          <code
            key={command}
            className="block whitespace-pre-wrap break-words font-mono text-xs leading-relaxed text-slate-800"
          >
            {command}
          </code>
        ))}
      </div>
      <p className="pt-2 text-sm font-medium text-emerald-950">Next CLI checks</p>
      <code className="block whitespace-pre-wrap break-words rounded-lg border border-emerald-200 bg-white p-3 font-mono text-xs leading-relaxed text-slate-800">
        {oracleSmokeBatchCommand()}
      </code>
    </div>
  );
}
