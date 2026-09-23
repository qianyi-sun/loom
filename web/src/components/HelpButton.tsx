import { useContext, type ReactNode } from "react";
import { Link } from "react-router-dom";
import type { HelpTopicId } from "../lib/helpContent";
import { HelpContext } from "./helpContext";

export function HelpButton({ topic, children = "Help" }: { topic: HelpTopicId; children?: ReactNode }): JSX.Element {
  const openHelp = useContext(HelpContext);
  const className = "inline-flex items-center gap-2 rounded-lg px-3 py-2 text-sm font-medium text-slate-600 hover:bg-slate-100 hover:text-slate-900 focus-visible:outline focus-visible:outline-2 focus-visible:outline-accent";
  return openHelp
    ? <button type="button" className={className} onClick={() => openHelp(topic)}>{children}</button>
    : <Link className={className} to={`/getting-started?topic=${topic}`}>{children}</Link>;
}
