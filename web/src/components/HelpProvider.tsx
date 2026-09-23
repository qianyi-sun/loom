import { useContext, useState, type ReactNode } from "react";
import { Link } from "react-router-dom";
import { HELP_TOPICS, type HelpTopicId } from "../lib/helpContent";
import { AuthContext } from "../auth/authContextValue";
import { HelpContext } from "./helpContext";
import { Modal } from "./Modal";
import { RepositoryDocLinks } from "./RepositoryDocLinks";

export function HelpProvider({ children }: { children: ReactNode }): JSX.Element {
  const [topic, setTopic] = useState<HelpTopicId | null>(null);
  const isAdmin = useContext(AuthContext)?.isAdmin === true;
  const visibleTopic = topic === "rates" && !isAdmin ? "quickstart" : topic;
  const content = HELP_TOPICS[visibleTopic ?? "quickstart"];
  return (
    <HelpContext.Provider value={setTopic}>
      {children}
      <Modal open={topic !== null} onClose={() => setTopic(null)} title={content.title} description="Help for the page you are using" size="md" placement="right">
        <div data-context-help="true" className="space-y-6">
          <p className="text-sm leading-6 text-slate-600">{content.summary}</p>
          <ol className="list-decimal space-y-4 pl-5 text-sm leading-6 text-slate-700">{content.steps.map((step) => <li key={step}>{step}</li>)}</ol>
          <RepositoryDocLinks docs={content.docs} />
          <Link to={`/getting-started?topic=${visibleTopic ?? "quickstart"}`} onClick={() => setTopic(null)} className="inline-flex rounded-lg border border-slate-200 px-4 py-2 text-sm font-medium text-accent">Open Getting started</Link>
        </div>
      </Modal>
    </HelpContext.Provider>
  );
}
