import { Link } from "react-router-dom";

export default function NotFound(): JSX.Element {
  return (
    <div className="flex min-h-[40vh] flex-col items-center justify-center gap-3 text-center">
      <p className="text-xs uppercase tracking-wider text-slate-400">404</p>
      <h1 className="text-2xl font-bold text-slate-900">Page not found</h1>
      <Link
        to="/trials"
        className="text-sm font-medium text-accent hover:text-accent-hover"
      >
        ← Back to trials
      </Link>
    </div>
  );
}
