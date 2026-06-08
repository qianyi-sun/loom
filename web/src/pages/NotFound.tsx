import { Link } from "react-router-dom";

export default function NotFound(): JSX.Element {
  return (
    <div className="loom-empty">
      <h2>Page not found</h2>
      <p>
        <Link to="/trials">Back to trials</Link>
      </p>
    </div>
  );
}
