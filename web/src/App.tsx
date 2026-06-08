import { Route, Routes } from "react-router-dom";

import Layout from "./components/Layout";
import Benchmarks from "./pages/Benchmarks";
import NotFound from "./pages/NotFound";
import Settings from "./pages/Settings";
import Tasks from "./pages/Tasks";
import TrialDetail from "./pages/TrialDetail";
import TrialsList from "./pages/TrialsList";

export default function App(): JSX.Element {
  return (
    <Routes>
      <Route path="/" element={<Layout />}>
        <Route index element={<TrialsList />} />
        <Route path="trials" element={<TrialsList />} />
        <Route path="trials/:trialId" element={<TrialDetail />} />
        <Route path="tasks" element={<Tasks />} />
        <Route path="benchmarks" element={<Benchmarks />} />
        <Route path="settings" element={<Settings />} />
        <Route path="*" element={<NotFound />} />
      </Route>
    </Routes>
  );
}
