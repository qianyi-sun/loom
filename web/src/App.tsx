import { Route, Routes } from "react-router-dom";

import Layout from "./components/Layout";
import Benchmarks from "./pages/Benchmarks";
import BatchDetail from "./pages/BatchDetail";
import BatchesList from "./pages/BatchesList";
import NewBatch from "./pages/NewBatch";
import NewWorkflow from "./pages/NewWorkflow";
import NotFound from "./pages/NotFound";
import RateCardsAdmin from "./pages/RateCardsAdmin";
import Settings from "./pages/Settings";
import Tasks from "./pages/Tasks";
import TrialCompare from "./pages/TrialCompare";
import TrialDetail from "./pages/TrialDetail";
import TrialsList from "./pages/TrialsList";
import UsageDashboard from "./pages/UsageDashboard";
import WorkflowDetail from "./pages/WorkflowDetail";
import Workflows from "./pages/Workflows";

export default function App(): JSX.Element {
  return (
    <Routes>
      <Route path="/" element={<Layout />}>
        <Route index element={<TrialsList />} />
        <Route path="trials" element={<TrialsList />} />
        <Route path="trials/compare" element={<TrialCompare />} />
        <Route path="trials/:trialId" element={<TrialDetail />} />
        <Route path="batches" element={<BatchesList />} />
        <Route path="batches/new" element={<NewBatch />} />
        <Route path="batches/:batchId" element={<BatchDetail />} />
        <Route path="workflows" element={<Workflows />} />
        <Route path="workflows/new" element={<NewWorkflow />} />
        <Route path="workflows/:workflowId" element={<WorkflowDetail />} />
        <Route path="tasks" element={<Tasks />} />
        <Route path="benchmarks" element={<Benchmarks />} />
        <Route path="usage" element={<UsageDashboard />} />
        <Route path="rate-cards" element={<RateCardsAdmin />} />
        <Route path="settings" element={<Settings />} />
        <Route path="*" element={<NotFound />} />
      </Route>
    </Routes>
  );
}
