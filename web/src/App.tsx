import { Route, Routes } from "react-router-dom";

import Layout from "./components/Layout";
import Benchmarks from "./pages/Benchmarks";
import CampaignDetail from "./pages/CampaignDetail";
import CampaignsList from "./pages/CampaignsList";
import NewCampaign from "./pages/NewCampaign";
import NotFound from "./pages/NotFound";
import RateCardsAdmin from "./pages/RateCardsAdmin";
import Settings from "./pages/Settings";
import Tasks from "./pages/Tasks";
import TrialCompare from "./pages/TrialCompare";
import TrialDetail from "./pages/TrialDetail";
import TrialsList from "./pages/TrialsList";
import UsageDashboard from "./pages/UsageDashboard";

export default function App(): JSX.Element {
  return (
    <Routes>
      <Route path="/" element={<Layout />}>
        <Route index element={<TrialsList />} />
        <Route path="trials" element={<TrialsList />} />
        <Route path="trials/compare" element={<TrialCompare />} />
        <Route path="trials/:trialId" element={<TrialDetail />} />
        <Route path="campaigns" element={<CampaignsList />} />
        <Route path="campaigns/new" element={<NewCampaign />} />
        <Route path="campaigns/:campaignId" element={<CampaignDetail />} />
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
